import os
import sys
import gc
import time
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor

import duckdb
import numpy as np
import pandas as pd

from src.schema import DEFAULT_DB_PATH, SOURCE_DB_PATH, init_database
from src.kdj_engine import (
    MultiCycleKDJEngine, PeriodState,
    DAILY_SNAPSHOT_COLUMNS, MINUTE_SNAPSHOT_COLUMNS
)


class FullMarketBuilder:
    """全市场多周期快照数据库批量构建引擎"""

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        source_db_path: str = SOURCE_DB_PATH,
        batch_size: int = 100,
        max_workers: Optional[int] = None
    ):
        self.db_path = db_path
        self.source_db_path = source_db_path
        self.batch_size = batch_size
        self.max_workers = max_workers or min(os.cpu_count() or 4, 8)

    def get_todo_symbols(self) -> List[Dict]:
        """
        获取待构建标的列表 (支持断点续传: 自动跳过 incremental_state 中已满 10 个周期的标的)
        """
        tgt_conn = duckdb.connect(self.db_path, read_only=True)
        # 1. 全部标的
        all_secs = tgt_conn.execute("""
            SELECT symbol, code, name, sec_type, exchange
            FROM security_info
            ORDER BY symbol ASC
        """).df()

        # 2. 检查已完成标的 (满 10 个周期)
        try:
            done_syms = set(tgt_conn.execute("""
                SELECT symbol
                FROM incremental_state
                GROUP BY symbol
                HAVING count(*) = 10
            """).df()['symbol'])
        except Exception:
            done_syms = set()

        tgt_conn.close()

        # 映射 source_symbol: 股票为 symbol, 指数为 code
        records = []
        for _, row in all_secs.iterrows():
            sym = row['symbol']
            if sym in done_syms:
                continue
            src_sym = row['code'] if row['sec_type'] != 'STOCK' else sym
            records.append({
                'symbol': sym,
                'code': row['code'],
                'name': row['name'],
                'sec_type': row['sec_type'],
                'exchange': row['exchange'],
                'source_symbol': src_sym
            })

        return records

    @staticmethod
    def _compute_single_symbol(
        sym_dict: Dict,
        df_d_sym: Optional[pd.DataFrame],
        df_m_sym: Optional[pd.DataFrame]
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], List[tuple], float]:
        """单标的并发计算工作单元"""
        sym = sym_dict['symbol']
        code = sym_dict['code']
        latest_adj = 1.0
        d_snap_df = None
        m_snap_df = None
        inc_rows = []
        now_ts = datetime.now()

        # 1. 计算日线
        if df_d_sym is not None and len(df_d_sym) > 0:
            df_d_sym = df_d_sym.sort_values('trade_date').reset_index(drop=True)
            df_d_sym['bar_seq'] = np.arange(len(df_d_sym), dtype=np.int32)
            d_snap_df, d_states = MultiCycleKDJEngine.compute_daily_snapshot(df_d_sym)
            latest_adj = float(df_d_sym['adj_factor'].iloc[-1])
            for st in d_states.values():
                inc_rows.append(st.to_db_row(updated_at=now_ts))

        # 2. 计算分钟线 (预热并截取 >= 2024-10-01)
        if df_m_sym is not None and len(df_m_sym) > 0:
            df_m_sym = df_m_sym.sort_values('bar_end').reset_index(drop=True)
            m_snap_df, m_states = MultiCycleKDJEngine.compute_minute_snapshot(
                df_m_sym, cutoff_date='2024-10-01'
            )
            for st in m_states.values():
                inc_rows.append(st.to_db_row(updated_at=now_ts))

        return d_snap_df, m_snap_df, inc_rows, latest_adj

    def process_batch(
        self,
        batch_syms: List[Dict],
        src_conn: duckdb.DuckDBPyConnection,
        tgt_conn: duckdb.DuckDBPyConnection
    ) -> Dict:
        """批量读取、并行计算与事务写入单批标的"""
        t_start = time.perf_counter()
        batch_df = pd.DataFrame(batch_syms)

        # ------------------------------------------------------------------
        # Step A: 批量单 SQL 读取该批次所有标的的日线与分钟线数据
        # ------------------------------------------------------------------
        t0 = time.perf_counter()
        src_conn.register("batch_syms_meta", batch_df[['symbol', 'code', 'source_symbol', 'sec_type']])

        # 批量读取日线
        df_daily_raw = src_conn.execute("""
            SELECT 
                s.symbol,
                s.code,
                d.trade_date,
                d.open, d.high, d.low, d.close, d.volume, d.amount,
                CASE WHEN s.sec_type = 'STOCK' THEN COALESCE(a.factor, 1.0) ELSE 1.0 END AS adj_factor
            FROM daily_bar d
            JOIN batch_syms_meta s ON d.symbol = s.source_symbol
            LEFT JOIN adj_factor a ON d.symbol = a.symbol AND d.trade_date = a.trade_date
            ORDER BY s.symbol, d.trade_date ASC
        """).df()

        # 批量读取分钟线 (2024-09-06 起递推预热)
        df_min_raw = src_conn.execute("""
            SELECT 
                s.symbol,
                s.code,
                m.trading_date,
                m.bar_end,
                m.slot_seq,
                m.open, m.high, m.low, m.close, m.volume, m.amount,
                CASE WHEN s.sec_type = 'STOCK' THEN COALESCE(a.factor, 1.0) ELSE 1.0 END AS adj_factor
            FROM min15_bar m
            JOIN batch_syms_meta s ON m.symbol = s.source_symbol
            LEFT JOIN adj_factor a ON m.symbol = a.symbol AND m.trading_date = a.trade_date
            WHERE m.trading_date >= '2024-09-06'
            ORDER BY s.symbol, m.bar_end ASC
        """).df()

        src_conn.unregister("batch_syms_meta")
        t_read = time.perf_counter() - t0

        # ------------------------------------------------------------------
        # Step B: 多线程并行计算多周期快照
        # ------------------------------------------------------------------
        t0 = time.perf_counter()
        # 字典快速分组 (极大加快切分速度)
        daily_groups = {sym: df_sub for sym, df_sub in df_daily_raw.groupby('symbol', sort=False)}
        min_groups = {sym: df_sub for sym, df_sub in df_min_raw.groupby('symbol', sort=False)}

        tasks = []
        for s_dict in batch_syms:
            sym = s_dict['symbol']
            tasks.append((s_dict, daily_groups.get(sym), min_groups.get(sym)))

        d_snap_list = []
        m_snap_list = []
        all_inc_rows = []
        latest_adj_updates = []  # (latest_adj, symbol)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._compute_single_symbol, t[0], t[1], t[2]) for t in tasks]
            for f in futures:
                d_snap, m_snap, inc_rows, latest_adj = f.result()
                if d_snap is not None and len(d_snap) > 0:
                    d_snap_list.append(d_snap)
                if m_snap is not None and len(m_snap) > 0:
                    m_snap_list.append(m_snap)
                all_inc_rows.extend(inc_rows)
                latest_adj_updates.append((latest_adj, s_dict['symbol']))

        t_calc = time.perf_counter() - t0

        # ------------------------------------------------------------------
        # Step C: 批量事务写入 A_DB.duckdb (统一严格使用 BY NAME 语法)
        # ------------------------------------------------------------------
        t0 = time.perf_counter()
        tgt_conn.execute("BEGIN TRANSACTION")

        # 1. 批量写入 daily_snapshot (新标的使用 IGNORE 或普通 INSERT 避免昂贵的唯一冲突扫描)
        if d_snap_list:
            batch_daily_df = pd.concat(d_snap_list, ignore_index=True)
            tgt_conn.register("df_batch_daily", batch_daily_df)
            tgt_conn.execute("INSERT OR IGNORE INTO daily_snapshot BY NAME SELECT * FROM df_batch_daily")
            tgt_conn.unregister("df_batch_daily")
            d_rows = len(batch_daily_df)
            del batch_daily_df
        else:
            d_rows = 0

        # 2. 批量写入 minute_snapshot
        if m_snap_list:
            batch_min_df = pd.concat(m_snap_list, ignore_index=True)
            tgt_conn.register("df_batch_min", batch_min_df)
            tgt_conn.execute("INSERT OR IGNORE INTO minute_snapshot BY NAME SELECT * FROM df_batch_min")
            tgt_conn.unregister("df_batch_min")
            m_rows = len(batch_min_df)
            del batch_min_df
        else:
            m_rows = 0

        # 3. 批量写入 incremental_state
        if all_inc_rows:
            batch_inc_df = pd.DataFrame(all_inc_rows, columns=[
                'symbol', 'period', 'last_closed_bucket_id', 'closed_highs_adj', 'closed_lows_adj',
                'sk_closed', 'sd_closed', 'mk_closed', 'md_closed', 'last_valid_rsv13', 'last_valid_rsv104',
                'bar_count', 'high_adj', 'low_adj', 'updated_at'
            ])
            tgt_conn.register("df_batch_inc", batch_inc_df)
            tgt_conn.execute("INSERT OR REPLACE INTO incremental_state BY NAME SELECT * FROM df_batch_inc")
            tgt_conn.unregister("df_batch_inc")
            del batch_inc_df

        # 4. 批量更新 security_info.latest_adj_factor
        if latest_adj_updates:
            df_adj_upd = pd.DataFrame(latest_adj_updates, columns=['latest_adj_factor', 'symbol'])
            tgt_conn.register("df_adj_upd", df_adj_upd)
            tgt_conn.execute("""
                UPDATE security_info s
                SET latest_adj_factor = u.latest_adj_factor
                FROM df_adj_upd u
                WHERE s.symbol = u.symbol
            """)
            tgt_conn.unregister("df_adj_upd")
            del df_adj_upd

        tgt_conn.execute("COMMIT")
        t_write = time.perf_counter() - t0
        t_total = time.perf_counter() - t_start

        inc_rows_count = len(all_inc_rows)

        # 主动清理内存垃圾
        del d_snap_list, m_snap_list, all_inc_rows, latest_adj_updates, daily_groups, min_groups, df_daily_raw, df_min_raw
        gc.collect()

        return {
            'symbols_count': len(batch_syms),
            'daily_rows': d_rows,
            'minute_rows': m_rows,
            'inc_rows': inc_rows_count,
            't_read': t_read,
            't_calc': t_calc,
            't_write': t_write,
            't_total': t_total
        }

    def build_all(self) -> Dict:
        """执行全市场全量构建主流程"""
        init_database(self.db_path, self.source_db_path)
        todo_symbols = self.get_todo_symbols()
        total_symbols = len(todo_symbols)

        print("=" * 70)
        print(f"全市场构建启动 | 总待处理标的: {total_symbols:,} 只 | 批大小: {self.batch_size} | 并行核数: {self.max_workers}")
        print("=" * 70)

        if total_symbols == 0:
            print("所有标的已构建完成，无需处理！")
            return {'status': 'ALREADY_COMPLETED'}

        src_conn = duckdb.connect(self.source_db_path, read_only=True)
        tgt_conn = duckdb.connect(self.db_path)

        tgt_conn.execute("SET memory_limit = '10GB'")
        tgt_conn.execute("SET preserve_insertion_order = false")
        tgt_conn.execute("SET threads = 8")

        batches = [todo_symbols[i:i + self.batch_size] for i in range(0, total_symbols, self.batch_size)]
        n_batches = len(batches)

        total_daily_rows = 0
        total_minute_rows = 0
        start_time = time.perf_counter()

        for b_idx, b_syms in enumerate(batches, 1):
            stat = self.process_batch(b_syms, src_conn, tgt_conn)
            total_daily_rows += stat['daily_rows']
            total_minute_rows += stat['minute_rows']
            elapsed = time.perf_counter() - start_time
            rate = (b_idx * self.batch_size) / elapsed if elapsed > 0 else 0

            print(
                f"[Batch {b_idx:02d}/{n_batches:02d}] 处理 {stat['symbols_count']} 只 | "
                f"日线: {stat['daily_rows']:,} 行 | 分钟线: {stat['minute_rows']:,} 行 | "
                f"耗时: {stat['t_total']:.1f}s (读 {stat['t_read']:.1f}s, 算 {stat['t_calc']:.1f}s, 写 {stat['t_write']:.1f}s) | "
                f"本轮进度: {min(b_idx * self.batch_size, total_symbols)}/{total_symbols} ({rate:.1f} 标的/秒)"
            )

            # 每 5 个批次进行一次 Checkpoint 整理内存页
            if b_idx % 5 == 0:
                tgt_conn.execute("CHECKPOINT")
                gc.collect()

        print("\n正在执行数据库 Checkpoint 与 Vacuum 整理...")
        tgt_conn.execute("CHECKPOINT")
        tgt_conn.execute("VACUUM")

        src_conn.close()
        tgt_conn.close()

        total_elapsed = time.perf_counter() - start_time
        print("=" * 70)
        print(f"全市场构建圆满完成！总耗时: {total_elapsed:.1f}秒 ({total_elapsed/60:.2f}分钟)")
        print(f"累计入库日线: {total_daily_rows:,} 行 | 分钟线: {total_minute_rows:,} 行")
        print("=" * 70)

        return {
            'status': 'SUCCESS',
            'total_symbols': total_symbols,
            'total_daily_rows': total_daily_rows,
            'total_minute_rows': total_minute_rows,
            'total_elapsed_s': total_elapsed
        }


if __name__ == '__main__':
    builder = FullMarketBuilder(batch_size=250)
    builder.build_all()
