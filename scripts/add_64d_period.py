# -*- coding: utf-8 -*-
"""
scripts/add_64d_period.py —— 为 A_DB 日线体系无损扩展 64d 周期存量回填脚本

功能：
1. 原地扩展 daily_snapshot 表结构：新增 sk_64d ~ mdxl_64d 共 8 列
2. 刷新前复权视图 (v_daily_qfq, v_minute_qfq)
3. 分批 (每批 500 只标的) 读取历史日线数据，利用 Numba 极速计算 64d 指标
4. 原地批量 UPDATE 回写 daily_snapshot，并同步更新 incremental_state 的 64d 状态
5. 进程互斥保护与断点续传支持
"""

import os
import sys
import time
import logging
import datetime
import numpy as np
import pandas as pd
import duckdb

try:
    import msvcrt
except ImportError:
    msvcrt = None

# 确保项目根目录在 sys.path 中
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.schema import VIEWS_DDL
from src.kdj_engine import PeriodState, states_to_df, _compute_period_kdj_numba
from src.fetcher import get_cn_now

DEFAULT_DB_PATH = r"D:\DB\A_DB.duckdb"
DEFAULT_LOCK_PATH = os.path.join(project_root, "logs", "update.lock")
BATCH_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("add_64d_period")


class SingleInstanceLock:
    """跨进程单实例互斥文件锁（基于 Windows msvcrt.locking）"""

    def __init__(self, lock_file_path: str):
        self.lock_file_path = lock_file_path
        self.handle = None

    def acquire(self) -> bool:
        if msvcrt is None:
            return True
        os.makedirs(os.path.dirname(self.lock_file_path), exist_ok=True)
        try:
            self.handle = open(self.lock_file_path, "a+")
            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            self.handle.truncate(0)
            self.handle.write(f"{os.getpid()}\n")
            self.handle.flush()
            return True
        except (IOError, OSError):
            if self.handle:
                try:
                    self.handle.close()
                except Exception:
                    pass
                self.handle = None
            return False

    def release(self):
        if self.handle:
            try:
                self.handle.seek(0)
                if msvcrt:
                    msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            except Exception:
                pass
            try:
                self.handle.close()
            except Exception:
                pass
            self.handle = None


def upgrade_schema_and_views(con: duckdb.DuckDBPyConnection) -> None:
    """幂等升级 daily_snapshot 物理表结构并刷新视图"""
    cols_info = con.execute("PRAGMA table_info('daily_snapshot')").df()
    existing_cols = set(cols_info["name"].str.lower())

    cols_to_add = [
        ("sk_64d", "DOUBLE"),
        ("sd_64d", "DOUBLE"),
        ("skxl_64d", "DOUBLE"),
        ("sdxl_64d", "DOUBLE"),
        ("mk_64d", "DOUBLE"),
        ("md_64d", "DOUBLE"),
        ("mkxl_64d", "DOUBLE"),
        ("mdxl_64d", "DOUBLE"),
    ]

    added = []
    for col_name, col_type in cols_to_add:
        if col_name not in existing_cols:
            con.execute(f"ALTER TABLE daily_snapshot ADD COLUMN {col_name} {col_type};")
            added.append(col_name)

    if added:
        logger.info(f"daily_snapshot 表已成功添加 8 列: {', '.join(added)}")
    else:
        logger.info("daily_snapshot 表已包含 64d 列，跳过 ALTER TABLE")

    # 刷新视图定义
    con.execute(VIEWS_DDL)
    logger.info("已刷新 v_daily_qfq 与 v_minute_qfq 视图定义")


def run_64d_backfill(db_path: str = DEFAULT_DB_PATH, batch_size: int = BATCH_SIZE) -> None:
    """执行 64d 存量历史回填与增量状态构建"""
    lock = SingleInstanceLock(DEFAULT_LOCK_PATH)
    if not lock.acquire():
        logger.error(f"获取互斥锁失败 ({DEFAULT_LOCK_PATH})，已有其他更新或迁移进程正在运行，退出。")
        sys.exit(1)

    t0 = time.time()
    logger.info("==================================================")
    logger.info("  A_DB 日线 64d 周期存量回填与状态构建迁移启动")
    logger.info(f"  目标数据库: {db_path}")
    logger.info(f"  批次大小: {batch_size}")
    logger.info("==================================================")

    try:
        con = duckdb.connect(db_path)

        # 1. 表结构与视图原地升级
        upgrade_schema_and_views(con)

        # 2. 读取全部证券列表并检查断点续传
        all_symbols = con.execute("SELECT symbol FROM security_info ORDER BY symbol ASC").df()["symbol"].tolist()
        total_symbols = len(all_symbols)

        # 断点续传检查：已在 incremental_state 中存在 64d 且日线已填充 64d 的标的自动跳过
        done_syms = set(con.execute("""
            SELECT s.symbol
            FROM incremental_state s
            LEFT JOIN (
                SELECT symbol, count(sk_64d) as cnt_64d, count(*) as cnt_total
                FROM daily_snapshot
                GROUP BY symbol
            ) d ON s.symbol = d.symbol
            WHERE s.period = '64d' AND (d.symbol IS NULL OR d.cnt_64d = d.cnt_total)
        """).df()["symbol"])

        todo_symbols = [s for s in all_symbols if s not in done_syms]
        logger.info(f"全市场共 {total_symbols} 只标的，已完成: {len(done_syms)}，待处理: {len(todo_symbols)}")

        if not todo_symbols:
            logger.info("所有标的 64d 周期已全部构建完毕，无需处理。")
            con.close()
            return

        # 3. 分批处理
        total_batches = (len(todo_symbols) + batch_size - 1) // batch_size
        empty_array = np.array([], dtype=np.float64)

        for b_idx in range(total_batches):
            b_start = b_idx * batch_size
            b_symbols = todo_symbols[b_start : b_start + batch_size]
            b_sym_set = set(b_symbols)

            t_b0 = time.time()

            # 批量读取本批次标的的历史日线
            # 注意使用参数化或列表转 tuple 查询
            placeholders = ", ".join(["?"] * len(b_symbols))
            query = f"""
                SELECT symbol, trade_date, bar_seq, high, low, close, adj_factor
                FROM daily_snapshot
                WHERE symbol IN ({placeholders})
                ORDER BY symbol ASC, bar_seq ASC
            """
            df_batch = con.execute(query, b_symbols).df()

            batch_updates = []
            batch_states = []

            if not df_batch.empty:
                # 按标的分组计算
                for sym, grp in df_batch.groupby("symbol", sort=False):
                    bar_seqs = grp["bar_seq"].to_numpy(dtype=np.int32)
                    adj_factors = grp["adj_factor"].to_numpy(dtype=np.float64)
                    high_adj = grp["high"].to_numpy(dtype=np.float64) * adj_factors
                    low_adj = grp["low"].to_numpy(dtype=np.float64) * adj_factors
                    close_adj = grp["close"].to_numpy(dtype=np.float64) * adj_factors

                    # 64d 桶闭合标志: (bar_seq + 1) % 64 == 0
                    is_closed = ((bar_seqs + 1) % 64 == 0)

                    st_init = PeriodState.initial(sym, "64d")
                    res = _compute_period_kdj_numba(
                        high_adj, low_adj, close_adj, is_closed,
                        empty_array, empty_array,
                        st_init.sk_closed, st_init.sd_closed, st_init.mk_closed, st_init.md_closed,
                        st_init.last_valid_rsv13, st_init.last_valid_rsv104,
                        0, 0.0, 0.0, -1
                    )

                    metrics_arr = res[0]  # shape (N, 8)
                    trade_dates = grp["trade_date"].tolist()

                    for idx, td in enumerate(trade_dates):
                        batch_updates.append((
                            sym, td,
                            metrics_arr[idx, 0],  # sk_64d
                            metrics_arr[idx, 1],  # sd_64d
                            metrics_arr[idx, 2],  # skxl_64d
                            metrics_arr[idx, 3],  # sdxl_64d
                            metrics_arr[idx, 4],  # mk_64d
                            metrics_arr[idx, 5],  # md_64d
                            metrics_arr[idx, 6],  # mkxl_64d
                            metrics_arr[idx, 7],  # mdxl_64d
                        ))

                    out_state = PeriodState(
                        symbol=sym,
                        period="64d",
                        last_closed_bucket_id=res[12],
                        closed_highs_adj=list(res[1]),
                        closed_lows_adj=list(res[2]),
                        sk_closed=res[3],
                        sd_closed=res[4],
                        mk_closed=res[5],
                        md_closed=res[6],
                        last_valid_rsv13=res[7],
                        last_valid_rsv104=res[8],
                        bar_count=res[9],
                        high_adj=res[10] if res[9] > 0 else None,
                        low_adj=res[11] if res[9] > 0 else None,
                    )
                    batch_states.append(out_state)

            # 对本批次中在 daily_snapshot 无日线记录的标的生成初始状态
            processed_syms = set(df_batch["symbol"].unique()) if not df_batch.empty else set()
            missing_syms = b_sym_set - processed_syms
            for ms in missing_syms:
                batch_states.append(PeriodState.initial(ms, "64d"))

            # 开启事务原地回写
            con.execute("BEGIN TRANSACTION;")

            # 回写 daily_snapshot
            if batch_updates:
                df_up = pd.DataFrame(batch_updates, columns=[
                    "symbol", "trade_date",
                    "sk_64d", "sd_64d", "skxl_64d", "sdxl_64d",
                    "mk_64d", "md_64d", "mkxl_64d", "mdxl_64d"
                ])
                con.register("tmp_64d_snap", df_up)
                con.execute("""
                    UPDATE daily_snapshot d
                    SET sk_64d = u.sk_64d,
                        sd_64d = u.sd_64d,
                        skxl_64d = u.skxl_64d,
                        sdxl_64d = u.sdxl_64d,
                        mk_64d = u.mk_64d,
                        md_64d = u.md_64d,
                        mkxl_64d = u.mkxl_64d,
                        mdxl_64d = u.mdxl_64d
                    FROM tmp_64d_snap u
                    WHERE d.symbol = u.symbol AND d.trade_date = u.trade_date;
                """)
                con.unregister("tmp_64d_snap")

            # 回写 incremental_state
            if batch_states:
                df_st = states_to_df(batch_states, updated_at=get_cn_now())
                con.register("tmp_64d_state", df_st)
                con.execute("""
                    INSERT OR REPLACE INTO incremental_state BY NAME
                    SELECT * FROM tmp_64d_state;
                """)
                con.unregister("tmp_64d_state")

            con.execute("COMMIT;")

            # 每 3 个批次执行一次 CHECKPOINT 释放 WAL 和内存
            if (b_idx + 1) % 3 == 0:
                con.execute("CHECKPOINT;")

            t_b1 = time.time()
            done_count = len(done_syms) + min((b_idx + 1) * batch_size, len(todo_symbols))
            logger.info(
                f"进度: 批次 [{b_idx + 1}/{total_batches}] | "
                f"标的: {len(b_symbols)} 只 | 更新日线: {len(batch_updates):,} 行 | "
                f"耗时: {t_b1 - t_b0:.2f}s | 累计进度: {done_count}/{total_symbols}"
            )

        # 全部批次完成，执行最终 CHECKPOINT
        logger.info("执行最终数据库 CHECKPOINT 固化...")
        con.execute("CHECKPOINT;")
        con.close()

        total_cost = time.time() - t0
        logger.info("==================================================")
        logger.info(f"  64d 周期存量回填与状态构建成功完成!")
        logger.info(f"  总耗时: {total_cost:.2f} 秒 ({total_cost / 60:.2f} 分钟)")
        logger.info("==================================================")

    finally:
        lock.release()


if __name__ == "__main__":
    run_64d_backfill()
