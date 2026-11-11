import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import time
import json
import duckdb
import numpy as np
import pandas as pd
from datetime import datetime
from src.schema import init_database, DEFAULT_DB_PATH, SOURCE_DB_PATH
from src.kdj_engine import MultiCycleKDJEngine, PeriodState, DAILY_PERIOD_NAMES, MINUTE_PERIOD_NAMES

PILOT_SYMBOLS = [
    ('SH600000', 'SH600000', '600000', '浦发银行', 'STOCK'),
    ('SZ000002', 'SZ000002', '000002', '万科A', 'STOCK'),
    ('SH000001', '000001', '000001', '上证指数', 'BROAD_INDEX'),
    ('TDX880301', '880301', '880301', '煤炭开采', 'INDUSTRY_INDEX')
]


def run_pilot():
    print("=" * 60)
    print(">>> 启动阶段 2 小样本试跑与核验程序")
    print("=" * 60)

    # 保护生产库：若 daily_snapshot 超过 100 万行且未加 --force-reset-pilot，拒绝清空与覆盖
    force_reset = "--force-reset-pilot" in sys.argv
    if os.path.exists(DEFAULT_DB_PATH):
        chk_conn = duckdb.connect(DEFAULT_DB_PATH, read_only=True)
        try:
            d_cnt = chk_conn.execute("SELECT COUNT(*) FROM daily_snapshot").fetchone()[0]
            if d_cnt > 1000000 and not force_reset:
                print(f"\n[安全拦截] 目标库 {DEFAULT_DB_PATH} 包含全量生产数据 ({d_cnt:,} 行日线)。")
                print("直接运行 pilot_verify 会清空样本标的的增量数据！")
                print("若确需在生产库中强制重置核验，请使用: python scripts/pilot_verify.py --force-reset-pilot\n")
                return
        except Exception:
            pass
        finally:
            chk_conn.close()

    # 1. 确保目标库与字典表初始化
    init_database()
    target_conn = duckdb.connect(DEFAULT_DB_PATH)
    source_conn = duckdb.connect(SOURCE_DB_PATH, read_only=True)

    # 清空可能存在的旧快照和增量状态 (仅清空样本标的)
    for sym_new, _, _, _, _ in PILOT_SYMBOLS:
        target_conn.execute(f"DELETE FROM daily_snapshot WHERE symbol = '{sym_new}'")
        target_conn.execute(f"DELETE FROM minute_snapshot WHERE symbol = '{sym_new}'")
        target_conn.execute(f"DELETE FROM incremental_state WHERE symbol = '{sym_new}'")

    summary = {
        'symbols': [],
        'perf': {},
        'consistency': {},
        'samples': {}
    }

    total_calc_time = 0.0
    total_daily_rows = 0
    total_minute_rows = 0

    for sym_new, sym_old, code, name, sec_type in PILOT_SYMBOLS:
        print(f"\n---> 开始处理样本标的: {sym_new} ({name}, {sec_type})")
        sym_perf = {}

        # ----------------------------------------------------
        # 2. 提取日线数据
        # ----------------------------------------------------
        t0 = time.perf_counter()
        if sec_type == 'STOCK':
            df_daily_raw = source_conn.execute(f"""
                SELECT 
                    '{sym_new}' AS symbol,
                    '{code}' AS code,
                    d.trade_date,
                    d.open, d.high, d.low, d.close, d.volume, d.amount,
                    COALESCE(a.factor, 1.0) AS adj_factor
                FROM daily_bar d
                LEFT JOIN adj_factor a ON d.symbol = a.symbol AND d.trade_date = a.trade_date
                WHERE d.symbol = '{sym_old}'
                ORDER BY d.trade_date ASC
            """).df()
        else:
            # 指数恒为 1.0
            df_daily_raw = source_conn.execute(f"""
                SELECT 
                    '{sym_new}' AS symbol,
                    '{code}' AS code,
                    d.trade_date,
                    d.open, d.high, d.low, d.close, d.volume, d.amount,
                    1.0 AS adj_factor
                FROM daily_bar d
                WHERE d.symbol = '{sym_old}'
                ORDER BY d.trade_date ASC
            """).df()
        t_daily_extract = time.perf_counter() - t0
        sym_perf['daily_extract_s'] = t_daily_extract
        sym_perf['daily_rows'] = len(df_daily_raw)
        total_daily_rows += len(df_daily_raw)

        # ----------------------------------------------------
        # 3. 计算日线多周期快照
        # ----------------------------------------------------
        ltgb_row = target_conn.execute(
            "SELECT liutongguben FROM security_info WHERE symbol = ?",
            [sym_new],
        ).fetchone()
        ltgb_val = float(ltgb_row[0]) if (ltgb_row and ltgb_row[0] is not None and sec_type == 'STOCK') else None

        df_daily_raw['bar_seq'] = np.arange(len(df_daily_raw), dtype=np.int32)
        t0 = time.perf_counter()
        df_daily_snap, d_states = MultiCycleKDJEngine.compute_daily_snapshot(
            df_daily_raw, liutongguben=ltgb_val
        )
        t_daily_calc = time.perf_counter() - t0
        sym_perf['daily_calc_s'] = t_daily_calc
        total_calc_time += t_daily_calc

        # ----------------------------------------------------
        # 4. 提取分钟线数据 (2024-09-06 起预热)
        # ----------------------------------------------------
        t0 = time.perf_counter()
        if sec_type == 'STOCK':
            df_min_raw = source_conn.execute(f"""
                SELECT 
                    '{sym_new}' AS symbol,
                    '{code}' AS code,
                    m.trading_date,
                    m.bar_end,
                    m.slot_seq,
                    m.open, m.high, m.low, m.close, m.volume, m.amount,
                    COALESCE(a.factor, 1.0) AS adj_factor
                FROM min15_bar m
                LEFT JOIN adj_factor a ON m.symbol = a.symbol AND m.trading_date = a.trade_date
                WHERE m.symbol = '{sym_old}'
                ORDER BY m.bar_end ASC
            """).df()
        else:
            df_min_raw = source_conn.execute(f"""
                SELECT 
                    '{sym_new}' AS symbol,
                    '{code}' AS code,
                    m.trading_date,
                    m.bar_end,
                    m.slot_seq,
                    m.open, m.high, m.low, m.close, m.volume, m.amount,
                    1.0 AS adj_factor
                FROM min15_bar m
                WHERE m.symbol = '{sym_old}'
                ORDER BY m.bar_end ASC
            """).df()
        t_min_extract = time.perf_counter() - t0
        sym_perf['minute_extract_s'] = t_min_extract
        sym_perf['minute_raw_rows'] = len(df_min_raw)

        # ----------------------------------------------------
        # 5. 计算分钟线多周期快照 (预热后截取 >= 2024-10-01)
        # ----------------------------------------------------
        t0 = time.perf_counter()
        df_min_snap, m_states = MultiCycleKDJEngine.compute_minute_snapshot(df_min_raw, cutoff_date='2024-10-01')
        t_min_calc = time.perf_counter() - t0
        sym_perf['minute_calc_s'] = t_min_calc
        sym_perf['minute_cut_rows'] = len(df_min_snap)
        total_calc_time += t_min_calc
        total_minute_rows += len(df_min_snap)

        # ----------------------------------------------------
        # 6. 数据入库 A_DB.duckdb (严格使用 BY NAME 杜绝任何列错位)
        # ----------------------------------------------------
        t0 = time.perf_counter()
        # 写入 daily_snapshot
        target_conn.register("df_daily_snap", df_daily_snap)
        target_conn.execute("INSERT OR REPLACE INTO daily_snapshot BY NAME SELECT * FROM df_daily_snap")
        target_conn.unregister("df_daily_snap")

        # 写入 minute_snapshot
        target_conn.register("df_min_snap", df_min_snap)
        target_conn.execute("INSERT OR REPLACE INTO minute_snapshot BY NAME SELECT * FROM df_min_snap")
        target_conn.unregister("df_min_snap")

        # 更新 security_info.latest_adj_factor
        latest_factor = float(df_daily_snap['adj_factor'].iloc[-1])
        target_conn.execute(f"UPDATE security_info SET latest_adj_factor = {latest_factor} WHERE symbol = '{sym_new}'")

        # 写入 incremental_state (10 个周期: 6 日线 + 4 分钟线)
        now_ts = datetime.now()
        inc_rows = []
        for p_name, st in d_states.items():
            inc_rows.append(st.to_db_row(updated_at=now_ts))
        for p_name, st in m_states.items():
            inc_rows.append(st.to_db_row(updated_at=now_ts))
        df_inc_rows = pd.DataFrame(inc_rows, columns=[
            'symbol', 'period', 'last_closed_bucket_id', 'closed_highs_adj', 'closed_lows_adj',
            'sk_closed', 'sd_closed', 'mk_closed', 'md_closed', 'last_valid_rsv13', 'last_valid_rsv104',
            'bar_count', 'high_adj', 'low_adj', 'updated_at'
        ])
        target_conn.register("df_inc_rows", df_inc_rows)
        target_conn.execute("INSERT OR REPLACE INTO incremental_state BY NAME SELECT * FROM df_inc_rows")
        target_conn.unregister("df_inc_rows")
        t_insert = time.perf_counter() - t0
        sym_perf['db_insert_s'] = t_insert

        summary['perf'][sym_new] = sym_perf

        # ----------------------------------------------------
        # 7. 真实数据增量精度一致性验证 (断言全量 vs 增量完全相等)
        # ----------------------------------------------------
        # 取最新 30 根日线 bar
        split_idx = len(df_daily_raw) - 30
        df_d_base = df_daily_raw.iloc[:split_idx]
        df_d_tail = df_daily_raw.iloc[split_idx:]
        _, base_states = MultiCycleKDJEngine.compute_daily_snapshot(
            df_d_base, liutongguben=ltgb_val
        )
        prev_vols = df_d_base['volume'].iloc[-9:].tolist()
        df_tail_inc, inc_states = MultiCycleKDJEngine.compute_daily_snapshot(
            df_d_tail,
            init_states=base_states,
            prev_volumes=prev_vols,
            liutongguben=ltgb_val,
        )

        max_daily_err = 0.0
        for p in DAILY_PERIOD_NAMES:
            for m in ['sk', 'sd', 'skxl', 'sdxl', 'mk', 'md', 'mkxl', 'mdxl']:
                col = f"{m}_{p}"
                full_vals = df_daily_snap[col].iloc[split_idx:].to_numpy()
                inc_vals = df_tail_inc[col].to_numpy()
                err = np.max(np.abs(full_vals - inc_vals))
                if err > max_daily_err:
                    max_daily_err = err

        # 取最新 16 根分钟线 bar
        m_split = len(df_min_raw) - 16
        df_m_base = df_min_raw.iloc[:m_split]
        df_m_tail = df_min_raw.iloc[m_split:]
        _, m_base_states = MultiCycleKDJEngine.compute_minute_snapshot(df_m_base, cutoff_date=None)
        df_m_tail_inc, m_inc_states = MultiCycleKDJEngine.compute_minute_snapshot(df_m_tail, cutoff_date=None, init_states=m_base_states)

        max_min_err = 0.0
        # 从全量无截取的输出对比最后 16 根
        df_m_full_all, _ = MultiCycleKDJEngine.compute_minute_snapshot(df_min_raw, cutoff_date=None)
        for p in MINUTE_PERIOD_NAMES:
            for m in ['sk', 'sd', 'skxl', 'sdxl', 'mk', 'md', 'mkxl', 'mdxl']:
                col = f"{m}_{p}"
                full_vals = df_m_full_all[col].iloc[m_split:].to_numpy()
                inc_vals = df_m_tail_inc[col].to_numpy()
                err = np.max(np.abs(full_vals - inc_vals))
                if err > max_min_err:
                    max_min_err = err

        summary['consistency'][sym_new] = {
            'max_daily_err': float(max_daily_err),
            'max_min_err': float(max_min_err),
            'passed': (max_daily_err < 1e-12 and max_min_err < 1e-12)
        }

        print(f"  √ 日线: {sym_perf['daily_rows']} 行 (计算耗时 {sym_perf['daily_calc_s']:.3f}s)")
        print(f"  √ 分钟线: {sym_perf['minute_cut_rows']} 行 (预热后耗时 {sym_perf['minute_calc_s']:.3f}s)")
        print(f"  √ 增量一致性: 日线最大误差 {max_daily_err:.2e}, 分钟线最大误差 {max_min_err:.2e} (通过: {summary['consistency'][sym_new]['passed']})")

        # ----------------------------------------------------
        # 8. 抽样采样数据 (首日 2024-10-08 与末日 2026-10-30)
        # ----------------------------------------------------
        d_sample_first = target_conn.execute(f"SELECT * FROM daily_snapshot WHERE symbol='{sym_new}' AND trade_date='2024-10-08'").df().to_dict(orient='records')
        d_sample_last = target_conn.execute(f"SELECT * FROM daily_snapshot WHERE symbol='{sym_new}' AND trade_date='2026-10-30'").df().to_dict(orient='records')
        m_sample_first = target_conn.execute(f"SELECT * FROM minute_snapshot WHERE symbol='{sym_new}' AND trading_date='2024-10-08' ORDER BY bar_end ASC LIMIT 2").df().to_dict(orient='records')
        m_sample_last = target_conn.execute(f"SELECT * FROM minute_snapshot WHERE symbol='{sym_new}' AND trading_date='2026-10-30' ORDER BY bar_end DESC LIMIT 2").df().to_dict(orient='records')

        summary['samples'][sym_new] = {
            'daily_20241008': d_sample_first[0] if d_sample_first else None,
            'daily_20261030': d_sample_last[0] if d_sample_last else None,
            'minute_20241008': m_sample_first,
            'minute_20261030': m_sample_last
        }

    # 验证视图查询
    v_daily_cnt = target_conn.execute("SELECT count(*) FROM v_daily_qfq").fetchone()[0]
    v_min_cnt = target_conn.execute("SELECT count(*) FROM v_minute_qfq").fetchone()[0]
    inc_state_cnt = target_conn.execute("SELECT count(*) FROM incremental_state").fetchone()[0]

    print("\n" + "=" * 60)
    print(f"试跑完成！A_DB 样本总行数:")
    print(f"  - daily_snapshot: {total_daily_rows:,} 行 (视图 v_daily_qfq: {v_daily_cnt:,} 行)")
    print(f"  - minute_snapshot: {total_minute_rows:,} 行 (视图 v_minute_qfq: {v_min_cnt:,} 行)")
    print(f"  - incremental_state: {inc_state_cnt} 行 (4 标的 × 10 周期)")
    print(f"  - 4 标的总纯计算耗时: {total_calc_time:.3f}s (平均单标的 {total_calc_time/4:.3f}s)")
    print("=" * 60)

    summary['totals'] = {
        'total_daily_rows': total_daily_rows,
        'total_minute_rows': total_minute_rows,
        'inc_state_cnt': inc_state_cnt,
        'total_calc_time_s': total_calc_time,
        'avg_calc_time_per_symbol_s': total_calc_time / 4
    }

    # 保存核验元数据
    with open(r"D:\DB\docs\phase2_pilot_details.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

    target_conn.close()
    source_conn.close()
    return summary


if __name__ == '__main__':
    run_pilot()
