# -*- coding: utf-8 -*-
"""
scripts/verify_64d.py —— A_DB 日线 64d 周期存量回填与计算精度核验脚本
"""

import os
import sys
import duckdb
import numpy as np
import pandas as pd

# 确保项目根目录在 sys.path 中
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.kdj_engine import MultiCycleKDJEngine

DEFAULT_DB_PATH = r"D:\DB\A_DB.duckdb"


def main(db_path: str = DEFAULT_DB_PATH):
    conn = duckdb.connect(db_path, read_only=True)

    print("=== 1. daily_snapshot 统计与 64d 空值核验 ===")
    d_stat = conn.execute("""
        SELECT 
            count(*) as total_rows,
            count(DISTINCT symbol) as symbol_count,
            count(CASE WHEN sk_64d IS NULL OR isnan(sk_64d) THEN 1 END) as null_sk_64d,
            count(CASE WHEN sd_64d IS NULL OR isnan(sd_64d) THEN 1 END) as null_sd_64d,
            count(CASE WHEN mk_64d IS NULL OR isnan(mk_64d) THEN 1 END) as null_mk_64d,
            count(CASE WHEN md_64d IS NULL OR isnan(md_64d) THEN 1 END) as null_md_64d
        FROM daily_snapshot
    """).fetchone()
    print(f"daily_snapshot 总行数: {d_stat[0]:,}")
    print(f"覆盖标的数: {d_stat[1]:,}")
    print(f"sk_64d 空值/NaN数: {d_stat[2]}")
    print(f"sd_64d 空值/NaN数: {d_stat[3]}")
    print(f"mk_64d 空值/NaN数: {d_stat[4]}")
    print(f"md_64d 空值/NaN数: {d_stat[5]}")

    print("\n=== 2. incremental_state 状态表核验 ===")
    inc_stat = conn.execute("""
        SELECT 
            count(*) as total_rows,
            count(DISTINCT symbol) as symbol_count,
            count(DISTINCT period) as period_count
        FROM incremental_state
    """).fetchone()
    print(f"incremental_state 总行数: {inc_stat[0]:,} (预期 58,685)")
    print(f"覆盖标的数: {inc_stat[1]:,}")
    print(f"周期数: {inc_stat[2]} (预期 11)")

    stat_64d = conn.execute("""
        SELECT 
            count(*) as total_64d,
            min(bar_count) as min_bar_count,
            max(bar_count) as max_bar_count,
            count(CASE WHEN sk_closed IS NULL OR isnan(sk_closed) THEN 1 END) as null_sk_closed
        FROM incremental_state
        WHERE period = '64d'
    """).fetchone()
    print(f"64d 状态行数: {stat_64d[0]} (预期 5,335)")
    print(f"64d bar_count 范围: [{stat_64d[1]}, {stat_64d[2]}] (必须在 [0, 63] 内)")
    print(f"64d sk_closed 空值数: {stat_64d[3]}")

    print("\n=== 3. 抽样四只标的在最新交易日的 v_daily_qfq 真实数值 ===")
    samples = ['SZ000001', 'SH600000', 'SH000001', 'TDX880301']
    max_date = conn.execute("SELECT max(trade_date) FROM daily_snapshot").fetchone()[0]
    print(f"最新交易日: {max_date}")

    df_samples = conn.execute(f"""
        SELECT symbol, code, trade_date, 
               round(close_qfq, 2) as close_qfq,
               round(sk_32d, 2) as sk_32d, round(sd_32d, 2) as sd_32d,
               round(sk_64d, 2) as sk_64d, round(sd_64d, 2) as sd_64d,
               round(mk_64d, 2) as mk_64d, round(md_64d, 2) as md_64d
        FROM v_daily_qfq
        WHERE symbol IN {tuple(samples)} AND trade_date = '{max_date}'
        ORDER BY symbol ASC
    """).df()
    print(df_samples.to_string(index=False))

    print("\n=== 4. SZ000001 增量计算 vs 全量历史计算 64d 精度比对 ===")
    df_sz = conn.execute("""
        SELECT symbol, code, trade_date, bar_seq, open, high, low, close, volume, amount, adj_factor
        FROM daily_snapshot
        WHERE symbol = 'SZ000001'
        ORDER BY bar_seq ASC
    """).df()

    # 1) 全量重新计算
    df_full, _ = MultiCycleKDJEngine.compute_daily_snapshot(df_sz)

    # 2) 模拟分段：前 N-30 根算出中间状态，后 30 根以此状态增量计算
    n_total = len(df_sz)
    split_idx = n_total - 30
    df_p1, states_p1 = MultiCycleKDJEngine.compute_daily_snapshot(df_sz.iloc[:split_idx])
    df_p2, states_p2 = MultiCycleKDJEngine.compute_daily_snapshot(df_sz.iloc[split_idx:], init_states=states_p1)
    df_inc = pd.concat([df_p1, df_p2], ignore_index=True)

    cols_64d = [f"{m}_64d" for m in ['sk', 'sd', 'skxl', 'sdxl', 'mk', 'md', 'mkxl', 'mdxl']]
    max_err = 0.0
    for col in cols_64d:
        err = np.max(np.abs(df_full[col].iloc[split_idx:].to_numpy() - df_inc[col].iloc[split_idx:].to_numpy()))
        max_err = max(max_err, err)
        print(f"  {col}: 最大绝对误差 = {err:.2e}")

    print(f"SZ000001 倒数 30 根 64d 总体最大绝对误差: {max_err:.2e}")
    conn.close()


if __name__ == "__main__":
    main()
