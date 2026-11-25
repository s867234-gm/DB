# -*- coding: utf-8 -*-
"""
scripts/verify_128d.py —— A_DB 日线 128d 周期存量回填与计算精度核验脚本
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
from src.schema import DEFAULT_DB_PATH


def main(db_path: str = DEFAULT_DB_PATH):
    conn = duckdb.connect(db_path, read_only=True)
    try:
        print("=== 1. daily_snapshot 统计与 128d 空值核验 ===")
        d_stat = conn.execute("""
            SELECT 
                count(*) as total_rows,
                count(DISTINCT symbol) as symbol_count,
                count(CASE WHEN sk_128d IS NULL OR isnan(sk_128d) THEN 1 END) as null_sk_128d,
                count(CASE WHEN sd_128d IS NULL OR isnan(sd_128d) THEN 1 END) as null_sd_128d,
                count(CASE WHEN mk_128d IS NULL OR isnan(mk_128d) THEN 1 END) as null_mk_128d,
                count(CASE WHEN md_128d IS NULL OR isnan(md_128d) THEN 1 END) as null_md_128d
            FROM daily_snapshot
        """).fetchone()
        print(f"daily_snapshot 总行数: {d_stat[0]:,}")
        print(f"覆盖标的数: {d_stat[1]:,}")
        print(f"sk_128d 空值/NaN数: {d_stat[2]}")
        print(f"sd_128d 空值/NaN数: {d_stat[3]}")
        print(f"mk_128d 空值/NaN数: {d_stat[4]}")
        print(f"md_128d 空值/NaN数: {d_stat[5]}")
        assert d_stat[2] == 0 and d_stat[3] == 0 and d_stat[4] == 0 and d_stat[5] == 0

        print("\n=== 2. incremental_state 状态表核验 ===")
        inc_stat = conn.execute("""
            SELECT 
                count(*) as total_rows,
                count(DISTINCT symbol) as symbol_count,
                count(DISTINCT period) as period_count
            FROM incremental_state
        """).fetchone()
        print(f"incremental_state 总行数: {inc_stat[0]:,} (预期 64,020)")
        print(f"覆盖标的数: {inc_stat[1]:,}")
        print(f"周期数: {inc_stat[2]} (预期 12)")
        assert inc_stat[0] == 64020 and inc_stat[2] == 12

        stat_128d = conn.execute("""
            SELECT 
                count(*) as total_128d,
                min(bar_count) as min_bar_count,
                max(bar_count) as max_bar_count,
                count(CASE WHEN sk_closed IS NULL OR isnan(sk_closed) THEN 1 END) as null_sk_closed
            FROM incremental_state
            WHERE period = '128d'
        """).fetchone()
        print(f"128d 状态行数: {stat_128d[0]} (预期 5,335)")
        print(f"128d bar_count 范围: [{stat_128d[1]}, {stat_128d[2]}] (必须在 [0, 127] 内)")
        print(f"128d sk_closed 空值数: {stat_128d[3]}")
        assert stat_128d[0] == 5335
        assert 0 <= stat_128d[1] and stat_128d[2] <= 127
        assert stat_128d[3] == 0

        print("\n=== 3. 抽样四只标的在最新交易日的 v_daily_qfq 真实数值 ===")
        samples = ('SZ000001', 'SH600000', 'SH000001', 'TDX880301')
        max_date = conn.execute("SELECT max(trade_date) FROM daily_snapshot").fetchone()[0]
        print(f"最新交易日: {max_date}")

        df_samples = conn.execute(f"""
            SELECT symbol, code, trade_date,
                   round(close_qfq, 2) as close_qfq,
                   round(sk_64d, 2) as sk_64d, round(sd_64d, 2) as sd_64d,
                   round(sk_128d, 2) as sk_128d, round(sd_128d, 2) as sd_128d,
                   round(mk_128d, 2) as mk_128d, round(md_128d, 2) as md_128d
            FROM v_daily_qfq
            WHERE symbol IN {samples} AND trade_date = '{max_date}'
            ORDER BY symbol ASC
        """).df()
        print(df_samples.to_string(index=False))

        print("\n=== 4. SZ000001 增量计算 vs 全量历史计算 128d 精度比对 ===")
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
        df_p2, _ = MultiCycleKDJEngine.compute_daily_snapshot(df_sz.iloc[split_idx:], init_states=states_p1)
        df_inc = pd.concat([df_p1, df_p2], ignore_index=True)

        cols_128d = [f"{m}_128d" for m in ['sk', 'sd', 'skxl', 'sdxl', 'mk', 'md', 'mkxl', 'mdxl']]
        max_err = 0.0
        for col in cols_128d:
            err = float(np.max(np.abs(df_full[col].iloc[split_idx:].to_numpy() - df_inc[col].iloc[split_idx:].to_numpy())))
            max_err = max(max_err, err)
            print(f"  {col}: 最大绝对误差 = {err:.2e}")

        print(f"SZ000001 倒数 30 根 128d 总体最大绝对误差: {max_err:.2e}")
        assert max_err < 1e-12, f"Max error {max_err} >= 1e-12!"
    finally:
        conn.close()


if __name__ == "__main__":
    main()
