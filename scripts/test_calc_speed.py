import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import duckdb
import time
from src.kdj_engine import MultiCycleKDJEngine

src_conn = duckdb.connect(r"D:\database\a_stock_v1.duckdb", read_only=True)
sym = "SZ000002"

df_d = src_conn.execute(f"""
    SELECT 
        '{sym}' AS symbol,
        '000002' AS code,
        d.trade_date,
        d.open, d.high, d.low, d.close, d.volume, d.amount,
        COALESCE(a.factor, 1.0) AS adj_factor
    FROM daily_bar d
    LEFT JOIN adj_factor a ON d.symbol = a.symbol AND d.trade_date = a.trade_date
    WHERE d.symbol = '{sym}'
    ORDER BY d.trade_date ASC
""").df()

t0 = time.time()
df_d_snap, d_states = MultiCycleKDJEngine.compute_daily_snapshot(df_d)
t1 = time.time()
print(f"Daily snapshot computation (6 periods, 8507 rows): {t1 - t0:.4f}s")
print("Daily columns count:", len(df_d_snap.columns))

df_m = src_conn.execute(f"""
    SELECT 
        '{sym}' AS symbol,
        '000002' AS code,
        m.trading_date,
        m.bar_end,
        m.slot_seq,
        m.open, m.high, m.low, m.close, m.volume, m.amount,
        COALESCE(a.factor, 1.0) AS adj_factor
    FROM min15_bar m
    LEFT JOIN adj_factor a ON m.symbol = a.symbol AND m.trading_date = a.trade_date
    WHERE m.symbol = '{sym}'
    ORDER BY m.bar_end ASC
""").df()

t2 = time.time()
df_m_snap, m_states = MultiCycleKDJEngine.compute_minute_snapshot(df_m, cutoff_date='2024-10-01')
t3 = time.time()
print(f"Minute snapshot computation (4 periods, {len(df_m)} raw rows -> {len(df_m_snap)} cut rows): {t3 - t2:.4f}s")
print("Minute columns count:", len(df_m_snap.columns))

src_conn.close()
