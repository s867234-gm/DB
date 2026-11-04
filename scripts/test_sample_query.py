import duckdb
import time

src_conn = duckdb.connect(r"D:\database\a_stock_v1.duckdb", read_only=True)

sym = "SZ000002"
t0 = time.time()
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
t1 = time.time()
print(f"Daily query took {t1 - t0:.3f}s, rows: {len(df_d)}")

t0 = time.time()
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
print(f"Minute query took {t2 - t1:.3f}s, rows: {len(df_m)}")

src_conn.close()
