import duckdb

con = duckdb.connect(r'D:\database\a_stock_v1.duckdb', read_only=True)

samples = [
    ('SH600000', 'SH600000'),
    ('SZ000002', 'SZ000002'),
    ('SH000001', '000001'),
    ('TDX880301', '880301')
]

for new_sym, old_sym in samples:
    d_cnt = con.execute(f"SELECT count(*), min(trade_date), max(trade_date) FROM daily_bar WHERE symbol='{old_sym}'").fetchone()
    m_cnt = con.execute(f"SELECT count(*), min(trading_date), max(trading_date) FROM min15_bar WHERE symbol='{old_sym}'").fetchone()
    a_cnt = con.execute(f"SELECT count(*), min(trade_date), max(trade_date), min(factor), max(factor) FROM adj_factor WHERE symbol='{old_sym}'").fetchone()
    print(f"[{new_sym} (old: {old_sym})]")
    print(f"  daily_bar: count={d_cnt[0]}, min={d_cnt[1]}, max={d_cnt[2]}")
    print(f"  min15_bar: count={m_cnt[0]}, min={m_cnt[1]}, max={m_cnt[2]}")
    print(f"  adj_factor: count={a_cnt[0]}, min={a_cnt[1]}, max={a_cnt[2]}, min_factor={a_cnt[3]}, max_factor={a_cnt[4]}")

con.close()
