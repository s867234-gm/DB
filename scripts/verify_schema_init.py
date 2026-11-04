import duckdb

conn = duckdb.connect(r'D:\DB\A_DB.duckdb', read_only=True)
tables = conn.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema='main' ORDER BY table_type, table_name").fetchall()
for t, typ in tables:
    cnt = conn.execute(f'SELECT count(*) FROM {t}').fetchone()[0] if typ=='BASE TABLE' else 'VIEW'
    print(f"{typ}: {t} -> {cnt}")

print("\nSecurity type distribution:")
for r in conn.execute('SELECT sec_type, exchange, count(*) FROM security_info GROUP BY sec_type, exchange ORDER BY sec_type, exchange').fetchall():
    print(r)

print("\nSamples from security_info:")
for r in conn.execute("SELECT symbol, code, name, sec_type, exchange, latest_adj_factor FROM security_info WHERE symbol IN ('SH600000', 'SZ000002', 'SH000001', 'TDX880301', 'SZ399005')").fetchall():
    print(r)

conn.close()
