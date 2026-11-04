import duckdb

con = duckdb.connect(r'D:\database\a_stock_v1.duckdb', read_only=True)

print("=== stock_basic exchanges ===")
for r in con.execute('SELECT exchange, count(*) FROM stock_basic GROUP BY exchange').fetchall():
    print(r)

print("\n=== index_basic categories ===")
for r in con.execute('SELECT index_category, count(*) FROM index_basic GROUP BY index_category').fetchall():
    print(r)

print("\n=== BROAD indexes ===")
for r in con.execute("SELECT symbol, index_name, exchange_or_provider FROM index_basic WHERE index_category='BROAD'").fetchall():
    print(r)

print("\n=== INDUSTRY indexes sample ===")
for r in con.execute("SELECT symbol, index_name, exchange_or_provider FROM index_basic WHERE index_category != 'BROAD' LIMIT 10").fetchall():
    print(r)

con.close()
