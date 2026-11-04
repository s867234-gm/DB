import duckdb

con = duckdb.connect(r'D:\database\a_stock_v1.duckdb', read_only=True)
print("corporate_action symbol samples:")
for r in con.execute('SELECT DISTINCT symbol FROM corporate_action LIMIT 10').fetchall():
    print(r)
print("Total rows:", con.execute('SELECT count(*) FROM corporate_action').fetchone()[0])
con.close()
