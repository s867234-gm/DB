import duckdb
import sys
import json

db_path = r"D:\database\a_stock_v1.duckdb"
print(f"Opening {db_path} in read_only mode...")

conn = duckdb.connect(db_path, read_only=True)

# 1. 查询所有表和视图
tables_info = conn.execute("""
    SELECT table_schema, table_name, table_type 
    FROM information_schema.tables 
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_type, table_name
""").fetchall()

print(f"Total tables/views found: {len(tables_info)}")
for schema, name, ttype in tables_info:
    print(f"[{ttype}] {schema}.{name}")

# 查询每张表的字段信息和行数
for schema, name, ttype in tables_info:
    full_name = f'"{schema}"."{name}"' if schema != 'main' else f'"{name}"'
    columns = conn.execute(f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{name}'
        ORDER BY ordinal_position
    """).fetchall()
    
    count = conn.execute(f"SELECT count(*) FROM {full_name}").fetchone()[0]
    print(f"\n--- {ttype}: {name} (Rows: {count:,}) ---")
    for col in columns:
        print(f"  {col[0]}: {col[1]} (nullable: {col[2]})")

conn.close()
