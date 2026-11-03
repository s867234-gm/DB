import duckdb
import pandas as pd
import json

db_path = r"D:\database\a_stock_v1.duckdb"
con = duckdb.connect(db_path, read_only=True)

# 1. 表与视图清单
tables_df = con.execute("""
    SELECT table_schema, table_name, table_type 
    FROM information_schema.tables 
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_type, table_schema, table_name
""").df()

tables = []
for idx, row in tables_df.iterrows():
    schema = row['table_schema']
    tname = row['table_name']
    ttype = row['table_type']
    
    # 字段信息
    cols_df = con.execute(f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{tname}'
        ORDER BY ordinal_position
    """).df()
    
    cols = cols_df.to_dict(orient='records')
    
    row_count = None
    if ttype == 'BASE TABLE':
        try:
            row_count = con.execute(f'SELECT count(*) FROM "{schema}"."{tname}"').fetchone()[0]
        except Exception as e:
            row_count = f"Error: {e}"
            
    tables.append({
        'schema': schema,
        'table_name': tname,
        'table_type': ttype,
        'row_count': row_count,
        'columns': cols
    })

with open(r"D:\DB\docs\tables_metadata.json", "w", encoding="utf-8") as f:
    json.dump(tables, f, ensure_ascii=False, indent=2)

print("Tables metadata extracted.")
con.close()
