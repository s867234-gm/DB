import json

with open("D:/DB/docs/tables_metadata.json", "r", encoding="utf-8") as f:
    data = json.load(f)

with open("D:/DB/docs/base_tables_schema.txt", "w", encoding="utf-8") as out:
    for t in data:
        if t['table_type'] == 'BASE TABLE':
            out.write(f"=== {t['schema']}.{t['table_name']} ({t['row_count']} rows) ===\n")
            for c in t['columns']:
                out.write(f"  {c['column_name']}: {c['data_type']} (nullable={c['is_nullable']})\n")
            out.write("\n")
