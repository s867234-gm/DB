import json

with open("D:/DB/docs/inspection_details.json", "r", encoding="utf-8") as f:
    data = json.load(f)

print("=== 标的统计 ===")
print("股票数量:", data['stock_count'])
print("指数数量:", data['index_count'])
print("股票代码示例 (symbol):", data['stock_symbol_samples'])
print("股票代码示例 (code):", data['stock_code_samples'])
print("指数代码示例 (symbol):", data['index_symbol_samples'])
print("指数类别分布:", data['index_categories'])
print("通达信 880xxx 在 index_basic 中的记录:", data['tdx_880_in_index'])
print("通达信 880xxx 在 daily_bar 中的不同代码数:", data['tdx_880_in_daily_count'])
print("中小板指 399005 在 index_basic:", data['idx_399005_in_index'])
print("中小板指 399005 在 daily_bar 的行数:", data['idx_399005_in_daily_count'])

print("\n=== 日线数据 (daily_bar) ===")
print(data['daily_bar_stats'])

print("\n=== 15分钟线数据 (min15_bar) ===")
print(data['min15_bar_stats'])

print("\n=== 复权因子 (adj_factor) ===")
print(data['adj_factor_stats'])

print("\n=== 除权事件 (corporate_action) ===")
print(data['corporate_action_stats'])
print("除权类别统计:", data['corporate_action_categories'])

print("\n=== 复权视图 SQL (v_day_qfq) ===")
print(data['views_def'].get('v_day_qfq', 'NOT FOUND'))
