import duckdb
import pandas as pd
import json

db_path = r"D:\database\a_stock_v1.duckdb"
con = duckdb.connect(db_path, read_only=True)

report = {}

# 1. 标的统计 (stock_basic, index_basic)
report['stock_count'] = con.execute("SELECT count(*) FROM stock_basic").fetchone()[0]
report['index_count'] = con.execute("SELECT count(*) FROM index_basic").fetchone()[0]

report['stock_symbol_samples'] = [r[0] for r in con.execute("SELECT symbol FROM stock_basic LIMIT 10").fetchall()]
report['stock_code_samples'] = [r[0] for r in con.execute("SELECT code FROM stock_basic LIMIT 10").fetchall()]
report['index_symbol_samples'] = [r[0] for r in con.execute("SELECT symbol FROM index_basic LIMIT 10").fetchall()]

# 检查 index_basic 的 index_category
index_categories = con.execute("SELECT index_category, count(*) FROM index_basic GROUP BY index_category").fetchall()
report['index_categories'] = [dict(category=r[0], count=r[1]) for r in index_categories]

# 检查通达信 880xxx 板块指数
tdx_880_in_index = con.execute("SELECT symbol, index_name, index_category FROM index_basic WHERE symbol LIKE '%880%' OR symbol LIKE '880%'").fetchall()
report['tdx_880_in_index'] = [dict(symbol=r[0], name=r[1], category=r[2]) for r in tdx_880_in_index]

tdx_880_in_daily_count = con.execute("SELECT count(DISTINCT symbol) FROM daily_bar WHERE symbol LIKE '%880%' OR symbol LIKE '880%'").fetchone()[0]
report['tdx_880_in_daily_count'] = tdx_880_in_daily_count

# 检查中小板指 399005
idx_399005_in_index = con.execute("SELECT symbol, index_name, index_category FROM index_basic WHERE symbol LIKE '%399005%'").fetchall()
report['idx_399005_in_index'] = [dict(symbol=r[0], name=r[1], category=r[2]) for r in idx_399005_in_index]
report['idx_399005_in_daily_count'] = con.execute("SELECT count(*) FROM daily_bar WHERE symbol LIKE '%399005%'").fetchone()[0]

# 检查 index_basic 中所有指数的完整列表
all_indices = con.execute("SELECT symbol, index_name, index_category, exchange_or_provider FROM index_basic ORDER BY symbol").df().to_dict(orient='records')
report['all_indices'] = all_indices

# 2. 日线与 15 分钟线时间范围与覆盖
daily_stats = con.execute("""
    SELECT 
        min(trade_date) as min_date, 
        max(trade_date) as max_date, 
        count(distinct trade_date) as trade_days,
        count(distinct symbol) as symbol_count,
        count(*) as total_rows
    FROM daily_bar
""").fetchone()

report['daily_bar_stats'] = {
    'min_date': str(daily_stats[0]),
    'max_date': str(daily_stats[1]),
    'trade_days': daily_stats[2],
    'symbol_count': daily_stats[3],
    'total_rows': daily_stats[4]
}

min15_stats = con.execute("""
    SELECT 
        min(trading_date) as min_trading_date,
        max(trading_date) as max_trading_date,
        min(bar_end) as min_bar_end, 
        max(bar_end) as max_bar_end, 
        count(distinct symbol) as symbol_count,
        count(*) as total_rows
    FROM min15_bar
""").fetchone()

report['min15_bar_stats'] = {
    'min_trading_date': str(min15_stats[0]),
    'max_trading_date': str(min15_stats[1]),
    'min_bar_end': str(min15_stats[2]),
    'max_bar_end': str(min15_stats[3]),
    'symbol_count': min15_stats[4],
    'total_rows': min15_stats[5]
}

# 3. 复权因子 (adj_factor & corporate_action)
adj_stats = con.execute("""
    SELECT 
        min(trade_date) as min_date,
        max(trade_date) as max_date,
        count(distinct symbol) as symbol_count,
        count(*) as total_rows
    FROM adj_factor
""").fetchone()

report['adj_factor_stats'] = {
    'min_date': str(adj_stats[0]),
    'max_date': str(adj_stats[1]),
    'symbol_count': adj_stats[2],
    'total_rows': adj_stats[3]
}

corp_stats = con.execute("""
    SELECT 
        min(event_date) as min_date,
        max(event_date) as max_date,
        count(distinct symbol) as symbol_count,
        count(*) as total_rows
    FROM corporate_action
""").fetchone()

report['corporate_action_stats'] = {
    'min_date': str(corp_stats[0]),
    'max_date': str(corp_stats[1]),
    'symbol_count': corp_stats[2],
    'total_rows': corp_stats[3]
}

# 查看 corporate_action 分类含义
report['corporate_action_categories'] = con.execute("""
    SELECT category, count(*) as cnt FROM corporate_action GROUP BY category ORDER BY category
""").df().to_dict(orient='records')

# 查看视图中如何复权 (检查 v_day_qfq 或类似视图的 SQL 定义)
views_def = con.execute("""
    SELECT view_name, sql 
    FROM duckdb_views() 
    WHERE view_name IN ('v_day_qfq', 'fac_day', 'v_min15_qfq_current', 'v_min15_qfq_done')
""").fetchall()
report['views_def'] = {v[0]: v[1] for v in views_def}

# 4. 核心表采样 (前5行和后5行)
core_tables_config = [
    ('daily_bar', 'trade_date'),
    ('min15_bar', 'bar_end'),
    ('stock_basic', 'symbol'),
    ('index_basic', 'symbol'),
    ('adj_factor', 'trade_date'),
    ('corporate_action', 'event_date'),
    ('trading_calendar', 'trade_date'),
    ('turn1_daily', 'trade_date'),
    ('stock_daily_feature_snapshot', 'trade_date'),
    ('stock_high_period_structure_snapshot', 'trade_date'),
    ('stock_intraday_feature_snapshot', 'bar_end'),
    ('stock_pivot_event', 'occurrence_date'),
    ('stock_pivot_state', 'updated_at'),
    ('data_version', 'created_at'),
    ('update_log', 'started_at')
]

samples = {}
for ct, sort_col in core_tables_config:
    try:
        head_df = con.execute(f'SELECT * FROM "{ct}" ORDER BY {sort_col} ASC LIMIT 5').df()
        tail_df = con.execute(f'SELECT * FROM "{ct}" ORDER BY {sort_col} DESC LIMIT 5').df()
        samples[ct] = {
            'sort_by': sort_col,
            'head_5': head_df.to_dict(orient='records'),
            'tail_5': tail_df.to_dict(orient='records')
        }
    except Exception as e:
        samples[ct] = f"Error: {e}"

report['core_tables_samples'] = samples

with open(r"D:\DB\docs\inspection_details.json", "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=2, default=str)

print("Full inspection details saved successfully!")
con.close()
