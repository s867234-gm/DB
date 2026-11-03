import duckdb
import pandas as pd

con = duckdb.connect(r"D:\database\a_stock_v1.duckdb", read_only=True)

# 抽样某只典型股票（例如 SZ000001 平安银行 / SZ000002 万科A）看 adj_factor 走势
sample_symbol = "SZ000002"
af_head = con.execute(f"SELECT symbol, trade_date, factor FROM adj_factor WHERE symbol='{sample_symbol}' ORDER BY trade_date ASC LIMIT 5").df()
af_tail = con.execute(f"SELECT symbol, trade_date, factor FROM adj_factor WHERE symbol='{sample_symbol}' ORDER BY trade_date DESC LIMIT 5").df()
print("SZ000002 adj_factor head:")
print(af_head)
print("\nSZ000002 adj_factor tail:")
print(af_tail)

# 查看 daily_bar 对应时间的价格
p_head = con.execute(f"SELECT symbol, trade_date, open, high, low, close, volume FROM daily_bar WHERE symbol='{sample_symbol}' ORDER BY trade_date ASC LIMIT 5").df()
p_tail = con.execute(f"SELECT symbol, trade_date, open, high, low, close, volume FROM daily_bar WHERE symbol='{sample_symbol}' ORDER BY trade_date DESC LIMIT 5").df()
print("\nSZ000002 daily_bar head:")
print(p_head)
print("\nSZ000002 daily_bar tail:")
print(p_tail)

# 查看 stock_daily_feature_snapshot 对应的前复权价格
snap_head = con.execute(f"SELECT symbol, trade_date, d1_open_qfq, d1_close_qfq FROM stock_daily_feature_snapshot WHERE symbol='{sample_symbol}' ORDER BY trade_date ASC LIMIT 5").df()
snap_tail = con.execute(f"SELECT symbol, trade_date, d1_open_qfq, d1_close_qfq FROM stock_daily_feature_snapshot WHERE symbol='{sample_symbol}' ORDER BY trade_date DESC LIMIT 5").df()
print("\nSZ000002 snap_head:")
print(snap_head)
print("\nSZ000002 snap_tail:")
print(snap_tail)

# 查看 data_source 和 data_version 的具体数据
ds = con.execute("SELECT * FROM data_source").df()
print("\ndata_source:")
print(ds)

dv = con.execute("SELECT * FROM data_version ORDER BY created_at DESC LIMIT 5").df()
print("\ndata_version (latest 5):")
print(dv)

con.close()
