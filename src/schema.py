import os
import duckdb
from typing import Optional

DEFAULT_DB_PATH = r"D:\DB\A_DB.duckdb"
SOURCE_DB_PATH = r"D:\database\a_stock_v1.duckdb"

# 宽基指数前缀映射 (旧库 symbol -> 新库 symbol, exchange)
BROAD_INDEX_MAP = {
    '000001': ('SH000001', 'SSE'),
    '000016': ('SH000016', 'SSE'),
    '000300': ('SH000300', 'SSE'),
    '000688': ('SH000688', 'SSE'),
    '000905': ('SH000905', 'SSE'),
    '399001': ('SZ399001', 'SZSE'),
    '399005': ('SZ399005', 'SZSE'),
    '399006': ('SZ399006', 'SZSE'),
}

TABLES_DDL = """
-- 1. 标的基础字典表
CREATE TABLE IF NOT EXISTS security_info (
    symbol VARCHAR PRIMARY KEY,
    code VARCHAR NOT NULL,
    name VARCHAR,
    sec_type VARCHAR NOT NULL, -- 'STOCK', 'BROAD_INDEX', 'INDUSTRY_INDEX'
    exchange VARCHAR NOT NULL, -- 'SSE', 'SZSE', 'TDX'
    board VARCHAR,
    list_date DATE,
    delist_date DATE,
    latest_adj_factor DOUBLE DEFAULT 1.0,
    updated_at TIMESTAMP
);

-- 2. 除权除息事件表
CREATE TABLE IF NOT EXISTS corporate_action (
    symbol VARCHAR NOT NULL,
    event_date DATE NOT NULL,
    category INTEGER NOT NULL,
    cash_dividend DOUBLE,
    bonus_share DOUBLE,
    rights_share DOUBLE,
    rights_price DOUBLE,
    PRIMARY KEY (symbol, event_date, category)
);

-- 3. 日线多周期快照宽表
CREATE TABLE IF NOT EXISTS daily_snapshot (
    symbol VARCHAR NOT NULL,
    code VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    bar_seq INTEGER NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE,
    amount DOUBLE,
    adj_factor DOUBLE NOT NULL,
    -- 1d (8列)
    sk_1d DOUBLE, sd_1d DOUBLE, skxl_1d DOUBLE, sdxl_1d DOUBLE,
    mk_1d DOUBLE, md_1d DOUBLE, mkxl_1d DOUBLE, mdxl_1d DOUBLE,
    -- 2d (8列)
    sk_2d DOUBLE, sd_2d DOUBLE, skxl_2d DOUBLE, sdxl_2d DOUBLE,
    mk_2d DOUBLE, md_2d DOUBLE, mkxl_2d DOUBLE, mdxl_2d DOUBLE,
    -- 4d (8列)
    sk_4d DOUBLE, sd_4d DOUBLE, skxl_4d DOUBLE, sdxl_4d DOUBLE,
    mk_4d DOUBLE, md_4d DOUBLE, mkxl_4d DOUBLE, mdxl_4d DOUBLE,
    -- 8d (8列)
    sk_8d DOUBLE, sd_8d DOUBLE, skxl_8d DOUBLE, sdxl_8d DOUBLE,
    mk_8d DOUBLE, md_8d DOUBLE, mkxl_8d DOUBLE, mdxl_8d DOUBLE,
    -- 16d (8列)
    sk_16d DOUBLE, sd_16d DOUBLE, skxl_16d DOUBLE, sdxl_16d DOUBLE,
    mk_16d DOUBLE, md_16d DOUBLE, mkxl_16d DOUBLE, mdxl_16d DOUBLE,
    -- 32d (8列)
    sk_32d DOUBLE, sd_32d DOUBLE, skxl_32d DOUBLE, sdxl_32d DOUBLE,
    mk_32d DOUBLE, md_32d DOUBLE, mkxl_32d DOUBLE, mdxl_32d DOUBLE,
    PRIMARY KEY (symbol, trade_date)
);

-- 4. 15分钟多周期快照宽表 (仅保留 2024-10-01 起数据)
CREATE TABLE IF NOT EXISTS minute_snapshot (
    symbol VARCHAR NOT NULL,
    code VARCHAR NOT NULL,
    trading_date DATE NOT NULL,
    bar_end TIMESTAMP NOT NULL,
    slot_seq TINYINT NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE,
    amount DOUBLE,
    adj_factor DOUBLE NOT NULL,
    -- 15m (8列)
    sk_15m DOUBLE, sd_15m DOUBLE, skxl_15m DOUBLE, sdxl_15m DOUBLE,
    mk_15m DOUBLE, md_15m DOUBLE, mkxl_15m DOUBLE, mdxl_15m DOUBLE,
    -- 30m (8列)
    sk_30m DOUBLE, sd_30m DOUBLE, skxl_30m DOUBLE, sdxl_30m DOUBLE,
    mk_30m DOUBLE, md_30m DOUBLE, mkxl_30m DOUBLE, mdxl_30m DOUBLE,
    -- 60m (8列)
    sk_60m DOUBLE, sd_60m DOUBLE, skxl_60m DOUBLE, sdxl_60m DOUBLE,
    mk_60m DOUBLE, md_60m DOUBLE, mkxl_60m DOUBLE, mdxl_60m DOUBLE,
    -- 120m (8列)
    sk_120m DOUBLE, sd_120m DOUBLE, skxl_120m DOUBLE, sdxl_120m DOUBLE,
    mk_120m DOUBLE, md_120m DOUBLE, mkxl_120m DOUBLE, mdxl_120m DOUBLE,
    PRIMARY KEY (symbol, bar_end)
);

-- 5. 增量计算状态表
CREATE TABLE IF NOT EXISTS incremental_state (
    symbol VARCHAR NOT NULL,
    period VARCHAR NOT NULL, -- '1d', '2d', '4d', '8d', '16d', '32d', '15m', '30m', '60m', '120m'
    last_closed_bucket_id INTEGER NOT NULL,
    closed_highs_adj VARCHAR, -- JSON Array: 最近已闭合桶复权最高价 (最多104个)
    closed_lows_adj VARCHAR,  -- JSON Array: 最近已闭合桶复权最低价 (最多104个)
    sk_closed DOUBLE NOT NULL,
    sd_closed DOUBLE NOT NULL,
    mk_closed DOUBLE NOT NULL,
    md_closed DOUBLE NOT NULL,
    last_valid_rsv13 DOUBLE NOT NULL,
    last_valid_rsv104 DOUBLE NOT NULL,
    bar_count INTEGER NOT NULL,
    high_adj DOUBLE,
    low_adj DOUBLE,
    updated_at TIMESTAMP,
    PRIMARY KEY (symbol, period)
);
"""

VIEWS_DDL = """
-- 动态日线前复权便捷视图
CREATE OR REPLACE VIEW v_daily_qfq AS
SELECT 
    d.symbol,
    d.code,
    d.trade_date,
    d.bar_seq,
    d.open,
    d.high,
    d.low,
    d.close,
    (d.open * d.adj_factor / s.latest_adj_factor) AS open_qfq,
    (d.high * d.adj_factor / s.latest_adj_factor) AS high_qfq,
    (d.low * d.adj_factor / s.latest_adj_factor) AS low_qfq,
    (d.close * d.adj_factor / s.latest_adj_factor) AS close_qfq,
    d.volume,
    d.amount,
    d.adj_factor,
    d.sk_1d, d.sd_1d, d.skxl_1d, d.sdxl_1d, d.mk_1d, d.md_1d, d.mkxl_1d, d.mdxl_1d,
    d.sk_2d, d.sd_2d, d.skxl_2d, d.sdxl_2d, d.mk_2d, d.md_2d, d.mkxl_2d, d.mdxl_2d,
    d.sk_4d, d.sd_4d, d.skxl_4d, d.sdxl_4d, d.mk_4d, d.md_4d, d.mkxl_4d, d.mdxl_4d,
    d.sk_8d, d.sd_8d, d.skxl_8d, d.sdxl_8d, d.mk_8d, d.md_8d, d.mkxl_8d, d.mdxl_8d,
    d.sk_16d, d.sd_16d, d.skxl_16d, d.sdxl_16d, d.mk_16d, d.md_16d, d.mkxl_16d, d.mdxl_16d,
    d.sk_32d, d.sd_32d, d.skxl_32d, d.sdxl_32d, d.mk_32d, d.md_32d, d.mkxl_32d, d.mdxl_32d
FROM daily_snapshot d
JOIN security_info s ON d.symbol = s.symbol;

-- 动态分钟前复权便捷视图
CREATE OR REPLACE VIEW v_minute_qfq AS
SELECT 
    m.symbol,
    m.code,
    m.trading_date,
    m.bar_end,
    m.slot_seq,
    m.open,
    m.high,
    m.low,
    m.close,
    (m.open * m.adj_factor / s.latest_adj_factor) AS open_qfq,
    (m.high * m.adj_factor / s.latest_adj_factor) AS high_qfq,
    (m.low * m.adj_factor / s.latest_adj_factor) AS low_qfq,
    (m.close * m.adj_factor / s.latest_adj_factor) AS close_qfq,
    m.volume,
    m.amount,
    m.adj_factor,
    m.sk_15m, m.sd_15m, m.skxl_15m, m.sdxl_15m, m.mk_15m, m.md_15m, m.mkxl_15m, m.mdxl_15m,
    m.sk_30m, m.sd_30m, m.skxl_30m, m.sdxl_30m, m.mk_30m, m.md_30m, m.mkxl_30m, m.mdxl_30m,
    m.sk_60m, m.sd_60m, m.skxl_60m, m.sdxl_60m, m.mk_60m, m.md_60m, m.mkxl_60m, m.mdxl_60m,
    m.sk_120m, m.sd_120m, m.skxl_120m, m.sdxl_120m, m.mk_120m, m.md_120m, m.mkxl_120m, m.mdxl_120m
FROM minute_snapshot m
JOIN security_info s ON m.symbol = s.symbol;
"""


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """创建 5 张核心物理表和 2 个动态前复权视图"""
    conn.execute(TABLES_DDL)
    conn.execute(VIEWS_DDL)


def init_security_info(conn: duckdb.DuckDBPyConnection, source_conn: duckdb.DuckDBPyConnection) -> int:
    """
    从源数据库导入 security_info:
    - 沪深 A 股个股 (SSE, SZSE)
    - 8 只宽基指数 (统一转换前缀 SH/SZ)
    - 99 只通达信行业指数 (统一转换前缀 TDX)
    """
    # 1. 导入个股 (从 stock_basic)
    stocks_df = source_conn.execute("""
        SELECT 
            symbol,
            code,
            name,
            'STOCK' AS sec_type,
            exchange,
            board,
            list_date,
            delist_date,
            1.0 AS latest_adj_factor,
            CURRENT_TIMESTAMP AS updated_at
        FROM stock_basic
        WHERE exchange IN ('SSE', 'SZSE')
    """).df()

    # 2. 导入指数 (从 index_basic)
    indices_raw = source_conn.execute("""
        SELECT symbol, index_name, index_category, exchange_or_provider
        FROM index_basic
    """).fetchall()

    indices_rows = []
    for sym, name, cat, exch in indices_raw:
        if cat == 'BROAD':
            new_sym, exchange = BROAD_INDEX_MAP.get(sym, (f"SH{sym}" if sym.startswith('000') else f"SZ{sym}", 'SSE'))
            sec_type = 'BROAD_INDEX'
            board = 'INDEX'
        else:
            # 通达信行业指数 (880xxx)
            new_sym = f"TDX{sym}"
            exchange = 'TDX'
            sec_type = 'INDUSTRY_INDEX'
            board = cat  # 'INDUSTRY_L1' 或 'INDUSTRY_L2'
        
        indices_rows.append({
            'symbol': new_sym,
            'code': sym,
            'name': name,
            'sec_type': sec_type,
            'exchange': exchange,
            'board': board,
            'list_date': None,
            'delist_date': None,
            'latest_adj_factor': 1.0,
            'updated_at': None
        })
    
    import pandas as pd
    indices_df = pd.DataFrame(indices_rows)
    all_sec_df = pd.concat([stocks_df, indices_df], ignore_index=True)

    conn.register("df_all_sec", all_sec_df)
    conn.execute("""
        INSERT OR REPLACE INTO security_info 
        SELECT symbol, code, name, sec_type, exchange, board, list_date, delist_date, latest_adj_factor, CURRENT_TIMESTAMP
        FROM df_all_sec
    """)
    conn.unregister("df_all_sec")

    count = conn.execute("SELECT count(*) FROM security_info").fetchone()[0]
    return count


def init_corporate_action(conn: duckdb.DuckDBPyConnection, source_conn: duckdb.DuckDBPyConnection) -> int:
    """从源数据库导入除权除息记录"""
    corp_df = source_conn.execute("""
        SELECT 
            symbol,
            event_date,
            category,
            cash_dividend,
            bonus_share,
            rights_share,
            rights_price
        FROM corporate_action
    """).df()

    conn.register("df_corp", corp_df)
    conn.execute("""
        INSERT OR REPLACE INTO corporate_action
        SELECT symbol, event_date, category, cash_dividend, bonus_share, rights_share, rights_price
        FROM df_corp
    """)
    conn.unregister("df_corp")

    count = conn.execute("SELECT count(*) FROM corporate_action").fetchone()[0]
    return count


def init_database(db_path: str = DEFAULT_DB_PATH, source_db_path: str = SOURCE_DB_PATH) -> None:
    """一键初始化全新 A_DB 数据库表结构并同步基础字典"""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = duckdb.connect(db_path)
    create_tables(conn)

    source_conn = duckdb.connect(source_db_path, read_only=True)
    sec_count = init_security_info(conn, source_conn)
    corp_count = init_corporate_action(conn, source_conn)

    source_conn.close()
    conn.close()
    print(f"Database initialized at {db_path}: {sec_count} securities, {corp_count} corporate actions.")


if __name__ == '__main__':
    init_database()
