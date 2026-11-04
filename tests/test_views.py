import datetime
import duckdb
import pytest
import pandas as pd
from src.schema import TABLES_DDL, VIEWS_DDL
from src.updater import DailyUpdater


def create_mock_db():
    con = duckdb.connect(":memory:")
    con.execute(TABLES_DDL)
    con.execute(VIEWS_DDL)
    return con


def test_views_qfq_calculation():
    con = create_mock_db()

    # 插入证券基础信息：最新复权因子为 2.5
    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, latest_adj_factor)
        VALUES ('SH600000', '600000', '浦发银行', 'STOCK', 'SSE', 2.5)
    """)

    # 插入日线数据：历史某日复权因子为 1.25，收盘价 10.0
    con.execute("""
        INSERT INTO daily_snapshot (
            symbol, code, trade_date, bar_seq, open, high, low, close, volume, amount, adj_factor,
            sk_1d, sd_1d, skxl_1d, sdxl_1d, mk_1d, md_1d, mkxl_1d, mdxl_1d
        ) VALUES (
            'SH600000', '600000', '2024-01-02', 1, 9.0, 11.0, 8.5, 10.0, 10000, 100000, 1.25,
            50.0, 50.0, 0.0, 0.0, 50.0, 50.0, 0.0, 0.0
        )
    """)

    # 插入分钟线数据：历史某时复权因子为 1.25，收盘价 10.0
    con.execute("""
        INSERT INTO minute_snapshot (
            symbol, code, trading_date, bar_end, slot_seq, open, high, low, close, volume, amount, adj_factor,
            sk_15m, sd_15m, skxl_15m, sdxl_15m, mk_15m, md_15m, mkxl_15m, mdxl_15m
        ) VALUES (
            'SH600000', '600000', '2024-01-02', '2024-01-02 09:45:00', 1, 9.0, 11.0, 8.5, 10.0, 1000, 10000, 1.25,
            50.0, 50.0, 0.0, 0.0, 50.0, 50.0, 0.0, 0.0
        )
    """)

    # 验证 v_daily_qfq
    df_d = con.execute("SELECT * FROM v_daily_qfq WHERE symbol = 'SH600000'").df()
    assert len(df_d) == 1
    # 前复权价格 = 真实价格 * 当日adj_factor / 最新adj_factor
    # close_qfq = 10.0 * 1.25 / 2.5 = 5.0
    # open_qfq = 9.0 * 1.25 / 2.5 = 4.5
    assert pytest.approx(df_d.iloc[0]["close_qfq"], 1e-4) == 5.0
    assert pytest.approx(df_d.iloc[0]["open_qfq"], 1e-4) == 4.5
    assert pytest.approx(df_d.iloc[0]["high_qfq"], 1e-4) == 5.5
    assert pytest.approx(df_d.iloc[0]["low_qfq"], 1e-4) == 4.25

    # 验证 v_minute_qfq
    df_m = con.execute("SELECT * FROM v_minute_qfq WHERE symbol = 'SH600000'").df()
    assert len(df_m) == 1
    assert pytest.approx(df_m.iloc[0]["close_qfq"], 1e-4) == 5.0
    assert pytest.approx(df_m.iloc[0]["open_qfq"], 1e-4) == 4.5
    con.close()


def test_resonance_query_execution():
    con = create_mock_db()

    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, latest_adj_factor)
        VALUES 
            ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', 1.0),
            ('SZ000002', '000002', '万科A', 'STOCK', 'SZSE', 1.0)
    """)

    # 构造 SZ000001 满足金叉与强势条件 (sk_1d > sd_1d, sk_2d > sd_2d)
    con.execute("""
        INSERT INTO daily_snapshot (
            symbol, code, trade_date, bar_seq, open, high, low, close, volume, amount, adj_factor,
            sk_1d, sd_1d, skxl_1d, sdxl_1d, mk_1d, md_1d, mkxl_1d, mdxl_1d,
            sk_2d, sd_2d, skxl_2d, sdxl_2d, mk_2d, md_2d, mkxl_2d, mdxl_2d
        ) VALUES 
            ('SZ000001', '000001', '2026-11-04', 100, 10.0, 11.0, 9.8, 10.5, 10000, 100000, 1.0,
             65.0, 55.0, 1.5, 0.8, 60.0, 50.0, 1.0, 0.5,
             70.0, 60.0, 1.2, 0.6, 68.0, 58.0, 1.1, 0.5),
            ('SZ000002', '000002', '2026-11-04', 100, 5.0, 5.2, 4.8, 4.9, 10000, 100000, 1.0,
             40.0, 55.0, -1.0, 0.2, 45.0, 50.0, -0.5, 0.1,
             42.0, 60.0, -0.8, 0.3, 40.0, 58.0, -0.7, 0.2)
    """)

    # 执行多周期共振查询 SQL
    query_sql = """
        SELECT 
            symbol, code, trade_date, close,
            sk_1d, sd_1d, sk_2d, sd_2d
        FROM v_daily_qfq
        WHERE trade_date = '2026-11-04'
          AND sk_1d > sd_1d
          AND sk_2d > sd_2d
    """
    res = con.execute(query_sql).df()
    assert len(res) == 1
    assert res.iloc[0]["symbol"] == "SZ000001"
    con.close()


def test_delisted_filter_in_updater(tmp_path):
    db_file = str(tmp_path / "test_delist.duckdb")
    con = duckdb.connect(db_file)
    con.execute(TABLES_DDL)
    con.execute(VIEWS_DDL)

    # 插入已退市标的 SZ002898 (退市日 2026-07-16) 与正常标的 SZ000001
    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, delist_date, latest_adj_factor)
        VALUES 
            ('SZ002898', '002898', '赛隆药业', 'STOCK', 'SZSE', '2026-07-16', 1.0),
            ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', NULL, 1.0)
    """)

    # SZ002898 日线数据已收录到 2026-07-16（退市日）
    con.execute("""
        INSERT INTO daily_snapshot (
            symbol, code, trade_date, bar_seq, open, high, low, close, volume, amount, adj_factor
        ) VALUES 
            ('SZ002898', '002898', '2026-07-16', 1500, 8.0, 8.1, 7.9, 8.0, 1000, 8000, 1.0),
            ('SZ000001', '000001', '2026-10-30', 2000, 10.0, 10.2, 9.9, 10.1, 2000, 20000, 1.0)
    """)
    con.close()

    updater = DailyUpdater(db_path=db_file)
    plan = updater.get_update_plan(target_date=datetime.date(2026, 11, 4))

    # 计划中应仅有正常标的 SZ000001，而已退市且数据完整的 SZ002898 应被排除
    plan_symbols = [t["symbol"] for t in plan]
    assert "SZ000001" in plan_symbols
    assert "SZ002898" not in plan_symbols
