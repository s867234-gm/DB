# -*- coding: utf-8 -*-
"""
tests/test_updater.py —— 增量更新引擎单元测试与幂等性校验
"""

import datetime
import time
from unittest.mock import MagicMock

import duckdb
import numpy as np
import pandas as pd
import pytest

from src.fetcher import (
    TdxConnectionPool,
    TdxFetcher,
    calculate_next_adj_factor,
    extract_liutongguben,
    extract_liutongguben_from_xdxr,
    parse_daily_bars_to_df,
    parse_minute_bars_to_df,
)
from src.kdj_engine import (
    DAILY_PERIOD_NAMES,
    DAILY_SNAPSHOT_COLUMNS,
    MINUTE_PERIOD_NAMES,
    MINUTE_SNAPSHOT_COLUMNS,
    MultiCycleKDJEngine,
    PeriodState,
    restore_state_from_db,
    states_to_df,
)
from src.schema import create_tables
from src.updater import DailyUpdater


def test_calculate_next_adj_factor():
    """测试除权除息等比复权因子推进计算"""
    prev_close = 10.0
    prev_factor = 1.5

    # 1. 无事件
    assert calculate_next_adj_factor(prev_factor, prev_close, None) == 1.5
    assert calculate_next_adj_factor(prev_factor, prev_close, {}) == 1.5

    # 2. 纯分红派息: 每10股派2元 (每股0.2元)
    event_div = {"fenhong": 2.0, "songzhuangu": 0.0, "peigu": 0.0, "peigujia": 0.0}
    # 除权参考价: (10.0 - 0.2) / 1.0 = 9.8
    # ratio = 10.0 / 9.8 = 1.020408163
    new_factor = calculate_next_adj_factor(prev_factor, prev_close, event_div)
    expected = 1.5 * (10.0 / 9.8)
    assert abs(new_factor - expected) < 1e-6
    assert new_factor > prev_factor

    # 3. 送转股: 每10股送转2股 (s=0.2)
    event_bonus = {"fenhong": 0.0, "songzhuangu": 2.0, "peigu": 0.0, "peigujia": 0.0}
    # 除权参考价: 10.0 / (1 + 0.2) = 10.0 / 1.2 = 8.333333
    # ratio = 10.0 / 8.333333 = 1.2
    new_factor_bonus = calculate_next_adj_factor(prev_factor, prev_close, event_bonus)
    assert abs(new_factor_bonus - 1.5 * 1.2) < 1e-6


def test_parse_and_filter_bars():
    """测试日线与 15 分钟线数据解析及槽位对齐"""
    raw_d = [
        {"datetime": "2026-10-30", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "vol": 1000, "amount": 10200},
        {"datetime": "2026-11-02", "open": 10.3, "high": 10.8, "low": 10.1, "close": 10.6, "vol": 1200, "amount": 12720},
    ]

    # 不带过滤
    df_d = parse_daily_bars_to_df("SZ000001", raw_d)
    assert len(df_d) == 2

    # 带 min_trade_date 过滤
    df_d_filtered = parse_daily_bars_to_df("SZ000001", raw_d, min_trade_date=datetime.date(2026, 10, 30))
    assert len(df_d_filtered) == 1
    assert df_d_filtered.iloc[0]["trade_date"] == datetime.date(2026, 11, 2)

    # 15分钟线槽位解析
    raw_m = [
        {"datetime": "2026-11-02 09:45", "open": 10.0, "high": 10.2, "low": 9.9, "close": 10.1, "vol": 100, "amount": 1010},
        {"datetime": "2026-11-02 10:00", "open": 10.1, "high": 10.3, "low": 10.0, "close": 10.2, "vol": 100, "amount": 1020},
        {"datetime": "2026-11-02 15:00", "open": 10.5, "high": 10.6, "low": 10.4, "close": 10.6, "vol": 200, "amount": 2120},
    ]
    df_m = parse_minute_bars_to_df("SZ000001", raw_m)
    assert len(df_m) == 3
    assert df_m.iloc[0]["slot_seq"] == 1
    assert df_m.iloc[1]["slot_seq"] == 2
    assert df_m.iloc[2]["slot_seq"] == 16
    assert df_m.iloc[2]["bar_end"] == datetime.datetime(2026, 11, 2, 15, 0)

    # 次正规浮点成交量噪点应被归零（日线/分钟线）
    raw_d_sub = [
        {"datetime": "2026-11-03", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "vol": 5.877471754111438e-39, "amount": 5.877471754111438e-39},
    ]
    df_d_sub = parse_daily_bars_to_df("SZ000001", raw_d_sub)
    assert df_d_sub.iloc[0]["volume"] == 0.0

    raw_m_sub = [
        {"datetime": "2026-11-03 09:45", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "vol": 5.877471754111438e-39, "amount": 5.877471754111438e-39},
    ]
    df_m_sub = parse_minute_bars_to_df("SZ000001", raw_m_sub)
    assert df_m_sub.iloc[0]["volume"] == 0.0


def test_updater_idempotence(tmp_path):
    """测试增量更新的幂等性：连续两次更新同一批数据，行数零增加且状态保持一致"""
    db_file = str(tmp_path / "test_idempotent.duckdb")
    con = duckdb.connect(db_file)
    create_tables(con)
    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, board, latest_adj_factor)
        VALUES ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', 'MAIN', 1.0)
    """)

    # 初始 10 根日线
    dates = pd.date_range("2026-10-01", periods=10, freq="B").date
    initial_daily_rows = []
    for i, d in enumerate(dates):
        initial_daily_rows.append({
            "symbol": "SZ000001",
            "code": "000001",
            "trade_date": d,
            "bar_seq": i,
            "open": 10.0 + i * 0.1,
            "high": 10.5 + i * 0.1,
            "low": 9.5 + i * 0.1,
            "close": 10.2 + i * 0.1,
            "volume": 10000.0,
            "amount": 102000.0,
            "adj_factor": 1.0,
        })
    df_init_d = pd.DataFrame(initial_daily_rows)
    df_init_d_snap, d_states = MultiCycleKDJEngine.compute_daily_snapshot(df_init_d)

    con.register("tmp_init_d", df_init_d_snap)
    con.execute("INSERT INTO daily_snapshot BY NAME SELECT * FROM tmp_init_d")

    # 保存初始 incremental_state
    df_init_states = states_to_df(list(d_states.values()))
    con.register("tmp_init_s", df_init_states)
    con.execute("INSERT INTO incremental_state BY NAME SELECT * FROM tmp_init_s")
    con.close()

    # 2. 模拟 Fetcher 返回 2 根新增日线 (2026-10-15, 2026-10-16)
    target_date = datetime.date(2026, 10, 16)
    mock_fetcher = MagicMock()
    mock_fetcher.get_latest_market_status.return_value = (target_date, True)
    mock_fetcher.fetch_xdxr_events.return_value = []
    mock_fetcher.fetch_minute_bars.return_value = []

    mock_daily_bars = [
        {"datetime": "2026-10-14", "open": 11.0, "high": 11.2, "low": 10.8, "close": 11.1, "vol": 1000, "amount": 11100},
        {"datetime": "2026-10-15", "open": 11.2, "high": 11.5, "low": 11.0, "close": 11.4, "vol": 1200, "amount": 13680},
        {"datetime": "2026-10-16", "open": 11.5, "high": 11.8, "low": 11.3, "close": 11.7, "vol": 1500, "amount": 17550},
    ]
    mock_fetcher.fetch_daily_bars.return_value = mock_daily_bars

    updater = DailyUpdater(db_path=db_file, fetcher=mock_fetcher)

    # 第一次运行增量更新
    res1 = updater.run_update(allow_intraday=False)
    assert res1["status"] == "SUCCESS"
    assert res1["symbols_updated"] == 1
    assert res1["daily_rows_added"] == 2

    # 验证库内总行数
    con = duckdb.connect(db_file, read_only=True)
    count_after_1st = con.execute("SELECT COUNT(*) FROM daily_snapshot WHERE symbol = 'SZ000001'").fetchone()[0]
    states_after_1st = con.execute("SELECT period, sk_closed, sd_closed, mk_closed, md_closed, bar_count FROM incremental_state WHERE symbol = 'SZ000001'").fetchall()
    con.close()
    assert count_after_1st == 12

    # 第二次运行增量更新（相同 target_date）
    res2 = updater.run_update(allow_intraday=False)
    # 应直接跳过已是最新的标的
    assert res2["status"] == "SKIPPED_ALREADY_UP_TO_DATE" or res2["symbols_updated"] == 0

    # 验证行数零增加，状态完全不变
    con = duckdb.connect(db_file, read_only=True)
    count_after_2nd = con.execute("SELECT COUNT(*) FROM daily_snapshot WHERE symbol = 'SZ000001'").fetchone()[0]
    states_after_2nd = con.execute("SELECT period, sk_closed, sd_closed, mk_closed, md_closed, bar_count FROM incremental_state WHERE symbol = 'SZ000001'").fetchall()
    con.close()

    assert count_after_2nd == count_after_1st
    assert states_after_2nd == states_after_1st


def test_updater_intraday_progression(tmp_path):
    """测试盘中多次增量更新（例如上午先更新 slot 1~4，下午再更新 slot 5~8，总行数正确追加为 8 行）"""
    db_file = str(tmp_path / "test_intraday.duckdb")
    con = duckdb.connect(db_file)
    create_tables(con)
    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, board, latest_adj_factor)
        VALUES ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', 'MAIN', 1.0)
    """)
    # 插入前一日 10 根日线与初始状态
    dates = pd.date_range("2026-10-01", periods=10, freq="B").date
    initial_daily_rows = [{
        "symbol": "SZ000001",
        "code": "000001",
        "trade_date": d,
        "bar_seq": i,
        "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2,
        "volume": 1000.0, "amount": 10200.0, "adj_factor": 1.0,
    } for i, d in enumerate(dates)]
    df_init_d = pd.DataFrame(initial_daily_rows)
    df_init_d_snap, d_states = MultiCycleKDJEngine.compute_daily_snapshot(df_init_d)
    con.register("tmp_init_d", df_init_d_snap)
    con.execute("INSERT INTO daily_snapshot BY NAME SELECT * FROM tmp_init_d")

    df_init_states = states_to_df(list(d_states.values()))
    con.register("tmp_init_s", df_init_states)
    con.execute("INSERT INTO incremental_state BY NAME SELECT * FROM tmp_init_s")
    con.close()

    # 1. 模拟上午盘中 (达到 slot 4: 10:30)
    target_date = datetime.date(2026, 10, 16)
    mock_fetcher = MagicMock()
    mock_fetcher.get_latest_market_status.return_value = (target_date, False)  # 盘中未收盘
    mock_fetcher.fetch_xdxr_events.return_value = []
    mock_fetcher.fetch_daily_bars.return_value = []

    morning_slots = [
        {"datetime": "2026-10-16 09:45", "open": 10.0, "high": 10.2, "low": 9.9, "close": 10.1, "vol": 10000, "amount": 101000},
        {"datetime": "2026-10-16 10:00", "open": 10.1, "high": 10.3, "low": 10.0, "close": 10.2, "vol": 10000, "amount": 102000},
        {"datetime": "2026-10-16 10:15", "open": 10.2, "high": 10.4, "low": 10.1, "close": 10.3, "vol": 10000, "amount": 103000},
        {"datetime": "2026-10-16 10:30", "open": 10.3, "high": 10.5, "low": 10.2, "close": 10.4, "vol": 10000, "amount": 104000},
    ]
    mock_fetcher.fetch_minute_bars.return_value = morning_slots

    updater = DailyUpdater(db_path=db_file, fetcher=mock_fetcher)
    res1 = updater.run_update(allow_intraday=True)
    assert res1["status"] == "SUCCESS"
    assert res1["minute_rows_added"] == 4

    con = duckdb.connect(db_file, read_only=True)
    m_count_1 = con.execute("SELECT COUNT(*) FROM minute_snapshot WHERE symbol = 'SZ000001'").fetchone()[0]
    last_slot_1 = con.execute("SELECT ARG_MAX(slot_seq, bar_end) FROM minute_snapshot WHERE symbol = 'SZ000001'").fetchone()[0]
    con.close()
    assert m_count_1 == 4
    assert last_slot_1 == 4

    # 2. 模拟下午盘中 (推进至 slot 8: 11:30)
    afternoon_slots = morning_slots + [
        {"datetime": "2026-10-16 10:45", "open": 10.4, "high": 10.6, "low": 10.3, "close": 10.5, "vol": 10000, "amount": 105000},
        {"datetime": "2026-10-16 11:00", "open": 10.5, "high": 10.7, "low": 10.4, "close": 10.6, "vol": 10000, "amount": 106000},
        {"datetime": "2026-10-16 11:15", "open": 10.6, "high": 10.8, "low": 10.5, "close": 10.7, "vol": 10000, "amount": 107000},
        {"datetime": "2026-10-16 11:30", "open": 10.7, "high": 10.9, "low": 10.6, "close": 10.8, "vol": 10000, "amount": 108000},
    ]
    mock_fetcher.fetch_minute_bars.return_value = afternoon_slots

    res2 = updater.run_update(allow_intraday=True)
    assert res2["status"] == "SUCCESS"
    assert res2["minute_rows_added"] == 4

    con = duckdb.connect(db_file, read_only=True)
    m_count_2 = con.execute("SELECT COUNT(*) FROM minute_snapshot WHERE symbol = 'SZ000001'").fetchone()[0]
    last_slot_2 = con.execute("SELECT ARG_MAX(slot_seq, bar_end) FROM minute_snapshot WHERE symbol = 'SZ000001'").fetchone()[0]
    slots = [r[0] for r in con.execute("SELECT slot_seq FROM minute_snapshot WHERE symbol = 'SZ000001' ORDER BY slot_seq").fetchall()]
    con.close()

    assert m_count_2 == 8
    assert last_slot_2 == 8
    assert slots == [1, 2, 3, 4, 5, 6, 7, 8]


def test_intraday_does_not_pollute_daily_snapshot(tmp_path):
    """测试盘中更新（--allow-intraday）绝不写入未收盘日线，且收盘后完整写入"""
    db_file = str(tmp_path / "test_intraday_pollution.duckdb")
    con = duckdb.connect(db_file)
    create_tables(con)
    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, board, latest_adj_factor)
        VALUES ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', 'MAIN', 1.0)
    """)
    # 历史日线写入至 2026-10-15
    dates = [datetime.date(2026, 10, 14), datetime.date(2026, 10, 15)]
    init_d = [{
        "symbol": "SZ000001", "code": "000001", "trade_date": d, "bar_seq": i,
        "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "volume": 1000.0, "amount": 10200.0, "adj_factor": 1.0,
    } for i, d in enumerate(dates)]
    df_init_d = pd.DataFrame(init_d)
    df_init_snap, d_states = MultiCycleKDJEngine.compute_daily_snapshot(df_init_d)
    con.register("tmp_d", df_init_snap)
    con.execute("INSERT INTO daily_snapshot BY NAME SELECT * FROM tmp_d")
    con.register("tmp_s", states_to_df(list(d_states.values())))
    con.execute("INSERT INTO incremental_state BY NAME SELECT * FROM tmp_s")
    con.close()

    # 1. 模拟 2026-10-16 盘中 (上午 10:30，未收盘)
    mock_fetcher = MagicMock()
    mock_fetcher.get_latest_market_dates.return_value = (
        datetime.date(2026, 10, 15), datetime.date(2026, 10, 16), False
    )
    mock_fetcher.get_latest_market_status.return_value = (
        datetime.date(2026, 10, 16), False
    )
    mock_fetcher.fetch_xdxr_events.return_value = []
    # 模拟通达信在盘中也返回了包含 2026-10-16 的未收盘日线
    mock_fetcher.fetch_daily_bars.return_value = [
        {"datetime": "2026-10-15", "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "vol": 1000, "amount": 10200},
        {"datetime": "2026-10-16", "open": 10.2, "high": 10.6, "low": 10.1, "close": 10.5, "vol": 500, "amount": 5250},
    ]
    # 分钟线返回 10-16 的 slot 1~4
    mock_fetcher.fetch_minute_bars.return_value = [
        {"datetime": "2026-10-16 09:45", "open": 10.2, "high": 10.3, "low": 10.1, "close": 10.3, "vol": 10000, "amount": 103000},
        {"datetime": "2026-10-16 10:00", "open": 10.3, "high": 10.4, "low": 10.2, "close": 10.4, "vol": 10000, "amount": 104000},
        {"datetime": "2026-10-16 10:15", "open": 10.4, "high": 10.5, "low": 10.3, "close": 10.4, "vol": 10000, "amount": 104000},
        {"datetime": "2026-10-16 10:30", "open": 10.4, "high": 10.6, "low": 10.3, "close": 10.5, "vol": 10000, "amount": 105000},
    ]

    updater = DailyUpdater(db_path=db_file, fetcher=mock_fetcher)
    res_intraday = updater.run_update(allow_intraday=True)
    assert res_intraday["status"] == "SUCCESS"
    assert res_intraday["minute_rows_added"] == 4
    assert res_intraday["daily_rows_added"] == 0

    con = duckdb.connect(db_file, read_only=True)
    # 断言: daily_snapshot 绝不包含 2026-10-16
    d_dates = [r[0] for r in con.execute("SELECT trade_date FROM daily_snapshot WHERE symbol = 'SZ000001'").fetchall()]
    assert datetime.date(2026, 10, 16) not in d_dates
    # 断言: minute_snapshot 包含了 2026-10-16 的 4 条
    m_count = con.execute("SELECT COUNT(*) FROM minute_snapshot WHERE symbol = 'SZ000001' AND trading_date = '2026-10-16'").fetchone()[0]
    assert m_count == 4
    con.close()

    # 2. 模拟 15:00 收盘后再次运行 (is_closed=True)
    mock_fetcher.get_latest_market_dates.return_value = (
        datetime.date(2026, 10, 16), datetime.date(2026, 10, 16), True
    )
    mock_fetcher.get_latest_market_status.return_value = (
        datetime.date(2026, 10, 16), True
    )
    # 模拟完整的日线与全天 16 根分钟线
    mock_fetcher.fetch_daily_bars.return_value = [
        {"datetime": "2026-10-15", "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "vol": 1000, "amount": 10200},
        {"datetime": "2026-10-16", "open": 10.2, "high": 10.8, "low": 10.1, "close": 10.7, "vol": 1200, "amount": 12840},
    ]
    all_16_min = mock_fetcher.fetch_minute_bars.return_value + [
        {"datetime": f"2026-10-16 {t}", "open": 10.5, "high": 10.7, "low": 10.4, "close": 10.6, "vol": 10000, "amount": 106000}
        for _, t in [
            (5, "10:45"), (6, "11:00"), (7, "11:15"), (8, "11:30"),
            (9, "13:15"), (10, "13:30"), (11, "13:45"), (12, "14:00"),
            (13, "14:15"), (14, "14:30"), (15, "14:45"), (16, "15:00")
        ]
    ]
    mock_fetcher.fetch_minute_bars.return_value = all_16_min

    res_closed = updater.run_update(allow_intraday=False)
    assert res_closed["status"] == "SUCCESS"
    assert res_closed["daily_rows_added"] == 1
    assert res_closed["minute_rows_added"] == 12

    con = duckdb.connect(db_file, read_only=True)
    d_rows = con.execute("SELECT trade_date, close, bar_seq FROM daily_snapshot WHERE symbol = 'SZ000001' ORDER BY trade_date").fetchall()
    assert len(d_rows) == 3
    assert d_rows[-1][0] == datetime.date(2026, 10, 16)
    assert d_rows[-1][1] == 10.7
    assert d_rows[-1][2] == 2

    m_total_16 = con.execute("SELECT COUNT(*) FROM minute_snapshot WHERE symbol = 'SZ000001' AND trading_date = '2026-10-16'").fetchone()[0]
    assert m_total_16 == 16
    con.close()


def test_multi_day_ex_dividend_minute_adj_factor_mapping(tmp_path):
    """测试跨多日补更遇除权除息时，分钟线 adj_factor 精确按日映射且互不串扰"""
    db_file = str(tmp_path / "test_ex_dividend_mapping.duckdb")
    con = duckdb.connect(db_file)
    create_tables(con)
    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, board, latest_adj_factor)
        VALUES ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', 'MAIN', 1.0)
    """)
    # 历史日线截至 2026-10-14，最新因子 1.0，收盘价 10.0
    con.execute("""
        INSERT INTO daily_snapshot (
            symbol, code, trade_date, bar_seq, open, high, low, close, volume, amount, adj_factor
        ) VALUES ('SZ000001', '000001', '2026-10-14', 10, 10.0, 10.5, 9.8, 10.0, 1000, 10000, 1.0)
    """)
    con.close()

    # 一次补更两个交易日: 2026-10-15 和 2026-10-16
    # 其中 2026-10-16 发生每 10 股派 5 元分红 (fenhong=5.0)
    target_date = datetime.date(2026, 10, 16)
    mock_fetcher = MagicMock()
    mock_fetcher.get_latest_market_dates.return_value = (target_date, target_date, True)
    mock_fetcher.get_latest_market_status.return_value = (target_date, True)

    # 10-15 无除权，10-16 除权分红
    mock_fetcher.fetch_xdxr_events.return_value = [
        {"year": 2026, "month": 10, "day": 16, "category": 1, "fenhong": 5.0, "songzhuangu": 0.0, "peigu": 0.0, "peigujia": 0.0}
    ]
    mock_fetcher.fetch_daily_bars.return_value = [
        {"datetime": "2026-10-14", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.0, "vol": 1000, "amount": 10000},
        {"datetime": "2026-10-15", "open": 10.0, "high": 10.2, "low": 9.9, "close": 10.0, "vol": 1000, "amount": 10000},
        {"datetime": "2026-10-16", "open": 9.5, "high": 9.8, "low": 9.4, "close": 9.6, "vol": 1000, "amount": 9600},
    ]
    mock_fetcher.fetch_minute_bars.return_value = [
        {"datetime": "2026-10-15 09:45", "open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0, "vol": 1000, "amount": 10000},
        {"datetime": "2026-10-15 15:00", "open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0, "vol": 1000, "amount": 10000},
        {"datetime": "2026-10-16 09:45", "open": 9.5, "high": 9.6, "low": 9.4, "close": 9.5, "vol": 1000, "amount": 9500},
        {"datetime": "2026-10-16 15:00", "open": 9.5, "high": 9.7, "low": 9.5, "close": 9.6, "vol": 1000, "amount": 9600},
    ]

    updater = DailyUpdater(db_path=db_file, fetcher=mock_fetcher)
    res = updater.run_update(allow_intraday=False)
    assert res["status"] == "SUCCESS"

    con = duckdb.connect(db_file, read_only=True)
    # 查看 minute_snapshot 的 adj_factor
    df_m = con.execute("SELECT trading_date, slot_seq, adj_factor FROM minute_snapshot WHERE symbol = 'SZ000001' ORDER BY trading_date, slot_seq").df()
    con.close()

    # 10-15 的分钟线必须保持除权前的 1.0
    m_15 = df_m[df_m["trading_date"] == datetime.date(2026, 10, 15)]
    assert (m_15["adj_factor"] == 1.0).all()

    # 10-16 的分钟线必须是除权后的新因子
    # 10-15 收盘价为 10.0，每10股派5元 (每股0.5元)，除权基准价 10.0 - 0.5 = 9.5
    # 新因子 = 1.0 * (10.0 / 9.5) = 1.0526315789...
    m_16 = df_m[df_m["trading_date"] == datetime.date(2026, 10, 16)]
    expected_new_adj = 1.0 * (10.0 / 9.5)
    for fac in m_16["adj_factor"]:
        assert abs(fac - expected_new_adj) < 1e-6
        assert fac > 1.0


def test_intraday_unclosed_bar_filtered_and_post_market_resumes(tmp_path, monkeypatch):
    """测试盘中未走完 15m K 线被自动拦截，且盘后无缝续接并与一次性全天更新严格一致"""
    # 1. 初始化数据库与历史数据（截至 2026-10-15）
    db_file = str(tmp_path / "test_unclosed.duckdb")
    con = duckdb.connect(db_file)
    create_tables(con)
    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, board, latest_adj_factor)
        VALUES ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', 'MAIN', 1.0)
    """)
    dates = pd.date_range("2026-10-01", "2026-10-15", freq="B").date
    initial_daily_rows = [{
        "symbol": "SZ000001", "code": "000001", "trade_date": d, "bar_seq": i,
        "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2,
        "volume": 1000.0, "amount": 10200.0, "adj_factor": 1.0,
    } for i, d in enumerate(dates)]
    df_init_d = pd.DataFrame(initial_daily_rows)
    df_init_d_snap, d_states = MultiCycleKDJEngine.compute_daily_snapshot(df_init_d)
    con.register("tmp_init_d", df_init_d_snap)
    con.execute("INSERT INTO daily_snapshot BY NAME SELECT * FROM tmp_init_d")
    con.register("tmp_init_s", states_to_df(list(d_states.values())))
    con.execute("INSERT INTO incremental_state BY NAME SELECT * FROM tmp_init_s")
    con.close()

    # 构造当日（2026-10-16）全部 16 根分钟线数据
    time_slots = [
        ("09:45", 10.0, 10.2, 9.9, 10.1),
        ("10:00", 10.1, 10.3, 10.0, 10.2),
        ("10:15", 10.2, 10.4, 10.1, 10.3),
        ("10:30", 10.3, 10.5, 10.2, 10.4),  # Slot 4 (盘中 10:20 时未完结)
        ("10:45", 10.4, 10.6, 10.3, 10.5),
        ("11:00", 10.5, 10.7, 10.4, 10.6),
        ("11:15", 10.6, 10.8, 10.5, 10.7),
        ("11:30", 10.7, 10.9, 10.6, 10.8),
        ("13:15", 10.8, 11.0, 10.7, 10.9),
        ("13:30", 10.9, 11.1, 10.8, 11.0),
        ("13:45", 11.0, 11.2, 10.9, 11.1),
        ("14:00", 11.1, 11.3, 11.0, 11.2),
        ("14:15", 11.2, 11.4, 11.1, 11.3),
        ("14:30", 11.3, 11.5, 11.2, 11.4),
        ("14:45", 11.4, 11.6, 11.3, 11.5),
        ("15:00", 11.5, 11.7, 11.4, 11.6),
    ]
    all_16_bars = [
        {"datetime": f"2026-10-16 {t}", "open": o, "high": h, "low": l, "close": c, "vol": 10000, "amount": 100000}
        for t, o, h, l, c in time_slots
    ]

    # --- 阶段 1：模拟盘中 10:20:00 运行 allow_intraday=True ---
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 10, 20, 0))
    monkeypatch.setattr("src.updater.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 10, 20, 0))

    mock_fetcher = MagicMock()
    # 盘中日线未收盘
    mock_fetcher.get_latest_market_dates.return_value = (
        datetime.date(2026, 10, 15), datetime.date(2026, 10, 16), False
    )
    mock_fetcher.get_latest_market_status.return_value = (datetime.date(2026, 10, 16), False)
    mock_fetcher.fetch_xdxr_events.return_value = []
    mock_fetcher.fetch_daily_bars.return_value = []
    # 通达信返回了前 4 根线（包含正在跳动的 10:30 第 4 槽）
    mock_fetcher.fetch_minute_bars.return_value = all_16_bars[:4]

    updater = DailyUpdater(db_path=db_file, fetcher=mock_fetcher)
    res_intra = updater.run_update(allow_intraday=True)

    assert res_intra["status"] == "SUCCESS"
    assert res_intra["minute_rows_added"] == 3  # Slot 4 被成功拦截！仅入库 Slot 1~3

    con = duckdb.connect(db_file, read_only=True)
    slots_intra = [r[0] for r in con.execute("SELECT slot_seq FROM minute_snapshot WHERE symbol = 'SZ000001' ORDER BY slot_seq").fetchall()]
    d_count_intra = con.execute("SELECT COUNT(*) FROM daily_snapshot WHERE trade_date = '2026-10-16'").fetchone()[0]
    con.close()
    assert slots_intra == [1, 2, 3]
    assert d_count_intra == 0  # 盘中绝不污染日线

    # --- 阶段 2：模拟盘后 15:35:00 运行 allow_intraday=False ---
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 35, 0))
    monkeypatch.setattr("src.updater.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 35, 0))

    mock_fetcher.get_latest_market_dates.return_value = (
        datetime.date(2026, 10, 16), datetime.date(2026, 10, 16), True
    )
    mock_fetcher.get_latest_market_status.return_value = (datetime.date(2026, 10, 16), True)
    mock_fetcher.fetch_daily_bars.return_value = [
        {"datetime": "2026-10-15", "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2, "vol": 1000, "amount": 10200},
        {"datetime": "2026-10-16", "open": 10.0, "high": 11.7, "low": 9.9, "close": 11.6, "vol": 160000, "amount": 1600000},
    ]
    mock_fetcher.fetch_minute_bars.return_value = all_16_bars

    res_post = updater.run_update(allow_intraday=False)
    assert res_post["status"] == "SUCCESS"
    assert res_post["daily_rows_added"] == 1
    assert res_post["minute_rows_added"] == 13  # 从 Slot 4 续接至 Slot 16 (共 13 根)

    con = duckdb.connect(db_file, read_only=True)
    all_slots = [r[0] for r in con.execute("SELECT slot_seq FROM minute_snapshot WHERE symbol = 'SZ000001' AND trading_date = '2026-10-16' ORDER BY slot_seq").fetchall()]
    df_actual = con.execute("SELECT * FROM minute_snapshot WHERE symbol = 'SZ000001' AND trading_date = '2026-10-16' ORDER BY slot_seq").df()
    con.close()
    assert all_slots == list(range(1, 17))

    # --- 阶段 3：在干净数据库上模拟一次性盘后全天运行，对比指标完全一致性 ---
    db_clean = str(tmp_path / "test_clean_direct.duckdb")
    con_c = duckdb.connect(db_clean)
    create_tables(con_c)
    con_c.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, board, latest_adj_factor)
        VALUES ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', 'MAIN', 1.0)
    """)
    con_c.register("tmp_init_d", df_init_d_snap)
    con_c.execute("INSERT INTO daily_snapshot BY NAME SELECT * FROM tmp_init_d")
    con_c.register("tmp_init_s", states_to_df(list(d_states.values())))
    con_c.execute("INSERT INTO incremental_state BY NAME SELECT * FROM tmp_init_s")
    con_c.close()

    updater_clean = DailyUpdater(db_path=db_clean, fetcher=mock_fetcher)
    updater_clean.run_update(allow_intraday=False)

    con_c = duckdb.connect(db_clean, read_only=True)
    df_expected = con_c.execute("SELECT * FROM minute_snapshot WHERE symbol = 'SZ000001' AND trading_date = '2026-10-16' ORDER BY slot_seq").df()
    con_c.close()

    # 断言 16 个槽位的全部指标值与一次性全天更新 100% 吻合
    metric_cols = [c for c in df_actual.columns if any(c.startswith(prefix) for prefix in ["sk_", "sd_", "skxl_", "sdxl_", "mk_", "md_", "mkxl_", "mdxl_"])]
    for col in metric_cols:
        diff = np.nanmax(np.abs(df_actual[col].values - df_expected[col].values))
        assert diff < 1e-9, f"指标 {col} 存在差异: {diff}"


def test_intraday_ex_dividend_no_double_multiply(tmp_path, monkeypatch):
    """测试除权除息股票盘中更新与盘后补齐日线时，复权因子严格幂等、绝无二次累乘"""
    db_file = str(tmp_path / "test_div_idempotent.duckdb")
    con = duckdb.connect(db_file)
    create_tables(con)
    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, board, latest_adj_factor)
        VALUES ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', 'MAIN', 1.0)
    """)
    # 截至 2026-10-15 收盘价 10.0，复权因子 1.0
    con.execute("""
        INSERT INTO daily_snapshot (
            symbol, code, trade_date, bar_seq, open, high, low, close, volume, amount, adj_factor
        ) VALUES ('SZ000001', '000001', '2026-10-15', 10, 10.0, 10.5, 9.8, 10.0, 1000, 10000, 1.0)
    """)
    con.close()

    # 2026-10-16 发生除权除息: 每10股派2元 (fenhong=2.0)
    # 除权参考价 = 10.0 - 0.2 = 9.8
    # 期望新因子 = 1.0 * (10.0 / 9.8) = 1.020408163...
    expected_factor = 1.0 * (10.0 / 9.8)
    event_div = [
        {"year": 2026, "month": 10, "day": 16, "category": 1, "fenhong": 2.0, "songzhuangu": 0.0, "peigu": 0.0, "peigujia": 0.0}
    ]

    time_slots = [
        ("09:45", 9.8, 9.9, 9.7, 9.8),
        ("10:00", 9.8, 10.0, 9.8, 9.9),
        ("10:15", 9.9, 10.1, 9.8, 10.0),
        ("10:30", 10.0, 10.2, 9.9, 10.1),
        ("10:45", 10.1, 10.3, 10.0, 10.2),
        ("11:00", 10.2, 10.3, 10.1, 10.2),
        ("11:15", 10.2, 10.4, 10.2, 10.3),
        ("11:30", 10.3, 10.5, 10.2, 10.4),  # Slot 8
        ("13:15", 10.4, 10.6, 10.3, 10.5),
        ("13:30", 10.5, 10.7, 10.4, 10.6),
        ("13:45", 10.6, 10.7, 10.5, 10.6),
        ("14:00", 10.6, 10.8, 10.6, 10.7),
        ("14:15", 10.7, 10.9, 10.6, 10.8),
        ("14:30", 10.8, 10.9, 10.7, 10.8),
        ("14:45", 10.8, 11.0, 10.8, 10.9),
        ("15:00", 10.9, 11.0, 10.8, 11.0),
    ]
    all_16_bars = [
        {"datetime": f"2026-10-16 {t}", "open": o, "high": h, "low": l, "close": c, "vol": 10000, "amount": 100000}
        for t, o, h, l, c in time_slots
    ]

    # --- 阶段 1a：模拟盘中 10:05:00 首次更新 allow_intraday=True (入库 Slot 1~2) ---
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 10, 5, 0))
    monkeypatch.setattr("src.updater.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 10, 5, 0))

    mock_fetcher = MagicMock()
    mock_fetcher.get_latest_market_dates.return_value = (
        datetime.date(2026, 10, 15), datetime.date(2026, 10, 16), False
    )
    mock_fetcher.get_latest_market_status.return_value = (datetime.date(2026, 10, 16), False)
    mock_fetcher.fetch_xdxr_events.return_value = event_div
    mock_fetcher.fetch_daily_bars.return_value = []
    mock_fetcher.fetch_minute_bars.return_value = all_16_bars[:2]  # Slot 1~2

    updater = DailyUpdater(db_path=db_file, fetcher=mock_fetcher)
    res_intra1a = updater.run_update(allow_intraday=True)
    assert res_intra1a["status"] == "SUCCESS"
    assert res_intra1a["minute_rows_added"] == 2
    # 首次触发除权预演，fetch_xdxr_events 被调用 1 次
    assert mock_fetcher.fetch_xdxr_events.call_count == 1

    con = duckdb.connect(db_file, read_only=True)
    m_factors_1a = [r[0] for r in con.execute("SELECT adj_factor FROM minute_snapshot WHERE symbol = 'SZ000001'").fetchall()]
    sec_adj_1a = con.execute("SELECT latest_adj_factor FROM security_info WHERE symbol = 'SZ000001'").fetchone()[0]
    ca_cnt_1a = con.execute("SELECT COUNT(*) FROM corporate_action WHERE symbol = 'SZ000001'").fetchone()[0]
    con.close()

    assert len(m_factors_1a) == 2
    for f in m_factors_1a:
        assert abs(f - expected_factor) < 1e-6
    assert abs(sec_adj_1a - expected_factor) < 1e-6
    assert ca_cnt_1a == 1

    # --- 阶段 1b：模拟盘中 11:35:00 继续更新 allow_intraday=True (入库 Slot 3~8) ---
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 11, 35, 0))
    monkeypatch.setattr("src.updater.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 11, 35, 0))
    mock_fetcher.fetch_minute_bars.return_value = all_16_bars[:8]  # Slot 1~8

    res_intra1b = updater.run_update(allow_intraday=True)
    assert res_intra1b["status"] == "SUCCESS"
    assert res_intra1b["minute_rows_added"] == 6  # 续接入库 Slot 3~8 (共 6 根)
    # 关键断言：同日 Slot 2~16 增量推进免拉取除权接口，fetch_xdxr_events 未被再次调用！
    assert mock_fetcher.fetch_xdxr_events.call_count == 1

    con = duckdb.connect(db_file, read_only=True)
    m_factors_1b = [r[0] for r in con.execute("SELECT adj_factor FROM minute_snapshot WHERE symbol = 'SZ000001' ORDER BY slot_seq").fetchall()]
    sec_adj_1b = con.execute("SELECT latest_adj_factor FROM security_info WHERE symbol = 'SZ000001'").fetchone()[0]
    ca_cnt_1b = con.execute("SELECT COUNT(*) FROM corporate_action WHERE symbol = 'SZ000001'").fetchone()[0]
    con.close()

    assert len(m_factors_1b) == 8
    for f in m_factors_1b:
        assert abs(f - expected_factor) < 1e-6
    assert abs(sec_adj_1b - expected_factor) < 1e-6
    assert ca_cnt_1b == 1

    # --- 阶段 2：模拟盘后 15:35:00 运行 allow_intraday=False ---
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 35, 0))
    monkeypatch.setattr("src.updater.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 35, 0))

    mock_fetcher.get_latest_market_dates.return_value = (
        datetime.date(2026, 10, 16), datetime.date(2026, 10, 16), True
    )
    mock_fetcher.get_latest_market_status.return_value = (datetime.date(2026, 10, 16), True)
    mock_fetcher.fetch_xdxr_events.return_value = event_div
    mock_fetcher.fetch_daily_bars.return_value = [
        {"datetime": "2026-10-15", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.0, "vol": 1000, "amount": 10000},
        {"datetime": "2026-10-16", "open": 9.8, "high": 11.0, "low": 9.7, "close": 11.0, "vol": 160000, "amount": 1600000},
    ]
    mock_fetcher.fetch_minute_bars.return_value = all_16_bars

    res_post = updater.run_update(allow_intraday=False)
    assert res_post["status"] == "SUCCESS"
    assert res_post["daily_rows_added"] == 1
    assert res_post["minute_rows_added"] == 8  # Slot 9~16

    con = duckdb.connect(db_file, read_only=True)
    d_row = con.execute("SELECT adj_factor, close FROM daily_snapshot WHERE symbol = 'SZ000001' AND trade_date = '2026-10-16'").fetchone()
    all_m_factors = [r[0] for r in con.execute("SELECT adj_factor FROM minute_snapshot WHERE symbol = 'SZ000001' ORDER BY slot_seq").fetchall()]
    sec_adj_post = con.execute("SELECT latest_adj_factor FROM security_info WHERE symbol = 'SZ000001'").fetchone()[0]
    ca_cnt_post = con.execute("SELECT COUNT(*) FROM corporate_action WHERE symbol = 'SZ000001'").fetchone()[0]
    con.close()

    # 关键断言：日线复权因子严格等于 expected_factor，绝未发生二次累乘 (如 1.0204 * 1.0204)
    assert abs(d_row[0] - expected_factor) < 1e-6
    # 全天 16 根分钟线复权因子严格等于 expected_factor
    assert len(all_m_factors) == 16
    for f in all_m_factors:
        assert abs(f - expected_factor) < 1e-6
    # 标的最新因子严格等于 expected_factor
    assert abs(sec_adj_post - expected_factor) < 1e-6
    # 除权事件表绝无重复记录
    assert ca_cnt_post == 1


def test_get_latest_market_dates_at_1450_not_closed(monkeypatch):
    """测试 14:45~15:00 尾盘时段即使 15m K 线带有 15:00 时间戳，也绝不误判 is_closed 为 True"""
    fetcher = TdxFetcher()

    # 模拟上证指数行情：最新日线为 2026-10-16，前一交易日为 2026-10-15
    mock_daily = [
        {"datetime": "2026-10-15", "open": 3200.0, "high": 3220.0, "low": 3190.0, "close": 3210.0},
        {"datetime": "2026-10-16", "open": 3210.0, "high": 3230.0, "low": 3205.0, "close": 3225.0},
    ]
    # 模拟通达信在 14:50 返回的第 16 根 K 线（已打上 15:00 时间戳）
    mock_min16 = [
        {"datetime": "2026-10-16 15:00", "open": 3220.0, "high": 3226.0, "low": 3218.0, "close": 3225.0}
    ]

    monkeypatch.setattr(fetcher, "fetch_daily_bars", lambda symbol, sec_type, offset=5: mock_daily)
    monkeypatch.setattr(fetcher, "fetch_minute_bars", lambda symbol, sec_type, offset=16: mock_min16)

    # 1. 模拟北京时间当日 14:50:00 (尚未收盘)
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 14, 50, 0))

    d_date_false, m_date_false, closed_false = fetcher.get_latest_market_dates(allow_intraday=False)
    assert closed_false is False
    assert d_date_false == datetime.date(2026, 10, 15)  # 严格回退为上一交易日
    assert m_date_false == datetime.date(2026, 10, 15)

    d_date_true, m_date_true, closed_true = fetcher.get_latest_market_dates(allow_intraday=True)
    assert closed_true is False
    assert d_date_true == datetime.date(2026, 10, 15)  # 严格回退为上一交易日
    assert m_date_true == datetime.date(2026, 10, 16)  # 盘中允许查看今日分钟线

    # 2. 模拟北京时间当日 15:35:00 (已走完 15:30:10 盘后固定价格交易缓冲，正式收盘)
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 35, 0))

    d_date_post, m_date_post, closed_post = fetcher.get_latest_market_dates(allow_intraday=False)
    assert closed_post is True
    assert d_date_post == datetime.date(2026, 10, 16)
    assert m_date_post == datetime.date(2026, 10, 16)


def test_get_latest_market_dates_before_and_after_1530(monkeypatch):
    """测试 15:30:10 盘后固定价格交易收盘阈值前后 is_closed 状态判定"""
    fetcher = TdxFetcher()

    mock_daily = [
        {"datetime": "2026-10-15", "open": 3200.0, "high": 3220.0, "low": 3190.0, "close": 3210.0},
        {"datetime": "2026-10-16", "open": 3210.0, "high": 3230.0, "low": 3205.0, "close": 3225.0},
    ]
    mock_min16 = [
        {"datetime": "2026-10-16 15:00", "open": 3220.0, "high": 3226.0, "low": 3218.0, "close": 3225.0}
    ]

    monkeypatch.setattr(fetcher, "fetch_daily_bars", lambda symbol, sec_type, offset=5: mock_daily)
    monkeypatch.setattr(fetcher, "fetch_minute_bars", lambda symbol, sec_type, offset=16: mock_min16)

    # 1. 14:50:00 (尾盘连续竞价时段) -> is_closed == False, daily_target_date == prev_day
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 14, 50, 0))
    d_d, m_d, closed = fetcher.get_latest_market_dates(allow_intraday=False)
    assert closed is False
    assert d_d == datetime.date(2026, 10, 15)

    # 2. 15:15:00 (盘后固定价格交易时段，日线未定格) -> is_closed == False, daily_target_date == prev_day
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 15, 0))
    d_d, m_d, closed = fetcher.get_latest_market_dates(allow_intraday=False)
    assert closed is False
    assert d_d == datetime.date(2026, 10, 15)
    # 盘中模式下分钟线可以看今日
    d_d_in, m_d_in, closed_in = fetcher.get_latest_market_dates(allow_intraday=True)
    assert closed_in is False
    assert d_d_in == datetime.date(2026, 10, 15)
    assert m_d_in == datetime.date(2026, 10, 16)

    # 3. 15:31:00 (盘后固定价格交易已完全结束并走完缓冲) -> is_closed == True, daily_target_date == latest_day
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 31, 0))
    d_d, m_d, closed = fetcher.get_latest_market_dates(allow_intraday=False)
    assert closed is True
    assert d_d == datetime.date(2026, 10, 16)
    assert m_d == datetime.date(2026, 10, 16)


def test_connection_pool_probe_and_filter_servers(monkeypatch):
    """测试通达信节点池并发预检：屏蔽断连/缺线异常节点、按延时升序择优、Epoch失效重连"""
    servers = [
        ("10.0.0.1", 7709, "dead"),
        ("10.0.0.2", 7709, "no_daily"),
        ("10.0.0.3", 7709, "no_min15"),
        ("10.0.0.4", 7709, "slow"),
        ("10.0.0.5", 7709, "fast"),
    ]

    class FakeApi:
        def __init__(self, heartbeat=False):
            self.ip = None
            self.client = None

        def connect(self, ip, port, time_out=2.0):
            if ip == "10.0.0.1":
                return False
            self.ip = ip
            self.client = object()
            return True

        def disconnect(self):
            self.client = None

        def get_security_bars(self, category, market, code, start, count):
            if self.ip == "10.0.0.2" and category == 4:
                return []
            if self.ip == "10.0.0.3" and category == 1:
                return []
            if self.ip == "10.0.0.4" and category == 4:
                time.sleep(0.06)
            return [{"datetime": "2026-11-10", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}]

        def get_xdxr_info(self, market, code):
            return []

    monkeypatch.setattr("src.fetcher.TdxHq_API", FakeApi)

    pool = TdxConnectionPool(servers=servers)
    res = pool.probe_and_filter_servers(timeout=1.0, max_active=2)

    assert res["fallback"] is False
    assert len(res["blocked"]) == 3
    blocked_reasons = {r["server"][2]: r["reason"] for r in res["blocked"]}
    assert blocked_reasons == {
        "dead": "connect_fail",
        "no_daily": "daily_empty",
        "no_min15": "min15_empty",
    }
    # 健康节点按耗时升序排列：fast 排在 slow 之前
    assert [s[2] for s in pool.servers] == ["fast", "slow"]

    # 验证 get_client() 优先绑定到筛选后的第一台极速节点
    client1 = pool.get_client()
    assert client1.ip == "10.0.0.5"

    # 若再次预检且限制 max_active=1 仅保留 10.0.0.4，Epoch 变更应强制释放旧绑定
    pool2_servers = [("10.0.0.4", 7709, "slow")]
    pool._all_servers = pool2_servers
    fetcher = TdxFetcher(pool=pool)
    res2 = fetcher.refresh_server_pool(timeout=1.0, max_active=1)
    assert len(res2["active"]) == 1
    assert [s[2] for s in pool.servers] == ["slow"]

    client2 = pool.get_client()
    assert client2.ip == "10.0.0.4"
    pool.disconnect_thread()


def test_connection_pool_probe_fallback_when_all_fail(monkeypatch):
    """测试全部通达信节点异常时安全回退保留全量候选列表，绝不导致连接池为空"""
    servers = [
        ("10.0.0.1", 7709, "dead1"),
        ("10.0.0.2", 7709, "xdxr_bad"),
    ]

    class AllFailApi:
        def __init__(self, heartbeat=False):
            self.ip = None
            self.client = None

        def connect(self, ip, port, time_out=2.0):
            if ip == "10.0.0.1":
                return False
            self.ip = ip
            self.client = object()
            return True

        def disconnect(self):
            self.client = None

        def get_security_bars(self, category, market, code, start, count):
            return [{"close": 1.0}]

        def get_xdxr_info(self, market, code):
            return None

    monkeypatch.setattr("src.fetcher.TdxHq_API", AllFailApi)

    pool = TdxConnectionPool(servers=servers)
    res = pool.probe_and_filter_servers(timeout=1.0, max_active=8)

    assert res["fallback"] is True
    assert res["active"] == []
    assert {r["reason"] for r in res["blocked"]} == {"connect_fail", "xdxr_none"}
    assert pool.servers == servers


def test_on_demand_incremental_state_and_postmarket_xdxr_reuse(tmp_path, monkeypatch):
    """测试按需状态读写隔离（盘中仅写分钟状态/盘后仅写日线状态）与盘后免重复拉取除权接口复用 last_m_adj"""
    db_file = str(tmp_path / "test_on_demand_state.duckdb")
    con = duckdb.connect(db_file)
    create_tables(con)
    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, board, latest_adj_factor)
        VALUES ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', 'MAIN', 1.0)
    """)
    dates = pd.date_range("2026-10-01", "2026-10-15", freq="B").date
    init_d = [{
        "symbol": "SZ000001", "code": "000001", "trade_date": d, "bar_seq": i,
        "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.0,
        "volume": 1000.0, "amount": 10000.0, "adj_factor": 1.0,
    } for i, d in enumerate(dates)]
    df_init_d_snap, d_states = MultiCycleKDJEngine.compute_daily_snapshot(pd.DataFrame(init_d))
    con.register("tmp_d", df_init_d_snap)
    con.execute("INSERT INTO daily_snapshot BY NAME SELECT * FROM tmp_d")

    sentinel_ts = datetime.datetime(2026, 10, 15, 15, 35, 0)
    m_states_init = [PeriodState.initial("SZ000001", p) for p in MINUTE_PERIOD_NAMES]
    df_all_init_states = states_to_df(list(d_states.values()) + m_states_init, updated_at=sentinel_ts)
    con.register("tmp_s", df_all_init_states)
    con.execute("INSERT INTO incremental_state BY NAME SELECT * FROM tmp_s")
    con.close()

    # 2026-10-16 分红除权事件: 每10股派5元 (基准收 10.0 -> 9.5, 新因子 10/9.5)
    expected_adj = 1.0 * (10.0 / 9.5)
    event_div = [
        {"year": 2026, "month": 10, "day": 16, "category": 1, "fenhong": 5.0, "songzhuangu": 0.0, "peigu": 0.0, "peigujia": 0.0}
    ]
    time_slots = [
        ("09:45", 9.5, 9.6, 9.4, 9.5), ("10:00", 9.5, 9.7, 9.5, 9.6),
        ("10:15", 9.6, 9.8, 9.5, 9.7), ("10:30", 9.7, 9.8, 9.6, 9.7),
        ("10:45", 9.7, 9.9, 9.6, 9.8), ("11:00", 9.8, 9.9, 9.7, 9.8),
        ("11:15", 9.8, 10.0, 9.8, 9.9), ("11:30", 9.9, 10.0, 9.8, 9.9),
        ("13:15", 9.9, 10.1, 9.8, 10.0), ("13:30", 10.0, 10.1, 9.9, 10.0),
        ("13:45", 10.0, 10.2, 9.9, 10.1), ("14:00", 10.1, 10.2, 10.0, 10.1),
        ("14:15", 10.1, 10.3, 10.0, 10.2), ("14:30", 10.2, 10.3, 10.1, 10.2),
        ("14:45", 10.2, 10.4, 10.1, 10.3), ("15:00", 10.3, 10.5, 10.2, 10.4),
    ]
    all_16_bars = [
        {"datetime": f"2026-10-16 {t}", "open": o, "high": h, "low": l, "close": c, "vol": 10000, "amount": 100000}
        for t, o, h, l, c in time_slots
    ]

    mock_fetcher = MagicMock()
    mock_fetcher.fetch_xdxr_events.return_value = event_div
    mock_fetcher.fetch_daily_bars.return_value = [
        {"datetime": "2026-10-15", "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.0, "vol": 1000, "amount": 10000},
        {"datetime": "2026-10-16", "open": 9.5, "high": 10.5, "low": 9.4, "close": 10.4, "vol": 160000, "amount": 1600000},
    ]

    updater = DailyUpdater(db_path=db_file, fetcher=mock_fetcher)
    captured_periods = []
    orig_process = updater.process_symbol

    def wrapped_process(task, td):
        res = orig_process(task, td)
        if res is not None and not res["state_df"].empty:
            captured_periods.append(set(res["state_df"]["period"].tolist()))
        return res

    monkeypatch.setattr(updater, "process_symbol", wrapped_process)

    # 1. 盘中更新（仅分钟线：Slot 1~16 在 15:05 走完，但日线未收盘）
    intra_ts = datetime.datetime(2026, 10, 16, 15, 5, 0)
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: intra_ts)
    monkeypatch.setattr("src.updater.get_cn_now", lambda: intra_ts)
    mock_fetcher.get_latest_market_dates.return_value = (
        datetime.date(2026, 10, 15), datetime.date(2026, 10, 16), False
    )
    mock_fetcher.fetch_minute_bars.return_value = all_16_bars

    res_intra = updater.run_update(allow_intraday=True)
    assert res_intra["status"] == "SUCCESS"
    assert res_intra["minute_rows_added"] == 16
    assert mock_fetcher.fetch_xdxr_events.call_count == 1
    # 断言：盘中仅写回 4 个分钟周期，绝不重写 7 个日线周期
    assert captured_periods[-1] == set(MINUTE_PERIOD_NAMES)

    con = duckdb.connect(db_file, read_only=True)
    d_state_ts = con.execute(
        "SELECT DISTINCT updated_at FROM incremental_state WHERE symbol = 'SZ000001' AND period IN ('1d', '2d', '4d', '8d', '16d', '32d', '64d')"
    ).fetchall()
    m_state_ts = con.execute(
        "SELECT DISTINCT updated_at FROM incremental_state WHERE symbol = 'SZ000001' AND period IN ('15m', '30m', '60m', '120m')"
    ).fetchall()
    con.close()
    assert d_state_ts == [(sentinel_ts,)]
    assert m_state_ts == [(intra_ts,)]

    # 2. 盘后 15:35 仅日线增量更新（当日 16 根分钟线已入库，应跳过 fetch_xdxr_events 并直接复用 last_m_adj）
    post_ts = datetime.datetime(2026, 10, 16, 15, 35, 0)
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: post_ts)
    monkeypatch.setattr("src.updater.get_cn_now", lambda: post_ts)
    mock_fetcher.fetch_xdxr_events.reset_mock()
    mock_fetcher.get_latest_market_dates.return_value = (
        datetime.date(2026, 10, 16), datetime.date(2026, 10, 16), True
    )

    res_post = updater.run_update(allow_intraday=False)
    assert res_post["status"] == "SUCCESS"
    assert res_post["daily_rows_added"] == 1
    assert res_post["minute_rows_added"] == 0
    # 关键断言：免调 fetch_xdxr_events
    assert mock_fetcher.fetch_xdxr_events.call_count == 0
    # 关键断言：仅写回 7 个日线周期
    assert captured_periods[-1] == set(DAILY_PERIOD_NAMES)

    con = duckdb.connect(db_file, read_only=True)
    d_adj = con.execute(
        "SELECT adj_factor FROM daily_snapshot WHERE symbol = 'SZ000001' AND trade_date = '2026-10-16'"
    ).fetchone()[0]
    m_state_ts_after = con.execute(
        "SELECT DISTINCT updated_at FROM incremental_state WHERE symbol = 'SZ000001' AND period IN ('15m', '30m', '60m', '120m')"
    ).fetchall()
    con.close()
    assert abs(d_adj - expected_adj) < 1e-6
    # 关键断言：分钟周期状态保持 15:05 时间戳未被重写
    assert m_state_ts_after == [(intra_ts,)]


def test_connection_pool_subnet_quota_circuit_breaker_and_empty_bars(monkeypatch):
    """测试 /24 子网配额去重、运行时连续异常动态熔断剔除、免探测极速重连及停牌空列表不销毁健康连接"""
    servers = [
        ("114.28.173.130", 7709, "subA_1"),
        ("114.28.173.131", 7709, "subA_2"),
        ("114.28.173.132", 7709, "subA_3"),
        ("114.28.173.133", 7709, "subA_4"),
        ("139.9.43.31", 7709, "subB_1"),
        ("139.9.43.104", 7709, "subB_2"),
        ("222.247.45.186", 7709, "subC_1"),
        ("103.251.85.93", 7709, "subD_1"),
    ]
    probe_rpc_calls = {"daily": 0, "min15": 0}

    class SubnetMockApi:
        def __init__(self, heartbeat=False):
            self.ip = None
            self.client = None

        def connect(self, ip, port, time_out=2.0):
            self.ip = ip
            self.client = object()
            return True

        def disconnect(self):
            self.client = None

        def get_security_bars(self, category, market, code, start, count):
            if category == 4:
                probe_rpc_calls["daily"] += 1
                if self.ip.startswith("114.28.173."):
                    time.sleep(0.005)
                elif self.ip.startswith("139.9.43."):
                    time.sleep(0.025)
                elif self.ip.startswith("222.247.45."):
                    time.sleep(0.04)
                else:
                    time.sleep(0.055)
            else:
                probe_rpc_calls["min15"] += 1
            return [{"datetime": "2026-11-11", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}]

        def get_xdxr_info(self, market, code):
            return []

    monkeypatch.setattr("src.fetcher.TdxHq_API", SubnetMockApi)

    pool = TdxConnectionPool(servers=servers)
    res = pool.probe_and_filter_servers(timeout=1.0, max_active=6, max_per_subnet=2)
    assert res["fallback"] is False
    assert len(pool.servers) == 6
    # 验证 114.28.173.* 虽然延时最低，但同 /24 子网最多入选 2 台
    sub_a_count = sum(1 for s in pool.servers if s[0].startswith("114.28.173."))
    assert sub_a_count == 2
    assert {s[2] for s in pool.servers} >= {"subB_1", "subB_2", "subC_1", "subD_1"}

    # 验证 _pool_epoch > 0 后 get_client() 不再重复发送探测请求
    probe_rpc_calls["daily"] = 0
    probe_rpc_calls["min15"] = 0
    pool.get_client()
    assert probe_rpc_calls["daily"] == 0
    assert probe_rpc_calls["min15"] == 0

    # 验证运行时动态熔断: 连续 3 次异常即从 pool.servers 移除，且至少保留 2 台保底
    bad_srv = pool.servers[0]
    assert pool.record_server_error(bad_srv, "timeout") is False
    assert pool.record_server_error(bad_srv, "timeout") is False
    assert bad_srv in pool.servers
    assert pool.record_server_error(bad_srv, "timeout") is True
    assert bad_srv not in pool.servers
    assert len(pool.servers) == 5

    # 继续熔断其余节点，验证保底至少剩 2 台
    for srv in list(pool.servers):
        for _ in range(3):
            pool.record_server_error(srv, "timeout")
    assert len(pool.servers) == 2

    # 重新预检后应清空故障计数并恢复全部优选节点
    res_reprobe = pool.probe_and_filter_servers(timeout=1.0, max_active=6, max_per_subnet=2)
    assert len(pool.servers) == 6
    assert pool._fail_counts == {}

    # 验证停牌空列表 [] 不触发断连重连，而 None 响应触发熔断计数与断连
    fetcher = TdxFetcher(pool=pool)
    client = pool.get_client()
    monkeypatch.setattr(client, "get_security_bars", lambda *args, **kwargs: [])
    disconnect_calls = {"n": 0}
    orig_disconnect = pool.disconnect_thread

    def spy_disconnect():
        disconnect_calls["n"] += 1
        orig_disconnect()

    monkeypatch.setattr(pool, "disconnect_thread", spy_disconnect)
    bars_empty = fetcher.fetch_daily_bars("SZ000001", "STOCK", offset=15)
    assert bars_empty == []
    assert disconnect_calls["n"] == 0

    # Node 1 返回 None 时应记录故障并断连切换到 Node 2 重试成功
    monkeypatch.setattr(client, "get_security_bars", lambda *args, **kwargs: None)
    bars_failover = fetcher.fetch_daily_bars("SZ000001", "STOCK", offset=15)
    assert len(bars_failover) == 1
    assert disconnect_calls["n"] == 1
    assert sum(pool._fail_counts.values()) == 1

    # 若全部节点均返回 None，重试耗尽后返回 [] 并累计两次故障
    monkeypatch.setattr(SubnetMockApi, "get_security_bars", lambda *args, **kwargs: None)
    pool.disconnect_thread()
    disconnect_calls["n"] = 0
    bars_none = fetcher.fetch_daily_bars("SZ000001", "STOCK", offset=15)
    assert bars_none == []
    assert disconnect_calls["n"] == 2
    assert sum(pool._fail_counts.values()) >= 3
    pool.disconnect_thread()


def test_fetch_finance_info_and_extract_liutongguben(monkeypatch):
    """测试 fetch_finance_info（指数跳过、异常重连重试）与 extract_liutongguben 校验"""
    assert extract_liutongguben(None) is None
    assert extract_liutongguben({}) is None
    assert extract_liutongguben({"liutongguben": 0.0}) is None
    assert extract_liutongguben({"liutongguben": -100.0}) is None
    assert extract_liutongguben({"liutongguben": float("nan")}) is None
    assert extract_liutongguben({"liutongguben": 19405685000.0}) == 19405685000.0

    pool = TdxConnectionPool(servers=[("10.0.0.1", 7709, "s1"), ("10.0.0.2", 7709, "s2")])
    fetcher = TdxFetcher(pool=pool)

    # 指数绝不发起网络请求
    assert fetcher.fetch_finance_info("SH000001") is None
    assert fetcher.fetch_finance_info("TDX880301") is None

    call_ips = []

    class FinMockApi:
        def __init__(self, heartbeat=False):
            self.ip = None
            self.client = None

        def connect(self, ip, port, time_out=2.0):
            self.ip = ip
            self.client = object()
            return True

        def disconnect(self):
            self.client = None

        def get_security_bars(self, category, market, code, start, count):
            return [{"close": 1.0}]

        def get_finance_info(self, market, code):
            call_ips.append(self.ip)
            if self.ip == "10.0.0.1":
                return None
            return {"market": market, "code": code, "liutongguben": 1250081562.5}

    monkeypatch.setattr("src.fetcher.TdxHq_API", FinMockApi)
    fin = fetcher.fetch_finance_info("SH600519")
    assert fin == {"market": 1, "code": "600519", "liutongguben": 1250081562.5}
    assert call_ips == ["10.0.0.1", "10.0.0.2"]
    pool.disconnect_thread()


def test_updater_liutongguben_and_turn10_incremental_persistence(tmp_path):
    """测试 DailyUpdater 自动刷新流通股本、持久化 turn10（含股本变更、0值保护、指数排除及 v_daily_qfq 查询）"""
    db_file = str(tmp_path / "test_turn10_updater.duckdb")
    con = duckdb.connect(db_file)
    create_tables(con)
    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, board, latest_adj_factor, liutongguben)
        VALUES
            ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', 'MAIN', 1.0, NULL),
            ('SZ000002', '000002', '万科A', 'STOCK', 'SZSE', 'MAIN', 1.0, 100000000.0),
            ('SH600519', '600519', '贵州茅台', 'STOCK', 'SSE', 'MAIN', 1.0, 50000000.0),
            ('SH000001', '000001', '上证指数', 'BROAD_INDEX', 'SSE', 'INDEX', 1.0, NULL)
    """)

    # 写入前 10 根日线 (2026-10-01 起 10 个工作日, bar_seq 0..9)
    dates = pd.date_range("2026-10-01", periods=10, freq="B").date
    all_init_d = []
    all_init_s = []
    for sym, code, sec_t, ltgb_init, base_vol in [
        ("SZ000001", "000001", "STOCK", None, 1000.0),
        ("SZ000002", "000002", "STOCK", 100_000_000.0, 2000.0),
        ("SH600519", "600519", "STOCK", 50_000_000.0, 500.0),
        ("SH000001", "000001", "BROAD_INDEX", None, 900000.0),
    ]:
        rows = []
        for i, d in enumerate(dates):
            v = np.nan if (sym == "SH600519" and i == 5) else base_vol
            rows.append({
                "symbol": sym, "code": code, "trade_date": d, "bar_seq": i,
                "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.2,
                "volume": v, "amount": 10000.0, "adj_factor": 1.0,
            })
        df_s, st_s = MultiCycleKDJEngine.compute_daily_snapshot(pd.DataFrame(rows), liutongguben=ltgb_init)
        all_init_d.append(df_s)
        all_init_s.extend(st_s.values())

    con.register("tmp_d", pd.concat(all_init_d, ignore_index=True))
    con.execute("INSERT INTO daily_snapshot BY NAME SELECT * FROM tmp_d")
    con.register("tmp_s", states_to_df(all_init_s))
    con.execute("INSERT INTO incremental_state BY NAME SELECT * FROM tmp_s")
    con.close()

    # 下一交易日 2026-10-15 收盘后增量更新
    d1 = datetime.date(2026, 10, 15)
    mock_fetcher = MagicMock()
    mock_fetcher.get_latest_market_dates.return_value = (d1, d1, True)
    mock_fetcher.get_latest_market_status.return_value = (d1, True)
    mock_fetcher.fetch_xdxr_events.return_value = []
    mock_fetcher.fetch_minute_bars.return_value = []

    def mock_daily_bars(symbol, sec_type, offset=15):
        vol_map = {"SZ000001": 200000, "SZ000002": 200000, "SH600519": 50000, "SH000001": 800000}
        return [{
            "datetime": "2026-10-15", "open": 10.0, "high": 10.5, "low": 9.5,
            "close": 10.3, "vol": vol_map[symbol], "amount": 200000,
        }]

    def mock_finance_info(symbol):
        # SZ000001 首次写入 1 亿股；SZ000002 返回 0.0 应保留原 1 亿股；SH600519 返回 5000 万股；指数绝不应被写入
        return {
            "SZ000001": {"liutongguben": 100_000_000.0},
            "SZ000002": {"liutongguben": 0.0},
            "SH600519": {"liutongguben": 50_000_000.0},
        }.get(symbol)

    mock_fetcher.fetch_daily_bars.side_effect = mock_daily_bars
    mock_fetcher.fetch_finance_info.side_effect = mock_finance_info

    updater = DailyUpdater(db_path=db_file, fetcher=mock_fetcher)
    res1 = updater.run_update(allow_intraday=False)
    assert res1["status"] == "SUCCESS"
    assert res1["daily_rows_added"] == 4

    con = duckdb.connect(db_file, read_only=True)
    # 1. SZ000001: 前9日(9*1000手) + 本日(2000手) = 11000手 * 10000 / 1e8 = 1.1%
    r_01 = con.execute(
        "SELECT turn10, liutongguben FROM v_daily_qfq WHERE symbol='SZ000001' AND trade_date='2026-10-15'"
    ).fetchone()
    assert np.isclose(r_01[0], 1.1)
    assert np.isclose(r_01[1], 100_000_000.0)

    # 2. SZ000002: fetch_finance_info 返回 0.0，保留原 DB 1e8，前9日(9*2000) + 本日(2000) = 20000手 -> 2.0%
    r_02 = con.execute(
        "SELECT turn10, liutongguben FROM v_daily_qfq WHERE symbol='SZ000002' AND trade_date='2026-10-15'"
    ).fetchone()
    assert np.isclose(r_02[0], 2.0)
    assert np.isclose(r_02[1], 100_000_000.0)

    # 3. SH600519: 前9根中包含 i=5 (volume=NULL)，故本日窗口含 NULL -> turn10 必须为 NULL
    r_mt = con.execute(
        "SELECT turn10, liutongguben FROM v_daily_qfq WHERE symbol='SH600519' AND trade_date='2026-10-15'"
    ).fetchone()
    assert r_mt[0] is None
    assert np.isclose(r_mt[1], 50_000_000.0)

    # 4. SH000001: 指数不计算 turn10 且不写入 liutongguben
    r_idx = con.execute(
        "SELECT turn10, liutongguben FROM v_daily_qfq WHERE symbol='SH000001' AND trade_date='2026-10-15'"
    ).fetchone()
    assert r_idx == (None, None)
    con.close()

    # 再过一个交易日 2026-10-16：模拟 SZ000001 限售股解禁（流通股本翻倍至 2 亿股）
    d2 = datetime.date(2026, 10, 16)
    mock_fetcher.get_latest_market_dates.return_value = (d2, d2, True)
    mock_fetcher.get_latest_market_status.return_value = (d2, True)
    mock_fetcher.fetch_daily_bars.side_effect = lambda sym, sec_type, offset=15: [{
        "datetime": "2026-10-16", "open": 10.3, "high": 10.6, "low": 10.2,
        "close": 10.5, "vol": 200000, "amount": 210000,
    }]
    mock_fetcher.fetch_finance_info.side_effect = lambda sym: (
        {"liutongguben": 200_000_000.0} if sym == "SZ000001" else {"liutongguben": 50_000_000.0}
    )

    res2 = updater.run_update(allow_intraday=False)
    assert res2["status"] == "SUCCESS"

    con = duckdb.connect(db_file, read_only=True)
    # SZ000001 2026-10-16: 前8日(8*1000) + 10-15(2000) + 10-16(2000) = 12000手 * 10000 / 2e8 = 0.6%
    # 同时 2026-10-15 的历史 turn10 仍保持 1.1% 不被污染
    hist_01 = con.execute(
        "SELECT trade_date, turn10 FROM daily_snapshot WHERE symbol='SZ000001' AND trade_date >= '2026-10-15' ORDER BY trade_date"
    ).fetchall()
    ltgb_01_new = con.execute(
        "SELECT liutongguben FROM security_info WHERE symbol='SZ000001'"
    ).fetchone()[0]
    con.close()
    assert np.isclose(hist_01[0][1], 1.1)
    assert np.isclose(hist_01[1][1], 0.6)
    assert np.isclose(ltgb_01_new, 200_000_000.0)


def test_migration_add_turn10_and_liutongguben(tmp_path):
    """测试迁移脚本对老旧库升级 schema/views、v1 历史对齐、未覆盖行补算与幂等重跑"""
    from scripts.add_turn10_and_liutongguben import backfill_turn10, upgrade_schema_and_views

    db_file = str(tmp_path / "legacy_adb.duckdb")
    v1_file = str(tmp_path / "mock_v1.duckdb")

    # 1. 构建不含 liutongguben 与 turn10 的旧结构 A_DB
    con = duckdb.connect(db_file)
    con.execute("""
        CREATE TABLE security_info (
            symbol VARCHAR PRIMARY KEY,
            code VARCHAR NOT NULL,
            name VARCHAR,
            sec_type VARCHAR NOT NULL,
            exchange VARCHAR NOT NULL,
            board VARCHAR,
            list_date DATE,
            delist_date DATE,
            latest_adj_factor DOUBLE DEFAULT 1.0,
            updated_at TIMESTAMP
        );
        CREATE TABLE daily_snapshot (
            symbol VARCHAR NOT NULL, code VARCHAR NOT NULL, trade_date DATE NOT NULL, bar_seq INTEGER NOT NULL,
            open DOUBLE NOT NULL, high DOUBLE NOT NULL, low DOUBLE NOT NULL, close DOUBLE NOT NULL,
            volume DOUBLE, amount DOUBLE, adj_factor DOUBLE NOT NULL,
            sk_1d DOUBLE, sd_1d DOUBLE, skxl_1d DOUBLE, sdxl_1d DOUBLE, mk_1d DOUBLE, md_1d DOUBLE, mkxl_1d DOUBLE, mdxl_1d DOUBLE,
            sk_2d DOUBLE, sd_2d DOUBLE, skxl_2d DOUBLE, sdxl_2d DOUBLE, mk_2d DOUBLE, md_2d DOUBLE, mkxl_2d DOUBLE, mdxl_2d DOUBLE,
            sk_4d DOUBLE, sd_4d DOUBLE, skxl_4d DOUBLE, sdxl_4d DOUBLE, mk_4d DOUBLE, md_4d DOUBLE, mkxl_4d DOUBLE, mdxl_4d DOUBLE,
            sk_8d DOUBLE, sd_8d DOUBLE, skxl_8d DOUBLE, sdxl_8d DOUBLE, mk_8d DOUBLE, md_8d DOUBLE, mkxl_8d DOUBLE, mdxl_8d DOUBLE,
            sk_16d DOUBLE, sd_16d DOUBLE, skxl_16d DOUBLE, sdxl_16d DOUBLE, mk_16d DOUBLE, md_16d DOUBLE, mkxl_16d DOUBLE, mdxl_16d DOUBLE,
            sk_32d DOUBLE, sd_32d DOUBLE, skxl_32d DOUBLE, sdxl_32d DOUBLE, mk_32d DOUBLE, md_32d DOUBLE, mkxl_32d DOUBLE, mdxl_32d DOUBLE,
            sk_64d DOUBLE, sd_64d DOUBLE, skxl_64d DOUBLE, sdxl_64d DOUBLE, mk_64d DOUBLE, md_64d DOUBLE, mkxl_64d DOUBLE, mdxl_64d DOUBLE,
            PRIMARY KEY (symbol, trade_date)
        );
        CREATE TABLE minute_snapshot (
            symbol VARCHAR NOT NULL, code VARCHAR NOT NULL, trading_date DATE NOT NULL, bar_end TIMESTAMP NOT NULL, slot_seq TINYINT NOT NULL,
            open DOUBLE NOT NULL, high DOUBLE NOT NULL, low DOUBLE NOT NULL, close DOUBLE NOT NULL,
            volume DOUBLE, amount DOUBLE, adj_factor DOUBLE NOT NULL,
            sk_15m DOUBLE, sd_15m DOUBLE, skxl_15m DOUBLE, sdxl_15m DOUBLE, mk_15m DOUBLE, md_15m DOUBLE, mkxl_15m DOUBLE, mdxl_15m DOUBLE,
            sk_30m DOUBLE, sd_30m DOUBLE, skxl_30m DOUBLE, sdxl_30m DOUBLE, mk_30m DOUBLE, md_30m DOUBLE, mkxl_30m DOUBLE, mdxl_30m DOUBLE,
            sk_60m DOUBLE, sd_60m DOUBLE, skxl_60m DOUBLE, sdxl_60m DOUBLE, mk_60m DOUBLE, md_60m DOUBLE, mkxl_60m DOUBLE, mdxl_60m DOUBLE,
            sk_120m DOUBLE, sd_120m DOUBLE, skxl_120m DOUBLE, sdxl_120m DOUBLE, mk_120m DOUBLE, md_120m DOUBLE, mkxl_120m DOUBLE, mdxl_120m DOUBLE,
            PRIMARY KEY (symbol, bar_end)
        );
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, latest_adj_factor)
        VALUES
            ('SZ000001', '000001', '平安银行', 'STOCK', 'SZSE', 1.0),
            ('SH000001', '000001', '上证指数', 'BROAD_INDEX', 'SSE', 1.0);
        INSERT INTO daily_snapshot (symbol, code, trade_date, bar_seq, open, high, low, close, volume, amount, adj_factor)
        VALUES
            ('SZ000001', '000001', '2026-11-10', 0, 10.0, 10.2, 9.9, 10.1, 1000.0, 10100.0, 1.0),
            ('SZ000001', '000001', '2026-11-11', 1, 10.1, 10.3, 10.0, 10.2, 2000.0, 20400.0, 1.0),
            ('SH000001', '000001', '2026-11-10', 0, 3000.0, 3010.0, 2990.0, 3005.0, 100000.0, 1000000.0, 1.0);
    """)

    # 2. 构建模拟源库 v1：仅包含 2026-11-10 的历史精确 turn10=9.99，不包含 2026-11-11
    con_v1 = duckdb.connect(v1_file)
    con_v1.execute("""
        CREATE TABLE stock_daily_feature_snapshot (
            symbol VARCHAR, trade_date TIMESTAMP, turn10 DOUBLE
        );
        INSERT INTO stock_daily_feature_snapshot VALUES ('SZ000001', TIMESTAMP '2026-11-10 00:00:00', 9.99);
    """)
    con_v1.close()

    # 3. 执行结构升级与写入流通股本 1 亿股
    upgrade_schema_and_views(con)
    con.execute("UPDATE security_info SET liutongguben = 100000000.0 WHERE symbol = 'SZ000001'")

    # 4. 首次回填：11-10 应严格对齐 v1 (9.99)，11-11 应补算 (1000+2000)*10000/1e8 = 0.3%，指数保持 NULL
    changed1 = backfill_turn10(con, source_db_path=v1_file, batch_size=500)
    assert changed1 == 2

    rows = con.execute("""
        SELECT symbol, trade_date, turn10, liutongguben
        FROM v_daily_qfq
        ORDER BY symbol, trade_date
    """).fetchall()
    # rows sorted by symbol: SH000001, SZ000001(11-10), SZ000001(11-11)
    assert rows[0][2] is None and rows[0][3] is None
    assert np.isclose(rows[1][2], 9.99)
    assert np.isclose(rows[2][2], 0.3)
    assert np.isclose(rows[2][3], 100_000_000.0)

    # 5. 二次重跑严格幂等（更新 0 行）
    changed2 = backfill_turn10(con, source_db_path=v1_file, batch_size=500)
    assert changed2 == 0
    con.close()


def test_extract_liutongguben_from_xdxr_and_fallbacks(tmp_path, monkeypatch):
    """测试 extract_liutongguben_from_xdxr、节点财务预检、空值防误伤熔断与 DailyUpdater 兜底"""
    # 1. extract_liutongguben_from_xdxr 单元测试
    assert extract_liutongguben_from_xdxr(None) is None
    assert extract_liutongguben_from_xdxr([]) is None
    assert extract_liutongguben_from_xdxr([{"year": 2025, "month": 1, "day": 1, "panhouliutong": 0.0}]) is None
    assert extract_liutongguben_from_xdxr([{"year": 2025, "month": 1, "day": 1, "panhouliutong": float("nan")}]) is None
    assert extract_liutongguben_from_xdxr([{"year": 2025, "month": 1, "day": 1, "houliutong": 5000.0}]) == 50_000_000.0

    # 乱序日期 + 后续纯分红事件不应覆盖最新有效股本（单位: 万股 -> 股）
    events_mix = [
        {"year": 2026, "month": 6, "day": 1, "category": 1, "panhouliutong": 0.0},
        {"year": 2025, "month": 12, "day": 31, "category": 5, "panhouliutong": 10126.25},
        {"year": 2022, "month": 4, "day": 22, "category": 5, "panhouliutong": 8000.0},
    ]
    assert extract_liutongguben_from_xdxr(events_mix) == 101_262_500.0

    # 2. _probe_one 财务接口预检与 fetch_finance_info 不误伤健康节点
    servers = [
        ("10.0.0.1", 7709, "bad_fin"),
        ("10.0.0.2", 7709, "good_fin"),
    ]

    class ProbeFinApi:
        def __init__(self, heartbeat=False):
            self.ip = None
            self.client = None

        def connect(self, ip, port, time_out=2.0):
            self.ip = ip
            self.client = object()
            return True

        def disconnect(self):
            self.client = None

        def get_security_bars(self, category, market, code, start, count):
            return [{"close": 1.0}]

        def get_xdxr_info(self, market, code):
            return []

        def get_finance_info(self, market, code):
            if self.ip == "10.0.0.1":
                return {"liutongguben": 0.0}
            return None if code == "002898" else {"liutongguben": 100000000.0}

    monkeypatch.setattr("src.fetcher.TdxHq_API", ProbeFinApi)
    pool = TdxConnectionPool(servers=servers)
    probe_res = pool.probe_and_filter_servers(timeout=1.0, max_active=2)
    assert [r["server"][2] for r in probe_res["blocked"]] == ["bad_fin"]
    assert probe_res["blocked"][0]["reason"] == "finance_invalid"
    assert [s[2] for s in pool.servers] == ["good_fin"]

    # fetch_finance_info 对特例股返回 None 时不应记录服务端故障
    fetcher = TdxFetcher(pool=pool)
    assert fetcher.fetch_finance_info("SZ002898") is None
    assert pool._fail_counts == {}
    pool.disconnect_thread()

    # 3. _compute_turn10_array 非有限值防御与 DailyUpdater xdxr 二级兜底
    arr_inf = MultiCycleKDJEngine._compute_turn10_array(
        np.array([1000.0, np.inf], dtype=np.float64),
        prev_volumes=None,
        liutongguben=100_000_000.0,
    )
    assert np.isfinite(arr_inf[0]) and np.isnan(arr_inf[1])

    db_file = str(tmp_path / "test_xdxr_fallback_updater.duckdb")
    con = duckdb.connect(db_file)
    create_tables(con)
    con.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, board, latest_adj_factor, liutongguben)
        VALUES ('SZ301139', '301139', '元道通信', 'STOCK', 'SZSE', 'CYB', 1.0, NULL)
    """)
    con.execute("""
        INSERT INTO daily_snapshot (
            symbol, code, trade_date, bar_seq, open, high, low, close, volume, amount, adj_factor
        ) VALUES ('SZ301139', '301139', '2026-10-14', 0, 20.0, 21.0, 19.5, 20.5, 1000.0, 20500.0, 1.0)
    """)
    con.close()

    d_target = datetime.date(2026, 10, 15)
    mock_fetcher = MagicMock()
    mock_fetcher.get_latest_market_dates.return_value = (d_target, d_target, True)
    mock_fetcher.get_latest_market_status.return_value = (d_target, True)
    mock_fetcher.fetch_daily_bars.return_value = [{
        "datetime": "2026-10-15", "open": 20.5, "high": 21.5, "low": 20.0,
        "close": 21.0, "vol": 200000, "amount": 4200000,
    }]
    mock_fetcher.fetch_minute_bars.return_value = []
    mock_fetcher.fetch_finance_info.return_value = {"liutongguben": 0.0}
    mock_fetcher.fetch_xdxr_events.return_value = [
        {"year": 2026, "month": 6, "day": 30, "category": 5, "panhouliutong": 10000.0}
    ]

    updater = DailyUpdater(db_path=db_file, fetcher=mock_fetcher)
    res_up = updater.run_update(allow_intraday=False)
    assert res_up["status"] == "SUCCESS"
    assert res_up["daily_rows_added"] == 1

    con = duckdb.connect(db_file, read_only=True)
    ltgb_saved, turn10_saved = con.execute(
        "SELECT liutongguben, turn10 FROM v_daily_qfq WHERE symbol='SZ301139' AND trade_date='2026-10-15'"
    ).fetchone()
    con.close()
    assert np.isclose(ltgb_saved, 100_000_000.0)
    # 前1日 1000手 + 本日 2000手 = 3000手 * 10000 / 1e8 = 0.3%
    assert np.isclose(turn10_saved, 0.3)

    # 4. 测试 populate_liutongguben 三级兜底（v1 turn10 反推）与 backfill_turn10 对 v1 NULL/NaN 自动补算
    from scripts.add_turn10_and_liutongguben import backfill_turn10, populate_liutongguben

    db_mig = str(tmp_path / "test_tier3_mig.duckdb")
    v1_mig = str(tmp_path / "test_tier3_v1.duckdb")

    con_m = duckdb.connect(db_mig)
    create_tables(con_m)
    con_m.execute("""
        INSERT INTO security_info (symbol, code, name, sec_type, exchange, board, latest_adj_factor, liutongguben)
        VALUES
            ('SH601059', '601059', '信达证券', 'STOCK', 'SSE', 'MAIN', 1.0, NULL),
            ('SH000001', '000001', '上证指数', 'BROAD_INDEX', 'SSE', 'INDEX', 1.0, 999.0)
    """)
    con_m.execute("""
        INSERT INTO daily_snapshot (
            symbol, code, trade_date, bar_seq, open, high, low, close, volume, amount, adj_factor
        ) VALUES
            ('SH601059', '601059', '2026-10-14', 0, 15.0, 15.5, 14.8, 15.2, 1000.0, 15200.0, 1.0),
            ('SH601059', '601059', '2026-10-15', 1, 15.2, 15.8, 15.0, 15.5, 2000.0, 31000.0, 1.0)
    """)

    con_v1_m = duckdb.connect(v1_mig)
    con_v1_m.execute("""
        CREATE TABLE stock_daily_feature_snapshot (symbol VARCHAR, trade_date TIMESTAMP, turn10 DOUBLE);
        INSERT INTO stock_daily_feature_snapshot VALUES
            ('SH601059', TIMESTAMP '2026-10-14 00:00:00', 0.1),
            ('SH601059', TIMESTAMP '2026-10-15 00:00:00', NULL);
    """)
    con_v1_m.close()

    class EmptyTdxFetcher:
        def refresh_server_pool(self):
            return {"active": [], "blocked": [], "fallback": False}

        def fetch_finance_info(self, sym):
            return None

        def fetch_xdxr_events(self, sym):
            return []

    monkeypatch.setattr("scripts.add_turn10_and_liutongguben.TdxFetcher", EmptyTdxFetcher)
    total_cnt, upd_cnt = populate_liutongguben(
        con_m, workers=1, only_missing=True, source_db_path=v1_mig
    )
    assert (total_cnt, upd_cnt) == (1, 1)

    inferred_ltgb = con_m.execute("SELECT liutongguben FROM security_info WHERE symbol='SH601059'").fetchone()[0]
    idx_ltgb = con_m.execute("SELECT liutongguben FROM security_info WHERE symbol='SH000001'").fetchone()[0]
    assert np.isclose(inferred_ltgb, 100_000_000.0)
    assert idx_ltgb is None

    # 10-14 对齐 v1 (0.1), 10-15 因 v1.turn10 为 NULL 应由 calc_turn10 自动补算 (1000+2000)*10000/1e8 = 0.3
    changed = backfill_turn10(con_m, source_db_path=v1_mig, batch_size=500)
    assert changed == 2
    t_rows = con_m.execute(
        "SELECT trade_date, turn10 FROM daily_snapshot WHERE symbol='SH601059' ORDER BY trade_date"
    ).fetchall()
    assert np.isclose(t_rows[0][1], 0.1)
    assert np.isclose(t_rows[1][1], 0.3)

    # 10-16 若含次正规浮点 volume 噪点 (5.877e-39) 且历史误写入 turn10，backfill_turn10 应严格洗回 NULL
    con_m.execute("""
        INSERT INTO daily_snapshot (
            symbol, code, trade_date, bar_seq, open, high, low, close, volume, amount, adj_factor, turn10
        ) VALUES ('SH601059', '601059', '2026-10-16', 2, 15.5, 15.5, 15.5, 15.5, 5.877471754111438e-39, 0.0, 1.0, 3.6813e-43)
    """)
    changed_sub = backfill_turn10(con_m, source_db_path=v1_mig, batch_size=500)
    assert changed_sub == 1
    sub_t10 = con_m.execute(
        "SELECT turn10 FROM daily_snapshot WHERE symbol='SH601059' AND trade_date='2026-10-16'"
    ).fetchone()[0]
    con_m.close()
    assert sub_t10 is None








