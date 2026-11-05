# -*- coding: utf-8 -*-
"""
tests/test_updater.py —— 增量更新引擎单元测试与幂等性校验
"""

import datetime
from unittest.mock import MagicMock

import duckdb
import numpy as np
import pandas as pd
import pytest

from src.fetcher import (
    TdxFetcher,
    calculate_next_adj_factor,
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

    # --- 阶段 2：模拟盘后 15:10:00 运行 allow_intraday=False ---
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 10, 0))
    monkeypatch.setattr("src.updater.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 10, 0))

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

    # --- 阶段 1：模拟盘中 11:35:00 运行 allow_intraday=True ---
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 11, 35, 0))
    monkeypatch.setattr("src.updater.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 11, 35, 0))

    mock_fetcher = MagicMock()
    mock_fetcher.get_latest_market_dates.return_value = (
        datetime.date(2026, 10, 15), datetime.date(2026, 10, 16), False
    )
    mock_fetcher.get_latest_market_status.return_value = (datetime.date(2026, 10, 16), False)
    mock_fetcher.fetch_xdxr_events.return_value = event_div
    mock_fetcher.fetch_daily_bars.return_value = []
    mock_fetcher.fetch_minute_bars.return_value = all_16_bars[:8]  # Slot 1~8

    updater = DailyUpdater(db_path=db_file, fetcher=mock_fetcher)
    res_intra = updater.run_update(allow_intraday=True)
    assert res_intra["status"] == "SUCCESS"
    assert res_intra["minute_rows_added"] == 8

    con = duckdb.connect(db_file, read_only=True)
    m_factors_intra = [r[0] for r in con.execute("SELECT adj_factor FROM minute_snapshot WHERE symbol = 'SZ000001'").fetchall()]
    sec_adj_intra = con.execute("SELECT latest_adj_factor FROM security_info WHERE symbol = 'SZ000001'").fetchone()[0]
    ca_cnt_intra = con.execute("SELECT COUNT(*) FROM corporate_action WHERE symbol = 'SZ000001'").fetchone()[0]
    con.close()

    assert len(m_factors_intra) == 8
    for f in m_factors_intra:
        assert abs(f - expected_factor) < 1e-6
    assert abs(sec_adj_intra - expected_factor) < 1e-6
    assert ca_cnt_intra == 1

    # --- 阶段 2：模拟盘后 15:10:00 运行 allow_intraday=False ---
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 10, 0))
    monkeypatch.setattr("src.updater.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 10, 0))

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

    # 2. 模拟北京时间当日 15:00:15 (已走完 15:00:10 缓冲，正式收盘)
    monkeypatch.setattr("src.fetcher.get_cn_now", lambda: datetime.datetime(2026, 10, 16, 15, 0, 15))

    d_date_post, m_date_post, closed_post = fetcher.get_latest_market_dates(allow_intraday=False)
    assert closed_post is True
    assert d_date_post == datetime.date(2026, 10, 16)
    assert m_date_post == datetime.date(2026, 10, 16)



