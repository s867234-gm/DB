import pytest
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from src.kdj_engine import MultiCycleKDJEngine, PeriodState, DAILY_PERIOD_NAMES, MINUTE_PERIOD_NAMES


def generate_mock_daily_data(n_bars: int = 200, seed: int = 42) -> pd.DataFrame:
    """生成模拟日线数据"""
    np.random.seed(seed)
    start_date = date(2020, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(n_bars)]
    
    close = 10.0 + np.cumsum(np.random.randn(n_bars) * 0.2)
    high = close + np.random.rand(n_bars) * 0.3
    low = close - np.random.rand(n_bars) * 0.3
    open_p = low + np.random.rand(n_bars) * (high - low)
    volume = np.random.randint(1000, 100000, size=n_bars).astype(float)
    amount = volume * close
    adj_factor = np.ones(n_bars, dtype=float)

    df = pd.DataFrame({
        'symbol': ['SZ000002'] * n_bars,
        'code': ['000002'] * n_bars,
        'trade_date': dates,
        'bar_seq': np.arange(n_bars, dtype=np.int32),
        'open': open_p,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
        'amount': amount,
        'adj_factor': adj_factor
    })
    return df


def generate_mock_minute_data(n_days: int = 10, seed: int = 42) -> pd.DataFrame:
    """生成模拟分钟线数据 (每天 16 个 slot)"""
    np.random.seed(seed)
    n_bars = n_days * 16
    start_date = date(2024, 9, 20)
    
    rows = []
    base_price = 20.0
    for d in range(n_days):
        t_date = start_date + timedelta(days=d)
        for s in range(1, 17):
            hour = 9 + (s * 15) // 60
            minute = (s * 15) % 60
            b_end = datetime(t_date.year, t_date.month, t_date.day, hour, minute)
            
            c = base_price + np.random.randn() * 0.1
            h = c + np.random.rand() * 0.1
            l = c - np.random.rand() * 0.1
            o = l + np.random.rand() * (h - l)
            v = float(np.random.randint(100, 10000))
            a = v * c
            rows.append({
                'symbol': 'SH600000',
                'code': '600000',
                'trading_date': t_date,
                'bar_end': b_end,
                'slot_seq': s,
                'open': o,
                'high': h,
                'low': l,
                'close': c,
                'volume': v,
                'amount': a,
                'adj_factor': 1.0
            })
            base_price = c

    return pd.DataFrame(rows)


def test_rule_a_unclosed_and_closed_logic():
    """测试规则 A: 验证未闭合动态快照与已闭合桶前值锚定"""
    # 构造精确已知行情验证 2d 周期
    # bar 0: (0+1)%2 != 0 未闭合, 前值取初值 50.0
    # bar 1: (1+1)%2 == 0 闭合, 前值仍取初值 50.0, 计算定型后作为下一桶前值
    # bar 2: (2+1)%2 != 0 未闭合, 前值严格取 bar 1 闭合定型值
    df = pd.DataFrame({
        'symbol': ['TEST'] * 4,
        'code': ['000001'] * 4,
        'trade_date': [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
        'bar_seq': np.arange(4, dtype=np.int32),
        'open': [10.0, 11.0, 12.0, 13.0],
        'high': [10.5, 11.5, 12.5, 13.5],
        'low': [9.5, 10.5, 11.5, 12.5],
        'close': [10.2, 11.2, 12.2, 13.2],
        'volume': [100.0] * 4,
        'amount': [1000.0] * 4,
        'adj_factor': [1.0] * 4
    })

    df_out, states = MultiCycleKDJEngine.compute_daily_snapshot(df)
    
    # 检验 2d 周期
    state_2d = states['2d']
    assert state_2d.last_closed_bucket_id == 1  # 4 根线共有 2 个已闭合桶 (id: 0, 1)
    assert len(state_2d.closed_highs_adj) == 2
    # 桶 0 (bar 0 & bar 1) 高点: max(10.5, 11.5) = 11.5, 低点: min(9.5, 10.5) = 9.5
    assert state_2d.closed_highs_adj[0] == 11.5
    assert state_2d.closed_lows_adj[0] == 9.5
    # 桶 1 (bar 2 & bar 3) 高点: 13.5, 低点: 11.5
    assert state_2d.closed_highs_adj[1] == 13.5
    assert state_2d.closed_lows_adj[1] == 11.5

    # 验证 bar 0 与 bar 1 引用的都是初始前值 50.0
    # 在 bar 0: HHV=10.5, LLV=9.5, C=10.2 -> RSV13 = (10.2-9.5)/(10.5-9.5)*100 = 70.0
    # SK = 0.5 * 50.0 + 0.5 * 70.0 = 60.0
    assert np.isclose(df_out['sk_2d'].iloc[0], 60.0)


def test_daily_incremental_matches_full():
    """测试日线全量计算与分段增量计算的 100% 精度一致性"""
    df = generate_mock_daily_data(n_bars=300, seed=123)

    # 1. 全量计算
    df_full, full_states = MultiCycleKDJEngine.compute_daily_snapshot(df)

    # 2. 分段增量计算 (前 200 根 + 后 100 根)
    split = 200
    df_part1, states_part1 = MultiCycleKDJEngine.compute_daily_snapshot(df.iloc[:split])
    df_part2, states_part2 = MultiCycleKDJEngine.compute_daily_snapshot(df.iloc[split:], init_states=states_part1)

    df_inc = pd.concat([df_part1, df_part2], ignore_index=True)

    # 检查 6 个周期的全部 48 列指标差值
    for p_name in DAILY_PERIOD_NAMES:
        for metric in ['sk', 'sd', 'skxl', 'sdxl', 'mk', 'md', 'mkxl', 'mdxl']:
            col = f"{metric}_{p_name}"
            diff = np.max(np.abs(df_full[col].to_numpy() - df_inc[col].to_numpy()))
            assert diff < 1e-12, f"Discrepancy in {col}: max_diff={diff}"

        # 检查最新状态是否完全一致
        s_full = full_states[p_name]
        s_inc = states_part2[p_name]
        assert s_full.last_closed_bucket_id == s_inc.last_closed_bucket_id
        assert np.isclose(s_full.sk_closed, s_inc.sk_closed)
        assert np.isclose(s_full.sd_closed, s_inc.sd_closed)
        assert np.isclose(s_full.mk_closed, s_inc.mk_closed)
        assert np.isclose(s_full.md_closed, s_inc.md_closed)
        assert s_full.bar_count == s_inc.bar_count


def test_minute_incremental_matches_full():
    """测试分钟线全量计算与分段增量计算的一致性"""
    df = generate_mock_minute_data(n_days=15, seed=456)

    # 全量
    df_full, full_states = MultiCycleKDJEngine.compute_minute_snapshot(df, cutoff_date=None)

    # 分段 (前 10 天 160 根 + 后 5 天 80 根)
    split = 160
    df_p1, s_p1 = MultiCycleKDJEngine.compute_minute_snapshot(df.iloc[:split], cutoff_date=None)
    df_p2, s_p2 = MultiCycleKDJEngine.compute_minute_snapshot(df.iloc[split:], cutoff_date=None, init_states=s_p1)

    df_inc = pd.concat([df_p1, df_p2], ignore_index=True)

    for p_name in MINUTE_PERIOD_NAMES:
        for metric in ['sk', 'sd', 'skxl', 'sdxl', 'mk', 'md', 'mkxl', 'mdxl']:
            col = f"{metric}_{p_name}"
            diff = np.max(np.abs(df_full[col].to_numpy() - df_inc[col].to_numpy()))
            assert diff < 1e-12, f"Minute discrepancy in {col}: max_diff={diff}"


def test_period_state_serialization():
    """测试增量状态序列化与反序列化"""
    state = PeriodState(
        symbol='SH600000',
        period='1d',
        last_closed_bucket_id=10,
        closed_highs_adj=[10.5, 11.2, 12.0],
        closed_lows_adj=[9.8, 10.1, 11.0],
        sk_closed=65.4321,
        sd_closed=62.1234,
        mk_closed=55.6789,
        md_closed=54.3210,
        last_valid_rsv13=70.0,
        last_valid_rsv104=58.0,
        bar_count=0,
        high_adj=None,
        low_adj=None
    )
    row = state.to_db_row(updated_at=datetime.now())
    restored = PeriodState.from_db_row(row)

    assert restored.symbol == state.symbol
    assert restored.period == state.period
    assert restored.last_closed_bucket_id == state.last_closed_bucket_id
    assert restored.closed_highs_adj == state.closed_highs_adj
    assert restored.closed_lows_adj == state.closed_lows_adj
    assert np.isclose(restored.sk_closed, state.sk_closed)
    assert np.isclose(restored.sd_closed, state.sd_closed)
    assert np.isclose(restored.mk_closed, state.mk_closed)
    assert np.isclose(restored.md_closed, state.md_closed)
