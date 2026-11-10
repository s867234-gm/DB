import json
import numpy as np
import pandas as pd
import numba as nb
from typing import Dict, Tuple, List, Optional
from dataclasses import dataclass, asdict

# 基础与指标列定义常量
DAILY_PERIODS = [1, 2, 4, 8, 16, 32, 64]
DAILY_PERIOD_NAMES = ['1d', '2d', '4d', '8d', '16d', '32d', '64d']

MINUTE_PERIODS = [1, 2, 4, 8]  # 对应 slot 聚合步长: 1(15m), 2(30m), 4(60m), 8(120m)
MINUTE_PERIOD_NAMES = ['15m', '30m', '60m', '120m']

METRIC_COLS_TEMPLATE = ['sk_{p}', 'sd_{p}', 'skxl_{p}', 'sdxl_{p}', 'mk_{p}', 'md_{p}', 'mkxl_{p}', 'mdxl_{p}']

# 严格匹配 schema.py 的物理表列顺序定义
DAILY_SNAPSHOT_COLUMNS = [
    'symbol', 'code', 'trade_date', 'bar_seq', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adj_factor'
] + [tmpl.format(p=p) for p in DAILY_PERIOD_NAMES for tmpl in METRIC_COLS_TEMPLATE]

MINUTE_SNAPSHOT_COLUMNS = [
    'symbol', 'code', 'trading_date', 'bar_end', 'slot_seq', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adj_factor'
] + [tmpl.format(p=p) for p in MINUTE_PERIOD_NAMES for tmpl in METRIC_COLS_TEMPLATE]


@dataclass
class PeriodState:
    """单个周期的增量计算状态"""
    symbol: str
    period: str
    last_closed_bucket_id: int
    closed_highs_adj: List[float]  # 最多 104 个已闭合桶的最高价
    closed_lows_adj: List[float]   # 最多 104 个已闭合桶的最低价
    sk_closed: float
    sd_closed: float
    mk_closed: float
    md_closed: float
    last_valid_rsv13: float
    last_valid_rsv104: float
    bar_count: int
    high_adj: Optional[float]
    low_adj: Optional[float]

    @classmethod
    def initial(cls, symbol: str, period: str) -> "PeriodState":
        return cls(
            symbol=symbol,
            period=period,
            last_closed_bucket_id=-1,
            closed_highs_adj=[],
            closed_lows_adj=[],
            sk_closed=50.0,
            sd_closed=50.0,
            mk_closed=50.0,
            md_closed=50.0,
            last_valid_rsv13=50.0,
            last_valid_rsv104=50.0,
            bar_count=0,
            high_adj=None,
            low_adj=None
        )

    def to_db_row(self, updated_at=None) -> tuple:
        """转换为 incremental_state 表的行元组"""
        return (
            self.symbol,
            self.period,
            int(self.last_closed_bucket_id),
            json.dumps(self.closed_highs_adj),
            json.dumps(self.closed_lows_adj),
            float(self.sk_closed),
            float(self.sd_closed),
            float(self.mk_closed),
            float(self.md_closed),
            float(self.last_valid_rsv13),
            float(self.last_valid_rsv104),
            int(self.bar_count),
            float(self.high_adj) if self.high_adj is not None else None,
            float(self.low_adj) if self.low_adj is not None else None,
            updated_at
        )

    @classmethod
    def from_db_row(cls, row) -> "PeriodState":
        """从 incremental_state 表的行记录还原"""
        return cls(
            symbol=row[0],
            period=row[1],
            last_closed_bucket_id=row[2],
            closed_highs_adj=json.loads(row[3]) if row[3] else [],
            closed_lows_adj=json.loads(row[4]) if row[4] else [],
            sk_closed=row[5],
            sd_closed=row[6],
            mk_closed=row[7],
            md_closed=row[8],
            last_valid_rsv13=row[9],
            last_valid_rsv104=row[10],
            bar_count=row[11],
            high_adj=row[12],
            low_adj=row[13]
        )


def restore_state_from_db(con, symbol: str) -> Dict[str, PeriodState]:
    """从数据库 incremental_state 还原指定标的的全套 PeriodState"""
    rows = con.execute("""
        SELECT symbol, period, last_closed_bucket_id, closed_highs_adj, closed_lows_adj,
               sk_closed, sd_closed, mk_closed, md_closed, last_valid_rsv13, last_valid_rsv104,
               bar_count, high_adj, low_adj, updated_at
        FROM incremental_state
        WHERE symbol = ?
    """, [symbol]).fetchall()
    states = {}
    for r in rows:
        st = PeriodState.from_db_row(r)
        states[st.period] = st
    return states


def states_to_df(states: List[PeriodState], updated_at=None) -> pd.DataFrame:
    """将 PeriodState 列表转换为 incremental_state DataFrame（显式指定 dtype 避免拼接时产生 FutureWarning）"""
    cols = [
        "symbol", "period", "last_closed_bucket_id", "closed_highs_adj", "closed_lows_adj",
        "sk_closed", "sd_closed", "mk_closed", "md_closed", "last_valid_rsv13", "last_valid_rsv104",
        "bar_count", "high_adj", "low_adj", "updated_at"
    ]
    if not states:
        return pd.DataFrame(columns=cols)
    data = [st.to_db_row(updated_at=updated_at) for st in states]
    df = pd.DataFrame(data, columns=cols)
    # 显式指定数值列类型
    df["last_closed_bucket_id"] = df["last_closed_bucket_id"].astype("int64")
    df["bar_count"] = df["bar_count"].astype("int32")
    for float_col in ["sk_closed", "sd_closed", "mk_closed", "md_closed", "last_valid_rsv13", "last_valid_rsv104", "high_adj", "low_adj"]:
        df[float_col] = pd.to_numeric(df[float_col], errors="coerce").astype("float64")
    return df



@nb.njit(fastmath=True)
def _compute_period_kdj_numba(
    high_adj: np.ndarray,
    low_adj: np.ndarray,
    close_adj: np.ndarray,
    is_closed_flags: np.ndarray,
    init_closed_highs: np.ndarray,
    init_closed_lows: np.ndarray,
    init_sk_closed: float,
    init_sd_closed: float,
    init_mk_closed: float,
    init_md_closed: float,
    init_rsv13: float,
    init_rsv104: float,
    init_bar_count: int,
    init_h_curr: float,
    init_l_curr: float,
    init_last_closed_bucket_id: int
):
    """
    Numba JIT 底层核心: 严格执行规则 A（动态快照 + 已闭合桶前值锚定）
    """
    n = len(close_adj)
    out = np.empty((n, 8), dtype=np.float64)

    closed_highs = np.empty(104, dtype=np.float64)
    closed_lows = np.empty(104, dtype=np.float64)
    
    n_init = len(init_closed_highs)
    k_closed = 0
    for i in range(n_init):
        closed_highs[i] = init_closed_highs[i]
        closed_lows[i] = init_closed_lows[i]
        k_closed += 1

    sk_closed = init_sk_closed
    sd_closed = init_sd_closed
    mk_closed = init_mk_closed
    md_closed = init_md_closed

    last_rsv13 = init_rsv13
    last_rsv104 = init_rsv104

    bar_cnt = init_bar_count
    h_curr = init_h_curr
    l_curr = init_l_curr
    last_bucket_id = init_last_closed_bucket_id

    for t in range(n):
        h_t = high_adj[t]
        l_t = low_adj[t]
        c_t = close_adj[t]

        # 累积当前桶复权高低点
        if bar_cnt == 0:
            h_curr = h_t
            l_curr = l_t
        else:
            if h_t > h_curr:
                h_curr = h_t
            if l_t < l_curr:
                l_curr = l_t
        bar_cnt += 1

        # 1. 13 周期极值 (最多 12 个历史已闭合桶 + 当前桶实时高低)
        hhv13 = h_curr
        llv13 = l_curr
        m13 = min(k_closed, 12)
        start_idx13 = k_closed - m13
        for idx in range(start_idx13, k_closed):
            if closed_highs[idx] > hhv13:
                hhv13 = closed_highs[idx]
            if closed_lows[idx] < llv13:
                llv13 = closed_lows[idx]

        # 2. 104 周期极值 (最多 103 个历史已闭合桶 + 当前桶实时高低)
        hhv104 = h_curr
        llv104 = l_curr
        m104 = min(k_closed, 103)
        start_idx104 = k_closed - m104
        for idx in range(start_idx104, k_closed):
            if closed_highs[idx] > hhv104:
                hhv104 = closed_highs[idx]
            if closed_lows[idx] < llv104:
                llv104 = closed_lows[idx]

        # 3. 计算 RSV13
        if hhv13 == llv13:
            if k_closed == 0:
                rsv13 = 50.0
            else:
                rsv13 = last_rsv13
        else:
            rsv13 = (c_t - llv13) / (hhv13 - llv13) * 100.0

        # 4. 计算 RSV104
        if hhv104 == llv104:
            if k_closed == 0:
                rsv104 = 50.0
            else:
                rsv104 = last_rsv104
        else:
            rsv104 = (c_t - llv104) / (hhv104 - llv104) * 100.0

        # 5. 递推计算当前动态快照 (前值严格锚定上一个已闭合桶定型值)
        sk = 0.5 * sk_closed + 0.5 * rsv13
        sd = 0.5 * sd_closed + 0.5 * sk
        skxl = sk - sk_closed
        sdxl = sd - sd_closed

        mk = (15.0 / 16.0) * mk_closed + (1.0 / 16.0) * rsv104
        md = (15.0 / 16.0) * md_closed + (1.0 / 16.0) * mk
        mkxl = (mk - mk_closed) * 8.0
        mdxl = (md - md_closed) * 8.0

        out[t, 0] = sk
        out[t, 1] = sd
        out[t, 2] = skxl
        out[t, 3] = sdxl
        out[t, 4] = mk
        out[t, 5] = md
        out[t, 6] = mkxl
        out[t, 7] = mdxl

        # 6. 检查是否触发闭合
        if is_closed_flags[t]:
            # 桶正式定型，更新已闭合定型值
            sk_closed = sk
            sd_closed = sd
            mk_closed = mk
            md_closed = md
            last_rsv13 = rsv13
            last_rsv104 = rsv104

            # 将当前桶高低点推入闭合历史窗口 (容量 104)
            if k_closed < 104:
                closed_highs[k_closed] = h_curr
                closed_lows[k_closed] = l_curr
                k_closed += 1
            else:
                for s in range(103):
                    closed_highs[s] = closed_highs[s + 1]
                    closed_lows[s] = closed_lows[s + 1]
                closed_highs[103] = h_curr
                closed_lows[103] = l_curr

            last_bucket_id += 1
            bar_cnt = 0
            h_curr = 0.0
            l_curr = 0.0

    ret_highs = closed_highs[:k_closed].copy()
    ret_lows = closed_lows[:k_closed].copy()

    return (
        out,
        ret_highs,
        ret_lows,
        sk_closed,
        sd_closed,
        mk_closed,
        md_closed,
        last_rsv13,
        last_rsv104,
        bar_cnt,
        h_curr,
        l_curr,
        last_bucket_id
    )


class MultiCycleKDJEngine:
    """多周期合成与 KDJ 计算引擎"""

    @staticmethod
    def compute_daily_snapshot(
        df_daily: pd.DataFrame,
        init_states: Optional[Dict[str, PeriodState]] = None
    ) -> Tuple[pd.DataFrame, Dict[str, PeriodState]]:
        """
        计算日线全历史 7 个周期 (1d, 2d, 4d, 8d, 16d, 32d, 64d) 快照与最新状态
        """
        if df_daily.empty:
            return pd.DataFrame(columns=DAILY_SNAPSHOT_COLUMNS), (init_states or {})

        df = df_daily.copy()
        if 'bar_seq' not in df.columns:
            df['bar_seq'] = np.arange(len(df), dtype=np.int32)

        # 确保复权列存在
        adj_factor = df['adj_factor'].to_numpy(dtype=np.float64)
        high_adj = df['high'].to_numpy(dtype=np.float64) * adj_factor
        low_adj = df['low'].to_numpy(dtype=np.float64) * adj_factor
        close_adj = df['close'].to_numpy(dtype=np.float64) * adj_factor
        bar_seqs = df['bar_seq'].to_numpy(dtype=np.int32)

        symbol = df['symbol'].iloc[0] if len(df) > 0 else ""
        out_states: Dict[str, PeriodState] = {}

        for n, p_name in zip(DAILY_PERIODS, DAILY_PERIOD_NAMES):
            # 桶闭合标志: (bar_seq + 1) % n == 0
            is_closed = ((bar_seqs + 1) % n == 0)

            state = (init_states.get(p_name) if init_states and p_name in init_states
                     else PeriodState.initial(symbol, p_name))

            init_highs = np.array(state.closed_highs_adj, dtype=np.float64)
            init_lows = np.array(state.closed_lows_adj, dtype=np.float64)
            init_h = state.high_adj if state.high_adj is not None else 0.0
            init_l = state.low_adj if state.low_adj is not None else 0.0

            res = _compute_period_kdj_numba(
                high_adj, low_adj, close_adj, is_closed,
                init_highs, init_lows,
                state.sk_closed, state.sd_closed, state.mk_closed, state.md_closed,
                state.last_valid_rsv13, state.last_valid_rsv104,
                state.bar_count, init_h, init_l,
                state.last_closed_bucket_id
            )

            metrics_arr = res[0]
            col_names = [c.format(p=p_name) for c in METRIC_COLS_TEMPLATE]
            for c_idx, c_name in enumerate(col_names):
                df[c_name] = metrics_arr[:, c_idx]

            # 保存最新状态
            out_states[p_name] = PeriodState(
                symbol=symbol,
                period=p_name,
                last_closed_bucket_id=res[12],
                closed_highs_adj=res[1].tolist(),
                closed_lows_adj=res[2].tolist(),
                sk_closed=res[3],
                sd_closed=res[4],
                mk_closed=res[5],
                md_closed=res[6],
                last_valid_rsv13=res[7],
                last_valid_rsv104=res[8],
                bar_count=res[9],
                high_adj=res[10] if res[9] > 0 else None,
                low_adj=res[11] if res[9] > 0 else None
            )

        # 严格重排字段顺序，完全契合 schema 物理表定义，杜绝任何错位与冗余临时列
        df = df[DAILY_SNAPSHOT_COLUMNS].copy()
        return df, out_states

    @staticmethod
    def compute_minute_snapshot(
        df_minute: pd.DataFrame,
        cutoff_date: Optional[str] = '2024-10-01',
        init_states: Optional[Dict[str, PeriodState]] = None
    ) -> Tuple[pd.DataFrame, Dict[str, PeriodState]]:
        """
        计算分钟线 4 个周期 (15m, 30m, 60m, 120m) 快照与最新状态
        输入数据通常从 2024-09-06 起预热，最终截取 trading_date >= cutoff_date 输出
        """
        if df_minute.empty:
            return pd.DataFrame(columns=MINUTE_SNAPSHOT_COLUMNS), (init_states or {})

        df = df_minute.copy()
        adj_factor = df['adj_factor'].to_numpy(dtype=np.float64)
        high_adj = df['high'].to_numpy(dtype=np.float64) * adj_factor
        low_adj = df['low'].to_numpy(dtype=np.float64) * adj_factor
        close_adj = df['close'].to_numpy(dtype=np.float64) * adj_factor
        slot_seqs = df['slot_seq'].to_numpy(dtype=np.int32)

        symbol = df['symbol'].iloc[0] if len(df) > 0 else ""
        out_states: Dict[str, PeriodState] = {}

        for n, p_name in zip(MINUTE_PERIODS, MINUTE_PERIOD_NAMES):
            # 分钟线桶闭合标志: 15m(每1slot), 30m(slot%2==0), 60m(slot%4==0), 120m(slot%8==0)
            is_closed = (slot_seqs % n == 0)

            state = (init_states.get(p_name) if init_states and p_name in init_states
                     else PeriodState.initial(symbol, p_name))

            init_highs = np.array(state.closed_highs_adj, dtype=np.float64)
            init_lows = np.array(state.closed_lows_adj, dtype=np.float64)
            init_h = state.high_adj if state.high_adj is not None else 0.0
            init_l = state.low_adj if state.low_adj is not None else 0.0

            res = _compute_period_kdj_numba(
                high_adj, low_adj, close_adj, is_closed,
                init_highs, init_lows,
                state.sk_closed, state.sd_closed, state.mk_closed, state.md_closed,
                state.last_valid_rsv13, state.last_valid_rsv104,
                state.bar_count, init_h, init_l,
                state.last_closed_bucket_id
            )

            metrics_arr = res[0]
            col_names = [c.format(p=p_name) for c in METRIC_COLS_TEMPLATE]
            for c_idx, c_name in enumerate(col_names):
                df[c_name] = metrics_arr[:, c_idx]

            out_states[p_name] = PeriodState(
                symbol=symbol,
                period=p_name,
                last_closed_bucket_id=res[12],
                closed_highs_adj=res[1].tolist(),
                closed_lows_adj=res[2].tolist(),
                sk_closed=res[3],
                sd_closed=res[4],
                mk_closed=res[5],
                md_closed=res[6],
                last_valid_rsv13=res[7],
                last_valid_rsv104=res[8],
                bar_count=res[9],
                high_adj=res[10] if res[9] > 0 else None,
                low_adj=res[11] if res[9] > 0 else None
            )

        # 截取 >= cutoff_date 的行返回，并严格按 schema 物理表定义重排字段
        if cutoff_date:
            cutoff_ts = pd.to_datetime(cutoff_date)
            df = df[pd.to_datetime(df['trading_date']) >= cutoff_ts].copy()

        df = df[MINUTE_SNAPSHOT_COLUMNS].copy()
        return df, out_states
