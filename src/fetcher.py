# -*- coding: utf-8 -*-
"""
src/fetcher.py —— 通达信 (TDX) 高可用行情与除权数据采集器

功能：
1. 内置实测可用的券商自建主站列表，支持自动验活与故障切换。
2. 判定最新完整收盘交易日（默认仅更新已走完 15:00 的交易日）。
3. 支持个股、宽基指数、通达信行业指数（880xxx）的日线与 15 分钟线拉取。
4. 严格对齐 15 分钟线槽位（1 ~ 16）与标准时间戳（09:45:00 ~ 15:00:00）。
5. 采集除权除息信息并计算等比前复权因子推进。
"""

import datetime
from datetime import timezone, timedelta
import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
from pytdx.hq import TdxHq_API

logger = logging.getLogger("fetcher")

CN_TZ = timezone(timedelta(hours=8))


def get_cn_now() -> datetime.datetime:
    """获取当前北京时间（UTC+8 无时区对象，与通达信时间戳完全兼容）"""
    return datetime.datetime.now(CN_TZ).replace(tzinfo=None)


# 实测高可用券商自建主站列表（针对深圳电信宽带全面实测选优，跨多家券商与华南极速节点）
BACKUP_SERVERS: List[Tuple[str, int, str]] = [
    ("139.9.90.169", 7709, "国泰君安-深圳华为云"),   # 深圳本地华为云: 延时 33ms, 总耗时 97ms
    ("39.108.166.99", 7709, "华泰证券-深圳阿里云1"),   # 深圳本地阿里云: 延时 27ms, 总耗时 103ms
    ("47.106.36.3", 7709, "华泰证券-深圳阿里云2"),     # 深圳本地阿里云: 延时 34ms, 总耗时 116ms
    ("222.247.45.186", 7709, "方正证券-核心节点"),    # 华南骨干节点: 延时 26ms, 总耗时 166ms
    ("114.28.173.136", 7709, "国泰君安-广东节点1"),   # 广东电信集群: 延时 50ms, 总耗时 174ms
    ("114.28.173.130", 7709, "国泰君安-广东节点2"),   # 广东电信集群: 延时 48ms, 总耗时 178ms
    ("114.28.173.134", 7709, "国泰君安-广东节点3"),   # 广东电信集群: 延时 50ms, 总耗时 180ms
    ("114.28.173.132", 7709, "国泰君安-广东节点4"),   # 广东电信集群: 延时 51ms, 总耗时 182ms
    ("114.28.173.137", 7709, "国泰君安-广东节点5"),   # 广东电信集群: 延时 48ms, 总耗时 190ms
    ("114.28.173.133", 7709, "国泰君安-广东节点6"),   # 广东电信集群: 延时 48ms, 总耗时 193ms
    ("114.28.173.138", 7709, "国泰君安-广东节点7"),   # 广东电信集群: 延时 48ms, 总耗时 193ms
    ("114.28.173.139", 7709, "国泰君安-广东节点8"),   # 广东电信集群: 延时 51ms, 总耗时 196ms
    ("103.251.85.93", 7709, "国泰君安-多线BGP"),      # 华南BGP多线: 延时 49ms, 总耗时 203ms
    ("114.28.173.140", 7709, "国泰君安-广东节点9"),   # 广东电信集群: 延时 32ms, 总耗时 207ms
]

# 15分钟线标准槽位时间映射 (1 ~ 16)
STANDARD_15M_SLOTS: List[Tuple[int, str]] = [
    (1, "09:45:00"),
    (2, "10:00:00"),
    (3, "10:15:00"),
    (4, "10:30:00"),
    (5, "10:45:00"),
    (6, "11:00:00"),
    (7, "11:15:00"),
    (8, "11:30:00"),
    (9, "13:15:00"),
    (10, "13:30:00"),
    (11, "13:45:00"),
    (12, "14:00:00"),
    (13, "14:15:00"),
    (14, "14:30:00"),
    (15, "14:45:00"),
    (16, "15:00:00"),
]
TIME_TO_SLOT: Dict[str, int] = {t: s for s, t in STANDARD_15M_SLOTS}


def parse_symbol(symbol: str) -> Tuple[int, str, bool]:
    """解析系统 symbol 转换为通达信 market 与 code。
    返回: (market, code, is_index)
    - SH000001: (1, '000001', True)
    - SZ399005: (0, '399005', True)
    - TDX880301: (1, '880301', True)
    - SH600000: (1, '600000', False)
    - SZ000001: (0, '000001', False)
    """
    s = str(symbol).strip().upper()
    if s.startswith("TDX"):
        code = s[3:]
        return 1, code, True
    elif s.startswith("SH"):
        code = s[2:]
        is_idx = code.startswith("000") or code.startswith("001")
        return 1, code, is_idx
    elif s.startswith("SZ"):
        code = s[2:]
        is_idx = code.startswith("399")
        return 0, code, is_idx
    else:
        # 兼容纯数字
        if s.startswith("6"):
            return 1, s, False
        elif s.startswith("880"):
            return 1, s, True
        elif s.startswith("000") and len(s) == 6:
            return 0, s, False
        elif s.startswith("399"):
            return 0, s, True
        else:
            return 0, s, False


class TdxConnectionPool:
    """管理与通达信服务器的连接池与故障切换"""

    _instance = None
    _lock = threading.Lock()
    _rr_counter = 0
    _rr_lock = threading.Lock()

    def __init__(self, servers=None):
        self.servers = list(servers or BACKUP_SERVERS)
        self._local = threading.local()

    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = TdxConnectionPool()
            return cls._instance

    def get_client(self) -> TdxHq_API:
        """获取当前线程绑定的可用 TdxHq_API 实例，失败时自动故障切换"""
        if hasattr(self._local, "api") and self._local.api is not None:
            # 简单心跳验证
            try:
                if self._local.api.client is not None:
                    return self._local.api
            except Exception:
                pass

        # 重新建立连接，通过轮询偏移均匀分散不同工作线程到多台券商主站
        with self._rr_lock:
            start_idx = self._rr_counter % len(self.servers)
            TdxConnectionPool._rr_counter += 1

        ordered_servers = self.servers[start_idx:] + self.servers[:start_idx]
        api = TdxHq_API(heartbeat=False)
        for ip, port, name in ordered_servers:
            try:
                if api.connect(ip, port, time_out=2.0):
                    # 快速验证是否能取到有效测试 K 线数据（过滤假死和空回包节点）
                    test = api.get_security_bars(4, 0, "000001", 0, 1)
                    if test is not None and len(test) > 0:
                        self._local.api = api
                        self._local.server = (ip, port, name)
                        return api
                api.disconnect()
            except Exception:
                try:
                    api.disconnect()
                except Exception:
                    pass

        raise RuntimeError("无法连接任何通达信可用行情服务器，请检查网络！")

    def disconnect_thread(self):
        if hasattr(self._local, "api") and self._local.api is not None:
            try:
                self._local.api.disconnect()
            except Exception:
                pass
            self._local.api = None


class TdxFetcher:
    """通达信行情与除权增量数据采集器"""

    def __init__(self, pool: Optional[TdxConnectionPool] = None):
        self.pool = pool or TdxConnectionPool.get_instance()

    def get_latest_market_dates(
        self, allow_intraday: bool = False
    ) -> Tuple[datetime.date, datetime.date, bool]:
        """识别最新交易日、日线目标日、分钟线目标日及收盘状态。
        通过拉取上证指数 SH000001 的日线与 15 分钟线判定：
        - 若 15 分钟线最后一条达到 15:00:00，则该日完整收盘（is_closed=True）；
        - 若未达到 15:00:00，则处于盘中未收盘状态（is_closed=False）。

        返回: (daily_target_date, minute_target_date, is_closed)
        - daily_target_date: 最新已完整走完 15:00 收盘的交易日（若 is_closed 则为 latest_day；若盘中未收盘，无论 allow_intraday 为何值，必须取前一个已完整收盘交易日 prev_day）。
        - minute_target_date: 若 is_closed 或 allow_intraday 则为 latest_day，否则为 prev_day。
        - is_closed: 当天是否已完整走完 15:00。
        """
        # 上证指数日线与 15 分钟线（使用带重试与故障切换的安全拉取方法）
        cn_now = get_cn_now()
        idx_daily = self.fetch_daily_bars("SH000001", "BROAD_INDEX", offset=5)
        idx_min15 = self.fetch_minute_bars("SH000001", "BROAD_INDEX", offset=16)

        if not idx_daily:
            raise RuntimeError("无法获取上证指数行情以确认市场状态")

        latest_day_str = idx_daily[-1]["datetime"][:10]
        latest_day = datetime.datetime.strptime(latest_day_str, "%Y-%m-%d").date()

        # 检查最新交易日是否已完整收盘（15分钟线最后一条达到 15:00 且当前北京时间已走完 15:00:10 预留缓冲）
        is_closed = False
        market_close_cutoff = datetime.datetime.combine(latest_day, datetime.time(15, 0, 10))
        if idx_min15:
            last_min = idx_min15[-1]
            last_min_dt = last_min["datetime"]
            if (
                last_min_dt.startswith(latest_day_str)
                and (last_min_dt.endswith("15:00") or last_min.get("hour") == 15)
                and (cn_now >= market_close_cutoff)
            ):
                is_closed = True

        prev_day = None
        if len(idx_daily) >= 2:
            prev_day_str = idx_daily[-2]["datetime"][:10]
            prev_day = datetime.datetime.strptime(prev_day_str, "%Y-%m-%d").date()
        else:
            prev_day = latest_day

        if is_closed:
            daily_target_date = latest_day
            minute_target_date = latest_day
        else:
            daily_target_date = prev_day
            minute_target_date = latest_day if allow_intraday else prev_day

        return daily_target_date, minute_target_date, is_closed

    def get_latest_market_status(
        self, allow_intraday: bool = False
    ) -> Tuple[datetime.date, bool]:
        """识别最新交易日及收盘状态（保留兼容旧接口）。
        返回: (minute_target_date, is_closed)
        """
        _, minute_target_date, is_closed = self.get_latest_market_dates(allow_intraday)
        return minute_target_date, is_closed

    def fetch_daily_bars(
        self, symbol: str, sec_type: str, offset: int = 15
    ) -> List[Dict]:
        """获取标的最新日线行情。
        返回原始 dict 列表，字段包含 open, high, low, close, vol, amount, datetime 等
        """
        market, code, is_idx = parse_symbol(symbol)
        for retry in range(2):
            try:
                api = self.pool.get_client()
                if is_idx or sec_type in ("BROAD_INDEX", "INDUSTRY_INDEX"):
                    bars = api.get_index_bars(4, market, code, 0, offset)
                else:
                    bars = api.get_security_bars(4, market, code, 0, offset)
                if bars is not None and len(bars) > 0:
                    return bars
                if not bars and retry == 0:
                    self.pool.disconnect_thread()
                    continue
                return bars or []
            except Exception:
                self.pool.disconnect_thread()
                if retry == 1:
                    logger.warning(f"{symbol} 拉取日线失败")
                    return []
        return []

    def fetch_minute_bars(
        self, symbol: str, sec_type: str, offset: int = 48
    ) -> List[Dict]:
        """获取标的最新 15 分钟线行情。
        通达信 category: 1 为 15 分钟线
        """
        market, code, is_idx = parse_symbol(symbol)
        for retry in range(2):
            try:
                api = self.pool.get_client()
                if is_idx or sec_type in ("BROAD_INDEX", "INDUSTRY_INDEX"):
                    bars = api.get_index_bars(1, market, code, 0, offset)
                else:
                    bars = api.get_security_bars(1, market, code, 0, offset)
                if bars is not None and len(bars) > 0:
                    return bars
                if not bars and retry == 0:
                    self.pool.disconnect_thread()
                    continue
                return bars or []
            except Exception:
                self.pool.disconnect_thread()
                if retry == 1:
                    logger.warning(f"{symbol} 拉取分钟线失败")
                    return []
        return []

    def fetch_xdxr_events(self, symbol: str) -> List[Dict]:
        """获取除权除息及股本变动事件（带 2 次重试与断开保护）"""
        market, code, is_idx = parse_symbol(symbol)
        if is_idx:
            return []
        for retry in range(2):
            try:
                api = self.pool.get_client()
                items = api.get_xdxr_info(market, code)
                if items is None and retry == 0:
                    self.pool.disconnect_thread()
                    continue
                return items or []
            except Exception as e:
                self.pool.disconnect_thread()
                if retry == 1:
                    logger.warning(f"{symbol} 拉取除权除息失败: {e}")
                    return []
        return []


def is_stock_symbol(symbol: str) -> bool:
    """判定是否为个股（非指数）。个股成交量通达信以股返回，需除以 100 换算为手；指数无需除以 100。"""
    s = str(symbol).strip().upper()
    if s.startswith("TDX") or s.startswith("SH000") or s.startswith("SH001") or s.startswith("SZ399"):
        return False
    if s.startswith("880") or s.startswith("399"):
        return False
    return True


def parse_daily_bars_to_df(
    symbol: str, raw_bars: List[Dict], min_trade_date: Optional[datetime.date] = None
) -> pd.DataFrame:
    """将通达信日线原始列表解析为 DataFrame，并按日期升序排列。
    若传入 min_trade_date，则仅保留 trade_date > min_trade_date 的行。
    """
    if not raw_bars:
        return pd.DataFrame()

    vol_divider = 100.0 if is_stock_symbol(symbol) else 1.0

    records = []
    for b in raw_bars:
        dt_str = b["datetime"][:10]
        t_date = datetime.datetime.strptime(dt_str, "%Y-%m-%d").date()
        if min_trade_date is not None and t_date <= min_trade_date:
            continue
        records.append({
            "trade_date": t_date,
            "open": float(b["open"]),
            "high": float(b["high"]),
            "low": float(b["low"]),
            "close": float(b["close"]),
            "volume": float(b["vol"]) / vol_divider,
            "amount": float(b["amount"]),
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = df.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last").reset_index(drop=True)
    return df


def parse_minute_bars_to_df(
    symbol: str,
    raw_bars: List[Dict],
    min_trade_date: Optional[datetime.date] = None,
    min_slot_seq: Optional[int] = None,
) -> pd.DataFrame:
    """将通达信 15 分钟线解析为标准 16 槽位 DataFrame。
    返回字段包含: trading_date, bar_end, slot_seq, open, high, low, close, volume, amount
    """
    if not raw_bars:
        return pd.DataFrame()

    vol_divider = 100.0 if is_stock_symbol(symbol) else 1.0

    records = []
    for b in raw_bars:
        dt_str = b["datetime"]  # '2026-11-03 09:45'
        parts = dt_str.split()
        if len(parts) != 2:
            continue
        d_str, t_str = parts[0], parts[1]
        t_date = datetime.datetime.strptime(d_str, "%Y-%m-%d").date()

        # 对齐时间字符串为 HH:MM:00
        if len(t_str) == 5:
            t_str = t_str + ":00"

        slot = TIME_TO_SLOT.get(t_str)
        if slot is None:
            # 容错：通达信部分行情站将上午收盘 11:30:00 的 15 分钟线标记为 13:00:00，归入 Slot 8
            if t_str == "13:00:00":
                t_str = "11:30:00"
                slot = 8
            else:
                continue

        # 过滤已入库的 slot
        if min_trade_date is not None:
            if t_date < min_trade_date:
                continue
            if t_date == min_trade_date and min_slot_seq is not None and slot <= min_slot_seq:
                continue

        bar_end_dt = datetime.datetime.strptime(f"{d_str} {t_str}", "%Y-%m-%d %H:%M:%S")

        records.append({
            "trading_date": t_date,
            "bar_end": bar_end_dt,
            "slot_seq": slot,
            "open": float(b["open"]),
            "high": float(b["high"]),
            "low": float(b["low"]),
            "close": float(b["close"]),
            "volume": float(b["vol"]) / vol_divider,
            "amount": float(b["amount"]),
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    # 严格按 trading_date, slot_seq 排序并去重（防止个别主站同时返回 11:30 与 13:00 导致 Slot 8 重复）
    df = df.sort_values(["trading_date", "slot_seq"]).drop_duplicates(subset=["trading_date", "slot_seq"], keep="last").reset_index(drop=True)

    # 自动剔除盘中当前正在跳动、尚未收完的当根 15m K 线（预留 10 秒安全缓冲）
    cutoff_time = get_cn_now() - timedelta(seconds=10)
    df = df[df["bar_end"] <= cutoff_time].reset_index(drop=True)
    return df


def calculate_next_adj_factor(
    prev_adj_factor: float,
    prev_close: float,
    event: Optional[Dict],
) -> float:
    """根据除权事件计算新复权因子。
    等比前复权公式:
    new_factor = prev_factor * (prev_close / ex_right_ref_price)
    """
    if not event or prev_close <= 0:
        return prev_adj_factor

    fenhong = float(event.get("fenhong") or 0.0)      # 每10股派息
    songzhuan = float(event.get("songzhuangu") or 0.0) # 每10股送转股
    peigu = float(event.get("peigu") or 0.0)          # 每10股配股
    peigujia = float(event.get("peigujia") or 0.0)    # 配股价

    d = fenhong / 10.0
    s = songzhuan / 10.0
    p = peigu / 10.0

    denom = 1.0 + s + p
    if denom <= 0:
        return prev_adj_factor

    ref_price = (prev_close - d + p * peigujia) / denom
    if ref_price <= 0.0001:
        return prev_adj_factor

    ratio = prev_close / ref_price
    return prev_adj_factor * ratio
