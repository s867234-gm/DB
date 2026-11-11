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

from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
from datetime import timezone, timedelta
import logging
import math
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


# 实测高可用券商自建主站列表（全量 171 台节点 5 项校验+5 股连发压测优选，跨 9 个 /24 网段，同子网上限 4 台）
BACKUP_SERVERS: List[Tuple[str, int, str]] = [
    ("139.9.50.246", 7709, "国泰君安-深圳华为云4"),    # 网段 139.9.50 (连发均值 13.5ms)
    ("139.9.43.31", 7709, "国泰君安-深圳华为云3"),     # 网段 139.9.43 (连发均值 14.5ms)
    ("139.9.43.104", 7709, "国泰君安-深圳华为云2"),    # 网段 139.9.43 (连发均值 15.8ms)
    ("139.9.90.169", 7709, "国泰君安-深圳华为云1"),    # 网段 139.9.90 (连发均值 16.2ms)
    ("222.247.45.186", 7709, "方正证券-核心节点"),     # 网段 222.247.45 (连发均值 24.7ms)
    ("124.232.133.136", 7709, "方正证券-长沙多线"),    # 网段 124.232.133 (连发均值 25.8ms)
    ("114.28.173.130", 7709, "国泰君安-广东节点1"),    # 网段 114.28.173 (连发均值 31.8ms, 同网段优选前4台)
    ("114.28.173.138", 7709, "国泰君安-广东节点2"),    # 网段 114.28.173 (连发均值 32.9ms)
    ("114.28.173.133", 7709, "国泰君安-广东节点3"),    # 网段 114.28.173 (连发均值 33.2ms)
    ("114.28.173.140", 7709, "国泰君安-广东节点4"),    # 网段 114.28.173 (连发均值 34.0ms)
    ("148.70.31.16", 7709, "国泰君安-成都节点2"),      # 网段 148.70.31 (连发均值 37.8ms)
    ("148.70.93.117", 7709, "国泰君安-成都节点1"),     # 网段 148.70.93 (连发均值 38.1ms)
    ("103.251.85.93", 7709, "国泰君安-多线BGP1"),      # 网段 103.251.85 (连发均值 50.0ms)
    ("103.251.85.94", 7709, "国泰君安-多线BGP2"),      # 网段 103.251.85 (连发均值 63.7ms)
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
        self._all_servers: List[Tuple[str, int, str]] = list(servers or BACKUP_SERVERS)
        self.servers: List[Tuple[str, int, str]] = list(self._all_servers)
        self._local = threading.local()
        self._pool_epoch = 0
        self._rr_counter = 0
        self._rr_lock = threading.Lock()
        self._fail_counts: Dict[Tuple[str, int, str], int] = {}

    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = TdxConnectionPool()
            return cls._instance

    @staticmethod
    def _subnet24(ip_addr: str) -> str:
        parts = ip_addr.split(".")
        return ".".join(parts[:3]) if len(parts) == 4 else ip_addr

    @staticmethod
    def _is_test_ip(ip_addr: str) -> bool:
        return ip_addr.startswith("10.0.0.") or ip_addr.startswith("127.0.")

    def record_server_error(
        self, server: Optional[Tuple[str, int, str]], reason: str = ""
    ) -> bool:
        """累计节点异常/慢响应计数，达到阈值时将其从本轮活跃池中动态熔断剔除。"""
        if server is None:
            return False
        evicted = False
        remaining = 0
        cnt = 0
        with self._rr_lock:
            cnt = self._fail_counts.get(server, 0) + 1
            self._fail_counts[server] = cnt
            if cnt >= 3 and server in self.servers and len(self.servers) > 2:
                self.servers.remove(server)
                evicted = True
                remaining = len(self.servers)
        if evicted:
            reason_desc = f", {reason}" if reason else ""
            logger.info(
                f"节点故障/慢响应累计达 {cnt} 次{reason_desc}，本轮动态熔断剔除: {server[2]} ({server[0]}:{server[1]})，剩余活跃节点 {remaining} 台"
            )
        return evicted

    def probe_and_filter_servers(
        self,
        timeout: float = 1.2,
        max_active: int = 10,
        max_per_subnet: int = 2,
    ) -> Dict:
        """并发预检全部候选节点：验证连通性、日线、双市场 15 分钟线与除权接口，
        临时屏蔽异常节点，并按 /24 子网配额与延时升序优选前 max_active 台主站。
        """
        t_start = time.perf_counter()
        candidates = list(self._all_servers)
        if not candidates:
            return {"elapsed": 0.0, "active": [], "blocked": [], "fallback": False}

        def _probe_one(server: Tuple[str, int, str]) -> Dict:
            ip, port, _ = server
            t0 = time.perf_counter()
            api = TdxHq_API(heartbeat=False)
            try:
                if not api.connect(ip, port, time_out=timeout):
                    return {
                        "server": server,
                        "ok": False,
                        "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
                        "reason": "connect_fail",
                    }
                client_sock = getattr(api, "client", None)
                if client_sock is not None and hasattr(client_sock, "settimeout"):
                    client_sock.settimeout(timeout)
                d_bars = api.get_security_bars(4, 0, "000001", 0, 1)
                if not d_bars:
                    return {
                        "server": server,
                        "ok": False,
                        "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
                        "reason": "daily_empty",
                    }
                m_sz = api.get_security_bars(1, 0, "000001", 0, 1)
                m_sh = api.get_security_bars(1, 1, "600519", 0, 1)
                if not m_sz or not m_sh:
                    return {
                        "server": server,
                        "ok": False,
                        "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
                        "reason": "min15_empty",
                    }
                xdxr = api.get_xdxr_info(0, "000001")
                if xdxr is None:
                    return {
                        "server": server,
                        "ok": False,
                        "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
                        "reason": "xdxr_none",
                    }
                return {
                    "server": server,
                    "ok": True,
                    "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
                    "reason": "ok",
                }
            except Exception as e:
                return {
                    "server": server,
                    "ok": False,
                    "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
                    "reason": f"error:{type(e).__name__}",
                }
            finally:
                try:
                    api.disconnect()
                except Exception:
                    pass

        results = []
        with ThreadPoolExecutor(max_workers=len(candidates)) as executor:
            futures = {executor.submit(_probe_one, srv): srv for srv in candidates}
            for fut in as_completed(futures):
                results.append(fut.result())

        healthy = sorted((r for r in results if r["ok"]), key=lambda r: r["latency_ms"])
        blocked = sorted((r for r in results if not r["ok"]), key=lambda r: r["latency_ms"])

        fallback = len(healthy) == 0
        if fallback:
            selected = []
            new_servers = list(self._all_servers)
        else:
            apply_subnet_cap = (
                bool(max_per_subnet and max_per_subnet > 0)
                and len(candidates) > 6
                and not all(self._is_test_ip(srv[0]) for srv in candidates)
            )
            if not apply_subnet_cap:
                selected = healthy[:max_active] if (max_active and max_active > 0) else healthy
            else:
                selected = []
                selected_servers = set()
                subnet_counts: Dict[str, int] = {}
                limit_n = max_active if (max_active and max_active > 0) else len(healthy)
                # 第一轮：同 /24 子网最多入选 max_per_subnet 台，保证网段多样性防限流
                for r in healthy:
                    if len(selected) >= limit_n:
                        break
                    sub = self._subnet24(r["server"][0])
                    if subnet_counts.get(sub, 0) < max_per_subnet:
                        selected.append(r)
                        selected_servers.add(r["server"])
                        subnet_counts[sub] = subnet_counts.get(sub, 0) + 1
                # 第二轮：若不同子网节点不足 max_active，则按延时升序从剩余健康节点补齐
                if len(selected) < limit_n:
                    for r in healthy:
                        if len(selected) >= limit_n:
                            break
                        if r["server"] not in selected_servers:
                            selected.append(r)
                            selected_servers.add(r["server"])
            new_servers = [r["server"] for r in selected]

        with self._rr_lock:
            self.disconnect_thread()
            self.servers = new_servers
            self._fail_counts.clear()
            self._pool_epoch += 1
            self._rr_counter = 0

        elapsed = round(time.perf_counter() - t_start, 2)
        if fallback:
            logger.warning(
                f"通达信节点池预检全部失败 ({len(blocked)} 台异常，耗时 {elapsed:.2f}s)，回退保留全量节点重试"
            )
        else:
            distinct_subnets = len({self._subnet24(r["server"][0]) for r in selected})
            active_desc = ", ".join(
                f"{r['server'][2]}({r['latency_ms']:.0f}ms)" for r in selected
            )
            logger.info(
                f"节点池预检完成 (耗时 {elapsed:.2f}s): 启用 {len(selected)}/{len(candidates)} 台优选节点 (覆盖 {distinct_subnets} 个/24网段) -> {active_desc}"
            )
            if blocked:
                blocked_desc = ", ".join(
                    f"{r['server'][2]}[{r['reason']}]" for r in blocked
                )
                logger.info(f"本轮屏蔽 {len(blocked)} 台异常节点: {blocked_desc}")

        return {
            "elapsed": elapsed,
            "active": selected,
            "blocked": blocked,
            "fallback": fallback,
        }

    def get_client(self) -> TdxHq_API:
        """获取当前线程绑定的可用 TdxHq_API 实例，失败时自动故障切换"""
        if hasattr(self._local, "api") and self._local.api is not None:
            curr_srv = getattr(self._local, "server", None)
            if (
                getattr(self._local, "epoch", -1) != self._pool_epoch
                or curr_srv not in self.servers
            ):
                self.disconnect_thread()
            else:
                # 简单心跳验证
                try:
                    if self._local.api.client is not None:
                        return self._local.api
                except Exception:
                    pass

        # 重新建立连接，通过轮询偏移均匀分散不同工作线程到多台优选券商主站
        with self._rr_lock:
            servers_snapshot = list(self.servers)
            epoch_snapshot = self._pool_epoch
            start_idx = self._rr_counter % len(servers_snapshot)
            self._rr_counter += 1

        ordered_servers = servers_snapshot[start_idx:] + servers_snapshot[:start_idx]
        api = TdxHq_API(heartbeat=False)
        for ip, port, name in ordered_servers:
            srv = (ip, port, name)
            try:
                if api.connect(ip, port, time_out=2.0):
                    client_sock = getattr(api, "client", None)
                    if client_sock is not None and hasattr(client_sock, "settimeout"):
                        client_sock.settimeout(2.0)
                    # 已通过并发预检 (_pool_epoch > 0) 时直接绑定返回，免去冗余探测 RPC
                    if epoch_snapshot > 0:
                        self._local.api = api
                        self._local.server = srv
                        self._local.epoch = epoch_snapshot
                        return api
                    # 未预检直连模式 (_pool_epoch == 0) 保留快速探测验证
                    test_d = api.get_security_bars(4, 0, "000001", 0, 1)
                    test_m = api.get_security_bars(1, 0, "000001", 0, 1)
                    if (
                        test_d is not None
                        and len(test_d) > 0
                        and test_m is not None
                        and len(test_m) > 0
                    ):
                        self._local.api = api
                        self._local.server = srv
                        self._local.epoch = epoch_snapshot
                        return api
                api.disconnect()
                self.record_server_error(srv, "connect_or_probe_fail")
            except Exception as e:
                try:
                    api.disconnect()
                except Exception:
                    pass
                self.record_server_error(srv, f"connect_exc:{type(e).__name__}")

        raise RuntimeError("无法连接任何通达信可用行情服务器，请检查网络！")

    def disconnect_thread(self):
        if hasattr(self._local, "api") and self._local.api is not None:
            try:
                self._local.api.disconnect()
            except Exception:
                pass
            self._local.api = None
            self._local.server = None


class TdxFetcher:
    """通达信行情与除权增量数据采集器"""

    def __init__(self, pool: Optional[TdxConnectionPool] = None):
        self.pool = pool or TdxConnectionPool.get_instance()

    def refresh_server_pool(
        self,
        timeout: float = 1.2,
        max_active: int = 10,
        max_per_subnet: int = 2,
    ) -> Dict:
        """并发预检通达信节点池，临时屏蔽异常节点并按 /24 子网配额与延时择优排序"""
        return self.pool.probe_and_filter_servers(
            timeout=timeout,
            max_active=max_active,
            max_per_subnet=max_per_subnet,
        )

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

        # 检查最新交易日是否已完整收盘（15分钟线最后一条达到 15:00 且当前北京时间已走完 15:30:10 盘后固定价格交易收盘缓冲）
        is_closed = False
        market_close_cutoff = datetime.datetime.combine(latest_day, datetime.time(15, 30, 10))
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
            curr_server = None
            try:
                api = self.pool.get_client()
                curr_server = getattr(getattr(self.pool, "_local", None), "server", None)
                t_rpc = time.perf_counter()
                if is_idx or sec_type in ("BROAD_INDEX", "INDUSTRY_INDEX"):
                    bars = api.get_index_bars(4, market, code, 0, offset)
                else:
                    bars = api.get_security_bars(4, market, code, 0, offset)
                rpc_elapsed = time.perf_counter() - t_rpc
                if rpc_elapsed > 1.8 and hasattr(self.pool, "record_server_error"):
                    self.pool.record_server_error(curr_server, f"daily_slow:{rpc_elapsed:.2f}s")
                if bars is None:
                    if hasattr(self.pool, "record_server_error"):
                        self.pool.record_server_error(curr_server, "daily_none")
                    self.pool.disconnect_thread()
                    if retry == 0:
                        continue
                    return []
                # 返回空列表 [] 且未抛异常时（如停牌或暂无数据），保持健康连接直接返回
                return bars
            except Exception as e:
                curr_server = getattr(getattr(self.pool, "_local", None), "server", None)
                if hasattr(self.pool, "record_server_error"):
                    self.pool.record_server_error(curr_server, f"daily_exc:{type(e).__name__}")
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
            curr_server = None
            try:
                api = self.pool.get_client()
                curr_server = getattr(getattr(self.pool, "_local", None), "server", None)
                t_rpc = time.perf_counter()
                if is_idx or sec_type in ("BROAD_INDEX", "INDUSTRY_INDEX"):
                    bars = api.get_index_bars(1, market, code, 0, offset)
                else:
                    bars = api.get_security_bars(1, market, code, 0, offset)
                rpc_elapsed = time.perf_counter() - t_rpc
                if rpc_elapsed > 1.8 and hasattr(self.pool, "record_server_error"):
                    self.pool.record_server_error(curr_server, f"min15_slow:{rpc_elapsed:.2f}s")
                if bars is None:
                    if hasattr(self.pool, "record_server_error"):
                        self.pool.record_server_error(curr_server, "min15_none")
                    self.pool.disconnect_thread()
                    if retry == 0:
                        continue
                    return []
                # 返回空列表 [] 且未抛异常时（如停牌或暂无数据），保持健康连接直接返回
                return bars
            except Exception as e:
                curr_server = getattr(getattr(self.pool, "_local", None), "server", None)
                if hasattr(self.pool, "record_server_error"):
                    self.pool.record_server_error(curr_server, f"min15_exc:{type(e).__name__}")
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
            curr_server = None
            try:
                api = self.pool.get_client()
                curr_server = getattr(getattr(self.pool, "_local", None), "server", None)
                t_rpc = time.perf_counter()
                items = api.get_xdxr_info(market, code)
                rpc_elapsed = time.perf_counter() - t_rpc
                if rpc_elapsed > 1.8 and hasattr(self.pool, "record_server_error"):
                    self.pool.record_server_error(curr_server, f"xdxr_slow:{rpc_elapsed:.2f}s")
                if items is None:
                    if hasattr(self.pool, "record_server_error"):
                        self.pool.record_server_error(curr_server, "xdxr_none")
                    self.pool.disconnect_thread()
                    if retry == 0:
                        continue
                    return []
                return items
            except Exception as e:
                curr_server = getattr(getattr(self.pool, "_local", None), "server", None)
                if hasattr(self.pool, "record_server_error"):
                    self.pool.record_server_error(curr_server, f"xdxr_exc:{type(e).__name__}")
                self.pool.disconnect_thread()
                if retry == 1:
                    logger.warning(f"{symbol} 拉取除权除息失败: {e}")
                    return []
        return []

    def fetch_finance_info(self, symbol: str) -> Optional[Dict]:
        """获取个股财务基本信息（含流通股本 liutongguben，单位：股；非个股返回 None）"""
        market, code, is_idx = parse_symbol(symbol)
        if is_idx or not is_stock_symbol(symbol):
            return None
        for retry in range(2):
            curr_server = None
            try:
                api = self.pool.get_client()
                curr_server = getattr(getattr(self.pool, "_local", None), "server", None)
                t_rpc = time.perf_counter()
                info = api.get_finance_info(market, code)
                rpc_elapsed = time.perf_counter() - t_rpc
                if rpc_elapsed > 1.8 and hasattr(self.pool, "record_server_error"):
                    self.pool.record_server_error(curr_server, f"finance_slow:{rpc_elapsed:.2f}s")
                if info is None or not isinstance(info, dict):
                    if hasattr(self.pool, "record_server_error"):
                        self.pool.record_server_error(curr_server, "finance_none")
                    self.pool.disconnect_thread()
                    if retry == 0:
                        continue
                    return None
                return info
            except Exception as e:
                curr_server = getattr(getattr(self.pool, "_local", None), "server", None)
                if hasattr(self.pool, "record_server_error"):
                    self.pool.record_server_error(curr_server, f"finance_exc:{type(e).__name__}")
                self.pool.disconnect_thread()
                if retry == 1:
                    logger.warning(f"{symbol} 拉取财务信息失败: {e}")
                    return None
        return None


def extract_liutongguben(info: Optional[Dict]) -> Optional[float]:
    """从 pytdx.get_finance_info 结果中安全提取有效流通股本（单位：股）；无效或 <=0 返回 None。"""
    if not isinstance(info, dict):
        return None
    val = info.get("liutongguben")
    if val is None:
        return None
    try:
        fval = float(val)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(fval) or fval <= 0.0:
        return None
    return fval


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
