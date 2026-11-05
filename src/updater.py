# -*- coding: utf-8 -*-
"""
src/updater.py —— 幂等增量更新引擎

功能：
1. 自动比对 A_DB.duckdb 与通达信最新行情，识别待更新标的。
2. 还原 incremental_state，无缝衔接历史 Numba KDJ 规则 A 状态。
3. 严格连续编号 bar_seq，并计算递推 adj_factor。
4. 使用 DuckDB 按列名 (BY NAME) 写入，原子事务提交。
5. 严格保证幂等性：二次运行零重复、零污染。
"""

import datetime
import gc
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd

from src.fetcher import (
    TdxFetcher,
    calculate_next_adj_factor,
    get_cn_now,
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

logger = logging.getLogger("updater")


def to_date(val) -> Optional[datetime.date]:
    """安全将任意日期类型转为 datetime.date"""
    if val is None or pd.isnull(val):
        return None
    if isinstance(val, pd.Timestamp):
        return val.date()
    if isinstance(val, datetime.datetime):
        return val.date()
    if isinstance(val, datetime.date):
        return val
    if isinstance(val, str):
        return datetime.datetime.strptime(val[:10], "%Y-%m-%d").date()
    return None


class DailyUpdater:
    """A_DB.duckdb 日常增量更新引擎"""

    def __init__(
        self,
        db_path: str = r"D:\DB\A_DB.duckdb",
        fetcher: Optional[TdxFetcher] = None,
        max_workers: int = 8,
    ):
        self.db_path = db_path
        self.fetcher = fetcher or TdxFetcher()
        self.max_workers = max_workers

    def _connect_rw(self, retries: int = 5, delay: float = 2.0) -> duckdb.DuckDBPyConnection:
        """获取 DuckDB 读写连接，遇到文件锁冲突自动重试（最多 retries 次，每次间隔 delay 秒）"""
        for i in range(retries):
            try:
                return duckdb.connect(self.db_path)
            except Exception as e:
                err_msg = str(e).lower()
                if ("lock" in err_msg or "conflict" in err_msg or "resource" in err_msg) and i < retries - 1:
                    logger.warning(f"DuckDB 读写连接锁冲突，等待 {delay}s 后重试 ({i + 1}/{retries})... 详情: {e}")
                    time.sleep(delay)
                else:
                    raise
        return duckdb.connect(self.db_path)

    def get_update_plan(
        self,
        target_date: datetime.date,
        allow_intraday: bool = False,
        daily_target_date: Optional[datetime.date] = None,
    ) -> List[Dict]:
        """查询数据库，分析每只标的当前进度，生成更新计划列表。"""
        con = duckdb.connect(self.db_path, read_only=True)
        try:
            # 1. 标的基本信息
            sec_df = con.execute("""
                SELECT symbol, code, sec_type, delist_date, latest_adj_factor
                FROM security_info
                ORDER BY symbol
            """).df()

            # 2. 当前日线最新进展 (增加 ARG_MAX(adj_factor, trade_date) 锚定日线复权因子基准)
            d_df = con.execute("""
                SELECT symbol, 
                       MAX(trade_date) AS last_d_date, 
                       MAX(bar_seq) AS last_bar_seq,
                       ARG_MAX(close, trade_date) AS last_close,
                       ARG_MAX(adj_factor, trade_date) AS last_daily_adj
                FROM daily_snapshot
                GROUP BY symbol
            """).df()

            # 3. 当前 15 分钟线最新进展
            m_df = con.execute("""
                SELECT symbol, 
                       MAX(trading_date) AS last_m_date, 
                       ARG_MAX(slot_seq, bar_end) AS last_m_slot
                FROM minute_snapshot
                GROUP BY symbol
            """).df()
        finally:
            con.close()

        # 合并各表信息
        plan_df = sec_df.merge(d_df, on="symbol", how="left")
        plan_df = plan_df.merge(m_df, on="symbol", how="left")

        target_date = to_date(target_date)
        if daily_target_date is None:
            daily_target_date = target_date
        else:
            daily_target_date = to_date(daily_target_date)

        tasks = []
        for _, row in plan_df.iterrows():
            sym = row["symbol"]
            last_d = to_date(row["last_d_date"])
            last_seq = int(row["last_bar_seq"]) if pd.notnull(row["last_bar_seq"]) else -1
            last_close = float(row["last_close"]) if pd.notnull(row["last_close"]) else None
            last_m = to_date(row["last_m_date"])
            last_slot = int(row["last_m_slot"]) if pd.notnull(row["last_m_slot"]) else 0
            latest_adj = float(row["latest_adj_factor"]) if pd.notnull(row["latest_adj_factor"]) else 1.0
            last_daily_adj = float(row["last_daily_adj"]) if pd.notnull(row["last_daily_adj"]) else None

            # 检查日线是否需要更新到 daily_target_date
            needs_daily = True
            if last_d is not None and last_d >= daily_target_date:
                needs_daily = False

            # 检查分钟线是否需要更新到 target_date
            needs_minute = True
            if last_m is not None:
                if last_m > target_date:
                    needs_minute = False
                elif last_m == target_date and last_slot >= 16:
                    needs_minute = False

            # 若标的已退市且数据已更新至退市日或之后，则不再产生更新任务
            delist_d = to_date(row.get("delist_date"))
            if delist_d is not None and last_d is not None and last_d >= delist_d:
                needs_daily = False
                needs_minute = False

            if needs_daily or needs_minute:
                tasks.append({
                    "symbol": sym,
                    "code": row["code"],
                    "sec_type": row["sec_type"],
                    "latest_adj_factor": latest_adj,
                    "last_daily_adj": last_daily_adj,
                    "last_d_date": last_d,
                    "last_bar_seq": last_seq,
                    "last_close": last_close,
                    "last_m_date": last_m,
                    "last_m_slot": last_slot,
                    "needs_daily": needs_daily,
                    "needs_minute": needs_minute,
                    "daily_target_date": daily_target_date,
                    "target_date": target_date,
                    "allow_intraday": allow_intraday,
                })

        return tasks

    def process_symbol(
        self, task: Dict, target_date: datetime.date
    ) -> Optional[Dict]:
        """单只标的拉取行情与增量计算核心逻辑。
        返回可供主线程批量写入的字典，若无新数据则返回 None。
        """
        sym = task["symbol"]
        sec_type = task["sec_type"]
        last_d = to_date(task["last_d_date"])
        last_bar_seq = task["last_bar_seq"]
        last_m = to_date(task["last_m_date"])
        last_m_slot = task["last_m_slot"]
        latest_adj = task["latest_adj_factor"]
        last_daily_adj = task.get("last_daily_adj")
        target_date = to_date(task.get("target_date") or target_date)
        daily_target_date = to_date(task.get("daily_target_date") or target_date)
        needs_daily = task.get("needs_daily", True)
        needs_minute = task.get("needs_minute", True)
        allow_intraday = task.get("allow_intraday", False)

        # 锚定日线复权因子基准：优先使用日线快照中最后一条真实成交日的复权因子；
        # 仅当该标的在 daily_snapshot 无记录时才回退使用 security_info.latest_adj_factor
        curr_adj = float(last_daily_adj) if (last_daily_adj is not None and not pd.isna(last_daily_adj)) else float(latest_adj)

        # 1. 还原增量状态 (从外部注入的 states_dict 中获取，避免子线程重复连接数据库)
        states_dict = task.get("states_dict") or {}
        daily_states = {p: states_dict.get(p, PeriodState.initial(sym, p)) for p in DAILY_PERIOD_NAMES}
        minute_states = {p: states_dict.get(p, PeriodState.initial(sym, p)) for p in MINUTE_PERIOD_NAMES}

        # 动态计算拉取窗口大小 (根据日期间隔动态计算，最大 800 条)
        days_gap_d = (daily_target_date - last_d).days if (last_d and daily_target_date) else 30
        daily_offset = min(max(days_gap_d + 10, 15), 800)

        days_gap_m = (target_date - last_m).days if (last_m and target_date) else 10
        minute_offset = min(max((days_gap_m + 3) * 16, 48), 800)

        # 2. 拉取日线增量数据（仅当 needs_daily 且过滤 <= daily_target_date，绝不入库未收盘日线）
        df_new_d = pd.DataFrame()
        if needs_daily:
            raw_d = self.fetcher.fetch_daily_bars(sym, sec_type, offset=daily_offset)
            df_new_d = parse_daily_bars_to_df(sym, raw_d, min_trade_date=last_d)
            if not df_new_d.empty:
                df_new_d = df_new_d[df_new_d["trade_date"] <= daily_target_date].reset_index(drop=True)

        # 3. 拉取 15 分钟线增量数据
        df_new_m = pd.DataFrame()
        if needs_minute:
            raw_m = self.fetcher.fetch_minute_bars(sym, sec_type, offset=minute_offset)
            df_new_m = parse_minute_bars_to_df(
                sym, raw_m, min_trade_date=last_m, min_slot_seq=last_m_slot
            )
            if not df_new_m.empty:
                cutoff_m = get_cn_now() - datetime.timedelta(seconds=10)
                df_new_m = df_new_m[
                    (df_new_m["trading_date"] <= target_date) & (df_new_m["bar_end"] <= cutoff_m)
                ].reset_index(drop=True)

        if df_new_d.empty and df_new_m.empty:
            return None

        # 4. 除权除息与 adj_factor 精确按日映射
        new_corp_actions = []
        adj_by_date: Dict[datetime.date, float] = {}
        adj_factor_changed = False

        if sec_type == "STOCK":
            # 判断是否需要拉取除权事件：
            # 1) 日线有新数据推进
            # 2) 盘中模式 (allow_intraday=True) 且 分钟目标日大于日线目标日 (target_date > daily_target_date) 且有分钟线
            has_intraday_preview = (
                allow_intraday
                and daily_target_date is not None
                and target_date > daily_target_date
                and not df_new_m.empty
            )

            if not df_new_d.empty or has_intraday_preview:
                events = self.fetcher.fetch_xdxr_events(sym)
                event_by_date = {}
                for e in events:
                    if e.get("category") == 1:
                        d_obj = datetime.date(e["year"], e["month"], e["day"])
                        event_by_date[d_obj] = e

                # 确定前收盘价基准 (优先使用上一交易日收盘价，次选新日线第一根 open)
                prev_c = task.get("last_close")
                if prev_c is None or prev_c <= 0:
                    if not df_new_d.empty:
                        prev_c = df_new_d.iloc[0]["open"]

                # 1) 跑完截至 daily_target_date 的日线循环推进
                if not df_new_d.empty:
                    adj_list = []
                    for _, r in df_new_d.iterrows():
                        t_date = r["trade_date"]
                        if t_date in event_by_date:
                            ev = event_by_date[t_date]
                            curr_adj = calculate_next_adj_factor(curr_adj, prev_c, ev)
                            adj_factor_changed = True
                            new_corp_actions.append({
                                "symbol": sym,
                                "event_date": t_date,
                                "category": 1,
                                "cash_dividend": float(ev.get("fenhong") or 0.0) / 10.0,
                                "bonus_share": float(ev.get("songzhuangu") or 0.0) / 10.0,
                                "rights_share": float(ev.get("peigu") or 0.0) / 10.0,
                                "rights_price": float(ev.get("peigujia") or 0.0),
                            })
                        adj_by_date[t_date] = curr_adj
                        adj_list.append(curr_adj)
                        prev_c = r["close"]
                    df_new_d["adj_factor"] = adj_list

                # 2) 盘中模式下的当日除权因子预演：
                # 检查在 (daily_target_date, target_date] 期间是否存在 category == 1 的除权除息记录
                if has_intraday_preview:
                    for ev_date, ev in sorted(event_by_date.items()):
                        if daily_target_date is None or (ev_date > daily_target_date and ev_date <= target_date):
                            if prev_c is not None and prev_c > 0:
                                intraday_adj = calculate_next_adj_factor(curr_adj, prev_c, ev)
                                curr_adj = intraday_adj
                                adj_factor_changed = True
                                new_corp_actions.append({
                                    "symbol": sym,
                                    "event_date": ev_date,
                                    "category": 1,
                                    "cash_dividend": float(ev.get("fenhong") or 0.0) / 10.0,
                                    "bonus_share": float(ev.get("songzhuangu") or 0.0) / 10.0,
                                    "rights_share": float(ev.get("peigu") or 0.0) / 10.0,
                                    "rights_price": float(ev.get("peigujia") or 0.0),
                                })
                            adj_by_date[ev_date] = curr_adj

                # 分钟线映射对应日期的复权因子
                if not df_new_m.empty:
                    df_new_m["adj_factor"] = df_new_m["trading_date"].map(
                        lambda d: adj_by_date.get(d, curr_adj)
                    )
            elif not df_new_m.empty:
                # 盘中纯分钟线更新模式且无除权预演需求：直接沿用 curr_adj
                df_new_m["adj_factor"] = curr_adj
        else:
            if not df_new_d.empty:
                df_new_d["adj_factor"] = 1.0
            if not df_new_m.empty:
                df_new_m["adj_factor"] = 1.0

        # 持久化写回 security_info 的最新复权因子：
        # 当日线实际入库推进，或盘中除权因子发生变更时，持久化新因子
        persisted_adj = curr_adj if (not df_new_d.empty or adj_factor_changed) else latest_adj

        # 5. 计算日线快照
        df_d_snap = pd.DataFrame()
        if not df_new_d.empty:
            df_new_d["symbol"] = sym
            df_new_d["code"] = task["code"]
            df_new_d["bar_seq"] = range(last_bar_seq + 1, last_bar_seq + 1 + len(df_new_d))
            df_d_snap, daily_states = MultiCycleKDJEngine.compute_daily_snapshot(
                df_new_d, init_states=daily_states
            )

        # 6. 计算 15 分钟线快照
        df_m_snap = pd.DataFrame()
        if not df_new_m.empty:
            df_new_m["symbol"] = sym
            df_new_m["code"] = task["code"]
            df_m_snap, minute_states = MultiCycleKDJEngine.compute_minute_snapshot(
                df_new_m, cutoff_date=None, init_states=minute_states
            )

        # 7. 序列化更新后的增量状态
        all_updated_states = list(daily_states.values()) + list(minute_states.values())
        df_state = states_to_df(all_updated_states, updated_at=get_cn_now())

        return {
            "symbol": sym,
            "latest_adj_factor": persisted_adj,
            "adj_factor_changed": adj_factor_changed,
            "daily_snap": df_d_snap,
            "minute_snap": df_m_snap,
            "state_df": df_state,
            "corp_actions": new_corp_actions,
        }

    def run_update(
        self,
        allow_intraday: bool = False,
        limit: Optional[int] = None,
        batch_size: int = 500,
    ) -> Dict:
        """执行日常增量更新主流程。"""
        start_time = time.time()
        logger.info("=== 启动日常增量更新流程 ===")

        # 1. 获取最新市场状态 (支持 get_latest_market_dates 与 get_latest_market_status 双向兼容)
        use_market_dates = False
        try:
            res_dates = self.fetcher.get_latest_market_dates(allow_intraday)
            if isinstance(res_dates, (tuple, list)) and len(res_dates) == 3:
                daily_target_date, minute_target_date, is_closed = res_dates
                use_market_dates = True
        except Exception:
            pass

        if not use_market_dates:
            target_date, is_closed = self.fetcher.get_latest_market_status(allow_intraday)
            daily_target_date = target_date
            minute_target_date = target_date

        logger.info(
            f"市场判定: 日线目标日={daily_target_date}, 分钟目标日={minute_target_date}, 是否完整收盘={is_closed}, allow_intraday={allow_intraday}"
        )

        # 2. 分析更新任务
        tasks = self.get_update_plan(
            target_date=minute_target_date,
            allow_intraday=allow_intraday,
            daily_target_date=daily_target_date,
        )
        if limit is not None and limit > 0:
            tasks = tasks[:limit]

        total_tasks = len(tasks)
        if total_tasks == 0:
            logger.info("全市场标的均已是最新状态，无需更新！")
            return {
                "status": "SKIPPED_ALREADY_UP_TO_DATE",
                "target_date": minute_target_date,
                "symbols_updated": 0,
                "elapsed_seconds": round(time.time() - start_time, 2),
            }

        logger.info(f"本次共有 {total_tasks} 只标的需要更新")

        # 3. 多线程处理与分批写入 (外层复用固定线程池)
        total_d_rows = 0
        total_m_rows = 0
        updated_symbols = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for batch_idx in range(0, total_tasks, batch_size):
                batch_tasks = tasks[batch_idx : batch_idx + batch_size]
                batch_results = []

                # 主线程预先单 SQL 批量读取该批次全部 symbol 的 incremental_state
                batch_symbols = [t["symbol"] for t in batch_tasks]
                con_st = duckdb.connect(self.db_path, read_only=True)
                try:
                    con_st.register("tmp_batch_syms", pd.DataFrame({"symbol": batch_symbols}))
                    st_rows = con_st.execute("""
                        SELECT s.symbol, s.period, s.last_closed_bucket_id, s.closed_highs_adj, s.closed_lows_adj,
                               s.sk_closed, s.sd_closed, s.mk_closed, s.md_closed, s.last_valid_rsv13, s.last_valid_rsv104,
                               s.bar_count, s.high_adj, s.low_adj, s.updated_at
                        FROM incremental_state s
                        JOIN tmp_batch_syms b ON s.symbol = b.symbol
                    """).fetchall()
                    con_st.unregister("tmp_batch_syms")
                finally:
                    con_st.close()

                states_by_sym = {}
                for r in st_rows:
                    st_obj = PeriodState.from_db_row(r)
                    states_by_sym.setdefault(st_obj.symbol, {})[st_obj.period] = st_obj

                for t in batch_tasks:
                    t["states_dict"] = states_by_sym.get(t["symbol"], {})

                futures = {
                    executor.submit(self.process_symbol, t, minute_target_date): t
                    for t in batch_tasks
                }
                for fut in as_completed(futures):
                    try:
                        res = fut.result()
                        if res is not None:
                            batch_results.append(res)
                    except Exception as e:
                        t = futures[fut]
                        logger.error(f"{t['symbol']} 处理异常: {e}")

                if not batch_results:
                    continue

                # 聚合本批次数据
                d_list = [r["daily_snap"] for r in batch_results if not r["daily_snap"].empty]
                m_list = [r["minute_snap"] for r in batch_results if not r["minute_snap"].empty]
                s_list = [r["state_df"] for r in batch_results if not r["state_df"].empty]
                ca_list = []
                for r in batch_results:
                    if r.get("corp_actions"):
                        ca_list.extend(r["corp_actions"])

                b_df_d = pd.concat(d_list, ignore_index=True) if d_list else pd.DataFrame()
                b_df_m = pd.concat(m_list, ignore_index=True) if m_list else pd.DataFrame()
                b_df_s = pd.concat(s_list, ignore_index=True) if s_list else pd.DataFrame()
                b_df_ca = pd.DataFrame(ca_list).drop_duplicates(subset=["symbol", "event_date", "category"], keep="last") if ca_list else pd.DataFrame()

                # 写入 DuckDB (原子事务 + BY NAME)
                con = self._connect_rw()
                try:
                    con.execute("PRAGMA threads=8;")
                    con.execute("BEGIN TRANSACTION;")

                    if not b_df_d.empty:
                        con.register("tmp_batch_daily", b_df_d)
                        con.execute(
                            "INSERT OR IGNORE INTO daily_snapshot BY NAME SELECT * FROM tmp_batch_daily"
                        )
                        con.unregister("tmp_batch_daily")
                        total_d_rows += len(b_df_d)

                    if not b_df_m.empty:
                        con.register("tmp_batch_minute", b_df_m)
                        con.execute(
                            "INSERT OR IGNORE INTO minute_snapshot BY NAME SELECT * FROM tmp_batch_minute"
                        )
                        con.unregister("tmp_batch_minute")
                        total_m_rows += len(b_df_m)

                    if not b_df_s.empty:
                        con.register("tmp_batch_state", b_df_s)
                        con.execute(
                            "INSERT OR REPLACE INTO incremental_state BY NAME SELECT * FROM tmp_batch_state"
                        )
                        con.unregister("tmp_batch_state")

                    if not b_df_ca.empty:
                        con.register("tmp_batch_ca", b_df_ca)
                        con.execute(
                            "INSERT OR IGNORE INTO corporate_action BY NAME SELECT * FROM tmp_batch_ca"
                        )
                        con.unregister("tmp_batch_ca")

                    # 批量单条 SQL 更新 security_info 的 latest_adj_factor 与 updated_at
                    adj_updates = [
                        {"symbol": r["symbol"], "latest_adj_factor": r["latest_adj_factor"]}
                        for r in batch_results
                        if not r["daily_snap"].empty or r.get("adj_factor_changed")
                    ]
                    if adj_updates:
                        df_adj_upd = pd.DataFrame(adj_updates).drop_duplicates(subset=["symbol"], keep="last")
                        con.register("tmp_batch_adj", df_adj_upd)
                        con.execute("""
                            UPDATE security_info s
                            SET latest_adj_factor = u.latest_adj_factor, updated_at = NOW()
                            FROM tmp_batch_adj u
                            WHERE s.symbol = u.symbol
                        """)
                        con.unregister("tmp_batch_adj")

                    con.execute("COMMIT;")
                    updated_symbols += len(batch_results)
                    del b_df_d, b_df_m, b_df_s, batch_results
                    gc.collect()

                    logger.info(
                        f"进度: 已更新 {updated_symbols}/{total_tasks} 标的 | 累积追加日线 {total_d_rows:,} 条, 分钟线 {total_m_rows:,} 条"
                    )
                except Exception as e:
                    con.execute("ROLLBACK;")
                    logger.error(f"批量写入事务回滚: {e}")
                    raise
                finally:
                    con.close()

        # 全部批次完成后触发 CHECKPOINT 保证 WAL 日志落盘
        con_chk = self._connect_rw()
        try:
            con_chk.execute("CHECKPOINT;")
        finally:
            con_chk.close()

        elapsed = round(time.time() - start_time, 2)
        logger.info(
            f"=== 增量更新完成! 标的数={updated_symbols}, 日线={total_d_rows:,}, 分钟线={total_m_rows:,}, 耗时={elapsed}s ==="
        )

        return {
            "status": "SUCCESS",
            "target_date": minute_target_date,
            "symbols_updated": updated_symbols,
            "daily_rows_added": total_d_rows,
            "minute_rows_added": total_m_rows,
            "elapsed_seconds": elapsed,
        }
