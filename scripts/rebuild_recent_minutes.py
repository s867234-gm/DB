# -*- coding: utf-8 -*-
"""
scripts/rebuild_recent_minutes.py —— 2026-10-30 干净基准锚点回溯重建工具

核心功能:
1. 从 2026-10-30 干净历史数据中重建全市场 5,335 只标的的精确 baseline 状态（零误差校准）。
2. 从通达信并发拉取 2026-11-02 ~ 2026-11-05 完整分钟线（每日严格 16 个 slot），按日线真实复权因子映射。
3. 一次性重算 4 个交易日的分钟线快照，原子事务写入 minute_snapshot，并校正 incremental_state (带当前时间戳 updated_at)。
"""

import os
import sys
import time
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

import duckdb
import pandas as pd

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.fetcher import TdxFetcher, parse_minute_bars_to_df
from src.kdj_engine import MultiCycleKDJEngine, PeriodState, MINUTE_PERIOD_NAMES, states_to_df

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("rebuild_minutes")

DB_PATH = r"D:\DB\A_DB.duckdb"
REBUILD_START_DATE = datetime.date(2026, 11, 2)
BASELINE_END_DATE = datetime.date(2026, 10, 30)
HIST_LOOKBACK_START = datetime.date(2026, 7, 15)
WORKERS = 24
BATCH_SIZE = 500


def get_all_symbols_and_adj() -> Tuple[List[dict], Dict[Tuple[str, datetime.date], float]]:
    con = duckdb.connect(DB_PATH, read_only=True)
    sym_rows = con.execute("""
        SELECT symbol, code, sec_type, latest_adj_factor
        FROM security_info
        ORDER BY symbol
    """).fetchall()
    tasks = [{"symbol": r[0], "code": r[1], "sec_type": r[2], "latest_adj_factor": r[3]} for r in sym_rows]

    adj_rows = con.execute("""
        SELECT symbol, trade_date, adj_factor
        FROM daily_snapshot
        WHERE trade_date >= ?
    """, [REBUILD_START_DATE]).fetchall()
    con.close()

    adj_map = {(r[0], r[1]): float(r[2]) for r in adj_rows}
    return tasks, adj_map


def build_baseline_states_for_batch(con_ro, batch_symbols: List[str]) -> Dict[str, Dict[str, PeriodState]]:
    """批量从 minute_snapshot 提取历史数据，精确计算截至 2026-10-30 15:00 的基准状态"""
    df = con_ro.execute("""
        SELECT symbol, code, trading_date, slot_seq, bar_end,
               open, high, low, close, volume, amount, adj_factor,
               sk_15m, sd_15m, mk_15m, md_15m,
               sk_30m, sd_30m, mk_30m, md_30m,
               sk_60m, sd_60m, mk_60m, md_60m,
               sk_120m, sd_120m, mk_120m, md_120m
        FROM minute_snapshot
        WHERE trading_date >= ? AND trading_date <= ?
          AND symbol IN (SELECT unnest(?))
        ORDER BY symbol, trading_date, slot_seq
    """, [HIST_LOOKBACK_START, BASELINE_END_DATE, batch_symbols]).df()

    states_by_sym = {}
    if df.empty:
        for sym in batch_symbols:
            states_by_sym[sym] = {p: PeriodState.initial(sym, p) for p in MINUTE_PERIOD_NAMES}
        return states_by_sym

    grouped = df.groupby("symbol", sort=False)
    for sym, sub_df in grouped:
        sub_df = sub_df.reset_index(drop=True)
        seeds = sub_df[sub_df["slot_seq"] == 16]
        if seeds.empty:
            states_by_sym[sym] = {p: PeriodState.initial(sym, p) for p in MINUTE_PERIOD_NAMES}
            continue

        seed_idx = seeds.index[0]
        seed_row = sub_df.iloc[seed_idx]
        init_states = {}
        for p in MINUTE_PERIOD_NAMES:
            st = PeriodState.initial(sym, p)
            st.sk_closed = float(seed_row[f"sk_{p}"])
            st.sd_closed = float(seed_row[f"sd_{p}"])
            st.mk_closed = float(seed_row[f"mk_{p}"])
            st.md_closed = float(seed_row[f"md_{p}"])
            st.bar_count = 0
            init_states[p] = st

        calc_df = sub_df.iloc[seed_idx + 1:].copy().reset_index(drop=True)
        if calc_df.empty:
            # 种子行就是最后一行
            states_by_sym[sym] = init_states
            continue

        _, sym_states = MultiCycleKDJEngine.compute_minute_snapshot(calc_df, cutoff_date=None, init_states=init_states)
        last_db_row = sub_df.iloc[-1]
        for p in MINUTE_PERIOD_NAMES:
            st = sym_states[p]
            st.sk_closed = float(last_db_row[f"sk_{p}"])
            st.sd_closed = float(last_db_row[f"sd_{p}"])
            st.mk_closed = float(last_db_row[f"mk_{p}"])
            st.md_closed = float(last_db_row[f"md_{p}"])
            st.bar_count = 0
            st.high_adj = None
            st.low_adj = None

        states_by_sym[sym] = sym_states

    # 兜底：未查到任何历史数据的标的
    for sym in batch_symbols:
        if sym not in states_by_sym:
            states_by_sym[sym] = {p: PeriodState.initial(sym, p) for p in MINUTE_PERIOD_NAMES}

    return states_by_sym


def process_symbol_task(
    task: dict,
    fetcher: TdxFetcher,
    adj_map: Dict[Tuple[str, datetime.date], float],
    baseline_states: Dict[str, PeriodState]
) -> dict:
    sym = task["symbol"]
    code = task["code"]
    sec_type = task["sec_type"]
    latest_adj = float(task["latest_adj_factor"]) if task["latest_adj_factor"] is not None else 1.0

    raw_m = fetcher.fetch_minute_bars(sym, sec_type, offset=80)
    df_new_m = parse_minute_bars_to_df(sym, raw_m, min_trade_date=REBUILD_START_DATE)

    if df_new_m.empty:
        return None

    df_new_m["symbol"] = sym
    df_new_m["code"] = code
    if sec_type == "STOCK":
        df_new_m["adj_factor"] = df_new_m["trading_date"].map(lambda d: adj_map.get((sym, d), latest_adj))
    else:
        df_new_m["adj_factor"] = 1.0

    snap_df, new_m_states = MultiCycleKDJEngine.compute_minute_snapshot(
        df_new_m, cutoff_date=None, init_states=baseline_states
    )
    state_df = states_to_df(list(new_m_states.values()), updated_at=datetime.datetime.now())

    return {
        "symbol": sym,
        "snap_df": snap_df,
        "state_df": state_df
    }


def main():
    start_time = time.time()
    logger.info("==================================================")
    logger.info("  A_DB 2026-11-02 ~ 2026-11-05 分钟线精密回溯重建")
    logger.info(f"  基准锚点: {BASELINE_END_DATE} 15:00:00")
    logger.info(f"  重算区间: {REBUILD_START_DATE} ~ 今日")
    logger.info(f"  并发线程: {WORKERS} | 批次大小: {BATCH_SIZE}")
    logger.info("==================================================")

    tasks, adj_map = get_all_symbols_and_adj()
    total = len(tasks)
    logger.info(f"共加载 {total} 只标的，日线复权映射 {len(adj_map)} 条记录")

    fetcher = TdxFetcher()
    total_rebuilt_bars = 0
    total_rebuilt_symbols = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for b_idx in range(0, total, BATCH_SIZE):
            batch_t0 = time.time()
            b_tasks = tasks[b_idx : b_idx + BATCH_SIZE]
            b_symbols = [t["symbol"] for t in b_tasks]
            batch_num = b_idx // BATCH_SIZE + 1
            total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

            logger.info(f"--- [批次 {batch_num}/{total_batches}] 开始处理 {len(b_tasks)} 只标的 ---")

            # 1. 批量构建 2026-10-30 基准状态
            con_ro = duckdb.connect(DB_PATH, read_only=True)
            baseline_states_map = build_baseline_states_for_batch(con_ro, b_symbols)
            con_ro.close()

            # 2. 并发拉取行情与重算 4 天分钟线
            futures = [
                executor.submit(process_symbol_task, t, fetcher, adj_map, baseline_states_map.get(t["symbol"]))
                for t in b_tasks
            ]
            b_results = []
            for f in as_completed(futures):
                res = f.result()
                if res is not None:
                    b_results.append(res)

            if not b_results:
                logger.warning(f"批次 {batch_num} 无有效数据产生，跳过")
                continue

            b_snap = pd.concat([r["snap_df"] for r in b_results if not r["snap_df"].empty], ignore_index=True)
            b_state = pd.concat([r["state_df"] for r in b_results if not r["state_df"].empty], ignore_index=True)
            b_syms = [r["symbol"] for r in b_results]

            # 3. 原子事务写入 DuckDB
            con_w = duckdb.connect(DB_PATH)
            try:
                con_w.execute("PRAGMA threads=8;")
                con_w.execute("BEGIN TRANSACTION;")

                con_w.register("tmp_syms", pd.DataFrame({"symbol": b_syms}))
                con_w.execute("""
                    DELETE FROM minute_snapshot
                    WHERE trading_date >= ?
                      AND symbol IN (SELECT symbol FROM tmp_syms)
                """, [REBUILD_START_DATE])
                con_w.unregister("tmp_syms")

                con_w.register("tmp_b_snap", b_snap)
                con_w.execute("INSERT INTO minute_snapshot BY NAME SELECT * FROM tmp_b_snap")
                con_w.unregister("tmp_b_snap")

                con_w.register("tmp_b_state", b_state)
                con_w.execute("INSERT OR REPLACE INTO incremental_state BY NAME SELECT * FROM tmp_b_state")
                con_w.unregister("tmp_b_state")

                con_w.execute("COMMIT;")
                con_w.execute("CHECKPOINT;")
            except Exception as e:
                con_w.execute("ROLLBACK;")
                logger.error(f"批次 {batch_num} 写入事务失败回滚: {e}")
                raise e
            finally:
                con_w.close()

            total_rebuilt_symbols += len(b_results)
            total_rebuilt_bars += len(b_snap)
            batch_dur = time.time() - batch_t0
            logger.info(
                f"[批次 {batch_num}/{total_batches}] 成功入库: {len(b_results)} 只标的, "
                f"{len(b_snap):,} 根分钟线, 耗时 {batch_dur:.2f}s | "
                f"累计进度: {total_rebuilt_symbols}/{total} 标的, {total_rebuilt_bars:,} 行"
            )

    total_time = time.time() - start_time
    logger.info("==================================================")
    logger.info(f"=== 重建全部完成! 成功标的={total_rebuilt_symbols}, 分钟线总行数={total_rebuilt_bars:,}, 总耗时={total_time:.2f}s ===")
    logger.info("==================================================")


if __name__ == "__main__":
    main()
