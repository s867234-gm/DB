# -*- coding: utf-8 -*-
"""
scripts/add_128d_period.py —— 为 A_DB 日线体系无损扩展 128d 周期存量回填脚本

功能：
1. 原地扩展 daily_snapshot 表结构：新增 sk_128d ~ mdxl_128d 共 8 列
2. 刷新前复权视图 (v_daily_qfq, v_minute_qfq)
3. 分批 (每批 500 只标的) 读取历史日线数据，利用 Numba 极速计算 128d 指标
4. 原地批量 UPDATE 回写 daily_snapshot，并同步更新 incremental_state 的 128d 状态
5. 进程互斥保护与断点续传支持
"""

import logging
import os
import sys
import time

import duckdb
import numpy as np
import pandas as pd

# 确保项目根目录在 sys.path 中
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.fetcher import get_cn_now
from src.kdj_engine import PeriodState, _compute_period_kdj_numba, states_to_df
from src.lock import SingleInstanceLock
from src.schema import DEFAULT_DB_PATH, VIEWS_DDL

DEFAULT_LOCK_PATH = os.path.join(project_root, "logs", "update.lock")
BATCH_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("add_128d_period")


def upgrade_schema_and_views(con: duckdb.DuckDBPyConnection) -> None:
    """幂等升级 daily_snapshot 物理表结构并刷新视图"""
    cols_info = con.execute("PRAGMA table_info('daily_snapshot')").df()
    existing_cols = set(cols_info["name"].str.lower())

    cols_to_add = [
        ("sk_128d", "DOUBLE"),
        ("sd_128d", "DOUBLE"),
        ("skxl_128d", "DOUBLE"),
        ("sdxl_128d", "DOUBLE"),
        ("mk_128d", "DOUBLE"),
        ("md_128d", "DOUBLE"),
        ("mkxl_128d", "DOUBLE"),
        ("mdxl_128d", "DOUBLE"),
    ]

    added = []
    for col_name, col_type in cols_to_add:
        if col_name not in existing_cols:
            con.execute(f"ALTER TABLE daily_snapshot ADD COLUMN {col_name} {col_type};")
            added.append(col_name)

    if added:
        logger.info(f"daily_snapshot 表已原地新增列: {', '.join(added)}")
    else:
        logger.info("daily_snapshot 表已包含 128d 全部 8 列，跳过 ALTER TABLE")

    con.execute(VIEWS_DDL)
    logger.info("已成功刷新 v_daily_qfq 与 v_minute_qfq 视图定义")


def run_migration(db_path: str = DEFAULT_DB_PATH, batch_size: int = BATCH_SIZE) -> None:
    lock = SingleInstanceLock(DEFAULT_LOCK_PATH)
    if not lock.acquire():
        logger.error(f"获取互斥锁失败 ({DEFAULT_LOCK_PATH})，已有其他更新或迁移进程正在运行，退出。")
        sys.exit(1)

    t0 = time.time()
    logger.info("==================================================")
    logger.info("  A_DB 日线 128d 周期无损扩展与存量回填启动")
    logger.info(f"  目标库: {db_path}")
    logger.info("==================================================")

    con = None
    try:
        con = duckdb.connect(db_path)
        con.execute("PRAGMA threads=8;")

        # 1. 表结构与视图原地升级
        upgrade_schema_and_views(con)

        # 2. 读取全部证券列表并检查断点续传
        all_symbols = con.execute("SELECT symbol FROM security_info ORDER BY symbol ASC").df()["symbol"].tolist()
        total_symbols = len(all_symbols)

        done_syms = set(con.execute("""
            SELECT s.symbol
            FROM incremental_state s
            LEFT JOIN (
                SELECT symbol, count(sk_128d) AS cnt_128d, count(*) AS cnt_total
                FROM daily_snapshot
                GROUP BY symbol
            ) d ON s.symbol = d.symbol
            WHERE s.period = '128d' AND (d.symbol IS NULL OR d.cnt_128d = d.cnt_total)
        """).df()["symbol"])

        todo_symbols = [s for s in all_symbols if s not in done_syms]
        logger.info(f"全市场共 {total_symbols} 只标的，已完成: {len(done_syms)}，待处理: {len(todo_symbols)}")

        if not todo_symbols:
            logger.info("所有标的 128d 周期已全部构建完毕，无需处理。")
            return

        # 3. 分批处理
        total_batches = (len(todo_symbols) + batch_size - 1) // batch_size
        empty_array = np.array([], dtype=np.float64)

        for b_idx in range(total_batches):
            b_start = b_idx * batch_size
            b_symbols = todo_symbols[b_start : b_start + batch_size]
            b_sym_set = set(b_symbols)

            t_b0 = time.time()

            placeholders = ", ".join(["?"] * len(b_symbols))
            query = f"""
                SELECT symbol, trade_date, bar_seq, high, low, close, adj_factor
                FROM daily_snapshot
                WHERE symbol IN ({placeholders})
                ORDER BY symbol ASC, bar_seq ASC
            """
            df_batch = con.execute(query, b_symbols).df()

            df_updates = []
            batch_states = []
            updated_rows_cnt = 0

            if not df_batch.empty:
                for sym, grp in df_batch.groupby("symbol", sort=False):
                    bar_seqs = grp["bar_seq"].to_numpy(dtype=np.int32)
                    adj_factors = grp["adj_factor"].to_numpy(dtype=np.float64)
                    high_adj = grp["high"].to_numpy(dtype=np.float64) * adj_factors
                    low_adj = grp["low"].to_numpy(dtype=np.float64) * adj_factors
                    close_adj = grp["close"].to_numpy(dtype=np.float64) * adj_factors

                    # 128d 桶闭合标志: (bar_seq + 1) % 128 == 0
                    is_closed = ((bar_seqs + 1) % 128 == 0)

                    st_init = PeriodState.initial(sym, "128d")
                    res = _compute_period_kdj_numba(
                        high_adj, low_adj, close_adj, is_closed,
                        empty_array, empty_array,
                        st_init.sk_closed, st_init.sd_closed, st_init.mk_closed, st_init.md_closed,
                        st_init.last_valid_rsv13, st_init.last_valid_rsv104,
                        0, 0.0, 0.0, -1,
                    )

                    metrics_arr = res[0]  # shape (N, 8)
                    sym_df = pd.DataFrame({
                        "symbol": sym,
                        "trade_date": grp["trade_date"].values,
                        "sk_128d": metrics_arr[:, 0],
                        "sd_128d": metrics_arr[:, 1],
                        "skxl_128d": metrics_arr[:, 2],
                        "sdxl_128d": metrics_arr[:, 3],
                        "mk_128d": metrics_arr[:, 4],
                        "md_128d": metrics_arr[:, 5],
                        "mkxl_128d": metrics_arr[:, 6],
                        "mdxl_128d": metrics_arr[:, 7],
                    })
                    df_updates.append(sym_df)
                    updated_rows_cnt += len(sym_df)

                    out_state = PeriodState(
                        symbol=sym,
                        period="128d",
                        last_closed_bucket_id=res[12],
                        closed_highs_adj=list(res[1]),
                        closed_lows_adj=list(res[2]),
                        sk_closed=res[3],
                        sd_closed=res[4],
                        mk_closed=res[5],
                        md_closed=res[6],
                        last_valid_rsv13=res[7],
                        last_valid_rsv104=res[8],
                        bar_count=res[9],
                        high_adj=res[10] if res[9] > 0 else None,
                        low_adj=res[11] if res[9] > 0 else None,
                    )
                    batch_states.append(out_state)

            # 对本批次中在 daily_snapshot 无日线记录的标的生成初始状态
            processed_syms = set(df_batch["symbol"].unique()) if not df_batch.empty else set()
            missing_syms = b_sym_set - processed_syms
            for ms in missing_syms:
                batch_states.append(PeriodState.initial(ms, "128d"))

            # 开启事务原地回写
            con.execute("BEGIN TRANSACTION;")
            try:
                if df_updates:
                    df_up = pd.concat(df_updates, ignore_index=True)
                    con.register("tmp_128d_snap", df_up)
                    con.execute("""
                        UPDATE daily_snapshot d
                        SET sk_128d = u.sk_128d,
                            sd_128d = u.sd_128d,
                            skxl_128d = u.skxl_128d,
                            sdxl_128d = u.sdxl_128d,
                            mk_128d = u.mk_128d,
                            md_128d = u.md_128d,
                            mkxl_128d = u.mkxl_128d,
                            mdxl_128d = u.mdxl_128d
                        FROM tmp_128d_snap u
                        WHERE d.symbol = u.symbol AND d.trade_date = u.trade_date;
                    """)
                    con.unregister("tmp_128d_snap")

                if batch_states:
                    df_st = states_to_df(batch_states, updated_at=get_cn_now())
                    con.register("tmp_128d_state", df_st)
                    con.execute("""
                        INSERT OR REPLACE INTO incremental_state BY NAME
                        SELECT * FROM tmp_128d_state;
                    """)
                    con.unregister("tmp_128d_state")

                con.execute("COMMIT;")
            except Exception:
                try:
                    con.execute("ROLLBACK;")
                except Exception:
                    pass
                raise

            # 每 3 个批次执行一次 CHECKPOINT 释放 WAL 和内存
            if (b_idx + 1) % 3 == 0:
                con.execute("CHECKPOINT;")

            t_b1 = time.time()
            done_count = len(done_syms) + min((b_idx + 1) * batch_size, len(todo_symbols))
            logger.info(
                f"进度: 批次 [{b_idx + 1}/{total_batches}] | "
                f"标的: {len(b_symbols)} 只 | 更新日线: {updated_rows_cnt:,} 行 | "
                f"耗时: {t_b1 - t_b0:.2f}s | 累计进度: {done_count}/{total_symbols}"
            )

        logger.info("执行最终数据库 CHECKPOINT 固化...")
        con.execute("CHECKPOINT;")

        total_cost = time.time() - t0
        logger.info("==================================================")
        logger.info("  128d 周期存量回填与状态构建成功完成!")
        logger.info(f"  总耗时: {total_cost:.2f} 秒 ({total_cost / 60:.2f} 分钟)")
        logger.info("==================================================")
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        lock.release()


if __name__ == "__main__":
    run_migration()
