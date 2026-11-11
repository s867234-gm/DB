# -*- coding: utf-8 -*-
"""
scripts/add_turn10_and_liutongguben.py —— A_DB 扩展流通股本 (liutongguben) 与 10日累计换手率 (turn10) 幂等迁移脚本

功能：
1. 原地扩展表结构与视图：
   - security_info 新增 liutongguben DOUBLE
   - daily_snapshot 新增 turn10 DOUBLE
   - 刷新 v_daily_qfq / v_minute_qfq 视图定义
2. 通过通达信接口 (pytdx.get_finance_info) 并发采集全市场个股最新流通股本写入 security_info（指数置 NULL，退市/0 值安全跳过）
3. 回填 daily_snapshot.turn10：
   - 历史日线优先严格对齐源库 a_stock_v1.duckdb (stock_daily_feature_snapshot.turn10)
   - 源库未覆盖的新增/复牌行，依据 security_info.liutongguben 与近10个实际交易日累计成交量自动补算
4. 严格支持单实例文件锁、事务批量回写与幂等重跑
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import math
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import duckdb
import pandas as pd

try:
    import msvcrt
except ImportError:
    msvcrt = None

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.fetcher import (
    TdxFetcher,
    extract_liutongguben,
    extract_liutongguben_from_xdxr,
)
from src.schema import DEFAULT_DB_PATH, SOURCE_DB_PATH, VIEWS_DDL

DEFAULT_LOCK_PATH = os.path.join(project_root, "logs", "update.lock")
BATCH_SIZE = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("add_turn10_and_liutongguben")


class SingleInstanceLock:
    """跨进程单实例互斥文件锁（基于 Windows msvcrt.locking）"""

    def __init__(self, lock_file_path: str):
        self.lock_file_path = lock_file_path
        self.handle = None

    def acquire(self) -> bool:
        if msvcrt is None:
            return True
        os.makedirs(os.path.dirname(self.lock_file_path), exist_ok=True)
        try:
            self.handle = open(self.lock_file_path, "a+")
            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            self.handle.truncate(0)
            self.handle.write(f"{os.getpid()}\n")
            self.handle.flush()
            return True
        except (IOError, OSError):
            if self.handle:
                try:
                    self.handle.close()
                except Exception:
                    pass
                self.handle = None
            return False

    def release(self):
        if self.handle:
            try:
                self.handle.seek(0)
                if msvcrt:
                    msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            except Exception:
                pass
            try:
                self.handle.close()
            except Exception:
                pass
            self.handle = None


def upgrade_schema_and_views(con: duckdb.DuckDBPyConnection) -> None:
    """幂等升级 security_info / daily_snapshot 表结构并刷新视图"""
    sec_cols = set(con.execute("PRAGMA table_info('security_info')").df()["name"].str.lower())
    if "liutongguben" not in sec_cols:
        con.execute("ALTER TABLE security_info ADD COLUMN liutongguben DOUBLE;")
        logger.info("security_info 表已新增列: liutongguben DOUBLE")
    else:
        logger.info("security_info 表已包含 liutongguben 列，跳过 ALTER TABLE")

    daily_cols = set(con.execute("PRAGMA table_info('daily_snapshot')").df()["name"].str.lower())
    if "turn10" not in daily_cols:
        con.execute("ALTER TABLE daily_snapshot ADD COLUMN turn10 DOUBLE;")
        logger.info("daily_snapshot 表已新增列: turn10 DOUBLE")
    else:
        logger.info("daily_snapshot 表已包含 turn10 列，跳过 ALTER TABLE")

    con.execute(VIEWS_DDL)
    logger.info("已刷新 v_daily_qfq 与 v_minute_qfq 视图定义")


def populate_liutongguben(
    con: duckdb.DuckDBPyConnection,
    workers: int = 24,
    only_missing: bool = False,
    source_db_path: str = SOURCE_DB_PATH,
) -> Tuple[int, int]:
    """并发拉取全市场个股最新流通股本 (liutongguben) 并写入 security_info（含 xdxr 与 v1 历史反推兜底）"""
    t0 = time.perf_counter()
    where_clause = "sec_type = 'STOCK' AND liutongguben IS NULL" if only_missing else "sec_type = 'STOCK'"
    stocks_df = con.execute(f"""
        SELECT symbol, liutongguben, delist_date
        FROM security_info
        WHERE {where_clause}
        ORDER BY symbol ASC
    """).df()

    if stocks_df.empty:
        logger.info("无需刷新流通股本，跳过拉取。")
        return 0, 0

    existing_map: Dict[str, float] = {
        row["symbol"]: float(row["liutongguben"])
        for _, row in stocks_df.iterrows()
        if pd.notnull(row["liutongguben"])
    }
    delisted_syms = {
        row["symbol"]
        for _, row in stocks_df.iterrows()
        if pd.notnull(row["delist_date"])
    }
    symbols = stocks_df["symbol"].tolist()

    fetcher = TdxFetcher()
    fetcher.refresh_server_pool()

    def _fetch_ltgb(sym: str) -> Tuple[Optional[float], str]:
        info = fetcher.fetch_finance_info(sym)
        val = extract_liutongguben(info)
        if val is not None:
            return val, "finance"
        # 二级兜底：从除权/股本变动事件提取最新有效流通股本
        events = fetcher.fetch_xdxr_events(sym)
        val_xdxr = extract_liutongguben_from_xdxr(events)
        if val_xdxr is not None:
            return val_xdxr, "xdxr"
        return None, "missing"

    updates = []
    missing_after_tier2 = []
    xdxr_fallback_syms = []
    v1_fallback_syms = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_fetch_ltgb, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                val, src = fut.result()
                if val is not None:
                    if src == "xdxr":
                        xdxr_fallback_syms.append(sym)
                    prev_val = existing_map.get(sym)
                    if prev_val is None or abs(val - prev_val) > 1e-3:
                        updates.append({"symbol": sym, "liutongguben": val})
                else:
                    missing_after_tier2.append(sym)
            except Exception as e:
                logger.warning(f"{sym} 获取流通股本异常: {e}")
                missing_after_tier2.append(sym)

    # 三级兜底：若仍缺失，从源库 v1 历史最新有效 turn10 反推流通股本（股）
    null_syms: List[str] = []
    if missing_after_tier2 and source_db_path and os.path.exists(source_db_path):
        src_sql_path = source_db_path.replace("\\", "/")
        attached_v1 = False
        try:
            con.execute(f"ATTACH '{src_sql_path}' AS v1_ltgb (READ_ONLY);")
            attached_v1 = True
            con.register("tmp_ltgb_missing", pd.DataFrame({"symbol": missing_after_tier2}))
            inferred_rows = con.execute("""
                WITH win AS (
                    SELECT
                        d.symbol,
                        d.bar_seq,
                        d.trade_date,
                        SUM(d.volume) OVER w AS sum_10_vol,
                        COUNT(d.volume) OVER w AS valid_cnt,
                        COUNT(*) OVER w AS win_cnt
                    FROM daily_snapshot d
                    JOIN tmp_ltgb_missing m ON d.symbol = m.symbol
                    WINDOW w AS (
                        PARTITION BY d.symbol ORDER BY d.bar_seq ASC
                        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                    )
                )
                SELECT
                    w.symbol,
                    ROUND((w.sum_10_vol * 10000.0) / v1.turn10) AS inferred_ltgb
                FROM win w
                JOIN v1_ltgb.stock_daily_feature_snapshot v1
                  ON w.symbol = v1.symbol
                 AND CAST(w.trade_date AS TIMESTAMP) = v1.trade_date
                WHERE w.valid_cnt = w.win_cnt
                  AND w.sum_10_vol > 0
                  AND v1.turn10 IS NOT NULL
                  AND NOT isnan(v1.turn10)
                  AND NOT isinf(v1.turn10)
                  AND v1.turn10 > 0
                QUALIFY ROW_NUMBER() OVER (PARTITION BY w.symbol ORDER BY w.bar_seq DESC) = 1
            """).fetchall()
            con.unregister("tmp_ltgb_missing")

            inferred_map: Dict[str, float] = {}
            for sym_i, val_i in inferred_rows:
                if val_i is not None:
                    f_val = float(val_i)
                    if math.isfinite(f_val) and f_val > 0.0:
                        inferred_map[sym_i] = f_val

            for sym in missing_after_tier2:
                val_v1 = inferred_map.get(sym)
                if val_v1 is not None:
                    v1_fallback_syms.append(sym)
                    prev_val = existing_map.get(sym)
                    if prev_val is None or abs(val_v1 - prev_val) > 1e-3:
                        updates.append({"symbol": sym, "liutongguben": val_v1})
                else:
                    null_syms.append(sym)
        finally:
            try:
                con.unregister("tmp_ltgb_missing")
            except Exception:
                pass
            if attached_v1:
                try:
                    con.execute("DETACH v1_ltgb;")
                except Exception:
                    pass
    else:
        null_syms = list(missing_after_tier2)

    con.execute("BEGIN TRANSACTION;")
    if updates:
        df_upd = pd.DataFrame(updates)
        con.register("tmp_ltgb_upd", df_upd)
        con.execute("""
            UPDATE security_info s
            SET liutongguben = u.liutongguben, updated_at = NOW()
            FROM tmp_ltgb_upd u
            WHERE s.symbol = u.symbol
        """)
        con.unregister("tmp_ltgb_upd")

    # 严格确保指数不含 liutongguben
    con.execute("UPDATE security_info SET liutongguben = NULL WHERE sec_type != 'STOCK' AND liutongguben IS NOT NULL;")
    con.execute("COMMIT;")

    active_nulls = [s for s in null_syms if s not in delisted_syms]
    elapsed = time.perf_counter() - t0
    logger.info(
        f"流通股本采集完成: 总个股={len(symbols)}, 更新写入={len(updates)} "
        f"(xdxr兜底={len(xdxr_fallback_syms)}: {xdxr_fallback_syms[:10]}, "
        f"v1反推兜底={len(v1_fallback_syms)}: {v1_fallback_syms[:10]}), "
        f"返回<=0/空={len(null_syms)} (其中非退市={len(active_nulls)}: {active_nulls[:10]}), 耗时={elapsed:.2f}s"
    )
    return len(symbols), len(updates)


def backfill_turn10(
    con: duckdb.DuckDBPyConnection,
    source_db_path: str = SOURCE_DB_PATH,
    batch_size: int = BATCH_SIZE,
) -> int:
    """分批回填 daily_snapshot.turn10：优先对齐源库 v1 历史值，未覆盖行依据 liutongguben 补算"""
    t0 = time.perf_counter()
    stock_symbols = [
        r[0]
        for r in con.execute(
            "SELECT symbol FROM security_info WHERE sec_type = 'STOCK' ORDER BY symbol ASC"
        ).fetchall()
    ]
    total_syms = len(stock_symbols)
    if total_syms == 0:
        return 0

    src_sql_path = source_db_path.replace("\\", "/")
    has_v1 = os.path.exists(source_db_path)
    if has_v1:
        con.execute(f"ATTACH '{src_sql_path}' AS v1 (READ_ONLY);")
        logger.info(f"已只读挂载历史对齐源库: {src_sql_path}")
    else:
        logger.warning(f"未找到历史源库 {source_db_path}，将全量使用 liutongguben 窗口计算 turn10")

    total_batches = (total_syms + batch_size - 1) // batch_size
    updated_rows_total = 0

    try:
        for b_idx in range(total_batches):
            t_b0 = time.perf_counter()
            b_syms = stock_symbols[b_idx * batch_size : (b_idx + 1) * batch_size]

            con.execute("BEGIN TRANSACTION;")
            con.register("tmp_b_syms", pd.DataFrame({"symbol": b_syms}))

            if has_v1:
                con.execute("""
                    CREATE OR REPLACE TEMP TABLE tmp_turn10_batch AS
                    WITH sym_calc AS (
                        SELECT
                            d.symbol,
                            d.trade_date,
                            CASE
                                WHEN s.liutongguben IS NOT NULL AND s.liutongguben > 0
                                     AND COUNT(d.volume) OVER w = COUNT(*) OVER w
                                THEN (SUM(d.volume) OVER w * 10000.0) / s.liutongguben
                                ELSE NULL
                            END AS calc_turn10
                        FROM daily_snapshot d
                        JOIN security_info s ON d.symbol = s.symbol
                        JOIN tmp_b_syms b ON d.symbol = b.symbol
                        WHERE s.sec_type = 'STOCK'
                        WINDOW w AS (
                            PARTITION BY d.symbol ORDER BY d.bar_seq ASC
                            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                        )
                    )
                    SELECT
                        c.symbol,
                        c.trade_date,
                        CASE
                            WHEN v1.turn10 IS NOT NULL AND NOT isnan(v1.turn10) AND NOT isinf(v1.turn10)
                            THEN v1.turn10
                            ELSE c.calc_turn10
                        END AS new_turn10
                    FROM sym_calc c
                    LEFT JOIN v1.stock_daily_feature_snapshot v1
                        ON c.symbol = v1.symbol
                       AND CAST(c.trade_date AS TIMESTAMP) = v1.trade_date;
                """)
            else:
                con.execute("""
                    CREATE OR REPLACE TEMP TABLE tmp_turn10_batch AS
                    SELECT
                        d.symbol,
                        d.trade_date,
                        CASE
                            WHEN s.liutongguben IS NOT NULL AND s.liutongguben > 0
                                 AND COUNT(d.volume) OVER w = COUNT(*) OVER w
                            THEN (SUM(d.volume) OVER w * 10000.0) / s.liutongguben
                            ELSE NULL
                        END AS new_turn10
                    FROM daily_snapshot d
                    JOIN security_info s ON d.symbol = s.symbol
                    JOIN tmp_b_syms b ON d.symbol = b.symbol
                    WHERE s.sec_type = 'STOCK'
                    WINDOW w AS (
                        PARTITION BY d.symbol ORDER BY d.bar_seq ASC
                        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                    );
                """)

            con.unregister("tmp_b_syms")

            diff_cnt = con.execute("""
                SELECT COUNT(*)
                FROM daily_snapshot d
                JOIN tmp_turn10_batch u ON d.symbol = u.symbol AND d.trade_date = u.trade_date
                WHERE u.new_turn10 IS DISTINCT FROM d.turn10
            """).fetchone()[0]

            if diff_cnt > 0:
                con.execute("""
                    UPDATE daily_snapshot d
                    SET turn10 = u.new_turn10
                    FROM tmp_turn10_batch u
                    WHERE d.symbol = u.symbol
                      AND d.trade_date = u.trade_date
                      AND u.new_turn10 IS DISTINCT FROM d.turn10;
                """)
                updated_rows_total += diff_cnt

            con.execute("DROP TABLE IF EXISTS tmp_turn10_batch;")
            con.execute("COMMIT;")

            if (b_idx + 1) % 3 == 0:
                con.execute("CHECKPOINT;")

            logger.info(
                f"turn10 回填批次 [{b_idx + 1}/{total_batches}] | 标的 {len(b_syms)} 只 | "
                f"写入变更 {diff_cnt:,} 行 | 耗时 {time.perf_counter() - t_b0:.2f}s"
            )

        # 确保指数 turn10 严格为 NULL
        con.execute("""
            UPDATE daily_snapshot d
            SET turn10 = NULL
            FROM security_info s
            WHERE d.symbol = s.symbol AND s.sec_type != 'STOCK' AND d.turn10 IS NOT NULL;
        """)
    finally:
        if has_v1:
            try:
                con.execute("DETACH v1;")
            except Exception:
                pass

    logger.info(f"turn10 存量回填结束，共更新 {updated_rows_total:,} 行，耗时 {time.perf_counter() - t0:.2f}s")
    return updated_rows_total


def run_migration(
    db_path: str = DEFAULT_DB_PATH,
    source_db_path: str = SOURCE_DB_PATH,
    workers: int = 24,
    batch_size: int = BATCH_SIZE,
    only_missing_ltgb: bool = False,
) -> None:
    lock = SingleInstanceLock(DEFAULT_LOCK_PATH)
    if not lock.acquire():
        logger.error(f"获取互斥锁失败 ({DEFAULT_LOCK_PATH})，已有其他更新或迁移进程正在运行，退出。")
        sys.exit(1)

    t_all = time.perf_counter()
    logger.info("==================================================")
    logger.info("  A_DB 流通股本 (liutongguben) 与 turn10 迁移启动")
    logger.info(f"  目标库: {db_path}")
    logger.info(f"  对齐源库: {source_db_path}")
    logger.info("==================================================")

    con = None
    try:
        con = duckdb.connect(db_path)
        con.execute("PRAGMA threads=8;")

        upgrade_schema_and_views(con)
        populate_liutongguben(
            con,
            workers=workers,
            only_missing=only_missing_ltgb,
            source_db_path=source_db_path,
        )
        backfill_turn10(con, source_db_path=source_db_path, batch_size=batch_size)

        logger.info("执行最终数据库 CHECKPOINT...")
        con.execute("CHECKPOINT;")
        logger.info(f"迁移全部完成！总耗时: {time.perf_counter() - t_all:.2f}s")
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        lock.release()


def main():
    parser = argparse.ArgumentParser(description="A_DB 扩展 liutongguben 与 turn10 迁移脚本")
    parser.add_argument("--db-path", type=str, default=DEFAULT_DB_PATH)
    parser.add_argument("--source-db-path", type=str, default=SOURCE_DB_PATH)
    parser.add_argument("--workers", type=int, default=24)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--only-missing-ltgb", action="store_true")
    args = parser.parse_args()

    run_migration(
        db_path=args.db_path,
        source_db_path=args.source_db_path,
        workers=args.workers,
        batch_size=args.batch_size,
        only_missing_ltgb=args.only_missing_ltgb,
    )


if __name__ == "__main__":
    main()
