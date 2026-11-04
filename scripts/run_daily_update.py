# -*- coding: utf-8 -*-
"""
scripts/run_daily_update.py —— 日常增量更新命令行一键运行入口

用法:
    python scripts/run_daily_update.py                      # 默认更新全市场最新收盘日
    python scripts/run_daily_update.py --allow-intraday      # 盘中允许同步最新分钟线
    python scripts/run_daily_update.py --limit 10            # 抽样更新 10 只标的
    python scripts/run_daily_update.py --workers 16          # 指定 16 线程并发
"""

import argparse
import datetime
import logging
import os
import sys
import time

# 确保项目根目录在 sys.path 中
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.updater import DailyUpdater


def setup_logger(log_dir: str = "logs"):
    os.makedirs(log_dir, exist_ok=True)
    today_str = datetime.date.today().strftime("%Y%m%d")
    log_file = os.path.join(log_dir, f"update_{today_str}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("run_daily_update")


def main():
    parser = argparse.ArgumentParser(description="A_DB A股全市场快照多周期增量更新")
    parser.add_argument(
        "--db-path",
        type=str,
        default=r"D:\DB\A_DB.duckdb",
        help="目标 DuckDB 数据库路径",
    )
    parser.add_argument(
        "--allow-intraday",
        action="store_true",
        help="允许更新盘中未走完 15:00 收盘的数据（默认仅更新已收盘交易日）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="限制更新标的数量（用于快速抽样调试）",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="并发拉取与计算线程数 (默认 8)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="数据库批量写入大小 (默认 100)",
    )

    args = parser.parse_args()
    logger = setup_logger()

    logger.info("==================================================")
    logger.info("  A_DB 数据库日常增量更新流程启动")
    logger.info(f"  目标数据库: {args.db_path}")
    logger.info(f"  盘中模式: {args.allow_intraday}")
    logger.info(f"  并发线程: {args.workers}")
    logger.info(f"  更新限制: {args.limit or '全市场'}")
    logger.info("==================================================")

    updater = DailyUpdater(
        db_path=args.db_path,
        max_workers=args.workers,
    )

    result = updater.run_update(
        allow_intraday=args.allow_intraday,
        limit=args.limit,
        batch_size=args.batch_size,
    )

    logger.info("==================================================")
    logger.info(f"  执行状态: {result['status']}")
    logger.info(f"  目标日期: {result.get('target_date')}")
    logger.info(f"  更新标的: {result.get('symbols_updated', 0)} 只")
    logger.info(f"  日线追加: {result.get('daily_rows_added', 0):,} 行")
    logger.info(f"  分钟追加: {result.get('minute_rows_added', 0):,} 行")
    logger.info(f"  总计耗时: {result.get('elapsed_seconds', 0)} 秒")
    logger.info("==================================================")


if __name__ == "__main__":
    main()
