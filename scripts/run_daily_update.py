# -*- coding: utf-8 -*-
"""
scripts/run_daily_update.py —— 全市场日常/盘中增量更新执行入口

用法:
    python scripts/run_daily_update.py                      # 默认更新全市场（盘后）
    python scripts/run_daily_update.py --allow-intraday      # 盘中同步最新分钟线
    python scripts/run_daily_update.py --limit 10            # 仅更新 10 只标的
    python scripts/run_daily_update.py --workers 24          # 指定 24 线程并发
    python scripts/run_daily_update.py --batch-size 500      # 每批 500 只标的
"""

import argparse
import datetime
import logging
import os
import sys
import time

try:
    import msvcrt
except ImportError:
    msvcrt = None

# 确保项目根目录在 sys.path 中
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.updater import DailyUpdater


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
    parser = argparse.ArgumentParser(description="A_DB A股全市场增量更新调度脚本")
    parser.add_argument(
        "--db-path",
        type=str,
        default=r"D:\DB\A_DB.duckdb",
        help="目标 DuckDB 数据库路径",
    )
    parser.add_argument(
        "--allow-intraday",
        action="store_true",
        help="允许更新未到 15:00 收盘的分钟线数据（默认仅推进完整收盘日）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="限制更新标的数量（用于快速抽检）",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=24,
        help="数据拉取并发线程数 (默认 24)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="数据库分批写入大小 (默认 500)",
    )

    args = parser.parse_args()
    logger = setup_logger()

    lock_file = os.path.join(project_root, "logs", "update.lock")
    lock = SingleInstanceLock(lock_file)
    if not lock.acquire():
        existing_pid = "未知"
        try:
            with open(lock_file, "r") as rf:
                existing_pid = rf.read().strip()
        except Exception:
            pass
        logger.warning(f"检测到已有更新进程正在运行 (PID: {existing_pid})，本实例安全退出")
        sys.exit(0)

    try:
        logger.info("==================================================")
        logger.info("  A_DB 数据库日常增量更新流程启动")
        logger.info(f"  目标数据库: {args.db_path}")
        logger.info(f"  盘中模式: {args.allow_intraday}")
        logger.info(f"  并发线程: {args.workers}")
        logger.info(f"  批次大小: {args.batch_size}")
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
    except Exception as e:
        logger.exception(f"增量更新发生未捕获异常: {e}")
        sys.exit(1)
    finally:
        lock.release()


if __name__ == "__main__":
    main()
