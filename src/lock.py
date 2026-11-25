# -*- coding: utf-8 -*-
"""跨进程单实例互斥文件锁（基于 Windows msvcrt.locking）"""

import os

try:
    import msvcrt
except ImportError:
    msvcrt = None


class SingleInstanceLock:
    """跨进程单实例互斥文件锁（基于 Windows msvcrt.locking）"""

    def __init__(self, lock_file_path: str):
        self.lock_file_path = lock_file_path
        self.handle = None

    def acquire(self) -> bool:
        if msvcrt is None:
            return True
        lock_dir = os.path.dirname(self.lock_file_path)
        if lock_dir:
            os.makedirs(lock_dir, exist_ok=True)
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
