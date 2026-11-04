import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.builder import FullMarketBuilder

if __name__ == '__main__':
    print(">>> 启动阶段 3 全市场历史数据高效批量迁移构建 (断点续传模式)...")
    builder = FullMarketBuilder(batch_size=100)
    builder.build_all()
