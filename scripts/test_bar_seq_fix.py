import numpy as np
import pandas as pd
from src.kdj_engine import MultiCycleKDJEngine, DAILY_PERIOD_NAMES

# 测试日线切片时保留原始 bar_seq
from tests.test_kdj_engine import generate_mock_daily_data

df = generate_mock_daily_data(n_bars=300, seed=123)
# df 中已经有 bar_seq 0..299

df_full, full_states = MultiCycleKDJEngine.compute_daily_snapshot(df)

split = 250
df_base = df.iloc[:split].copy()
df_tail = df.iloc[split:].copy() # 保留原始 bar_seq!

_, base_states = MultiCycleKDJEngine.compute_daily_snapshot(df_base)
df_tail_inc, inc_states = MultiCycleKDJEngine.compute_daily_snapshot(df_tail, init_states=base_states)

max_err = 0.0
for p in DAILY_PERIOD_NAMES:
    for m in ['sk', 'sd', 'skxl', 'sdxl', 'mk', 'md', 'mkxl', 'mdxl']:
        col = f"{m}_{p}"
        full_vals = df_full[col].iloc[split:].to_numpy()
        inc_vals = df_tail_inc[col].to_numpy()
        err = np.max(np.abs(full_vals - inc_vals))
        if err > max_err:
            max_err = err

print(f"Max daily error with correct bar_seq: {max_err}")
assert max_err < 1e-12
