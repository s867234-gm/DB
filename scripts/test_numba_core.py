import numpy as np
import numba as nb

@nb.njit(fastmath=True)
def compute_kdj_rule_a(high_adj, low_adj, close_adj, is_closed_flags,
                       init_closed_highs, init_closed_lows,
                       init_sk_closed, init_sd_closed, init_mk_closed, init_md_closed,
                       init_rsv13, init_rsv104,
                       init_bar_count, init_h_curr, init_l_curr,
                       init_last_closed_bucket_id):
    n = len(close_adj)
    out = np.empty((n, 8), dtype=np.float64)

    closed_highs = np.empty(104, dtype=np.float64)
    closed_lows = np.empty(104, dtype=np.float64)
    
    n_init = len(init_closed_highs)
    k_closed = 0
    for i in range(n_init):
        closed_highs[i] = init_closed_highs[i]
        closed_lows[i] = init_closed_lows[i]
        k_closed += 1

    sk_closed = init_sk_closed
    sd_closed = init_sd_closed
    mk_closed = init_mk_closed
    md_closed = init_md_closed

    last_rsv13 = init_rsv13
    last_rsv104 = init_rsv104

    bar_cnt = init_bar_count
    h_curr = init_h_curr
    l_curr = init_l_curr
    last_bucket_id = init_last_closed_bucket_id

    for t in range(n):
        h_t = high_adj[t]
        l_t = low_adj[t]
        c_t = close_adj[t]

        if bar_cnt == 0:
            h_curr = h_t
            l_curr = l_t
        else:
            if h_t > h_curr:
                h_curr = h_t
            if l_t < l_curr:
                l_curr = l_t
        bar_cnt += 1

        # 13 周期极值 (最多 12 个历史闭合桶 + 当前桶)
        hhv13 = h_curr
        llv13 = l_curr
        m13 = min(k_closed, 12)
        start_idx13 = k_closed - m13
        for idx in range(start_idx13, k_closed):
            if closed_highs[idx] > hhv13:
                hhv13 = closed_highs[idx]
            if closed_lows[idx] < llv13:
                llv13 = closed_lows[idx]

        # 104 周期极值 (最多 103 个历史闭合桶 + 当前桶)
        hhv104 = h_curr
        llv104 = l_curr
        m104 = min(k_closed, 103)
        start_idx104 = k_closed - m104
        for idx in range(start_idx104, k_closed):
            if closed_highs[idx] > hhv104:
                hhv104 = closed_highs[idx]
            if closed_lows[idx] < llv104:
                llv104 = closed_lows[idx]

        # RSV13
        if hhv13 == llv13:
            if k_closed == 0:
                rsv13 = 50.0
            else:
                rsv13 = last_rsv13
        else:
            rsv13 = (c_t - llv13) / (hhv13 - llv13) * 100.0

        # RSV104
        if hhv104 == llv104:
            if k_closed == 0:
                rsv104 = 50.0
            else:
                rsv104 = last_rsv104
        else:
            rsv104 = (c_t - llv104) / (hhv104 - llv104) * 100.0

        # 动态快照 (前值锚定上一闭合桶定型值)
        sk = 0.5 * sk_closed + 0.5 * rsv13
        sd = 0.5 * sd_closed + 0.5 * sk
        skxl = sk - sk_closed
        sdxl = sd - sd_closed

        mk = (15.0 / 16.0) * mk_closed + (1.0 / 16.0) * rsv104
        md = (15.0 / 16.0) * md_closed + (1.0 / 16.0) * mk
        mkxl = (mk - mk_closed) * 8.0
        mdxl = (md - md_closed) * 8.0

        out[t, 0] = sk
        out[t, 1] = sd
        out[t, 2] = skxl
        out[t, 3] = sdxl
        out[t, 4] = mk
        out[t, 5] = md
        out[t, 6] = mkxl
        out[t, 7] = mdxl

        if is_closed_flags[t]:
            sk_closed = sk
            sd_closed = sd
            mk_closed = mk
            md_closed = md
            last_rsv13 = rsv13
            last_rsv104 = rsv104

            if k_closed < 104:
                closed_highs[k_closed] = h_curr
                closed_lows[k_closed] = l_curr
                k_closed += 1
            else:
                for s in range(103):
                    closed_highs[s] = closed_highs[s + 1]
                    closed_lows[s] = closed_lows[s + 1]
                closed_highs[103] = h_curr
                closed_lows[103] = l_curr

            last_bucket_id += 1
            bar_cnt = 0
            h_curr = 0.0
            l_curr = 0.0

    ret_highs = closed_highs[:k_closed].copy()
    ret_lows = closed_lows[:k_closed].copy()

    return out, ret_highs, ret_lows, sk_closed, sd_closed, mk_closed, md_closed, last_rsv13, last_rsv104, bar_cnt, h_curr, l_curr, last_bucket_id


# 测试 1: 随机生成 500 根模拟 K 线
np.random.seed(42)
N = 500
prices = 10.0 + np.cumsum(np.random.randn(N) * 0.2)
highs = prices + np.random.rand(N) * 0.1
lows = prices - np.random.rand(N) * 0.1
closes = prices + (np.random.rand(N) - 0.5) * 0.1

# 测试 2d 周期: 每 2 根闭合一次 (bar_seq + 1) % 2 == 0
flags_2d = np.array([(i + 1) % 2 == 0 for i in range(N)], dtype=np.bool_)

# 1. 全量计算
empty_f64 = np.empty(0, dtype=np.float64)
full_out, f_highs, f_lows, f_sk, f_sd, f_mk, f_md, f_r13, f_r104, f_cnt, f_h, f_l, f_bid = compute_kdj_rule_a(
    highs, lows, closes, flags_2d,
    empty_f64, empty_f64,
    50.0, 50.0, 50.0, 50.0,
    50.0, 50.0,
    0, 0.0, 0.0,
    -1
)

# 2. 分段增量计算: 前 350 根 + 后 150 根
split = 350
part1_out, p1_highs, p1_lows, p1_sk, p1_sd, p1_mk, p1_md, p1_r13, p1_r104, p1_cnt, p1_h, p1_l, p1_bid = compute_kdj_rule_a(
    highs[:split], lows[:split], closes[:split], flags_2d[:split],
    empty_f64, empty_f64,
    50.0, 50.0, 50.0, 50.0,
    50.0, 50.0,
    0, 0.0, 0.0,
    -1
)

part2_out, p2_highs, p2_lows, p2_sk, p2_sd, p2_mk, p2_md, p2_r13, p2_r104, p2_cnt, p2_h, p2_l, p2_bid = compute_kdj_rule_a(
    highs[split:], lows[split:], closes[split:], flags_2d[split:],
    p1_highs, p1_lows,
    p1_sk, p1_sd, p1_mk, p1_md,
    p1_r13, p1_r104,
    p1_cnt, p1_h, p1_l,
    p1_bid
)

combined_out = np.vstack([part1_out, part2_out])
max_diff = np.max(np.abs(full_out - combined_out))
print(f"Max difference between full and incremental: {max_diff}")
assert max_diff == 0.0 or max_diff < 1e-12, "Incremental does not match full!"

# 测试逐根 step 增量计算 (后 150 根一根一根灌入)
step_out_list = []
cur_highs, cur_lows = p1_highs, p1_lows
cur_sk, cur_sd, cur_mk, cur_md = p1_sk, p1_sd, p1_mk, p1_md
cur_r13, cur_r104 = p1_r13, p1_r104
cur_cnt, cur_h, cur_l, cur_bid = p1_cnt, p1_h, p1_l, p1_bid

for idx in range(split, N):
    single_out, cur_highs, cur_lows, cur_sk, cur_sd, cur_mk, cur_md, cur_r13, cur_r104, cur_cnt, cur_h, cur_l, cur_bid = compute_kdj_rule_a(
        highs[idx:idx+1], lows[idx:idx+1], closes[idx:idx+1], flags_2d[idx:idx+1],
        cur_highs, cur_lows,
        cur_sk, cur_sd, cur_mk, cur_md,
        cur_r13, cur_r104,
        cur_cnt, cur_h, cur_l,
        cur_bid
    )
    step_out_list.append(single_out[0])

step_combined = np.vstack([part1_out, np.array(step_out_list)])
step_diff = np.max(np.abs(full_out - step_combined))
print(f"Max difference between full and bar-by-bar step: {step_diff}")
assert step_diff == 0.0 or step_diff < 1e-12, "Bar-by-bar does not match full!"
print("All assertions passed with 100% precision!")
