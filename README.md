# A-Share Multi-Cycle Snapshot Database (A_DB)

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.5.5-yellow.svg)](https://duckdb.org/)
[![Numba](https://img.shields.io/badge/Numba-JIT-orange.svg)](https://numba.pydata.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A 股全市场高频多周期快照时序数据库。基于 **DuckDB 列式存储引擎** 与 **Numba JIT 规则 A（动态快照 + 已闭合桶前值锚定）增量计算引擎**，覆盖沪深两市全部 **5,335 只标的**（5,228 只 A 股个股 + 107 只核心指数与通达信行业指数），支持日线（1d, 2d, 4d, 8d, 16d, 32d）与 15 分钟线（15m, 30m, 60m, 120m）共 10 个周期的实时 KDJ 指标平铺，并具备幂等增量自动同步能力。

---

## 一、系统架构与技术特色

```
                        ┌──────────────────────────────────────────────┐
                        │ 通达信券商主站集群 (TDX 7709 协议 / mootdx)    │
                        └──────────────────────┬───────────────────────┘
                                               │
                                               ▼
                                    ┌──────────────────────┐
                                    │   src/fetcher.py     │
                                    │ (自动验活 / 故障切换) │
                                    └──────────┬───────────┘
                                               │
                                               ▼
                                    ┌──────────────────────┐
                                    │   src/updater.py     │
                                    │ (幂等增量更新调度器)   │
                                    └──────────┬───────────┘
                                               │ 恢复 incremental_state
                                               ▼
                                    ┌──────────────────────┐
                                    │  src/kdj_engine.py   │
                                    │ (Numba JIT 规则 A)   │
                                    └──────────┬───────────┘
                                               │ 原子事务 (BY NAME)
                                               ▼
                        ┌──────────────────────────────────────────────┐
                        │             D:\DB\A_DB.duckdb                │
                        │  5 张核心物理表 + 2 个前复权便捷视图          │
                        └──────────────────────────────────────────────┘
```

1. **零冗余极简架构**：告别传统数据库数十张分表与过度嵌套视图，全库仅收录 **5 张物理表**（标的字典、除权事件、日线快照、分钟快照、增量状态）和 **2 个前复权便捷视图**。
2. **规则 A 动态快照 + 已闭合桶前值锚定**：
   - 彻底解决传统软件在大周期桶未闭合时 KDJ 指标“闪烁漂移”的问题。
   - 未闭合桶的 $K, D$ 计算严格以**上一已闭合桶的终值**为迭代前值，盘中每一根 bar 的价格变动均生成客观、可复现的动态快照。
   - 桶正式闭合瞬间，终值固化并压入历史序列，成为下一桶的前值锚点。
3. **Numba JIT 纳秒级吞吐**：底层全部采用 `float64` 连续内存数组与 JIT 编译循环，全量 5,335 只标的（1,700 万行日线 + 4,200 万行分钟线）全量计算构建仅用时约 5 分钟。
4. **历史复权因子永久锁定 + 增量等比推进**：
   - 历史行 `adj_factor` 一旦入库永久固化，杜绝除权除息导致历史整表重算。
   - 视图中前复权价格动态除以 `security_info.latest_adj_factor`，既保证历史复权价格严谨连续，又免去百万级存量重写。
5. **绝对幂等性与断点续传**：
   - 增量调度器自动探测各标的在日线与 15 分钟槽位（1~16）的入库边界。
   - 重复运行、盘中多次更新绝不新增重复记录、绝不导致中间状态漂移。

---

## 二、工程目录规范

```text
D:\DB\
├── src/
│   ├── schema.py           # 数据库 DDL、表结构定义与初始化
│   ├── kdj_engine.py       # Numba JIT 规则 A 多周期 KDJ 引擎与状态序列化
│   ├── fetcher.py          # 通达信券商高可用连接池、行情与除权采集器
│   ├── builder.py          # 阶段 3 全市场历史数据全量构建引擎
│   └── updater.py          # 阶段 4 幂等增量更新调度器
├── scripts/
│   ├── pilot_verify.py     # 阶段 2 核心样本指标与规则 A 核验脚本
│   ├── run_phase3_full_build.py # 全量构建入口
│   ├── run_daily_update.py # 日常增量更新命令行运行入口
│   ├── verify_phase4.py    # 阶段 4 增量同步与全库体检核验脚本
│   └── verify_full_db.py   # 全库数据质量核验与健康体检脚本
├── tests/
│   ├── test_kdj_engine.py  # KDJ 计算精度、列对齐与状态步进测试
│   ├── test_updater.py     # 增量幂等性、除权推进与槽位对齐测试
│   └── test_views.py       # 前复权便捷视图与共振选股逻辑测试
├── docs/
│   ├── phase1_inspection_report.md  # 阶段 1 源库探查报告
│   ├── phase2_pilot_report.md       # 阶段 2 小样本核验报告
│   ├── phase3_full_build_report.md  # 阶段 3 全市场全量构建报告
│   └── phase4_update_report.md      # 阶段 4 增量更新与实战验收报告
├── update_db.bat           # Windows 批处理一键更新脚本
├── .gitignore              # 忽略 DuckDB 二进制与日志缓存
└── README.md               # 本项目完整技术文档
```

---

## 三、数据字典 (Data Dictionary)

### 1. 物理表 `security_info`（标的资产元数据）
- **主键**: `symbol`
- **总行数**: 5,335 行（包含沪深 A 股 5,228 只，上证系列/深证成指/中小100等 8 只宽基指数，99 只通达信行业指数；北交所标的数为 0）

| 字段名 | 类型 | 说明 | 示例 |
| :--- | :--- | :--- | :--- |
| `symbol` | VARCHAR (PK) | 统一规范编码（沪市加 SH，深市加 SZ，通达信指数加 TDX） | `'SZ000001'`, `'SH600000'`, `'TDX880301'` |
| `code` | VARCHAR | 原始 6 位纯数字代码 | `'000001'`, `'600000'`, `'880301'` |
| `name` | VARCHAR | 证券简称 | `'平安银行'`, `'浦发银行'`, `'煤炭开采'` |
| `sec_type` | VARCHAR | 标的类别 (`STOCK`, `BROAD_INDEX`, `INDUSTRY_INDEX`) | `'STOCK'` |
| `exchange` | VARCHAR | 交易所 (`SSE`, `SZSE`, `TDX`) | `'SZSE'` |
| `board` | VARCHAR | 板块 (`MAIN`, `CYB`, `KCB`, `INDEX`) | `'MAIN'` |
| `list_date` | DATE | 上市日期 | `1991-04-03` |
| `delist_date` | DATE | 退市日期（未退市为 NULL） | `NULL` |
| `latest_adj_factor` | DOUBLE | 最新复权因子（增量更新时自动刷新） | `1.0`, `112.435` |
| `updated_at` | TIMESTAMP | 元数据最后更新时间戳 | `2026-11-04 14:15:00` |

### 2. 物理表 `corporate_action`（除权除息与股本事件）
- **主键**: `(symbol, event_date, category)`

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `symbol` | VARCHAR | 标的代码 |
| `event_date` | DATE | 除权除息日 |
| `category` | INTEGER | 事件类别（1: 除权除息） |
| `cash_dividend` | DOUBLE | 每股现金分红（元） |
| `bonus_share` | DOUBLE | 每股送转股（股） |
| `rights_share` | DOUBLE | 每股配股（股） |
| `rights_price` | DOUBLE | 配股价（元） |

### 3. 物理表 `daily_snapshot`（日线级多周期快照）
- **主键**: `(symbol, trade_date)`
- **覆盖周期**: `1d, 2d, 4d, 8d, 16d, 32d`（6 个周期平铺）
- **前 11 列基础行情**:
  - `symbol, code, trade_date, bar_seq, open, high, low, close, volume, amount, adj_factor`
- **后续 48 列指标**（每个周期包含 8 个指标：`sk, sd, skxl, sdxl, mk, md, mkxl, mdxl`）：
  - `sk_1d, sd_1d, skxl_1d, sdxl_1d, mk_1d, md_1d, mkxl_1d, mdxl_1d`
  - `sk_2d, sd_2d, skxl_2d, sdxl_2d, mk_2d, md_2d, mkxl_2d, mdxl_2d`
  - `sk_4d, sd_4d, skxl_4d, sdxl_4d, mk_4d, md_4d, mkxl_4d, mdxl_4d`
  - `sk_8d, sd_8d, skxl_8d, sdxl_8d, mk_8d, md_8d, mkxl_8d, mdxl_8d`
  - `sk_16d, sd_16d, skxl_16d, sdxl_16d, mk_16d, md_16d, mkxl_16d, mdxl_16d`
  - `sk_32d, sd_32d, skxl_32d, sdxl_32d, mk_32d, md_32d, mkxl_32d, mdxl_32d`

### 4. 物理表 `minute_snapshot`（15分钟级多周期快照）
- **主键**: `(symbol, bar_end)`
- **覆盖周期**: `15m, 30m, 60m, 120m`（4 个周期平铺）
- **前 12 列基础行情**:
  - `symbol, code, trading_date, bar_end, slot_seq, open, high, low, close, volume, amount, adj_factor`
  - `slot_seq`: 当日 15 分钟槽位，取值范围 `1 ~ 16`。
- **后续 32 列指标**（每个周期 8 个指标）：
  - `sk_15m, sd_15m, skxl_15m, sdxl_15m, mk_15m, md_15m, mkxl_15m, mdxl_15m`
  - `sk_30m, sd_30m, skxl_30m, sdxl_30m, mk_30m, md_30m, mkxl_30m, mdxl_30m`
  - `sk_60m, sd_60m, skxl_60m, sdxl_60m, mk_60m, md_60m, mkxl_60m, mdxl_60m`
  - `sk_120m, sd_120m, skxl_120m, sdxl_120m, mk_120m, md_120m, mkxl_120m, mdxl_120m`

### 5. 物理表 `incremental_state`（10 周期增量迭代中间状态）
- **主键**: `(symbol, period)`
- **说明**: 存储每只标的在各个周期下最近闭合桶的高低点滚动窗口（JSON 数组）及闭合终值，确保增量更新无缝衔接历史序列。

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `symbol` | VARCHAR | 标的代码 |
| `period` | VARCHAR | 周期标识 (`1d`, `2d`, `4d`, `8d`, `16d`, `32d`, `15m`, `30m`, `60m`, `120m`) |
| `last_closed_bucket_id` | INTEGER | 最近已闭合桶序号 |
| `closed_highs_adj` | VARCHAR | 最近 104 个已闭合桶前复权最高价 (JSON Array) |
| `closed_lows_adj` | VARCHAR | 最近 104 个已闭合桶前复权最低价 (JSON Array) |
| `sk_closed` | DOUBLE | 最近闭合桶的 SK 终值 |
| `sd_closed` | DOUBLE | 最近闭合桶的 SD 终值 |
| `mk_closed` | DOUBLE | 最近闭合桶的 MK 终值 |
| `md_closed` | DOUBLE | 最近闭合桶的 MD 终值 |
| `last_valid_rsv13` | DOUBLE | 遇到极值重合时的回退 RSV13 |
| `last_valid_rsv104` | DOUBLE | 遇到极值重合时的回退 RSV104 |
| `bar_count` | INTEGER | 当前未闭合桶内已累积的子 bar 计数 |
| `high_adj` | DOUBLE | 当前未闭合桶内累积的最高价 |
| `low_adj` | DOUBLE | 当前未闭合桶内累积的最低价 |
| `updated_at` | TIMESTAMP | 状态最后更新时间 |

### 6. 便捷视图 `v_daily_qfq` 与 `v_minute_qfq`
基于 `daily_snapshot` 和 `minute_snapshot` 动态关联 `security_info.latest_adj_factor`，提供即开即用的前复权行情与完整 48 / 32 列指标：
- `open_qfq = open * adj_factor / latest_adj_factor`
- `high_qfq = high * adj_factor / latest_adj_factor`
- `low_qfq = low * adj_factor / latest_adj_factor`
- `close_qfq = close * adj_factor / latest_adj_factor`

---

## 四、KDJ 规则 A（动态快照 + 闭合桶锚定）算法原理

### 1. 周期与桶结构定义
- **日线系列**：以标的历史首根日线为起点编号 `bar_seq = 0, 1, 2...`。
  - 周期 $N \in \{1, 2, 4, 8, 16, 32\}$：当 `(bar_seq + 1) % N == 0` 时，该桶宣告正式闭合。
- **15 分钟系列**：以交易日内标准槽位编号 `slot_seq \in [1, 16]`。
  - $15m$（每 1 槽闭合）、$30m$（槽号为偶数时闭合）、$60m$（槽号模 4 为 0 时闭合）、$120m$（槽号模 8 为 0 时闭合）。

### 2. 核心数学公式
设当前为桶内第 $t$ 根子 bar，前复权最高、最低、收盘价分别为 $H_t^{adj}, L_t^{adj}, C_t^{adj}$：

1. **当前桶内极值累积**：
   $$H_{curr} = \max_{1 \le i \le t} (H_i^{adj}), \quad L_{curr} = \min_{1 \le i \le t} (L_i^{adj})$$

2. **多周期极值窗口融合**（融合历史已闭合桶与当前未闭合桶）：
   $$HHV_{13} = \max( \text{closed\_highs}[-12:], H_{curr} )$$
   $$LLV_{13} = \min( \text{closed\_lows}[-12:], L_{curr} )$$
   $$HHV_{104} = \max( \text{closed\_highs}[-103:], H_{curr} )$$
   $$LLV_{104} = \min( \text{closed\_lows}[-103:], L_{curr} )$$

3. **动态未闭合 RSV 计算**：
   $$RSV_{13} = \frac{C_t^{adj} - LLV_{13}}{HHV_{13} - LLV_{13}} \times 100$$
   $$RSV_{104} = \frac{C_t^{adj} - LLV_{104}}{HHV_{104} - LLV_{104}} \times 100$$

4. **严格以前一已闭合桶终值锚定推导当前快照**：
   $$SK = 0.5 \times SK_{closed} + 0.5 \times RSV_{13}$$
   $$SD = 0.5 \times SD_{closed} + 0.5 \times SK$$
   $$SKXL = SK - SK_{closed}, \quad SDXL = SD - SD_{closed}$$
   $$MK = \frac{15}{16} \times MK_{closed} + \frac{1}{16} \times RSV_{104}$$
   $$MD = \frac{15}{16} \times MD_{closed} + \frac{1}{16} \times MK$$
   $$MKXL = (MK - MK_{closed}) \times 8.0, \quad MDXL = (MD - MD_{closed}) \times 8.0$$

5. **桶闭合固化事件**：当触发闭合条件时，固化 $SK_{closed} = SK, SD_{closed} = SD, MK_{closed} = MK, MD_{closed} = MD$，并将当前桶的极值压入历史已闭合队列，清空计数器，等待下一桶开启。

---

## 五、日常使用与快速上手

### 1. 命令行增量更新
```bash
# 1. 默认更新：同步至最新已完整收盘的交易日（推荐收盘后 15:10 以后执行）
python scripts/run_daily_update.py

# 2. 盘中更新：允许同步当天盘中已走过的 15 分钟线
python scripts/run_daily_update.py --allow-intraday

# 3. 指定高并发线程与批次
python scripts/run_daily_update.py --workers 16 --batch-size 200
```

### 2. Windows 桌面双击一键更新
直接双击根目录下的 `update_db.bat`，即可自动激活环境并完成最新行情增量同步。

---

## 六、实战 SQL 查询指南

### 1. 极速查询单只标的前复权日线与多周期指标
```sql
SELECT 
    symbol, trade_date, open_qfq, high_qfq, low_qfq, close_qfq, volume,
    sk_1d, sd_1d, skxl_1d,
    sk_4d, sd_4d, skxl_4d,
    sk_16d, sd_16d, skxl_16d
FROM v_daily_qfq
WHERE symbol = 'SZ000001'
ORDER BY trade_date DESC
LIMIT 10;
```

### 2. 10 周期 KDJ 共振金叉强势选股实战 SQL
在量化多周期策略中，当**大周期处于上升趋势（多头向上）**且**中小周期同时出现金叉或斜率共振**时，通常代表极强的动量爆发点。以下 SQL 可直接在 DuckDB 中毫秒级筛选出日线（1d, 4d, 16d）与分钟线（15m, 60m）共振向上的标的：

```sql
WITH latest_daily AS (
    SELECT 
        symbol, trade_date, close,
        sk_1d, sd_1d, skxl_1d,
        sk_4d, sd_4d, skxl_4d,
        sk_16d, sd_16d, skxl_16d
    FROM daily_snapshot
    WHERE trade_date = (SELECT MAX(trade_date) FROM daily_snapshot)
),
latest_minute AS (
    SELECT 
        symbol, trading_date, slot_seq,
        sk_15m, sd_15m, skxl_15m,
        sk_60m, sd_60m, skxl_60m
    FROM minute_snapshot
    WHERE (trading_date, slot_seq) = (
        SELECT trading_date, slot_seq 
        FROM minute_snapshot 
        ORDER BY trading_date DESC, slot_seq DESC 
        LIMIT 1
    )
)
SELECT 
    s.symbol,
    s.name,
    s.board,
    d.close AS latest_close,
    -- 日线指标
    ROUND(d.sk_1d, 2) AS sk_1d,
    ROUND(d.sd_1d, 2) AS sd_1d,
    ROUND(d.sk_4d, 2) AS sk_4d,
    ROUND(d.sk_16d, 2) AS sk_16d,
    -- 分钟指标
    ROUND(m.sk_15m, 2) AS sk_15m,
    ROUND(m.sk_60m, 2) AS sk_60m
FROM latest_daily d
JOIN latest_minute m ON d.symbol = m.symbol
JOIN security_info s ON d.symbol = s.symbol
WHERE s.sec_type = 'STOCK'
  -- 1. 大周期处于多头安全区
  AND d.sk_16d >= d.sd_16d 
  AND d.skxl_16d > 0
  -- 2. 中周期向上发散
  AND d.sk_4d >= d.sd_4d
  AND d.skxl_4d > 0
  -- 3. 日线 1d 处于金叉发力阶段
  AND d.sk_1d > d.sd_1d
  AND d.skxl_1d > 1.5
  -- 4. 15分钟短周期共振向上
  AND m.sk_15m > m.sd_15m
  AND m.skxl_15m > 0
ORDER BY d.skxl_1d DESC
LIMIT 50;
```

---

## 七、质量保证与测试体系

本项目配有完备的自动化单元测试（覆盖算法精度、列对齐、状态无缝衔接与幂等性）：

```bash
# 执行全部单元测试
pytest tests/ -v
```

- `test_rule_a_unclosed_and_closed_logic`: 严格断言未闭合桶以前值锚定，闭合瞬间固化。
- `test_daily_incremental_matches_full`: 验证分批增量迭代与一次性全量计算的数值误差为 0。
- `test_minute_incremental_matches_full`: 验证分钟级别跨日跨槽位增量结果与全量结果绝对一致。
- `test_column_alignment_and_by_name_insertion`: 验证 DuckDB `BY NAME` 按列名映射，防止位置错位。
- `test_calculate_next_adj_factor`: 验证除权除息等比递推计算模型。
- `test_updater_idempotence`: 模拟同一批增量行情连续执行两次，断言行数零增加且状态哈希完全相等。
