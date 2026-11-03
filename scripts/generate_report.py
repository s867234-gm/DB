import json
import os

with open(r"D:\DB\docs\tables_metadata.json", "r", encoding="utf-8") as f:
    tables_meta = json.load(f)

with open(r"D:\DB\docs\inspection_details.json", "r", encoding="utf-8") as f:
    details = json.load(f)

base_tables = [t for t in tables_meta if t['table_type'] == 'BASE TABLE']
views = [t for t in tables_meta if t['table_type'] == 'VIEW']

md = []

md.append("# A 股多周期快照数据库（A_DB）阶段 1 准备与探查报告\n")
md.append("> **报告生成时间**: 2026-11-04  \n> **探查对象**: 本地源数据库 `D:\\database\\a_stock_v1.duckdb` (只读模式打开，文件大小约 48.0 GB)  \n> **目标工程**: `D:\\DB` -> [https://github.com/s867234-gm/DB](https://github.com/s867234-gm/DB)\n\n---\n")

# 一、环境检查
md.append("## 一、Python 与系统运行环境检查结果\n")
md.append("| 组件 / 工具 | 当前检测版本 | 要求状态 | 备注说明 |")
md.append("| :--- | :--- | :--- | :--- |")
md.append("| **Python** | `3.12.10` | 正常 | 64-bit Windows 环境 |")
md.append("| **duckdb** | `1.5.5` | 已安装 | 支持高效只读/并发 OLAP 分析 |")
md.append("| **mootdx** | `0.11.7` | 已安装 | 通达信行情/本地读取协议支持 |")
md.append("| **pandas** | `2.3.3` | 已安装 | 数据处理与清洗核心库 |")
md.append("| **numpy** | `2.4.6` | 已安装 | 向量化高性能计算支持 |")
md.append("| **numba** | `0.66.0` | 已安装 | JIT 加速支持 |")
md.append("| **pyarrow** | `25.0.1` | 已安装 | 列式存储与零拷贝序列化 |")
md.append("| **git** | `2.55.0.windows.3` | 正常 | 版本管理工具 |")
md.append("| **gh (GitHub CLI)** | `2.100.0` | 正常 | 已登录账号 `s867234-gm` (具备 repo 权限) |")
md.append("\n**GitHub 远程仓库状态**：已成功使用 `gh` 创建并绑定远程仓库 `https://github.com/s867234-gm/DB` (Public)，本地远程分支为 `origin`。\n\n---\n")

# 二、工程目录与 Git 初始化
md.append("## 二、工程目录与 Git 仓库初始化\n")
md.append("已在 `D:\\DB` 下完成初始化，规范目录结构如下：")
md.append("```text")
md.append("D:\\DB")
md.append("├── .git/")
md.append("├── .gitignore               # 已配置忽略 *.duckdb, *.duckdb.wal, *.parquet, __pycache__, logs 等")
md.append("├── docs/                    # 阶段技术文档与探查报告")
md.append("│   └── phase1_inspection_report.md")
md.append("├── scripts/                 # 探查、构建与迁移辅助脚本")
md.append("├── src/                     # 生产业务代码 (ETL、快照构建、校验)")
md.append("│   └── __init__.py")
md.append("└── tests/                   # 单元测试与完整性断言测试")
md.append("    └── __init__.py")
md.append("```\n---\n")

# 三、源数据库结构
md.append("## 三、源数据库架构全景（Tables & Views）\n")
md.append(f"源数据库共包含 **{len(base_tables)} 张物理基表（BASE TABLE）** 与 **{len(views)} 个视图（VIEW）**。\n")

md.append("### 3.1 物理基表清单（BASE TABLE）\n")
md.append("| 表名 (Table Name) | 行数 (Total Rows) | 字段数 (Cols) | 主要职责与业务含义 |")
md.append("| :--- | :--- | :--- | :--- |")

base_desc = {
    'daily_bar': '日线原始行情数据（全市场个股 + 指数历史日 K）',
    'min15_bar': '15分钟级别行情数据（开高低收量额、slot_seq 时间片槽位）',
    'adj_factor': '前复权因子日频表（累乘因子，最新交易日对齐为 1.0）',
    'corporate_action': '除权除息原始事件明细表（分红派息、送股、配股、拆细等）',
    'stock_basic': '个股基础字典信息（代码、名称、上市退市日、所属板块状态）',
    'index_basic': '指数字典信息（宽基指数、通达信行业分类指数）',
    'trading_calendar': '交易日历表（交易所交易日标记、前后有效开盘日映射）',
    'turn1_daily': '换手率日频计算衍生表',
    'stock_daily_feature_snapshot': '日线多周期特征快照宽表（含 d1~d32 预计算前复权及衍生指标）',
    'stock_intraday_feature_snapshot': '日内多周期特征快照宽表（含 m15~m120 预计算前复权指标）',
    'stock_high_period_structure_snapshot': '高周期结构特征快照表（d2~d32 顶底结构与趋势状态）',
    'stock_pivot_event': '顶底分型/极值转折确认事件流',
    'stock_pivot_state': '顶底分型跟踪机运行状态持久化',
    'factor_chain_seed': '多周期因子计算链路种子表',
    'factor_state': '增量因子计算中间状态 JSON',
    'data_version': '数据快照与因子版本元数据表',
    'publication': '快照发布批次记录',
    'update_log': '增量同步运行日志',
    'data_source': '行情源配置登记表'
}

for t in base_tables:
    name = t['table_name']
    r_count = f"{t['row_count']:,}" if isinstance(t['row_count'], int) else str(t['row_count'])
    c_count = len(t['columns'])
    desc = base_desc.get(name, '内部衍生表/辅助配置表')
    md.append(f"| `{t['schema']}.{name}` | {r_count} | {c_count} | {desc} |")

md.append("\n### 3.2 视图分类汇总（VIEW）\n")
md.append("源数据库内建的 50 个视图主要承载以下职责：")
md.append("1. **前复权快速查询视图**：`v_day_qfq`, `v_min15_qfq_current`, `v_min15_qfq_done`, `v_min30_qfq_done` 等。")
md.append("2. **多周期聚合与因子视图**：`fac_day`, `fac_day_2d`~`32d`, `fac_min15`~`min120`。")
md.append("3. **数据完整性与监控视图**：`v_calendar_coverage`, `v_intraday_bucket_gaps`, `v_snapshot_coverage`, `v_symbol_freshness`。")
md.append("4. **版本与对外兼容合约**：`contract_v1.daily_bar`, `contract_v1.stock_basic`, `contract_v1.trading_calendar`。\n\n---\n")

# 四、标的类型与代码格式分析
md.append("## 四、标的类型与代码规范统计\n")
md.append("### 4.1 标的分类与数量\n")
md.append(f"- **个股总数**: **{details['stock_count']:,}** 只（`stock_basic` 表中收录）。")
md.append(f"- **指数总数**: **{details['index_count']}** 只（`index_basic` 表中收录）。")
md.append(f"- **行情总覆盖标的数**: `daily_bar` 与 `min15_bar` 中均覆盖 **{details['daily_bar_stats']['symbol_count']:,}** 只标的（5,228 只个股 + 107 只核心指数，完全闭环对齐）。\n")

md.append("### 4.2 指数分类统计\n")
md.append("| 指数分类 (`index_category`) | 数量 | 包含标的特征与举例 |")
md.append("| :--- | :--- | :--- |")
md.append("| **宽基指数 (BROAD)** | 8 只 | 上证指数(`000001`)、深证成指(`399001`)、**中小100(`399005`)**、创业板指(`399006`)、上证50(`000016`)、科创50(`000688`)、沪深300(`000300`)、中证500(`000905`) |")
md.append("| **通达信一级行业 (INDUSTRY_L1)** | 44 只 | `880301`~`880497`（如煤炭、石油、有色、半导体、医药、房地产、证券等） |")
md.append("| **通达信二级行业 (INDUSTRY_L2)** | 55 只 | `880302`~`880486`（如白酒、铜、化学制药、证券软件、影视院线等细分行业） |")

md.append("\n### 4.3 核心标的针对性核验\n")
md.append("1. **通达信 880xxx 行业板块指数**：")
md.append("   - `index_basic` 字典表中已收录 **99** 只标准通达信一级和二级行业指数（代码格式为纯 6 位数字 `880xxx`）。")
md.append(f"   - 在历史日线行情表 `daily_bar` 中，实际记录了 **{details['tdx_880_in_daily_count']}** 只 `880xxx` 标的（除 99 只行业指数外，还保留了部分历史扩展概念与细分板块行情）。")
md.append("2. **中小板指 399005（中小100）**：")
md.append("   - **已完整存在**！在 `index_basic` 中登记为 `symbol='399005'`, `index_name='中小100'`, `index_category='BROAD'`。")
md.append(f"   - 在 `daily_bar` 中有 **{details['idx_399005_in_daily_count']:,}** 条历史日线数据，历史数据完整。\n")

md.append("### 4.4 标的代码命名格式规范\n")
md.append("| 标的类别 | 数据表字段 | 格式示例 | 规律特征 |")
md.append("| :--- | :--- | :--- | :--- |")
md.append("| **个股 (Stock)** | `symbol` | `SZ000002`, `SH600000`, `BJ830799` | 交易所大写英文缩写前缀 + 6 位数字 |")
md.append("| **个股 (Stock)** | `code` | `000002`, `600000`, `830799` | 纯 6 位数字代码 |")
md.append("| **指数 (Index)** | `symbol` | `000001`, `399005`, `880301` | **统一为纯 6 位数字代码**（无 SH/SZ 前缀） |")
md.append("| **行情表引用** | `daily_bar.symbol` / `min15_bar.symbol` | 个股为带前缀（如 `SZ000002`），指数为纯 6 位数字（如 `000001`） | 在设计统一 A_DB 时需评估是否需要统一规范化编码前缀 |\n\n---\n")

# 五、日线与 15 分钟线时间覆盖与复权机制
md.append("## 五、行情时间跨度与复权存储机制\n")
md.append("### 5.1 行情时间跨度\n")
md.append("| 周期级别 | 最早时间 (Min Date/Time) | 最新时间 (Max Date/Time) | 标的覆盖数 | 总行数 (Total Rows) | 说明 |")
md.append("| :--- | :--- | :--- | :--- | :--- | :--- |")
d_stats = details['daily_bar_stats']
m_stats = details['min15_bar_stats']
md.append(f"| **日线 (`daily_bar`)** | `{d_stats['min_date']}` | `{d_stats['max_date']}` | {d_stats['symbol_count']:,} | {d_stats['total_rows']:,} | 涵盖 A 股自 1990 年开市以来 8,809 个历史交易日 |")
md.append(f"| **15分钟线 (`min15_bar`)** | `{m_stats['min_bar_end']}` | `{m_stats['max_bar_end']}` | {m_stats['symbol_count']:,} | {m_stats['total_rows']:,} | 涵盖约 2 年多高频日内数据（从 2024-09-06 起，每交易日 16 个 slot） |\n")

md.append("### 5.2 复权因子与除权机制存储原理\n")
md.append("源数据库采用了**双轨制**并辅以**快照预计算**的复权体系：\n")
md.append("1. **累计前复权因子表 (`adj_factor`)**：")
md.append("   - 行数：**16,777,551** 行，覆盖全部 5,228 只股票自上市以来的所有交易日。")
md.append("   - 核心字段：`symbol`, `trade_date`, `factor`。")
md.append("   - **机制判定**：经对 SZ000002（万科A）全历史数据核算验证，此表为**前复权累乘因子**。")
md.append("     - 最新交易日（2026-10-30）`factor = 1.0`。")
md.append("     - 历史上市首日（1991-01-29）`factor = 0.005579`。")
md.append("     - **计算公式**：`前复权价格 = 原始未复权价格 × factor`。")
md.append("     - 例如：1991-01-29 原始收盘价 14.58 元，前复权后价格为 `14.58 × 0.005579 ≈ 0.08134 元`。")
md.append("2. **原始除权除息事件表 (`corporate_action`)**：")
md.append("   - 行数：**179,285** 条记录，覆盖 1990-03-01 至 2026-11-06 的全部分红、送转股、配股明细。")
md.append("   - 核心字段：`symbol`, `event_date`, `category`, `cash_dividend`, `bonus_share`, `rights_share`, `rights_price`。")
md.append("   - 职责：供复权因子引擎动态重算因子链，保障因子具备从事件源重新追溯校验的能力。")
md.append("3. **快照前复权加速宽表 (`stock_daily_feature_snapshot` / `stock_intraday_feature_snapshot`)**：")
md.append("   - 已经将前复权的高开低收成交量直接物化在列中（如 `d1_open_qfq`, `d1_close_qfq`, `m15_open_qfq` 等），并在外部通过 `v_day_qfq` 等视图提供零开销查询。\n\n---\n")

# 六、核心表前 5 行与后 5 行抽样
md.append("## 六、核心数据表抽样明细（前 5 行与后 5 行）\n")

samples = details['core_tables_samples']
for table_name in ['stock_basic', 'index_basic', 'daily_bar', 'min15_bar', 'adj_factor', 'corporate_action', 'trading_calendar', 'data_version']:
    if table_name not in samples or not isinstance(samples[table_name], dict):
        continue
    info = samples[table_name]
    sort_by = info.get('sort_by', 'default')
    md.append(f"### 6.{table_name}（排序键：`{sort_by}`）\n")
    
    head_rows = info.get('head_5', [])
    tail_rows = info.get('tail_5', [])
    
    if head_rows:
        cols = list(head_rows[0].keys())
        # 若字段太多只展示前 8 个主要字段
        display_cols = cols[:8] if len(cols) > 8 else cols
        
        md.append(f"**【前 5 行数据 (Head 5)】**\n")
        md.append("| " + " | ".join(display_cols) + " |")
        md.append("| " + " | ".join([":---" for _ in display_cols]) + " |")
        for r in head_rows:
            vals = [str(r.get(c, '')).replace('\n', ' ') for c in display_cols]
            md.append("| " + " | ".join(vals) + " |")
        md.append("\n")
        
        md.append(f"**【后 5 行数据 (Tail 5)】**\n")
        md.append("| " + " | ".join(display_cols) + " |")
        md.append("| " + " | ".join([":---" for _ in display_cols]) + " |")
        for r in tail_rows:
            vals = [str(r.get(c, '')).replace('\n', ' ') for c in display_cols]
            md.append("| " + " | ".join(vals) + " |")
        md.append("\n")

# 七、阶段 2 迁移建议
md.append("---\n## 七、架构师审查总结与后续建议\n")
md.append("1. **标的编码一致性建议**：源库中个股使用 `SZ000001` 而指数使用 `000001` / `880301`。建议在 A_DB 设计时设立统一的标准标的代码体系（例如统一带有市场后缀或前缀，或在字典表建立严谨映射）。")
md.append("2. **存储分层与轻量化建议**：源库 48GB 中，`min15_bar`（4330万行）、`stock_intraday_feature_snapshot`（4243万行）与 `stock_daily_feature_snapshot`（1677万行）占据了 90% 以上空间。新库 `A_DB.duckdb` 在构建时应明确是否全量物化快照，还是采用高效 Parquet 外表或按需视图计算。")
md.append("3. **增量通道与工具链**：本地已安装 `mootdx 0.11.7` 和 `duckdb 1.5.5`，已具备连接通达信最新行情服务器进行后续增量更新的技术条件。\n")

report_text = "\n".join(md)
with open(r"D:\DB\docs\phase1_inspection_report.md", "w", encoding="utf-8") as f:
    f.write(report_text)

print(f"Report written successfully to D:\\DB\\docs\\phase1_inspection_report.md (Size: {len(report_text)} chars)")
