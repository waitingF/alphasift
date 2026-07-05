# 板块主力流排行（board-flow）

基于本地 `flow_bars`（Tushare `moneyflow`）与 `industry_map.csv`（AkShare 行业/概念成分映射），按**行业**和**概念**汇总主力净流入，并列出板块内个股 Top。

## 口径说明

| 项 | 说明 |
|----|------|
| 主力净流入 | 大单 + 特大单净流入，单位 **万元**（与 `flow-bars` / 选股硬筛一致） |
| 默认指标 | `main_net_inflow_5d`：近 **5 个交易日**主力净流入合计 |
| 板块净流入 | 板块内所有**有本地 flow 数据**的成分股，对所选指标 **求和** |
| 板块归属 | 来自 `industry-cache` 的 AkShare 东财板块列表，非 Tushare 官方板块资金流 |
| 概念板块 | 一股可属多个概念，汇总时会在每个概念里各计一次 |

这不是交易所官方「板块资金流」接口（Tushare `moneyflow_ind_dc` 尚未接入），而是**成分股加总**口径。

## 前置准备

### 1. 同步个股资金流库

```bash
export TUSHARE_TOKEN=your_token
export ALPHASIFT_DATA_DIR=./data

# 首次全量（耗时较长）
alphasift flow-bars init --lookback-days 800

# 之后每个交易日收盘后增量
alphasift flow-bars sync

# 可选：检查库状态
alphasift flow-bars status --explain
```

### 2. 生成行业/概念映射

```bash
alphasift industry-cache \
  --max-boards 80 \
  --output data/industry_map.csv \
  --explain
```

默认输出 `${ALPHASIFT_DATA_DIR}/industry_map.csv`，主要字段：

- `code`：6 位股票代码
- `industry`：行业板块（一股通常一个）
- `concepts`：概念板块，逗号分隔（一股可多个）

## 基本用法

### 同时看行业 + 概念 Top（默认）

```bash
alphasift board-flow rank --explain
```

默认行为：

- `--board-type both`：先输出**行业** Top 15，再输出**概念** Top 15
- `--metric main_net_inflow_5d`：按近 5 日主力净流入合计排序
- `--top-stocks 10`：每个板块列出净流入最多的 10 只个股

### 仅看行业板块

```bash
alphasift board-flow rank --board-type industry --explain
```

### 仅看概念板块

```bash
alphasift board-flow rank --board-type concept --explain
```

### 查看单个板块内的个股

```bash
alphasift board-flow rank --board 半导体 --explain
alphasift board-flow rank --board 银行 --board-type industry --explain
```

`--board` 为**精确匹配**板块名称（与 `industry_map.csv` / 东财板块名一致）。

## 指标选择

| `--metric` | 含义 |
|------------|------|
| `main_net_inflow_5d` | **默认** 近 5 日累计主力净流入（万元） |
| `main_net_inflow` | 最近一个交易日主力净流入 |
| `main_net_inflow_10d` | 近 10 日累计 |
| `main_net_inflow_20d` | 近 20 日累计 |

示例：看最近一个交易日谁最强

```bash
alphasift board-flow rank --metric main_net_inflow --top-boards 20 --explain
```

## JSON 输出（便于脚本消费）

```bash
alphasift board-flow rank --json > data/board_flow_rank.json
```

JSON 结构要点：

- `boards[]`：`board_type`、`board`、`flow_sum`、`flow_mean`、`stock_count`
- `constituents["industry:银行"]` / `constituents["concept:AI算力"]`：板块内个股列表
- `notes[]`：数据覆盖率等提示

## 常用参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `--board-type` | `both` | `industry` / `concept` / `both` |
| `--metric` | `main_net_inflow_5d` | 排序与汇总字段 |
| `--top-boards` | `15` | 每类板块输出 Top N |
| `--top-stocks` | `10` | 每个板块内个股 Top N |
| `--mapping` | `data/industry_map.csv` | 板块成分映射文件 |
| `--lookback-days` | `60` | 读取本地 flow 的历史窗口 |
| `--explain` | - | 人类可读摘要 |
| `--json` | - | JSON 输出 |

## 输出示例（explain）

```text
metric=main_net_inflow_5d flow=buy_lg+buy_elg-sell_lg-sell_elg (万元) stocks=4821 membership=152340
flow_store=./data/flow_bars
mapping=./data/industry_map.csv

=== 行业板块 Top (main_net_inflow_5d 合计, 万元) ===
半导体              sum=    125430.50 mean=  852.30 count=147 as_of=2026-04-05
  code    ts_code     main_net_inflow_5d  streak
  600584  600584.SH        12500.00  5
  ...

=== 概念板块 Top (main_net_inflow_5d 合计, 万元) ===
AI算力              sum=     98200.00 mean=  910.20 count=108 as_of=2026-04-05
  ...
```

## 推荐工作流

```bash
# 每日收盘后（flow 约 19:00 后更新）
alphasift flow-bars sync

# 每周或映射过期时刷新板块成分（可选）
alphasift industry-cache --output data/industry_map.csv

# 查看近期资金流入最多的行业/概念及龙头个股
alphasift board-flow rank --explain

# 对感兴趣的板块深入看成分
alphasift board-flow rank --board 半导体 --top-stocks 20 --explain
```

## 与选股 pipeline 衔接

1. 用 `board-flow rank` 找出近期净流入多的板块
2. 对板块内个股用策略硬筛，例如：

```bash
alphasift daily-bars sync
alphasift flow-bars sync
alphasift screen main_inflow_momentum --no-llm --explain
```

`main_inflow_momentum` 使用同一 Tushare 主力口径；其中「价涨量出」guard 需本地 **daily-bars** 提供收盘价。策略**不会**自动按板块过滤；若只筛某板块成分，需自行准备 code 列表或后续扩展策略。

## 局限与注意

1. **覆盖率**：`flow-bars init` 未覆盖的股票不会进入板块汇总；关注输出中的 `stock_count`。
2. **概念重复**：同一股票属于多个概念时，每个概念的 `flow_sum` 都会包含该股。
3. **板块名依赖 mapping**：板块名须与 `industry_map.csv` 一致；可先 `industry-cache --explain` 确认。
4. **非官方板块流**：与东财 `hotspot` 的实时 `net_inflow` 口径不同；本命令与 `flow-bars` 选股一致。

## 相关命令

| 命令 | 用途 |
|------|------|
| `alphasift flow-bars sync` | 更新个股主力流本地库 |
| `alphasift industry-cache` | 更新行业/概念成分映射 |
| `alphasift hotspots` | 东财实时板块热度（不同口径） |
| `alphasift screen main_inflow_momentum` | 个股主力流硬筛选股 |

更多配置见 [configuration.md](configuration.md) 与 [2026-07-05-flow-bars-migration.md](plans/2026-07-05-flow-bars-migration.md)。
