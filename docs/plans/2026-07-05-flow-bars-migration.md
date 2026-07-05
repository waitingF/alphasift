# 方案：Tushare 资金流本地库 + 硬筛集成（self-stock-project 移植）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `self-stock-project` 中已实现的 Tushare `moneyflow` 日频本地库、衍生指标与资金面条件，移植到 alphasift；在快照硬筛后对候选池做资金流特征增强与硬筛，并升级 LLM 候选上下文中的 `fund_flow` 为本地结构化口径。数据目录**仅**通过 `ALPHASIFT_DATA_DIR` 统一配置，其下固定子目录区分 daily / flow，不再引入独立的 `FLOW_BARS_DIR` / `DAILY_BARS_DIR` 作为主配置项。

**Architecture:** 新增 `FlowBarStore` + `flow-bars sync` CLI（镜像现有 `DailyBarStore` + `daily-bars` 范式）；移植 `stock_flow_engine` 纯计算层为 `alphasift/flow_metrics.py` 等；`pipeline.screen()` 在 `flow_needed` 时采用与 daily 相同的两阶段硬筛；`candidate_context` 本地库优先、AkShare 降级。

**Tech Stack:** Python 3.11+、pandas、pyarrow（Parquet）、tushare SDK、alphasift 现有 `daily_store` / `daily_sync` / `filter` / `pipeline` 模式。

**Source reference:** `self-stock-project` 设计文档 `docs/2026-07-02-main-force-capital-flow-design.md`（P0–P2 已实现）。

---

## Global Constraints

- 硬条件仍由代码执行，LLM 不参与资金流阈值判断（对齐 `docs/design.md` 与日 K 方案）。
- **单行** flow 读取/特征失败采用 **per-row soft-fail**：写入 `flow_quality_flags=missing|stale`，硬筛淘汰该行，**不** abort 整次 screen。
- 测试不得依赖 live 网络；Tushare 同步逻辑用 monkeypatch/fake pro API。
- 不移植 `self-stock-project` 的 Web 层（`flow_service` / `CombinedScanService`）；能力通过 CLI + `screen()` 暴露。
- 不引入 `stock_data_store` 子模块；只抽取 `flow.py` 同步语义，按 alphasift 风格重写为 Parquet 存储。
- 「主力」口径固定为 **大单 + 特大单净流入（万元）**，UI/audit 须标注，保留 `net_mf_amount` 作对照。
- Tushare `moneyflow` 为 **日频盘后**（约 19:00 后更新），非实时 L2。
- 积分门槛：**2000**（仅 `moneyflow`；大盘/板块官方接口 P3+ 不在首期范围）。
- 文档、`.env.example`、`docs/configuration.md` 与实现同步更新。

---

## 1. 背景与问题

### 1.1 alphasift 现状

| 能力 | 实现 | 局限 |
|------|------|------|
| LLM 候选上下文 | `candidate_context.fetch_stock_fund_flow_summary()` | 实时 AkShare，仅文本摘要 |
| 热点评分 | `hotspot.py` 的 `net_inflow` | 东财实时字段，口径不一致 |
| 策略 | `capital_heat.yaml` | 用成交额/量比/换手率**代理**资金热度 |
| 本地日 K | `daily_store` + `daily-bars` CLI | **已有成熟范式**（见 `2026-06-26-full-daily-k-hard-filter.md`） |
| 资金流本地库 | 无 | 无法做「5 日主力持续净流入」类硬筛 |

### 1.2 self-stock-project 可移植资产

| 模块 | 职责 | 移植方式 |
|------|------|----------|
| `stock_data_store/services/flow.py` | Tushare 同步、对账、`main_net_inflow` 落盘 | 重写为 `flow_store.py` + `flow_sync.py`（Parquet） |
| `stock_flow_engine/specs.py` | 字段常量、单位 | 几乎原样 → `flow_specs.py` |
| `stock_flow_engine/metrics.py` | 滚动累计、streak、背离 | → `flow_metrics.py` |
| `stock_flow_engine/conditions.py` | 4 个资金面条件 | → `flow_conditions.py` |
| `web_app/services/flow_scan_filter.py` | 技术面后置过滤 | 逻辑内化进 `pipeline.py` 两阶段硬筛 |
| `web_app/*`、组合策略 presets | Web / B1+B2 | **不移植**；用 YAML `hard_filters` 表达 |

### 1.3 非目标

- 实时 L2 盘口、分钟级资金流
- 全市场 5000+ 无快照预筛的 flow 扫描
- `moneyflow_mkt_dc` / `moneyflow_ind_dc`（需 5000–6000 积分，P3+）
- 下单、回测仿真

---

## 2. 统一数据目录规范

### 2.1 原则

**只配置一个根目录 `ALPHASIFT_DATA_DIR`**。daily、flow 及现有缓存/运行产物均在其下**固定子路径**派生，避免 `DAILY_BARS_DIR`、`FLOW_BARS_DIR` 等与根目录重复的 env 项。

```env
# 唯一需要用户关心的数据根目录
ALPHASIFT_DATA_DIR=./data
```

代码中通过 `Config.data_dir` 派生各子路径，**不**在 `.env.example` 中暴露 `DAILY_BARS_DIR` / `FLOW_BARS_DIR` 作为主配置。

### 2.2 目录布局（canonical）

```text
${ALPHASIFT_DATA_DIR}/
├── daily_bars/                 # 本地 Tushare 日 K Parquet 库（已有）
│   ├── manifest.json
│   ├── bars/raw/*.parquet
│   ├── bars/adj_factor/*.parquet
│   └── bars/meta/
├── flow_bars/                  # 本地 Tushare moneyflow Parquet 库（新增）
│   ├── manifest.json
│   ├── moneyflow/*.parquet     # 每股一文件，如 600519.SH.parquet
│   └── meta/
│       └── sync_progress.json
├── daily_history/              # 在线日 K 抓取缓存（已有）
├── industry_provider_cache/    # 行业映射缓存（已有）
├── candidate_context/          # LLM 候选上下文缓存（已有）
├── runs/                       # screen 运行记录（已有）
├── evaluations/                # T+N 评估结果（已有）
└── snapshot.last_good.json     # 快照降级缓存（已有）
```

### 2.3 Config 派生规则

在 `alphasift/config.py` 中：

```python
# 固定子目录 — 仅由 data_dir 派生
daily_bars_dir: Path   # = data_dir / "daily_bars"
flow_bars_dir: Path    # = data_dir / "flow_bars"   （新增）
```

**迁移期兼容（可选，低优先级）：**

- 若环境变量 `DAILY_BARS_DIR` 已设置，仍可读作 override（与当前行为一致），但文档与 `.env.example` **不再推荐**。
- **不新增** `FLOW_BARS_DIR` env；flow 库路径永远为 `{ALPHASIFT_DATA_DIR}/flow_bars`。
- 实现完成后，从 `.env.example` 移除或注释 `# DAILY_BARS_DIR=...` 示例行。

### 2.4 与 self-stock-project 路径对照

| self-stock-project | alphasift |
|--------------------|-----------|
| `stock_data/flow/moneyflow/<ts_code>.csv` | `{ALPHASIFT_DATA_DIR}/flow_bars/moneyflow/<ts_code>.parquet` |
| `stock_data/daily/<ts_code>.csv` | `{ALPHASIFT_DATA_DIR}/daily_bars/bars/raw/<ts_code>.parquet` |
| `stock_data/meta/flow_update_progress.json` | `{ALPHASIFT_DATA_DIR}/flow_bars/meta/sync_progress.json` |

可选一次性脚本 `scripts/migrate_self_stock_flow_csv.py`：将 self-stock CSV 转为 alphasift Parquet，避免重复拉 Tushare。

---

## 3. 目标架构

```text
                    ┌─────────────────────────────────────┐
                    │  flow-bars sync (离线/定时)          │
                    │  Tushare moneyflow → FlowBarStore   │
                    │  根目录: {DATA_DIR}/flow_bars       │
                    └─────────────────┬───────────────────┘
                                      │ Parquet
                                      ▼
┌──────────────┐   快照硬筛    ┌──────────────────────────────┐
│ 全市场快照    │ ──────────► │ 快照筛后 df                   │
└──────────────┘              └──────────────┬───────────────┘
                                             │
              flow_needed 或 FLOW_ENRICH_ENABLED
                                             ▼
                              enrich_flow_features(候选池)
                              └─ FlowBarStore.read
                              └─ flow_metrics.enrich_moneyflow_frame
                              └─ 可选 join DailyBarStore（价量背离）
                                             │
                                             ▼
                              apply_hard_filters(含 flow 条件)
                                             │
                                             ▼
                              compute_screen_scores → L2 → L3
```

### 3.1 模块依赖

```text
alphasift/flow_store.py     →  pyarrow + pandas（无 Tushare）
alphasift/flow_sync.py      →  flow_store + tushare
alphasift/flow_specs.py     →  常量
alphasift/flow_metrics.py   →  纯 pandas/numpy
alphasift/flow_conditions.py→  flow_metrics
alphasift/flow.py             →  enrich_flow_features（编排）
alphasift/pipeline.py       →  两阶段硬筛
alphasift/config.py         →  data_dir 派生 flow_bars_dir
```

对齐 self-stock 边界：`flow_metrics` / `flow_conditions` **禁止** import Tushare 或 `flow_store`。

### 3.2 与 daily  pipeline 协同

含 **日 K + 资金流** 双硬筛的策略，推荐顺序：

```text
快照筛 → daily enrich + daily 硬筛 → flow enrich + flow 硬筛 → screen_score → L2/L3
```

两者均只对快照筛后池操作（典型 100–400 只），IO 可控。

**价涨量出（`require_no_price_up_flow_out`）依赖日 K 收盘价**：`requires_daily_features()` 在该条件开启时返回 `True`，pipeline 会先拉日 K 再算 `price_up_flow_out`；无日 K 时该字段为 `NA`，硬筛会淘汰该行（不会误放行）。

---

## 4. 数据层设计（Phase P0）

### 4.1 FlowBarStore

路径：`alphasift/flow_store.py`

| 方法 | 说明 |
|------|------|
| `__init__(root, ...)` | `root` = `config.flow_bars_dir` |
| `has_code(code)` | 是否存在该股 Parquet |
| `manifest()` | 读 `manifest.json`（`last_trade_date`, `dataset`, `schema_version`） |
| `read(code, *, lookback_days, end_date)` | 按 code 切片，返回升序 DataFrame |
| `write(code, frame)` | reconcile 写入 |
| `list_codes()` | 已落盘 ts_code 列表 |

### 4.2 moneyflow Parquet Schema

主键：`trade_date`（`YYYY-MM-DD`，对齐 `daily_store.format_date_iso`）

| 列 | 说明 |
|----|------|
| `ts_code` | TS 代码 |
| `trade_date` | 交易日 |
| Tushare 原字段 | `buy_sm_*` … `sell_elg_*`, `net_mf_vol`, `net_mf_amount` |
| `main_net_inflow` | **写入时衍生**：`buy_lg+buy_elg-sell_lg-sell_elg`（万元） |
| `retail_net_inflow` | **写入时衍生**：小单+中单净流入（万元） |

### 4.3 flow_sync 同步策略

移植 self-stock P0 逻辑（见源项目 §1.4.3）：

| 模式 | API | 进度 |
|------|-----|------|
| 日更（默认） | `pro.moneyflow(trade_date=T)` 全市场 | `completed_trade_dates` |
| 单股回补 | `pro.moneyflow(ts_code, start, end)` | `next_index` + `symbols` |

日更步骤：

```text
1. pro.moneyflow(trade_date=T) → 全市场 DataFrame
2. 按 ts_code groupby，内存缓冲
3. 批量 reconcile_and_write 至 flow_bars/moneyflow/
4. 更新 manifest.last_trade_date
```

默认目标日：18:00 前 → 上一交易日；18:00 及以后 → 当日（对齐 `daily_sync`）。

### 4.4 CLI

```bash
alphasift flow-bars init --lookback-days 800 [--workers 4]
alphasift flow-bars sync [--trade-date YYYYMMDD]
alphasift flow-bars status
```

内部使用 `config.flow_bars_dir`（= `{ALPHASIFT_DATA_DIR}/flow_bars`），不接受 `--dir` 覆盖根路径（保持目录规范单一）。

### 4.5 Config 新增项（无独立目录 env）

| 变量 | 默认 | 说明 |
|------|------|------|
| `FLOW_ENRICH_ENABLED` | `false` | 非 flow 硬筛策略也可 opt-in 增强 |
| `FLOW_ENRICH_MAX_CANDIDATES` | `100` | Top N（与 daily 对齐） |
| `FLOW_ENRICH_FULL_POOL` | `false` | 快照筛后全量 flow enrich |
| `FLOW_LOOKBACK_DAYS` | `60` | 读盘窗口 |
| `FLOW_SYNC_REQUESTS_PER_SECOND` | `2.0` | 全市场日更限速 |
| `FLOW_SYNC_RETRY` | `3` | 重试次数 |
| `FLOW_SYNC_RETRY_INTERVAL` | `1.0` | 重试间隔（秒） |
| `FLOW_SYNC_PROGRESS_SAVE_EVERY` | `50` | 进度节流 |
| `FLOW_SYNC_PROGRESS_SAVE_INTERVAL` | `15.0` | 进度节流（秒） |

`flow_bars_dir` **仅**代码属性：`Config.flow_bars_dir = Config.data_dir / "flow_bars"`，无对应 env。

---

## 5. 计算层设计（Phase P1）

### 5.1 移植文件

| 源 | 目标 |
|----|------|
| `stock_flow_engine/specs.py` | `alphasift/flow_specs.py` |
| `stock_flow_engine/metrics.py` | `alphasift/flow_metrics.py` |
| `stock_flow_engine/conditions.py` | `alphasift/flow_conditions.py` |

### 5.2 核心 API

```python
def enrich_moneyflow_frame(
    frame: pd.DataFrame,
    daily_bars: pd.DataFrame | None = None,
    *,
    windows: tuple[int, ...] = (5, 10, 20),
) -> pd.DataFrame: ...

def build_stock_flow_snapshot(
    moneyflow: pd.DataFrame,
    daily_bars: pd.DataFrame | None,
    *,
    as_of_date: str | None = None,
) -> dict[str, Any]: ...

def evaluate_flow_conditions(
    moneyflow_frame: pd.DataFrame,
    daily_frame: pd.DataFrame | None,
    conditions: list[dict[str, Any]],
    *,
    as_of_date: str | None = None,
) -> dict[str, Any] | None: ...
```

### 5.3 enrich_flow_features

路径：`alphasift/flow.py`

```python
def enrich_flow_features(
    df: pd.DataFrame,
    *,
    flow_store: FlowBarStore,
    daily_store: DailyBarStore | None = None,
    lookback_days: int = 60,
    max_rows: int | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Per-row soft-fail；输出列见 §5.4。"""
```

### 5.4 输出列（写入候选 DataFrame）

| 列 | 含义 |
|----|------|
| `main_net_inflow` | 当日主力净流入（万元） |
| `main_net_inflow_5d` / `_10d` / `_20d` | N 日累计 |
| `main_inflow_streak` | 连续净流入天数 |
| `main_net_inflow_rate` | 占成交额比 |
| `main_net_inflow_zscore_20d` | 20 日 z-score |
| `price_up_flow_out` | 价涨量出 |
| `price_down_flow_in` | 价跌量入 |
| `flow_as_of` | 有效交易日 |
| `flow_quality_flags` | `missing` / `stale` / 空 |

---

## 6. Pipeline 硬筛集成（Phase P2）

### 6.1 HardFilterConfig 扩展

`alphasift/models.py` + `alphasift/filter.py`：

```python
main_inflow_streak_min: int | None = None
main_net_inflow_5d_min: float | None = None       # 万元
main_net_inflow_min: float | None = None        # 当日，万元
main_net_inflow_rate_min: float | None = None
require_no_price_up_flow_out: bool = False  # 开启时 requires_daily_features() 亦为 True
```

新增辅助函数（对齐 daily 模式）：

- `requires_flow_features(filters) -> bool`
- `without_flow_filters(filters) -> HardFilterConfig`

`require_no_price_up_flow_out` 会同时触发日 K enrich（见 §3.2）；`flow_metrics.enrich_moneyflow_frame` 在无日 K 时将 `price_up_flow_out` 置为 `NA`，硬筛 `_filter_bool_false` 要求该列非空且不为 `True`。

### 6.2 pipeline.screen() 分支

```python
flow_needed = requires_flow_features(screening.hard_filters)
flow_requested = config.flow_enrich_enabled if flow_enrich is None else flow_enrich

snapshot_filters = without_flow_filters(...) if flow_needed else screening.hard_filters
# ... 快照硬筛 ...
# ... 可选 daily enrich + daily 硬筛 ...
# flow enrich（Top N 或 full_pool）
# apply_hard_filters(完整 hard_filters)
```

`ScreenResult` 增加字段（可选）：`flow_enriched: bool`, `flow_enrich_count: int`, `flow_store_last_trade_date: str | None`。

`screen()` 在拉快照前调用 `validate_screen_prerequisites()`：含 flow 硬条件或价涨量出 guard 时，检查本地 `flow_bars` / `daily_bars` 是否就绪；不满足则抛出 `ScreenPrerequisitesError` 并提示 init/sync 命令。

### 6.3 策略 YAML 示例

```yaml
# strategies/main_inflow_momentum.yaml
name: main_inflow_momentum
display_name: 主力持续流入
screening:
  hard_filters:
    amount_min: 300000000
    change_pct_min: 1.0
    change_pct_max: 9.0
    main_inflow_streak_min: 5
    main_net_inflow_5d_min: 0
    require_no_price_up_flow_out: true
```

也可扩展 `capital_heat.yaml`，将 `scorecard_profile.capital_confirmed_bonus` 与真实 `main_net_inflow_5d` 挂钩。

---

## 7. 候选上下文升级（Phase P3）

改造 `alphasift/candidate_context.py`：

```python
def fetch_stock_fund_flow_summary(
    code: str,
    *,
    flow_store: FlowBarStore | None = None,
) -> str:
    if flow_store and flow_store.has_code(code):
        snapshot = build_stock_flow_snapshot(flow_store.read(code), daily_bars=None)
        return _format_flow_snapshot(snapshot)
    return _fetch_akshare_fallback(code)  # 现有逻辑
```

`collect_candidate_context` 传入 `config.flow_bars_dir` 构建 store；audit 标注 `flow_context_source=local|akshare`。

---

## 8. 评分增强（Phase P3，可选）

`alphasift/scorer.py`：

- 新增 factor `capital_flow`，或扩展 `activity` 使用 `main_net_inflow_5d`、`main_inflow_streak`
- 缺 flow 数据时 factor 中性分（50），不 crash

---

## 9. 可选后续（Phase P4+）

| 项 | 说明 |
|----|------|
| `flow_environment.py` | 全市场 `Σ main_net_inflow` 聚合 gate（self-stock P2.5） |
| `moneyflow_mkt_dc` | 需 6000 积分，官方大盘口径 |
| CSV 迁移脚本 | self-stock → alphasift Parquet |
| scorer 与 hotspot 口径统一 | hotspot 可选读本地 flow |

---

## 10. 测试策略

| 层级 | 文件 | 内容 |
|------|------|------|
| 存储 | `tests/test_flow_store.py` | 读写、manifest、reconcile |
| 同步 | `tests/test_flow_sync.py` | mock Tushare、日更拆分、进度恢复 |
| 指标 | `tests/test_flow_metrics.py` | streak、5d 累计、背离（移植 self-stock 用例） |
| 条件 | `tests/test_flow_conditions.py` | 4 条件 pass/fail |
| pipeline | `tests/test_pipeline_flow.py` | 两阶段硬筛、Top N / full_pool |
| config | `tests/test_config.py` | `flow_bars_dir == data_dir / "flow_bars"` |

禁止 CI 调用真实 Tushare。

---

## 11. 文档与配置变更清单

| 文件 | 变更 |
|------|------|
| `.env.example` | 仅保留 `ALPHASIFT_DATA_DIR`；移除/注释 `DAILY_BARS_DIR`；新增 `FLOW_*` 同步/enrich 项（**无** `FLOW_BARS_DIR`） |
| `docs/configuration.md` | 统一数据目录 §；flow 配置表 |
| `README.md` / `README.zh-CN.md` | `flow-bars` CLI；目录树 |
| `alphasift/config.py` | `flow_bars_dir` 派生；`daily_bars_dir` 改为仅 `data_dir / "daily_bars"`（override 可选保留） |

---

## 12. 实施分期与 PR 切分

### Phase P0 — 数据层（约 1 周）

- [ ] **Task 0.1** `flow_store.py`：Parquet 读写、manifest、schema
- [ ] **Task 0.2** `flow_sync.py`：init/sync/status、进度文件
- [ ] **Task 0.3** `config.py`：`flow_bars_dir = data_dir / "flow_bars"`；整理 `daily_bars_dir` 派生逻辑
- [ ] **Task 0.4** `cli.py`：`flow-bars` 子命令
- [ ] **Task 0.5** `tests/test_flow_store.py`、`tests/test_flow_sync.py`
- [ ] **Task 0.6** `.env.example`、`docs/configuration.md` 统一目录说明

### Phase P1 — 计算层（约 3–4 天）

- [ ] **Task 1.1** 移植 `flow_specs.py`、`flow_metrics.py`、`flow_conditions.py`
- [ ] **Task 1.2** `flow.py`：`enrich_flow_features`
- [ ] **Task 1.3** `tests/test_flow_metrics.py`、`tests/test_flow_conditions.py`

### Phase P2 — Pipeline 硬筛（约 1 周）

- [ ] **Task 2.1** `HardFilterConfig` + `filter.py` flow 字段
- [ ] **Task 2.2** `requires_flow_features` / `without_flow_filters`
- [ ] **Task 2.3** `pipeline.py` 两阶段分支；与 daily 顺序编排
- [ ] **Task 2.4** `strategies/main_inflow_momentum.yaml`（或扩展 `capital_heat`）
- [ ] **Task 2.5** `tests/test_pipeline_flow.py`

### Phase P3 — 上下文与评分（约 3–5 天）

- [ ] **Task 3.1** `candidate_context` 本地优先
- [ ] **Task 3.2** `scorer.py` capital_flow factor（可选）
- [ ] **Task 3.3** README 工作流示例

### 建议 PR 顺序

1. **PR-1** P0：数据层 + 统一目录 config + CLI + 文档
2. **PR-2** P1 + P2：计算 + pipeline 硬筛 + 示例策略
3. **PR-3** P3：candidate_context + scorer

**MVP（可运行硬筛策略）**：P0 + P1 + P2，约 **2–2.5 周**。

---

## 13. 风险与缓解

| 风险 | 缓解 |
|------|------|
| 目录配置分散 | 仅 `ALPHASIFT_DATA_DIR`；代码固定 `daily_bars` / `flow_bars` 子路径 |
| 「主力」与 App 不一致 | 固定口径文案；保留 `net_mf_amount` |
| 单位混用 | `flow_specs.py` 集中常量 + 单测 |
| daily + flow 双 enrich 性能 | 默认 Top N；`FLOW_ENRICH_FULL_POOL` opt-in |
| 停牌无 flow | soft-fail + `flow_quality_flags=missing` |
| 已有 self-stock CSV | 可选迁移脚本 |
| `DAILY_BARS_DIR` 老用户 | 迁移期仍读 env override；文档标记 deprecated |

---

## 14. 附录：self-stock 模块对照

| self-stock-project | alphasift |
|--------------------|-----------|
| `stock_data_store/services/flow.py` | `flow_store.py` + `flow_sync.py` |
| `stock_flow_engine/*` | `flow_specs.py` + `flow_metrics.py` + `flow_conditions.py` |
| `update_flow_data.py` | `alphasift flow-bars sync` |
| `web_app/services/flow_scan_filter.py` | `pipeline.py` + `filter.py` |
| `GET /api/flow` | 无；`build_stock_flow_snapshot` 供 audit/debug |
| `stock_data/flow/moneyflow/*.csv` | `{ALPHASIFT_DATA_DIR}/flow_bars/moneyflow/*.parquet` |

---

*文档版本：2026-07-05 v1.0*
