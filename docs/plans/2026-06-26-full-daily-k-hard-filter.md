# 方案 C：全量日 K 硬筛 + 本地 Tushare 日 K 库

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 对含日 K 硬条件的策略，在快照硬筛后对**全部**剩余候选执行日 K 特征计算与日 K 硬筛，消除 Top N 预排序截断导致的漏选；日 K 数据优先从用户预下载的本地 Tushare 全量 A 股库读取，screen 路径不再依赖逐只在线拉取。

**Architecture:** 新增「本地日 K 存储层（DailyBarStore）」与「同步 CLI（daily-bars sync）」负责 Tushare 全量/增量落盘；`fetch_daily_history()` 增加 `local` 数据源，从本地库按 `(code, lookback_days, adj)` 切片返回标准 OHLCV DataFrame；`pipeline.screen()` 在 `daily_needed` 且开启全量模式时，对快照筛后完整 `df` 调用 `enrich_daily_features(max_rows=len(df))`，不再 `head(daily_limit)`，并在日 K 硬筛通过后再计算最终 `screen_score`。

**Tech Stack:** Python 3.11+、pandas、pyarrow（Parquet，新增可选依赖）、现有 tushare SDK、alphasift 现有 `daily.py` / `filter.py` / `pipeline.py`。

## Global Constraints

- 硬条件仍由代码执行，LLM 不参与日 K 阈值判断（`docs/design.md`）。
- 快照级关键字段缺失仍 fail-fast；**单行**日 K 读取/特征失败采用 **per-row soft-fail**（见 §6.6）：该候选写入 `daily_quality_flags=fetch_failed` 并在日 K 硬筛中淘汰，**不** abort 整次 screen。
- 首版**不**提供 `--strict` / strict 模式；库陈旧、单行 miss 等可恢复问题统一写入 **degradation**；**不可恢复**错误（本地库目录缺失、pyarrow 未安装、`daily-bars sync` 存在 failed codes）分别由 screen **RuntimeError** 或 sync CLI **exit code 1** 表达（见 §6.3、§4.7.3）。
- 全量模式是**显式 opt-in**，默认行为保持 Top N，避免拖慢未配置本地库的用户。
- 本地库与 screen 使用的复权方式必须一致（默认 `qfq`，对齐现有 `TUSHARE_DAILY_ADJ`）。
- **前复权/后复权库禁止「只 append 已复权 K 线」**：除权、分红、送转等事件会改变历史前复权价，增量 sync 必须检测 `adj_factor` 变化并触发受影响标的的历史重算或整段重建。
- 测试不得依赖 live 网络；Tushare 同步逻辑用 monkeypatch/fake pro API。
- 不引入完整回测/交易框架；仍是 L1 后轻量特征增强，只是候选池从 Top N 扩到快照筛后全量。
- 文档、`.env.example`、`docs/configuration.md` 与实现同步更新。

---

## 1. 背景与问题

### 1.1 当前行为（需改造）

含日 K 硬条件的策略（如 `shrink_pullback`、`volume_breakout`）在 `pipeline.py` 中：

1. 快照硬筛（去掉日 K 条件）
2. 用**快照版** `screen_score` 排序
3. **只保留 Top N**（默认 100）进入日 K 增强
4. 对 Top N 做日 K 硬筛
5. 重新计算 `screen_score`，进入 L2/L3

问题：

- 排名第 101 及以后的候选**直接被丢弃**，不会参与日 K 硬筛。
- 预排序依据快照字段，与「均线多头 / 缩量回踩 / 20 日突破」等日 K 逻辑可能不一致。
- `enrich_daily_features()` 未接入 `daily_history_cache_dir`，screen 路径重复在线拉取。
- 即使已有 per-code JSON 缓存，全量 300–500 只候选的首次冷启动仍慢，且受 API 限速影响。

### 1.2 目标用户场景

用户已有 Tushare Pro，可**提前下载并维护全量 A 股日 K**，希望：

- 日 K 策略语义正确：快照筛后**全量**做日 K 硬筛
- screen 时以本地读盘为主，秒级~分钟级完成数百只候选的特征计算
- 与现有策略 YAML、`hard_filters`、L2/L3 流程兼容

### 1.3 非目标

- 不做全市场 5000+ 无快照预筛的日 K 扫描（仍应先快照硬筛）
- 不替换 T+N 评估的价格路径逻辑（可复用同一 DailyBarStore，但不在本方案首要范围）
- 不实现分布式数据湖 / 实时行情合成
- 不改变 `hard_filters` schema（除非后续单独扩展字段）

---

## 2. 目标架构

```text
                    ┌─────────────────────────────────────┐
                    │  daily-bars sync (离线/定时)         │
                    │  Tushare Pro → DailyBarStore         │
                    └─────────────────┬───────────────────┘
                                      │ Parquet/SQLite
                                      ▼
┌──────────────┐   快照硬筛    ┌──────────────────────────────┐
│ 全市场快照    │ ──────────► │ 快照筛后 df (例如 126~400 只)   │
└──────────────┘              └──────────────┬───────────────┘
                                             │
                         DAILY_ENRICH_FULL_POOL=true
                         DAILY_SOURCE=local
                                             ▼
                              enrich_daily_features(全量 df)
                              └─ fetch_daily_history(source=local)
                              └─ compute_daily_features()
                                             │
                                             ▼
                              apply_hard_filters(完整 hard_filters)
                                             │
                                             ▼
                              compute_screen_scores → L2 → L3
```

### 2.1 与现有组件关系

| 组件 | 改造后职责 |
|------|-----------|
| `alphasift/daily_store.py`（新建） | 本地日 K 读写、manifest、按 code 切片 |
| `alphasift/daily_sync.py`（新建） | Tushare 全量/增量同步 orchestration |
| `alphasift/daily.py` | 增加 `local` source；`enrich_daily_features` 支持 batch 读盘 |
| `alphasift/pipeline.py` | 全量模式分支；接入 cache_dir；去掉 daily_needed 时的 Top N 截断 |
| `alphasift/config.py` | 新 env 配置项 |
| `alphasift/cli.py` | `daily-bars` 子命令 |
| `tests/test_daily_store.py`（新建） | 存储层单测 |
| `tests/test_daily_sync.py`（新建） | 同步逻辑单测（mock Tushare） |
| `tests/test_pipeline_daily.py` | 全量日 K 硬筛 pipeline 测试 |

### 2.2 保留的二级缓存

现有 `data/daily_history/*.json`（per-code、per-source、TTL）继续用于：

- `DAILY_SOURCE=auto/tushare` 在线路径
- `evaluate` 价格路径
- local 源 miss 时的可选 fallback（默认关闭）

全量本地库是**主存储**；JSON 缓存是**请求级加速**，二者不互相替代。

---

## 3. 本地日 K 存储设计（DailyBarStore）

### 3.0 物理存储选型（已定）

#### 3.0.1 一股票一文件 vs 全市场一文件

**默认：一股票一文件（按 `ts_code`）**，即 `bars/raw/{ts_code}.parquet` 与 `bars/adj_factor/{ts_code}.parquet`。

| 维度 | 一股票一文件 ✅ | 全市场一文件 |
|------|----------------|--------------|
| screen 读（100–500 只 × 120 日） | 各读小文件，与 pipeline 匹配 | 大文件按 code 过滤，IO/元数据更重 |
| `pro.daily(trade_date=)` 增量 | 按 code 分发 append（可并发） | 单日 append 写入简单 |
| 除权 rebuild | 只替换单票 raw/adj | 需 rewrite 大文件或复杂分区 |
| 单票修复 | 删/重写一个文件 | 影响面大 |

全市场单文件更适合全历史 panel 研究/回测扫截面，**不是** AlphaSift screen 的主路径。

可选两阶段（Phase 5+）：`ingest/daily/{trade_date}.parquet` 作同步缓冲 → 夜间 compaction 到 per-code；首版不做。

#### 3.0.2 Parquet vs SQLite

**默认：Parquet（per-code）** 存 OHLCV 与 `adj_factor`；**manifest / code sidecar 用 JSON**。

| 维度 | Parquet ✅ | SQLite |
|------|----------|--------|
| pandas 集成 | 原生 | 需 SQL 层 |
| 压缩 | 列存，适合 OHLCV | 行存，体积偏大 |
| 按 code 读 | 一文件一读 | `INDEX(ts_code, date)` 很好 |
| 按日全市场写 | N 次小写（可并发） | 单事务 bulk INSERT 更顺 |
| 除权重算 | 替换单文件 | DELETE+INSERT 单 code 区间 |
| 并发写 | 多文件并行 | 单写者锁 |
| 运维 | 目录 + 多文件 | 单 `daily_bars.db` 易拷贝 |
| 依赖 | pyarrow | 标准库 |

规模约 5000 票 × 800 日 ≈ 数百 MB～2GB（压缩后更小），两者均可；首版选 Parquet 是为与现有 `daily.py`/pandas 路径一致、除权 rebuild 语义简单、避免 sync 时 SQLite 写锁。

**SQLite 备选（Phase 5+）**：整库单文件诉求强时，可用 `daily_bars.db`（`daily_raw` / `adj_factor` / `code_meta` 表 + 索引）；或 **仅** 用 SQLite 存 sync 状态/除权日历，OHLCV 仍 Parquet。

**DuckDB**：后续可用 `read_parquet('bars/raw/*.parquet')` 做 ad-hoc 分析，无需改主存储。

#### 3.0.3 首版默认

```text
storage_mode   = raw_plus_adj_factor   # 落盘仅 raw OHLCV + adj_factor，不落盘已复权价
file_granularity = per_ts_code
file_format    = parquet
metadata       = manifest.json + bars/meta/{ts_code}.json
read_time_adj  = qfq（默认，对齐 TUSHARE_DAILY_ADJ；hfq 可选）
derived_qfq    = 默认关闭（读时动态复权；§4.5 一致性单测为 P0）
```

**读时复权 vs 落盘内容（P0）：** sync 只持久化**未复权** `raw/` 与 `adj_factor/`；screen 默认在读出时按 `TUSHARE_DAILY_ADJ`（默认 `qfq`）动态复权。须有用例证明：**读时复权结果与在线 `fetch_daily_history(source="tushare")` 在同一 `(code, end_date, lookback_days, adj)` 下 OHLCV 一致**（见 §4.5、§8.1）。

### 3.1 目录布局

默认根目录：`${ALPHASIFT_DATA_DIR}/daily_bars`（可用 `DAILY_BARS_DIR` 覆盖）

```text
daily_bars/
├── manifest.json              # 库级元数据
├── codes.parquet              # 可选：code ↔ ts_code ↔ name 索引
├── meta/                      # sync 运维元数据（非行情）
│   ├── sync_progress.json
│   └── sync_progress_symbols.json   # 可选：大 symbol 列表外置
└── bars/
    ├── raw/                   # 未复权 OHLCV（pro.daily 原价；增量经 §3.6.1 upsert）
    │   ├── 600519.SH.parquet
    │   └── ...
    ├── adj_factor/            # 复权因子序列（与 raw 等长/可 merge）
    │   ├── 600519.SH.parquet
    │   └── ...
    ├── meta/                  # 单票 sidecar（sync 边界 + 除权检测）
    │   ├── 600519.SH.json
    │   └── ...
    └── derived/               # 可选：物化缓存，adj=qfq|hfq，事件后可整文件重建
        └── adj=qfq/
            ├── 600519.SH.parquet
            └── ...
```

**存储原则（重要）：**

| 层 | 内容 | 增量策略 |
|----|------|----------|
| `raw/` | 未复权 open/high/low/close/volume/amount | 新交易日 **upsert** 一行（§3.6.1） |
| `adj_factor/` | `date`, `adj_factor` | 新交易日 upsert；**历史因子变化时整段 replace** |
| `derived/adj=qfq/` | 物化前复权 K 线（可选） | **不可**在除权日后简单 append；须 rebuild |

默认实现：**持久化 `raw` + `adj_factor`，读取时调用与 `daily.py::_apply_tushare_adjustment` 等价的逻辑动态生成 qfq/hfq**。这样增量 sync 只需维护原价与因子，不会出现「历史 qfq 价未回溯调整」的 silent corruption。

若启用 `derived/` 物化层（可选性能优化），必须在 `adj_factor` 变更时对受影响 `ts_code` **整文件重写**，禁止 append 物化 qfq 行作为唯一更新手段。

### 3.2 `manifest.json` schema

```json
{
  "version": 1,
  "provider": "tushare",
  "adj": "qfq",
  "storage_mode": "raw_plus_adj_factor",
  "lookback_cap_days": 800,
  "last_sync_at": "2026-06-26T09:30:00+08:00",
  "last_trade_date": "20260625",
  "code_count": 5123,
  "schema": {
    "raw_columns": ["date", "open", "high", "low", "close", "volume", "amount"],
    "adj_factor_columns": ["date", "adj_factor"],
    "date_format": "YYYYMMDD"
  },
  "sync_stats": {
    "added_rows": 5123,
    "updated_codes": 4800,
    "rebuilt_codes": 37,
    "failed_codes": ["xxxx"],
    "source_errors": []
  }
}
```

### 3.3 单票 Parquet schema

**`bars/raw/{ts_code}.parquet`**

| 列 | 类型 | 说明 |
|----|------|------|
| `date` | string | `YYYYMMDD`，升序 |
| `open` | float64 | 未复权 |
| `high` | float64 | 未复权 |
| `low` | float64 | 未复权 |
| `close` | float64 | 未复权 |
| `volume` | float64 | 对应 Tushare `vol` |
| `amount` | float64 | 可选 |

**`bars/adj_factor/{ts_code}.parquet`**

| 列 | 类型 | 说明 |
|----|------|------|
| `date` | string | `YYYYMMDD`，升序 |
| `adj_factor` | float64 | Tushare `pro.adj_factor` |

**可选 sidecar `bars/meta/{ts_code}.json`**

```json
{
  "ts_code": "600519.SH",
  "last_trade_date": "20260625",
  "latest_adj_factor": 12.345,
  "latest_adj_factor_date": "20260625",
  "adj_factor_fingerprint": "sha256:...",
  "last_rebuild_at": "2026-06-26T09:30:00+08:00"
}
```

Sidecar 字段分工：

| 字段 | 用途 |
|------|------|
| `last_trade_date` | **sync 专用**：本地 raw 最后一根 K 线的 `YYYYMMDD`；增量 sync 读 sidecar 即可确定 fetch 起点，**不必**为边界探测打开完整 Parquet |
| `latest_adj_factor` / `latest_adj_factor_date` | 除权检测：与截面 `adj_factor(T)` 快速比对 |
| `adj_factor_fingerprint` | 对本地 `adj_factor` 序列 canonical hash（算法见 §3.3.1），用于增量 sync 快速判断是否需要 rebuild |
| `last_rebuild_at` | 最近一次整段 rebuild 的时间戳（审计） |

`last_trade_date` 与 `adj_factor_fingerprint` **职责分离**：前者管「下一根该拉哪天」，后者管「历史因子是否被除权改写」。

#### 3.3.1 `adj_factor_fingerprint` 算法（P0，首版必做）

实现与单测须与此一致，避免漏 rebuild 或过度 rebuild：

```python
def compute_adj_factor_fingerprint(
    factors: pd.DataFrame,
    *,
    window_days: int = 60,
) -> str:
    """Return ``sha256:<hex>`` over the canonical adj_factor tail window."""
    df = factors[["date", "adj_factor"]].copy()
    df["date"] = df["date"].astype(str)
    df["adj_factor"] = pd.to_numeric(df["adj_factor"], errors="coerce").round(6)
    df = df.dropna(subset=["adj_factor"]).drop_duplicates("date", keep="last")
    df = df.sort_values("date").tail(window_days)
    payload = json.dumps(
        list(zip(df["date"].tolist(), df["adj_factor"].tolist())),
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()
```

**增量 sync 判定 `rebuild_required`（按序短路）：**

1. 若 sidecar 缺失或本地 `adj_factor` 为空 → `rebuild_required=true`
2. 若 `adj_t` 中 `adj_factor(T)` 与 sidecar `latest_adj_factor` **数值不等**（`round(., 6)` 后比较）→ `rebuild_required=true`
3. 否则对 **除权敏感窗口** `[T-5, T]`（交易日，由 `trade_cal` 解析）：拉取 `pro.adj_factor(ts_code, start, end)` 与本地同区间逐日 diff；任一 `(date, round(adj_factor, 6))` 不一致 → `rebuild_required=true`
4. 否则比较 sidecar `adj_factor_fingerprint` 与本地 Parquet 按 §3.3.1 重算 fingerprint；不等 → `rebuild_required=true`
5. 以上均否 → `rebuild_required=false`；upsert `adj_factor(T)` 并更新 sidecar fingerprint

除权日历（§4.3.2 Step D）可在步骤 1 之前预标记 `rebuild_required`，但 **不得**跳过步骤 3–4。

读取 qfq 时：加载 raw + adj_factor → 调用 §4.5 `apply_adj()` → 按 `end_date` 切片 → `tail(lookback_days)`。

### 3.4 读取 API（`daily_store.py`）

```python
class DailyBarStore:
    def __init__(self, root: Path, *, adj: str = "qfq"): ...

    def has_code(self, code: str) -> bool: ...

    def read_history(
        self,
        code: str,
        *,
        lookback_days: int = 120,
        end_date: str | None = None,
    ) -> pd.DataFrame: ...

    def manifest(self) -> dict[str, object]: ...
```

约定：

- `code` 接受 `600519` 或 `600519.SH`，内部 normalize 为 Tushare `ts_code`
- 返回 DataFrame 列与 `_normalize_daily_history()` 输入一致（见 §3.6.3）；**默认读时前复权（qfq）**，与 `TUSHARE_DAILY_ADJ` 一致
- **`end_date` 切片（P0）**：入参缺省时由调用方传入 **effective trade date**（§6.5）；store 只返回 `date <= end_date` 的 bar，再 `tail(lookback_days)`。禁止 silent 使用「库内最新 bar」而忽略快照/固定交易日
- 设置 `df.attrs["daily_source"] = "local"`、`df.attrs["daily_end_date"]` = 实际使用的 `YYYYMMDD`
- 历史不足 `lookback_days` 时返回可用行并在 attrs 标记 `short_history`
- 读时做与 `stock-data-store` 同等的数据卫生：drop NaN OHLCV、按 `date` dedupe（keep last）、升序；raw 与 adj_factor 日期集合不对齐时在 attrs 标记 `adj_mismatch`

### 3.5 依赖

在 `pyproject.toml` 增加可选 extra：

```toml
[project.optional-dependencies]
daily-store = ["pyarrow>=14.0"]
```

`local` 源启用时若缺少 pyarrow，给出明确安装提示：`pip install "alphasift[daily-store]"`。

### 3.6 Parquet 写入语义与读取归一化（P0，首版必做）

Parquet **不支持** CSV 式的行级 `mode="a"` append。文档中「append 一行」在实现上统一指 **read → merge → atomic replace**，禁止对损坏/半写文件做 blind concat。

#### 3.6.1 单票更新流程

对 `bars/raw/{ts_code}.parquet` 与 `bars/adj_factor/{ts_code}.parquet` 的每次写入：

1. 若文件存在：读入现有 DataFrame；不存在则视为空表
2. 与新行/新段 merge：按 `date` 去重（incoming 覆盖同日期），升序排序
3. 写入临时路径 `{path}.tmp.{pid}`，`fsync` 后 **原子 `rename` 覆盖** 目标文件
4. 同步更新 sidecar 的 `last_trade_date` / fingerprint / `latest_adj_factor*`

**幂等**：同一 `trade_date` 重复 sync 不得产生重复行。

#### 3.6.2 并发与文件锁

- **同一 `ts_code` 同一时刻只允许一个写者**（进程内锁 + 可选 `fcntl`/`portalocker` 文件锁）
- init/sync 多 worker 按 `ts_code` 分片，保证不同 worker 不写同一文件
- 检测到 `.tmp.*` 残留且目标文件缺失时：若 tmp 完整可读则 promote，否则删除 tmp 并标记该 code 待 `fetch` 补洞

#### 3.6.3 读出 → `_normalize_daily_history` 的 date 约定

| 阶段 | `date` 格式 | 说明 |
|------|-------------|------|
| Parquet 落盘 | `YYYYMMDD` string | 与 Tushare `trade_date` 一致，便于 merge |
| `DailyBarStore.read_history` 返回 | **`YYYY-MM-DD` string 列 `date`** | 与在线 `fetch_daily_history` 及 `_normalize_daily_history()` 对齐 |
| `end_date` 入参 | 接受 `YYYYMMDD` 或 `YYYY-MM-DD` | 内部 normalize 后切片 |

转换在 store 读出、动态复权**之后**、`tail(lookback_days)` **之前**完成；screen 路径不得感知 Parquet 内部 `YYYYMMDD` 格式。

---

## 4. Tushare 同步 CLI（daily-bars sync）

### 4.1 命令

```bash
# 初始化：下载全 A 股最近 N 个交易日（默认 800 自然日窗口 → 约 400+ 根 K 线）
alphasift daily-bars init --lookback-days 800

# 增量：从 manifest.last_trade_date 同步到最新交易日
alphasift daily-bars sync

# 检查库状态
alphasift daily-bars status

# 单票/少量补洞
alphasift daily-bars fetch 600519 000001 --lookback-days 120

# 运维常用 flags（§4.6 / §4.7）
#   --requests-per-second 2.0   全局限速
#   --reset-progress              忽略 checkpoint 从头跑
#   --refresh-stock-basic         sync 前刷新 universe（§4.4 新股）
```

### 4.2 初始化流程（`daily_sync.py`）

1. 读取 `TUSHARE_TOKEN`，配置 client（复用 `daily.py` 的 `_configure_tushare_client` / `_to_tushare_code`）
2. `pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,list_date')` 获取在市 A 股列表
3. 过滤 ST/退市（与 `exclude_st` 一致，可选 `--include-st`）
4. 对每只股票：
   - 调 `pro.daily(ts_code, start_date, end_date, fields=...)` → 写入 `bars/raw/`
   - 调 `pro.adj_factor(ts_code, start_date, end_date, ...)` → 写入 `bars/adj_factor/`
   - 写入/更新 `bars/meta/` sidecar（`last_trade_date`, `latest_adj_factor`, fingerprint）
   - 若启用 `derived/` 物化层，再离线生成 `derived/adj=qfq/`（可选）
5. 更新 `manifest.json`；记录失败 code 列表
6. 支持 `--max-codes N`（测试/调试）、`--workers N`（默认 4，可配置）
7. **init 起点**：默认 `start_date = max(list_date, today - lookback_days)`（per-code），非全市场统一窗口；新股不浪费 API，老股不拉 listing 前空窗
8. init 全程写入 **progress checkpoint**（见 §4.7），支持 Ctrl+C 续跑

### 4.3 增量 sync 流程（含除权分红）

#### 4.3.1 为什么不能简单 append 前复权 K 线

前复权（qfq）以**最新复权因子**为基准，将历史价格按比例回溯调整。除权、分红、送转等事件发生时：

- 当日及以后的**未复权** `pro.daily` 价格会跳空或变化；
- `pro.adj_factor` 在除权日及**历史区间**的因子序列相对「除权前快照」可能整体变化；
- 因此：**仅 append 一根已算好的 qfq K 线，不会修正库内既有历史 qfq 价格**，会导致 MA、60 日涨幅、`pullback_to_ma20_pct` 等特征与 Tushare 在线口径 drift，硬筛结果不可信。

后复权（hfq）同理：基准因子变化时历史 hfq 也需重算。

#### 4.3.2 推荐增量算法（raw + adj_factor）

对每个待同步交易日 `T`（`pro.trade_cal` 得到 `(last_trade_date, today]` 列表）：

**Step A — 拉取当日全市场未复权截面（省 API）**

```python
daily_t = pro.daily(trade_date=T, fields="ts_code,trade_date,open,high,low,close,vol,amount")
```

**Step B — 拉取当日复权因子截面**

```python
# 优先全市场接口（若积分/权限允许）；否则按 ts_code 批量
adj_t = pro.adj_factor(trade_date=T, fields="ts_code,trade_date,adj_factor")
```

**Step C — 按 ts_code 分类处理**

对 `daily_t` 中每只 `ts_code`：

1. **Upsert raw**：将 `T` 日未复权 OHLCV 经 §3.6.1 read-merge-replace 写入 `bars/raw/{ts_code}.parquet`（同日期幂等覆盖）
2. **检测 adj_factor 是否变化**（核心，算法 §3.3.1）：
   - 按 §3.3.1 五步判定 `rebuild_required`（`latest_adj_factor` 快路径 + 敏感窗口逐日 diff + fingerprint 兜底）
3. **分支**：
   - **`rebuild_required=false`**（绝大多数交易日、无除权事件）：
     - upsert `adj_factor(T)` 到 `bars/adj_factor/{ts_code}.parquet`（§3.6.1）
     - 更新 sidecar `last_trade_date` 与 fingerprint
     - 若启用 `derived/`：`latest_adj_factor` 未变时，可 append 当日物化 qfq 一行；或统一从 raw+adj 本地重算整文件（实现取一，须单测覆盖）
   - **`rebuild_required=true`**（除权/分红/送转等）：
     - **禁止**只 append derived qfq 或只 append 单行 adj 而不回溯历史
     - 对该 `ts_code` 重拉 `pro.daily(ts_code, start_date, end_date)` 全窗口 raw（或与本地 raw merge 后以 API 为准覆盖冲突日期）
     - 重拉 `pro.adj_factor(ts_code, start_date, end_date)` **整段替换** `bars/adj_factor/{ts_code}.parquet`
     - **必须** rebuild sidecar + `derived/` 整文件（历史 qfq 全变）
     - 记入 `manifest.sync_stats.rebuilt_codes`

**Step D — 除权日探测增强（可选）**

- 调用 `pro.dividend` / `pro.stk_div` / `pro.share_float` 等（按 Tushare 权限选可用接口），在 sync 前获取 `(last_trade_date, today]` 的除权除息日历，预标记 `ex_date` 涉及 `ts_code` 为 `rebuild_required`，减少全量 adj diff 开销。
- 即使无除权日历，**adj_factor fingerprint diff 仍是最终正确性兜底**。

**Step E — 更新 manifest**

- `last_trade_date = T`
- 汇总 `added_rows` / `rebuilt_codes` / `failed_codes`

#### 4.3.3 伪代码

```python
for trade_date in missing_trade_dates:
    daily_t = pro.daily(trade_date=trade_date, ...)
    adj_t = pro.adj_factor(trade_date=trade_date, ...)  # or batched fallback

    ex_date_codes = load_ex_date_codes(trade_date)  # optional

    for row in daily_t.itertuples():
        ts_code = row.ts_code
        upsert_raw_bar(ts_code, row)  # §3.6.1 read-merge-replace

        adj_row = lookup(adj_t, ts_code)
        rebuild = ts_code in ex_date_codes or adj_factor_changed(ts_code, adj_row)

        if rebuild:
            raw = fetch_daily_full(ts_code, start, end)
            factors = fetch_adj_factor_full(ts_code, start, end)
            replace_raw(ts_code, raw)
            replace_adj_factor(ts_code, factors)
            rebuild_derived_if_enabled(ts_code)
            stats.rebuilt_codes.append(ts_code)
        else:
            append_adj_factor(ts_code, adj_row)
            update_sidecar_fingerprint(ts_code)
            maybe_append_derived_bar_if_enabled(ts_code)  # latest_adj_factor 未变时才安全
```

#### 4.3.4 禁止事项（写入实现 checklist）

- ❌ 对 Parquet 做 CSV 式 `mode="a"` 行级 append（须 §3.6.1 read-merge-replace）
- ❌ 无文件锁多 worker 写同一 `{ts_code}.parquet`
- ❌ 对 `derived/adj=qfq/*.parquet` 在除权日后仅 `concat` 新一行
- ❌ 用「昨日 close × 今日涨跌幅」手工推算 qfq 补洞
- ❌ 假设 `pro.daily(trade_date=)` 返回已是 qfq 价并直接落盘（Tushare `daily` 为未复权，复权需配合 `adj_factor`）
- ✅ 以 `adj_factor` 变化为 rebuild 触发条件；除权日历仅作优化

#### 4.3.5 API 次数估算

| 场景 | 典型 API 调用 |
|------|--------------|
| 普通交易日（无除权） | 1× `daily(trade_date)` + 1× `adj_factor(trade_date)` + 本地 append |
| 除权日涉及 M 只股票 | 上述 + M × (`daily(ts_code)` + `adj_factor(ts_code)` 全窗口) |

全市场 5000 只里单日除权通常远小于 5000，整体仍远优于逐只 daily 拉取。

### 4.4 截面 sync 边界情况（P0，首版必做）

截面 `pro.daily(trade_date=T)` 省 API，但需显式处理以下 case（参考 `stock-data-store` 的 per-symbol 增量语义补齐缺口）：

| 场景 | 行为 |
|------|------|
| **停牌 / 当日无成交** | `daily_t` 无该 `ts_code` → **skip**，不写入、不 advance sidecar `last_trade_date`；`daily-bars status` 可汇总「截面缺失但 universe 在册」只数 |
| **新上市** | sync 前 `--refresh-stock-basic`（或 init 后每次 sync 默认 refresh）；新 code 走 **per-code 全窗口 init**（`list_date` 作 start），不走截面 append |
| **退市 / 摘牌** | 本地 Parquet **保留**（历史 screen 仍可读）；自 `stock_basic` 移除后不再 sync；`code_count` 以 manifest + 在册 universe 双口径 optional 展示 |
| **中间缺日** | sidecar `last_trade_date` 落后 `T` 超过 1 个交易日 → 标记 `gap`，对该 code 调 `pro.daily(ts_code, start=last+1, end=T)` **补洞**（merge 写入），不单靠截面单日 |
| **截面有、adj 无** | 记 `failed_codes` + warning；禁止只写 raw 不写 adj（否则动态复权失败） |
| **单票补洞** | `daily-bars fetch` 优先级高于截面 sync 的 skip；`manifest.sync_stats.failed_codes` 应用 fetch 清零后重试 |

init/sync 结束后，sidecar `last_trade_date` 应与 raw 文件最后一行一致；不一致时下次 sync 以 **Parquet 实际末行** 为准并回写 sidecar。

### 4.5 读取路径与复权计算（P0）

**落盘 vs 读出：** sync 仅写 `bars/raw/`（未复权）与 `bars/adj_factor/`；**不**将 Tushare 在线已复权价落盘。读出时默认 **前复权（qfq）**，由 `TUSHARE_DAILY_ADJ` 控制（`hfq` / 不复权同理）。

将 `_apply_tushare_adjustment` 核心逻辑抽到纯函数（不依赖 live `pro`）：

```python
# alphasift/daily_adjust.py（或 daily.py 内模块级函数）
def apply_adj(
    raw: pd.DataFrame,
    factors: pd.DataFrame,
    *,
    adj: str,  # "qfq" | "hfq"
) -> pd.DataFrame:
    """与 alphasift.daily._apply_tushare_adjustment 数值等价。"""
    ...
```

`DailyBarStore.read_history(code, lookback_days, adj="qfq", end_date=...)`：

1. 读 `raw` + `adj_factor`，按 `end_date` 切片（§6.5）
2. `apply_adj(raw, factors, adj=adj)` → 得到与在线 Tushare 同口径的 OHLCV
3. date 归一化为 `YYYY-MM-DD`（§3.6.3）→ `tail(lookback_days)`

**一致性单测（P0，首版必做）：** 对 fixture（含除权前后样本）或 mock `pro` 返回的 `(raw, adj_factor)`：

- 路径 A：`DailyBarStore.read_history(..., adj="qfq", end_date=T)` 的 `open/high/low/close/volume`
- 路径 B：同窗口在线 `_fetch_daily_tushare` + `_apply_tushare_adjustment`

断言逐列 `allclose(rtol=1e-6, atol=1e-4)`（`date` 集合完全一致）。此用例是「读时复权」正确性的验收门槛；**禁止**仅测 sidecar fingerprint 而不测 OHLCV 数值。

screen 与在线 `fetch_daily_history(source="tushare")` 共用 `apply_adj()`，保证 local/online 一致。

### 4.6 全局 API 限速与 retry（P0，首版必做）

对齐 `stock-data-store` 的 `TushareClient` 模式：所有 worker **共享**同一限速器与 retry 策略，避免 `--workers 4` 把 Tushare 配额打爆。

#### 4.6.1 配置项

| 环境变量 / CLI | 默认 | 说明 |
|----------------|------|------|
| `DAILY_SYNC_REQUESTS_PER_SECOND` | `2.0` | 全进程 API 调用速率上限（0 = 不限速） |
| `DAILY_SYNC_RETRY` | `3` | 单次 API 最大重试次数 |
| `DAILY_SYNC_RETRY_INTERVAL` | `1.0` | 重试间隔（秒），可指数退避 |
| `--requests-per-second` | 覆盖 env | init/sync/fetch 子命令均可指定 |
| `--workers` | `4` | 与限速器正交：worker 多 ≠ 请求更快 |

#### 4.6.2 行为约定

- 每次 `pro.*` 调用前 `acquire` 限速 token；失败计入 `manifest.sync_stats.source_errors`
- **可重试**：网络超时、连接重置、Tushare 返回「请稍后再试」类 transient 错误
- **不可重试**：token 无效、积分不足、权限不够 → fail-fast 并写入 `source_errors`，不无限 retry
- 截面接口 `pro.daily(trade_date=T)` / `pro.adj_factor(trade_date=T)` 失败时：**整段 trade_date 标记 pending**，下次 sync 优先重放，不 advance `manifest.last_trade_date`
- per-code rebuild / gap-fill 与截面调用**共用**同一限速器实例
- CLI 结束时输出：`api_attempts` / `api_retries` / `api_failures`（写入 manifest 或 progress，见 §4.7）

实现位置：`alphasift/daily_sync.py` 内独立 `TushareSyncClient`（或复用 thin wrapper），**不**与 screen 在线 fetch 的 retry 混用配置项。

### 4.7 Progress checkpoint 与中断续跑（P0，首版必做）

5000 只 init 可能运行数小时；必须支持 Ctrl+C 安全退出与续跑（参考 `stock-data-store/cli/update_daily.py`）。

#### 4.7.1 文件布局

```text
daily_bars/
└── meta/
    ├── sync_progress.json           # init/sync 共用，run signature 区分
    └── sync_progress_symbols.json   # 大 symbol 列表外置（可选）
```

#### 4.7.2 Progress schema（示意）

```json
{
  "signature": {"command": "init", "lookback_days": 800, "end_date": "20260625"},
  "next_index": 1842,
  "symbols": ["600519.SH", "..."],
  "updated": 1800,
  "skipped": 30,
  "failed": 12,
  "rebuilt": 3,
  "adjustment_refreshed": 3,
  "last_symbol": "000858.SZ",
  "errors": [{"ts_code": "...", "error": "..."}],
  "api_stats": {"attempts": 4200, "retries": 18, "failures": 2}
}
```

#### 4.7.3 行为

- **signature 不匹配**或 `--reset-progress` → 从头开始
- 每 N 只（默认 50）或每 M 秒（默认 15）落盘 progress
- `KeyboardInterrupt` → 保存 progress，打印续跑提示（**同命令重跑即可**）
- 正常完成 → 删除 progress 文件，汇总写入 `manifest.json`
- init **部分完成**后 resume：已写入 Parquet 的 code skip（读 sidecar `last_trade_date` ≥ target 则 `skipped`）
- 非零 `failed` → CLI **exit code 1**

### 4.8 与 screen 的一致性

| 配置项 | 同步 | screen 读取 |
|--------|------|------------|
| `TUSHARE_DAILY_ADJ` | 写入 `manifest.adj`（偏好记录）；**落盘仍为 raw+adj_factor** | `DailyBarStore(adj=...)` **读时** `apply_adj()` |
| `DAILY_LOOKBACK_DAYS` | init lookback 应 ≥ screen lookback | `read_history(lookback_days=...)` |
| `TUSHARE_TRADE_DATE` | sync / init 的 `end_date` 应与此一致（若设置） | **必须**传入 `read_history(end_date=...)`（§6.5） |

`derived/adj=qfq/` 为可选物化层（§3.0.3 默认关闭）；首版 screen **不依赖** derived 目录。

建议在 `daily-bars status` 输出：

- 库 `manifest.last_trade_date` vs 快照 **effective trade date**（§6.5）是否一致；不一致 → degradation 提示，**不** fail-fast
- 缺失 code 数量（相对 stock_basic）

---

## 5. `daily.py` 改造

### 5.1 新数据源 `local`

在 `fetch_daily_history()` 增加分支：

```python
elif src == "local":
    store = DailyBarStore(configured_root, adj=_normalize_tushare_adj(...))
    result = store.read_history(
        code,
        lookback_days=lookback_days,
        end_date=effective_trade_date,  # 由 pipeline 解析，§6.5；禁止省略
    )
```

`DAILY_SOURCE=local` 时 **不** 走 tushare/tencent 降级链（避免 screen 意外打 API）。

可选：`DAILY_LOCAL_FALLBACK_LIVE=true` 时 local miss 再试 `tushare`（默认 false）。

### 5.2 `enrich_daily_features()` 全量优化

当前：对 `result.index[:max_rows]` 逐只 `fetch_daily_history`。

改造：

1. **接入 cache_dir**（无论是否全量）：
   ```python
   cache_dir=config.daily_history_cache_dir,
   cache_ttl_seconds=config.daily_history_cache_ttl_hours * 3600,
   ```
2. 当 `source=local` 且 `DailyBarStore` 可用：
   - 批量收集 codes
   - 可选实现 `store.read_histories(codes, lookback_days=..., end_date=...)` 减少 Parquet 打开次数
   - 仍逐行 `compute_daily_features()`（CPU 轻量，500 只可接受）
3. 新增参数 `end_date: str | None`（§6.5），透传至 `fetch_daily_history` / `read_history`
4. `max_rows` 语义：
   - 全量模式：`max_rows=len(df)`
   - 兼容模式：保持现有 Top N
5. 保留 `ThreadPoolExecutor` 用于 **在线源**；local 源默认单线程读盘即可（或小块并行）
6. 汇总 `daily_fetch_failed_codes` 至 attrs（§6.6）

### 5.3 attrs / degradation 扩展

`enrich_daily_features` 返回 attrs 增加：

- `daily_store_root`
- `daily_store_manifest_last_trade_date`
- `daily_end_date`（实际用于切片的 trade date）
- `daily_store_miss_codes: list[str]`
- `daily_fetch_failed_codes: list[str]`（`fetch_failed` 行，§6.6）
- `daily_enrich_mode: "full_pool" | "top_n"`

---

## 6. `pipeline.py` 改造（核心）

### 6.1 新配置项

| 环境变量 | 默认 | 说明 |
|----------|------|------|
| `DAILY_ENRICH_FULL_POOL` | `false` | 含日 K 硬条件时，对快照筛后**全量**做日 K 增强+硬筛 |
| `DAILY_BARS_DIR` | `${ALPHASIFT_DATA_DIR}/daily_bars` | 本地库根目录 |
| `DAILY_SOURCE` | `auto` | 全量模式推荐设为 `local` |
| `DAILY_LOCAL_FALLBACK_LIVE` | `false` | local miss 是否回退在线源 |
| `DAILY_FULL_POOL_WARN_THRESHOLD` | `500` | 超过此数量输出 degradation 警告 |

CLI：

```bash
alphasift screen shrink_pullback \
  --daily-enrich-full-pool \
  --daily-source local
```

### 6.2 分支逻辑（伪代码）

```python
daily_needed = requires_daily_features(screening.hard_filters)
full_pool = config.daily_enrich_full_pool  # 仅 daily_needed 时生效

if daily_needed or daily_requested:
    if daily_needed and full_pool:
        # 不再预排序截断；对快照筛后全量增强
        enrich_df = df
        enrich_count = len(enrich_df)
        degradation.append("Daily enrich mode: full_pool")
    else:
        provisional = _sort_screened_candidates(compute_screen_scores(df, screening), screening)
        enrich_count = min(daily_limit, len(provisional))
        enrich_df = provisional.head(enrich_count)
        degradation.append(f"Daily enrich mode: top_n limit={enrich_count}")

    effective_trade_date = resolve_effective_trade_date(config, snapshot_df)  # §6.5

    enriched = enrich_daily_features(
        enrich_df,
        max_rows=enrich_count,
        lookback_days=config.daily_lookback_days,
        source=config.daily_source,
        fetch_retries=config.daily_fetch_retries,
        max_workers=config.daily_fetch_max_workers,
        cache_dir=config.daily_history_cache_dir,
        cache_ttl_seconds=config.daily_history_cache_ttl_hours * 3600,
        daily_bars_dir=config.daily_bars_dir,
        end_date=effective_trade_date,
    )

    if daily_needed:
        df = apply_hard_filters(enriched, screening.hard_filters)
    else:
        df = enriched
```

要点：

- **全量模式下取消「快照 screen_score 预排序决定谁能进日 K」**；预排序只在 Top N 模式保留。
- 日 K 硬筛通过后，仍执行 `_sort_screened_candidates(compute_screen_scores(df, screening), screening)` 作为 L2 前排序（此时 screen_score 已含日 K 字段）。

### 6.3 启动前校验与错误分级（P0）

首版**不提供** `--strict` / `DAILY_BARS_STRICT`；问题分级如下：

| 级别 | 场景 | screen 行为 | CLI exit code |
|------|------|-------------|---------------|
| **配置 fail-fast** | `daily_source=local` 但库目录不存在 / manifest 不可读 / 未安装 pyarrow | `RuntimeError`，整次 screen 失败 | 非 0（由 CLI 包装） |
| **degradation 警告** | `manifest.last_trade_date` 早于 §6.5 effective trade date；全量池超过 `DAILY_FULL_POOL_WARN_THRESHOLD`；`daily_fetch_failed_codes` 非空 | 继续运行，写入 degradation | 0 |
| **sync 失败** | `daily-bars init/sync` 存在 `failed` codes 或不可重试 API 错误 | 不适用 | **1**（§4.7.3） |

当 `daily_needed and full_pool and daily_source == "local"` 时，启动前检查：

1. `DailyBarStore` 目录存在且 manifest 可读（否则 fail-fast）
2. pyarrow 已安装（否则 fail-fast，提示 `pip install "alphasift[daily-store]"`）
3. 解析 §6.5 `effective_trade_date`；若 `manifest.last_trade_date < effective_trade_date`（按 `trade_cal` 比较交易日，非自然日）→ **degradation 警告**，不 abort

### 6.4 `ScreenResult` 元数据

扩展字段（可选）：

- `daily_enrich_mode: str`
- `daily_full_pool: bool`
- `daily_store_last_trade_date: str | None`
- `daily_effective_trade_date: str | None`

### 6.5 交易日对齐（P0）

日 K 特征（`change_60d`、`breakout_20d_pct` 等）必须与快照截面**同一交易日**截止，否则 full_pool 语义正确但数值不可复现。

**`effective_trade_date` 解析顺序（`resolve_effective_trade_date`）：**

1. 环境变量 `TUSHARE_TRADE_DATE`（若设置，与 `snapshot._resolve_tushare_trade_date` 一致）
2. 否则快照 DataFrame attrs / metadata 中的 `trade_date`（Tushare 快照路径应写入）
3. 否则 `DailyBarStore.manifest()["last_trade_date"]`（仅作 fallback，并写 degradation 说明来源）

**传递链：** `pipeline.screen()` → `enrich_daily_features(..., end_date=)` → `fetch_daily_history(..., end_date=)` → `DailyBarStore.read_history(..., end_date=)`。

**校验（degradation，非 fail-fast）：** screen 开始时比较 `manifest.last_trade_date` 与 `effective_trade_date`：

- 若库日期 **落后** effective date（按 A 股 `trade_cal` 计）→ `degradation: daily store stale: manifest=... effective=...`
- 若库日期 **领先** effective date（例如用了未来 bar）→ `degradation: daily store ahead of snapshot: ...`（仍按 effective date 切片，不读未来 bar）

在线 `fetch_daily_history(source="tushare")` 后续也应接受同一 `end_date` 参数，与 local 路径对齐（Phase 1 可仅在 local 分支强制，Phase 3 与 pipeline 贯通）。

### 6.6 单行日 K 失败语义（P0）

与 Global Constraints 一致：全量模式下 **per-row soft-fail**，不 abort 整次 screen。

| 事件 | 行级行为 | 硬筛结果 |
|------|----------|----------|
| local 文件缺失 / Parquet 损坏 | `enrich_daily_features` 捕获异常，写入 `_DAILY_FEATURE_DEFAULTS`，`daily_quality_flags=fetch_failed` | `apply_hard_filters` 中 bool/数值条件因 `NA` / `!= True` **淘汰**该行 |
| `short_history` / `adj_mismatch` | 仍计算特征并设 quality flags | 由策略硬条件决定；degradation 汇总 flag 计数 |
| 配置级错误（库根目录不存在） | 在 enrich 之前 fail-fast | 整次 screen 失败 |

**degradation 输出（full_pool 必填）：**

```text
Daily enrich mode: full_pool; attempted=N succeeded=M fetch_failed=K
Daily K-line fetch_failed codes: 000001, ... (+J more)   # K>0 时
```

当 `fetch_failed / attempted > 5%` 时追加警告：`daily fetch failure rate high; check daily-bars sync or DAILY_BARS_DIR`。

**与 Top N 模式一致：** 现有 `enrich_daily_features` 已在单行失败时填 defaults；full_pool 仅扩大 attempted 规模，**不改变**淘汰机制。单测须覆盖：mock local miss 的 code 不会进入 L2，且 `after_filter_count` 正确（§8.4）。

---

## 7. `config.py` / `.env.example` 变更

新增 `Config` 字段：

```python
daily_enrich_full_pool: bool = False
daily_bars_dir: Path | None = None
daily_local_fallback_live: bool = False
daily_full_pool_warn_threshold: int = 500
# daily-bars sync 专用（§4.6，与 screen 在线 fetch 分离）
daily_sync_requests_per_second: float = 2.0
daily_sync_retry: int = 3
daily_sync_retry_interval: float = 1.0
daily_sync_progress_save_every: int = 50
daily_sync_progress_save_interval: float = 15.0
```

`.env.example` 推荐组合：

```env
TUSHARE_TOKEN=...
TUSHARE_DAILY_ADJ=qfq

# 离线库
DAILY_BARS_DIR=./data/daily_bars

# daily-bars sync（§4.6 / §4.7）
DAILY_SYNC_REQUESTS_PER_SECOND=2.0
DAILY_SYNC_RETRY=3
DAILY_SYNC_RETRY_INTERVAL=1.0

# 方案 C：全量日 K 硬筛
DAILY_ENRICH_FULL_POOL=true
DAILY_SOURCE=local
DAILY_LOOKBACK_DAYS=120
DAILY_FETCH_MAX_WORKERS=4

# 可选：screen 仍保留 JSON 二级缓存
DAILY_HISTORY_CACHE_DIR=./data/daily_history
DAILY_HISTORY_CACHE_TTL_HOURS=24
```

---

## 8. 测试计划

### 8.1 `tests/test_daily_store.py`

- [ ] `DailyBarStore.read_history` 从 fixture Parquet 读取最后 N 根 K 线
- [ ] code normalize：`600519` / `600519.SH` 等价
- [ ] 缺失文件 → 抛出可识别错误或返回 empty + flag
- [ ] manifest 解析；**落盘无 derived 时**读时 qfq 仍可用
- [ ] **date 归一化**：Parquet 内 `YYYYMMDD` → 读出 `YYYY-MM-DD` 列 `date`，`_normalize_daily_history` 可消费
- [ ] **`end_date` 切片**：固定 `end_date=T` 时末 bar 日期为 T，且不包含 T 之后 bar
- [ ] 读时 dedupe / drop NaN；raw 与 adj 日期不齐 → `adj_mismatch` attrs
- [ ] **Parquet upsert 幂等**：同日期重复写入仅保留一行
- [ ] **原子写**：模拟 rename 前中断，目标文件仍为旧版或不存在，不产生半写 Parquet
- [ ] **读时复权一致性（P0）**：fixture raw+adj_factor 经 `apply_adj(qfq)` 与 mock 在线 `_apply_tushare_adjustment` 路径 OHLCV `allclose`（含除权样本）
- [ ] **`compute_adj_factor_fingerprint`**：同输入稳定；窗口 tail、round(6) 与 §3.3.1 一致

### 8.2 `tests/test_daily_sync.py`

- [ ] mock `pro.stock_basic` + `pro.daily` + `pro.adj_factor`，`init` 写入 raw 与 adj_factor 两套 Parquet
- [ ] 普通交易日增量：仅 upsert raw/adj 新行，不触发 rebuild
- [ ] **除权场景**：模拟 `adj_factor` 历史序列变化 → 断言触发 `rebuild_required`，raw/adj 全窗口被替换；rebuild 后 **读时 qfq close** 与 `apply_adj` 重算一致
- [ ] `adj_factor_changed()` / §3.3.1 fingerprint 与逐步 rebuild 判定一致
- [ ] 失败 code 写入 manifest.sync_stats.failed_codes / rebuilt_codes
- [ ] **截面边界**：停牌 skip、新股 per-code init、gap-fill 补洞、截面有 adj 无 → failed
- [ ] **sidecar**：sync 后 `last_trade_date` 与 raw 末行一致
- [ ] **progress resume**：中断后 signature 匹配则 `next_index` 续跑，已完成 code 为 skipped
- [ ] **限速 + retry**：mock transient 错误重试成功；mock 权限错误 fail-fast 无无限 retry
- [ ] 多 worker mock 下同一 ts_code 无并发写 corrupt（文件锁或分片保证）

### 8.3 `tests/test_daily.py` 扩展

- [ ] `fetch_daily_history(..., source="local")` 读 fixture store
- [ ] `enrich_daily_features` 传入 `cache_dir` 时使用 JSON 缓存

### 8.4 `tests/test_pipeline_daily.py` 扩展

- [ ] `daily_enrich_full_pool=True` 时 mock enrich 收到 `max_rows == 快照筛后数量`，而非 100
- [ ] 全量模式下不应先 `head(100)`（可通过 mock 断言传入 df 行数）
- [ ] mock enrich 收到 `end_date == effective_trade_date`（固定 `TUSHARE_TRADE_DATE` 场景）
- [ ] 日 K 硬筛后候选数 ≤ 传入 enrich 数
- [ ] **local miss 行**：mock `fetch_failed` 的 code 被日 K 硬筛淘汰，不进入 picks
- [ ] **stale manifest**：库 date 落后 effective date 时 degradation 含 stale 警告，screen 仍完成（无 strict）
- [ ] Top N 默认行为回归不变

### 8.5 `tests/test_cli.py`

- [ ] `daily-bars status` 输出 manifest 摘要
- [ ] `screen --daily-enrich-full-pool` 解析正确

### 8.6 验证命令

```bash
pytest tests/test_daily_store.py tests/test_daily_sync.py tests/test_daily.py tests/test_pipeline_daily.py tests/test_cli.py -q
pytest -q
ruff check alphasift tests
```

---

## 9. 分阶段实施任务

### Phase 0 — 文档与配置脚手架

- [ ] **Task 0.1** 本文档落盘（当前文件）
- [ ] **Task 0.2** 在 `docs/configuration.md` 增加「方案 C / 全量日 K 硬筛」章节（链接本文档）
- [ ] **Task 0.3** 更新 `.env.example` 注释块

### Phase 1 — DailyBarStore（只读）

- [ ] **Task 1.1** 新建 `alphasift/daily_store.py`，实现 manifest + Parquet 读取 + §3.6.3 date 归一化 + §4.5 `apply_adj` + §3.3.1 fingerprint
- [ ] **Task 1.2** 添加 `pyproject.toml` optional dependency `daily-store`
- [ ] **Task 1.3** 新建 `tests/test_daily_store.py` + fixture 小 Parquet（含除权 raw/adj 样本）
- [ ] **Task 1.4** `fetch_daily_history(source="local")` 接入 DailyBarStore（含 `end_date`）

### Phase 2 — daily-bars sync CLI

- [ ] **Task 2.1** 新建 `alphasift/daily_sync.py`（init/sync/status）+ §3.6 Parquet upsert/原子写/文件锁
- [ ] **Task 2.2** `cli.py` 增加 `daily-bars` 子命令 + `--requests-per-second` / `--reset-progress`
- [ ] **Task 2.3** 新建 `tests/test_daily_sync.py`（mock Tushare，含 §8.2 P0 用例）
- [ ] **Task 2.4** README.zh-CN.md 增加「离线日 K 库初始化」示例
- [ ] **Task 2.5** `TushareSyncClient`：全局限速 + retry（§4.6）+ progress checkpoint（§4.7）
- [ ] **Task 2.6** 截面 sync 边界处理（§4.4）：停牌 skip、新股 init、gap-fill、sidecar `last_trade_date`

### Phase 3 — pipeline 全量模式

- [ ] **Task 3.1** `config.py` 新字段 + env 解析
- [ ] **Task 3.2** `pipeline.py` 实现 `full_pool` 分支 + §6.5 `resolve_effective_trade_date` + §6.6 degradation
- [ ] **Task 3.3** `enrich_daily_features` 传入 `cache_dir` / `daily_bars_dir` / `end_date`
- [ ] **Task 3.4** `cli.py screen` 增加 `--daily-enrich-full-pool` / `--daily-source`
- [ ] **Task 3.5** 扩展 `tests/test_pipeline_daily.py`
- [ ] **Task 3.6** `audit.py`：日 K 策略 + 非 full_pool → warn；full_pool + 非 local → info

### Phase 4 — 体验与运维

- [ ] **Task 4.1** `daily-bars status` 对比快照 trade_date，提示需 sync
- [ ] **Task 4.2** screen degradation 输出 full_pool 统计（attempted/succeeded/miss）
- [ ] **Task 4.3** `docs/strategy-guide.md` 更新日 K 段落：说明 full_pool 与 Top N 差异
- [ ] **Task 4.4** 性能冒烟：本地库 + `shrink_pullback` full_pool，记录 200/400 只耗时基线

---

## 10. 推荐运维流程（用户侧）

```bash
# 1. 一次性初始化本地库（收盘后跑）
export TUSHARE_TOKEN=...
export TUSHARE_DAILY_ADJ=qfq
alphasift daily-bars init --lookback-days 800

# 2. 每个交易日收盘后增量
alphasift daily-bars sync

# 3. 检查库新鲜度
alphasift daily-bars status

# 4. 全量日 K 硬筛选股
export DAILY_ENRICH_FULL_POOL=true
export DAILY_SOURCE=local
alphasift screen shrink_pullback --no-llm --explain
alphasift screen volume_breakout --no-llm --explain
```

预期体验（本地 SSD，~300 只快照筛后候选）：

- 日 K 读取 + 特征计算：**数秒 ~ 30 秒**
- 不再出现「快照分高但日 K 形态更好却排在 100 名外」的系统性漏选

---

## 11. 风险与缓解

| 风险 | 缓解 |
|------|------|
| 快照筛后仍有 1000+ 只，CPU/IO 压力 | 收紧快照 `hard_filters`；`DAILY_FULL_POOL_WARN_THRESHOLD` 警告；文档建议目标池 < 500 |
| 本地库 last_trade_date 落后于快照 effective date | `daily-bars status` + screen degradation（§6.5 stale 警告）；用户手动 sync；**无 strict fail-fast** |
| 读时复权与在线 Tushare 漂移 | 落盘 raw+adj_factor；`apply_adj()` 单测与 §8.1 数值对齐用例；除权后 rebuild |
| **除权后历史 qfq 未回溯** | 持久化 raw+adj_factor；增量检测因子变化并 rebuild；禁止 append 物化 qfq |
| 除权日 rebuild 导致 sync 变慢 | 除权日历预标记；仅对 `rebuilt_codes` 全量拉取；manifest 记录 rebuild 数量 |
| Parquet 文件过多（5000+） | 可接受；后续 Phase 5 可选按 `exchange` 分区或 SQLite 聚合 |
| 与 evaluate 价格路径重复存储 | 后续让 evaluate 优先读 DailyBarStore（YAGNI，本方案不强制） |
| init/sync 中断导致半写库 | §3.6 原子 rename + §4.7 progress 续跑；损坏 tmp 不 promote |
| 多 worker 打爆 Tushare 配额 | §4.6 全局限速 + transient retry；截面失败不 advance last_trade_date |
| 截面 sync 漏停牌/新股/缺日 | §4.4 显式 skip/init/gap-fill 策略 + sidecar `last_trade_date` |

---

## 12. 后续可选优化（Phase 5+，不在首版范围）

- 向量化批量 `compute_daily_features`（一次处理多 code）
- `daily-bars verify`：随机抽样与 Tushare 在线数据 diff
- 按 trade_date 宽表存储，加速「某日全市场截面」查询
- evaluate / T+N 价格路径默认读 DailyBarStore
- 策略级 YAML 标记 `screening.daily_enrich_mode: full_pool`（覆盖 env）

---

## 13. 验收标准

1. `DAILY_ENRICH_FULL_POOL=true` + `DAILY_SOURCE=local` 时，`shrink_pullback` / `volume_breakout` 对快照筛后**全部**候选执行日 K 硬筛，无 Top N 截断。
2. 默认配置下现有 Top N 行为与测试全部通过（向后兼容）。
3. `daily-bars init/sync/status` 在无网络 mock 测试中可验证；用户文档包含完整初始化步骤。
4. screen 输出 degradation 能看清：全量模式、**effective trade date**、本地库 trade_date、attempted/succeeded/**fetch_failed** 只数。
5. 本地库缺失或 pyarrow 未安装时，错误信息 actionable（告诉用户如何 init / install）；**库陈旧仅 degradation，不 abort**。
6. **除权分红回归**：fixture 模拟 `adj_factor` 变更后，sync rebuild + **读时 qfq** 与在线 `apply_adj` 结果一致；不存在「只 append 导致旧历史 qfq 不变」的路径。
7. **P0 工程约束**：Parquet read-merge-replace 幂等；init 中断后续跑；截面边界（停牌/新股/缺日）有单测；`read_history` 返回 `YYYY-MM-DD`；API 全局限速 + retry 可配置且 transient 可恢复。
8. **交易日对齐（P0）**：固定 `TUSHARE_TRADE_DATE` 时，local 读出末 bar 日期与快照 effective date 一致；特征计算不包含 future bar。
9. **读时复权数值对齐（P0）**：§8.1 一致性单测通过；落盘仅为 raw+adj_factor，screen 默认 qfq 读时复权。

---

## 14. 相关文件索引

| 文件 | 当前状态 | 改造 |
|------|----------|------|
| `alphasift/pipeline.py` | Top N 截断 | 全量分支 |
| `alphasift/daily.py` | 在线 fetch + JSON cache | + local source |
| `alphasift/config.py` | 无 full_pool | + 新字段 |
| `alphasift/cli.py` | 无 daily-bars | + 子命令 |
| `alphasift/daily_store.py` | 不存在 | 新建（含 `apply_adj`、fingerprint） |
| `alphasift/daily_adjust.py` | 不存在 | 新建（可选，或置于 `daily.py`） |
| `alphasift/daily_sync.py` | 不存在 | 新建 |
| `docs/strategy-guide.md` | Top N 描述 | 补充 full_pool |
| `docs/configuration.md` | 部分 daily 配置 | 补充方案 C |
| `.env.example` | 有 TUSHARE/DAILY_* | 补充 full_pool 块 |
