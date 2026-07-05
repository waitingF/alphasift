# Changelog

## Unreleased

- 新增 `daily_indicators.py`：移植 self-stock ZG/KDJ/BOLL/brick 指标与砖型图 XG 组合信号
- 扩展 `HardFilterConfig` / `filter.py`：支持 B1/B2 族 hard filter 字段（`kdj_j_max`、`require_zg_short_above_long` 等）
- 新增策略 YAML：`b1`、`b1_above_long`、`b1_perfect`、`b2`、`brick_turn_up`
- 新增 flow 组合策略：`b1_main_inflow_5d`、`b1_main_inflow_5d_no_divergence`、`b2_main_inflow_5d`
- 新增 `alphasift.pattern` 模块与 `pattern-search` CLI（7 种形态相似度度量，依赖 daily-bars）
- `Pick` 与 daily enrich 追加 ZG/KDJ/BOLL/brick 可选展示字段；不足 114 根 K 线时写入 `zg_insufficient_bars` 质量标记
- 支持 DSA 通过 `context["dsa"]` 注入候选 provider，AlphaSift 会在 L1 初筛后、LLM 重排前补充 DSA 行情、基本面和新闻上下文
- `dsa_adapter.screen()` 现在会透传 DSA context，并在候选结果中保留 `dsa_context`、`dsa_news` 和 `dsa_analysis_summary`
- LLM ranking prompt 会读取候选上的 DSA provider context，便于排序阶段利用 DSA 已有数据能力

## 2026-04-12

- 明确说明 `DSA` 指外部项目 `daily_stock_analysis`，补充两者的职责边界与调用关系
- 更新 README、Skill 文档和设计说明，说明 DSA 只在最终入围候选上调用
- 修正文档中过期描述：移除对 `shrink_pullback` 可直接运行和“未实现 L3 deep_analysis”的错误说法
- 补充当前 DSA overlay 行为说明：结构化结果会在最后阶段影响 `final_score`、风险判断和最终排名
