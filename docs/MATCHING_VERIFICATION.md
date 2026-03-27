# 匹配管线验证说明（精确余弦索引）

本文档描述本仓库内市场匹配、二筛与日志行为。

## 索引检索

- 每个类别内用堆叠矩阵做一次 `scores = X @ q`（L2 归一化 TF-IDF 下即**精确余弦**），再按 `score >= SIMILARITY_THRESHOLD` 过滤，取前 `SIMILARITY_TOP_K` 条（见 `system_params.py`）。

## 与「全不能变差」的关系

- 在**同一阈值、同一 Top-K、同一分桶与向量化**下，候选由「全体点积 ≥ 阈值」定义。
- 若调整 `SIMILARITY_TOP_K` 或阈值，应重新做对比审计。

## 监控输出（每日 CSV）

- 路径：`logs/monitor_YYYY-MM-DD.csv`（按**本地日期**切分）。
- **仅套利行**：每次验证通过追加一行，无 `cycle_report`、无周期 Top10 汇总行。列含 `event_time_utc_rfc3339`、`event_time_local`、`cycle_id`、`cycle_phase` 及与终端详单一致的数值字段；两侧订单簿前 5 档为 `orderbook_pm_top5_json` / `orderbook_kalshi_top5_json`（JSON 数组 `[[价,量],...]`）。
- 解析日窗口：`RESOLUTION_HORIZON_DAYS = 21`，无解析日期的市场保留；有日期且晚于「当前 UTC + 21 天」的剔除（全量拉取与追踪列表修剪）。

## 建议操作

### 1. 安装与试跑

```powershell
cd D:\cross_platform_arbitrage_python
pip install -r requirements.txt
python main.py
```

### 2. 关注日志

- `构建` / 索引与匹配进度输出  
- `初筛匹配对`、`二筛过滤`  

## 可调参数（`system_params.py`）

| 常量 | 含义 |
|------|------|
| `SIMILARITY_THRESHOLD` | 余弦下限 |
| `SIMILARITY_TOP_K` | 每 query×类别 保留条数；**仅增大**会扩大初筛候选集 |

---

类内检索使用 `numpy` 做点积（L2 归一化 TF-IDF 下即精确余弦）。  
全库阶段：`market_matcher.py` 先按**大类**用矩阵块乘（`MATCH_MATMUL_CHUNK_ROWS`）做 PM↔Kalshi 初筛，再统一跑 `validation` 二筛流水线。  
文本向量化：`snowballstemmer`（English）词干、`ceil(max_df_ratio * n_docs)` 的文档频率上限、分词边界保留 `-`、在非字母数字处切分。
