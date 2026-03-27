# 跨市场套利监控（Python）

[English](README.md) | **简体中文**

## 项目简介

本仓库是一个 **Python** 实现的监控程序：在 **Polymarket** 与 **Kalshi** 两类预测市场之间，自动发现**语义相近的跨平台**候选市场对，维护高置信度的关注列表，并基于**实时订单簿**数据评估在简化假设下的**可执行层面盈亏**。程序仅做**只读分析**——拉取公开的市场列表与订单簿、执行匹配与盈亏模型、写入结构化日志——**不会**代你下单或进行交易。

完整流水线包括：

- 从双方平台拉取开放市场，支持可配置的分页与安全上限。
- 基于 **TF-IDF 风格**文本向量、**余弦相似度**，并结合 `config/categories.toml` 的**类别规则**完成文本匹配。
- 在向量匹配之后，由 `validation.py` 执行**二筛校验流水线**，过滤跨领域或结构不兼容的伪匹配，再进入订单簿分析。
- 通过 `market_filter.py`（参数见 `system_params.py`）按**预计解析日**做**时间窗口**筛选，使监控集中在特定期限内将结算的市场（若启用相关配置）。
- 使用 `tracking.py` 在多次监控周期之间**持续追踪**已关注市场对，在「全量重建候选集」与「增量更新」之间按参数切换。
- **模拟盘（可选）**：在 `system_params.py` 中开启后，由 `paper_trading.py` 维护虚拟资金与持仓，按周期在双边 **bid** 上评估提前平仓，冷却后再开仓，并可写入 `logs/paper_trades.csv`（`SESSION_*`、`OPEN`、`CLOSE`、`NO_CLOSE`）。**`backtest`** 包（`python -m backtest`）按 UTC 会话启动日交互式汇总该 CSV。
- 将结果写入结构化日志：按自然日划分的 `logs/` 下监控 CSV，以及可选的 `logs/unclassified/` 未分类样本记录。

上述能力共同支持对「同一命题在不同场所定价是否一致」的持续观察；其实际意义与风险边界见文末**免责声明**。

## 功能概览

### 市场数据

- 从 **Polymarket Gamma API** 与 **Kalshi Trade API** 获取**开放**市场列表。
- 分页大小、单次请求上限、全局拉取上限等集中在 `system_params.py`，便于在不改核心逻辑的前提下调节扫描强度。
- 市场条目在 `market.py` 中归一化为统一结构，供匹配与日志模块使用。

### 匹配与分类

- **向量化**：`text_vectorizer.py` 中对英文进行分词与词干提取（依赖 `snowballstemmer`），生成与经典 TF-IDF 思路相近的稀疏向量。
- **索引与检索**：`vector_index.py` 在各类别内做点积（L2 归一化 TF-IDF 即余弦）；`market_matcher.py` 拟合每类向量化器、构建双平台索引，按**大类**做矩阵块乘初筛（`MATCH_MATMUL_CHUNK_ROWS`），再统一跑 `validation` 二筛，并应用 `system_params.py` 中的 **Top‑K** 与**相似度阈值**。
- **类别信号**：`config/categories.toml` 提供类别名称、关键词与权重；`category_mapper.py` 与 `category_vectorizer.py` 将类别信息与文本相似度结合，使候选配对在数值接近的同时也符合业务上的类别一致性。

### 二筛校验（第二道过滤）

- 向量相似度给出候选后，`validation.py` 按**固定顺序**执行大量基于规则的类型检查（例如体育与政治、总进球与夺冠命题、电竞单图胜负与系列赛、让分盘与独赢盘等跨类型互斥）。
- 未通过校验的配对**不会**再请求订单簿或计算盈亏，从而减少仅靠嵌入相似带来的噪声。

### 解析时间窗口

- `market_filter.py` 可根据配置的 **`RESOLUTION_HORIZON_DAYS`** 等参数，将参与匹配的市场限制在「预计在未来若干日内解析」的子集内，便于聚焦近端事件（具体行为以 `system_params.py` 与实现为准）。

### 订单簿情景盈亏（可执行性建模）

- 对每个通过相似度与业务校验的配对，程序请求双方**当前**订单簿，将**卖盘流动性**解析为按价格**升序**排列的档位。
- 在每条腿上按**固定本金上限**逐档模拟吃单，得到各自可成交份数；取**较小值**作为统一对冲规模 **n**，再精确计算 **n** 份下的总成本、成交量加权均价、手续费与预设 Gas，得到 **`net_profit_100`** 等指标（详见下文**订单簿盈亏模型**）。
- 核心实现位于 `arbitrage_detector.py`，由 `main.py` 中的 `validate_arbitrage_pair` 及相关逻辑调用。

### 启动与周期编号

- 首次拉取市场并建索引后，程序用**同一批快照与向量**再跑一轮**全量匹配作为周期 #0**（不重复 HTTP、不重训向量化器）。随后周期计数进位，**从 #1 起**以价格追踪为主；每隔 **`FULL_FETCH_INTERVAL`** 周期仍会**全量拉取、重建索引并匹配**。

### 跨周期追踪

- `tracking.py` 维护当前关注列表、最近相似度与盈亏表现，并配合 `system_params.py` 中的**全量刷新间隔**等参数，在「周期性重建全集」与「增量维护」之间切换。
- 便于长时间运行时不必每一轮都从零重建全部候选关系。

### 日志与辅助脚本

- **`monitor_logger.py`**：以**本地自然日**为粒度，向 `logs/monitor_YYYY-MM-DD.csv` 追加写入与套利监控相关的 CSV 行。
- **`cycle_statistics.py`**：汇总周期级统计（例如长时间运行下的累计或整轮次资本回报率类摘要，具体以控制台输出为准）。
- **`unclassified_logger.py`**：在无法归入已配置类别时，可选地记录样本。
- **`check_unclassified.py`**：用于查看或汇总未分类日志的辅助脚本。

### 模拟盘（Paper）

- 由 **`PAPER_TRADING_ENABLED`**、**`PAPER_WRITE_TRADE_LOG`** 等开关控制（见 `system_params.py`）。Demo 开启时单腿/每对名义上限由 **`DEMO_REFERENCE_BUDGET_USD`** 与 `PER_LEG_CAP_FRAC_OF_REFERENCE`、`PAIR_CAP_FRAC_OF_REFERENCE` 导出；否则用 **`LOCAL_TOTAL_USD`** 与同比例——与主流程验证及可选 Demo IOC 口径一致。
- 可选环境变量 **`PAPER_RUN_LABEL`**（对应 `PAPER_RUN_LABEL_ENV`）用于在 CSV 的 `notes` 中标记测试轮次。

### 模拟回测 CLI

```bash
python -m backtest
```

默认读取 `logs/paper_trades.csv`，或通过环境变量 **`PAPER_TRADES_CSV`** 指定路径。交互流程按 UTC 会话起点解析，并在终端输出框线报表。

## 订单簿盈亏模型

用于展示与排序的场景（例如周期 **Top 10**、**`net_profit_100`**）在代码中定义为：

1. **订单簿快照**  
   对每个已通过校验的匹配市场对，程序通过 HTTP 获取双方**当前订单簿**，将可买入的**卖盘**流动性解析为按价格**升序**排列的档位 `(价格, 数量)`。

2. **单腿本金上限**  
   每条腿使用 `paper_caps_demo()` 或 `paper_caps_local()` 给出的 **`每腿 USD 上限`**（在 `main.py` 中作为 `trade_amount` 传入）。在对应卖档上**逐档累加**，直至达到该上限或档位耗尽（`calculate_slippage_with_fixed_usdt`），得到该腿**可成交的合约份数**。Kalshi Demo 的 IOC 下单限额使用同一 **`per_leg_cap_usd`** 参数，与订单簿探针本金独立传参。

3. **对冲规模**  
   取两腿可成交份数的**较小值**作为统一成交规模 **n**，以保证两腿可按**相同份数**同时完成上述意义上的建仓。

4. **规模 n 下的成本与利润**  
   对规模 **n**，在两侧订单簿上再次按档位精确计算总支出与**成交量加权均价**（`cost_for_exact_contracts`），得到 **`capital_used`**；再扣除平台手续费与预设 Gas，得到 **`net_profit_100`**。是否将该配对视为「当前周期内值得关注的机会」，由 **`net_profit_100`** 是否高于检测器配置的最小阈值决定；该判定基于**全深度扫档**后的结果，而非单一报价层面的简化。

**实现位置**：`main.py` 中的 `validate_arbitrage_pair`；`arbitrage_detector.py` 中的 `calculate_arbitrage_100usdt`、`calculate_slippage_with_fixed_usdt`、`cost_for_exact_contracts`。

## 环境要求

- 推荐使用 **Python 3.10 及以上**版本。
- 通过 `requirements.txt` 安装依赖：
  - **`aiohttp`**：异步 HTTP 客户端，用于调用平台 API。
  - **`numpy`**：向量与数值运算。
  - **`snowballstemmer`**：英文词干提取，服务文本向量化。
  - **`toml`**：解析 `config/categories.toml`。
  - **`questionary`**：`python -m backtest` 的交互选项。
  - **`wcwidth`**：回测报表在终端中的列宽（中文等宽字符）。
- 需要能够稳定访问 Polymarket 与 Kalshi 的**公开 API**；若端点、鉴权或频控策略变更，请以平台最新文档为准并相应调整客户端代码或参数。

## 快速开始

```bash
pip install -r requirements.txt
python main.py
```

首次运行前请确保已存在 **`config/categories.toml`**（本仓库预期在 `config/` 目录下提供初始配置）。

## 配置说明

| 路径 | 说明 |
|------|------|
| `config/categories.toml` | 类别名称、权重与关键词列表，用于带类别约束的匹配。 |
| `system_params.py` | 请求节奏、分页上限、**`KALSHI_DEMO_MODE_ENABLED`**、**`SIMILARITY_THRESHOLD`**、**`MATCH_MATMUL_CHUNK_ROWS`**、**`FULL_FETCH_INTERVAL`**、**`RESOLUTION_HORIZON_DAYS`**、Demo/本地资金标尺与模拟盘开关等。 |

### 环境变量（可选）

- **`POLYMARKET_TAG_SLUG`**：设置后，Polymarket 市场拉取可按指定 tag 过滤（实现见 `clients.py`）。
- **`KALSHI_DEMO_API_KEY_ID`**、**`KALSHI_DEMO_PRIVATE_KEY_PATH`**：当 `system_params` 中 **`KALSHI_DEMO_MODE_ENABLED=True`** 时必填（Demo API RSA 私钥 PEM）。
- **`PAPER_RUN_LABEL`**：模拟盘写 CSV 时附加到 `notes`（见 `system_params.PAPER_RUN_LABEL_ENV`）。
- **`PAPER_TRADES_CSV`**：`python -m backtest` 读取的模拟成交 CSV 路径（见 `system_params.PAPER_TRADES_CSV_ENV`）。

若安装 **`python-dotenv`**，`main.py` 会从与 `main.py` 同目录的 **`.env`** 注入环境变量。

## 仓库结构

```
main.py                 程序入口与监控主循环
clients.py              Polymarket / Kalshi HTTP 客户端
market.py               归一化后的市场数据结构
market_matcher.py       匹配、相似度检索与索引构建
text_vectorizer.py      分词、词干与向量化
category_vectorizer.py  类别相关向量辅助
category_mapper.py      基于配置的类别映射
vector_index.py         向量索引与近邻搜索
validation.py           候选对的二筛规则流水线
market_filter.py        解析期限等列表过滤
arbitrage_detector.py   订单簿遍历、手续费、Gas 与盈亏计算
system_params.py        全局常量、API 节奏、模拟盘参数
paper_trading.py        模拟持仓与 paper_trades.csv
backtest/               python -m backtest，paper CSV 会话绩效 CLI
log_format.py           UTC/本地时间字符串（CSV 列）
tracking.py             周期内对已追踪对的维护
monitor_logger.py       按日 CSV 监控日志
cycle_statistics.py     周期统计汇总
unclassified_logger.py  未分类市场记录
check_unclassified.py   未分类日志查看工具
config/
  categories.toml
docs/
  MATCHING_VERIFICATION.md
requirements.txt
```

匹配行为与验证说明的进一步文档：**[docs/MATCHING_VERIFICATION.md](docs/MATCHING_VERIFICATION.md)**。

## 免责声明

- 本项目仅供**研究、学习与个人技术实验**，不构成**投资建议、交易建议或法律意见**。
- 预测市场在**规则、结算方式、流动性、手续费与网络时延**等方面差异很大；界面或日志中展示的盈亏均为基于**某一时刻订单簿快照**与简化假设（含手续费、Gas 等）的**模型输出**，**实盘结果可能显著不同**。
- 使用第三方 API 时，你需自行遵守各平台的**服务条款**、**API 使用政策**以及你所在司法辖区的**法律法规**。

## 许可证

若本仓库未包含 `LICENSE` 文件，则默认**保留所有权利**；若计划以开放许可分发，请自行补充许可证文件。
