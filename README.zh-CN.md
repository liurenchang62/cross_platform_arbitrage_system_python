# 跨市场套利监控（Python 版）

[English](README.md) | **简体中文**

## 项目简介

本仓库为 Rust 版 `arbitrage-monitor`（参考工程 `cross_market_arbitrage_project`）的 **Python 实现**，行为与输出与之对齐：在 **Polymarket** 与 **Kalshi** 之间基于文本相似度与类别规则发现可对齐的预测市场，维护高置信配对并周期性评估**可执行层面的经济结果**。定价与盈亏以**实时订单簿**解析结果为依据；程序**只读分析**，不发起下单。

## 功能概览

- **市场数据**：通过 Polymarket Gamma API、Kalshi Trade API 获取开放市场，分页与数量上限可配置。
- **智能匹配**：TF-IDF 风格文本向量与余弦相似度；结合 `config/categories.toml` 中的类别关键词。
- **订单簿场景盈亏**：对候选配对在双方卖档深度上按**固定本金上限**模拟吃单，得到 `net_profit_100` 等（见 **订单簿盈亏模型**）。
- **状态追踪**：维护追踪列表，按 `query_params.py` 进行全量重建与周期更新。
- **日志**：按**本地自然日**写入 `logs/monitor_YYYY-MM-DD.csv`（仅套利行）；未分类样本可记入 `logs/unclassified/`。

## 订单簿盈亏模型

与 Rust 版定义一致，详见 [README.md](README.md) 英文小节或源码 `main.py` / `arbitrage_detector.py`。

## 环境要求

- **Python** 3.10+ 推荐  
- 依赖见 `requirements.txt`  
- 需可访问上述公开 API  

## 快速开始

```bash
pip install -r requirements.txt
python main.py
```

首次运行前请确保存在 **`config/categories.toml`**。

## 配置说明

| 路径 | 说明 |
|------|------|
| `config/categories.toml` | 类别名称、权重、关键词 |
| `query_params.py` | 请求节奏、分页上限、`SIMILARITY_THRESHOLD`、`SIMILARITY_TOP_K`、`FULL_FETCH_INTERVAL`、`RESOLUTION_HORIZON_DAYS` 等 |

### 环境变量（可选）

- **`POLYMARKET_TAG_SLUG`**：设置后 Polymarket 拉取可按 tag 过滤（见 `clients.py`）。

## 仓库结构

与 Rust 版模块一一对应，并增加 `market_filter.py`（解析日 21 天窗口筛选）。详见 [README.md](README.md) 中的 layout 表。

匹配与索引说明见 **[docs/MATCHING_VERIFICATION.md](docs/MATCHING_VERIFICATION.md)**。

## 免责声明

- 仅供**研究与学习**，不构成投资建议。  
- 展示盈亏为模型输出，**实盘可能不同**。  
- 须遵守各平台服务条款与适用法律法规。  

## 许可证

若未包含 `LICENSE` 文件，默认保留所有权利。
