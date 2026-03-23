# query_params.py
#! API 查询参数统一管理

# ==================== 通用参数 ====================
# 请求间隔（毫秒）
REQUEST_INTERVAL_MS = 200

# 最大重试次数
MAX_RETRIES = 3

# 重试初始等待时间（毫秒）
RETRY_INITIAL_DELAY_MS = 1000

# ==================== Polymarket 参数 ====================
# Polymarket 单次请求最大事件数
POLYMARKET_PAGE_LIMIT = 200

# Polymarket 最大获取市场数（与 Rust `query_params.rs` 一致）
POLYMARKET_MAX_MARKETS = 20000

# ==================== Kalshi 参数 ====================
# Kalshi 每页市场数
KALSHI_PAGE_LIMIT = 1000

# Kalshi 最大获取市场数（与 Rust `query_params.rs` 一致）
KALSHI_MAX_MARKETS = 20000

# ==================== 向量化参数 ====================
# 最大词汇表大小（降维用）
# None 表示无上限
MAX_VOCAB_SIZE = None

# ==================== 匹配参数 ====================
# 相似度阈值
SIMILARITY_THRESHOLD = 0.8

# 全量获取周期（每 N 个追踪周期执行一次全量获取；与 Rust 一致）
FULL_FETCH_INTERVAL = 180