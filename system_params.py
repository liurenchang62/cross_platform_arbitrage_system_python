# system_params.py
# 系统级常量：HTTP/分页、匹配与索引、市场窗口、模拟交易（Paper）等。
import math

# ==================== 通用参数 ====================
REQUEST_INTERVAL_MS = 200
MAX_RETRIES = 3
RETRY_INITIAL_DELAY_MS = 1000

# ==================== Polymarket 参数 ====================
POLYMARKET_PAGE_LIMIT = 200
POLYMARKET_MAX_MARKETS = 50000

# ==================== Kalshi 参数 ====================
KALSHI_PAGE_LIMIT = 1000
KALSHI_MAX_MARKETS = 50000

# ==================== 向量化参数 ====================
MAX_VOCAB_SIZE = None

# ==================== 匹配参数 ====================
SIMILARITY_THRESHOLD = 0.8
SIMILARITY_TOP_K = 15
FULL_FETCH_INTERVAL = 180

# ==================== 市场时间窗口 ====================
RESOLUTION_HORIZON_DAYS = 21

# ==================== 模拟交易（Paper）====================
PAPER_TRADING_ENABLED = True
PAPER_WRITE_TRADE_LOG = True
PAPER_RUN_LABEL_ENV = "PAPER_RUN_LABEL"
PAPER_SESSION_COUNTER_FILE = "logs/paper_session_counter.txt"
PAPER_TRADES_CSV = "logs/paper_trades.csv"
# 回测 CLI 读取路径的环境变量名（与 `backtest` crate 一致）
PAPER_TRADES_CSV_ENV = "PAPER_TRADES_CSV"
PAPER_PER_LEG_CAP_USDT = 100.0
PAPER_COOLDOWN_CYCLES = 5
PAPER_INITIAL_CASH = 100_000.0
PAPER_MAX_OPEN_POSITIONS = 50
PAPER_MIN_EDGE_EARLY_USD = 0.5


def paper_settlement_fee_estimate(n: float, fee_pm: float, fee_ks: float) -> float:
    if n <= 0.0 or not math.isfinite(n):
        return 0.0
    return n * (fee_pm + fee_ks)
