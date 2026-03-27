# system_params.py
# 系统级常量：HTTP/分页、匹配与索引、市场窗口、模拟交易（Paper）等。
import math

# ==================== 通用参数 ====================
REQUEST_INTERVAL_MS = 200
MAX_RETRIES = 3
RETRY_INITIAL_DELAY_MS = 1000

# ==================== Polymarket 参数 ====================
POLYMARKET_PAGE_LIMIT = 200
POLYMARKET_MAX_MARKETS = 40000

# ==================== Kalshi 参数 ====================
KALSHI_PAGE_LIMIT = 1000
KALSHI_MAX_MARKETS = 40000

# Kalshi Demo：为 true 时必须配置 KALSHI_DEMO_* 环境变量；为 false 时使用生产 Trade API，并忽略 Demo 环境变量。
KALSHI_DEMO_MODE_ENABLED = False

KALSHI_DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"

KALSHI_DEMO_API_KEY_ID_ENV = "KALSHI_DEMO_API_KEY_ID"

KALSHI_DEMO_PRIVATE_KEY_PATH_ENV = "KALSHI_DEMO_PRIVATE_KEY_PATH"

# ==================== 向量化参数 ====================
MAX_VOCAB_SIZE = None

# ==================== 匹配参数 ====================
SIMILARITY_THRESHOLD = 0.8
SIMILARITY_TOP_K = 15
# 按大类计算 P·Kᵀ / K·Pᵀ 时，行块大小（用于控制矩阵乘法子任务粒度）
MATCH_MATMUL_CHUNK_ROWS = 512
FULL_FETCH_INTERVAL = 180

# ==================== 市场时间窗口 ====================
RESOLUTION_HORIZON_DAYS = 21

# ==================== 模拟交易（Paper）====================
PAPER_TRADING_ENABLED = False
PAPER_WRITE_TRADE_LOG = True
PAPER_RUN_LABEL_ENV = "PAPER_RUN_LABEL"
PAPER_SESSION_COUNTER_FILE = "logs/paper_session_counter.txt"
PAPER_TRADES_CSV = "logs/paper_trades.csv"
# 回测 CLI：`python -m backtest` 可通过此环境变量覆盖默认 CSV 路径
PAPER_TRADES_CSV_ENV = "PAPER_TRADES_CSV"

CASH_UTILIZATION_MAX = 0.95

DEMO_REFERENCE_BUDGET_USD = 500.0

LOCAL_TOTAL_USD = 10_000.0

PAIR_CAP_FRAC_OF_REFERENCE = 0.04

PER_LEG_CAP_FRAC_OF_REFERENCE = 0.02


def paper_caps_demo() -> tuple[float, float]:
    r = DEMO_REFERENCE_BUDGET_USD
    return (
        r * PER_LEG_CAP_FRAC_OF_REFERENCE,
        r * PAIR_CAP_FRAC_OF_REFERENCE,
    )


def paper_caps_local() -> tuple[float, float]:
    r = LOCAL_TOTAL_USD
    return (
        r * PER_LEG_CAP_FRAC_OF_REFERENCE,
        r * PAIR_CAP_FRAC_OF_REFERENCE,
    )


PAPER_COOLDOWN_CYCLES = 5
PAPER_INITIAL_CASH = LOCAL_TOTAL_USD
PAPER_MIN_EDGE_EARLY_USD = 0.5


def paper_settlement_fee_estimate(n: float, fee_pm: float, fee_ks: float) -> float:
    if n <= 0.0 or not math.isfinite(n):
        return 0.0
    return n * (fee_pm + fee_ks)
