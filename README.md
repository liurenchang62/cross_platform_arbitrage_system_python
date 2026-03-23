# Cross-market arbitrage monitor (Python)

**English** | [简体中文](README.zh-CN.md)

## Overview

This **Python** port mirrors the behavior of the Rust `arbitrage-monitor` in `cross_market_arbitrage_project`: it discovers cross-venue relationships between **Polymarket** and **Kalshi** prediction markets, maintains a watchlist of high-confidence pairs, and evaluates economics from **live order-book data**. It performs **read-only analysis** and does not submit orders.

The pipeline combines text-based market matching (vector similarity and category rules), scheduled full refreshes and incremental tracking cycles, and structured logging under `logs/`.

## Features

- **Market data**: Loads open markets from the Polymarket Gamma API and the Kalshi Trade API, with configurable pagination and upper bounds.
- **Matching**: TF-IDF-style text vectors and cosine similarity; optional category constraints and scoring from `config/categories.toml`.
- **Execution-style PnL**: For each candidate pair, costs and net profit for a **notional-capped, depth-walked** buy scenario are derived from **parsed ask ladders** on both venues (see **Order-book PnL model**).
- **Tracking**: Maintains tracked pairs across cycles, with periodic full rebuilds and parameter-driven intervals (`query_params.py`).
- **Logging**: Append-only monitor CSVs under `logs/monitor_YYYY-MM-DD.csv` (local calendar day); unmatched items may be recorded under `logs/unclassified/`.

## Order-book PnL model

Same definition as the Rust project:

1. **Order-book snapshots** — ascending ask ladders `(price, size)`.
2. **Per-leg notional cap** — **100 USDT** per leg (`trade_amount` in `main.py`).
3. **Hedged size** — **n** = minimum of the two per-leg fillable contract counts.
4. **Cost and profit at n** — `cost_for_exact_contracts`, fees, gas → **`net_profit_100`**.

**Implementation reference**: `validate_arbitrage_pair` in `main.py`; `calculate_arbitrage_100usdt`, `calculate_slippage_with_fixed_usdt`, and `cost_for_exact_contracts` in `arbitrage_detector.py`.

## Requirements

- **Python** 3.10+ recommended
- Dependencies: see `requirements.txt`
- Network access to Polymarket and Kalshi public APIs

## Quick start

```bash
pip install -r requirements.txt
python main.py
```

Ensure **`config/categories.toml`** exists before the first run.

## Configuration

| Path | Purpose |
|------|---------|
| `config/categories.toml` | Category names, weights, and keywords |
| `query_params.py` | Request pacing, limits, `SIMILARITY_THRESHOLD`, `SIMILARITY_TOP_K`, `FULL_FETCH_INTERVAL`, `RESOLUTION_HORIZON_DAYS`, etc. |

### Optional environment variables

- **`POLYMARKET_TAG_SLUG`**: When set, Polymarket fetches may be restricted by tag (see `clients.py`).

## Repository layout

```
main.py                 Entry point and monitor loop
clients.py              HTTP clients for Polymarket and Kalshi
market_matcher.py       Matching and index construction
text_vectorizer.py      Text vectorization
vector_index.py         Vector search
arbitrage_detector.py   Order-book traversal, fees, and PnL helpers
query_params.py         Shared tuning constants
validation.py           Validation helpers
tracking.py             Per-cycle monitor state
market_filter.py        Resolution-horizon filtering (21d window)
monitor_logger.py       Daily CSV monitor log
cycle_statistics.py     Cumulative / full-cycle ROI stats
config/
  categories.toml
docs/
  MATCHING_VERIFICATION.md
```

Further detail: **[docs/MATCHING_VERIFICATION.md](docs/MATCHING_VERIFICATION.md)**.

## Disclaimer

- Provided for **research and educational use** only; not investment advice.
- Reported PnL is a **model output** from snapshots and assumptions; **live results may differ**.
- Comply with each platform’s terms of service and applicable law.

## License

If no `LICENSE` file is present, all rights are reserved; add one if you intend to distribute under open terms.
