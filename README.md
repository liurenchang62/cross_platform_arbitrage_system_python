# Cross-market arbitrage monitor (Python)

**English** | [简体中文](README.zh-CN.md)

## Overview

This repository contains a **Python** application that discovers cross-venue relationships between **Polymarket** and **Kalshi** prediction markets, maintains a watchlist of high-confidence candidate pairs, and evaluates **executable economics** from **live order-book data**. The program performs **read-only analysis**: it fetches public market listings and order books, runs matching and profitability models, and writes structured logs. It **does not** submit orders or trade on your behalf.

The end-to-end pipeline includes:

- Pulling open markets from both platforms with configurable pagination and safety limits.
- **Text-based matching** using TF-IDF–style vectors, cosine similarity, and category-aware rules driven by `config/categories.toml`.
- A **second-pass validation** stage (`validation.py`) that filters implausible pairings (cross-sport or structurally incompatible market types) before order-book analysis.
- Optional **resolution-horizon filtering** so that only markets expected to resolve within a configured calendar window are considered (`market_filter.py`, parameters in `system_params.py`).
- **Tracking** of pairs across monitor cycles, with periodic full rebuilds of the candidate set and incremental updates for pairs already under watch (`tracking.py`).
- **Paper trading (optional)**: When enabled in `system_params.py`, `paper_trading.py` simulates cash, open positions per verified pair, per-cycle **early exit** on combined bid liquidity (with cooldown after closes), and append-only CSV under `logs/paper_trades.csv` (session markers, `OPEN`, `CLOSE`, `NO_CLOSE`). The **`backtest`** package (`python -m backtest`) interactively summarizes that CSV by UTC session-start day, matching the reference workspace’s `backtest` crate.
- **Structured logging**: daily monitor CSV files under `logs/`, plus optional capture of hard-to-classify markets under `logs/unclassified/`.

Together, these pieces support continuous monitoring for situations where the same underlying proposition may be priced differently across venues, subject to the limitations described under **Disclaimer**.

## Features

### Market data

- Loads **open** markets from the **Polymarket Gamma API** and the **Kalshi Trade API**.
- Pagination, per-request limits, and global caps are centralized in `system_params.py` so you can tune how aggressively the monitor scans without changing core logic.
- Market records are normalized into internal structures (`market.py`) for consistent handling in matching and logging.

### Matching and classification

- **Vectorization**: English-oriented tokenization and stemming (via `snowballstemmer` in `text_vectorizer.py`) produce sparse vectors comparable in spirit to classic TF-IDF weighting.
- **Indexing and search**: `vector_index.py` supports efficient similarity queries; `market_matcher.py` orchestrates building indices per venue, cross-querying, and applying **top‑K** and **similarity threshold** cuts from `system_params.py`.
- **Categories**: `config/categories.toml` supplies category labels, keyword hints, and weights. `category_mapper.py` and `category_vectorizer.py` integrate category signals with text similarity so that matches are both numerically close and semantically plausible.

### Second-pass validation

- After vector similarity proposes a candidate pair, `validation.py` runs a **fixed-order pipeline** of rule-based checks (sports vs politics, totals vs outrights, esports map winners vs series winners, handicap vs moneyline, and many other cross-type guards).
- Pairs that fail validation are **discarded** before any order-book or PnL work runs, reducing noise from embeddings alone.

### Resolution horizon

- `market_filter.py` can restrict the universe to markets whose **expected resolution** falls within a configured number of days ahead (`RESOLUTION_HORIZON_DAYS` and related settings in `system_params.py`), so monitoring focuses on nearer-dated events when desired.

### Execution-style profit and loss (order-book scenario)

- For each **validated** pair that passes similarity and business rules, the program requests **current** order books on both venues and parses **ask-side liquidity** into ascending price ladders.
- It then computes a **notional-capped, depth-walked** buy scenario on each leg, derives a **common hedged size**, and reports **capital used**, **fees**, **assumed gas**, and **`net_profit_100`** (see **Order-book PnL model** below).
- Implementation lives in `arbitrage_detector.py` and is invoked from `main.py` (`validate_arbitrage_pair` and related helpers).

### Tracking across cycles

- `tracking.py` records which pairs are under active watch, their last-seen similarity and profitability, and supports **full refresh** intervals versus lighter incremental passes, as configured in `system_params.py`.
- This allows the monitor to keep continuity across runs without re-deriving the entire opportunity set every cycle.

### Logging and auxiliary tools

- **`monitor_logger.py`**: append-only **daily** CSV logs (`logs/monitor_YYYY-MM-DD.csv` in local calendar terms) capturing arbitrage-relevant rows for later analysis.
- **`cycle_statistics.py`**: aggregates cycle-level statistics (for example cumulative or full-pass return-on-capital style summaries printed during long runs).
- **`unclassified_logger.py`**: optional logging when markets do not map cleanly to configured categories.
- **`check_unclassified.py`**: helper script to inspect or summarize unclassified logs.

### Paper trading (simulation)

- Controlled by **`PAPER_TRADING_ENABLED`** and related constants in `system_params.py` (per-leg cap aligns with **`PAPER_PER_LEG_CAP_USDT`**, same notional as the monitor’s depth-based check).
- Writes optional **`logs/paper_trades.csv`** when **`PAPER_WRITE_TRADE_LOG`** is true; optional run label via environment variable **`PAPER_RUN_LABEL`**.

## Order-book PnL model

The scenario used for ranking and reporting (for example cycle **Top 10** and **`net_profit_100`**) is defined as follows:

1. **Order-book snapshots**  
   For each matched pair that survives validation, the program requests the **current** Polymarket and Kalshi **order books** over HTTP and parses resting **sell-side** liquidity into **ascending ask ladders** `(price, size)`.

2. **Per-leg notional cap**  
   Each leg is allocated a maximum spend of **100 USDT** (`PAPER_PER_LEG_CAP_USDT` in `system_params.py`, assigned in `main.py` as `trade_amount`). The ladder is traversed **level by level** until that cap is reached or liquidity is exhausted (`calculate_slippage_with_fixed_usdt` in `arbitrage_detector.py`), yielding a **fillable contract count** per venue for that cap.

3. **Hedged size**  
   The scenario size **n** is the **minimum** of the two per-leg contract counts so that both legs can be notionally filled at the **same** number of contracts.

4. **Cost and profit at size n**  
   For exactly **n** contracts, per-leg total cost and **volume-weighted average prices** are recomputed by walking each ladder again (`cost_for_exact_contracts`). Combined legs yield **`capital_used`**. Platform fees and a **fixed gas assumption** are subtracted to obtain **`net_profit_100`**. A row is treated as an actionable opportunity when **`net_profit_100`** exceeds the detector’s configured minimum; this gate is applied to the **depth-based** result, not to a single price level in isolation.

**Implementation reference**: `validate_arbitrage_pair` in `main.py`; `calculate_arbitrage_100usdt`, `calculate_slippage_with_fixed_usdt`, and `cost_for_exact_contracts` in `arbitrage_detector.py`.

## Requirements

- **Python** 3.10 or newer recommended.
- Install dependencies from `requirements.txt`:
  - **`aiohttp`** — asynchronous HTTP client for API calls.
  - **`numpy`** — numerical routines used in vector operations.
- **`snowballstemmer`** — English stemming for text tokenization.
- **`toml`** — parsing `config/categories.toml`.
- **`questionary`** — interactive selects for `python -m backtest`.
- **`wcwidth`** — terminal column alignment for CJK in the backtest report (same layout intent as `unicode-width` in the reference tool).
- Reliable **network access** to Polymarket and Kalshi public APIs. Endpoint URLs and rate limits may change; verify against current platform documentation if something stops working.

## Quick start

```bash
pip install -r requirements.txt
python main.py
```

### Paper backtest CLI

```bash
python -m backtest
```

Reads `logs/paper_trades.csv` by default, or the path in **`PAPER_TRADES_CSV`**. Interactive prompts follow the same UTC session-start rules and print the same boxed report as the reference `backtest` binary.

Ensure **`config/categories.toml`** exists before the first run (a starter file is expected to live under `config/` in this repository).

## Configuration

| Path | Purpose |
|------|---------|
| `config/categories.toml` | Category names, weights, and keyword lists used for classification-aware matching. |
| `system_params.py` | Request pacing, page sizes, fetch caps, **`SIMILARITY_THRESHOLD`**, **`SIMILARITY_TOP_K`**, **`FULL_FETCH_INTERVAL`**, **`RESOLUTION_HORIZON_DAYS`**, paper-trading toggles (`PAPER_TRADING_ENABLED`, `PAPER_PER_LEG_CAP_USDT`, …), and other global tuning constants. |

### Optional environment variables

- **`POLYMARKET_TAG_SLUG`**: When set, Polymarket market fetches may be restricted to a specific tag slug (see `clients.py`).
- **`PAPER_RUN_LABEL`**: When paper logging is enabled, appended to CSV `notes` / session rows to tag test runs (see `system_params.PAPER_RUN_LABEL_ENV`).
- **`PAPER_TRADES_CSV`**: Overrides the default path for the paper-trades file when running **`python -m backtest`** (see `system_params.PAPER_TRADES_CSV_ENV`).

## Repository layout

```
main.py                 Entry point and monitor loop
clients.py              HTTP clients for Polymarket and Kalshi
market.py               Normalized market record types
market_matcher.py       Matching, similarity search, and index construction
text_vectorizer.py      Tokenization, stemming, and vectorization
category_vectorizer.py  Category-aware vector helpers
category_mapper.py      Category assignment from configuration
vector_index.py         Vector index and nearest-neighbor search
validation.py           Second-pass rule pipeline for candidate pairs
market_filter.py        Resolution-horizon and related listing filters
arbitrage_detector.py   Order-book traversal, fees, gas, and PnL helpers
system_params.py        Shared tuning constants, API pacing, paper trading
paper_trading.py        Optional simulated positions and trade log CSV
backtest/               `python -m backtest` — paper CSV session performance CLI
log_format.py           UTC/local time strings aligned with chrono CSV columns
tracking.py             Per-cycle watch state for tracked pairs
monitor_logger.py       Daily CSV monitor log writer
cycle_statistics.py     Cycle-level statistical summaries
unclassified_logger.py  Logging for unclassified markets
check_unclassified.py   Utility to inspect unclassified logs
config/
  categories.toml
docs/
  MATCHING_VERIFICATION.md
requirements.txt
```

Further detail on matching behavior and verification notes: **[docs/MATCHING_VERIFICATION.md](docs/MATCHING_VERIFICATION.md)**.

## Disclaimer

- Provided for **research and educational use** only; nothing herein is **investment**, **trading**, or **legal** advice.
- Prediction markets differ in **rules**, **settlement**, **liquidity**, **fees**, and **latency**. Reported PnL is a **model output** derived from **point-in-time** order-book snapshots and simplifying assumptions (including fees and gas); **live results may differ materially**.
- You are responsible for complying with each platform’s **terms of service**, **API policies**, and **applicable laws and regulations** in your jurisdiction.

## License

If no `LICENSE` file is present in this repository, all rights are reserved. Add a license file if you intend to distribute this project under open terms.
