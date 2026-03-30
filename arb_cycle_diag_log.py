# arb_cycle_diag_log.py
# 每周期套利校验汇总 CSV（与终端「追踪判定摘要」同源计数）。
from __future__ import annotations

import csv
import threading
from pathlib import Path

from system_params import ARB_CYCLE_DIAG_CSV, ARB_CYCLE_DIAG_CSV_ENABLED

_LOCK = threading.Lock()

# 顺序须与 main.TrackingArbDiag.as_csv_row 一致
ARB_CYCLE_DIAG_HEADER = [
    "time_utc_rfc3339",
    "cycle_id",
    "cycle_phase",
    "pool_size",
    "arb_track_concurrency",
    "attribution_primary",
    "attribution_top_json",
    "attempts",
    "accepted",
    "no_pm_clob_token",
    "pm_book_missing",
    "ks_book_missing",
    "ladders_failed",
    "ladder_pm_invalid_side",
    "ladder_pm_missing_bids",
    "ladder_pm_missing_asks",
    "ladder_pm_malformed",
    "ladder_pm_liq_empty_side",
    "ladder_pm_liq_all_filtered",
    "ladder_ks_no_body",
    "ladder_ks_invalid_side",
    "ladder_ks_malformed",
    "ladder_ks_liq_empty_side",
    "ladder_ks_liq_all_filtered",
    "rejected_strict",
    "missing_best_ask",
    "sum_ask_ge_1",
    "sum_ask_lt_1",
    "loose_pass_strict_fail",
    "loose_still_fail",
]


def append_arb_cycle_diagnostic_row(values: list[str]) -> None:
    if not ARB_CYCLE_DIAG_CSV_ENABLED:
        return
    if len(values) != len(ARB_CYCLE_DIAG_HEADER):
        raise ValueError(
            f"arb cycle diag: expected {len(ARB_CYCLE_DIAG_HEADER)} cols, got {len(values)}"
        )
    path = Path(ARB_CYCLE_DIAG_CSV)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK:
        new_file = not path.exists()
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            if new_file:
                w.writerow(ARB_CYCLE_DIAG_HEADER)
            w.writerow(values)
