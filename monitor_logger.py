# monitor_logger.py — 按日 monitor_*.csv 追加套利行
"""按本地自然日追加：`logs/monitor_YYYY-MM-DD.csv`；每行一次验证通过的套利。"""

from __future__ import annotations

import csv
import json
import os
import threading
from datetime import datetime, timezone
from typing import List, Optional

from arbitrage_detector import ArbitrageOpportunity
from log_format import local_datetime_line, utc_datetime_to_rfc3339

CSV_HEADER: List[str] = [
    "event_time_utc_rfc3339",
    "event_time_local",
    "cycle_id",
    "cycle_phase",
    "pm_market_id",
    "kalshi_market_id",
    "pm_title",
    "kalshi_title",
    "text_similarity",
    "match_pm_side",
    "match_kalshi_side",
    "needs_inversion",
    "pm_resolution_utc",
    "kalshi_resolution_utc",
    "strategy",
    "pm_action_verb",
    "pm_action_outcome",
    "pm_action_price",
    "kalshi_action_verb",
    "kalshi_action_outcome",
    "kalshi_action_price",
    "total_cost",
    "gross_profit",
    "fees_simple",
    "net_profit_simple",
    "roi_percent_simple",
    "gas_fee_field",
    "final_profit_field",
    "final_roi_field",
    "pm_optimal",
    "kalshi_optimal",
    "pm_avg_slipped",
    "kalshi_avg_slipped",
    "contracts_n",
    "capital_used",
    "fees_amount",
    "gas_amount",
    "net_profit_100",
    "roi_100_percent",
    "orderbook_pm_top5_json",
    "orderbook_kalshi_top5_json",
]


def _fmt_f64(x: float) -> str:
    return f"{x:.12f}"


def _utc_rfc3339(dt: Optional[datetime]) -> str:
    if dt is None:
        return ""
    return utc_datetime_to_rfc3339(dt)


class MonitorLogger:
    def __init__(self, logs_dir: str = "logs") -> None:
        self.logs_dir = logs_dir
        self._lock = threading.Lock()
        os.makedirs(logs_dir, exist_ok=True)

    def _path_for_local_date(self, date_local: str) -> str:
        return os.path.join(self.logs_dir, f"monitor_{date_local}.csv")

    @staticmethod
    def _local_date_string() -> str:
        return datetime.now().astimezone().strftime("%Y-%m-%d")

    def _append_records(self, rows: List[List[str]]) -> None:
        with self._lock:
            date = self._local_date_string()
            path = self._path_for_local_date(date)
            parent = os.path.dirname(path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            need_header = (not os.path.exists(path)) or os.path.getsize(path) == 0

            with open(path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f, lineterminator="\n")
                if need_header:
                    w.writerow(CSV_HEADER)
                for row in rows:
                    w.writerow(row)

    def log_arbitrage_opportunity(
        self,
        cycle_id: int,
        cycle_phase: str,
        opp: ArbitrageOpportunity,
        pm_market_id: str,
        kalshi_market_id: str,
        pm_title: str,
        kalshi_title: str,
        similarity: float,
        match_pm_side: str,
        match_kalshi_side: str,
        needs_inversion: bool,
        pm_resolution: Optional[datetime],
        kalshi_resolution: Optional[datetime],
    ) -> None:
        at_utc = datetime.now(timezone.utc)
        local_line = local_datetime_line(at_utc)

        ob_pm = json.dumps(
            [[p, s] for p, s in opp.orderbook_pm_top5], separators=(",", ":")
        )
        ob_ks = json.dumps(
            [[p, s] for p, s in opp.orderbook_kalshi_top5], separators=(",", ":")
        )

        r: List[str] = [""] * len(CSV_HEADER)
        r[0] = utc_datetime_to_rfc3339(at_utc)
        r[1] = local_line
        r[2] = str(cycle_id)
        r[3] = cycle_phase
        r[4] = pm_market_id
        r[5] = kalshi_market_id
        r[6] = pm_title
        r[7] = kalshi_title
        r[8] = f"{similarity:.6f}"
        r[9] = match_pm_side
        r[10] = match_kalshi_side
        r[11] = "true" if needs_inversion else "false"
        r[12] = _utc_rfc3339(pm_resolution)
        r[13] = _utc_rfc3339(kalshi_resolution)
        r[14] = opp.strategy
        r[15] = opp.polymarket_action[0]
        r[16] = opp.polymarket_action[1]
        r[17] = _fmt_f64(opp.polymarket_action[2])
        r[18] = opp.kalshi_action[0]
        r[19] = opp.kalshi_action[1]
        r[20] = _fmt_f64(opp.kalshi_action[2])
        r[21] = _fmt_f64(opp.total_cost)
        r[22] = _fmt_f64(opp.gross_profit)
        r[23] = _fmt_f64(opp.fees)
        r[24] = _fmt_f64(opp.net_profit)
        r[25] = _fmt_f64(opp.roi_percent)
        r[26] = _fmt_f64(opp.gas_fee)
        r[27] = _fmt_f64(opp.final_profit)
        r[28] = _fmt_f64(opp.final_roi_percent)
        r[29] = _fmt_f64(opp.pm_optimal)
        r[30] = _fmt_f64(opp.kalshi_optimal)
        r[31] = _fmt_f64(opp.pm_avg_slipped)
        r[32] = _fmt_f64(opp.kalshi_avg_slipped)
        r[33] = _fmt_f64(opp.contracts)
        r[34] = _fmt_f64(opp.capital_used)
        r[35] = _fmt_f64(opp.fees_amount)
        r[36] = _fmt_f64(opp.gas_amount)
        r[37] = _fmt_f64(opp.net_profit_100)
        r[38] = _fmt_f64(opp.roi_100_percent)
        r[39] = ob_pm
        r[40] = ob_ks

        assert len(r) == len(CSV_HEADER)
        self._append_records([r])
