# monitor_logger.py — 与 Rust `monitor_logger.rs` 对齐
import os
from datetime import datetime, timezone
from typing import Optional

from arbitrage_detector import ArbitrageOpportunity


class MonitorLogger:
    def __init__(self, logs_dir: str = "logs"):
        self.logs_dir = logs_dir
        os.makedirs(logs_dir, exist_ok=True)

    def _time_bucket_15m(self, dt: datetime) -> str:
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        hour = dt.strftime("%H")
        minute = int(dt.strftime("%M"))
        minute_bucket = (minute // 15) * 15
        return f"{year}-{month}-{day}_{hour}-{minute_bucket:02d}"

    def _ensure_logs_dir(self) -> None:
        os.makedirs(self.logs_dir, exist_ok=True)

    def _append_monitor_log(self, line: str, at: Optional[datetime] = None) -> None:
        if at is None:
            at = datetime.now(timezone.utc)
        self._ensure_logs_dir()
        bucket = self._time_bucket_15m(at)
        filename = f"monitor_{bucket}.log"
        filepath = os.path.join(self.logs_dir, filename)
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def log_opportunity(self, opportunity: ArbitrageOpportunity) -> None:
        at = datetime.now(timezone.utc)
        ts = at.isoformat().replace("+00:00", "Z")
        if opportunity.contracts > 0.0:
            line = (
                f"[{ts}] 策略: {opportunity.strategy}, "
                f"本金: ${opportunity.capital_used:.2f}, "
                f"净利: ${opportunity.net_profit_100:.2f}, "
                f"ROI: {opportunity.roi_100_percent:.1f}%"
            )
        else:
            line = (
                f"[{ts}] 策略: {opportunity.strategy}, "
                f"成本: {opportunity.total_cost:.3f}, "
                f"净利: {opportunity.net_profit:.3f}, "
                f"ROI: {opportunity.roi_percent:.1f}%"
            )
        self._append_monitor_log(line, at)

    def log_message(self, message: str) -> None:
        at = datetime.now(timezone.utc)
        ts = at.isoformat().replace("+00:00", "Z")
        line = f"[{ts}] {message}"
        self._append_monitor_log(line, at)
