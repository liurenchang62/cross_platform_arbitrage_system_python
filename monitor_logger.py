# monitor_logger.py
import os
from datetime import datetime
from typing import Optional
from pathlib import Path

from arbitrage_detector import ArbitrageOpportunity


class MonitorLogger:
    """监控日志系统"""

    def __init__(self, logs_dir: str = "logs"):
        self.logs_dir = logs_dir
        os.makedirs(logs_dir, exist_ok=True)

    def _time_bucket_15m(self, dt: datetime) -> str:
        """获取15分钟时间桶"""
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        hour = dt.strftime("%H")
        minute = int(dt.strftime("%M"))
        minute_bucket = (minute // 15) * 15
        return f"{year}-{month}-{day}_{hour}-{minute_bucket:02d}"

    def _ensure_logs_dir(self) -> None:
        """确保日志目录存在"""
        os.makedirs(self.logs_dir, exist_ok=True)

    def _append_monitor_log(self, line: str, at: Optional[datetime] = None) -> None:
        """追加监控日志"""
        if at is None:
            at = datetime.utcnow()

        self._ensure_logs_dir()

        bucket = self._time_bucket_15m(at)
        filename = f"monitor_{bucket}.log"
        filepath = os.path.join(self.logs_dir, filename)

        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(line + '\n')

    def log_opportunity(self, opportunity: ArbitrageOpportunity) -> None:
        """记录套利机会"""
        at = datetime.utcnow()
        line = (
            f"[{at.isoformat()}] 策略: {opportunity.strategy}, "
            f"成本: {opportunity.total_cost:.3f}, "
            f"净利: {opportunity.net_profit:.3f}, "
            f"ROI: {opportunity.roi_percent:.1f}%"
        )
        self._append_monitor_log(line, at)

    def log_message(self, message: str) -> None:
        """记录普通消息"""
        at = datetime.utcnow()
        line = f"[{at.isoformat()}] {message}"
        self._append_monitor_log(line, at)