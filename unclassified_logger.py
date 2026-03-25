# unclassified_logger.py
#! 未分类日志模块：记录没有匹配到任何类别的市场（与参考实现 CSV 行格式一致）
import os
from datetime import datetime
from pathlib import Path
from typing import List, Set

from market import Market


def _trim_non_alphanumeric_edges(word: str) -> str:
    if not word:
        return word
    start, end = 0, len(word)
    while start < end and not word[start].isalnum():
        start += 1
    while start < end and not word[end - 1].isalnum():
        end -= 1
    return word[start:end]


class UnclassifiedLogger:
    def __init__(self, log_dir: str = "logs/unclassified"):
        self.log_dir = Path(log_dir)
        os.makedirs(self.log_dir, exist_ok=True)
        self.today_records: Set[str] = set()
        self.current_date = datetime.now().strftime("%Y-%m-%d")

    def _check_date_change(self) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self.current_date:
            self.today_records.clear()
            self.current_date = today

    def log_unclassified(self, market: Market) -> None:
        self._check_date_change()
        record_id = f"{market.platform}:{market.market_id}"
        if record_id in self.today_records:
            return

        seen: Set[str] = set()
        kw_order: List[str] = []
        for w in market.title.lower().split():
            cw = _trim_non_alphanumeric_edges(w)
            if len(cw) > 3 and cw not in seen:
                seen.add(cw)
                kw_order.append(cw)
        keywords_joined = ",".join(kw_order)

        self._write_record(market, keywords_joined)
        self.today_records.add(record_id)

    def _write_record(self, market: Market, keywords_joined: str) -> None:
        ts = (
            datetime.now()
            .astimezone()
            .replace(microsecond=0)
            .strftime("%Y-%m-%d %H:%M:%S")
        )
        date = datetime.now().strftime("%Y-%m-%d")
        log_file = self.log_dir / f"unclassified-{date}.csv"
        file_exists = log_file.exists()
        title_esc = market.title.replace('"', '""')
        line = (
            f"{ts},{market.market_id},{market.platform},"
            f'"{title_esc}",{keywords_joined}\n'
        )
        with open(log_file, "a", encoding="utf-8") as f:
            if not file_exists:
                f.write("timestamp,market_id,platform,title,keywords\n")
            f.write(line)

    def log_batch_unclassified(self, markets: List[Market]) -> int:
        count = 0
        for market in markets:
            try:
                self.log_unclassified(market)
                count += 1
            except Exception:
                continue
        return count

    def today_record_count(self) -> int:
        return len(self.today_records)

    def get_today_log_path(self) -> Path:
        date = datetime.now().strftime("%Y-%m-%d")
        return self.log_dir / f"unclassified-{date}.csv"
