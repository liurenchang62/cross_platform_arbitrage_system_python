# unclassified_logger.py
# 未分类日志：记录未能归入配置类别的市场（CSV 行格式与监控日志分列一致）。
from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Set, Tuple

from market import Market


_analyze_log_dir = Path("logs/unclassified")


def _trim_non_alphanumeric_edges(word: str) -> str:
    if not word:
        return word
    start, end = 0, len(word)
    while start < end and not word[start].isalnum():
        start += 1
    while start < end and not word[end - 1].isalnum():
        end -= 1
    return word[start:end]


def analyze_recent_logs(days: int) -> List[Tuple[str, int]]:
    """分析最近 N 天的未分类日志，统计关键词频次。"""
    log_dir = _analyze_log_dir
    if not log_dir.is_dir():
        return []

    cutoff_dt = datetime.now() - timedelta(days=days)
    keyword_count: dict[str, int] = defaultdict(int)

    for entry in log_dir.iterdir():
        path = entry
        if not path.is_file() or path.suffix.lower() != ".csv":
            continue
        stem = path.stem
        if not stem.startswith("unclassified-"):
            continue
        date_str = stem[len("unclassified-") :]
        try:
            file_midnight = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            continue
        if file_midnight < cutoff_dt:
            continue

        try:
            with open(path, encoding="utf-8") as f:
                lines = f.readlines()
        except OSError:
            continue

        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            fields = line.split(",")
            if len(fields) < 5:
                continue
            keywords_str = fields[4]
            for keyword in keywords_str.split(","):
                if keyword:
                    keyword_count[keyword] += 1

    result: List[Tuple[str, int]] = list(keyword_count.items())
    result.sort(key=lambda x: (-x[1], x[0]))
    return result[:30]


def log_unclassified_market(logger: UnclassifiedLogger, market: Market) -> None:
    try:
        logger.log_unclassified(market)
    except Exception as e:
        print(f"⚠️ 记录未分类市场失败: {e}", file=sys.stderr)


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
        for w in market.title.lower().split():
            cw = _trim_non_alphanumeric_edges(w)
            if len(cw) > 3:
                seen.add(cw)
        keywords_joined = ",".join(sorted(seen))

        self._write_record(market, keywords_joined)
        self.today_records.add(record_id)

    def _write_record(self, market: Market, keywords_joined: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
