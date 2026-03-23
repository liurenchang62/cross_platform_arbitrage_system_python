# unclassified_logger.py
#! 未分类日志模块：记录没有匹配到任何类别的市场

import os
import csv
from datetime import datetime, timedelta
from typing import List, Set, Optional, Dict, Tuple
from pathlib import Path

from market import Market


def _trim_non_alphanumeric_edges(word: str) -> str:
    """与 Rust `trim_matches(|c: char| !c.is_alphanumeric())` 一致：去掉两端非字母数字字符。"""
    if not word:
        return word
    start, end = 0, len(word)
    while start < end and not word[start].isalnum():
        start += 1
    while start < end and not word[end - 1].isalnum():
        end -= 1
    return word[start:end]


class UnclassifiedLogger:
    """未分类日志器：记录没有匹配到任何类别的市场"""

    def __init__(self, log_dir: str = "logs/unclassified"):
        self.log_dir = Path(log_dir)
        os.makedirs(self.log_dir, exist_ok=True)
        self.today_records: Set[str] = set()
        self.current_date = datetime.now().strftime("%Y-%m-%d")

    def _check_date_change(self) -> None:
        """检查日期是否变化，如果变化则清空今日记录"""
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self.current_date:
            self.today_records.clear()
            self.current_date = today

    def log_unclassified(self, market: Market) -> None:
        """记录未分类市场"""
        self._check_date_change()

        # 生成唯一标识用于去重
        record_id = f"{market.platform}:{market.market_id}"

        # 检查是否已在今日记录过
        if record_id in self.today_records:
            return

        # 从标题提取关键词（长度>3的词，去重）
        keywords = set()
        for word in market.title.lower().split():
            clean_word = word.strip('.,!?;:()[]{}"\'')
            if len(clean_word) > 3:
                keywords.add(clean_word)

        # 写入日志文件
        self._write_record(market, sorted(keywords))

        # 记录已处理
        self.today_records.add(record_id)

    def _write_record(self, market: Market, keywords: List[str]) -> None:
        """写入记录到文件"""
        date = datetime.now().strftime("%Y-%m-%d")
        log_file = self.log_dir / f"unclassified-{date}.csv"

        file_exists = log_file.exists()

        with open(log_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # 如果是新文件，写入表头
            if not file_exists:
                writer.writerow(["timestamp", "market_id", "platform", "title", "keywords"])

            # 写入记录
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                market.market_id,
                market.platform,
                market.title,
                ','.join(keywords),
            ])

    def log_batch_unclassified(self, markets: List[Market]) -> int:
        """批量记录未分类市场"""
        count = 0
        for market in markets:
            try:
                self.log_unclassified(market)
                count += 1
            except Exception:
                continue
        return count

    def today_record_count(self) -> int:
        """获取今日已记录数量"""
        return len(self.today_records)

    def get_today_log_path(self) -> Path:
        """获取今日日志文件路径"""
        date = datetime.now().strftime("%Y-%m-%d")
        return self.log_dir / f"unclassified-{date}.csv"

    @staticmethod
    def analyze_recent_logs(days: int = 7) -> List[Tuple[str, int]]:
        """分析最近N天的日志，统计高频关键词"""
        log_dir = Path("logs/unclassified")
        if not log_dir.exists():
            return []

        cutoff_date = datetime.now() - timedelta(days=days)
        keyword_count: Dict[str, int] = {}

        for file_path in log_dir.glob("unclassified-*.csv"):
            # 只处理 .csv 文件
            if file_path.suffix != '.csv':
                continue

            # 从文件名提取日期
            filename = file_path.stem
            date_str = filename.replace("unclassified-", "")

            # 解析日期并检查是否在范围内
            try:
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                if file_date < cutoff_date:
                    continue
            except:
                continue

            # 读取文件内容
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)  # 跳过表头
                for row in reader:
                    if len(row) >= 5:
                        keywords_str = row[4]
                        for keyword in keywords_str.split(','):
                            if keyword:
                                keyword_count[keyword] = keyword_count.get(keyword, 0) + 1

        # 排序并返回前30个
        sorted_keywords = sorted(keyword_count.items(), key=lambda x: x[1], reverse=True)
        return sorted_keywords[:30]


# 便捷函数：快速记录未分类市场
def log_unclassified_market(logger: UnclassifiedLogger, market: Market) -> None:
    """快速记录未分类市场"""
    try:
        logger.log_unclassified(market)
    except Exception as e:
        print(f"⚠️ 记录未分类市场失败: {e}")