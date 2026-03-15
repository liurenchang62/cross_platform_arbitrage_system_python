# unclassified_logger.py
import os
import csv
from datetime import datetime
from typing import List, Set, Optional, Dict, Tuple
from pathlib import Path
from collections import Counter

from event import Event


class UnclassifiedLogger:
    """未分类日志器：记录没有匹配到任何类别的事件"""

    def __init__(self, log_dir: str = "logs/unclassified"):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self.today_records: Set[str] = set()
        self.current_date = datetime.now().strftime("%Y-%m-%d")

    def _check_date_change(self) -> None:
        """检查日期是否变化，如果变化则清空今日记录"""
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self.current_date:
            self.today_records.clear()
            self.current_date = today

    def log_unclassified(self, event: Event) -> None:
        """记录未分类事件"""
        self._check_date_change()

        # 生成标题哈希用于去重
        title_hash = f"{event.platform}:{event.event_id}"

        # 检查是否已在今日记录过
        if title_hash in self.today_records:
            return

        # 从标题提取关键词（简单提取长度>3的词）
        keywords = []
        for word in event.title.lower().split():
            clean_word = word.strip('.,!?;:()[]{}"\'')
            if len(clean_word) > 3:
                keywords.append(clean_word)

        # 写入CSV文件
        date = datetime.now().strftime("%Y-%m-%d")
        log_file = os.path.join(self.log_dir, f"unclassified-{date}.csv")

        file_exists = os.path.exists(log_file)

        with open(log_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # 如果是新文件，写入表头
            if not file_exists:
                writer.writerow(["timestamp", "event_id", "platform", "title", "keywords"])

            # 写入记录
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                event.event_id,
                event.platform,
                event.title.replace('"', '""'),  # CSV 转义
                ','.join(keywords[:10])  # 最多10个关键词
            ])

        # 记录哈希
        self.today_records.add(title_hash)

    def log_batch_unclassified(self, events: List[Event]) -> int:
        """批量记录未分类事件"""
        count = 0
        for event in events:
            try:
                self.log_unclassified(event)
                count += 1
            except Exception:
                continue
        return count

    def today_record_count(self) -> int:
        """获取今日已记录数量"""
        return len(self.today_records)

    def get_log_file_path(self) -> str:
        """获取日志文件路径"""
        date = datetime.now().strftime("%Y-%m-%d")
        return os.path.join(self.log_dir, f"unclassified-{date}.csv")

    @staticmethod
    def analyze_logs(days: int = 7) -> List[Tuple[str, int]]:
        """分析日志文件，统计高频关键词"""
        keyword_count: Dict[str, int] = {}
        log_dir = Path("logs/unclassified")

        if not log_dir.exists():
            return []

        cutoff_date = datetime.now() - timedelta(days=days)

        for file_path in log_dir.glob("unclassified-*.csv"):
            # 检查文件修改时间
            mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            if mtime < cutoff_date:
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


# 便捷函数
def log_unclassified_event(logger: UnclassifiedLogger, event: Event) -> None:
    """快速记录未分类事件"""
    try:
        logger.log_unclassified(event)
    except Exception as e:
        print(f"⚠️ 记录未分类事件失败: {e}")