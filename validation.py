# validation.py
#! 二筛模块：对向量匹配结果进行精确验证

import re
from typing import Optional, List, Tuple, Dict, Set
from dataclasses import dataclass, field

# 安全词列表（单方有日期时放行）
SAFE_WORDS = [
    "next", "upcoming", "today", "tonight", "future", "current"
]

# 体育比分关键词
SPORTS_KEYWORDS = [
    "points", "goals", "runs", "o/u", "over/under", "over", "under",
    "winner", "win", "tie", "draw", "spread", "moneyline", "total",
    "vs", "versus", "score", "scored", "mvp", "championship", "points",
    "rebounds", "assists"
]

# 体育垃圾市场关键词
GARBAGE_KEYWORDS = [
    "o/u", "rounds", "sets", "games", "maps", "upsets",
    "quarters", "halves", "periods", "wins"
]

# 统计数据类型（必须互斥）
STAT_TYPES = [
    "points", "rebounds", "assists", "steals",
    "blocks", "threes", "double", "triple"
]

# 月份名称映射
MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


@dataclass
class MatchInfo:
    """匹配结果（带方向信息）"""
    pm_title: str
    kalshi_title: str
    similarity: float
    category: str
    pm_side: str      # "YES" 或 "NO"
    kalshi_side: str  # "YES" 或 "NO"
    needs_inversion: bool  # 是否需要颠倒 Y/N 含义


@dataclass
class DateInfo:
    """提取的日期信息"""
    month: int
    day: int
    has_year: bool
    year: Optional[int] = None


@dataclass
class NumberInfo:
    """提取的数值信息"""
    value: float
    context: str
    is_year: bool


@dataclass
class RetainedSample:
    """留存样本信息"""
    pm_title: str
    kalshi_title: str
    similarity: float
    category: str
    pm_side: str
    kalshi_side: str
    needs_inversion: bool


# ==================== 工具函数 ====================

def extract_number(text: str) -> Optional[float]:
    """从文本中提取数字"""
    match = re.search(r'(\d+\.?\d*)', text)
    if match:
        try:
            return float(match.group(1))
        except:
            pass
    return None


def extract_first_team(title: str) -> str:
    """提取第一个队伍名称"""
    if " vs " in title:
        return title.split(" vs ")[0].strip()
    elif " vs. " in title:
        return title.split(" vs. ")[0].strip()
    return ""


def extract_winner(title: str) -> str:
    """提取胜者"""
    if " - " in title:
        return title.split(" - ")[-1].strip()
    return ""


# ==================== 垃圾市场检测 ====================

class GarbageMarketDetector:
    """垃圾市场检测器"""

    @staticmethod
    def is_garbage_sports_market(title: str) -> bool:
        """判断是否为垃圾体育市场"""
        lower = title.lower()

        # 硬规则: O/U X.X Rounds 这种直接扔
        if "o/u" in lower and "rounds" in lower:
            upper_count = sum(1 for c in title if c.isupper())
            has_specific = (" vs " in lower or
                           " at " in lower or
                           " - " in lower or
                           upper_count > 2)

            if not has_specific:
                return True

        # 检查垃圾关键词
        has_garbage = any(kw in lower for kw in GARBAGE_KEYWORDS)
        if has_garbage:
            upper_count = sum(1 for c in title if c.isupper())
            has_specific = (" vs " in lower or
                           " at " in lower or
                           " - " in lower or
                           upper_count > 1)

            if not has_specific:
                numbers = NumberComparator.extract_numbers(title)
                if numbers:
                    return True

        return False


# ==================== 胜负市场验证器 ====================

class WinnerMarketValidator:
    """胜负市场验证器"""

    @staticmethod
    def validate(pm_title: str, kalshi_title: str) -> Optional[Tuple[str, str, bool]]:
        """验证胜负市场"""
        # 检查是否都是胜负市场
        pm_is_winner = " vs " in pm_title or " vs. " in pm_title
        ks_is_winner = "Winner" in kalshi_title or " - " in kalshi_title

        if not pm_is_winner or not ks_is_winner:
            return None

        pm_team = extract_first_team(pm_title)
        ks_winner = extract_winner(kalshi_title)

        if not pm_team or not ks_winner:
            return None

        # 判断是否匹配
        if pm_team.lower() == ks_winner.lower():
            # 直接匹配：PM 买 Yes（前者胜） = Kalshi 买 Yes（前者胜）
            return ("YES", "YES", False)
        else:
            # 颠倒匹配：PM 买 Yes（前者胜） = Kalshi 买 No（后者胜）
            return ("YES", "NO", True)


# ==================== 得分市场验证器 ====================

class ScoreMarketValidator:
    """得分市场验证器"""

    @staticmethod
    def validate(pm_title: str, kalshi_title: str) -> Optional[Tuple[str, str, bool]]:
        """验证得分市场"""
        # 检查是否都是得分市场
        pm_is_score = "O/U" in pm_title or "Points" in pm_title
        ks_is_score = '+' in kalshi_title or '-' in kalshi_title or "points" in kalshi_title

        if not pm_is_score or not ks_is_score:
            return None

        pm_num = extract_number(pm_title)
        ks_num = extract_number(kalshi_title)

        if pm_num is None or ks_num is None:
            return None

        # 判断方向
        pm_is_over = "under" not in pm_title.lower()

        if '+' in kalshi_title:
            # Kalshi + 表示 Over
            if not pm_is_over:
                return None  # 方向不一致
            pm_threshold = int(pm_num + 0.5)  # ceil
            ks_threshold = int(ks_num)

            if abs(pm_threshold - ks_threshold) <= 1:
                return ("YES", "YES", False)

        elif '-' in kalshi_title:
            # Kalshi - 表示 Under
            if pm_is_over:
                return None  # 方向不一致
            pm_threshold = int(pm_num)  # floor
            ks_threshold = int(ks_num)

            if abs(pm_threshold - ks_threshold) <= 1:
                # 方向相反，需要颠倒 Y/N
                return ("YES", "NO", True)

        else:
            # 默认按 Over 处理
            pm_threshold = int(pm_num + 0.5) if pm_is_over else int(pm_num)
            ks_threshold = int(ks_num)

            if abs(pm_threshold - ks_threshold) <= 1:
                return ("YES", "YES", False)

        return None


# ==================== 日期验证器 ====================

class DateValidator:
    """日期验证器"""

    def __init__(self):
        pass

    @staticmethod
    def extract_date(text: str) -> Optional[DateInfo]:
        """从文本中提取日期"""
        text_lower = text.lower()

        # 匹配 "Month Day, Year" 格式
        month_pattern = r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2})(?:,?\s*(\d{4}))?'
        match = re.search(month_pattern, text_lower, re.IGNORECASE)
        if match:
            month_name = match.group(1).lower()
            day = int(match.group(2))

            # 找到月份
            month = None
            for name, m in MONTH_MAP.items():
                if name in month_name:
                    month = m
                    break

            if month is None:
                return None

            year = None
            if match.group(3):
                year = int(match.group(3))

            return DateInfo(
                month=month,
                day=day,
                has_year=year is not None,
                year=year
            )

        # 匹配纯年份
        year_match = re.search(r'\b(20\d{2})\b', text)
        if year_match:
            year = int(year_match.group(1))
            return DateInfo(
                month=0,
                day=0,
                has_year=True,
                year=year
            )

        return None

    @staticmethod
    def has_safe_word(text: str) -> bool:
        """检查是否包含安全词"""
        text_lower = text.lower()
        return any(word in text_lower for word in SAFE_WORDS)

    @staticmethod
    def dates_match(d1: DateInfo, d2: DateInfo) -> bool:
        """比较两个日期是否匹配"""
        if d1.month > 0 and d1.day > 0 and d2.month > 0 and d2.day > 0:
            return d1.month == d2.month and d1.day == d2.day
        return False

    def validate(self, pm_title: str, kalshi_title: str) -> bool:
        """验证两个标题的日期是否匹配"""
        pm_date = self.extract_date(pm_title)
        kalshi_date = self.extract_date(kalshi_title)

        if pm_date and kalshi_date:
            return self.dates_match(pm_date, kalshi_date)
        elif pm_date and not kalshi_date:
            return self.has_safe_word(pm_title)
        elif not pm_date and kalshi_date:
            return self.has_safe_word(kalshi_title)
        else:
            return True


# ==================== 体育比分识别器 ====================

class SportsIdentifier:
    """体育比分识别器"""

    @staticmethod
    def is_sports_market(title: str) -> bool:
        """判断是否为体育市场"""
        title_lower = title.lower()
        return any(kw in title_lower for kw in SPORTS_KEYWORDS)


# ==================== 数值比较器 ====================

class NumberComparator:
    """数值比较器"""

    @staticmethod
    def extract_numbers(text: str) -> List[NumberInfo]:
        """从文本中提取数值"""
        numbers = []
        matches = re.finditer(r'(\d+\.?\d*)', text)
        for match in matches:
            try:
                value = float(match.group(1))
                is_year = 2000 <= value < 2100
                numbers.append(NumberInfo(
                    value=value,
                    context=text,
                    is_year=is_year
                ))
            except:
                continue
        return numbers

    @staticmethod
    def compare_numbers(nums1: List[NumberInfo], nums2: List[NumberInfo]) -> bool:
        """比较两组数值是否匹配"""
        if not nums1 and not nums2:
            return True

        if bool(nums1) != bool(nums2):
            return False

        for n1 in nums1:
            for n2 in nums2:
                if n1.is_year != n2.is_year:
                    continue
                if abs(n1.value - n2.value) <= 0.5:
                    return True

        return False


# ==================== 主验证管道 ====================

class ValidationPipeline:
    """主验证管道"""

    def __init__(self):
        self.date_validator = DateValidator()
        self.filtered_count = 0
        self.filtered_samples: List[Tuple[str, str, str]] = []
        self.retained_samples: Dict[str, List[RetainedSample]] = {}

    def validate(self, pm_title: str, kalshi_title: str, similarity: float, category: str) -> Optional[MatchInfo]:
        """验证匹配对"""
        # 0. 垃圾市场检测
        if (GarbageMarketDetector.is_garbage_sports_market(pm_title) or
            GarbageMarketDetector.is_garbage_sports_market(kalshi_title)):
            self._record_filter(pm_title, kalshi_title, "垃圾市场")
            return None

        # 1. 日期验证
        if not self.date_validator.validate(pm_title, kalshi_title):
            self._record_filter(pm_title, kalshi_title, "日期不匹配")
            return None

        # 2. 尝试胜负市场匹配
        winner_result = WinnerMarketValidator.validate(pm_title, kalshi_title)
        if winner_result:
            pm_side, kalshi_side, needs_inversion = winner_result
            match_info = MatchInfo(
                pm_title=pm_title,
                kalshi_title=kalshi_title,
                similarity=similarity,
                category=category,
                pm_side=pm_side,
                kalshi_side=kalshi_side,
                needs_inversion=needs_inversion,
            )
            self._record_retained(match_info)
            return match_info

        # 3. 尝试得分市场匹配
        score_result = ScoreMarketValidator.validate(pm_title, kalshi_title)
        if score_result:
            pm_side, kalshi_side, needs_inversion = score_result
            match_info = MatchInfo(
                pm_title=pm_title,
                kalshi_title=kalshi_title,
                similarity=similarity,
                category=category,
                pm_side=pm_side,
                kalshi_side=kalshi_side,
                needs_inversion=needs_inversion,
            )
            self._record_retained(match_info)
            return match_info

        # 4. 默认数值比较
        pm_numbers = NumberComparator.extract_numbers(pm_title)
        kalshi_numbers = NumberComparator.extract_numbers(kalshi_title)

        if not NumberComparator.compare_numbers(pm_numbers, kalshi_numbers):
            self._record_filter(pm_title, kalshi_title, "数值不匹配")
            return None

        match_info = MatchInfo(
            pm_title=pm_title,
            kalshi_title=kalshi_title,
            similarity=similarity,
            category=category,
            pm_side="YES",
            kalshi_side="YES",
            needs_inversion=False,
        )
        self._record_retained(match_info)
        return match_info

    def _record_filter(self, pm: str, ks: str, reason: str) -> None:
        """记录过滤的样本"""
        self.filtered_count += 1
        if self.filtered_count <= 3:
            self.filtered_samples.append((pm, ks, reason))
            print(f"\n         🔍 二筛过滤 #{self.filtered_count} [{reason}]:")
            print(f"            PM: {pm}")
            print(f"            Kalshi: {ks}")

    def _record_retained(self, info: MatchInfo) -> None:
        """记录留存的样本"""
        sample = RetainedSample(
            pm_title=info.pm_title,
            kalshi_title=info.kalshi_title,
            similarity=info.similarity,
            category=info.category,
            pm_side=info.pm_side,
            kalshi_side=info.kalshi_side,
            needs_inversion=info.needs_inversion,
        )

        if info.category not in self.retained_samples:
            self.retained_samples[info.category] = []
        self.retained_samples[info.category].append(sample)

    def reset_filtered_count(self) -> None:
        """重置计数"""
        self.filtered_count = 0
        self.filtered_samples.clear()
        self.retained_samples.clear()

    def print_retained_samples(self) -> None:
        """打印留存样本"""
        print("\n📊 二筛后各类别最高分样本 (每个类别最多3个):")

        categories = sorted(self.retained_samples.keys())

        for category in categories[:5]:
            samples = self.retained_samples[category]
            sorted_samples = sorted(samples, key=lambda x: x.similarity, reverse=True)

            print(f"\n  类别 [{category}]: {len(samples)} 个留存")
            for i, sample in enumerate(sorted_samples[:3]):
                inversion_note = " [Y/N颠倒]" if sample.needs_inversion else ""
                print(f"    {i+1}. 相似度: {sample.similarity:.3f}{inversion_note}")
                print(f"       PM {sample.pm_side}: {sample.pm_title}")
                print(f"       Kalshi {sample.kalshi_side}: {sample.kalshi_title}")
            if len(samples) > 3:
                print(f"       ... 还有 {len(samples) - 3} 个")

        if len(categories) > 5:
            print(f"   ... 以及其他 {len(categories) - 5} 个类别")