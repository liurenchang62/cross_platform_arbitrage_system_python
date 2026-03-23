# tracking.py
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from dataclasses import dataclass

from market import Market, MarketPrices
from market_filter import tracked_pair_exceeds_horizon


@dataclass
class TrackedArbitrage:
    """追踪的套利对"""
    pair_id: str
    pm_market: Market
    kalshi_market: Market
    similarity: float
    pm_side: str
    kalshi_side: str
    needs_inversion: bool
    last_pm_price: Optional[MarketPrices] = None
    last_kalshi_price: Optional[MarketPrices] = None
    best_profit: float = 0.0
    last_check: datetime = None
    active: bool = True

    def __post_init__(self):
        if self.last_check is None:
            self.last_check = datetime.now(timezone.utc)

    @classmethod
    def new(
        cls,
        pm_market: Market,
        kalshi_market: Market,
        similarity: float,
        pm_side: str,
        kalshi_side: str,
        needs_inversion: bool,
    ) -> 'TrackedArbitrage':
        """创建新的追踪套利对"""
        return cls(
            pair_id=f"{pm_market.market_id}:{kalshi_market.market_id}",
            pm_market=pm_market,
            kalshi_market=kalshi_market,
            similarity=similarity,
            pm_side=pm_side,
            kalshi_side=kalshi_side,
            needs_inversion=needs_inversion,
        )


class MonitorState:
    """监控状态"""

    def __init__(self, full_match_interval: int, market_limit: int):
        self.tracked_pairs: List[TrackedArbitrage] = []
        self.current_cycle = 0
        self.full_match_interval = full_match_interval
        self.market_limit = market_limit

    def should_full_match(self) -> bool:
        """判断是否应该执行全量匹配"""
        return self.current_cycle % self.full_match_interval == 0

    def next_cycle(self) -> None:
        """进入下一周期"""
        self.current_cycle += 1

    def update_tracked_pairs(
        self,
        new_matches: List[Tuple[Market, Market, float, str, str, bool]]
    ) -> None:
        """更新追踪列表"""
        # 标记旧的为不活跃
        for pair in self.tracked_pairs:
            pair.active = False

        # 添加新的匹配对
        for pm, kalshi, similarity, pm_side, kalshi_side, needs_inversion in new_matches:
            pair_id = f"{pm.market_id}:{kalshi.market_id}"
            existing = next((p for p in self.tracked_pairs if p.pair_id == pair_id), None)

            if existing:
                existing.active = True
                existing.similarity = similarity
                existing.pm_side = pm_side
                existing.kalshi_side = kalshi_side
                existing.needs_inversion = needs_inversion
            else:
                self.tracked_pairs.append(TrackedArbitrage.new(
                    pm, kalshi, similarity, pm_side, kalshi_side, needs_inversion
                ))

        # 清理不活跃的
        self.tracked_pairs = [p for p in self.tracked_pairs if p.active]

    def get_active_pairs(self) -> List[TrackedArbitrage]:
        """获取活跃的追踪对"""
        return [p for p in self.tracked_pairs if p.active]

    def prune_tracked_beyond_resolution_horizon(self, now: datetime) -> None:
        """剔除任一侧「有解析日且晚于 horizon」的追踪对（与全量池筛选规则一致）。"""
        self.tracked_pairs = [
            p
            for p in self.tracked_pairs
            if not tracked_pair_exceeds_horizon(p.pm_market, p.kalshi_market, now)
        ]