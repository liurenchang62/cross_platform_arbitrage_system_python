# event.py
from dataclasses import dataclass, field
from typing import Optional, List, Tuple
from datetime import datetime
import json


@dataclass
class MarketPrices:
    """市场价格数据结构，与Rust版本完全对应"""
    yes: float
    no: float
    liquidity: float
    yes_ask: Optional[float] = None
    no_ask: Optional[float] = None
    last_price: Optional[float] = None

    def validate(self) -> bool:
        """验证YES+NO是否接近1"""
        return abs(self.yes + self.no - 1.0) < 0.01

    def yes_ask_or_fallback(self) -> float:
        """获取YES卖价，如果没有则用yes价格"""
        return self.yes_ask if self.yes_ask is not None else self.yes

    def no_ask_or_fallback(self) -> float:
        """获取NO卖价，如果没有则用no价格"""
        return self.no_ask if self.no_ask is not None else self.no

    @classmethod
    def new(cls, yes: float, no: float, liquidity: float) -> 'MarketPrices':
        """创建新的市场价格实例"""
        return cls(yes=yes, no=no, liquidity=liquidity)

    def with_asks(self, yes_ask: float, no_ask: float, last_price: Optional[float]) -> 'MarketPrices':
        """添加买卖价信息"""
        self.yes_ask = yes_ask
        self.no_ask = no_ask
        self.last_price = last_price
        return self


@dataclass
class Event:
    """统一事件数据结构，与Rust版本完全对应"""
    platform: str
    event_id: str
    title: str
    description: str
    resolution_date: Optional[datetime] = None
    category: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    slug: Optional[str] = None
    token_ids: List[str] = field(default_factory=list)
    outcome_prices: Optional[Tuple[float, float]] = None
    best_ask: Optional[float] = None
    best_bid: Optional[float] = None
    last_trade_price: Optional[float] = None
    vector_cache: Optional[List[float]] = None
    categories: List[str] = field(default_factory=list)

    @classmethod
    def new(cls, platform: str, event_id: str, title: str, description: str) -> 'Event':
        """创建新事件"""
        return cls(
            platform=platform,
            event_id=event_id,
            title=title,
            description=description
        )

    def with_resolution_date(self, date: datetime) -> 'Event':
        """设置解析日期"""
        self.resolution_date = date
        return self

    def with_category(self, category: str) -> 'Event':
        """设置类别"""
        self.category = category
        return self

    def with_tags(self, tags: List[str]) -> 'Event':
        """设置标签"""
        self.tags = tags
        return self

    def with_slug(self, slug: str) -> 'Event':
        """设置slug"""
        self.slug = slug
        return self

    def with_token_ids(self, token_ids: List[str]) -> 'Event':
        """设置token IDs"""
        self.token_ids = token_ids
        return self

    def with_outcome_prices(self, yes: float, no: float) -> 'Event':
        """设置结果价格"""
        self.outcome_prices = (yes, no)
        return self

    def with_market_data(self, best_ask: float, best_bid: float, last_trade: Optional[float]) -> 'Event':
        """设置市场数据"""
        self.best_ask = best_ask
        self.best_bid = best_bid
        self.last_trade_price = last_trade
        return self

    def with_vector_cache(self, vector: List[float]) -> 'Event':
        """设置向量缓存"""
        self.vector_cache = vector
        return self

    def slug_is_15m_crypto(self) -> bool:
        """检查是否为15分钟加密货币市场"""
        if self.slug:
            return "updown-15m" in self.slug
        return False

    @staticmethod
    def ticker_looks_15m_crypto(ticker: str) -> bool:
        """检查ticker是否像15分钟加密货币"""
        lower = ticker.lower()
        has_15m = "15m" in lower
        has_coin = any(x in lower for x in ["btc", "eth", "sol", "bitcoin", "ethereum", "solana"])
        return has_15m and has_coin

    def is_15m_crypto_market(self) -> bool:
        """判断是否为15分钟加密货币市场"""
        if self.slug_is_15m_crypto():
            return True
        ticker = self.slug if self.slug else self.event_id
        return self.platform == "kalshi" and self.ticker_looks_15m_crypto(ticker)

    def coin_from_slug(self) -> Optional[str]:
        """从slug中提取币种"""
        if self.slug:
            if "updown-15m" in self.slug:
                prefix = self.slug.split("-updown-15m")[0]
                if prefix:
                    return prefix.lower()

        ticker = (self.slug if self.slug else self.event_id).lower()
        if "btc" in ticker or "bitcoin" in ticker:
            return "btc"
        if "eth" in ticker or "ethereum" in ticker:
            return "eth"
        if "sol" in ticker or "solana" in ticker:
            return "sol"
        return None