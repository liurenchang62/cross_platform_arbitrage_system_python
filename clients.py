# clients.py
import requests
import asyncio
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from event import Event, MarketPrices


class PriceCacheEntry:
    """价格缓存条目"""

    def __init__(self, prices: MarketPrices, timestamp: datetime):
        self.prices = prices
        self.timestamp = timestamp


class PriceCache:
    """价格缓存"""

    def __init__(self, ttl_seconds: int = 60):
        self.entries: Dict[str, PriceCacheEntry] = {}
        self.ttl = timedelta(seconds=ttl_seconds)

    async def get(self, key: str) -> Optional[MarketPrices]:
        if key in self.entries:
            entry = self.entries[key]
            if datetime.now() - entry.timestamp < self.ttl:
                return entry.prices
        return None

    async def set(self, key: str, prices: MarketPrices):
        self.entries[key] = PriceCacheEntry(prices, datetime.now())


class PolymarketClient:
    """Polymarket API客户端"""

    GAMMA_API_BASE = "https://gamma-api.polymarket.com"



    def __init__(self):
        self.base_url = "https://gamma-api.polymarket.com"
        self.price_cache = PriceCache(60)
        self.session = None

    async def get_order_book(self, token_id: str) -> Optional[dict]:
        """获取Polymarket订单簿"""
        url = "https://clob.polymarket.com/book"
        params = {"token_id": token_id}

        try:
            data = await self._request("GET", url, params=params)
            return data
        except Exception as e:
            print(f"获取Polymarket订单簿失败: {e}")
            return None

    async def _request(self, method: str, url: str, **kwargs) -> Dict:
        """统一请求处理"""
        loop = asyncio.get_event_loop()

        def sync_request():
            try:
                if method.upper() == "GET":
                    response = requests.get(url, **kwargs)
                elif method.upper() == "POST":
                    response = requests.post(url, **kwargs)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                if response.status_code != 200:
                    print(f"请求失败: {response.status_code}")
                    print(f"响应内容: {response.text[:200]}")
                    response.raise_for_status()

                return response.json()
            except requests.exceptions.Timeout:
                print("请求超时")
                raise
            except requests.exceptions.ConnectionError as e:
                print(f"连接错误: {e}")
                raise
            except Exception as e:
                print(f"请求异常: {e}")
                raise

        return await loop.run_in_executor(None, sync_request)

    async def close(self):
        """关闭客户端（requests不需要关闭，保留用于兼容）"""
        pass

    async def fetch_events(self) -> List[Event]:
        """获取所有事件"""
        import os
        tag_slug = os.environ.get("POLYMARKET_TAG_SLUG")
        if tag_slug and tag_slug.strip():
            return await self.fetch_events_from_gamma(tag_slug, 200)
        else:
            return await self.fetch_events_from_gamma(None, 200)

    async def fetch_events_from_gamma(
            self,
            tag_slug: Optional[str],
            limit: int,
    ) -> List[Event]:
        """从Gamma API获取事件"""
        limit = min(limit, 100)

        params = {
            "active": "true",
            "closed": "false",
            "limit": str(limit)
        }

        if tag_slug and tag_slug.strip():
            params["tag_slug"] = tag_slug

        url = f"{self.GAMMA_API_BASE}/events"

        try:
            data = await self._request("GET", url, params=params, timeout=30)
        except Exception as e:
            print(f"获取Polymarket事件失败: {e}")
            return []

        events = []

        for event_data in data:
            # 处理tags
            tags = []
            if "tags" in event_data and isinstance(event_data["tags"], list):
                for t in event_data["tags"]:
                    tag = t.get("slug") or t.get("label")
                    if tag:
                        tags.append(tag)

            category = event_data.get("category")

            # 获取该事件下的所有市场
            if "markets" in event_data and isinstance(event_data["markets"], list):
                for market in event_data["markets"]:
                    # 使用字段进行筛选：未关闭且未结算
                    is_closed = market.get("closed", True)
                    is_resolved = market.get("umaResolutionStatus") == "resolved"

                    if is_closed or is_resolved:
                        continue

                    event_id = market.get("id", "")
                    question = market.get("question", "")

                    # 解析 outcomePrices
                    yes_price = 0.0
                    no_price = 0.0
                    prices_str = market.get("outcomePrices")
                    if prices_str and isinstance(prices_str, str):
                        try:
                            prices = json.loads(prices_str)
                            if len(prices) >= 2:
                                yes_price = float(prices[0])
                                no_price = float(prices[1])
                        except:
                            pass

                    # 获取其他价格字段
                    best_ask = market.get("bestAsk")
                    if best_ask is not None:
                        best_ask = float(best_ask)

                    best_bid = market.get("bestBid")
                    if best_bid is not None:
                        best_bid = float(best_bid)

                    last_trade_price = market.get("lastTradePrice")
                    if last_trade_price is not None:
                        last_trade_price = float(last_trade_price)

                    # 成交量
                    volume = 0.0
                    volume_str = market.get("volume")
                    if volume_str and isinstance(volume_str, str):
                        try:
                            volume = float(volume_str)
                        except:
                            pass
                    else:
                        volume = market.get("volumeNum", 0.0)

                    # 解析 token_ids
                    token_ids = []
                    token_ids_str = market.get("clobTokenIds")
                    if token_ids_str and isinstance(token_ids_str, str):
                        try:
                            token_ids = json.loads(token_ids_str)
                        except:
                            pass

                    # 解析到期日
                    resolution_date = None
                    end_date = market.get("endDate")
                    if end_date:
                        try:
                            resolution_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                        except:
                            pass

                    event = Event(
                        platform="polymarket",
                        event_id=event_id,
                        title=question,
                        description=market.get("description", ""),
                        resolution_date=resolution_date,
                        category=category,
                        tags=tags.copy(),
                        slug=market.get("slug"),
                        token_ids=token_ids,
                        outcome_prices=(yes_price, no_price),
                        best_ask=best_ask,
                        best_bid=best_bid,
                        last_trade_price=last_trade_price,
                        vector_cache=None,
                        categories=[]
                    )

                    events.append(event)

        return events

    async def fetch_prices(self, event: Event) -> MarketPrices:
        """获取事件价格"""
        # 先查缓存
        cached = await self.price_cache.get(event.event_id)
        if cached:
            return cached

        # 直接从event中获取价格数据
        if event.outcome_prices:
            yes_price, no_price = event.outcome_prices
        else:
            # 如果没有outcomePrices，尝试用bestAsk/bestBid估算
            if event.best_ask is not None and event.best_bid is not None:
                mid = (event.best_ask + event.best_bid) / 2.0
                yes_price = mid
                no_price = 1.0 - mid
            elif event.best_ask is not None:
                yes_price = event.best_ask
                no_price = 1.0 - event.best_ask
            elif event.best_bid is not None:
                yes_price = event.best_bid
                no_price = 1.0 - event.best_bid
            elif event.last_trade_price is not None:
                yes_price = event.last_trade_price
                no_price = 1.0 - event.last_trade_price
            else:
                raise Exception(f"No price data available for event {event.event_id}")

        liquidity = 0.0

        prices = MarketPrices.new(yes_price, no_price, liquidity).with_asks(
            event.best_ask if event.best_ask is not None else yes_price,
            1.0 - event.best_bid if event.best_bid is not None else no_price,
            event.last_trade_price
        )

        await self.price_cache.set(event.event_id, prices)
        return prices

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class KalshiClient:
    """Kalshi API客户端"""

    KALSHI_DEFAULT_BASE = "https://api.elections.kalshi.com/trade-api/v2"

    def __init__(self):
        self.base_url = self.KALSHI_DEFAULT_BASE
        self.price_cache = PriceCache(60)

    async def get_order_book(self, ticker: str) -> Optional[dict]:
        """获取Kalshi订单簿"""
        url = f"{self.base_url}/markets/{ticker}/orderbook"

        try:
            data = await self._request("GET", url)
            return data
        except Exception as e:
            print(f"获取Kalshi订单簿失败: {e}")
            return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _request(self, method: str, url: str, **kwargs) -> Dict:
        """统一请求处理"""
        loop = asyncio.get_event_loop()

        def sync_request():
            try:
                if method.upper() == "GET":
                    response = requests.get(url, **kwargs)
                elif method.upper() == "POST":
                    response = requests.post(url, **kwargs)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                if response.status_code != 200:
                    print(f"请求失败: {response.status_code}")
                    print(f"响应内容: {response.text[:200]}")
                    response.raise_for_status()

                return response.json()
            except requests.exceptions.Timeout:
                print("请求超时")
                raise
            except requests.exceptions.ConnectionError as e:
                print(f"连接错误: {e}")
                raise
            except Exception as e:
                print(f"请求异常: {e}")
                raise

        return await loop.run_in_executor(None, sync_request)

    async def close(self):
        """关闭客户端"""
        pass

    async def fetch_markets_by_series(self, series_ticker: str) -> List[Dict]:
        """获取系列下的所有市场"""
        url = f"{self.base_url}/markets"
        params = {
            "series_ticker": series_ticker,
            "status": "open",
            "limit": "200"
        }

        data = await self._request("GET", url, params=params)

        # 使用字段进行筛选：只保留活跃且未结算的市场
        markets = []
        for market in data.get("markets", []):
            is_active = market.get("status") == "active"
            is_settled = market.get("result", "") != ""
            if is_active and not is_settled:
                markets.append(market)

        return markets

    async def fetch_events(self) -> List[Event]:
        """获取所有Kalshi事件（串行版本）"""
        from datetime import timezone

        # 第一步：获取所有事件系列
        url = f"{self.base_url}/events"
        params = {
            "status": "open",
            "limit": "200"
        }

        data = await self._request("GET", url, params=params)

        # 提取所有 series_ticker 及其对应的 category
        series_info = []  # (series_ticker, category)

        for event_data in data.get("events", []):
            series = event_data.get("series_ticker")
            if series:
                category = event_data.get("category")
                series_info.append((series, category))

        # 第二步：串行为每个 series_ticker 获取市场
        all_events = []
        market_count = 0

        for series, category in series_info:
            try:
                markets = await self.fetch_markets_by_series(series)
            except Exception as e:
                print(f"警告: 获取系列 {series} 的市场失败: {e}")
                continue

            for market in markets:
                market_count += 1

                # 提取候选人名称（如果有）
                candidate_name = market.get("yes_sub_title", "")
                if candidate_name is None:
                    candidate_name = ""

                # 提取价格（美分转美元）
                yes_ask_cents = market.get("yes_ask", 0)
                if yes_ask_cents is None:
                    yes_ask_cents = 0

                yes_bid_cents = market.get("yes_bid", 0)
                if yes_bid_cents is None:
                    yes_bid_cents = 0

                last_price_cents = market.get("last_price")

                # 构建标题：如果存在候选人，附加到标题后
                title = market.get("title", "")
                if candidate_name:
                    title = f"{title} - {candidate_name}"

                # 获取市场的 ticker（具体市场的 ID）
                market_ticker = market.get("ticker", "")

                # 解析到期日
                resolution_date = None
                exp_time = market.get("expiration_time")
                if exp_time:
                    try:
                        resolution_date = datetime.fromisoformat(exp_time.replace('Z', '+00:00'))
                    except:
                        pass

                # 构建 Event - 使用市场的 ticker 作为 event_id
                event = Event(
                    platform="kalshi",
                    event_id=market_ticker,
                    title=title,
                    description=market.get("subtitle", ""),
                    resolution_date=resolution_date,
                    category=category,
                    tags=[],
                    slug=market_ticker,
                    token_ids=[],
                    outcome_prices=None,
                    best_ask=float(yes_ask_cents) / 100.0 if yes_ask_cents else None,
                    best_bid=float(yes_bid_cents) / 100.0 if yes_bid_cents else None,
                    last_trade_price=float(last_price_cents) / 100.0 if last_price_cents else None,
                    vector_cache=None,
                    categories=[]
                )



                all_events.append(event)

        print(f"   ✅ 获取到 {len(all_events)} 个Kalshi具体市场")
        return all_events

    async def fetch_prices(self, event_id: str) -> MarketPrices:
        """获取事件价格（通过事件ID）"""
        cached = await self.price_cache.get(event_id)
        if cached:
            return cached

        path = f"/events/{event_id}/markets"
        url = f"{self.base_url}{path}"

        data = await self._request("GET", url)

        yes_price = 0.0
        no_price = 0.0
        liquidity = 0.0

        for market in data.get("markets", []):
            subtitle = market.get("subtitle", "")
            last_price = market.get("last_price", 0)
            if last_price:
                last_price = float(last_price) / 100.0

            if subtitle == "Yes":
                yes_price = last_price
            elif subtitle == "No":
                no_price = last_price

            vol = market.get("volume")
            if vol:
                liquidity += float(vol)

        prices = MarketPrices.new(yes_price, no_price, liquidity)
        await self.price_cache.set(event_id, prices)
        return prices

    async def get_market(self, ticker: str) -> Optional[Dict]:
        """获取单个市场信息"""
        url = f"{self.base_url}/markets/{ticker}"
        try:
            data = await self._request("GET", url)
            return data
        except:
            return None

    async def get_market_prices(self, ticker: str) -> Optional[MarketPrices]:
        """获取市场价格（通过市场ticker）"""
        cached = await self.price_cache.get(ticker)
        if cached:
            return cached

        # 使用 /markets/{ticker} 接口
        url = f"{self.base_url}/markets/{ticker}"

        try:
            data = await self._request("GET", url)
        except:
            return None

        # 提取市场数据
        market = data.get("market")
        if not market:
            return None

        # 从字符串解析美元价格
        yes_ask_dollars_str = market.get("yes_ask_dollars", "0")
        yes_bid_dollars_str = market.get("yes_bid_dollars", "0")

        try:
            yes_ask_dollars = float(yes_ask_dollars_str)
        except:
            yes_ask_dollars = 0.0

        try:
            yes_bid_dollars = float(yes_bid_dollars_str)
        except:
            yes_bid_dollars = 0.0

        # 处理 last_price
        last_price_cents = market.get("last_price")
        if last_price_cents is None:
            last_price_dollars_str = market.get("last_price_dollars", "0")
            try:
                last_price_dollars = float(last_price_dollars_str)
                last_price_cents = int(last_price_dollars * 100)
            except:
                last_price_cents = None

        # 处理 volume_24h
        volume = market.get("volume_24h_fp", 0.0)
        if volume is None:
            volume = 0.0

        # 对于二元市场，YES和NO的价格之和应该接近1
        # 使用买卖价的中间值作为当前价格
        yes_price = (yes_ask_dollars + yes_bid_dollars) / 2.0
        no_price = 1.0 - yes_price

        prices = MarketPrices.new(yes_price, no_price, float(volume)).with_asks(
            yes_ask_dollars,
            1.0 - yes_bid_dollars,
            float(last_price_cents) / 100.0 if last_price_cents else None
        )

        await self.price_cache.set(ticker, prices)
        return prices

    async def get_orderbook(self, ticker: str) -> Optional[Dict]:
        """获取订单簿"""
        url = f"{self.base_url}/markets/{ticker}/orderbook"
        try:
            data = await self._request("GET", url)
            return data
        except:
            return None

    @staticmethod
    def orderbook_to_best_ask(yes_bids: List, no_bids: List) -> Tuple[float, float]:
        """从订单簿计算最佳卖价"""
        best_yes_bid_cents = 0
        if yes_bids and len(yes_bids) > 0:
            last_bid = yes_bids[-1]
            if isinstance(last_bid, list) and len(last_bid) > 0:
                best_yes_bid_cents = float(last_bid[0])

        best_no_bid_cents = 0
        if no_bids and len(no_bids) > 0:
            last_bid = no_bids[-1]
            if isinstance(last_bid, list) and len(last_bid) > 0:
                best_no_bid_cents = float(last_bid[0])

        yes_ask = (100.0 - best_no_bid_cents) / 100.0
        no_ask = (100.0 - best_yes_bid_cents) / 100.0
        return (yes_ask, no_ask)