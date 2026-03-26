# clients.py
import requests
import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
from market import Market, MarketPrices
from system_params import *


def _parse_rfc3339_field(market_data: dict, key: str) -> Optional[datetime]:
    v = market_data.get(key)
    if v is None or v == "":
        return None
    s = v.strip() if isinstance(v, str) else str(v).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_polymarket_market_resolution_date(market_data: dict) -> Optional[datetime]:
    for key in ("endDate", "end_date"):
        s = market_data.get(key)
        if s and isinstance(s, str) and s.strip():
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except ValueError:
                continue
    s = market_data.get("endDateIso")
    if s and isinstance(s, str) and s.strip():
        try:
            d = datetime.strptime(s.strip(), "%Y-%m-%d").date()
            return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def parse_kalshi_market_resolution_date(market_data: dict) -> Optional[datetime]:
    for key in ("expected_expiration_time", "expiration_time", "close_time", "latest_expiration_time"):
        dt = _parse_rfc3339_field(market_data, key)
        if dt is not None:
            return dt
    return None


def parse_polymarket_gamma_market_row(
    market_data: dict,
    category: Optional[str],
    tags: Optional[List[str]],
) -> Optional[Market]:
    """Gamma 单条 market JSON → Market（与 Rust `parse_polymarket_gamma_market_row` 对齐）。"""
    is_closed = market_data.get("closed", True)
    if not isinstance(is_closed, bool):
        is_closed = bool(is_closed)
    is_resolved = market_data.get("umaResolutionStatus") == "resolved"
    if is_closed or is_resolved:
        return None

    market_id = str(market_data.get("id") or "").strip()
    if not market_id:
        return None
    question = str(market_data.get("question") or "")

    yes_price = 0.0
    no_price = 0.0
    prices_str = market_data.get("outcomePrices")
    if isinstance(prices_str, str):
        try:
            prices = json.loads(prices_str)
            if isinstance(prices, list) and len(prices) >= 2:
                yes_price = float(str(prices[0]).strip())
                no_price = float(str(prices[1]).strip())
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    elif isinstance(prices_str, list) and len(prices_str) >= 2:
        try:
            yes_price = float(str(prices_str[0]).strip())
            no_price = float(str(prices_str[1]).strip())
        except (TypeError, ValueError):
            pass

    best_ask = market_data.get("bestAsk")
    if best_ask is not None:
        best_ask = float(best_ask)
    best_bid = market_data.get("bestBid")
    if best_bid is not None:
        best_bid = float(best_bid)
    ltp = market_data.get("lastTradePrice")
    if ltp is not None:
        ltp = float(ltp)

    vol = market_data.get("volume24hr")
    if vol is None:
        volume_24h = 0.0
    else:
        try:
            volume_24h = float(vol)
        except (TypeError, ValueError):
            volume_24h = 0.0

    token_ids: List[str] = []
    tid = market_data.get("clobTokenIds")
    if isinstance(tid, str):
        try:
            parsed = json.loads(tid)
            if isinstance(parsed, list):
                token_ids = parsed
        except json.JSONDecodeError:
            pass

    resolution_date = parse_polymarket_market_resolution_date(market_data)
    tag_list = list(tags) if tags else []

    return Market(
        platform="polymarket",
        market_id=market_id,
        title=question,
        description=str(market_data.get("description") or ""),
        resolution_date=resolution_date,
        category=category,
        tags=tag_list,
        slug=market_data.get("slug"),
        token_ids=token_ids,
        outcome_prices=(yes_price, no_price),
        best_ask=best_ask,
        best_bid=best_bid,
        last_trade_price=ltp,
        vector_cache=None,
        categories=[],
        volume_24h=volume_24h,
    )


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

    async def clear(self) -> None:
        self.entries.clear()


class PolymarketClient:
    """Polymarket API客户端"""

    GAMMA_API_BASE = "https://gamma-api.polymarket.com"

    def __init__(self):
        self.http_client = None  # 使用requests同步，但通过run_in_executor包装
        self.base_url = "https://gamma-api.polymarket.com"
        self.price_cache = PriceCache(60)

    async def request_with_retry(self, func, *args, **kwargs):
        """带重试的请求"""
        retries = 0
        while True:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                retries += 1
                if retries >= MAX_RETRIES:
                    raise e
                delay = RETRY_INITIAL_DELAY_MS * (1 << (retries - 1)) / 1000.0
                print(f"⚠️ 请求失败，{delay}秒后重试 ({retries}/{MAX_RETRIES}): {e}")
                await asyncio.sleep(delay)

    async def _request(self, method: str, url: str, **kwargs) -> Any:
        """统一请求处理"""
        loop = asyncio.get_event_loop()

        def sync_request():
            try:
                timeout = kwargs.pop('timeout', 10)
                if method.upper() == "GET":
                    response = requests.get(url, timeout=timeout, **kwargs)
                elif method.upper() == "POST":
                    response = requests.post(url, timeout=timeout, **kwargs)
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

    async def fetch_all_markets(self) -> List[Market]:
        """获取所有Polymarket市场"""
        tag_slug = os.environ.get("POLYMARKET_TAG_SLUG")
        tag_slug = tag_slug if tag_slug and tag_slug.strip() else None

        all_markets = []
        offset = 0
        limit = POLYMARKET_PAGE_LIMIT

        print(f"   📡 获取 Polymarket 所有市场 (上限 {POLYMARKET_MAX_MARKETS} 个)...")

        while len(all_markets) < POLYMARKET_MAX_MARKETS:
            try:
                markets = await self.request_with_retry(
                    self.fetch_markets_page, tag_slug, offset
                )

                if not markets:
                    print("      无更多市场，获取完成")
                    break

                print(f"      偏移 {offset}: {len(markets)} 个市场, 累计 {len(all_markets)} 个")

                all_markets.extend(markets)
                offset += limit

                await asyncio.sleep(REQUEST_INTERVAL_MS / 1000.0)

            except Exception as e:
                print(f"      获取失败: {e}")
                break

        if len(all_markets) >= POLYMARKET_MAX_MARKETS:
            print(f"      达到获取上限 {POLYMARKET_MAX_MARKETS} 个，停止获取")
            all_markets = all_markets[:POLYMARKET_MAX_MARKETS]

        print(f"   ✅ 获取到 {len(all_markets)} 个 Polymarket 市场")
        return all_markets

    async def fetch_markets_page(self, tag_slug: Optional[str], offset: int) -> List[Market]:
        """获取单页Polymarket市场"""
        limit = POLYMARKET_PAGE_LIMIT

        params = {
            "active": "true",
            "closed": "false",
            "limit": str(limit),
            "offset": str(offset),
            "order": "volume24hr",
            "ascending": "false",
        }

        if tag_slug:
            params["tag_slug"] = tag_slug

        url = f"{self.GAMMA_API_BASE}/events"
        data = await self._request("GET", url, params=params)

        markets = []

        for event_data in data:
            # 处理tags
            tags = []
            if "tags" in event_data and isinstance(event_data["tags"], list):
                for t in event_data["tags"]:
                    tag = t.get("slug") or t.get("label")
                    if tag:
                        tags.append(tag)

            raw_cat = event_data.get("category")
            category = raw_cat if isinstance(raw_cat, str) else None

            if "markets" in event_data and isinstance(event_data["markets"], list):
                for market_data in event_data["markets"]:
                    if not isinstance(market_data, dict):
                        continue
                    m = parse_polymarket_gamma_market_row(market_data, category, tags)
                    if m is not None:
                        markets.append(m)

        return markets

    async def fetch_prices(self, market: Market) -> MarketPrices:
        """获取市场价格"""
        cached = await self.price_cache.get(market.market_id)
        if cached:
            return cached

        if market.outcome_prices:
            yes_price, no_price = market.outcome_prices
        else:
            if market.best_ask is not None and market.best_bid is not None:
                mid = (market.best_ask + market.best_bid) / 2.0
                yes_price = mid
                no_price = 1.0 - mid
            elif market.best_ask is not None:
                yes_price = market.best_ask
                no_price = 1.0 - market.best_ask
            elif market.best_bid is not None:
                yes_price = market.best_bid
                no_price = 1.0 - market.best_bid
            elif market.last_trade_price is not None:
                yes_price = market.last_trade_price
                no_price = 1.0 - market.last_trade_price
            else:
                raise Exception(f"No price data available for market {market.market_id}")

        prices = MarketPrices.new(yes_price, no_price, 0.0).with_asks(
            market.best_ask if market.best_ask is not None else yes_price,
            (1.0 - market.best_bid) if market.best_bid is not None else no_price,
            market.last_trade_price,
        )

        await self.price_cache.set(market.market_id, prices)
        return prices

    async def get_order_book(self, token_id: str) -> Optional[dict]:
        """获取Polymarket订单簿"""
        url = "https://clob.polymarket.com/book"
        params = {"token_id": token_id}

        try:
            return await self._request("GET", url, params=params)
        except Exception:
            return None

    async def clear_price_cache(self) -> None:
        await self.price_cache.clear()

    async def fetch_market_snapshot_by_id(self, market_id: str) -> Market:
        if not str(market_id).strip():
            raise ValueError("empty polymarket market_id")
        url = f"{self.GAMMA_API_BASE}/markets"
        arr = await self._request("GET", url, params={"id": market_id, "limit": "1"})
        if not arr or not isinstance(arr, list) or not arr:
            raise ValueError(f"Polymarket market not found: {market_id}")
        m = parse_polymarket_gamma_market_row(arr[0], None, [])
        if m is None:
            raise ValueError(f"Polymarket market closed or unparsable: {market_id}")
        return m

    async def fetch_resolution_by_market_id(self, market_id: str) -> Optional[datetime]:
        if not str(market_id).strip():
            return None
        url = f"{self.GAMMA_API_BASE}/markets"
        try:
            arr = await self._request("GET", url, params={"id": market_id, "limit": "1"})
            if not arr or not isinstance(arr, list) or not arr:
                return None
            return parse_polymarket_market_resolution_date(arr[0])
        except Exception:
            return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class KalshiClient:
    """Kalshi API客户端"""

    KALSHI_DEFAULT_BASE = "https://api.elections.kalshi.com/trade-api/v2"

    def __init__(self, base_url: Optional[str] = None):
        u = (base_url or "").strip()
        self.base_url = u if u else self.KALSHI_DEFAULT_BASE
        self.price_cache = PriceCache(60)

    async def request_with_retry(self, func, *args, **kwargs):
        """带重试的请求"""
        retries = 0
        while True:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                retries += 1
                if retries >= MAX_RETRIES:
                    raise e
                delay = RETRY_INITIAL_DELAY_MS * (1 << (retries - 1)) / 1000.0
                print(f"⚠️ 请求失败，{delay}秒后重试 ({retries}/{MAX_RETRIES}): {e}")
                await asyncio.sleep(delay)

    async def _request(self, method: str, url: str, **kwargs) -> Any:
        """统一请求处理"""
        loop = asyncio.get_event_loop()

        def sync_request():
            try:
                silent = bool(kwargs.pop("_silent", False))
                timeout = kwargs.pop('timeout', 10)
                if method.upper() == "GET":
                    response = requests.get(url, timeout=timeout, **kwargs)
                elif method.upper() == "POST":
                    response = requests.post(url, timeout=timeout, **kwargs)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                if response.status_code != 200:
                    if not silent:
                        print(f"请求失败: {response.status_code}")
                        print(f"响应内容: {response.text[:200]}")
                    response.raise_for_status()

                return response.json()
            except requests.exceptions.Timeout:
                if not silent:
                    print("请求超时")
                raise
            except requests.exceptions.ConnectionError as e:
                if not silent:
                    print(f"连接错误: {e}")
                raise
            except Exception as e:
                if not silent:
                    print(f"请求异常: {e}")
                raise

        return await loop.run_in_executor(None, sync_request)

    async def fetch_all_markets(self) -> List[Market]:
        """获取所有Kalshi市场"""
        all_markets = []
        cursor = ""
        limit = KALSHI_PAGE_LIMIT

        print(f"   📡 获取 Kalshi 所有市场 (上限 {KALSHI_MAX_MARKETS} 个)...")

        # 第一步：先获取所有系列信息，用于 category 映射
        series_category_map = {}

        try:
            events_data = await self.fetch_series_info()
            for event in events_data.get("events", []):
                series = event.get("series_ticker")
                if series:
                    category = event.get("category")
                    series_category_map[series] = category
        except Exception:
            pass

        # 第二步：分页获取所有市场
        page_count = 0

        while len(all_markets) < KALSHI_MAX_MARKETS:
            page_count += 1

            try:
                if not cursor:
                    markets, next_cursor = await self.fetch_markets_page("", limit)
                else:
                    markets, next_cursor = await self.request_with_retry(
                        self.fetch_markets_page, cursor, limit
                    )

                if not markets:
                    break

                print(f"      第 {page_count} 页: {len(markets)} 个市场, 累计 {len(all_markets)} 个")

                # 处理 markets
                for market_data in markets:
                    if len(all_markets) >= KALSHI_MAX_MARKETS:
                        break

                    # 过滤活跃市场
                    is_active = market_data.get("status") == "active"
                    is_settled = market_data.get("result", "") != ""
                    if not is_active or is_settled:
                        continue

                    # 提取候选人名称
                    candidate_name = market_data.get("yes_sub_title", "")
                    if candidate_name is None:
                        candidate_name = ""

                    # 提取价格
                    yes_ask_cents = market_data.get("yes_ask", 0)
                    if yes_ask_cents is None:
                        yes_ask_cents = 0

                    yes_bid_cents = market_data.get("yes_bid", 0)
                    if yes_bid_cents is None:
                        yes_bid_cents = 0

                    last_price_cents = market_data.get("last_price")

                    # 提取24小时成交量
                    volume_24h_str = market_data.get("volume_24h_fp", "0")
                    try:
                        volume_24h = float(volume_24h_str)
                    except:
                        volume_24h = 0.0

                    # 构建标题
                    title = market_data.get("title", "")
                    if candidate_name:
                        title = f"{title} - {candidate_name}"

                    market_ticker = market_data.get("ticker", "")
                    event_ticker = market_data.get("event_ticker", "")

                    # 解析到期日
                    resolution_date = None
                    exp_time = market_data.get("expiration_time")
                    if exp_time:
                        try:
                            resolution_date = datetime.fromisoformat(exp_time.replace('Z', '+00:00'))
                        except:
                            pass

                    # 获取系列对应的 category
                    series_prefix = event_ticker.split('-')[0] if event_ticker else ""
                    category = series_category_map.get(series_prefix)

                    market = Market(
                        platform="kalshi",
                        market_id=market_ticker,
                        title=title,
                        description=market_data.get("subtitle", ""),
                        resolution_date=resolution_date,
                        category=category,
                        tags=[],
                        slug=None,
                        token_ids=[],
                        outcome_prices=None,
                        best_ask=float(yes_ask_cents) / 100.0 if yes_ask_cents else None,
                        best_bid=float(yes_bid_cents) / 100.0 if yes_bid_cents else None,
                        last_trade_price=float(last_price_cents) / 100.0 if last_price_cents else None,
                        vector_cache=None,
                        categories=[],
                        volume_24h=volume_24h,
                    )

                    all_markets.append(market)

                # 更新 cursor
                cursor = next_cursor

                # 请求间隔
                await asyncio.sleep(REQUEST_INTERVAL_MS / 1000.0)

                if not cursor:
                    break

            except Exception as e:
                print(f"      获取失败: {e}")
                break

        if len(all_markets) >= KALSHI_MAX_MARKETS:
            print(f"      达到获取上限 {KALSHI_MAX_MARKETS} 个，停止获取")
            all_markets = all_markets[:KALSHI_MAX_MARKETS]

        print(f"   ✅ 获取到 {len(all_markets)} 个 Kalshi 市场")
        return all_markets

    async def fetch_series_info(self) -> dict:
        """获取系列信息"""
        url = f"{self.base_url}/events"
        params = {
            "status": "open",
            "limit": "1000"
        }
        try:
            return await self._request("GET", url, params=params, _silent=True)
        except Exception:
            # 获取系列信息失败不应打断主流程，也不输出控制台错误噪音
            return {"events": []}

    async def fetch_markets_page(self, cursor: str, limit: int) -> Tuple[List[dict], str]:
        """获取单页市场"""
        url = f"{self.base_url}/markets"

        params = {
            "status": "open",
            "limit": str(limit),
            "mve_filter": "exclude",
        }

        if cursor:
            params["cursor"] = cursor

        data = await self._request("GET", url, params=params)
        markets = data.get("markets", [])
        next_cursor = data.get("cursor", "")
        return markets, next_cursor

    async def get_market_prices(self, ticker: str) -> Optional[MarketPrices]:
        """获取市场价格"""
        cached = await self.price_cache.get(ticker)
        if cached:
            return cached

        url = f"{self.base_url}/markets/{ticker}"

        try:
            data = await self._request("GET", url)
        except:
            return None

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

        # 处理 volume
        volume = market.get("volume_24h_fp", 0.0)
        if volume is None:
            volume = 0.0

        # 对于二元市场，YES和NO的价格之和应该接近1
        yes_price = (yes_ask_dollars + yes_bid_dollars) / 2.0
        no_price = 1.0 - yes_price

        prices = MarketPrices.new(yes_price, no_price, float(volume)).with_asks(
            yes_ask_dollars,
            1.0 - yes_bid_dollars,
            float(last_price_cents) / 100.0 if last_price_cents else None
        )

        await self.price_cache.set(ticker, prices)
        return prices

    async def get_order_book(self, ticker: str) -> Optional[dict]:
        """获取Kalshi订单簿"""
        url = f"{self.base_url}/markets/{ticker}/orderbook"

        try:
            return await self._request("GET", url)
        except Exception:
            return None

    async def clear_price_cache(self) -> None:
        await self.price_cache.clear()

    async def fetch_resolution_by_ticker(self, ticker: str) -> Optional[datetime]:
        if not str(ticker).strip():
            return None
        url = f"{self.base_url}/markets/{ticker}"
        try:
            data = await self._request("GET", url)
            m = data.get("market")
            if not m:
                return None
            return parse_kalshi_market_resolution_date(m)
        except Exception:
            return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass