# arbitrage_detector.py
from dataclasses import dataclass
from typing import Optional, Tuple, List
from market import MarketPrices

# Gas 费配置（固定值，单位 USDT）
GAS_FEE = 0.02  # 每笔交易 $0.02


@dataclass
class ArbitrageOpportunity:
    """套利机会数据结构"""
    strategy: str
    kalshi_action: Tuple[str, str, float]  # (action, side, price)
    polymarket_action: Tuple[str, str, float]  # (action, side, price)
    total_cost: float
    gross_profit: float
    fees: float
    net_profit: float
    roi_percent: float
    gas_fee: float
    final_profit: float
    final_roi_percent: float


@dataclass
class Fees:
    """手续费配置"""
    polymarket: float = 0.01  # 1%
    kalshi: float = 0.01  # 1%


@dataclass
class SlippageInfo:
    """滑点信息"""
    avg_price: float
    slippage_percent: float
    filled: bool
    filled_amount: float
    filled_contracts: float


class ArbitrageDetector:
    """套利检测器"""

    def __init__(self, min_profit_threshold: float = 0.02):
        self.min_profit_threshold = min_profit_threshold
        self.fees = Fees()

    def with_fees(self, polymarket_fee: float, kalshi_fee: float) -> 'ArbitrageDetector':
        """设置手续费"""
        self.fees.polymarket = polymarket_fee
        self.fees.kalshi = kalshi_fee
        return self

    def calculate_arbitrage_with_direction(
        self,
        pm_prices: MarketPrices,
        kalshi_prices: MarketPrices,
        pm_side: str,
        kalshi_side: str,
        needs_inversion: bool,
    ) -> Optional[ArbitrageOpportunity]:
        """根据方向计算套利机会"""
        # 根据方向确定实际买卖
        if pm_side == "YES":
            pm_action = "BUY"
            pm_price = pm_prices.yes_ask if pm_prices.yes_ask is not None else pm_prices.yes
        else:
            pm_action = "BUY"
            pm_price = pm_prices.no_ask if pm_prices.no_ask is not None else pm_prices.no

        if kalshi_side == "YES":
            kalshi_action = "BUY"
            kalshi_price = kalshi_prices.yes_ask if kalshi_prices.yes_ask is not None else kalshi_prices.yes
        else:
            kalshi_action = "BUY"
            kalshi_price = kalshi_prices.no_ask if kalshi_prices.no_ask is not None else kalshi_prices.no

        # 计算成本
        total_cost = pm_price + kalshi_price
        profit = 1.0 - total_cost
        total_fees = self.fees.polymarket + self.fees.kalshi

        if profit <= total_fees + self.min_profit_threshold:
            return None

        net_profit = profit - total_fees
        final_profit = net_profit - GAS_FEE

        if final_profit <= self.min_profit_threshold:
            return None

        roi = (final_profit / total_cost) * 100.0 if total_cost > 0 else 0.0

        # 构建策略描述
        inversion_note = " [Y/N颠倒]" if needs_inversion else ""
        strategy = f"Buy {pm_side} on Polymarket + Buy {kalshi_side} on Kalshi{inversion_note}"

        return ArbitrageOpportunity(
            strategy=strategy,
            kalshi_action=(kalshi_action, kalshi_side, kalshi_price),
            polymarket_action=(pm_action, pm_side, pm_price),
            total_cost=total_cost,
            gross_profit=profit,
            fees=total_fees,
            net_profit=net_profit,
            roi_percent=roi,
            gas_fee=GAS_FEE,
            final_profit=final_profit,
            final_roi_percent=roi,
        )

    def calculate_final_profit(
        self,
        pm_prices: MarketPrices,
        kalshi_prices: MarketPrices,
        pm_slippage: float,
        kalshi_slippage: float,
    ) -> Optional[ArbitrageOpportunity]:
        """考虑滑点后的最终利润计算"""
        opportunity = self.check_arbitrage_optimal(pm_prices, kalshi_prices)
        if opportunity is None:
            return None

        # 根据策略确定方向并应用滑点
        if "Buy Yes on Polymarket" in opportunity.strategy:
            pm_slipped = pm_prices.yes * (1.0 + pm_slippage / 100.0)
        else:
            pm_slipped = pm_prices.no * (1.0 + pm_slippage / 100.0)

        if "Buy Yes on Kalshi" in opportunity.strategy:
            kalshi_slipped = kalshi_prices.yes * (1.0 + kalshi_slippage / 100.0)
        else:
            kalshi_slipped = kalshi_prices.no * (1.0 + kalshi_slippage / 100.0)

        slipped_cost = pm_slipped + kalshi_slipped
        slipped_profit = 1.0 - slipped_cost

        if slipped_profit <= 0.0:
            return None

        total_fees = self.fees.polymarket + self.fees.kalshi
        net_profit = slipped_profit - total_fees
        final_profit = net_profit - GAS_FEE

        if final_profit <= self.min_profit_threshold:
            return None

        roi = (final_profit / slipped_cost) * 100.0 if slipped_cost > 0 else 0.0

        # 复用原有的 action 信息
        return ArbitrageOpportunity(
            strategy=opportunity.strategy,
            kalshi_action=opportunity.kalshi_action,
            polymarket_action=opportunity.polymarket_action,
            total_cost=slipped_cost,
            gross_profit=slipped_profit,
            fees=total_fees,
            net_profit=net_profit,
            roi_percent=(net_profit / slipped_cost) * 100.0,
            gas_fee=GAS_FEE,
            final_profit=final_profit,
            final_roi_percent=roi,
        )

    def check_arbitrage_optimal(
        self,
        pm_prices: MarketPrices,
        kalshi_prices: MarketPrices,
    ) -> Optional[ArbitrageOpportunity]:
        """只用最优价检查潜在机会（快速筛选）"""
        # 验证价格有效性
        if kalshi_prices.yes == 0.0 and kalshi_prices.no == 0.0:
            return None
        if not pm_prices.validate() or not kalshi_prices.validate():
            return None

        # 确保有必要的 ask 数据
        if pm_prices.yes_ask is None or pm_prices.no_ask is None:
            return None
        if kalshi_prices.yes_ask is None or kalshi_prices.no_ask is None:
            return None

        pm_yes_ask = pm_prices.yes_ask
        pm_no_ask = pm_prices.no_ask
        kalshi_yes_ask = kalshi_prices.yes_ask
        kalshi_no_ask = kalshi_prices.no_ask

        # 策略1: Buy Yes on Kalshi + Buy No on Polymarket
        cost_strategy_1 = kalshi_yes_ask + pm_no_ask
        profit_strategy_1 = 1.0 - cost_strategy_1

        # 策略2: Buy No on Kalshi + Buy Yes on Polymarket
        cost_strategy_2 = kalshi_no_ask + pm_yes_ask
        profit_strategy_2 = 1.0 - cost_strategy_2

        total_fees = self.fees.polymarket + self.fees.kalshi

        # 检查策略1
        if profit_strategy_1 > total_fees + self.min_profit_threshold:
            net_profit = profit_strategy_1 - total_fees
            roi = (net_profit / cost_strategy_1) * 100.0 if cost_strategy_1 > 0 else 0.0

            return ArbitrageOpportunity(
                strategy="Buy Yes on Kalshi + Buy No on Polymarket",
                kalshi_action=("BUY", "YES", kalshi_yes_ask),
                polymarket_action=("BUY", "NO", pm_no_ask),
                total_cost=cost_strategy_1,
                gross_profit=profit_strategy_1,
                fees=total_fees,
                net_profit=net_profit,
                roi_percent=roi,
                gas_fee=0.0,
                final_profit=0.0,
                final_roi_percent=0.0,
            )

        # 检查策略2
        if profit_strategy_2 > total_fees + self.min_profit_threshold:
            net_profit = profit_strategy_2 - total_fees
            roi = (net_profit / cost_strategy_2) * 100.0 if cost_strategy_2 > 0 else 0.0

            return ArbitrageOpportunity(
                strategy="Buy No on Kalshi + Buy Yes on Polymarket",
                kalshi_action=("BUY", "NO", kalshi_no_ask),
                polymarket_action=("BUY", "YES", pm_yes_ask),
                total_cost=cost_strategy_2,
                gross_profit=profit_strategy_2,
                fees=total_fees,
                net_profit=net_profit,
                roi_percent=roi,
                gas_fee=0.0,
                final_profit=0.0,
                final_roi_percent=0.0,
            )

        return None

    # 兼容旧调用
    check_arbitrage = check_arbitrage_optimal


def calculate_slippage_with_fixed_usdt(
    asks: List[Tuple[float, float]],
    usdt_amount: float
) -> SlippageInfo:
    """
    根据固定USDT金额计算滑点
    asks: (价格, 数量) 列表，已按价格升序
    usdt_amount: 固定投入金额
    """
    remaining_usdt = usdt_amount
    total_contracts = 0.0
    total_cost = 0.0
    best_price = asks[0][0] if asks else 0.0

    for price, size in asks:
        # 这一档的总价值 = 价格 × 数量
        level_value = price * size

        if remaining_usdt >= level_value:
            # 可以吃掉整档
            total_contracts += size
            total_cost += level_value
            remaining_usdt -= level_value
        else:
            # 只能吃部分
            buy_size = remaining_usdt / price
            total_contracts += buy_size
            total_cost += remaining_usdt
            remaining_usdt = 0.0
            break

    filled_amount = usdt_amount - remaining_usdt
    filled = remaining_usdt == 0.0

    if total_contracts == 0.0:
        return SlippageInfo(0.0, 0.0, filled, filled_amount, 0.0)

    avg_price = total_cost / total_contracts
    slippage_percent = (avg_price - best_price) / best_price * 100.0 if best_price > 0 else 0.0

    return SlippageInfo(avg_price, slippage_percent, filled, filled_amount, total_contracts)


def parse_polymarket_orderbook(data: dict, side: str) -> Optional[List[Tuple[float, float]]]:
    """
    解析Polymarket订单簿
    side: "YES" 或 "NO"
    """
    if side == "YES":
        # 买 YES：直接取 asks
        asks = data.get("asks", [])
        result = []
        for ask in asks:
            price = float(ask.get("price", "0"))
            size = float(ask.get("size", "0"))
            result.append((price, size))
        result.sort(key=lambda x: x[0])
        return result

    elif side == "NO":
        # 买 NO：从 bids 转换 (NO卖价 = 1 - YES买价)
        bids = data.get("bids", [])
        result = []
        for bid in bids:
            bid_price = float(bid.get("price", "0"))
            size = float(bid.get("size", "0"))
            ask_price = 1.0 - bid_price
            if 0.01 < ask_price < 1.0:
                result.append((ask_price, size))
        result.sort(key=lambda x: x[0])
        return result

    return None


def parse_kalshi_orderbook(data: dict, side: str) -> Optional[List[Tuple[float, float]]]:
    """
    解析Kalshi订单簿
    side: "YES" 或 "NO"
    """
    orderbook = data.get("orderbook_fp", {})

    if side == "YES":
        # 买 YES：从 no_dollars 转换 (YES卖价 = 1 - NO买价)
        no_bids = orderbook.get("no_dollars", [])
        result = []
        for entry in no_bids:
            bid_price = float(entry[0])
            size = float(entry[1])
            ask_price = 1.0 - bid_price
            if 0.01 < ask_price < 1.0:
                result.append((ask_price, size))
        result.sort(key=lambda x: x[0])
        return result

    elif side == "NO":
        # 买 NO：从 yes_dollars 转换 (NO卖价 = 1 - YES买价)
        yes_bids = orderbook.get("yes_dollars", [])
        result = []
        for entry in yes_bids:
            bid_price = float(entry[0])
            size = float(entry[1])
            ask_price = 1.0 - bid_price
            if 0.01 < ask_price < 1.0:
                result.append((ask_price, size))
        result.sort(key=lambda x: x[0])
        return result

    return None