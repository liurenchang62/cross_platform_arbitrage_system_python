# arbitrage_detector.py
# 与 Rust `arbitrage_detector.rs` 对齐：100 USDT 探针、精确 n 份成本、Gas 两腿
import math
from dataclasses import dataclass, field
from typing import Optional, Tuple, List

from market import MarketPrices

# Gas 费配置（固定值，单位 USDT）— 每笔交易 $0.02，两腿共 $0.04
GAS_FEE_PER_TX: float = 0.02
# 兼容旧名：两腿总 Gas（与 Rust `calculate_arbitrage_with_direction` 中 `* 2.0` 一致）
GAS_FEE: float = GAS_FEE_PER_TX * 2.0


@dataclass
class ArbitrageOpportunity:
    """套利机会数据结构（与 Rust ArbitrageOpportunity 字段对齐）"""
    strategy: str
    kalshi_action: Tuple[str, str, float]
    polymarket_action: Tuple[str, str, float]
    total_cost: float
    gross_profit: float
    fees: float
    net_profit: float
    roi_percent: float
    gas_fee: float
    final_profit: float
    final_roi_percent: float
    # 100 USDT 本金模式 / 订单簿展示
    pm_optimal: float = 0.0
    kalshi_optimal: float = 0.0
    pm_avg_slipped: float = 0.0
    kalshi_avg_slipped: float = 0.0
    contracts: float = 0.0
    capital_used: float = 0.0
    fees_amount: float = 0.0
    gas_amount: float = 0.0
    net_profit_100: float = 0.0
    roi_100_percent: float = 0.0
    orderbook_pm_top5: List[Tuple[float, float]] = field(default_factory=list)
    orderbook_kalshi_top5: List[Tuple[float, float]] = field(default_factory=list)


@dataclass
class Fees:
    polymarket: float = 0.01
    kalshi: float = 0.01


@dataclass
class SlippageInfo:
    avg_price: float
    slippage_percent: float
    filled: bool
    filled_amount: float
    filled_contracts: float


class ArbitrageDetector:
    def __init__(self, min_profit_threshold: float = 0.02):
        self.min_profit_threshold = min_profit_threshold
        self.fees = Fees()

    def with_fees(self, polymarket_fee: float, kalshi_fee: float) -> "ArbitrageDetector":
        self.fees.polymarket = polymarket_fee
        self.fees.kalshi = kalshi_fee
        return self

    def calculate_arbitrage_100usdt(
        self,
        pm_optimal: float,
        kalshi_optimal: float,
        pm_orderbook: Optional[List[Tuple[float, float]]],
        kalshi_orderbook: Optional[List[Tuple[float, float]]],
        pm_side: str,
        kalshi_side: str,
        needs_inversion: bool,
        capital_usdt: float,
    ) -> Optional[ArbitrageOpportunity]:
        """与 Rust `calculate_arbitrage_100usdt` 一致。"""
        total_cost_opt = pm_optimal + kalshi_optimal
        if total_cost_opt >= 1.0 or total_cost_opt <= 0.0:
            return None
        if capital_usdt <= 0.0:
            return None

        if pm_orderbook is not None:
            if not pm_orderbook:
                return None
            contracts_pm = calculate_slippage_with_fixed_usdt(pm_orderbook, capital_usdt).filled_contracts
        elif pm_optimal > 0.0:
            contracts_pm = capital_usdt / pm_optimal
        else:
            return None

        if kalshi_orderbook is not None:
            if not kalshi_orderbook:
                return None
            contracts_ks = calculate_slippage_with_fixed_usdt(kalshi_orderbook, capital_usdt).filled_contracts
        elif kalshi_optimal > 0.0:
            contracts_ks = capital_usdt / kalshi_optimal
        else:
            return None

        if contracts_pm > 0.0 and contracts_ks > 0.0:
            n = min(contracts_pm, contracts_ks)
        else:
            return None

        if pm_orderbook is not None:
            cpm = cost_for_exact_contracts(pm_orderbook, n)
            if cpm is None:
                return None
            c_pm, pm_avg = cpm
        else:
            c_pm = n * pm_optimal
            pm_avg = pm_optimal

        if kalshi_orderbook is not None:
            cks = cost_for_exact_contracts(kalshi_orderbook, n)
            if cks is None:
                return None
            c_ks, kalshi_avg = cks
        else:
            c_ks = n * kalshi_optimal
            kalshi_avg = kalshi_optimal

        capital_used = c_pm + c_ks
        gross = n * 1.0
        fees_amount = c_pm * self.fees.polymarket + c_ks * self.fees.kalshi
        gas_amount = GAS_FEE_PER_TX * 2.0
        net_profit_100 = gross - capital_used - fees_amount - gas_amount

        if net_profit_100 <= self.min_profit_threshold:
            return None
        roi_100 = (net_profit_100 / capital_used) * 100.0 if capital_used > 0.0 else 0.0

        inversion_note = " [Y/N颠倒]" if needs_inversion else ""
        strategy = f"Buy {pm_side} on Polymarket + Buy {kalshi_side} on Kalshi{inversion_note}"

        orderbook_pm_top5 = list(pm_orderbook[:5]) if pm_orderbook else []
        orderbook_kalshi_top5 = list(kalshi_orderbook[:5]) if kalshi_orderbook else []

        return ArbitrageOpportunity(
            strategy=strategy,
            kalshi_action=("BUY", kalshi_side, kalshi_optimal),
            polymarket_action=("BUY", pm_side, pm_optimal),
            total_cost=total_cost_opt,
            gross_profit=1.0 - total_cost_opt,
            fees=self.fees.polymarket + self.fees.kalshi,
            net_profit=(1.0 - total_cost_opt) - (self.fees.polymarket + self.fees.kalshi),
            roi_percent=roi_100,
            gas_fee=gas_amount,
            final_profit=net_profit_100,
            final_roi_percent=roi_100,
            pm_optimal=pm_optimal,
            kalshi_optimal=kalshi_optimal,
            pm_avg_slipped=pm_avg,
            kalshi_avg_slipped=kalshi_avg,
            contracts=n,
            capital_used=capital_used,
            fees_amount=fees_amount,
            gas_amount=gas_amount,
            net_profit_100=net_profit_100,
            roi_100_percent=roi_100,
            orderbook_pm_top5=orderbook_pm_top5,
            orderbook_kalshi_top5=orderbook_kalshi_top5,
        )

    def calculate_arbitrage_with_direction(
        self,
        pm_prices: MarketPrices,
        kalshi_prices: MarketPrices,
        pm_side: str,
        kalshi_side: str,
        needs_inversion: bool,
    ) -> Optional[ArbitrageOpportunity]:
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

        total_cost = pm_price + kalshi_price
        profit = 1.0 - total_cost
        total_fees = self.fees.polymarket + self.fees.kalshi

        if profit <= total_fees + self.min_profit_threshold:
            return None

        net_profit = profit - total_fees
        gas_total = GAS_FEE_PER_TX * 2.0
        final_profit = net_profit - gas_total

        if final_profit <= self.min_profit_threshold:
            return None

        roi = (final_profit / total_cost) * 100.0 if total_cost > 0 else 0.0

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
            gas_fee=gas_total,
            final_profit=final_profit,
            final_roi_percent=roi,
            pm_optimal=pm_price,
            kalshi_optimal=kalshi_price,
            pm_avg_slipped=pm_price,
            kalshi_avg_slipped=kalshi_price,
        )

    def calculate_final_profit(
        self,
        pm_prices: MarketPrices,
        kalshi_prices: MarketPrices,
        pm_slippage: float,
        kalshi_slippage: float,
    ) -> Optional[ArbitrageOpportunity]:
        opportunity = self.check_arbitrage_optimal(pm_prices, kalshi_prices)
        if opportunity is None:
            return None

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
        gas_total = GAS_FEE_PER_TX * 2.0
        final_profit = net_profit - gas_total

        if final_profit <= self.min_profit_threshold:
            return None

        roi = (final_profit / slipped_cost) * 100.0 if slipped_cost > 0 else 0.0

        return ArbitrageOpportunity(
            strategy=opportunity.strategy,
            kalshi_action=opportunity.kalshi_action,
            polymarket_action=opportunity.polymarket_action,
            total_cost=slipped_cost,
            gross_profit=slipped_profit,
            fees=total_fees,
            net_profit=net_profit,
            roi_percent=(net_profit / slipped_cost) * 100.0 if slipped_cost > 0 else 0.0,
            gas_fee=gas_total,
            final_profit=final_profit,
            final_roi_percent=roi,
            pm_optimal=opportunity.pm_optimal,
            kalshi_optimal=opportunity.kalshi_optimal,
            pm_avg_slipped=pm_slipped,
            kalshi_avg_slipped=kalshi_slipped,
        )

    def check_arbitrage_optimal(
        self,
        pm_prices: MarketPrices,
        kalshi_prices: MarketPrices,
    ) -> Optional[ArbitrageOpportunity]:
        if kalshi_prices.yes == 0.0 and kalshi_prices.no == 0.0:
            return None
        if not pm_prices.validate() or not kalshi_prices.validate():
            return None

        if pm_prices.yes_ask is None or pm_prices.no_ask is None:
            return None
        if kalshi_prices.yes_ask is None or kalshi_prices.no_ask is None:
            return None

        pm_yes_ask = pm_prices.yes_ask
        pm_no_ask = pm_prices.no_ask
        kalshi_yes_ask = kalshi_prices.yes_ask
        kalshi_no_ask = kalshi_prices.no_ask

        cost_strategy_1 = kalshi_yes_ask + pm_no_ask
        profit_strategy_1 = 1.0 - cost_strategy_1

        cost_strategy_2 = kalshi_no_ask + pm_yes_ask
        profit_strategy_2 = 1.0 - cost_strategy_2

        total_fees = self.fees.polymarket + self.fees.kalshi

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
                pm_optimal=pm_no_ask,
                kalshi_optimal=kalshi_yes_ask,
                pm_avg_slipped=pm_no_ask,
                kalshi_avg_slipped=kalshi_yes_ask,
            )

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
                pm_optimal=pm_yes_ask,
                kalshi_optimal=kalshi_no_ask,
                pm_avg_slipped=pm_yes_ask,
                kalshi_avg_slipped=kalshi_no_ask,
            )

        return None

    check_arbitrage = check_arbitrage_optimal


def cost_for_exact_contracts(asks: List[Tuple[float, float]], n: float) -> Optional[Tuple[float, float]]:
    """从卖盘（价格升序）吃进恰好 n 份合约；与 Rust 一致。"""
    if n <= 0.0 or not math.isfinite(n):
        return None
    eps = 1e-9
    remaining = n
    total_cost = 0.0
    for price, size in asks:
        if remaining <= eps:
            break
        if size <= 0.0 or price <= 0.0:
            continue
        take = min(remaining, size)
        total_cost += take * price
        remaining -= take
    if remaining > 1e-6:
        return None
    return (total_cost, total_cost / n)


def calculate_slippage_with_fixed_usdt(
    asks: List[Tuple[float, float]],
    usdt_amount: float,
) -> SlippageInfo:
    remaining_usdt = usdt_amount
    total_contracts = 0.0
    total_cost = 0.0
    best_price = asks[0][0] if asks else 0.0

    for price, size in asks:
        level_value = price * size
        if remaining_usdt >= level_value:
            total_contracts += size
            total_cost += level_value
            remaining_usdt -= level_value
        else:
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


def orderbook_best_ask_price(levels: List[Tuple[float, float]]) -> Optional[float]:
    if not levels:
        return None
    return levels[0][0]


def _json_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return 0.0


def parse_polymarket_orderbook(data: dict, side: str) -> Optional[List[Tuple[float, float]]]:
    if side == "YES":
        asks = data.get("asks", [])
        result = []
        for ask in asks:
            price = _json_float(ask.get("price"))
            size = _json_float(ask.get("size"))
            result.append((price, size))
        result.sort(key=lambda x: x[0])
        return result

    if side == "NO":
        bids = data.get("bids", [])
        result = []
        for bid in bids:
            bid_price = _json_float(bid.get("price"))
            size = _json_float(bid.get("size"))
            ask_price = 1.0 - bid_price
            if 0.01 < ask_price < 1.0:
                result.append((ask_price, size))
        result.sort(key=lambda x: x[0])
        return result

    return None


def parse_kalshi_orderbook(data: dict, side: str) -> Optional[List[Tuple[float, float]]]:
    orderbook = data.get("orderbook_fp", {})

    if side == "YES":
        no_bids = orderbook.get("no_dollars", [])
        result = []
        for entry in no_bids:
            bid_price = _json_float(entry[0])
            size = _json_float(entry[1])
            ask_price = 1.0 - bid_price
            if 0.01 < ask_price < 1.0:
                result.append((ask_price, size))
        result.sort(key=lambda x: x[0])
        return result

    if side == "NO":
        yes_bids = orderbook.get("yes_dollars", [])
        result = []
        for entry in yes_bids:
            bid_price = _json_float(entry[0])
            size = _json_float(entry[1])
            ask_price = 1.0 - bid_price
            if 0.01 < ask_price < 1.0:
                result.append((ask_price, size))
        result.sort(key=lambda x: x[0])
        return result

    return None
