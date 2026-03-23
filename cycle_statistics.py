# cycle_statistics.py — 与 Rust `cycle_statistics.rs` 对齐
from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Tuple

if TYPE_CHECKING:
    from arbitrage_detector import ArbitrageOpportunity

OpportunityTuple = Tuple[
    "ArbitrageOpportunity",
    str,
    str,
    Optional[datetime],
    Optional[datetime],
]


@dataclass
class _CumulativeStats:
    arb_hits: int = 0
    sum_capital: float = 0.0
    sum_gas: float = 0.0
    sum_fees: float = 0.0
    sum_gross_payout: float = 0.0
    sum_net_profit: float = 0.0
    full_match_cycles_completed: int = 0


@dataclass
class _BigPeriodStats:
    arb_hits: int = 0
    sum_capital: float = 0.0
    sum_gas: float = 0.0
    sum_fees: float = 0.0
    sum_gross_payout: float = 0.0
    sum_net_profit: float = 0.0


_GLOBAL = _CumulativeStats()
_BIG_PERIOD = _BigPeriodStats()
_LOCK = threading.Lock()


def record_opportunity(opp: "ArbitrageOpportunity") -> None:
    with _LOCK:
        g = _GLOBAL
        g.arb_hits += 1
        g.sum_capital += opp.capital_used
        g.sum_gas += opp.gas_amount
        g.sum_fees += opp.fees_amount
        g.sum_gross_payout += opp.contracts
        g.sum_net_profit += opp.net_profit_100

        bp = _BIG_PERIOD
        bp.arb_hits += 1
        bp.sum_capital += opp.capital_used
        bp.sum_gas += opp.gas_amount
        bp.sum_fees += opp.fees_amount
        bp.sum_gross_payout += opp.contracts
        bp.sum_net_profit += opp.net_profit_100


def flush_big_period_report_at_boundary(current_cycle: int, interval: int) -> str:
    global _BIG_PERIOD
    ended_period_no = current_cycle // interval
    n_track = max(0, interval - 1)

    with _LOCK:
        bp = _BIG_PERIOD
        bp_copy = _BigPeriodStats(
            arb_hits=bp.arb_hits,
            sum_capital=bp.sum_capital,
            sum_gas=bp.sum_gas,
            sum_fees=bp.sum_fees,
            sum_gross_payout=bp.sum_gross_payout,
            sum_net_profit=bp.sum_net_profit,
        )
        _BIG_PERIOD = _BigPeriodStats()
        g = _CumulativeStats(
            arb_hits=_GLOBAL.arb_hits,
            sum_capital=_GLOBAL.sum_capital,
            sum_gas=_GLOBAL.sum_gas,
            sum_fees=_GLOBAL.sum_fees,
            sum_gross_payout=_GLOBAL.sum_gross_payout,
            sum_net_profit=_GLOBAL.sum_net_profit,
            full_match_cycles_completed=_GLOBAL.full_match_cycles_completed,
        )

    bp_margin = (bp_copy.sum_net_profit / bp_copy.sum_capital * 100.0) if bp_copy.sum_capital > 1e-12 else 0.0
    global_margin = (g.sum_net_profit / g.sum_capital * 100.0) if g.sum_capital > 1e-12 else 0.0

    lines: List[str] = []
    lines.append("")
    lines.append("╔══════════════════════════════════════════════════════════════════════╗")
    lines.append(
        f"║  📊 上一大周期总绩效（大周期 #{ended_period_no} 已结束 · 1 次全量匹配 + {n_track} 次价格追踪）     ║"
    )
    lines.append("╚══════════════════════════════════════════════════════════════════════╝")
    lines.append("")
    lines.append("┌─ 本大周期内累计（结帐于下一匹配周期开始前）────────────────────────────")
    lines.append(f"│  套利识别次数:               {bp_copy.arb_hits}")
    lines.append(f"│  总本金 Σcapital:           ${bp_copy.sum_capital:.2f}")
    lines.append(f"│  总 Gas Σgas:               ${bp_copy.sum_gas:.2f}")
    lines.append(f"│  总手续费 Σfees:            ${bp_copy.sum_fees:.2f}")
    lines.append(f"│  总回报(兑付额 Σn):         ${bp_copy.sum_gross_payout:.2f}")
    lines.append(f"│  总净利润 Σnet:             ${bp_copy.sum_net_profit:.2f}")
    lines.append(f"│  本大周期利润率 (Σnet/Σcapital): {bp_margin:.2f}%")
    lines.append("└────────────────────────────────────────────────────────────────────────")
    lines.append("")
    lines.append("┌─ 自进程启动以来累计（每次识别均独立计数，含全量+追踪周期）────────────────")
    lines.append(f"│  已完成全量匹配周期数 N:     {g.full_match_cycles_completed}")
    lines.append(f"│  套利识别总次数:             {g.arb_hits}")
    lines.append(f"│  总成本 Σcapital:           ${g.sum_capital:.2f}")
    lines.append(f"│  总 Gas Σgas:               ${g.sum_gas:.2f}")
    lines.append(f"│  总手续费 Σfees:            ${g.sum_fees:.2f}")
    lines.append(f"│  总回报(兑付额 Σn):         ${g.sum_gross_payout:.2f}")
    lines.append(f"│  总净利润 Σnet:             ${g.sum_net_profit:.2f}")
    lines.append(f"│  整体利润率 (Σnet/Σcapital): {global_margin:.2f}%")
    lines.append("└────────────────────────────────────────────────────────────────────────")
    lines.append("")
    return "\n".join(lines)


def on_full_cycle_completed(rows: List[OpportunityTuple]) -> str:
    with _LOCK:
        _GLOBAL.full_match_cycles_completed += 1
    s = format_full_cycle_roi_top10_only(rows)
    print(s, end="")
    return s


def format_full_cycle_roi_top10_only(rows: List[OpportunityTuple]) -> str:
    with _LOCK:
        n = _GLOBAL.full_match_cycles_completed

    lines: List[str] = []
    lines.append("")
    lines.append("╔══════════════════════════════════════════════════════════════════════╗")
    lines.append(
        f"║  📈 全量匹配周期 #{n} · 利润率 Top 10（按 ROI%，100 USDT 腿资金口径）      ║"
    )
    lines.append("╚══════════════════════════════════════════════════════════════════════╝")

    if not rows:
        lines.append("   （本全量周期无验证通过的套利）")
    else:
        sorted_rows = sorted(rows, key=lambda r: r[0].roi_100_percent, reverse=True)
        for i, (opp, pm_title, ks_title, _, _) in enumerate(sorted_rows[:10]):
            lines.append("")
            lines.append(
                f"   #{i + 1:>2}  ROI {opp.roi_100_percent:>7.2f}%  净利 ${opp.net_profit_100:<10.2f}  | PM: {_truncate_title(pm_title, 72)} …"
            )
            lines.append(f"        Kalshi: {_truncate_title(ks_title, 76)}")
    lines.append("")
    return "\n".join(lines)


def _truncate_title(s: str, max_chars: int) -> str:
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 3] + "..."
