# paper_trading.py
# 模拟交易与持仓跟踪；参数见 system_params。
from __future__ import annotations

import csv
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from arbitrage_detector import (
    ArbitrageDetector,
    ArbitrageOpportunity,
    Fees,
    GAS_FEE_PER_TX,
    PairOrderbookLadders,
    orderbook_best_ask_price,
    proceeds_for_exact_contracts_sell,
)
from system_params import (
    PAPER_COOLDOWN_CYCLES,
    PAPER_INITIAL_CASH,
    PAPER_MAX_OPEN_POSITIONS,
    PAPER_MIN_EDGE_EARLY_USD,
    PAPER_RUN_LABEL_ENV,
    PAPER_SESSION_COUNTER_FILE,
    PAPER_TRADES_CSV,
    PAPER_TRADING_ENABLED,
    PAPER_WRITE_TRADE_LOG,
    paper_settlement_fee_estimate,
)


def validate_opportunity_from_ladders(
    arb_detector: ArbitrageDetector,
    ladders: PairOrderbookLadders,
    pm_side: str,
    kalshi_side: str,
    needs_inversion: bool,
    capital_usdt: float,
) -> Optional[ArbitrageOpportunity]:
    pm_optimal = orderbook_best_ask_price(ladders.pm_asks)
    kalshi_optimal = orderbook_best_ask_price(ladders.ks_asks)
    if pm_optimal is None or kalshi_optimal is None:
        return None
    return arb_detector.calculate_arbitrage_100usdt(
        pm_optimal,
        kalshi_optimal,
        ladders.pm_asks,
        ladders.ks_asks,
        pm_side,
        kalshi_side,
        needs_inversion,
        capital_usdt,
    )


@dataclass
class PaperPosition:
    trade_id: str
    pair_label: str
    opened_cycle: int
    simulated_open_time_utc: str
    pm_market_id: str
    kalshi_market_id: str
    pm_token_id: str
    n: float
    pm_side: str
    kalshi_side: str
    entry_capital_used: float
    pm_entry_avg: float
    ks_entry_avg: float
    fees_open: float
    gas_open: float


def _next_session_id() -> int:
    path = Path(PAPER_SESSION_COUNTER_FILE)
    n = 0
    if path.exists():
        try:
            n = int(path.read_text(encoding="utf-8").strip() or "0")
        except (ValueError, OSError):
            n = 0
    n += 1
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{n}\n", encoding="utf-8")
    return n


class PaperEngine:
    HEADER = (
        "event",
        "session_id",
        "trade_id",
        "pair_label",
        "cycle",
        "simulated_open_time_utc",
        "check_time_utc",
        "pm_market_id",
        "kalshi_market_id",
        "pm_token_id",
        "pm_side",
        "kalshi_side",
        "n",
        "entry_capital",
        "pm_entry_avg",
        "ks_entry_avg",
        "fees_open",
        "gas_open",
        "exit_type",
        "pm_exit_avg",
        "ks_exit_avg",
        "proceeds_gross",
        "fees_close",
        "gas_close",
        "pnl_realized",
        "cash_after",
        "no_close_reason",
        "notes",
    )

    @classmethod
    def try_new(cls) -> Optional[PaperEngine]:
        if not PAPER_TRADING_ENABLED:
            return None
        Path("logs").mkdir(parents=True, exist_ok=True)
        session_id = _next_session_id()
        write_trade_log = PAPER_WRITE_TRADE_LOG
        session_wall_started = datetime.now(timezone.utc)
        engine = cls(
            cash=float(PAPER_INITIAL_CASH),
            initial_cash=float(PAPER_INITIAL_CASH),
            session_id=session_id,
            write_trade_log=write_trade_log,
            max_cycle_seen=0,
            session_end_logged=False,
            session_wall_started=session_wall_started,
            positions={},
            cooldown_remaining={},
            fees=Fees(),
        )
        if write_trade_log:
            engine._ensure_csv_header()
            engine._append_session_start()
        print(
            f"   📒 [Paper] 模拟盘已启用 | session_id={session_id} | "
            f"初始资金 ${PAPER_INITIAL_CASH:.2f} | 冷却 {PAPER_COOLDOWN_CYCLES} 周期 | 写交易文件={write_trade_log}"
        )
        return engine

    def __init__(
        self,
        cash: float,
        initial_cash: float,
        session_id: int,
        write_trade_log: bool,
        max_cycle_seen: int,
        session_end_logged: bool,
        session_wall_started: datetime,
        positions: Dict[str, PaperPosition],
        cooldown_remaining: Dict[str, int],
        fees: Fees,
    ) -> None:
        self.cash = cash
        self.initial_cash = initial_cash
        self.session_id = session_id
        self.write_trade_log = write_trade_log
        self.max_cycle_seen = max_cycle_seen
        self.session_end_logged = session_end_logged
        self.session_wall_started = session_wall_started
        self.positions = positions
        self.cooldown_remaining = cooldown_remaining
        self.fees = fees

    def __del__(self) -> None:
        try:
            self.write_session_end("drop")
        except Exception:
            pass

    def _ensure_csv_header(self) -> None:
        p = Path(PAPER_TRADES_CSV)
        if p.exists():
            return
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(self.HEADER)

    def tick_cooldowns(self) -> None:
        for k in list(self.cooldown_remaining.keys()):
            v = self.cooldown_remaining[k] - 1
            if v <= 0:
                del self.cooldown_remaining[k]
            else:
                self.cooldown_remaining[k] = v

    def open_count(self) -> int:
        return len(self.positions)

    def has_open(self, pair_label: str) -> bool:
        return pair_label in self.positions

    def in_cooldown(self, pair_label: str) -> bool:
        return self.cooldown_remaining.get(pair_label, 0) > 0

    def snapshot_open_positions(self) -> List[PaperPosition]:
        return list(self.positions.values())

    def _bump_cycle(self, cycle: int) -> None:
        self.max_cycle_seen = max(self.max_cycle_seen, cycle)

    def _append_row(self, row: List[str]) -> None:
        if not self.write_trade_log:
            return
        Path(PAPER_TRADES_CSV).parent.mkdir(parents=True, exist_ok=True)
        with open(PAPER_TRADES_CSV, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)

    def check_early_close_at_cycle(
        self,
        pair_label: str,
        ladders: PairOrderbookLadders,
        check_cycle: int,
        check_time: datetime,
    ) -> None:
        pos = self.positions.get(pair_label)
        if pos is None:
            return

        gas_close = GAS_FEE_PER_TX * 2.0
        total_open_outlay = pos.entry_capital_used + pos.fees_open + pos.gas_open
        notes_base = os.environ.get(PAPER_RUN_LABEL_ENV, "")

        pm_fill = proceeds_for_exact_contracts_sell(ladders.pm_bids_desc, pos.n)
        ks_fill = proceeds_for_exact_contracts_sell(ladders.ks_bids_desc, pos.n)

        if pm_fill is None:
            notes = f"{notes_base} | insufficient_liquidity_pm_bids" if notes_base else "insufficient_liquidity_pm_bids"
            self._append_no_close(pos, check_cycle, check_time, "insufficient_liquidity_pm_bids", notes)
            return
        if ks_fill is None:
            notes = (
                f"{notes_base} | insufficient_liquidity_kalshi_bids"
                if notes_base
                else "insufficient_liquidity_kalshi_bids"
            )
            self._append_no_close(pos, check_cycle, check_time, "insufficient_liquidity_kalshi_bids", notes)
            return

        proceeds_pm, pm_avg_sell = pm_fill
        proceeds_ks, ks_avg_sell = ks_fill
        fees_close = proceeds_pm * self.fees.polymarket + proceeds_ks * self.fees.kalshi
        u_early_net = proceeds_pm + proceeds_ks - fees_close - gas_close
        fee_settle = paper_settlement_fee_estimate(
            pos.n, self.fees.polymarket, self.fees.kalshi
        )
        u_hold = pos.n - fee_settle

        if u_early_net <= u_hold + PAPER_MIN_EDGE_EARLY_USD:
            diag = (
                f"u_early_net={u_early_net:.4f} u_hold={u_hold:.4f} "
                f"min_edge_usd={PAPER_MIN_EDGE_EARLY_USD:.2f}"
            )
            notes = f"{notes_base} | {diag}" if notes_base else diag
            self._append_no_close(pos, check_cycle, check_time, "edge_below_threshold", notes)
            return

        pnl_realized = u_early_net - total_open_outlay
        self.cash += u_early_net
        del self.positions[pair_label]
        self.cooldown_remaining[pair_label] = PAPER_COOLDOWN_CYCLES

        self._append_close(
            pos,
            check_cycle,
            check_time,
            pm_avg_sell,
            ks_avg_sell,
            proceeds_pm + proceeds_ks,
            fees_close,
            gas_close,
            pnl_realized,
            notes_base,
        )
        print(
            f"   📒 [Paper] 平仓 early_bid | {pair_label} | trade_id={pos.trade_id} | "
            f"pnl=${pnl_realized:.2f} | cash=${self.cash:.2f}"
        )

    def log_no_close_book_error(
        self,
        pos: PaperPosition,
        check_cycle: int,
        check_time: datetime,
        reason: str,
    ) -> None:
        notes_env = os.environ.get(PAPER_RUN_LABEL_ENV, "")
        notes = f"{notes_env} | {reason}" if notes_env else reason
        self._append_no_close(pos, check_cycle, check_time, reason, notes)

    def _append_no_close(
        self,
        pos: PaperPosition,
        check_cycle: int,
        check_time: datetime,
        no_close_reason: str,
        notes: str,
    ) -> None:
        if not self.write_trade_log:
            return
        self._bump_cycle(check_cycle)
        self._append_row(
            [
                "NO_CLOSE",
                str(self.session_id),
                pos.trade_id,
                pos.pair_label,
                str(check_cycle),
                pos.simulated_open_time_utc,
                check_time.strftime("%Y-%m-%dT%H:%M:%S") + "+00:00",
                pos.pm_market_id,
                pos.kalshi_market_id,
                pos.pm_token_id,
                pos.pm_side,
                pos.kalshi_side,
                f"{pos.n:.6f}",
                f"{pos.entry_capital_used:.4f}",
                f"{pos.pm_entry_avg:.6f}",
                f"{pos.ks_entry_avg:.6f}",
                f"{pos.fees_open:.4f}",
                f"{pos.gas_open:.4f}",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                f"{self.cash:.4f}",
                no_close_reason,
                notes,
            ]
        )

    def _append_close(
        self,
        pos: PaperPosition,
        check_cycle: int,
        check_time: datetime,
        pm_exit_avg: float,
        ks_exit_avg: float,
        proceeds_gross: float,
        fees_close: float,
        gas_close: float,
        pnl_realized: float,
        notes: str,
    ) -> None:
        if not self.write_trade_log:
            return
        self._bump_cycle(check_cycle)
        self._append_row(
            [
                "CLOSE",
                str(self.session_id),
                pos.trade_id,
                pos.pair_label,
                str(check_cycle),
                pos.simulated_open_time_utc,
                check_time.strftime("%Y-%m-%dT%H:%M:%S") + "+00:00",
                pos.pm_market_id,
                pos.kalshi_market_id,
                pos.pm_token_id,
                pos.pm_side,
                pos.kalshi_side,
                f"{pos.n:.6f}",
                f"{pos.entry_capital_used:.4f}",
                f"{pos.pm_entry_avg:.6f}",
                f"{pos.ks_entry_avg:.6f}",
                f"{pos.fees_open:.4f}",
                f"{pos.gas_open:.4f}",
                "early_bid",
                f"{pm_exit_avg:.6f}",
                f"{ks_exit_avg:.6f}",
                f"{proceeds_gross:.4f}",
                f"{fees_close:.4f}",
                f"{gas_close:.4f}",
                f"{pnl_realized:.4f}",
                f"{self.cash:.4f}",
                "",
                notes,
            ]
        )

    def try_open(
        self,
        pair_label: str,
        opp: ArbitrageOpportunity,
        pm_side: str,
        kalshi_side: str,
        cycle: int,
        pm_market_id: str,
        kalshi_market_id: str,
        pm_token_id: str,
        opened_at: datetime,
    ) -> bool:
        if self.has_open(pair_label) or self.in_cooldown(pair_label):
            return False
        if self.open_count() >= PAPER_MAX_OPEN_POSITIONS:
            return False

        total_open_outlay = opp.capital_used + opp.fees_amount + opp.gas_amount
        if self.cash < total_open_outlay:
            print(
                f"   📒 [Paper] 资金不足，跳过开仓 {pair_label} | "
                f"需要 ${total_open_outlay:.2f}（含费与 gas）剩余 ${self.cash:.2f}"
            )
            return False

        trade_id = str(uuid.uuid4())
        open_time = opened_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "+00:00"
        pos = PaperPosition(
            trade_id=trade_id,
            pair_label=pair_label,
            opened_cycle=cycle,
            simulated_open_time_utc=open_time,
            pm_market_id=pm_market_id,
            kalshi_market_id=kalshi_market_id,
            pm_token_id=pm_token_id,
            n=opp.contracts,
            pm_side=pm_side,
            kalshi_side=kalshi_side,
            entry_capital_used=opp.capital_used,
            pm_entry_avg=opp.pm_avg_slipped,
            ks_entry_avg=opp.kalshi_avg_slipped,
            fees_open=opp.fees_amount,
            gas_open=opp.gas_amount,
        )

        self.cash -= total_open_outlay
        self.positions[pair_label] = pos

        notes_parts = [f"open_session={self.session_id}"]
        run_lbl = os.environ.get(PAPER_RUN_LABEL_ENV, "")
        if run_lbl:
            notes_parts.append(run_lbl)
        notes = " ".join(notes_parts).strip()

        if self.write_trade_log:
            self._bump_cycle(cycle)
            self._append_row(
                [
                    "OPEN",
                    str(self.session_id),
                    trade_id,
                    pair_label,
                    str(cycle),
                    open_time,
                    "",
                    pm_market_id,
                    kalshi_market_id,
                    pm_token_id,
                    pm_side,
                    kalshi_side,
                    f"{pos.n:.6f}",
                    f"{pos.entry_capital_used:.4f}",
                    f"{pos.pm_entry_avg:.6f}",
                    f"{pos.ks_entry_avg:.6f}",
                    f"{pos.fees_open:.4f}",
                    f"{pos.gas_open:.4f}",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    f"{self.cash:.4f}",
                    "",
                    notes,
                ]
            )

        print(
            f"   📒 [Paper] 开仓 OPEN | {pair_label} | trade_id={trade_id} | n={opp.contracts:.4f} | "
            f"成本(含费/gas)${total_open_outlay:.2f} | cash=${self.cash:.2f}"
        )
        return True

    def _append_session_start(self) -> None:
        notes = (
            f"marker=session_start wall_utc={self.session_wall_started.strftime('%Y-%m-%dT%H:%M:%S')}+00:00 "
            f"initial_cash={self.initial_cash:.2f}"
        )
        self._write_session_marker_row(
            "SESSION_START",
            0,
            self.session_wall_started,
            self.session_wall_started,
            "",
            "-",
            notes,
        )

    def write_session_end(self, reason: str) -> None:
        if self.session_end_logged or not self.write_trade_log:
            return
        wall = datetime.now(timezone.utc)
        notes = (
            f"marker=session_end wall_utc_end={wall.strftime('%Y-%m-%dT%H:%M:%S')}+00:00 "
            f"last_cycle={self.max_cycle_seen} reason={reason}"
        )
        try:
            self._write_session_marker_row(
                "SESSION_END",
                self.max_cycle_seen,
                self.session_wall_started,
                wall,
                reason,
                "-",
                notes,
            )
            self.session_end_logged = True
        except OSError as e:
            print(f"📒 [Paper] SESSION_END 写入失败: {e}")

    def _write_session_marker_row(
        self,
        event: str,
        cycle: int,
        simulated_wall: datetime,
        check_wall: Optional[datetime],
        exit_type: str,
        trade_id: str,
        notes: str,
    ) -> None:
        if not self.write_trade_log:
            return
        sim_str = simulated_wall.strftime("%Y-%m-%dT%H:%M:%S") + "+00:00"
        chk = ""
        if check_wall is not None:
            chk = check_wall.strftime("%Y-%m-%dT%H:%M:%S") + "+00:00"
        self._append_row(
            [
                event,
                str(self.session_id),
                trade_id,
                "-",
                str(cycle),
                sim_str,
                chk,
                "",
                "",
                "",
                "",
                "",
                "0.000000",
                "0.0000",
                "0.000000",
                "0.000000",
                "0.0000",
                "0.0000",
                exit_type,
                "",
                "",
                "",
                "",
                "",
                "",
                f"{self.cash:.4f}",
                "",
                notes,
            ]
        )
