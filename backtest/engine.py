# 与参考 `backtest/src/main.rs` 逻辑对齐的解析与报表（仅读 CSV，无副作用）
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import wcwidth

FEE_PM = 0.01
FEE_KS = 0.01

LABEL_DISP_W = 18
VALUE_DISP_W = 44
COL_GAP = "  "
COL_GAP_DISP_W = 2
INNER_DISP_W = LABEL_DISP_W + COL_GAP_DISP_W + VALUE_DISP_W


def settlement_fee_estimate(n: float) -> float:
    if n <= 0.0 or not (n == n and n != float("inf") and n != float("-inf")):
        return 0.0
    return n * (FEE_PM + FEE_KS)


def u_hold_notional(n: float) -> float:
    return n - settlement_fee_estimate(n)


def open_total_outlay(entry_capital: float, fees_open: float, gas_open: float) -> float:
    return entry_capital + fees_open + gas_open


def locked_pnl_at_open(n: float, entry_capital: float, fees_open: float, gas_open: float) -> float:
    return u_hold_notional(n) - open_total_outlay(entry_capital, fees_open, gas_open)


def parse_f64(s: str) -> float:
    try:
        return float(s.strip())
    except ValueError:
        return 0.0


def parse_u64(s: str) -> int:
    try:
        return int(s.strip())
    except ValueError:
        return 0


def parse_usize(s: str) -> int:
    try:
        return int(s.strip())
    except ValueError:
        return 0


def parse_dt(s: str) -> Optional[datetime]:
    t = s.strip()
    if not t:
        return None
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


@dataclass
class Row:
    line: int
    event: str
    session_id: int
    pair_label: str
    cycle: int
    simulated_open_time_utc: str
    check_time_utc: str
    n: float
    entry_capital: float
    fees_open: float
    gas_open: float
    pnl_realized: float
    cash_after: float
    notes: str


def effective_time_utc(r: Row) -> Optional[datetime]:
    if r.event in ("SESSION_START", "SESSION_END"):
        return parse_dt(r.check_time_utc) or parse_dt(r.simulated_open_time_utc)
    if r.event == "OPEN":
        return parse_dt(r.simulated_open_time_utc)
    if r.event in ("NO_CLOSE", "CLOSE"):
        return parse_dt(r.check_time_utc) or parse_dt(r.simulated_open_time_utc)
    return parse_dt(r.check_time_utc) or parse_dt(r.simulated_open_time_utc)


def session_start_utc(rows: List[Row], session_id: int) -> Optional[datetime]:
    from_marker = next(
        (
            effective_time_utc(r)
            for r in rows
            if r.session_id == session_id and r.event == "SESSION_START"
        ),
        None,
    )
    if from_marker is not None:
        return from_marker
    times = [t for r in rows if r.session_id == session_id for t in [effective_time_utc(r)] if t]
    return min(times) if times else None


def session_start_calendar_date(rows: List[Row], session_id: int) -> Optional[date]:
    t = session_start_utc(rows, session_id)
    return t.date() if t else None


def load_csv(path: Path) -> List[Row]:
    out: List[Row] = []
    with path.open(newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for i, record in enumerate(rdr):
            event = (record.get("event") or "").strip()
            if not event:
                continue
            out.append(
                Row(
                    line=i + 2,
                    event=event,
                    session_id=parse_u64(record.get("session_id") or ""),
                    pair_label=(record.get("pair_label") or "").strip(),
                    cycle=parse_usize(record.get("cycle") or ""),
                    simulated_open_time_utc=(record.get("simulated_open_time_utc") or "").strip(),
                    check_time_utc=(record.get("check_time_utc") or "").strip(),
                    n=parse_f64(record.get("n") or ""),
                    entry_capital=parse_f64(record.get("entry_capital") or ""),
                    fees_open=parse_f64(record.get("fees_open") or ""),
                    gas_open=parse_f64(record.get("gas_open") or ""),
                    pnl_realized=parse_f64(record.get("pnl_realized") or ""),
                    cash_after=parse_f64(record.get("cash_after") or ""),
                    notes=(record.get("notes") or "").strip(),
                )
            )
    return out


def collect_session_anchor_dates(rows: List[Row]) -> List[date]:
    ids: Set[int] = {r.session_id for r in rows}
    dates: Set[date] = set()
    for sid in ids:
        d = session_start_calendar_date(rows, sid)
        if d is not None:
            dates.add(d)
    return sorted(dates)


def sessions_started_on_date(rows: List[Row], d: date) -> List[int]:
    sids = sorted(
        {
            sid
            for sid in {r.session_id for r in rows}
            if session_start_calendar_date(rows, sid) == d
        }
    )
    return sids


@dataclass
class OpenPosition:
    n: float
    entry_capital: float
    fees_open: float
    gas_open: float


@dataclass
class SessionSpanInfo:
    start: Optional[datetime]
    end: Optional[datetime]
    cycle_min: Optional[int]
    cycle_max: Optional[int]


def compute_session_span(session_rows: List[Row]) -> SessionSpanInfo:
    start = next(
        (effective_time_utc(r) for r in session_rows if r.event == "SESSION_START"),
        None,
    )
    if start is None:
        times = [t for r in session_rows for t in [effective_time_utc(r)] if t]
        start = min(times) if times else None

    end_from_marker = next(
        (effective_time_utc(r) for r in session_rows if r.event == "SESSION_END"),
        None,
    )
    end_times = [t for r in session_rows for t in [effective_time_utc(r)] if t]
    end_last = max(end_times) if end_times else None
    end = end_from_marker or end_last

    cmin: Optional[int] = None
    cmax: Optional[int] = None
    for r in session_rows:
        cmin = r.cycle if cmin is None else min(cmin, r.cycle)
        cmax = r.cycle if cmax is None else max(cmax, r.cycle)

    return SessionSpanInfo(start=start, end=end, cycle_min=cmin, cycle_max=cmax)


def fmt_dt_hm(t: datetime) -> str:
    u = t.astimezone(timezone.utc)
    return u.strftime("%Y-%m-%d %H:%M UTC")


def fmt_wall_span(start: datetime, end: datetime) -> str:
    d = end - start
    secs = int(max(0, d.total_seconds()))
    days = secs // 86400
    h = (secs % 86400) // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    if days > 0:
        return f"{days} 天 {h} 小时 {m} 分"
    if h > 0:
        return f"{h} 小时 {m} 分"
    if m > 0:
        return f"{m} 分"
    return f"{max(1, s)} 秒"


def _term_width(s: str) -> int:
    w = 0
    for ch in s:
        v = wcwidth.wcwidth(ch)
        w += 1 if v is None or v < 0 else v
    return w


def pad_fill_display(target: int, used: int) -> str:
    return " " * max(0, target - used)


def fit_display_width(s: str, max_w: int) -> str:
    out = []
    used = 0
    for ch in s:
        cw = wcwidth.wcwidth(ch)
        cw = 1 if cw is None or cw < 0 else cw
        if used + cw > max_w:
            break
        out.append(ch)
        used += cw
    return "".join(out) + pad_fill_display(max_w, used)


def pad_label(label: str) -> str:
    w = _term_width(label)
    if w <= LABEL_DISP_W:
        return label + pad_fill_display(LABEL_DISP_W, w)
    return fit_display_width(label, LABEL_DISP_W)


def pad_value(value: str) -> str:
    w = _term_width(value)
    if w <= VALUE_DISP_W:
        return pad_fill_display(VALUE_DISP_W, w) + value
    return fit_display_width(value, VALUE_DISP_W)


def print_outer_top() -> None:
    print(f"  ┌{'─' * INNER_DISP_W}┐")


def print_outer_bottom() -> None:
    print(f"  └{'─' * INNER_DISP_W}┘")


def print_inner_sep() -> None:
    print(f"  ├{'─' * INNER_DISP_W}┤")


def row_line(label: str, value: str) -> None:
    body = f"{pad_label(label)}{COL_GAP}{pad_value(value)}"
    print(f"  │{body}│")


def fmt_money(v: float) -> str:
    if v != v or v in (float("inf"), float("-inf")):
        return "—"
    cents_i = round(v * 100.0)
    neg = cents_i < 0
    abs_cents = abs(cents_i)
    dollars_u = abs_cents // 100
    frac = abs_cents % 100
    s = str(dollars_u)
    with_commas = []
    for i, c in enumerate(reversed(s)):
        if i > 0 and i % 3 == 0:
            with_commas.append(",")
        with_commas.append(c)
    s = "".join(reversed(with_commas))
    sign = "-" if neg else ""
    return f"{sign}{s}.{frac:02d} USD"


def section_title(title: str) -> None:
    w = _term_width(title)
    if w <= INNER_DISP_W:
        line = title + pad_fill_display(INNER_DISP_W, w)
    else:
        line = fit_display_width(title, INNER_DISP_W)
    print(f"  │{line}│")


def parse_initial_cash_from_notes(notes: str) -> Optional[float]:
    for part in notes.split():
        if part.startswith("initial_cash="):
            try:
                return float(part[len("initial_cash=") :])
            except ValueError:
                return None
    return None


def print_performance_report(
    session_id: int,
    anchor_date: Optional[date],
    span: SessionSpanInfo,
    initial_cash: Optional[float],
    last_cash: Optional[float],
    u_hold_open_sum: float,
    open_event_count: int,
    close_count: int,
    open_pairs_end: int,
    close_realized_sum: float,
    locked_pnl_still_open: float,
) -> None:
    total_profit = close_realized_sum + locked_pnl_still_open
    equity = (last_cash + u_hold_open_sum) if last_cash is not None else None
    delta_from_funds = (
        (equity - initial_cash)
        if initial_cash is not None and equity is not None
        else None
    )
    reconcile_eps = 0.05
    reconcile_ok = (
        abs(delta_from_funds - total_profit) <= reconcile_eps
        if delta_from_funds is not None
        else True
    )

    print()
    print_outer_top()
    row_line("会话编号", str(session_id))
    if anchor_date is not None:
        row_line("归属 UTC 日", str(anchor_date))
    else:
        row_line("归属 UTC 日", "—")
    print_inner_sep()

    section_title("时间跨度")
    if span.start is not None and span.end is not None:
        row_line("启动", fmt_dt_hm(span.start))
        row_line("结束", fmt_dt_hm(span.end))
        row_line("墙钟跨度", fmt_wall_span(span.start, span.end))
    elif span.start is not None:
        row_line("启动", fmt_dt_hm(span.start))
        row_line("结束", "—")
        row_line("墙钟跨度", "—")
    else:
        row_line("启动", "—")
        row_line("结束", "—")
        row_line("墙钟跨度", "—")

    if span.cycle_min is not None and span.cycle_max is not None:
        n = span.cycle_max - span.cycle_min + 1
        row_line("监控周期数", f"{n} 个")
    else:
        row_line("监控周期数", "—")
    print_inner_sep()

    section_title("资金")
    row_line("初始资金", fmt_money(initial_cash) if initial_cash is not None else "—")
    row_line("期末现金", fmt_money(last_cash) if last_cash is not None else "—")
    row_line("到期估算权益", fmt_money(equity) if equity is not None else "—")
    if initial_cash is not None and delta_from_funds is not None:
        row_line("较初始变动", fmt_money(delta_from_funds))
    else:
        row_line("较初始变动", "—")
    print_inner_sep()

    section_title("仓位")
    row_line("开仓笔数", str(open_event_count))
    row_line("平仓笔数", str(close_count))
    row_line("期末未平", str(open_pairs_end))
    print_inner_sep()

    section_title("利润")
    row_line("总利润", fmt_money(total_profit))
    row_line("未平仓锁定", fmt_money(locked_pnl_still_open))
    row_line("平仓利润", fmt_money(close_realized_sum))

    print_outer_bottom()

    print()
    print("* 选日规则：仅列出「会话启动时刻」落在该 UTC 日的会话；跨自然日不拆分会话，整段绩效仍按会话汇总。")
    print("* 平仓利润为已平仓实现盈亏加总；未平仓锁定按持有到期回款估计减开仓成本；总利润为二者之和，与较初始变动一致。")
    print("* 到期权益为期末现金加未平仓估算回款；结算侧费率按双边各百分之一估算。")
    print("* 初始资金来自会话启动记录；期末现金取时间序上最后一笔后的现金余额。")
    if not reconcile_ok and delta_from_funds is not None:
        print(
            f"* 分项利润与权益差存在 {abs(delta_from_funds - total_profit):.4f} 美元量级的偏差，多为舍入或记录异常，建议核对源文件。"
        )


def _row_sort_key(r: Row) -> Tuple[int, datetime, int, int]:
    t = effective_time_utc(r)
    if t is None:
        return (1, datetime.min.replace(tzinfo=timezone.utc), r.cycle, r.line)
    return (0, t, r.cycle, r.line)


def analyze_session(rows: List[Row], session_id: int) -> None:
    session_rows = [r for r in rows if r.session_id == session_id]
    session_rows.sort(key=_row_sort_key)

    span = compute_session_span(session_rows)

    close_count = 0
    open_event_count = 0
    close_realized_sum = 0.0
    opens: Dict[str, OpenPosition] = {}
    initial_cash: Optional[float] = None
    last_cash: Optional[float] = None

    for r in session_rows:
        last_cash = r.cash_after
        if r.event == "SESSION_START":
            ic = parse_initial_cash_from_notes(r.notes)
            if ic is not None:
                initial_cash = ic
        elif r.event == "OPEN" and r.pair_label not in ("-", "") and r.pair_label:
            open_event_count += 1
            opens[r.pair_label] = OpenPosition(
                n=r.n,
                entry_capital=r.entry_capital,
                fees_open=r.fees_open,
                gas_open=r.gas_open,
            )
        elif r.event == "CLOSE" and r.pair_label not in ("-", "") and r.pair_label:
            close_count += 1
            close_realized_sum += r.pnl_realized
            opens.pop(r.pair_label, None)

    locked_pnl_still_open = sum(
        locked_pnl_at_open(p.n, p.entry_capital, p.fees_open, p.gas_open)
        for p in opens.values()
    )
    u_hold_open_sum = sum(u_hold_notional(p.n) for p in opens.values())

    print_performance_report(
        session_id,
        session_start_calendar_date(rows, session_id),
        span,
        initial_cash,
        last_cash,
        u_hold_open_sum,
        open_event_count,
        close_count,
        len(opens),
        close_realized_sum,
        locked_pnl_still_open,
    )
