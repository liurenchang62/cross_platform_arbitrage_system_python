# validation.py
#! 二筛模块：与 Rust `validation.rs` 对齐（逻辑、顺序、默认 Y/N 与过滤原因）

from __future__ import annotations

import contextlib
import io
import math
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# 预编译正则（对应 Rust Lazy<Regex>）
# ---------------------------------------------------------------------------
ELECTORAL_STATE_DISTRICT_RE = re.compile(r"(?i)\b[a-z]{2}-\d{1,2}\b")
ELECTORAL_NTH_PLACE_RE = re.compile(r"(?i)\b\d+(?:st|nd|rd|th)\s+place\b")

RE_STAT_PLUS = re.compile(r"(?i)(\d+\.?\d*)\s*\+\s*([a-z]+)")
RE_STAT_MINUS = re.compile(r"(?i)(\d+\.?\d*)\s*-\s*([a-z]+)")
RE_KS_PLUS_BOUND = re.compile(r"\b\d+(?:\.\d+)?\s*\+")
RE_KS_MINUS_BOUND = re.compile(r"\b\d+(?:\.\d+)?\s*-")
RE_GAME_MAP_MATCH_NUM = re.compile(r"(?i)(?:game|map|match)\s*(\d+)")
RE_HAS_GAME_KEYWORD = re.compile(r"(?i)(?:game|map|match)\s*\d+")
RE_SINGLE_GAME_WINNER = re.compile(r"(?i)(?:game|map|match)\s*\d+\s*winner")
RE_WIN_MAP = re.compile(
    r"(?i)win\s+(?:map|game|match)\s+\d+|(?:map|game|match)\s+\d+.*win"
)
RE_DIGIT = re.compile(r"\d")
RE_WIN_THEN_MATCH = re.compile(r"(?i)\bwin\b[^?]{0,160}\bmatch\b")
RE_WIN_MAP_ONLY = re.compile(r"(?i)win\s+(?:map|game|match)\s+\d+")
RE_MAP_WINNER_ONLY = re.compile(r"(?i)(?:game|map|match)\s*\d+\s*winner")
RE_WIN_SET = re.compile(r"(?i)win\s+set\s+\d+")
RE_BO = re.compile(r"(?i)\bbo\d+\b")
RE_WTA_ATP_PREFIX = re.compile(r"(?i)\b(wta|atp)\b[^:]{0,120}:")
RE_AT_START = re.compile(
    r"(?i)^[A-Za-z0-9\s]+at\s+[A-Za-z0-9\s]+"
)
RE_WEATHER_IN_AT = re.compile(
    r"(?i)(?:in|at)\s+([A-Za-z][A-Za-z\s\-']{2,}?)(?:\s+(?:be|on|,|or|\?)|$)"
)
RE_WEATHER_WILL_HIGHEST = re.compile(
    r"^(?:Will\s+(?:the\s+)?(?:highest|maximum|minimum|high|low)\s+temperature\s+in\s+)([A-Za-z][A-Za-z\s\-']+?)\s+be",
    re.IGNORECASE,
)
RE_WILL_WIN_THE = re.compile(r"(?i)will\s+.+\s+win\s+the\s+")
# `EsportsTournamentWinnerVsSportsGoalsValidator`：Will X win <赛事>…（无 vs）
RE_WILL_WIN_EVENT = re.compile(r"(?i)will\s+.+\s+win\s+")
RE_OR_BELOW_F = re.compile(r"(?i)(\d+)\s*°\s*F\s*or\s+below")
RE_OR_BELOW_PLAIN = re.compile(r"(?i)(\d+)\s*°\s*or\s+below")
RE_WINS_BY_POINTS = re.compile(r"(?i)wins?\s+by\s+(over|under)\s+\d+\.?\d*\s*points")

# 与 `EsportsTournamentWinnerVsSportsGoalsValidator::has_esports_series_anchor` 一致
ESPORTS_SERIES_ANCHORS = (
    "dreamhack",
    "esl one",
    "esl pro",
    "esl challenge",
    "esl ",
    "blast",
    "iem ",
    " iem",
    " vct",
    "vct ",
    "valorant champions",
    "lck",
    "lec",
    "lcs",
    "lol worlds",
    "league of legends",
    " worlds 20",
    "the international",
    "dota 2",
    "dota2",
    "dota ",
    "counter-strike",
    "counter strike",
    "cs2",
    "cs:go",
    "cs go",
    "faceit",
    "pgl ",
    " pgl",
    "six invitational",
    "rainbow six",
    "rlcs",
    "rocket league",
    "overwatch",
    "fortnite",
    "pubg",
    "starcraft",
    "evo 20",
    "capcom cup",
    "tekken",
    "smash bros",
    "fighting games",
    "esports",
    "e-sports",
)


# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------
SAFE_WORDS = [
    "next", "upcoming", "today", "tonight", "future", "current",
]

SPORTS_KEYWORDS = [
    "points", "goals", "runs", "o/u", "over/under", "over", "under",
    "winner", "win", "tie", "draw", "spread", "moneyline", "total",
    "vs", "versus", "score", "scored", "mvp", "championship", "points",
    "rebounds", "assists",
]

GARBAGE_KEYWORDS = [
    "o/u", "rounds", "sets", "games", "maps", "upsets",
    "quarters", "halves", "periods", "wins",
]

STAT_TYPES = [
    "points", "rebounds", "assists", "steals",
    "blocks", "threes", "double", "triple",
]

MONTH_MAP: List[Tuple[str, int]] = [
    ("jan", 1), ("january", 1),
    ("feb", 2), ("february", 2),
    ("mar", 3), ("march", 3),
    ("apr", 4), ("april", 4),
    ("may", 5), ("may", 5),
    ("jun", 6), ("june", 6),
    ("jul", 7), ("july", 7),
    ("aug", 8), ("august", 8),
    ("sep", 9), ("september", 9),
    ("oct", 10), ("october", 10),
    ("nov", 11), ("november", 11),
    ("dec", 12), ("december", 12),
]

RE_EXTRACT_NUMBER = re.compile(r"(\d+\.?\d*)")
RE_MONTH_DAY = re.compile(
    r"(?i)(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2})(?:,?\s*(\d{4}))?"
)
RE_YEAR = re.compile(r"\b(20\d{2})\b")


@dataclass
class MatchInfo:
    pm_title: str
    kalshi_title: str
    similarity: float
    category: str
    pm_side: str
    kalshi_side: str
    needs_inversion: bool


@dataclass
class DateInfo:
    month: int
    day: int
    has_year: bool
    year: Optional[int] = None


@dataclass
class NumberInfo:
    value: float
    context: str
    is_year: bool


@dataclass
class RetainedSample:
    pm_title: str
    kalshi_title: str
    similarity: float
    category: str
    pm_side: str
    kalshi_side: str
    needs_inversion: bool


# ==================== 工具函数 ====================

def extract_number(text: str) -> Optional[float]:
    m = RE_EXTRACT_NUMBER.search(text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def extract_first_team(title: str) -> str:
    if " vs " in title:
        return title.split(" vs ", 1)[0].strip()
    if " vs. " in title:
        return title.split(" vs.", 1)[0].strip()
    return ""


def extract_teams(title: str) -> Optional[Tuple[str, str]]:
    if " vs " in title:
        a, b = title.split(" vs ", 1)
        t1, t2 = a.strip(), b.strip()
        if t1 and t2:
            return (t1, t2)
    if " vs. " in title:
        a, b = title.split(" vs.", 1)
        t1, t2 = a.strip(), b.strip()
        if t1 and t2:
            return (t1, t2)
    return None


def extract_winner(title: str) -> str:
    if " - " in title:
        return title.rsplit(" - ", 1)[-1].strip()
    return ""


def strip_team_event_prefix(team: str) -> str:
    t = team.strip()
    i = t.rfind(":")
    if i != -1:
        after = t[i + 1 :].strip()
        if after:
            return after
    return t


def extract_kalshi_moneyline_pair(title: str) -> Optional[Tuple[str, str]]:
    parts = title.split(" - ", 1)
    main = parts[0].strip()
    if not main:
        return None
    lower = main.lower()

    idx = lower.find(" in the ")
    if idx != -1:
        after_in = main[idx + 8 :].strip()
        l_after = after_in.lower()
        end_m = l_after.rfind(" match")
        if end_m != -1:
            mid = after_in[:end_m].strip().rstrip("?").strip()
            ml = mid.lower()
            vs_dot = ml.find(" vs. ")
            if vs_dot != -1:
                t1 = mid[:vs_dot].strip()
                t2 = mid[vs_dot + 5 :].strip().rstrip("?").strip()
                if t1 and t2:
                    return (t1, t2)
            vs_s = ml.find(" vs ")
            if vs_s != -1:
                t1 = mid[:vs_s].strip()
                t2 = mid[vs_s + 4 :].strip().rstrip("?").strip()
                if t1 and t2:
                    return (t1, t2)

    if " at " in lower:
        if " winner" in lower:
            before_winner = main[: lower.find(" winner")].strip()
        else:
            before_winner = main.split("?", 1)[0].strip()
        bl = before_winner.lower()
        pos = bl.find(" at ")
        if pos != -1:
            left = before_winner[:pos].strip()
            right = before_winner[pos + 4 :].strip()
            if left and right:
                return (left, right)

    wt = lower.find("win the ")
    if wt != -1:
        after = main[wt + 8 :]
        l_after = after.lower()
        ends = []
        ci = after.find(":")
        if ci != -1:
            ends.append(ci)
        for key in (" round", " match"):
            ki = l_after.find(key)
            if ki != -1:
                ends.append(ki)
        end = min(ends) if ends else len(after)
        segment = after[:end].strip()
    else:
        end = lower.find(" winner")
        segment = main[: end if end != -1 else len(main)].strip()

    sl = segment.lower()
    vs_pos = sl.find(" vs ")
    if vs_pos != -1:
        t1 = segment[:vs_pos].strip()
        rest = segment[vs_pos + 4 :].strip()
        t2 = rest.split(":", 1)[0].strip()
        if t1 and t2:
            return (t1, t2)
    vs_pos = sl.find(" vs.")
    if vs_pos != -1:
        t1 = segment[:vs_pos].strip()
        rest = segment[vs_pos + 5 :].strip()
        t2 = rest.split(":", 1)[0].strip()
        if t1 and t2:
            return (t1, t2)

    return None


def kalshi_head_to_head_pair_required(title: str) -> bool:
    l = title.lower()
    return (
        ("win the " in l and (" vs" in l or "vs." in l))
        or (" at " in l and "winner" in l)
        or ((" vs " in l or " vs." in l) and "winner" in l)
    )


class FinalsConsistencyValidator:
    @staticmethod
    def has_finals_keyword(title: str) -> bool:
        lower = title.lower()
        return (
            "finals" in lower
            or "championship" in lower
            or "conference finals" in lower
        )

    @staticmethod
    def trim_team_suffix(s: str) -> str:
        if " Winner" in s:
            s = s[: s.find(" Winner")].rstrip()
        if " - " in s:
            s = s[: s.find(" - ")].rstrip()
        return s.strip()

    @staticmethod
    def extract_teams_cleaned(title: str) -> Optional[Tuple[str, str]]:
        teams = extract_teams(title)
        if not teams:
            return None
        t1, t2 = teams
        return (
            FinalsConsistencyValidator.trim_team_suffix(t1),
            FinalsConsistencyValidator.trim_team_suffix(t2),
        )

    @staticmethod
    def finals_consistency_match(pm_title: str, kalshi_title: str) -> bool:
        pm_finals = FinalsConsistencyValidator.has_finals_keyword(pm_title)
        ks_finals = FinalsConsistencyValidator.has_finals_keyword(kalshi_title)
        if pm_finals == ks_finals:
            return True
        pm_teams = FinalsConsistencyValidator.extract_teams_cleaned(pm_title)
        ks_teams = FinalsConsistencyValidator.extract_teams_cleaned(kalshi_title)
        if not pm_teams or not ks_teams:
            return False
        pm_a, pm_b = pm_teams
        ks_a, ks_b = ks_teams
        return (
            names_match(pm_a, ks_a) and names_match(pm_b, ks_b)
        ) or (
            names_match(pm_a, ks_b) and names_match(pm_b, ks_a)
        )


def normalize_entity_name(text: str) -> str:
    normalized: List[str] = []
    last_space = False
    for ch in text.lower():
        if ch.isalnum():
            normalized.append(ch)
            last_space = False
        elif ch.isspace() or ch in "-_'\u2019":
            if not last_space:
                normalized.append(" ")
                last_space = True
    return "".join(normalized).strip()


def names_match(a: str, b: str) -> bool:
    na = normalize_entity_name(a)
    nb = normalize_entity_name(b)
    if not na or not nb:
        return False
    if na == nb:
        return True

    def compact(s: str) -> str:
        return "".join(c for c in s if not c.isspace())

    ca, cb = compact(na), compact(nb)
    if len(ca) >= 4 and len(cb) >= 4:
        if ca == cb:
            return True
        if len(ca) >= 5 and len(cb) >= 5 and (ca in cb or cb in ca):
            return True
    if len(na) >= 4 and nb.find(na) != -1:
        return True
    if len(nb) >= 3 and na.find(nb) != -1:
        return True
    return False


def two_team_sets_consistent(pm_a: str, pm_b: str, ks_a: str, ks_b: str) -> bool:
    pm_a = FinalsConsistencyValidator.trim_team_suffix(pm_a)
    pm_b = FinalsConsistencyValidator.trim_team_suffix(pm_b)
    ks_a = FinalsConsistencyValidator.trim_team_suffix(ks_a)
    ks_b = FinalsConsistencyValidator.trim_team_suffix(ks_b)
    p1 = strip_team_event_prefix(pm_a)
    p2 = strip_team_event_prefix(pm_b)
    k1 = strip_team_event_prefix(ks_a)
    k2 = strip_team_event_prefix(ks_b)
    return (
        (names_match(p1, k1) and names_match(p2, k2))
        or (names_match(p1, k2) and names_match(p2, k1))
    )


class BracketAdvanceVsSingleGameValidator:
    @staticmethod
    def is_bracket_advance_proposition(title: str) -> bool:
        l = title.lower()
        progress = (
            "advance to" in l
            or "advance into" in l
            or "reach the" in l
            or "make the" in l
            or "make it to" in l
        )
        rnd = (
            "sweet sixteen" in l
            or "sweet 16" in l
            or "final four" in l
            or "elite eight" in l
            or "elite 8" in l
            or "national championship" in l
        )
        return progress and rnd

    @staticmethod
    def allows_pair(pm_title: str, kalshi_title: str) -> bool:
        pm_adv = BracketAdvanceVsSingleGameValidator.is_bracket_advance_proposition(
            pm_title
        )
        ks_adv = BracketAdvanceVsSingleGameValidator.is_bracket_advance_proposition(
            kalshi_title
        )
        if pm_adv and SportsSingleVsFinalsValidator.is_single_game_format(kalshi_title):
            return False
        if ks_adv and SportsSingleVsFinalsValidator.is_single_game_format(pm_title):
            return False
        return True


class WeatherValidator:
    @staticmethod
    def is_temperature_market(title: str) -> bool:
        lower = title.lower()
        temp_kw = (
            "temperature" in lower
            or "°" in title
            or "°c" in lower
            or "°f" in lower
        )
        hi_lo = (
            "highest" in lower
            or "maximum" in lower
            or "minimum" in lower
            or "high" in lower
            or "low" in lower
        )
        return temp_kw and hi_lo

    @staticmethod
    def extract_region(title: str) -> Optional[str]:
        m = RE_WEATHER_IN_AT.search(title)
        if m:
            region = m.group(1).strip().lower()
            if len(region) >= 2 and not all(c in " -" for c in region):
                return region
        m2 = RE_WEATHER_WILL_HIGHEST.match(title.strip())
        if m2:
            region = m2.group(1).strip().lower()
            if len(region) >= 2:
                return region
        return None

    @staticmethod
    def regions_match(pm_title: str, kalshi_title: str) -> bool:
        if not WeatherValidator.is_temperature_market(
            pm_title
        ) or not WeatherValidator.is_temperature_market(kalshi_title):
            return True
        pm_r = WeatherValidator.extract_region(pm_title)
        ks_r = WeatherValidator.extract_region(kalshi_title)
        pm_n = normalize_entity_name(pm_r) if pm_r else None
        ks_n = normalize_entity_name(ks_r) if ks_r else None
        if pm_n is not None and ks_n is not None:
            return names_match(pm_n, ks_n)
        if (pm_n is not None) != (ks_n is not None):
            return False
        return True

    @staticmethod
    def extract_or_below_fahrenheit_threshold(title: str) -> Optional[int]:
        m = RE_OR_BELOW_F.search(title)
        if m:
            try:
                n = int(m.group(1))
                if 20 <= n <= 120:
                    return n
            except ValueError:
                pass
        m2 = RE_OR_BELOW_PLAIN.search(title)
        if m2:
            try:
                n = int(m2.group(1))
                if 20 <= n <= 120:
                    return n
            except ValueError:
                pass
        return None

    @staticmethod
    def fahrenheit_or_below_buckets_match(pm_title: str, kalshi_title: str) -> bool:
        if not WeatherValidator.is_temperature_market(
            pm_title
        ) or not WeatherValidator.is_temperature_market(kalshi_title):
            return True
        pm = WeatherValidator.extract_or_below_fahrenheit_threshold(pm_title)
        ks = WeatherValidator.extract_or_below_fahrenheit_threshold(kalshi_title)
        if pm is not None and ks is not None:
            return pm == ks
        return True


class EsportsGameValidator:
    @staticmethod
    def extract_game_number(title: str) -> Optional[int]:
        m = RE_GAME_MAP_MATCH_NUM.search(title)
        if not m:
            return None
        try:
            return int(m.group(1))
        except ValueError:
            return None

    @staticmethod
    def is_esports_style_match(title: str) -> bool:
        has_vs = " vs " in title or " vs." in title
        return has_vs and bool(RE_HAS_GAME_KEYWORD.search(title))

    @staticmethod
    def game_numbers_match(pm_title: str, kalshi_title: str) -> bool:
        if not EsportsGameValidator.is_esports_style_match(
            pm_title
        ) or not EsportsGameValidator.is_esports_style_match(kalshi_title):
            return True
        pm_n = EsportsGameValidator.extract_game_number(pm_title)
        ks_n = EsportsGameValidator.extract_game_number(kalshi_title)
        if pm_n is not None and ks_n is not None:
            return pm_n == ks_n
        if (pm_n is not None) != (ks_n is not None):
            return False
        return True

    @staticmethod
    def is_single_game_winner(title: str) -> bool:
        has_vs = " vs " in title or " vs." in title
        w1 = bool(RE_SINGLE_GAME_WINNER.search(title))
        w2 = bool(RE_WIN_MAP.search(title))
        return has_vs and (w1 or w2)

    @staticmethod
    def is_total_maps_market(title: str) -> bool:
        lower = title.lower()
        if "maps" not in lower:
            return False
        if "over" not in lower and "under" not in lower:
            return False
        return bool(RE_DIGIT.search(title))

    @staticmethod
    def single_vs_total_match(pm_title: str, kalshi_title: str) -> bool:
        pm_s = EsportsGameValidator.is_single_game_winner(pm_title)
        pm_t = EsportsGameValidator.is_total_maps_market(pm_title)
        ks_s = EsportsGameValidator.is_single_game_winner(kalshi_title)
        ks_t = EsportsGameValidator.is_total_maps_market(kalshi_title)
        if (pm_s and ks_t) or (pm_t and ks_s):
            return False
        pm_ts = EsportsGameValidator.is_total_sets_market(pm_title)
        ks_ts = EsportsGameValidator.is_total_sets_market(kalshi_title)
        pm_ss = EsportsGameValidator.is_single_set_winner(pm_title)
        ks_ss = EsportsGameValidator.is_single_set_winner(kalshi_title)
        if (pm_ts and ks_ss) or (pm_ss and ks_ts):
            return False
        return True

    @staticmethod
    def is_handicap_style_title(title: str) -> bool:
        l = title.lower()
        return (
            "handicap" in l
            or "让分" in l
            or "spread" in l
            or "map handicap" in l
        )

    @staticmethod
    def handicap_vs_total_maps_match(pm_title: str, kalshi_title: str) -> bool:
        pm_h = EsportsGameValidator.is_handicap_style_title(pm_title)
        ks_h = EsportsGameValidator.is_handicap_style_title(kalshi_title)
        pm_t = EsportsGameValidator.is_total_maps_market(pm_title)
        ks_t = EsportsGameValidator.is_total_maps_market(kalshi_title)
        if (pm_h and ks_t) or (ks_h and pm_t):
            return False
        return True

    @staticmethod
    def is_total_sets_market(title: str) -> bool:
        lower = title.lower()
        return (
            "total sets" in lower
            and ("o/u" in lower or "over" in lower or "under" in lower)
            and bool(RE_DIGIT.search(title))
        )

    @staticmethod
    def is_single_set_winner(title: str) -> bool:
        return bool(RE_WIN_SET.search(title))

    @staticmethod
    def is_series_or_bo_market(title: str) -> bool:
        lower = title.lower()
        has_vs = " vs " in title or " vs." in title
        has_bo = bool(RE_BO.search(title)) or "(bo5)" in lower or "(bo3)" in lower
        has_series = (
            "first stand" in lower
            or "group " in lower
            or "group a" in lower
            or "group b" in lower
        )
        return has_vs and (has_bo or has_series)

    @staticmethod
    def single_vs_series_match(pm_title: str, kalshi_title: str) -> bool:
        pm_s = EsportsGameValidator.is_single_game_winner(pm_title)
        pm_sr = EsportsGameValidator.is_series_or_bo_market(pm_title)
        ks_s = EsportsGameValidator.is_single_game_winner(kalshi_title)
        ks_sr = EsportsGameValidator.is_series_or_bo_market(kalshi_title)
        if (pm_s and ks_sr) or (pm_sr and ks_s):
            return False
        return True

    @staticmethod
    def is_single_map_winner(title: str) -> bool:
        return bool(RE_WIN_MAP_ONLY.search(title)) or bool(
            RE_MAP_WINNER_ONLY.search(title)
        )

    @staticmethod
    def is_whole_match_winner(title: str) -> bool:
        lower = title.lower()
        if not (" vs " in title or " vs." in title):
            return False
        if EsportsGameValidator.is_single_map_winner(
            title
        ) or EsportsGameValidator.is_single_game_winner(title):
            return False
        return bool(RE_WIN_THEN_MATCH.search(title))

    @staticmethod
    def map_winner_vs_whole_match_match(pm_title: str, kalshi_title: str) -> bool:
        pm_r = EsportsGameValidator.is_single_map_winner(
            pm_title
        ) or EsportsGameValidator.is_single_game_winner(pm_title)
        ks_r = EsportsGameValidator.is_single_map_winner(
            kalshi_title
        ) or EsportsGameValidator.is_single_game_winner(kalshi_title)
        pm_w = EsportsGameValidator.is_whole_match_winner(pm_title)
        ks_w = EsportsGameValidator.is_whole_match_winner(kalshi_title)
        if (pm_r and ks_w) or (ks_r and pm_w):
            return False
        return True


class DrawVsWinnerValidator:
    @staticmethod
    def is_draw_or_tie_proposition(title: str) -> bool:
        l = title.lower()
        return (
            "draw" in l
            or "end in a draw" in l
            or " tie " in l
            or "finish in a tie" in l
            or l.startswith("tie ")
        )

    @staticmethod
    def is_vs_winner_moneyline(title: str) -> bool:
        l = title.lower()
        return (
            "winner" in l or "Winner" in title
        ) and (" vs " in title or " vs." in title or " at " in l)

    @staticmethod
    def allows_pair(pm_title: str, kalshi_title: str) -> bool:
        if DrawVsWinnerValidator.is_draw_or_tie_proposition(
            pm_title
        ) and DrawVsWinnerValidator.is_vs_winner_moneyline(kalshi_title):
            return False
        if DrawVsWinnerValidator.is_draw_or_tie_proposition(
            kalshi_title
        ) and DrawVsWinnerValidator.is_vs_winner_moneyline(pm_title):
            return False
        return True


class TossVsMatchMarketValidator:
    @staticmethod
    def is_toss_proposition(title: str) -> bool:
        l = title.lower()
        return (
            "who wins the toss" in l
            or "win the toss" in l
            or "wins the toss" in l
            or "coin toss" in l
            or "toss winner" in l
            or "winner of the toss" in l
            or (" toss" in l and " who " in l)
        )

    @staticmethod
    def allows_pair(pm_title: str, kalshi_title: str) -> bool:
        return TossVsMatchMarketValidator.is_toss_proposition(
            pm_title
        ) == TossVsMatchMarketValidator.is_toss_proposition(kalshi_title)


class ExactScoreVsGoalsTotalsValidator:
    @staticmethod
    def is_exact_score_market(title: str) -> bool:
        l = title.lower()
        return "exact score" in l or "correct score" in l

    @staticmethod
    def is_goals_totals_line(title: str) -> bool:
        l = title.lower()
        if "maps" in l or " map " in l:
            return False
        if "goal" not in l:
            return False
        has_ou = (
            "over " in l
            or "under " in l
            or "o/u" in l
            or "totals" in l
            or "total " in l
        )
        return has_ou and bool(RE_DIGIT.search(title))

    @staticmethod
    def allows_pair(pm_title: str, kalshi_title: str) -> bool:
        pm_ex = ExactScoreVsGoalsTotalsValidator.is_exact_score_market(pm_title)
        ks_ex = ExactScoreVsGoalsTotalsValidator.is_exact_score_market(kalshi_title)
        pm_go = ExactScoreVsGoalsTotalsValidator.is_goals_totals_line(pm_title)
        ks_go = ExactScoreVsGoalsTotalsValidator.is_goals_totals_line(kalshi_title)
        return not ((pm_ex and ks_go) or (ks_ex and pm_go))


class EsportsTournamentWinnerVsSportsGoalsValidator:
    """电竞「Will X win 大赛」vs 传统体育进球 / 净胜分 Points 盘。"""

    @staticmethod
    def is_wins_by_points_margin_line(title: str) -> bool:
        return bool(RE_WINS_BY_POINTS.search(title))

    @staticmethod
    def is_will_side_win_event_proposition(title: str) -> bool:
        main = title.split(" - ", 1)[0].strip()
        lm = main.lower()
        if " vs " in lm or " vs." in lm:
            return False
        return bool(RE_WILL_WIN_EVENT.search(main))

    @staticmethod
    def has_esports_series_anchor(title: str) -> bool:
        l = title.lower()
        return any(a in l for a in ESPORTS_SERIES_ANCHORS)

    @staticmethod
    def allows_pair(pm_title: str, kalshi_title: str) -> bool:
        for a, b in ((pm_title, kalshi_title), (kalshi_title, pm_title)):
            if (
                EsportsTournamentWinnerVsSportsGoalsValidator.is_will_side_win_event_proposition(
                    a
                )
                and EsportsTournamentWinnerVsSportsGoalsValidator.has_esports_series_anchor(
                    a
                )
                and (
                    ExactScoreVsGoalsTotalsValidator.is_goals_totals_line(b)
                    or EsportsTournamentWinnerVsSportsGoalsValidator.is_wins_by_points_margin_line(
                        b
                    )
                )
            ):
                return False
        return True


class TournamentOutrightVsMatchValidator:
    @staticmethod
    def is_tournament_outright_winner(title: str) -> bool:
        main = title.split(" - ", 1)[0].strip()
        l = main.lower()
        if " vs " in l or " vs." in l:
            return False
        if not RE_WILL_WIN_THE.search(main):
            return False
        if (
            "win the match" in l
            or "win the game" in l
            or "win map " in l
        ):
            return False
        return (
            " open" in l
            or "open?" in l
            or "wta " in l
            or "atp " in l
            or "masters" in l
            or "grand slam" in l
            or "indian wells" in l
            or "wimbledon" in l
            or "roland" in l
            or "french open" in l
            or "australian open" in l
            or "us open" in l
            or "miami open" in l
        )

    @staticmethod
    def is_event_head_to_head_match(title: str) -> bool:
        if not (" vs " in title or " vs." in title):
            return False
        l = title.lower()
        return (
            "open:" in l
            or "masters:" in l
            or bool(RE_WTA_ATP_PREFIX.search(title))
        )

    @staticmethod
    def allows_pair(pm_title: str, kalshi_title: str) -> bool:
        pm_o = TournamentOutrightVsMatchValidator.is_tournament_outright_winner(
            pm_title
        )
        ks_o = TournamentOutrightVsMatchValidator.is_tournament_outright_winner(
            kalshi_title
        )
        pm_h = TournamentOutrightVsMatchValidator.is_event_head_to_head_match(pm_title)
        ks_h = TournamentOutrightVsMatchValidator.is_event_head_to_head_match(
            kalshi_title
        )
        return not ((pm_h and ks_o) or (ks_h and pm_o))


class TeamSidePropVsMatchWinnerValidator:
    @staticmethod
    def is_top_batter_or_team_scorer_prop(title: str) -> bool:
        l = title.lower()
        return (
            "top batter" in l
            or "top batsman" in l
            or "top bowler" in l
            or ("team top" in l and ("batter" in l or "batsman" in l))
        )

    @staticmethod
    def is_plain_head_to_head_winner(title: str) -> bool:
        l = title.lower()
        if "winner" not in l:
            return False
        if TeamSidePropVsMatchWinnerValidator.is_top_batter_or_team_scorer_prop(title):
            return False
        return " vs " in title or " vs." in title or " at " in l

    @staticmethod
    def allows_pair(pm_title: str, kalshi_title: str) -> bool:
        pm_p = TeamSidePropVsMatchWinnerValidator.is_top_batter_or_team_scorer_prop(
            pm_title
        )
        ks_p = TeamSidePropVsMatchWinnerValidator.is_top_batter_or_team_scorer_prop(
            kalshi_title
        )
        pm_pl = TeamSidePropVsMatchWinnerValidator.is_plain_head_to_head_winner(
            pm_title
        )
        ks_pl = TeamSidePropVsMatchWinnerValidator.is_plain_head_to_head_winner(
            kalshi_title
        )
        return not ((pm_p and ks_pl) or (ks_p and pm_pl))


class EntertainmentChartValidator:
    @staticmethod
    def looks_like_billboard_chart(title: str) -> bool:
        l = title.lower()
        return "billboard" in l or "hot 100" in l or "hot100" in l

    @staticmethod
    def looks_like_spotify_chart(title: str) -> bool:
        return "spotify" in title.lower()

    @staticmethod
    def has_number_one_rank(title: str) -> bool:
        l = title.lower()
        return (
            "#1" in l
            or "# 1" in l
            or "number one" in l
            or "no. 1" in l
            or "no 1 " in l
        )

    @staticmethod
    def has_top_ten_rank(title: str) -> bool:
        l = title.lower()
        return "top 10" in l or "top10" in l

    @staticmethod
    def is_billboard_spotify_cross(pm_title: str, kalshi_title: str) -> bool:
        pm_bb = EntertainmentChartValidator.looks_like_billboard_chart(pm_title)
        ks_bb = EntertainmentChartValidator.looks_like_billboard_chart(kalshi_title)
        pm_sp = EntertainmentChartValidator.looks_like_spotify_chart(pm_title)
        ks_sp = EntertainmentChartValidator.looks_like_spotify_chart(kalshi_title)
        return (pm_sp and ks_bb) or (pm_bb and ks_sp)

    @staticmethod
    def allows_pair(pm_title: str, kalshi_title: str) -> bool:
        pm_bb = EntertainmentChartValidator.looks_like_billboard_chart(pm_title)
        ks_bb = EntertainmentChartValidator.looks_like_billboard_chart(kalshi_title)
        pm_sp = EntertainmentChartValidator.looks_like_spotify_chart(pm_title)
        ks_sp = EntertainmentChartValidator.looks_like_spotify_chart(kalshi_title)
        if (pm_sp and ks_bb) or (pm_bb and ks_sp):
            return False
        if not pm_bb or not ks_bb:
            return True
        pm_one = EntertainmentChartValidator.has_number_one_rank(pm_title)
        ks_one = EntertainmentChartValidator.has_number_one_rank(kalshi_title)
        pm_t10 = EntertainmentChartValidator.has_top_ten_rank(pm_title)
        ks_t10 = EntertainmentChartValidator.has_top_ten_rank(kalshi_title)
        if (pm_one and ks_t10) or (ks_one and pm_t10):
            return False
        return True


class HandicapVsSingleWinnerValidator:
    @staticmethod
    def is_handicap_market(title: str) -> bool:
        lower = title.lower()
        return "handicap" in lower or "让分" in lower or "spread" in lower

    @staticmethod
    def is_moneyline_winner_proposition(title: str) -> bool:
        l = title.lower()
        if " at " in l and ("winner" in l or "Winner" in title):
            return True
        return bool(RE_WIN_THEN_MATCH.search(title))

    @staticmethod
    def handicap_vs_single_winner_match(pm_title: str, kalshi_title: str) -> bool:
        pm_h = HandicapVsSingleWinnerValidator.is_handicap_market(pm_title)
        ks_h = HandicapVsSingleWinnerValidator.is_handicap_market(kalshi_title)
        pm_s = EsportsGameValidator.is_single_map_winner(pm_title)
        ks_s = EsportsGameValidator.is_single_map_winner(kalshi_title)
        if (pm_h and ks_s) or (ks_h and pm_s):
            return False
        pm_ml = HandicapVsSingleWinnerValidator.is_moneyline_winner_proposition(
            pm_title
        )
        ks_ml = HandicapVsSingleWinnerValidator.is_moneyline_winner_proposition(
            kalshi_title
        )
        if (pm_h and ks_ml) or (ks_h and pm_ml):
            return False
        return True


class SportsSingleVsFinalsValidator:
    @staticmethod
    def is_single_game_format(title: str) -> bool:
        lower = title.lower()
        if (" at " in lower) and ("winner" in lower or " - " in lower):
            return True
        return bool(RE_AT_START.match(title.strip()))

    @staticmethod
    def is_finals_format(title: str) -> bool:
        lower = title.lower()
        return (
            "finals" in lower
            or "championship" in lower
            or "conference finals" in lower
        )

    @staticmethod
    def single_vs_finals_match(pm_title: str, kalshi_title: str) -> bool:
        pm_s = SportsSingleVsFinalsValidator.is_single_game_format(pm_title)
        pm_f = SportsSingleVsFinalsValidator.is_finals_format(pm_title)
        ks_s = SportsSingleVsFinalsValidator.is_single_game_format(kalshi_title)
        ks_f = SportsSingleVsFinalsValidator.is_finals_format(kalshi_title)
        if (pm_s and ks_f) or (pm_f and ks_s):
            return False
        return True


class GarbageMarketDetector:
    @staticmethod
    def is_garbage_sports_market(title: str) -> bool:
        lower = title.lower()
        if "o/u" in lower and "rounds" in lower:
            has_matchup = (
                " vs " in lower
                or " vs." in lower
                or " at " in lower
            )
            if not has_matchup:
                return True
        has_garbage = any(kw in lower for kw in GARBAGE_KEYWORDS)
        if has_garbage:
            upper_count = sum(1 for c in title if c.isupper())
            has_specific = (
                " vs " in lower
                or " at " in lower
                or " - " in lower
                or upper_count > 1
            )
            if not has_specific:
                nums = NumberComparator.extract_numbers(title)
                if nums:
                    return True
        return False


class WinnerMarketValidator:
    @staticmethod
    def validate(pm_title: str, kalshi_title: str) -> Optional[Tuple[str, str, bool]]:
        if "o/u" in pm_title.lower():
            return None
        pm_lower = pm_title.lower()
        if (
            "draw" in pm_lower
            or "end in a draw" in pm_lower
            or " tie " in pm_lower
        ):
            return None
        pm_is_winner = " vs " in pm_title or " vs." in pm_title
        ks_is_winner = "Winner" in kalshi_title or " - " in kalshi_title
        if not pm_is_winner or not ks_is_winner:
            return None
        teams = extract_teams(pm_title)
        if not teams:
            return None
        pm_team1, pm_team2 = teams
        ks_winner = extract_winner(kalshi_title)
        if not ks_winner:
            return None
        pair = extract_kalshi_moneyline_pair(kalshi_title)
        if pair:
            k1, k2 = pair
            if not two_team_sets_consistent(pm_team1, pm_team2, k1, k2):
                return None
        else:
            if kalshi_head_to_head_pair_required(kalshi_title):
                return None
        pt1 = strip_team_event_prefix(
            FinalsConsistencyValidator.trim_team_suffix(pm_team1)
        )
        pt2 = strip_team_event_prefix(
            FinalsConsistencyValidator.trim_team_suffix(pm_team2)
        )
        if names_match(pt1, ks_winner):
            return ("YES", "NO", False)
        if names_match(pt2, ks_winner):
            return ("YES", "YES", True)
        return None


def normalize_stat_type(s: str) -> Optional[str]:
    lower = s.lower()
    if "points" in lower and "rebounds" not in lower and "assists" not in lower:
        return "points"
    if "rebounds" in lower:
        return "rebounds"
    if "assists" in lower:
        return "assists"
    if "three" in lower or "threes" in lower:
        return "threes"
    return None


class StatMarketValidator:
    @staticmethod
    def extract_pm_stat(title: str) -> Optional[Tuple[str, float, bool]]:
        lower = title.lower()
        has_ou = "O/U" in title or " over " in lower or " under " in lower
        if not has_ou:
            return None
        stat = normalize_stat_type(title)
        if not stat:
            return None
        num = extract_number(title)
        if num is None:
            return None
        is_over = "under" not in lower
        return (stat, num, is_over)

    @staticmethod
    def extract_ks_stat(title: str) -> Optional[Tuple[str, float, bool]]:
        m = RE_STAT_PLUS.search(title)
        if m:
            num = float(m.group(1))
            word = m.group(2)
            stat = normalize_stat_type(word) or normalize_stat_type(title)
            if stat:
                return (stat, num, True)
        m2 = RE_STAT_MINUS.search(title)
        if m2:
            num = float(m2.group(1))
            word = m2.group(2)
            stat = normalize_stat_type(word) or normalize_stat_type(title)
            if stat:
                return (stat, num, False)
        return None

    @staticmethod
    def is_stat_market_pair(pm_title: str, kalshi_title: str) -> bool:
        return (
            StatMarketValidator.extract_pm_stat(pm_title) is not None
            and StatMarketValidator.extract_ks_stat(kalshi_title) is not None
        )

    @staticmethod
    def validate(pm_title: str, kalshi_title: str) -> Optional[Tuple[str, str, bool]]:
        pm_s = StatMarketValidator.extract_pm_stat(pm_title)
        ks_s = StatMarketValidator.extract_ks_stat(kalshi_title)
        if not pm_s or not ks_s:
            return None
        pm_stat, pm_num, pm_is_over = pm_s
        ks_stat, ks_num, ks_is_plus = ks_s
        if pm_stat != ks_stat:
            return None
        pm_th = math.ceil(pm_num) if pm_is_over else math.floor(pm_num)
        ks_ceil = math.ceil(ks_num)
        ks_floor = math.floor(ks_num)
        if ks_is_plus:
            if pm_is_over:
                if pm_th == ks_ceil:
                    return ("YES", "NO", False)
            else:
                if pm_th + 1 == ks_ceil:
                    return ("YES", "YES", True)
        else:
            if not pm_is_over:
                if pm_th == ks_floor:
                    return ("YES", "NO", False)
            else:
                if pm_th == ks_floor + 1:
                    return ("YES", "YES", True)
        return None


class ScoreMarketValidator:
    @staticmethod
    def validate(pm_title: str, kalshi_title: str) -> Optional[Tuple[str, str, bool]]:
        if StatMarketValidator.is_stat_market_pair(pm_title, kalshi_title):
            return None
        pm_is_score = "O/U" in pm_title or "Points" in pm_title
        ks_is_score = (
            "+" in kalshi_title
            or "-" in kalshi_title
            or "points" in kalshi_title.lower()
        )
        if not pm_is_score or not ks_is_score:
            return None
        pm_num = extract_number(pm_title)
        ks_num = extract_number(kalshi_title)
        if pm_num is None or ks_num is None:
            return None
        pm_is_over = "under" not in pm_title.lower()
        ks_is_plus = bool(RE_KS_PLUS_BOUND.search(kalshi_title))
        ks_is_minus = bool(RE_KS_MINUS_BOUND.search(kalshi_title))
        pm_th = math.ceil(pm_num) if pm_is_over else math.floor(pm_num)
        ks_ceil = math.ceil(ks_num)
        ks_floor = math.floor(ks_num)
        if ks_is_plus:
            if pm_is_over:
                if pm_th == ks_ceil:
                    return ("YES", "NO", False)
            else:
                if pm_th + 1 == ks_ceil:
                    return ("YES", "YES", True)
        elif ks_is_minus:
            if not pm_is_over:
                if pm_th == ks_floor:
                    return ("YES", "NO", False)
            else:
                if pm_th == ks_floor + 1:
                    return ("YES", "YES", True)
        else:
            ks_th = ks_ceil if pm_is_over else ks_floor
            if pm_th == ks_th:
                return ("YES", "NO", False)
        return None


class DateValidator:
    def __init__(self) -> None:
        pass

    @staticmethod
    def extract_date(text: str) -> Optional[DateInfo]:
        m = RE_MONTH_DAY.search(text)
        if m:
            month_name = m.group(1)
            day = int(m.group(2))
            month = None
            mn = month_name.lower()
            for name, mm in MONTH_MAP:
                if name in mn:
                    month = mm
                    break
            if month is None:
                return None
            year = int(m.group(3)) if m.group(3) else None
            return DateInfo(
                month=month,
                day=day,
                has_year=year is not None,
                year=year,
            )
        ym = RE_YEAR.search(text)
        if ym:
            return DateInfo(month=0, day=0, has_year=True, year=int(ym.group(1)))
        return None

    @staticmethod
    def has_safe_word(text: str) -> bool:
        tl = text.lower()
        return any(w in tl for w in SAFE_WORDS)

    @staticmethod
    def dates_match(d1: DateInfo, d2: DateInfo) -> bool:
        if d1.month > 0 and d1.day > 0 and d2.month > 0 and d2.day > 0:
            return d1.month == d2.month and d1.day == d2.day
        return False

    def validate(self, pm_title: str, kalshi_title: str) -> bool:
        pm_d = self.extract_date(pm_title)
        ks_d = self.extract_date(kalshi_title)
        if pm_d and ks_d:
            return self.dates_match(pm_d, ks_d)
        if pm_d and not ks_d:
            return self.has_safe_word(pm_title)
        if not pm_d and ks_d:
            return self.has_safe_word(kalshi_title)
        return True


class SportsIdentifier:
    @staticmethod
    def is_sports_market(title: str) -> bool:
        tl = title.lower()
        return any(kw in tl for kw in SPORTS_KEYWORDS)


class NumberComparator:
    @staticmethod
    def extract_numbers(text: str) -> List[NumberInfo]:
        out: List[NumberInfo] = []
        for m in RE_EXTRACT_NUMBER.finditer(text):
            try:
                value = float(m.group(1))
            except ValueError:
                continue
            is_year = 2000.0 <= value < 2100.0
            out.append(NumberInfo(value=value, context=text, is_year=is_year))
        return out

    @staticmethod
    def compare_numbers(nums1: List[NumberInfo], nums2: List[NumberInfo]) -> bool:
        e1 = len(nums1) == 0
        e2 = len(nums2) == 0
        if e1 != e2:
            return False
        if e1 and e2:
            return True
        for n1 in nums1:
            for n2 in nums2:
                if n1.is_year != n2.is_year:
                    continue
                if abs(n1.value - n2.value) <= 0.5:
                    return True
        return False


class ElectoralPropositionValidator:
    @staticmethod
    def looks_political_election_context(l: str) -> bool:
        return (
            "house seat" in l
            or "u.s. house" in l
            or "us house" in l
            or (
                "senate" in l
                and ("seat" in l or "race" in l or "election" in l)
            )
            or "congressional district" in l
            or "congressional " in l
            or "special election" in l
            or "governor" in l
            or "mayor" in l
            or "presidential" in l
            or "primary" in l
            or "nominee" in l
            or "nomination" in l
            or "democratic party" in l
            or "republican party" in l
            or "the gop" in l
            or " gop " in l
            or bool(ELECTORAL_STATE_DISTRICT_RE.search(l))
        )

    @staticmethod
    def is_party_wins_seat_proposition(l: str) -> bool:
        if "nominee" in l or "nomination" in l:
            return False
        has_party = (
            "democratic party" in l
            or "republican party" in l
            or "the gop" in l
            or " gop " in l
        )
        has_seat = (
            "house seat" in l
            or "congressional" in l
            or ("senate" in l and "seat" in l)
        )
        has_win = "win" in l
        return has_party and has_seat and has_win

    @staticmethod
    def is_candidate_nominee_proposition(l: str) -> bool:
        return (
            "nominee" in l
            or "nomination for" in l
            or " nomination" in l
        )

    @staticmethod
    def has_explicit_placement_or_rank(l: str) -> bool:
        if (
            "finish 2nd" in l
            or "finish second" in l
            or "finishes 2nd" in l
            or "finishes second" in l
            or "2nd place" in l
            or "second place" in l
            or "finish 3rd" in l
            or "finish third" in l
            or "3rd place" in l
            or "third place" in l
            or "runner-up" in l
            or "runner up" in l
            or "comes in second" in l
            or "come in second" in l
        ):
            return True
        return bool(ELECTORAL_NTH_PLACE_RE.search(l))

    @staticmethod
    def allows_pair(pm_title: str, kalshi_title: str) -> bool:
        pm_l = pm_title.lower()
        ks_l = kalshi_title.lower()
        if not ElectoralPropositionValidator.looks_political_election_context(
            pm_l
        ) or not ElectoralPropositionValidator.looks_political_election_context(ks_l):
            return True
        pm_ps = ElectoralPropositionValidator.is_party_wins_seat_proposition(pm_l)
        ks_ps = ElectoralPropositionValidator.is_party_wins_seat_proposition(ks_l)
        pm_nom = ElectoralPropositionValidator.is_candidate_nominee_proposition(pm_l)
        ks_nom = ElectoralPropositionValidator.is_candidate_nominee_proposition(ks_l)
        if (pm_ps and ks_nom) or (ks_ps and pm_nom):
            return False
        pm_r = ElectoralPropositionValidator.has_explicit_placement_or_rank(pm_l)
        ks_r = ElectoralPropositionValidator.has_explicit_placement_or_rank(ks_l)
        if pm_r != ks_r:
            return False
        return True


class ValidationPipeline:
    def __init__(self) -> None:
        self.date_validator = DateValidator()
        self.filtered_count: int = 0
        self.filtered_samples: List[Tuple[str, str, str]] = []
        self.retained_samples: Dict[str, List[RetainedSample]] = {}

    def validate(
        self,
        pm_title: str,
        kalshi_title: str,
        similarity: float,
        category: str,
    ) -> Optional[MatchInfo]:
        if GarbageMarketDetector.is_garbage_sports_market(
            pm_title
        ) or GarbageMarketDetector.is_garbage_sports_market(kalshi_title):
            self._record_filter(pm_title, kalshi_title, "垃圾市场")
            return None
        if not self.date_validator.validate(pm_title, kalshi_title):
            self._record_filter(pm_title, kalshi_title, "日期不匹配")
            return None
        if not WeatherValidator.regions_match(pm_title, kalshi_title):
            self._record_filter(pm_title, kalshi_title, "天气地区不匹配")
            return None
        if not WeatherValidator.fahrenheit_or_below_buckets_match(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title, kalshi_title, "温度档位(°For below)不一致"
            )
            return None
        if EntertainmentChartValidator.is_billboard_spotify_cross(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title,
                kalshi_title,
                "娱乐榜单来源不一致(Billboard与Spotify)",
            )
            return None
        if not EntertainmentChartValidator.allows_pair(pm_title, kalshi_title):
            self._record_filter(
                pm_title, kalshi_title, "娱乐榜单#1与Top10不能匹配"
            )
            return None
        if not EsportsGameValidator.game_numbers_match(pm_title, kalshi_title):
            self._record_filter(pm_title, kalshi_title, "电竞局数不匹配")
            return None
        if not EsportsGameValidator.single_vs_total_match(pm_title, kalshi_title):
            self._record_filter(
                pm_title, kalshi_title, "电竞单局与总局数不能匹配"
            )
            return None
        if not EsportsGameValidator.handicap_vs_total_maps_match(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title, kalshi_title, "让分盘与总局maps盘不能匹配"
            )
            return None
        if not EsportsGameValidator.single_vs_series_match(pm_title, kalshi_title):
            self._record_filter(
                pm_title, kalshi_title, "电竞单局与BO5/系列赛不能匹配"
            )
            return None
        if not EsportsGameValidator.map_winner_vs_whole_match_match(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title, kalshi_title, "电竞Map局胜者与整场赛果不能匹配"
            )
            return None
        if not SportsSingleVsFinalsValidator.single_vs_finals_match(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title, kalshi_title, "体育单场与决赛不能匹配"
            )
            return None
        if not HandicapVsSingleWinnerValidator.handicap_vs_single_winner_match(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title, kalshi_title, "让分盘口与某局胜者不能匹配"
            )
            return None
        if not ExactScoreVsGoalsTotalsValidator.allows_pair(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title, kalshi_title, "确切比分与总进球数不能匹配"
            )
            return None
        if not EsportsTournamentWinnerVsSportsGoalsValidator.allows_pair(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title,
                kalshi_title,
                "电竞赛事夺冠命题与体育进球Totals/净胜分Points盘不能匹配",
            )
            return None
        if not TournamentOutrightVsMatchValidator.allows_pair(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title, kalshi_title, "公开赛总冠军与单场对阵不能匹配"
            )
            return None
        if not TeamSidePropVsMatchWinnerValidator.allows_pair(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title,
                kalshi_title,
                "队内最佳击球员等与全场胜负盘不能匹配",
            )
            return None
        if not FinalsConsistencyValidator.finals_consistency_match(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title,
                kalshi_title,
                "决赛不一致（一方有Finals另一方无且非同一两队）",
            )
            return None
        if not DrawVsWinnerValidator.allows_pair(pm_title, kalshi_title):
            self._record_filter(
                pm_title, kalshi_title, "平局市场与胜负盘不能匹配"
            )
            return None
        if not BracketAdvanceVsSingleGameValidator.allows_pair(
            pm_title, kalshi_title
        ):
            self._record_filter(
                pm_title, kalshi_title, "锦标赛晋级命题与单场胜负不能匹配"
            )
            return None
        if not TossVsMatchMarketValidator.allows_pair(pm_title, kalshi_title):
            self._record_filter(
                pm_title, kalshi_title, "抛硬币/掷币与赛果命题不一致"
            )
            return None
        if not ElectoralPropositionValidator.allows_pair(pm_title, kalshi_title):
            self._record_filter(
                pm_title,
                kalshi_title,
                "选举命题类型不一致(党席/提名或名次/获胜)",
            )
            return None

        wr = WinnerMarketValidator.validate(pm_title, kalshi_title)
        if wr:
            pm_side, kalshi_side, inv = wr
            mi = MatchInfo(
                pm_title=pm_title,
                kalshi_title=kalshi_title,
                similarity=similarity,
                category=category,
                pm_side=pm_side,
                kalshi_side=kalshi_side,
                needs_inversion=inv,
            )
            self._record_retained(mi)
            return mi

        sr = ScoreMarketValidator.validate(pm_title, kalshi_title)
        if sr:
            pm_side, kalshi_side, inv = sr
            mi = MatchInfo(
                pm_title=pm_title,
                kalshi_title=kalshi_title,
                similarity=similarity,
                category=category,
                pm_side=pm_side,
                kalshi_side=kalshi_side,
                needs_inversion=inv,
            )
            self._record_retained(mi)
            return mi

        if StatMarketValidator.is_stat_market_pair(pm_title, kalshi_title):
            tr = StatMarketValidator.validate(pm_title, kalshi_title)
            if tr:
                pm_side, kalshi_side, inv = tr
                mi = MatchInfo(
                    pm_title=pm_title,
                    kalshi_title=kalshi_title,
                    similarity=similarity,
                    category=category,
                    pm_side=pm_side,
                    kalshi_side=kalshi_side,
                    needs_inversion=inv,
                )
                self._record_retained(mi)
                return mi
            self._record_filter(
                pm_title, kalshi_title, "技术统计类型或阈值不匹配"
            )
            return None

        pm_map = EsportsGameValidator.is_single_map_winner(
            pm_title
        ) or EsportsGameValidator.is_single_game_winner(pm_title)
        ks_map = EsportsGameValidator.is_single_map_winner(kalshi_title)
        if pm_map and ks_map:
            self._record_filter(
                pm_title, kalshi_title, "电竞Map局胜者须通过胜负与两队校验"
            )
            return None

        pm_nums = NumberComparator.extract_numbers(pm_title)
        ks_nums = NumberComparator.extract_numbers(kalshi_title)
        if not pm_nums and not ks_nums:
            self._record_filter(
                pm_title, kalshi_title, "默认路径需至少一侧有锚点数字"
            )
            return None
        if not NumberComparator.compare_numbers(pm_nums, ks_nums):
            self._record_filter(pm_title, kalshi_title, "数值不匹配")
            return None

        mi = MatchInfo(
            pm_title=pm_title,
            kalshi_title=kalshi_title,
            similarity=similarity,
            category=category,
            pm_side="YES",
            kalshi_side="NO",
            needs_inversion=False,
        )
        self._record_retained(mi)
        return mi

    def _record_filter(self, pm: str, ks: str, reason: str) -> None:
        self.filtered_count += 1
        if self.filtered_count <= 3:
            self.filtered_samples.append((pm, ks, reason))
            print(f"\n         🔍 二筛过滤 #{self.filtered_count} [{reason}]:")
            print(f"            PM: {pm}")
            print(f"            Kalshi: {ks}")

    def _record_retained(self, info: MatchInfo) -> None:
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
        self.filtered_count = 0
        self.filtered_samples.clear()
        self.retained_samples.clear()

    def print_retained_samples(self) -> None:
        print("\n📊 二筛后各类别最高分样本 (每个类别最多3个):")
        categories = sorted(self.retained_samples.keys())
        for category in categories[:5]:
            samples = self.retained_samples[category]
            sorted_s = sorted(samples, key=lambda x: x.similarity, reverse=True)
            print(f"\n  类别 [{category}]: {len(samples)} 个留存")
            for i, sample in enumerate(sorted_s[:3]):
                inv = " [Y/N颠倒]" if sample.needs_inversion else ""
                print(f"    {i + 1}. 相似度: {sample.similarity:.3f}{inv}")
                print(f"       PM {sample.pm_side}: {sample.pm_title}")
                print(f"       Kalshi {sample.kalshi_side}: {sample.kalshi_title}")
            if len(samples) > 3:
                print(f"       ... 还有 {len(samples) - 3} 个")
        if len(categories) > 5:
            print(f"   ... 以及其他 {len(categories) - 5} 个类别")


def _validation_smoke_tests() -> None:
    """与 Rust `validation.rs` 中关键用例对齐的烟测。"""
    pm = "Will Cloud9 New York win DreamHack Major 2?"
    ks = "New York R wins by over 2.5 goals? - New York R wins by over 2.5 goals"
    assert not EsportsTournamentWinnerVsSportsGoalsValidator.allows_pair(pm, ks)
    pipe = ValidationPipeline()
    # Windows 控制台常为 GBK，_record_filter 含 emoji 会 UnicodeEncodeError
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        assert pipe.validate(pm, ks, 0.95, "esports") is None
    assert EsportsTournamentWinnerVsSportsGoalsValidator.allows_pair(
        "Will FaZe win IEM Cologne?",
        "FaZe vs NaVi Winner? - FaZe",
    )
    assert EsportsTournamentWinnerVsSportsGoalsValidator.allows_pair(
        "Will Arsenal win the Premier League?",
        "Arsenal vs Chelsea: Over 2.5 goals?",
    )
    print("validation.py smoke tests OK")


if __name__ == "__main__":
    _validation_smoke_tests()
