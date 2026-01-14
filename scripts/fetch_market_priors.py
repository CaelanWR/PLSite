#!/usr/bin/env python3
"""
Fetch market priors from Kalshi + Polymarket and append snapshots to data/market_priors.json.

Notes:
  - Kalshi requires credentials (env vars or flags).
  - Polymarket is public, no auth required.
  - Snapshots are appended and trimmed to the configured retention window.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from fetch_kalshi_impact import (  # type: ignore
    KALSHI_BASE_URL,
    _fetch_candles,
    _fetch_markets,
    _market_close_ts,
    _parse_strike,
    _select_payroll_markets_for_payroll_month,
)
from fetch_polymarket_impact import (  # type: ignore
    DEFAULT_CLOB_URL,
    DEFAULT_GAMMA_URL,
    _fetch_cached_json as _fetch_poly_cached_json,
    _fetch_token_price_history_chunked,
    _parse_iso_to_ts,
)
from impact_common import _first_weekday_of_month, _prev_month, _to_utc_ts, _ym_string  # type: ignore

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_OUT = BASE_DIR / "data/market_priors.json"
DEFAULT_CONFIG = BASE_DIR / "data/market_priors_config.json"


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def _parse_ts(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        return int(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(parsed.timestamp())
    except ValueError:
        return None


def _iso_from_ts(ts: int) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_fixed_offset(value: str) -> dt.tzinfo:
    match = re.match(r"^([+-])(\d{2}):(\d{2})$", value.strip())
    if not match:
        raise ValueError(f"Invalid UTC offset (+HH:MM): {value!r}")
    sign = -1 if match.group(1) == "-" else 1
    hours = int(match.group(2))
    minutes = int(match.group(3))
    return dt.timezone(sign * dt.timedelta(hours=hours, minutes=minutes))


def _date_in_tz(ts: int, tz_name: str, fallback_offset: str) -> dt.date:
    base = dt.datetime.fromtimestamp(ts, dt.timezone.utc)
    try:
        from zoneinfo import ZoneInfo  # type: ignore

        return base.astimezone(ZoneInfo(tz_name)).date()
    except Exception:
        return base.astimezone(_parse_fixed_offset(fallback_offset)).date()


def _nfp_release_ts_for_month(year: int, month: int) -> int:
    jobs_date = _first_weekday_of_month(year, month, weekday=4)  # Friday
    if month == 1 and jobs_date.day <= 3:
        jobs_date = jobs_date + dt.timedelta(days=7)
    return _to_utc_ts(jobs_date, "08:30", "America/New_York", "-05:00")


def _now_iso() -> str:
    return _iso_from_ts(int(time.time()))


def _release_month_from_ts(ts: int) -> Optional[str]:
    try:
        release_dt = dt.datetime.fromtimestamp(ts, dt.timezone.utc)
    except Exception:
        return None
    return _ym_string(release_dt.year, release_dt.month)


def _payroll_month_from_release_ts(ts: int) -> Optional[str]:
    release_month = _release_month_from_ts(ts)
    if not release_month:
        return None
    try:
        year, month = [int(part) for part in release_month.split("-")]
    except Exception:
        return None
    py, pm = _prev_month(year, month)
    return _ym_string(py, pm)


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_json_arrayish(value: Any) -> Optional[List[Any]]:
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            parsed = json.loads(s)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, list) else None
    return None


def _parse_numeric(value: str) -> Optional[float]:
    s = value.strip().lower().replace(",", "").replace("%", "")
    if not s:
        return None
    match = re.match(r"^(-?\d+(?:\.\d+)?)(k|m)?$", s)
    if not match:
        return None
    num = float(match.group(1))
    unit = match.group(2)
    if unit == "k":
        num *= 1000.0
    elif unit == "m":
        num *= 1_000_000.0
    return num


def _parse_bracket_label(label: str) -> Optional[Dict[str, Any]]:
    s = str(label or "").strip()
    if not s:
        return None
    raw_lower = s.lower()
    if "no change" in raw_lower or "nochange" in raw_lower:
        return {"kind": "point", "value": 0.0}
    if "bps" in raw_lower or "basis points" in raw_lower:
        match = re.search(r"(-?\d+(?:\.\d+)?)", raw_lower)
        if match:
            bps = _to_float(match.group(1))
            if bps is not None:
                value = bps / 100.0
                if any(word in raw_lower for word in ("decrease", "cut", "down")):
                    value = -abs(value)
                elif any(word in raw_lower for word in ("increase", "hike", "up")):
                    value = abs(value)
                has_plus = any(token in raw_lower for token in ("+", "plus", "or more", "more than"))
                if has_plus:
                    if value < 0:
                        return {"kind": "range", "lower": None, "upper": value}
                    return {"kind": "range", "lower": value, "upper": None}
                return {"kind": "point", "value": value}

    lowered = (
        raw_lower.replace(" ", "")
        .replace("–", "-")
        .replace("—", "-")
        .replace("−", "-")
        .replace("≤", "<=")
        .replace("≥", ">=")
    )

    match = re.match(r"^(?:<=|<)\s*([0-9][0-9,.\-]*)(k|m)?%?$", lowered)
    if match:
        upper = _parse_numeric(match.group(1) + (match.group(2) or ""))
        if upper is None:
            return None
        return {"kind": "range", "lower": None, "upper": upper}

    match = re.match(r"^(?:>=|>)\s*([0-9][0-9,.\-]*)(k|m)?%?\+?$", lowered)
    if match:
        lower = _parse_numeric(match.group(1) + (match.group(2) or ""))
        if lower is None:
            return None
        return {"kind": "range", "lower": lower, "upper": None}

    match = re.match(r"^([0-9][0-9,.\-]*)(k|m)?\+$", lowered)
    if match:
        lower = _parse_numeric(match.group(1) + (match.group(2) or ""))
        if lower is None:
            return None
        return {"kind": "range", "lower": lower, "upper": None}

    match = re.match(r"^([0-9][0-9,.\-]*)(k|m)?-([0-9][0-9,.\-]*)(k|m)?$", lowered)
    if match:
        lo_unit = match.group(2) or match.group(4) or ""
        hi_unit = match.group(4) or match.group(2) or ""
        lower = _parse_numeric(match.group(1) + lo_unit)
        upper = _parse_numeric(match.group(3) + hi_unit)
        if lower is None or upper is None:
            return None
        return {"kind": "range", "lower": lower, "upper": upper}

    match = re.match(r"^([0-9][0-9,.\-]*)(k|m)?%?$", lowered)
    if match:
        value = _parse_numeric(match.group(1) + (match.group(2) or ""))
        if value is None:
            return None
        return {"kind": "point", "value": value}

    return None


def _kalshi_market_volume(market: Dict[str, Any]) -> Optional[float]:
    for key in ("volume_dollars", "volume_usd", "volumeUSD", "volumeUsd", "volume_notional", "dollar_volume"):
        value = _to_float(market.get(key))
        if value is not None:
            return value

    volume = None
    for key in ("volume", "volume_24h", "open_interest"):
        value = _to_float(market.get(key))
        if value is not None:
            volume = value
            break
    if volume is None:
        return None

    notional = _to_float(market.get("notional_value_dollars"))
    if notional is None:
        notional = _to_float(market.get("notional_value"))
    if notional is not None:
        return volume * notional

    last_price = _to_float(market.get("last_price_dollars"))
    if last_price is None:
        last_price_raw = _to_float(market.get("last_price"))
        last_price = last_price_raw / 100.0 if last_price_raw is not None else None
    if last_price is not None:
        return volume * last_price

    return volume


def _sum_kalshi_volume(markets: List[Dict[str, Any]]) -> Optional[float]:
    total = 0.0
    seen = False
    for market in markets:
        vol = _kalshi_market_volume(market)
        if vol is None:
            continue
        total += vol
        seen = True
    return total if seen else None


def _sum_kalshi_notional_from_candles(
    candles: List[List[Optional[float]]],
    start_ts: int,
    end_ts: int,
) -> Optional[float]:
    total = 0.0
    seen = False
    for ts, yes, vol in candles:
        if ts is None or vol is None or yes is None:
            continue
        if ts < start_ts:
            continue
        if ts >= end_ts:
            break
        total += vol * (yes / 100.0)
        seen = True
    return total if seen else None


def _sum_kalshi_notional_volume(
    *,
    markets: List[Dict[str, Any]],
    series_ticker: str,
    auth: Dict[str, Any],
    base_url: str,
    cache_dir: Path,
    window_days: int,
    interval_minutes: int,
    end_ts: int,
) -> Optional[float]:
    if window_days <= 0:
        return None
    start_ts = end_ts - window_days * 86400
    total = 0.0
    seen = False
    for market in markets:
        ticker = str(market.get("ticker") or "").strip()
        if not ticker:
            continue
        candles = _fetch_candles(
            base_url=base_url,
            auth=auth,
            series_ticker=series_ticker,
            market_ticker=ticker,
            start_ts=start_ts,
            end_ts=end_ts,
            interval=f"{interval_minutes}m",
            cache_dir=cache_dir,
        )
        if not candles:
            continue
        vol = _sum_kalshi_notional_from_candles(candles, start_ts, end_ts)
        if vol is None:
            continue
        total += vol
        seen = True
    return total if seen else None


def _polymarket_volume_keys(window_days: Optional[int]) -> Tuple[str, ...]:
    if window_days:
        if window_days <= 7:
            return ("volume1wk", "volume1wkClob", "volumeNum", "volumeClob", "volume")
        if window_days <= 30:
            return ("volume1mo", "volume1moClob", "volumeNum", "volumeClob", "volume")
        if window_days <= 365:
            return ("volume1yr", "volume1yrClob", "volumeNum", "volumeClob", "volume")
    return ("volumeNum", "volumeClob", "volume")


def _polymarket_market_volume(market: Dict[str, Any], window_days: Optional[int] = None) -> Optional[float]:
    for key in _polymarket_volume_keys(window_days):
        value = _to_float(market.get(key))
        if value is not None:
            return value
    return None


def _sum_polymarket_volume(event: Dict[str, Any], window_days: Optional[int] = None) -> Optional[float]:
    markets = event.get("markets") if isinstance(event.get("markets"), list) else []
    total = 0.0
    seen = False
    for market in markets:
        if not isinstance(market, dict):
            continue
        vol = _polymarket_market_volume(market, window_days)
        if vol is None:
            continue
        total += vol
        seen = True
    return total if seen else None


def _pick_yes_token_id(market: Dict[str, Any]) -> Optional[str]:
    outcomes_raw = _parse_json_arrayish(market.get("outcomes"))
    tokens_raw = _parse_json_arrayish(market.get("clobTokenIds")) or _parse_json_arrayish(market.get("clobTokenIDs"))
    outcomes = [str(o) for o in (outcomes_raw or [])]
    tokens = [str(t) for t in (tokens_raw or [])]
    if outcomes and len(outcomes) == len(tokens):
        for i, out in enumerate(outcomes):
            if out.strip().lower() == "yes":
                return tokens[i]
    return tokens[0] if tokens else None


def _default_point_width(value: float) -> float:
    abs_val = abs(value)
    if abs_val <= 10:
        return 0.1
    if abs_val <= 100:
        return 1.0
    if abs_val <= 1000:
        return 10.0
    if abs_val <= 10000:
        return 100.0
    return 10000.0


def _extract_bracket_markets_from_event(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    markets = event.get("markets") if isinstance(event.get("markets"), list) else []
    out: List[Dict[str, Any]] = []
    points: List[Tuple[str, str, float]] = []
    for m in markets:
        if not isinstance(m, dict):
            continue
        token_id = _pick_yes_token_id(m)
        if not token_id:
            continue
        label = str(m.get("groupItemTitle") or m.get("question") or "").strip()
        if not label:
            label = str(m.get("slug") or token_id)
        strike = _parse_bracket_label(label) or {"kind": "range", "lower": None, "upper": None}
        if strike.get("kind") == "point":
            value = _to_float(strike.get("value"))
            if value is None:
                continue
            points.append((label, str(token_id), value))
            continue
        lower = strike.get("lower")
        upper = strike.get("upper")
        if lower is None and upper is None:
            continue
        out.append({"label": label, "token_id": str(token_id), "strike": strike})

    if points:
        values = sorted({p[2] for p in points})
        if len(values) >= 2:
            gaps = [b - a for a, b in zip(values, values[1:]) if b - a > 0]
            gaps.sort()
            step = gaps[len(gaps) // 2] if gaps else None
        else:
            step = None
        for label, token_id, value in points:
            width = step if step and step > 0 else _default_point_width(value)
            lower = value - width / 2.0
            upper = value + width / 2.0
            out.append(
                {
                    "label": label,
                    "token_id": token_id,
                    "strike": {"kind": "range", "lower": lower, "upper": upper},
                }
            )
    return out


def _kalshi_auth_from_env(
    *,
    token: Optional[str],
    key_id: Optional[str],
    private_key_path: Optional[str],
) -> Optional[Dict[str, Any]]:
    if token:
        return {"kind": "bearer", "token": token}
    token_env = (os.environ.get("KALSHI_TOKEN") or "").strip()
    if token_env:
        return {"kind": "bearer", "token": token_env}

    key_id_val = (key_id or os.environ.get("KALSHI_KEY_ID") or "").strip()
    key_path_val = (private_key_path or os.environ.get("KALSHI_PRIVATE_KEY_PATH") or "").strip()
    if key_id_val and key_path_val:
        return {"kind": "key", "key_id": key_id_val, "private_key_path": key_path_val}
    return None


def _kalshi_event_ticker(market: Dict[str, Any]) -> str:
    event_ticker = str(market.get("event_ticker") or "").strip()
    if event_ticker:
        return event_ticker
    ticker = str(market.get("ticker") or "")
    if "-T" in ticker:
        return ticker.split("-T")[0]
    return ""


def _kalshi_yes_price(market: Dict[str, Any]) -> Optional[float]:
    yes_bid = _to_float(market.get("yes_bid"))
    yes_ask = _to_float(market.get("yes_ask"))
    last_price = _to_float(market.get("last_price"))
    if yes_bid is not None and yes_ask is not None:
        return (yes_bid + yes_ask) / 2.0
    if last_price is not None:
        return last_price
    if yes_ask is not None:
        return yes_ask
    if yes_bid is not None:
        return yes_bid
    return None


def _kalshi_strike_value(market: Dict[str, Any]) -> Optional[float]:
    custom = market.get("custom_strike")
    if isinstance(custom, dict) and custom:
        for raw_key, raw_value in custom.items():
            label = str(raw_key or "").strip().lower()
            value_text = str(raw_value or "").strip().lower()
            if value_text in ("no change", "unchanged"):
                return 0.0
            match = re.search(r"-?\d+(?:\.\d+)?", value_text)
            if not match:
                continue
            magnitude = _to_float(match.group(0))
            if magnitude is None or not math.isfinite(magnitude):
                continue
            if value_text.startswith(">"):
                magnitude *= 2.0
            elif value_text.startswith("<"):
                magnitude *= 0.5
            if "cut" in label or "decrease" in label:
                return -abs(magnitude)
            if "hike" in label or "increase" in label:
                return abs(magnitude)
            return magnitude
    for key in ("floor_strike", "strike", "strike_value"):
        value = _to_float(market.get(key))
        if value is not None and math.isfinite(value):
            return value
    title = str(market.get("title") or "")
    strike = _parse_strike(title)
    if strike and strike.get("value") is not None:
        value = _to_float(strike.get("value"))
        if value is not None and math.isfinite(value):
            return value
    return None


def _kalshi_pick_event_markets(
    markets: List[Dict[str, Any]],
    *,
    event_ticker: Optional[str],
    target_ts: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    if event_ticker:
        chosen = [m for m in markets if _kalshi_event_ticker(m) == event_ticker]
        close_ts = min((_market_close_ts(m) or 0) for m in chosen) if chosen else None
        return chosen, close_ts if close_ts and close_ts > 0 else None

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for market in markets:
        key = _kalshi_event_ticker(market)
        if not key:
            continue
        grouped.setdefault(key, []).append(market)

    now = int(time.time())
    candidates: List[Tuple[int, str]] = []
    for key, group in grouped.items():
        close_times = [_market_close_ts(m) for m in group]
        close_times = [c for c in close_times if isinstance(c, int) and c > 0]
        if not close_times:
            continue
        soonest = min(close_times)
        candidates.append((soonest, key))
    if not candidates:
        return [], None

    if target_ts:
        _, selected = sorted(candidates, key=lambda x: abs(x[0] - target_ts))[0]
    else:
        future = [c for c in candidates if c[0] >= now]
        if future:
            _, selected = sorted(future, key=lambda x: x[0])[0]
        else:
            _, selected = sorted(candidates, key=lambda x: x[0])[-1]
    selected_markets = grouped.get(selected, [])
    close_ts = min((_market_close_ts(m) or 0) for m in selected_markets) if selected_markets else None
    return selected_markets, close_ts if close_ts and close_ts > 0 else None


def _kalshi_ranges_from_markets(
    markets: List[Dict[str, Any]],
    *,
    scale: float = 1.0,
    bucket_width: Optional[float] = None,
) -> List[Dict[str, Any]]:
    points: List[Tuple[float, float]] = []
    custom_mode = False
    for market in markets:
        value = _kalshi_strike_value(market)
        prob = _kalshi_yes_price(market)
        if value is None or prob is None:
            continue
        if not isinstance(value, (int, float)) or not math.isfinite(value):
            continue
        prob = max(0.0, min(1.0, float(prob) / 100.0))
        strike_type = str(market.get("strike_type") or "").strip().lower()
        if strike_type == "custom" or isinstance(market.get("custom_strike"), dict):
            custom_mode = True
        if strike_type in ("less", "below", "lt"):
            prob = max(0.0, min(1.0, 1.0 - prob))
        points.append((float(value) * scale, prob))

    if not points:
        return []
    if custom_mode:
        values = sorted({v for v, _ in points})
        diffs = [b - a for a, b in zip(values, values[1:]) if b > a]
        width = bucket_width
        if width is None:
            width = min(diffs) if diffs else 0.25
        half = width / 2.0
        ranges: List[Dict[str, Any]] = []
        for value, prob in points:
            ranges.append({"lower": value - half, "upper": value + half, "prob": prob})
        return ranges
    return _kalshi_ranges_from_points(points)


def _kalshi_ranges_from_points(points: List[Tuple[float, float]]) -> List[Dict[str, Any]]:
    points = [(float(strike), max(0.0, min(1.0, float(prob)))) for strike, prob in points]
    points.sort(key=lambda x: x[0])
    if not points:
        return []

    ranges: List[Dict[str, Any]] = []
    if len(points) == 1:
        strike, p_above = points[0]
        ranges.append({"lower": None, "upper": strike, "prob": max(0.0, 1.0 - p_above)})
        ranges.append({"lower": strike, "upper": None, "prob": p_above})
        return ranges

    first_strike, first_p = points[0]
    ranges.append({"lower": None, "upper": first_strike, "prob": max(0.0, 1.0 - first_p)})
    for i in range(len(points) - 1):
        lo, p_lo = points[i]
        hi, p_hi = points[i + 1]
        ranges.append({"lower": lo, "upper": hi, "prob": max(0.0, p_lo - p_hi)})
    last_strike, last_p = points[-1]
    ranges.append({"lower": last_strike, "upper": None, "prob": max(0.0, last_p)})
    return ranges


def _ranges_from_custom_points(points: List[Tuple[float, float]], width: float) -> List[Dict[str, Any]]:
    if not points:
        return []
    clean_width = width if isinstance(width, (int, float)) and math.isfinite(width) and width > 0 else 0.25
    half = clean_width / 2.0
    ranges: List[Dict[str, Any]] = []
    for strike, prob in points:
        ranges.append({"lower": strike - half, "upper": strike + half, "prob": max(0.0, min(1.0, prob))})
    return ranges


def _expected_from_ranges(ranges: List[Dict[str, Any]], default_width: float) -> Optional[float]:
    total = 0.0
    weight = 0.0
    for r in ranges:
        lower = _to_float(r.get("lower"))
        upper = _to_float(r.get("upper"))
        prob = _to_float(r.get("prob"))
        if prob is None:
            continue
        if lower is None and upper is None:
            continue
        if lower is None:
            midpoint = upper - default_width / 2.0
        elif upper is None:
            midpoint = lower + default_width / 2.0
        else:
            midpoint = (lower + upper) / 2.0
        total += prob * midpoint
        weight += prob
    if weight <= 0:
        return None
    return total / weight


def _normalize_ranges(ranges: List[Dict[str, Any]], default_width: float) -> List[Tuple[float, float, float]]:
    cleaned: List[Tuple[float, float, float]] = []
    for r in ranges:
        lower = _to_float(r.get("lower"))
        upper = _to_float(r.get("upper"))
        prob = _to_float(r.get("prob"))
        if prob is None or prob <= 0:
            continue
        if lower is None and upper is None:
            continue
        if lower is None:
            lower = upper - default_width
        if upper is None:
            upper = lower + default_width
        if upper <= lower:
            continue
        cleaned.append((lower, upper, prob))
    total = sum(prob for _, _, prob in cleaned)
    if total <= 0:
        return []
    return [(lower, upper, prob / total) for lower, upper, prob in cleaned]


def _percentile_from_ranges(
    ranges: List[Dict[str, Any]],
    q: float,
    default_width: float,
) -> Optional[float]:
    if not ranges:
        return None
    if q <= 0:
        return None
    if q >= 1:
        return None
    normalized = _normalize_ranges(ranges, default_width)
    if not normalized:
        return None
    normalized.sort(key=lambda r: r[0])
    cumulative = 0.0
    for lower, upper, prob in normalized:
        next_cum = cumulative + prob
        if q <= next_cum:
            frac = (q - cumulative) / prob if prob > 0 else 0.0
            return lower + frac * (upper - lower)
        cumulative = next_cum
    return normalized[-1][1]


def _build_time_grid(start_ts: int, end_ts: int, interval_minutes: int) -> List[int]:
    if interval_minutes <= 0:
        return []
    return list(range(int(start_ts), int(end_ts) + 1, int(interval_minutes) * 60))


def _pick_value_at_or_before(candles: List[List[Optional[float]]], ts: int) -> Optional[float]:
    for row in reversed(candles):
        if not row:
            continue
        c_ts = row[0]
        val = row[1]
        if c_ts is None or val is None:
            continue
        if c_ts <= ts:
            return float(val)
    return None


def _estimate_width_from_strikes(strikes: List[int], fallback: float) -> float:
    if len(strikes) < 2:
        return fallback
    gaps = [b - a for a, b in zip(sorted(strikes), sorted(strikes)[1:]) if b - a > 0]
    if not gaps:
        return fallback
    gaps.sort()
    mid = len(gaps) // 2
    return float(gaps[mid])


def _build_kalshi_history(
    *,
    markets: List[Dict[str, Any]],
    series_ticker: str,
    auth: Dict[str, Any],
    base_url: str,
    cache_dir: Path,
    start_ts: int,
    end_ts: int,
    interval_minutes: int,
    scale: float = 1.0,
    bucket_width: Optional[float] = None,
) -> List[Dict[str, Any]]:
    strikes: List[float] = []
    market_meta: List[Tuple[float, str, str, bool]] = []
    custom_mode = False
    for market in markets:
        title = str(market.get("title") or "")
        strike_value = _kalshi_strike_value(market)
        if strike_value is None or not isinstance(strike_value, (int, float)) or not math.isfinite(strike_value):
            continue
        ticker = str(market.get("ticker") or "").strip()
        if not ticker:
            continue
        strike_type = str(market.get("strike_type") or "").strip().lower()
        has_custom = strike_type == "custom" or isinstance(market.get("custom_strike"), dict)
        custom_mode = custom_mode or has_custom
        scaled = float(strike_value) * scale
        strikes.append(scaled)
        market_meta.append((scaled, ticker, strike_type, has_custom))

    if not market_meta:
        return []

    grid = _build_time_grid(start_ts, end_ts, interval_minutes)
    strike_series: Dict[int, List[List[Optional[float]]]] = {}
    for strike_value, ticker, _strike_type, _has_custom in market_meta:
        candles = _fetch_candles(
            base_url=base_url,
            auth=auth,
            series_ticker=series_ticker,
            market_ticker=ticker,
            start_ts=start_ts,
            end_ts=end_ts,
            interval=f"{interval_minutes}m",
            cache_dir=cache_dir,
        )
        if candles:
            strike_series[strike_value] = candles

    if not strike_series:
        return []

    default_width = _estimate_width_from_strikes(strikes, fallback=0.25 if custom_mode else 50000.0)
    width = bucket_width if bucket_width is not None else default_width
    out: List[Dict[str, Any]] = []
    quantiles = (0.05, 0.1, 0.25, 0.75, 0.9, 0.95)
    for ts in grid:
        points: List[Tuple[float, float]] = []
        for strike_value, ticker, strike_type, has_custom in market_meta:
            candles = strike_series.get(strike_value)
            if not candles:
                continue
            val = _pick_value_at_or_before(candles, ts)
            if val is None:
                continue
            prob = val / 100.0
            if strike_type in ("less", "below", "lt"):
                prob = 1.0 - prob
            points.append((strike_value, prob))
        if len(points) < 1:
            continue
        ranges = _ranges_from_custom_points(points, width) if custom_mode else _kalshi_ranges_from_points(points)
        expected = _expected_from_ranges(ranges, width if custom_mode else default_width)
        if expected is None:
            continue
        payload = {"ts": ts, "expected": expected}
        for q in quantiles:
            val = _percentile_from_ranges(ranges, q, width if custom_mode else default_width)
            if val is None:
                continue
            payload[f"p{int(q * 100):02d}"] = val
        out.append(payload)
    return out


def _build_polymarket_history(
    *,
    event: Dict[str, Any],
    clob_url: str,
    cache_dir: Path,
    start_ts: int,
    end_ts: int,
    interval_minutes: int,
    chunk_days: int,
) -> List[Dict[str, Any]]:
    markets = _extract_bracket_markets_from_event(event)
    if not markets:
        return []
    grid = _build_time_grid(start_ts, end_ts, interval_minutes)
    token_series: Dict[str, List[List[Optional[float]]]] = {}
    ranges_meta: List[Dict[str, Any]] = []
    for market in markets:
        token_id = market.get("token_id")
        if not token_id:
            continue
        candles = _fetch_token_price_history_chunked(
            clob_url=clob_url,
            token_id=str(token_id),
            start_ts=start_ts,
            end_ts=end_ts,
            fidelity_minutes=interval_minutes,
            cache_dir=cache_dir,
            chunk_days=chunk_days,
        )
        if candles:
            token_series[str(token_id)] = candles
            strike = market.get("strike") or {}
            ranges_meta.append(
                {
                    "token_id": str(token_id),
                    "lower": strike.get("lower"),
                    "upper": strike.get("upper"),
                }
            )

    if not token_series:
        return []

    widths = []
    for r in ranges_meta:
        lower = _to_float(r.get("lower"))
        upper = _to_float(r.get("upper"))
        if lower is not None and upper is not None and upper > lower:
            widths.append(upper - lower)
    default_width = float(sorted(widths)[len(widths) // 2]) if widths else 50000.0

    out: List[Dict[str, Any]] = []
    quantiles = (0.05, 0.1, 0.25, 0.75, 0.9, 0.95)
    for ts in grid:
        ranges: List[Dict[str, Any]] = []
        for meta in ranges_meta:
            token_id = meta.get("token_id")
            candles = token_series.get(str(token_id))
            if not candles:
                continue
            val = _pick_value_at_or_before(candles, ts)
            if val is None:
                continue
            ranges.append(
                {
                    "lower": meta.get("lower"),
                    "upper": meta.get("upper"),
                    "prob": val / 100.0,
                }
            )
        expected = _expected_from_ranges(ranges, default_width)
        if expected is None:
            continue
        payload = {"ts": ts, "expected": expected}
        for q in quantiles:
            val = _percentile_from_ranges(ranges, q, default_width)
            if val is None:
                continue
            payload[f"p{int(q * 100):02d}"] = val
        out.append(payload)
    return out


def _gamma_public_search_events(
    *,
    gamma_url: str,
    query: str,
    cache_dir: Path,
    limit_per_type: int = 10,
) -> List[Dict[str, Any]]:
    params = {
        "q": query,
        "limit_per_type": limit_per_type,
        "search_tags": "false",
        "search_profiles": "false",
        "keep_closed_markets": 1,
    }
    url = f"{gamma_url.rstrip('/')}/public-search?{urlencode(params)}"
    payload = _fetch_poly_cached_json(url=url, cache_dir=cache_dir, cache_namespace="public_search", timeout_s=45)
    events = payload.get("events") if isinstance(payload, dict) else None
    return [e for e in events if isinstance(e, dict)] if isinstance(events, list) else []


def _gamma_fetch_event_by_slug(*, gamma_url: str, slug: str, cache_dir: Path) -> Optional[Dict[str, Any]]:
    slug_clean = str(slug or "").strip()
    if not slug_clean:
        return None
    url = f"{gamma_url.rstrip('/')}/events/slug/{slug_clean}"
    payload = _fetch_poly_cached_json(url=url, cache_dir=cache_dir, cache_namespace="event_slug", timeout_s=45)
    return payload if isinstance(payload, dict) else None


def _pick_polymarket_event(
    candidates: List[Dict[str, Any]],
    *,
    match_terms: List[str],
) -> Optional[Dict[str, Any]]:
    filtered = []
    for event in candidates:
        title = str(event.get("title") or event.get("question") or "").lower()
        if match_terms and not all(term in title for term in match_terms):
            continue
        filtered.append(event)
    if not filtered:
        filtered = candidates

    now = int(time.time())
    scored = []
    for event in filtered:
        end_ts = _parse_iso_to_ts(event.get("endDate") or event.get("end_date") or event.get("close_time"))
        if not isinstance(end_ts, int):
            continue
        scored.append((end_ts, event))
    if not scored:
        return filtered[0] if filtered else None

    future = [item for item in scored if item[0] >= now]
    if future:
        future.sort(key=lambda x: x[0])
        return future[0][1]
    scored.sort(key=lambda x: x[0])
    return scored[-1][1]


def _pick_polymarket_event_with_brackets(
    candidates: List[Dict[str, Any]],
    *,
    match_terms: List[str],
    gamma_url: str,
    cache_dir: Path,
) -> Optional[Dict[str, Any]]:
    filtered = []
    for event in candidates:
        title = str(event.get("title") or event.get("question") or "").lower()
        if match_terms and not all(term in title for term in match_terms):
            continue
        filtered.append(event)
    if not filtered:
        filtered = candidates
    if not filtered:
        return None

    scored = []
    for event in filtered:
        slug = str(event.get("slug") or "").strip()
        if not slug:
            continue
        full = _gamma_fetch_event_by_slug(gamma_url=gamma_url, slug=slug, cache_dir=cache_dir)
        if not isinstance(full, dict):
            continue
        brackets = _extract_bracket_markets_from_event(full)
        bracket_count = len(brackets)
        end_ts = _parse_iso_to_ts(full.get("endDate") or full.get("end_date") or full.get("close_time"))
        scored.append((bracket_count, end_ts, full))

    if not scored:
        return None

    candidates_with_brackets = [item for item in scored if item[0] > 0]
    chosen = candidates_with_brackets if candidates_with_brackets else scored

    now = int(time.time())
    future = [item for item in chosen if isinstance(item[1], int) and item[1] >= now]
    if future:
        future.sort(key=lambda x: x[1])
        return future[0][2]
    chosen.sort(key=lambda x: x[1] or 0)
    return chosen[-1][2]


def _polymarket_midpoint(
    *,
    clob_url: str,
    token_id: str,
    cache_dir: Path,
) -> Optional[float]:
    url = f"{clob_url.rstrip('/')}/midpoint?token_id={token_id}"
    try:
        payload = _fetch_poly_cached_json(url=url, cache_dir=cache_dir, cache_namespace="midpoint", timeout_s=20)
    except Exception:
        return None
    raw = None
    if isinstance(payload, dict):
        raw = payload.get("mid") or payload.get("midpoint") or payload.get("price")
    price = _to_float(raw)
    if price is None:
        return None
    return price / 100.0 if price > 1.5 else price


def _polymarket_ranges_from_event(
    event: Dict[str, Any],
    *,
    clob_url: str,
    cache_dir: Path,
) -> List[Dict[str, Any]]:
    brackets = _extract_bracket_markets_from_event(event)
    ranges: List[Dict[str, Any]] = []
    for bracket in brackets:
        token_id = str(bracket.get("token_id") or "").strip()
        if not token_id:
            continue
        prob = _polymarket_midpoint(clob_url=clob_url, token_id=token_id, cache_dir=cache_dir)
        if prob is None:
            continue
        strike = bracket.get("strike") or {}
        ranges.append(
            {
                "lower": strike.get("lower"),
                "upper": strike.get("upper"),
                "prob": prob,
            }
        )
    return ranges


def _trim_snapshots(snapshots: List[Dict[str, Any]], retention_days: int) -> List[Dict[str, Any]]:
    if retention_days <= 0:
        return snapshots
    cutoff = int(time.time()) - retention_days * 86400
    out = []
    for snap in snapshots:
        ts = _parse_ts(snap.get("as_of"))
        if ts is None or ts >= cutoff:
            out.append(snap)
    out.sort(key=lambda s: _parse_ts(s.get("as_of")) or 0)
    return out


def _load_supplemental_sources(config: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    loaded: List[Dict[str, Any]] = []
    for src in config:
        if not isinstance(src, dict):
            continue
        src_id = str(src.get("id") or "").strip()
        path_raw = str(src.get("path") or "").strip()
        if not src_id or not path_raw:
            continue
        path = (BASE_DIR / path_raw).resolve()
        if not path.exists():
            continue
        try:
            payload = _read_json(path)
        except Exception:
            continue
        loaded.append(
            {
                "id": src_id,
                "label": str(src.get("label") or src_id),
                "type": str(src.get("type") or "index"),
                "data": payload,
            }
        )
    return loaded


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Kalshi/Polymarket distributions and update market_priors.json.")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="Output JSON path.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Config JSON path for event sources.")
    parser.add_argument("--kalshi-base-url", default=KALSHI_BASE_URL, help="Kalshi API base URL.")
    parser.add_argument("--gamma-url", default=DEFAULT_GAMMA_URL, help="Polymarket Gamma API base URL.")
    parser.add_argument("--clob-url", default=DEFAULT_CLOB_URL, help="Polymarket CLOB API base URL.")
    parser.add_argument("--kalshi-cache-dir", default="data/kalshi_cache", help="Kalshi cache directory.")
    parser.add_argument("--poly-cache-dir", default="data/polymarket_cache", help="Polymarket cache directory.")
    parser.add_argument("--kalshi-token", default="", help="Kalshi bearer token (overrides env).")
    parser.add_argument("--kalshi-key-id", default="", help="Kalshi API key id (overrides env).")
    parser.add_argument("--kalshi-private-key-path", default="", help="Kalshi private key path (overrides env).")
    parser.add_argument("--verbose", action="store_true", help="Print progress details.")
    args = parser.parse_args()

    out_path = Path(args.out).expanduser()
    cfg_path = Path(args.config).expanduser()
    if not cfg_path.exists():
        print(f"Error: config not found at {cfg_path}", file=sys.stderr)
        return 2
    config = _read_json(cfg_path)
    events_cfg = config.get("events") if isinstance(config, dict) else None
    if not isinstance(events_cfg, list):
        print(f"Error: invalid config in {cfg_path}", file=sys.stderr)
        return 2
    retention_days = int(config.get("retention_days") or 30)
    history_days = int(config.get("history_days") or 0)
    history_interval_minutes = int(config.get("history_interval_minutes") or 60)
    history_chunk_days = int(config.get("history_chunk_days") or 5)
    supplemental_cfg = config.get("supplemental_sources") if isinstance(config, dict) else None
    supplemental_sources = _load_supplemental_sources(supplemental_cfg or [])

    existing = {"events": []}
    if out_path.exists():
        try:
            existing = _read_json(out_path)
        except Exception:
            existing = {"events": []}
    existing_events = {str(e.get("id")): e for e in existing.get("events", []) if isinstance(e, dict)}

    kalshi_auth = _kalshi_auth_from_env(
        token=args.kalshi_token or None,
        key_id=args.kalshi_key_id or None,
        private_key_path=args.kalshi_private_key_path or None,
    )
    kalshi_cache = (BASE_DIR / args.kalshi_cache_dir).resolve()
    poly_cache = (BASE_DIR / args.poly_cache_dir).resolve()

    new_events: List[Dict[str, Any]] = []
    for event_cfg in events_cfg:
        if not isinstance(event_cfg, dict):
            continue
        event_id = str(event_cfg.get("id") or "").strip()
        if not event_id:
            continue
        name = str(event_cfg.get("name") or event_id)
        fmt = str(event_cfg.get("format") or "jobs")
        decimals = event_cfg.get("decimals")
        if decimals is None:
            decimals = 0 if fmt == "jobs" else 1

        base = existing_events.get(event_id, {})
        snapshots = list(base.get("snapshots") or [])
        kalshi_ranges: List[Dict[str, Any]] = []
        polymarket_ranges: List[Dict[str, Any]] = []
        next_release_ts: Optional[int] = None
        close_ts: Optional[int] = None
        close_ts_chosen = False
        supplemental: Dict[str, Any] = {}
        history: Dict[str, Any] = {}
        event_markets: List[Dict[str, Any]] = []

        kalshi_cfg = event_cfg.get("kalshi") if isinstance(event_cfg.get("kalshi"), dict) else {}
        kalshi_series = str(kalshi_cfg.get("series_ticker") or "").strip()
        kalshi_event_ticker = str(kalshi_cfg.get("event_ticker") or "").strip() or None
        if kalshi_series:
            kalshi_series = kalshi_series.upper()
        if kalshi_event_ticker:
            kalshi_event_ticker = kalshi_event_ticker.upper()
        markets: List[Dict[str, Any]] = []
        if kalshi_series and kalshi_auth:
            try:
                markets = _fetch_markets(
                    base_url=str(args.kalshi_base_url),
                    auth=kalshi_auth,
                    series_ticker=kalshi_series,
                    cache_dir=kalshi_cache,
                )
            except Exception as exc:
                if args.verbose:
                    print(f"Kalshi fetch failed for {event_id}: {exc}", file=sys.stderr)
        elif kalshi_series and not kalshi_auth and args.verbose:
            print(f"Kalshi creds missing; skipping {event_id}.", file=sys.stderr)

        poly_cfg = event_cfg.get("polymarket") if isinstance(event_cfg.get("polymarket"), dict) else {}
        poly_slug = str(poly_cfg.get("slug") or "").strip()
        poly_query = str(poly_cfg.get("query") or "").strip()
        match_terms = [str(t).lower() for t in (poly_cfg.get("match") or []) if str(t).strip()]
        poly_event = None
        poly_end_ts: Optional[int] = None
        if poly_slug:
            poly_event = _gamma_fetch_event_by_slug(gamma_url=str(args.gamma_url), slug=poly_slug, cache_dir=poly_cache)
        elif poly_query:
            candidates = _gamma_public_search_events(
                gamma_url=str(args.gamma_url),
                query=poly_query,
                cache_dir=poly_cache,
                limit_per_type=10,
            )
            poly_event = _pick_polymarket_event_with_brackets(
                candidates,
                match_terms=match_terms,
                gamma_url=str(args.gamma_url),
                cache_dir=poly_cache,
            )
            if poly_event is None:
                poly_event = _pick_polymarket_event(candidates, match_terms=match_terms)
                if poly_event and poly_event.get("slug"):
                    poly_event = _gamma_fetch_event_by_slug(
                        gamma_url=str(args.gamma_url),
                        slug=str(poly_event.get("slug")),
                        cache_dir=poly_cache,
                    )

        if isinstance(poly_event, dict):
            polymarket_ranges = _polymarket_ranges_from_event(
                poly_event,
                clob_url=str(args.clob_url),
                cache_dir=poly_cache,
            )
            poly_end_ts = _parse_iso_to_ts(poly_event.get("endDate") or poly_event.get("end_date") or poly_event.get("close_time"))
            if isinstance(poly_end_ts, int):
                next_release_ts = poly_end_ts if next_release_ts is None else min(next_release_ts, poly_end_ts)

        if markets:
            target_ts = poly_end_ts if isinstance(poly_end_ts, int) else next_release_ts
            if event_id in ("cpi", "unemployment", "fed"):
                target_ts = None
            if event_id == "nfp" and kalshi_series and not kalshi_event_ticker and isinstance(target_ts, int):
                payroll_month = _payroll_month_from_release_ts(target_ts)
                if payroll_month:
                    event_markets = _select_payroll_markets_for_payroll_month(
                        markets,
                        series_ticker=kalshi_series,
                        payroll_month=payroll_month,
                    )
                    if event_markets:
                        close_ts = min((_market_close_ts(m) or 0) for m in event_markets) or None

            if not event_markets:
                event_markets, close_ts = _kalshi_pick_event_markets(
                    markets,
                    event_ticker=kalshi_event_ticker,
                    target_ts=target_ts,
                )

            if event_id == "fed":
                kalshi_ranges = _kalshi_ranges_from_markets(
                    event_markets,
                    scale=0.01,
                    bucket_width=0.25,
                )
            else:
                kalshi_ranges = _kalshi_ranges_from_markets(event_markets)
            if close_ts:
                if event_id in ("cpi", "unemployment", "fed"):
                    close_date = _date_in_tz(close_ts, "America/New_York", "-05:00")
                    if event_id == "fed":
                        next_release_ts = _to_utc_ts(close_date, "14:00", "America/New_York", "-05:00")
                    else:
                        next_release_ts = _to_utc_ts(close_date, "08:30", "America/New_York", "-05:00")
                    close_ts_chosen = True
                else:
                    next_release_ts = close_ts if next_release_ts is None else min(next_release_ts, close_ts)

        if event_id == "nfp" and next_release_ts:
            local_date = _date_in_tz(next_release_ts, "America/New_York", "-05:00")
            next_release_ts = _nfp_release_ts_for_month(local_date.year, local_date.month)
        elif event_id in ("cpi", "unemployment") and next_release_ts and not close_ts_chosen:
            local_date = _date_in_tz(next_release_ts, "America/New_York", "-05:00")
            next_release_ts = _to_utc_ts(local_date, "08:30", "America/New_York", "-05:00")
        elif event_id == "fed" and next_release_ts and not close_ts_chosen:
            local_date = _date_in_tz(next_release_ts, "America/New_York", "-05:00")
            next_release_ts = _to_utc_ts(local_date, "14:00", "America/New_York", "-05:00")

        event_history_days = int(event_cfg.get("history_days") or history_days)
        event_history_interval = int(event_cfg.get("history_interval_minutes") or history_interval_minutes)
        event_history_chunk_days = int(event_cfg.get("history_chunk_days") or history_chunk_days)
        if event_history_days > 0 and event_history_interval > 0:
            end_ts = int(time.time())
            start_ts = end_ts - event_history_days * 86400
            history_sources: Dict[str, Any] = {}
            if kalshi_series and kalshi_auth and event_markets:
                history_scale = 0.01 if event_id == "fed" else 1.0
                history_bucket = 0.25 if event_id == "fed" else None
                kalshi_hist = _build_kalshi_history(
                    markets=event_markets,
                    series_ticker=kalshi_series,
                    auth=kalshi_auth,
                    base_url=str(args.kalshi_base_url),
                    cache_dir=kalshi_cache,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    interval_minutes=event_history_interval,
                    scale=history_scale,
                    bucket_width=history_bucket,
                )
                if kalshi_hist:
                    history_sources["kalshi"] = kalshi_hist
            if isinstance(poly_event, dict):
                poly_hist = _build_polymarket_history(
                    event=poly_event,
                    clob_url=str(args.clob_url),
                    cache_dir=poly_cache,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    interval_minutes=event_history_interval,
                    chunk_days=event_history_chunk_days,
                )
                if poly_hist:
                    history_sources["polymarket"] = poly_hist
            if history_sources:
                history = {
                    "interval_minutes": event_history_interval,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "sources": history_sources,
                }

        if supplemental_sources:
            for src in supplemental_sources:
                src_data = src.get("data") if isinstance(src, dict) else None
                if not isinstance(src_data, dict):
                    continue
                event_payload = (src_data.get("events") or {}).get(event_id)
                if not isinstance(event_payload, dict):
                    continue
                entry: Dict[str, Any] = {
                    "label": src.get("label"),
                    "type": src.get("type"),
                    "as_of": src_data.get("as_of"),
                }
                if "value" in event_payload:
                    entry["value"] = event_payload.get("value")
                if "ranges" in event_payload:
                    entry["ranges"] = event_payload.get("ranges")
                if "themes" in event_payload:
                    entry["themes"] = event_payload.get("themes")
                if "note" in event_payload:
                    entry["note"] = event_payload.get("note")
                supplemental[str(src.get("id"))] = entry

        if kalshi_ranges or polymarket_ranges or supplemental:
            source_meta: Dict[str, Any] = {}
            volume_window_days = int(event_cfg.get("volume_window_days") or event_history_days or history_days or 30)
            volume_interval_minutes = int(
                event_cfg.get("volume_interval_minutes") or event_history_interval or history_interval_minutes or 60
            )
            now_ts = int(time.time())
            volume_end_candidates = [t for t in (close_ts, poly_end_ts, now_ts) if isinstance(t, int) and t > 0]
            volume_end_ts = min(volume_end_candidates) if volume_end_candidates else now_ts
            if event_markets:
                kalshi_volume = None
                if kalshi_series and kalshi_auth:
                    try:
                        kalshi_volume = _sum_kalshi_notional_volume(
                            markets=event_markets,
                            series_ticker=kalshi_series,
                            auth=kalshi_auth,
                            base_url=str(args.kalshi_base_url),
                            cache_dir=kalshi_cache,
                            window_days=volume_window_days,
                            interval_minutes=volume_interval_minutes,
                            end_ts=volume_end_ts,
                        )
                    except Exception as exc:
                        if args.verbose:
                            print(f"Kalshi volume calc failed for {event_id}: {exc}", file=sys.stderr)
                if kalshi_volume is None:
                    kalshi_volume = _sum_kalshi_volume(event_markets)
                if kalshi_volume is not None:
                    kalshi_meta = {"volume": kalshi_volume}
                    if event_markets:
                        kalshi_meta["event_ticker"] = _kalshi_event_ticker(event_markets[0])
                    source_meta["kalshi"] = kalshi_meta
            if isinstance(poly_event, dict):
                poly_volume = _sum_polymarket_volume(poly_event, window_days=volume_window_days)
                if poly_volume is not None:
                    source_meta.setdefault("polymarket", {})["volume"] = poly_volume

            snapshot_payload = {
                "as_of": _now_iso(),
                "sources": {
                    "kalshi": kalshi_ranges,
                    "polymarket": polymarket_ranges,
                },
                "supplemental": supplemental,
            }
            if source_meta:
                snapshot_payload["source_meta"] = source_meta
            snapshots.append(snapshot_payload)

        override_ts = _parse_ts(event_cfg.get("next_release_override"))
        if override_ts:
            next_release_ts = override_ts
        snapshots = _trim_snapshots(snapshots, retention_days)
        next_release = _iso_from_ts(next_release_ts) if next_release_ts else base.get("next_release")
        new_events.append(
            {
                "id": event_id,
                "name": name,
                "format": fmt,
                "decimals": decimals,
                "next_release": next_release,
                "snapshots": snapshots,
                "history": history,
            }
        )

    for event_id, event in existing_events.items():
        if any(e.get("id") == event_id for e in new_events):
            continue
        new_events.append(event)

    new_events.sort(key=lambda e: str(e.get("id") or ""))
    out_payload = {
        "updated_at": _now_iso(),
        "events": new_events,
    }
    _write_json(out_path, out_payload)
    if args.verbose:
        print(f"Updated {out_path} with {len(new_events)} events.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
