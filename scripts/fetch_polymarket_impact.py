#!/usr/bin/env python3
"""
Polymarket payroll market impact pipeline.

Generates `data/polymarket_impact.json`, which is consumed by `impact.html`
when Provider = Polymarket (NFP).

What this does
  - Finds the Polymarket “How many jobs added in {Month}?” *event* for each payroll month
    (using the public Gamma search + events endpoints)
  - For each release event (ADP/Revelio/Jobs) in the requested release-month range,
    pulls 1-minute token price history for every bracket outcome around the event
    (using the public CLOB `/prices-history` endpoint)
  - Writes a dataset shaped similarly to `data/kalshi_impact.json` so the same UI can render it.

Notes
  - No auth required (public endpoints), but network access is required.
  - Prices are stored as percentage probabilities (0–100), matching Kalshi’s representation.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import http.client
import json
import math
import os
import re
import socket
import ssl
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# Reuse shared logic + summarizers.
from impact_common import (  # type: ignore[import-not-found]
    DEFAULT_TZ,
    _attach_announced_values,
    _generate_events,
    _iso_from_ts,
    _load_events_from_csv,
    _parse_horizons,
    _parse_year_month,
    _pick_baseline,
    _pick_level_at_or_after,
    _summarize_dataset,
    _summarize_event,
    _to_utc_ts,
)


BASE_DIR = Path(__file__).resolve().parent.parent


def _resolve_repo_path(value: str) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (BASE_DIR / path).resolve()

DEFAULT_GAMMA_URL = "https://gamma-api.polymarket.com"
DEFAULT_CLOB_URL = "https://clob.polymarket.com"
DEFAULT_OUT = "data/polymarket_impact.json"


def _sha1(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def _http_get_json(url: str, timeout_s: int = 30, retries: int = 2, backoff_s: float = 1.0) -> Any:
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "Newsfeed/polymarket-impact"}, method="GET")
    last_error: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            with urlopen(request, timeout=timeout_s) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                body = response.read().decode(charset)
                return json.loads(body)
        except HTTPError as exc:
            last_error = exc
            if exc.code in {429, 500, 502, 503, 504} and attempt < retries:
                time.sleep(backoff_s * (2**attempt))
                continue
            raise
        except (
            URLError,
            TimeoutError,
            socket.timeout,
            ssl.SSLError,
            http.client.IncompleteRead,
            http.client.RemoteDisconnected,
        ) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(backoff_s * (2**attempt))
                continue
            raise
    if last_error:
        raise last_error
    raise RuntimeError("HTTP request failed without an exception.")


def _fetch_cached_json(*, url: str, cache_dir: Path, cache_namespace: str, timeout_s: int = 30) -> Any:
    cache_key = _sha1(f"{cache_namespace}:{url}")
    cache_path = cache_dir / f"{cache_key}.json"
    if cache_path.exists():
        return _read_json(cache_path)
    payload = _http_get_json(url, timeout_s=timeout_s)
    cache_dir.mkdir(parents=True, exist_ok=True)
    _write_json(cache_path, payload)
    return payload


def _parse_iso_to_ts(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        # Gamma sometimes returns ms.
        n = int(value)
        return int(n / 1000) if n > 1_000_000_000_000 else n
    s = str(value).strip()
    if not s:
        return None
    try:
        parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(parsed.timestamp())
    except ValueError:
        return None


def _month_name(payroll_month: str) -> str:
    y, m = _parse_year_month(payroll_month)
    return dt.date(y, m, 1).strftime("%B")


def _next_month(year: int, month: int) -> Tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _release_month_from_payroll_month(payroll_month: str) -> str:
    y, m = _parse_year_month(payroll_month)
    ny, nm = _next_month(y, m)
    return f"{ny:04d}-{nm:02d}"


def _is_nfp_event(event: Dict[str, Any]) -> bool:
    title = str(event.get("title") or event.get("question") or "").lower()
    slug = str(event.get("slug") or "").lower()
    return "how many jobs added" in title or slug.startswith("how-many-jobs-added-in-")


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


def _merge_events(base: List[Dict[str, Any]], extra: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    for event in base:
        key = str(event.get("id") or "")
        if not key:
            continue
        by_id[key] = event
    for event in extra:
        key = str(event.get("id") or "")
        if not key:
            continue
        by_id[key] = event
    merged = list(by_id.values())
    merged.sort(key=lambda e: int(e.get("release_ts") or 0))
    return merged


def _parse_jobs_value(raw: str) -> Optional[int]:
    s = raw.strip().lower().replace(",", "")
    match = re.match(r"^(-?\d+(?:\.\d+)?)\s*(k|m)?$", s)
    if not match:
        return None
    value = float(match.group(1))
    unit = match.group(2)
    if unit == "k":
        value *= 1000
    elif unit == "m":
        value *= 1_000_000
    return int(round(value))


def _parse_outcome_bracket(label: str) -> Optional[Dict[str, Any]]:
    s = str(label or "").strip()
    if not s:
        return None
    lowered = (
        s.lower()
        .replace(" ", "")
        .replace("–", "-")
        .replace("—", "-")
        .replace("−", "-")
    )

    # Common patterns: "<100K", "100K-150K", "200K+", ">=200K"
    match = re.match(r"^(?:<=|<)\s*([0-9][0-9,]*)(k|m)?$", lowered)
    if match:
        upper = _parse_jobs_value(match.group(1) + (match.group(2) or ""))
        if upper is None:
            return None
        rep = max(0, upper - 25_000)
        return {"kind": "range", "lower": None, "upper": upper, "value": rep, "label": s}

    match = re.match(r"^(?:>=|>)\s*([0-9][0-9,]*)(k|m)?\+?$", lowered)
    if match:
        lower = _parse_jobs_value(match.group(1) + (match.group(2) or ""))
        if lower is None:
            return None
        rep = lower + 25_000
        return {"kind": "range", "lower": lower, "upper": None, "value": rep, "label": s}

    match = re.match(r"^([0-9][0-9,]*)(k|m)?\+$", lowered)
    if match:
        lower = _parse_jobs_value(match.group(1) + (match.group(2) or ""))
        if lower is None:
            return None
        rep = lower + 25_000
        return {"kind": "range", "lower": lower, "upper": None, "value": rep, "label": s}

    match = re.match(r"^([0-9][0-9,]*)(k|m)?-([0-9][0-9,]*)(k|m)?$", lowered)
    if match:
        lo_unit = match.group(2) or match.group(4) or ""
        hi_unit = match.group(4) or match.group(2) or ""
        lo = _parse_jobs_value(match.group(1) + lo_unit)
        hi = _parse_jobs_value(match.group(3) + hi_unit)
        if lo is None or hi is None:
            return None
        rep = int(round((lo + hi) / 2))
        return {"kind": "range", "lower": lo, "upper": hi, "value": rep, "label": s}

    return {"kind": "range", "lower": None, "upper": None, "value": None, "label": s}


def _pick_last_level_at_or_before(
    candles: List[List[Optional[float]]], target_ts: int
) -> Tuple[Optional[float], Optional[float]]:
    for ts, yes, _vol in reversed(candles):
        if ts is None or yes is None:
            continue
        if ts <= target_ts:
            return ts, yes
    return None, None


def _pick_level_nearest_available(
    candles: List[List[Optional[float]]], target_ts: int
) -> Tuple[Optional[float], Optional[float]]:
    ts, yes = _pick_level_at_or_after(candles, target_ts)
    if yes is not None:
        return ts, yes
    return _pick_last_level_at_or_before(candles, target_ts)


def _clip_and_pad_candles(
    candles: List[List[Optional[float]]], *, start_ts: int, end_ts: int
) -> List[List[Optional[float]]]:
    if not candles:
        return []
    windowed = [c for c in candles if c and c[0] is not None and start_ts <= float(c[0]) <= end_ts]
    carry_in_ts, carry_in_yes = _pick_last_level_at_or_before(candles, start_ts)
    carry_out_ts, carry_out_yes = _pick_last_level_at_or_before(candles, end_ts)

    out: List[List[Optional[float]]] = []
    if windowed:
        windowed.sort(key=lambda row: float(row[0] or 0))
        first_ts = float(windowed[0][0] or 0)
        if carry_in_yes is not None and first_ts > start_ts:
            out.append([float(start_ts), float(carry_in_yes), None])
        out.extend(windowed)
        last_ts = float(out[-1][0] or 0)
        if carry_out_yes is not None and last_ts < end_ts:
            out.append([float(end_ts), float(carry_out_yes), None])
    else:
        if carry_in_yes is not None:
            out.append([float(start_ts), float(carry_in_yes), None])
        if carry_out_yes is not None and end_ts != start_ts:
            if not out or float(out[-1][0] or 0) != float(end_ts):
                out.append([float(end_ts), float(carry_out_yes), None])

    # De-dup by timestamp, keeping the last non-null yes.
    dedup: Dict[float, List[Optional[float]]] = {}
    for row in out:
        if not row or row[0] is None:
            continue
        ts = float(row[0])
        prev = dedup.get(ts)
        if prev is None or (prev[1] is None and row[1] is not None):
            dedup[ts] = [ts, row[1], None]
    final = list(dedup.values())
    final.sort(key=lambda r: float(r[0] or 0))
    return final


def _gamma_public_search_events(
    *,
    gamma_url: str,
    query: str,
    cache_dir: Path,
    limit_per_type: int = 10,
    keep_closed_markets: int = 1,
) -> List[Dict[str, Any]]:
    params = {
        "q": query,
        "limit_per_type": limit_per_type,
        "search_tags": "false",
        "search_profiles": "false",
        "keep_closed_markets": keep_closed_markets,
    }
    url = f"{gamma_url.rstrip('/')}/public-search?{urlencode(params)}"
    payload = _fetch_cached_json(url=url, cache_dir=cache_dir, cache_namespace="public_search", timeout_s=45)
    events = payload.get("events") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return []
    return [e for e in events if isinstance(e, dict) and _is_nfp_event(e)]


def _gamma_fetch_event_by_slug(*, gamma_url: str, slug: str, cache_dir: Path) -> Optional[Dict[str, Any]]:
    slug_clean = str(slug or "").strip()
    if not slug_clean:
        return None
    url = f"{gamma_url.rstrip('/')}/events/slug/{slug_clean}"
    payload = _fetch_cached_json(url=url, cache_dir=cache_dir, cache_namespace="event_slug", timeout_s=45)
    return payload if isinstance(payload, dict) else None


def _pick_best_event_from_candidates(
    candidates: List[Dict[str, Any]], *, payroll_month: str, jobs_report_ts: int, close_window_days: int = 14
) -> Optional[Dict[str, Any]]:
    month = _month_name(payroll_month).lower()
    target_year = dt.datetime.utcfromtimestamp(int(jobs_report_ts)).year
    filtered = []
    for e in candidates:
        title = str(e.get("title") or e.get("question") or "").lower()
        if month not in title:
            continue
        end_ts = _parse_iso_to_ts(e.get("endDate") or e.get("end_date") or e.get("close_time"))
        if end_ts is not None:
            end_year = dt.datetime.utcfromtimestamp(int(end_ts)).year
            # Avoid accidentally selecting an older year's “October” etc if the desired year has no market.
            if abs(end_year - target_year) > 1:
                continue
        filtered.append(e)
    if not filtered:
        filtered = candidates[:]
    window_s = close_window_days * 24 * 3600
    scored: List[Tuple[int, float, str, Dict[str, Any]]] = []
    for e in filtered:
        end_ts = _parse_iso_to_ts(e.get("endDate") or e.get("end_date") or e.get("close_time"))
        dist = abs(end_ts - jobs_report_ts) if end_ts is not None else 10**12
        in_window = 0 if dist <= window_s else 1
        slug = str(e.get("slug") or "")
        scored.append((in_window, float(dist), slug, e))
    scored.sort(key=lambda row: (row[0], row[1], row[2]))
    if not scored:
        return None
    if scored[0][0] != 0:
        return None
    return scored[0][3]


def _parse_scheduled_release_from_description(description: Any) -> Optional[Tuple[dt.date, str]]:
    """
    Polymarket NFP event descriptions often contain:
      '... scheduled to be released on December 16, 2025, 8:30 AM ET.'
    Return (date, hh:mm) in America/New_York local time.
    """
    text = str(description or "").strip()
    if not text:
        return None
    match = re.search(
        r"scheduled to be released on\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4}),\s+(\d{1,2}):(\d{2})\s*(AM|PM)\s*ET",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    month_name = match.group(1)
    day = int(match.group(2))
    year = int(match.group(3))
    hour12 = int(match.group(4))
    minute = int(match.group(5))
    ampm = match.group(6).upper()
    try:
        month = dt.datetime.strptime(month_name, "%B").month
    except ValueError:
        try:
            month = dt.datetime.strptime(month_name, "%b").month
        except ValueError:
            return None
    hour = hour12 % 12
    if ampm == "PM":
        hour += 12
    hhmm = f"{hour:02d}:{minute:02d}"
    try:
        return dt.date(year, month, day), hhmm
    except ValueError:
        return None


def _previous_weekday(before: dt.date, weekday: int) -> dt.date:
    d = before - dt.timedelta(days=1)
    while d.weekday() != weekday:
        d -= dt.timedelta(days=1)
    return d


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


def _extract_bracket_markets_from_event(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Polymarket implements this as an event with multiple binary markets (each bracket is a market).
    We treat each bracket market's YES token as an "outcome" to match the UI model.
    """
    markets = event.get("markets") if isinstance(event.get("markets"), list) else []
    out: List[Dict[str, Any]] = []
    for m in markets:
        if not isinstance(m, dict):
            continue
        token_id = _pick_yes_token_id(m)
        if not token_id:
            continue
        label = str(m.get("groupItemTitle") or m.get("question") or "").strip()
        if not label:
            label = str(m.get("slug") or token_id)
        strike = _parse_outcome_bracket(label) or {"kind": "range", "lower": None, "upper": None, "value": None, "label": label}
        out.append(
            {
                "label": label,
                "token_id": str(token_id),
                "strike": strike,
                "market_slug": m.get("slug"),
                "question": m.get("question"),
                "condition_id": m.get("conditionId") or m.get("condition_id"),
            }
        )
    return out


def _fetch_token_price_history(
    *,
    clob_url: str,
    token_id: str,
    start_ts: int,
    end_ts: int,
    fidelity_minutes: int,
    cache_dir: Path,
) -> List[List[Optional[float]]]:
    params = {
        "market": token_id,  # token ID per docs
        "startTs": start_ts,
        "endTs": end_ts,
        "fidelity": fidelity_minutes,
    }
    url = f"{clob_url.rstrip('/')}/prices-history?{urlencode(params)}"
    payload = _fetch_cached_json(url=url, cache_dir=cache_dir, cache_namespace="prices_history", timeout_s=45)
    history = payload.get("history") if isinstance(payload, dict) else None
    if not isinstance(history, list):
        return []
    out: List[List[Optional[float]]] = []
    for entry in history:
        if not isinstance(entry, dict):
            continue
        t = entry.get("t")
        p = entry.get("p")
        try:
            ts = int(float(t))
        except (TypeError, ValueError):
            continue
        price = None
        try:
            price = float(p) * 100.0  # convert 0..1 -> 0..100
        except (TypeError, ValueError):
            price = None
        out.append([float(ts), price, None])
    out.sort(key=lambda row: row[0] if row and row[0] is not None else 0)
    return out


def _fetch_token_price_history_chunked(
    *,
    clob_url: str,
    token_id: str,
    start_ts: int,
    end_ts: int,
    fidelity_minutes: int,
    cache_dir: Path,
    chunk_days: int,
) -> List[List[Optional[float]]]:
    out: List[List[Optional[float]]] = []
    if end_ts <= start_ts:
        return out
    chunk_days = max(1, int(chunk_days))
    current_days = chunk_days
    cursor = int(start_ts)
    while cursor < end_ts:
        chunk_end = min(end_ts, cursor + current_days * 86400)
        try:
            part = _fetch_token_price_history(
                clob_url=clob_url,
                token_id=token_id,
                start_ts=cursor,
                end_ts=chunk_end,
                fidelity_minutes=fidelity_minutes,
                cache_dir=cache_dir,
            )
        except (
            HTTPError,
            URLError,
            TimeoutError,
            socket.timeout,
            ssl.SSLError,
            http.client.IncompleteRead,
            http.client.RemoteDisconnected,
        ) as exc:
            if isinstance(exc, HTTPError) and exc.code == 400 and current_days > 1:
                current_days = max(1, current_days // 2)
                continue
            if current_days > 1:
                current_days = max(1, current_days // 2)
                time.sleep(0.2)
                continue
            raise
        out.extend(part)
        cursor = chunk_end
        if cursor < end_ts:
            time.sleep(0.02)
    out.sort(key=lambda row: row[0] if row and row[0] is not None else 0)
    return out


def _fetch_token_price_history_with_fallback(
    *,
    clob_url: str,
    token_id: str,
    start_ts: int,
    end_ts: int,
    fidelity_minutes: int,
    cache_dir: Path,
    fallback_pre_days: int,
    fallback_post_days: int,
) -> Tuple[List[List[Optional[float]]], Dict[str, Any]]:
    candles = _fetch_token_price_history(
        clob_url=clob_url,
        token_id=token_id,
        start_ts=start_ts,
        end_ts=end_ts,
        fidelity_minutes=fidelity_minutes,
        cache_dir=cache_dir,
    )
    meta: Dict[str, Any] = {
        "requested": {"start_ts": start_ts, "end_ts": end_ts, "fidelity_minutes": fidelity_minutes},
        "used_fallback": False,
        "fallback": None,
    }
    if candles:
        return candles, meta
    if fallback_pre_days <= 0 and fallback_post_days <= 0:
        return candles, meta

    wide_start = start_ts - max(0, int(fallback_pre_days)) * 24 * 3600
    wide_end = end_ts + max(0, int(fallback_post_days)) * 24 * 3600
    wide_candles = _fetch_token_price_history(
        clob_url=clob_url,
        token_id=token_id,
        start_ts=wide_start,
        end_ts=wide_end,
        fidelity_minutes=fidelity_minutes,
        cache_dir=cache_dir,
    )
    meta["used_fallback"] = True
    meta["fallback"] = {"start_ts": wide_start, "end_ts": wide_end, "fidelity_minutes": fidelity_minutes}
    return wide_candles, meta


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Polymarket NFP bracket prices around payroll releases and export JSON.")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"Output JSON path (default: {DEFAULT_OUT})")
    parser.add_argument("--gamma-url", default=os.environ.get("POLY_GAMMA_URL", DEFAULT_GAMMA_URL), help="Gamma API base URL.")
    parser.add_argument("--clob-url", default=os.environ.get("POLY_CLOB_URL", DEFAULT_CLOB_URL), help="CLOB API base URL.")
    parser.add_argument("--start-month", required=True, help="Start release month (YYYY-MM), e.g. 2025-07")
    parser.add_argument("--end-month", required=True, help="End release month (YYYY-MM), e.g. 2025-12")
    parser.add_argument("--tz", default=DEFAULT_TZ, help=f"Timezone name (default: {DEFAULT_TZ}).")
    parser.add_argument("--adp-time", default="08:15", help="ADP release time in ET (HH:MM).")
    parser.add_argument("--revelio-time", default="08:30", help="Revelio release time in ET (HH:MM).")
    parser.add_argument("--jobs-time", default="08:30", help="Jobs report time in ET (HH:MM).")
    parser.add_argument("--tz-fallback-offset", default="-05:00", help="Fallback offset if tz database missing.")
    parser.add_argument("--cache-dir", default="data/polymarket_cache", help="Cache directory for API responses.")
    parser.add_argument(
        "--horizons",
        default="5,30,60,120,240",
        help="Comma-separated post-release horizons in minutes.",
    )
    parser.add_argument("--pre-minutes", type=int, default=30, help="Minutes before release to fetch.")
    parser.add_argument("--post-minutes", type=int, default=240, help="Minutes after release to fetch.")
    parser.add_argument("--fidelity", type=int, default=1, help="Price history resolution in minutes (default: 1).")
    parser.add_argument(
        "--longterm-days",
        type=int,
        default=0,
        help="Days before/after release for a long-term series (default: 0 disables).",
    )
    parser.add_argument(
        "--longterm-fidelity",
        type=int,
        default=60,
        help="Resolution in minutes for the long-term series (default: 60).",
    )
    parser.add_argument(
        "--longterm-chunk-days",
        type=int,
        default=7,
        help="Chunk size in days for long-term history requests (default: 7).",
    )
    parser.add_argument(
        "--custom-events",
        default="data/impact_events_custom.csv",
        help="Optional CSV of custom events to append (default: data/impact_events_custom.csv if present).",
    )
    parser.add_argument(
        "--fallback-pre-days",
        type=int,
        default=7,
        help="If the main window returns no history, extend the query this many days earlier (default: 7).",
    )
    parser.add_argument(
        "--fallback-post-days",
        type=int,
        default=2,
        help="If the main window returns no history, extend the query this many days later (default: 2).",
    )
    parser.add_argument("--verbose", action="store_true", help="Print progress details.")
    args = parser.parse_args()

    out_path = _resolve_repo_path(args.out)
    cache_dir = _resolve_repo_path(args.cache_dir)
    horizons = _parse_horizons(args.horizons)
    if not horizons:
        print("Error: --horizons must contain at least one positive integer minute value.", file=sys.stderr)
        return 2
    pre_minutes = int(args.pre_minutes)
    post_minutes = int(args.post_minutes)
    if max(horizons) > post_minutes:
        print("Error: --post-minutes must be >= max(--horizons).", file=sys.stderr)
        return 2
    fidelity = int(args.fidelity)
    if fidelity <= 0:
        print("Error: --fidelity must be a positive integer minute value.", file=sys.stderr)
        return 2
    longterm_days = max(0, int(args.longterm_days))
    longterm_fidelity = int(args.longterm_fidelity)
    if longterm_days > 0 and longterm_fidelity <= 0:
        print("Error: --longterm-fidelity must be a positive integer minute value.", file=sys.stderr)
        return 2
    longterm_chunk_days = max(1, int(args.longterm_chunk_days))
    fallback_pre_days = int(getattr(args, "fallback_pre_days", 7))
    fallback_post_days = int(getattr(args, "fallback_post_days", 2))

    if args.verbose:
        print(f"Polymarket config: gamma={args.gamma_url} clob={args.clob_url} fidelity={fidelity}m", flush=True)

    # Reuse the same event schedule and announced values logic.
    events = _generate_events(
        args.start_month,
        args.end_month,
        tz_name=args.tz,
        adp_time=args.adp_time,
        revelio_time=args.revelio_time,
        jobs_time=args.jobs_time,
        tz_fallback_offset=args.tz_fallback_offset,
    )
    _attach_announced_values(
        events,
        adp_history_csv=_resolve_repo_path("data/raw/ADP_NER_history.csv"),
        revelio_national_csv=_resolve_repo_path("data/raw/employment_national_revelio.csv"),
    )

    custom_path = _resolve_repo_path(args.custom_events) if args.custom_events else None
    if custom_path and custom_path.exists():
        custom_events = _load_events_from_csv(custom_path)
        if custom_events:
            events = _merge_events(events, custom_events)

    print("Resolving Polymarket NFP events…", flush=True)

    # Build per-payroll-month mapping.
    by_payroll_month: Dict[str, Dict[str, Any]] = {}
    payroll_months = sorted({str(e.get("payroll_month") or "") for e in events if e.get("payroll_month")})
    for payroll_month in payroll_months:
        # Use the Jobs report timestamp for the corresponding release month (closest window).
        jobs_report_ts = None
        for e in events:
            if str(e.get("payroll_month") or "") != payroll_month:
                continue
            jt = e.get("jobs_report_ts")
            if isinstance(jt, int):
                jobs_report_ts = jt
                break
        if jobs_report_ts is None:
            continue

        month = _month_name(payroll_month)
        candidates = _gamma_public_search_events(
            gamma_url=str(args.gamma_url),
            query=f"how many jobs added in {month}",
            cache_dir=cache_dir,
            limit_per_type=10,
            keep_closed_markets=1,
        )
        picked = _pick_best_event_from_candidates(candidates, payroll_month=payroll_month, jobs_report_ts=jobs_report_ts)
        if not picked:
            if args.verbose:
                print(f"  {payroll_month}: no matching event candidates", flush=True)
            continue
        slug = str(picked.get("slug") or "").strip()
        if not slug:
            continue
        full = _gamma_fetch_event_by_slug(gamma_url=str(args.gamma_url), slug=slug, cache_dir=cache_dir)
        if not full:
            if args.verbose:
                print(f"  {payroll_month}: unable to fetch event slug={slug}", flush=True)
            continue
        scheduled = _parse_scheduled_release_from_description(full.get("description"))
        scheduled_date = scheduled[0] if scheduled else None
        scheduled_hhmm = scheduled[1] if scheduled else None
        scheduled_jobs_ts = (
            _to_utc_ts(scheduled_date, scheduled_hhmm, str(args.tz), str(args.tz_fallback_offset))
            if scheduled_date and scheduled_hhmm
            else None
        )
        brackets = _extract_bracket_markets_from_event(full)
        if not brackets:
            if args.verbose:
                print(f"  {payroll_month}: event slug={slug} had no bracket markets", flush=True)
            continue
        by_payroll_month[payroll_month] = {
            "event_id": full.get("id"),
            "event_slug": full.get("slug") or slug,
            "event_title": full.get("title") or picked.get("title") or picked.get("question") or slug,
            "event_end_ts": _parse_iso_to_ts(full.get("endDate")),
            "scheduled_jobs_ts": scheduled_jobs_ts,
            "scheduled_jobs_date": scheduled_date.isoformat() if scheduled_date else None,
            "scheduled_jobs_time": scheduled_hhmm,
            "brackets": brackets,
        }
        if args.verbose:
            sched_label = (
                f" scheduled={scheduled_date.isoformat()} {scheduled_hhmm} ET"
                if scheduled_date and scheduled_hhmm
                else ""
            )
            print(f"  {payroll_month}: {slug} ({len(brackets)} brackets){sched_label}", flush=True)

    if args.verbose:
        print(f"Matched payroll months: {len(by_payroll_month)}/{len(payroll_months)}", flush=True)

    # Align our event timestamps to Polymarket's scheduled BLS release when available.
    for event in events:
        payroll_month = str(event.get("payroll_month") or "")
        if not payroll_month:
            continue
        mapping = by_payroll_month.get(payroll_month)
        jobs_ts = mapping.get("scheduled_jobs_ts") if mapping else None
        jobs_date_iso = mapping.get("scheduled_jobs_date") if mapping else None
        if not isinstance(jobs_ts, int) or not jobs_date_iso:
            continue
        try:
            jobs_date = dt.date.fromisoformat(str(jobs_date_iso))
        except ValueError:
            continue

        event["jobs_report_ts"] = int(jobs_ts)
        event["jobs_report_iso"] = _iso_from_ts(int(jobs_ts))

        typ = str(event.get("type") or "").lower()
        if typ == "revelio":
            rev_date = jobs_date - dt.timedelta(days=1)
            rev_ts = _to_utc_ts(rev_date, str(args.revelio_time), str(args.tz), str(args.tz_fallback_offset))
            event["release_ts"] = int(rev_ts)
            event["release_iso"] = _iso_from_ts(int(rev_ts))
        elif typ == "adp":
            adp_date = _previous_weekday(jobs_date, weekday=2)  # Wednesday
            adp_ts = _to_utc_ts(adp_date, str(args.adp_time), str(args.tz), str(args.tz_fallback_offset))
            event["release_ts"] = int(adp_ts)
            event["release_iso"] = _iso_from_ts(int(adp_ts))

    # Fetch price history per event.
    for event in events:
        payroll_month = str(event.get("payroll_month") or "")
        release_ts = int(event.get("release_ts") or 0)
        if not payroll_month or not release_ts:
            event["markets"] = []
            continue
        m = by_payroll_month.get(payroll_month)
        if not m:
            event["markets"] = []
            continue

        start_ts = release_ts - pre_minutes * 60
        end_ts = release_ts + post_minutes * 60
        longterm_start_ts = release_ts - longterm_days * 86400
        longterm_end_ts = release_ts + longterm_days * 86400
        brackets: List[Dict[str, Any]] = list(m.get("brackets") or [])
        event_markets: List[Dict[str, Any]] = []

        for idx, bracket in enumerate(brackets):
            outcome = str(bracket.get("label") or "")
            token_id = str(bracket.get("token_id") or "")
            strike = bracket.get("strike")
            if not token_id:
                continue
            try:
                candles_full, fetch_meta = _fetch_token_price_history_with_fallback(
                    clob_url=str(args.clob_url),
                    token_id=str(token_id),
                    start_ts=start_ts,
                    end_ts=end_ts,
                    fidelity_minutes=fidelity,
                    cache_dir=cache_dir,
                    fallback_pre_days=fallback_pre_days,
                    fallback_post_days=fallback_post_days,
                )
            except (
                HTTPError,
                URLError,
                json.JSONDecodeError,
                TimeoutError,
                socket.timeout,
                ssl.SSLError,
                http.client.IncompleteRead,
                http.client.RemoteDisconnected,
            ) as exc:
                print(f"Warning: prices-history failed for token {token_id}: {exc}", file=sys.stderr)
                candles_full = []
                fetch_meta = {"requested": {"start_ts": start_ts, "end_ts": end_ts}, "used_fallback": False, "fallback": None}
            longterm_candles: List[List[Optional[float]]] = []
            if longterm_days > 0:
                try:
                    longterm_candles = _fetch_token_price_history_chunked(
                        clob_url=str(args.clob_url),
                        token_id=str(token_id),
                        start_ts=longterm_start_ts,
                        end_ts=longterm_end_ts,
                        fidelity_minutes=longterm_fidelity,
                        cache_dir=cache_dir,
                        chunk_days=longterm_chunk_days,
                    )
                except (
                    HTTPError,
                    URLError,
                    json.JSONDecodeError,
                    TimeoutError,
                    socket.timeout,
                    ssl.SSLError,
                    http.client.IncompleteRead,
                    http.client.RemoteDisconnected,
                ) as exc:
                    print(f"Warning: long-term prices-history failed for token {token_id}: {exc}", file=sys.stderr)
                    longterm_candles = []

            baseline_ts, baseline_yes = _pick_baseline(candles_full, release_ts)
            candles = _clip_and_pad_candles(candles_full, start_ts=start_ts, end_ts=end_ts)
            levels: Dict[str, Optional[float]] = {}
            deltas: Dict[str, Optional[float]] = {}
            for mins in horizons:
                key = f"{mins}m"
                _t, yes = _pick_level_nearest_available(candles_full, release_ts + mins * 60)
                levels[key] = yes
                deltas[key] = (yes - baseline_yes) if (yes is not None and baseline_yes is not None) else None

            market_payload = {
                "ticker": str(token_id),
                "title": str(outcome),
                "strike": strike,
                "close_ts": None,
                "close_iso": None,
                "candles": candles,
                "summary": {
                    "baseline_ts": baseline_ts,
                    "baseline_yes": baseline_yes,
                    "levels": levels,
                    "deltas": deltas,
                    "volume": {"pre": None, "post": None},
                },
                "fetch": fetch_meta,
            }
            if longterm_days > 0:
                market_payload["longterm_candles"] = longterm_candles
            event_markets.append(market_payload)
            if idx % 8 == 0:
                time.sleep(0.05)

        event["polymarket"] = {
            "event_id": m.get("event_id"),
            "event_slug": m.get("event_slug"),
            "event_title": m.get("event_title"),
        }
        event["markets"] = event_markets
        event["summary"] = _summarize_event(
            event,
            horizons=horizons,
            pre_minutes=pre_minutes,
            post_minutes=post_minutes,
            stability_epsilon_pp=0.5,
        )

    dataset = {
        "generated_at": _iso_from_ts(int(time.time())),
        "polymarket": {
            "gamma_url": str(args.gamma_url),
            "clob_url": str(args.clob_url),
            "interval": "1m",
            "fidelity_minutes": fidelity,
            "window": {"pre_minutes": pre_minutes, "post_minutes": post_minutes},
            "horizons_minutes": horizons,
            "demo": False,
        },
        "events": events,
    }
    if longterm_days > 0:
        dataset["polymarket"]["longterm"] = {
            "days": longterm_days,
            "fidelity_minutes": longterm_fidelity,
            "chunk_days": longterm_chunk_days,
        }
    dataset["summary"] = _summarize_dataset(events, horizons=horizons)

    _write_json(out_path, dataset)
    print(f"Wrote dataset: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
