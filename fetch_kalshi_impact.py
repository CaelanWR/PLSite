#!/usr/bin/env python3
"""
Kalshi payroll market impact pipeline.

Generates `data/kalshi_impact.json`, which is consumed by `impact.html`.

Two modes:
  - Demo (no network): `python3 fetch_kalshi_impact.py --demo`
  - Live (requires Kalshi credentials + network access):
      KALSHI_KEY_ID="..." KALSHI_PRIVATE_KEY_PATH="/path/to/kalshi.key" python3 fetch_kalshi_impact.py --start-month 2025-07 --end-month 2025-12
      # or (if you already have a bearer token)
      KALSHI_TOKEN="..." python3 fetch_kalshi_impact.py --start-month 2025-07 --end-month 2025-12

The exported JSON includes:
  - Per-market deltas + volume for each event
  - Per-event summaries (mean/|mean| moves, volume-rate ratios, stabilization time)
  - Per-source summaries under the top-level `summary` key (ADP vs Revelio comparisons)

Notes:
  - Uses the Python stdlib; API-key signing uses either the `openssl` CLI or the optional `cryptography` package.
  - Candlestick endpoint details can vary; this script tries common variants.
  - Default event schedule is rule-based:
      * ADP: first Wednesday of release month @ 08:15 ET
      * Revelio (RPLS): day before Jobs Friday @ 08:30 ET
      * Jobs Friday: first Friday of release month @ 08:30 ET
"""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import hashlib
import json
import math
import os
import random
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from impact_common import (  # type: ignore[import-not-found]
    DEFAULT_TZ,
    _attach_announced_values,
    _first_weekday_of_month,
    _generate_events,
    _iso_from_ts,
    _parse_horizons,
    _parse_year_month,
    _pick_baseline,
    _pick_level_at_or_after,
    _prev_month,
    _summarize_dataset,
    _summarize_event,
    _to_utc_ts,
    _ym_string,
)

try:  # Optional dependency for API-key auth signing (not required)
    from cryptography.hazmat.primitives import hashes, serialization  # type: ignore[import-not-found]
    from cryptography.hazmat.primitives.asymmetric import padding  # type: ignore[import-not-found]

    _HAS_CRYPTOGRAPHY = True
except Exception:  # pragma: no cover
    _HAS_CRYPTOGRAPHY = False

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
DEFAULT_SERIES_TICKER = "KXPAYROLLS"
MIN_REQUEST_INTERVAL_S = 0.25
MAX_RETRY_ATTEMPTS = 6
RETRY_BACKOFF_BASE_S = 1.0
_LAST_REQUEST_AT = 0.0


def _throttle_requests(min_interval_s: float = MIN_REQUEST_INTERVAL_S) -> None:
    global _LAST_REQUEST_AT
    now = time.time()
    wait = min_interval_s - (now - _LAST_REQUEST_AT)
    if wait > 0:
        time.sleep(wait)
    _LAST_REQUEST_AT = time.time()


def _retry_sleep_for(exc: Exception, attempt: int) -> float:
    retry_after = None
    if isinstance(exc, HTTPError) and exc.headers:
        raw = exc.headers.get("Retry-After")
        if raw:
            try:
                retry_after = float(raw)
            except ValueError:
                retry_after = None
    base = RETRY_BACKOFF_BASE_S * (2 ** attempt)
    jitter = random.uniform(0.0, 0.5)
    if retry_after is None:
        return base + jitter
    return max(retry_after, base) + jitter


def _interval_to_period_minutes(value: str) -> int:
    raw = str(value or "").strip().lower()
    if not raw:
        return 1
    if raw.isdigit():
        minutes = int(raw)
        if minutes <= 0:
            raise ValueError(f"Invalid interval minutes: {value!r}")
        return minutes
    match = re.match(r"^(\d+)\s*([mhd])$", raw)
    if not match:
        raise ValueError(f"Invalid interval format (expected 1m/1h/1d): {value!r}")
    n = int(match.group(1))
    unit = match.group(2)
    if n <= 0:
        raise ValueError(f"Invalid interval: {value!r}")
    if unit == "m":
        return n
    if unit == "h":
        return n * 60
    if unit == "d":
        return n * 1440
    raise ValueError(f"Invalid interval unit: {value!r}")


def _sha1(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def _http_get_json(url: str, headers: Dict[str, str], timeout_s: int = 30) -> Any:
    request = Request(url, headers=headers, method="GET")
    with urlopen(request, timeout=timeout_s) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        body = response.read().decode(charset)
        return json.loads(body)

def _kalshi_sign_pss_sha256(private_key_path: Path, message: bytes) -> str:
    # Preferred: cryptography (if installed).
    if _HAS_CRYPTOGRAPHY:
        try:
            private_key = serialization.load_pem_private_key(private_key_path.read_bytes(), password=None)
            signature = private_key.sign(
                message,
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                hashes.SHA256(),
            )
            return base64.b64encode(signature).decode("ascii")
        except Exception:
            pass

    # Fallback: OpenSSL CLI (commonly available on macOS/Linux).
    # Kalshi requires RSA-PSS with SHA256 and salt length == digest length.
    candidates: List[List[str]] = [
        [
            "openssl",
            "dgst",
            "-sha256",
            "-sign",
            str(private_key_path),
            "-sigopt",
            "rsa_padding_mode:pss",
            "-sigopt",
            "rsa_pss_saltlen:-1",
            "-binary",
        ],
        [
            "openssl",
            "pkeyutl",
            "-sign",
            "-inkey",
            str(private_key_path),
            "-pkeyopt",
            "digest:sha256",
            "-pkeyopt",
            "rsa_padding_mode:pss",
            "-pkeyopt",
            "rsa_pss_saltlen:-1",
        ],
    ]
    last_err = ""
    for cmd in candidates:
        try:
            proc = subprocess.run(
                cmd,
                input=message,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
        except FileNotFoundError as exc:  # pragma: no cover
            raise RuntimeError(
                "OpenSSL not found. Install OpenSSL or `pip install cryptography`, "
                "or use bearer token auth via --token / KALSHI_TOKEN."
            ) from exc
        except subprocess.CalledProcessError as exc:
            last_err = (exc.stderr or b"").decode("utf-8", errors="replace").strip()
            continue
        if proc.stdout:
            return base64.b64encode(proc.stdout).decode("ascii")
        last_err = (proc.stderr or b"").decode("utf-8", errors="replace").strip()

    raise RuntimeError(f"Kalshi signature generation failed. {last_err}".strip())


def _kalshi_auth_headers(
    auth: Dict[str, Any], *, url: str, method: str, user_agent: str = "Newsfeed/kalshi-impact"
) -> Dict[str, str]:
    """
    Supports:
      - Bearer token: `Authorization: Bearer ...`
      - API key auth: `KALSHI-ACCESS-*` headers (key id + RSA-PSS-SHA256 signature)
        See: https://docs.kalshi.com/getting_started/quick_start_authenticated_requests
    """
    headers: Dict[str, str] = {
        "User-Agent": user_agent,
        "Accept": "application/json",
    }
    kind = str(auth.get("kind") or "")
    if kind == "bearer":
        token = str(auth.get("token") or "").strip()
        if not token:
            raise ValueError("Kalshi bearer token missing.")
        headers["Authorization"] = f"Bearer {token}"
        return headers

    if kind == "key":
        key_id = str(auth.get("key_id") or "").strip()
        private_key_path_raw = str(auth.get("private_key_path") or "").strip()
        private_key_path = Path(private_key_path_raw).expanduser()
        if not key_id:
            raise ValueError("Kalshi API key id missing.")
        if not private_key_path_raw:
            raise ValueError("Kalshi private key path missing.")
        if not private_key_path.exists():
            raise ValueError(f"Kalshi private key file not found: {private_key_path}")

        ts_ms = int(time.time() * 1000)
        parsed = urlparse(url)
        sign_path = parsed.path  # must exclude query params per Kalshi docs
        message = f"{ts_ms}{method.upper()}{sign_path}".encode("utf-8")
        signature_b64 = _kalshi_sign_pss_sha256(private_key_path, message)

        headers.update(
            {
                "KALSHI-ACCESS-KEY": key_id,
                "KALSHI-ACCESS-TIMESTAMP": str(ts_ms),
                "KALSHI-ACCESS-SIGNATURE": signature_b64,
            }
        )
        return headers

    raise ValueError("Kalshi auth missing. Provide --token (bearer) or --key-id + --private-key-path.")


def _fetch_cached_json(
    *,
    base_url: str,
    path: str,
    params: Dict[str, Any],
    auth: Dict[str, Any],
    cache_dir: Path,
    cache_namespace: str,
    timeout_s: int = 30,
) -> Any:
    query = urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{query}"

    cache_key = _sha1(f"{cache_namespace}:{url}")
    cache_path = cache_dir / f"{cache_key}.json"
    if cache_path.exists():
        return _read_json(cache_path)

    headers = _kalshi_auth_headers(auth, url=url, method="GET")

    attempt = 0
    while True:
        _throttle_requests()
        try:
            payload = _http_get_json(url, headers=headers, timeout_s=timeout_s)
            cache_dir.mkdir(parents=True, exist_ok=True)
            _write_json(cache_path, payload)
            return payload
        except HTTPError as exc:
            status = getattr(exc, "code", None)
            if status == 429 or (isinstance(status, int) and 500 <= status < 600):
                if attempt >= MAX_RETRY_ATTEMPTS:
                    raise
                time.sleep(_retry_sleep_for(exc, attempt))
                attempt += 1
                continue
            raise
        except (URLError, json.JSONDecodeError) as exc:
            if attempt >= MAX_RETRY_ATTEMPTS:
                raise
            time.sleep(_retry_sleep_for(exc, attempt))
            attempt += 1


def _market_close_ts(market: Dict[str, Any]) -> Optional[int]:
    for key in ("close_time", "expiration_time", "settlement_time"):
        value = market.get(key)
        if value is None:
            continue
        if isinstance(value, (int, float)) and math.isfinite(value):
            return int(value)
        s = str(value).strip()
        if not s:
            continue
        if s.isdigit():
            try:
                return int(s)
            except ValueError:
                continue
        try:
            parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            return int(parsed.timestamp())
        except ValueError:
            continue
    return None


def _parse_strike(title: str) -> Optional[Dict[str, Any]]:
    if not title:
        return None
    lowered = title.lower()
    match = re.search(r"\babove\s+(-?\d[\d,]*)(?:\s*(k|m))?\b", lowered)
    if not match:
        return None
    raw = match.group(1).replace(",", "")
    unit = match.group(2)
    try:
        value = int(raw)
    except ValueError:
        return None
    if unit == "k":
        value *= 1000
    elif unit == "m":
        value *= 1_000_000
    label = f"Above {value:,}"
    if abs(value) >= 1000 and value % 1000 == 0:
        label = f"Above {int(value / 1000):,}K"
    return {"kind": "above", "value": value, "label": label}


def _load_events_from_csv(path: Path) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            event_type = (row.get("type") or "").strip().lower()
            release_iso = (row.get("release_iso") or row.get("release") or "").strip()
            release_month = (row.get("release_month") or "").strip()
            payroll_month = (row.get("payroll_month") or "").strip()
            label = (row.get("label") or "").strip()
            event_id = (row.get("id") or row.get("event_id") or "").strip()
            if not event_type or not release_iso:
                continue

            parsed = release_iso.replace("Z", "+00:00")
            try:
                release_dt = dt.datetime.fromisoformat(parsed)
            except ValueError:
                print(f"Warning: invalid release_iso in {path}: {release_iso!r}", file=sys.stderr)
                continue
            release_ts = int(release_dt.timestamp())

            if not release_month:
                release_month = release_dt.strftime("%Y-%m")
            if not payroll_month and release_month:
                y, m = _parse_year_month(release_month)
                py, pm = _prev_month(y, m)
                payroll_month = _ym_string(py, pm)
            if not event_id:
                event_id = f"{release_month}-{event_type}"

            actual = row.get("actual")
            expected = row.get("expected")
            unit = (row.get("unit") or "jobs").strip()

            value: Optional[Dict[str, Any]] = None
            if actual is not None or expected is not None:
                def _to_float(v: Optional[str]) -> Optional[float]:
                    if v is None:
                        return None
                    v = v.strip()
                    if not v:
                        return None
                    try:
                        return float(v.replace(",", ""))
                    except ValueError:
                        return None

                act = _to_float(actual)
                exp = _to_float(expected)
                if act is not None or exp is not None:
                    value = {"actual": act, "expected": exp, "unit": unit}

            events.append(
                {
                    "id": event_id,
                    "type": event_type,
                    "label": label or event_type.upper(),
                    "release_month": release_month,
                    "payroll_month": payroll_month,
                    "release_ts": release_ts,
                    "release_iso": _iso_from_ts(release_ts),
                    "value": value,
                }
            )
    events.sort(key=lambda e: int(e["release_ts"]))
    return events


def _fetch_markets(
    *,
    base_url: str,
    auth: Dict[str, Any],
    series_ticker: Optional[str],
    cache_dir: Path,
    limit: int = 200,
    max_pages: int = 50,
) -> List[Dict[str, Any]]:
    markets: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    for _ in range(max_pages):
        params: Dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        # Some Kalshi deployments support server-side filtering; if not, we'll filter client-side.
        if series_ticker:
            params["series_ticker"] = series_ticker

        try:
            payload = _fetch_cached_json(
                base_url=base_url,
                path="/markets",
                params=params,
                auth=auth,
                cache_dir=cache_dir,
                cache_namespace="markets",
            )
        except HTTPError as exc:
            if exc.code == 400 and series_ticker and "series_ticker" in params:
                # Retry without unsupported filter.
                params.pop("series_ticker", None)
                payload = _fetch_cached_json(
                    base_url=base_url,
                    path="/markets",
                    params=params,
                    auth=auth,
                    cache_dir=cache_dir,
                    cache_namespace="markets",
                )
            else:
                raise

        batch = payload.get("markets") if isinstance(payload, dict) else None
        if not isinstance(batch, list) or not batch:
            break
        markets.extend(batch)

        cursor = payload.get("cursor") or payload.get("next_cursor") or payload.get("nextCursor")
        if not cursor:
            break
        time.sleep(0.15)
    return markets


def _select_payroll_markets_for_jobs_report(
    markets: List[Dict[str, Any]],
    *,
    series_ticker: str,
    jobs_report_ts: int,
    close_window_hours: int = 18,
) -> List[Dict[str, Any]]:
    # Backwards-compatible wrapper; this heuristic worked for some Kalshi deployments
    # where markets closed shortly after the Jobs report. Keep it available, but the
    # preferred selector is `_select_payroll_markets_for_payroll_month`.
    window_s = close_window_hours * 3600
    out: List[Dict[str, Any]] = []
    for market in markets:
        st = str(market.get("series_ticker") or "")
        ticker = str(market.get("ticker") or "")
        if st and st != series_ticker:
            continue
        if not st and not ticker.startswith(f"{series_ticker}-"):
            continue
        close_ts = _market_close_ts(market)
        if close_ts is None:
            continue
        if abs(close_ts - jobs_report_ts) > window_s:
            continue
        out.append(market)
    return out


def _kalshi_event_suffix(payroll_month: str) -> str:
    y, m = _parse_year_month(payroll_month)
    abbr = dt.date(y, m, 1).strftime("%b").upper()
    return f"{y % 100:02d}{abbr}"


def _select_payroll_markets_for_payroll_month(
    markets: List[Dict[str, Any]],
    *,
    series_ticker: str,
    payroll_month: str,
) -> List[Dict[str, Any]]:
    """
    Select the strike ladder for a given payroll month.

    In the elections API, markets commonly have:
      - `event_ticker` like `KXPAYROLLS-25NOV`
      - per-strike tickers like `KXPAYROLLS-25NOV-T75000`
    """
    suffix = _kalshi_event_suffix(payroll_month)
    event_ticker = f"{series_ticker}-{suffix}"
    prefix = f"{event_ticker}-"
    out: List[Dict[str, Any]] = []
    for market in markets:
        et = str(market.get("event_ticker") or "")
        ticker = str(market.get("ticker") or "")
        if et == event_ticker or ticker.startswith(prefix):
            out.append(market)
    return out


def _parse_candle_ts(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        return int(value)
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        try:
            return int(s)
        except ValueError:
            return None
    # ISO-ish fallback
    try:
        parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(parsed.timestamp())
    except ValueError:
        return None


def _parse_candles(payload: Any) -> List[List[Optional[float]]]:
    if not isinstance(payload, dict):
        return []
    raw = payload.get("candlesticks") or payload.get("candles") or payload.get("data") or []
    if not isinstance(raw, list):
        return []
    out: List[List[Optional[float]]] = []
    for candle in raw:
        if not isinstance(candle, dict):
            continue
        ts = (
            candle.get("end_period_ts")
            or candle.get("end_ts")
            or candle.get("start_ts")
            or candle.get("start_time")
            or candle.get("ts")
            or candle.get("t")
            or candle.get("time")
            or candle.get("timestamp")
        )
        ts_parsed = _parse_candle_ts(ts)
        if ts_parsed is None:
            continue

        yes = None
        price_obj = candle.get("price") if isinstance(candle.get("price"), dict) else None
        if price_obj:
            for key in ("close", "open", "previous", "mean"):
                if key not in price_obj:
                    continue
                try:
                    raw_yes = price_obj[key]
                    yes = float(raw_yes) if raw_yes is not None else None
                except (TypeError, ValueError):
                    yes = None
                if yes is not None:
                    break
        if yes is None:
            for key in ("yes_close", "close", "yes", "yes_price", "last_price", "close_yes"):
                if key not in candle:
                    continue
                try:
                    yes = float(candle[key])
                except (TypeError, ValueError):
                    yes = None
                break

        vol = None
        for key in ("volume", "trade_volume", "trades", "count"):
            if key not in candle:
                continue
            try:
                vol = float(candle[key])
            except (TypeError, ValueError):
                vol = None
            break

        out.append([float(ts_parsed), yes, vol])

    out.sort(key=lambda row: row[0] if row and row[0] is not None else 0)
    return out


def _fetch_candles(
    *,
    base_url: str,
    auth: Dict[str, Any],
    series_ticker: str,
    market_ticker: str,
    start_ts: int,
    end_ts: int,
    interval: str,
    cache_dir: Path,
) -> List[List[Optional[float]]]:
    period_minutes = _interval_to_period_minutes(interval)
    endpoint_variants = [
        # Documented endpoints (see https://docs.kalshi.com/api-reference/market/get-market-candlesticks)
        f"/series/{series_ticker}/markets/{market_ticker}/candlesticks",
        # Fallback: batch endpoint (single ticker)
        "/markets/candlesticks",
    ]
    param_variants = [
        {"start_ts": start_ts, "end_ts": end_ts, "period_interval": period_minutes},
    ]

    last_error: Optional[Exception] = None
    for path in endpoint_variants:
        for params in param_variants:
            try:
                if path == "/markets/candlesticks":
                    params = dict(params)
                    params["market_tickers"] = market_ticker
                payload = _fetch_cached_json(
                    base_url=base_url,
                    path=path,
                    params=params,
                    auth=auth,
                    cache_dir=cache_dir,
                    cache_namespace="candles",
                    timeout_s=45,
                )
                # Batch endpoint returns a slightly different wrapper shape; normalize.
                if path == "/markets/candlesticks" and isinstance(payload, dict):
                    payload = (
                        payload.get("candlesticks")
                        or payload.get("market_candlesticks")
                        or payload.get("data")
                        or payload
                    )
                    if isinstance(payload, dict) and isinstance(payload.get("candlesticks"), list):
                        # Sometimes the response is already single-market shaped.
                        pass
                    elif isinstance(payload, list):
                        # Try to locate the entry for our ticker.
                        chosen = None
                        for entry in payload:
                            if not isinstance(entry, dict):
                                continue
                            if str(entry.get("ticker") or entry.get("market_ticker") or "") == market_ticker:
                                chosen = entry
                                break
                        if chosen is not None:
                            payload = chosen
                candles = _parse_candles(payload)
                if candles:
                    return candles
            except (HTTPError, URLError, json.JSONDecodeError) as exc:
                last_error = exc
                continue
    if last_error:
        raise last_error
    return []


def _sum_volume(candles: List[List[Optional[float]]], start_ts: int, end_ts: int) -> Optional[float]:
    total = 0.0
    seen = False
    for ts, _yes, vol in candles:
        if ts is None or vol is None:
            continue
        if ts < start_ts:
            continue
        if ts >= end_ts:
            break
        total += vol
        seen = True
    return total if seen else None


def _build_demo_dataset(out_path: Path) -> None:
    random.seed(7)
    now = dt.datetime.now(tz=dt.timezone.utc)
    base_release = int((now - dt.timedelta(days=7)).timestamp())

    def make_candles(release_ts: int, shock_pp: float) -> List[List[Optional[float]]]:
        points: List[List[Optional[float]]] = []
        price = 55.0 + random.uniform(-3, 3)
        for i in range(-30, 241):
            ts = release_ts + i * 60
            drift = random.uniform(-0.15, 0.15)
            if i == 0:
                drift += shock_pp
            price = max(1.0, min(99.0, price + drift))
            vol = max(0.0, random.gauss(120, 30))
            points.append([float(ts), float(price), float(vol)])
        return points

    strikes = [0, 50_000, 100_000, 150_000, 200_000]

    def mk_market(strike: int, release_ts: int, shock: float) -> Dict[str, Any]:
        candles = make_candles(release_ts, shock_pp=shock)
        baseline_ts, baseline_yes = _pick_baseline(candles, release_ts)
        levels: Dict[str, Optional[float]] = {}
        deltas: Dict[str, Optional[float]] = {}
        for key, mins in (("5m", 5), ("30m", 30), ("60m", 60), ("240m", 240)):
            _t, yes = _pick_level_at_or_after(candles, release_ts + mins * 60)
            levels[key] = yes
            deltas[key] = (yes - baseline_yes) if (yes is not None and baseline_yes is not None) else None
        return {
            "ticker": f"KXPAYROLLS-DEMO-ABOVE-{strike}",
            "title": f"Demo payrolls above {strike:,}",
            "strike": {"kind": "above", "value": strike, "label": f"Above {int(strike/1000):,}K" if strike else "Above 0"},
            "close_ts": release_ts + 7 * 24 * 3600,
            "close_iso": _iso_from_ts(release_ts + 7 * 24 * 3600),
            "candles": candles,
            "summary": {
                "baseline_ts": baseline_ts,
                "baseline_yes": baseline_yes,
                "levels": levels,
                "deltas": deltas,
                "volume": {
                    "pre": _sum_volume(candles, release_ts - 30 * 60, release_ts),
                    "post": _sum_volume(candles, release_ts, release_ts + 240 * 60),
                },
            },
        }

    events: List[Dict[str, Any]] = []
    for idx, event_type in enumerate(("adp", "revelio")):
        release_ts = base_release + idx * 24 * 3600
        shock = -1.2 if event_type == "adp" else -5.0
        markets = [mk_market(s, release_ts, shock * (0.4 + (s / 200_000))) for s in strikes]
        events.append(
            {
                "id": f"demo-{event_type}",
                "type": event_type,
                "label": "Demo event",
                "release_month": "2025-12",
                "payroll_month": "2025-11",
                "release_ts": release_ts,
                "release_iso": _iso_from_ts(release_ts),
                "jobs_report_ts": release_ts + 2 * 24 * 3600,
                "jobs_report_iso": _iso_from_ts(release_ts + 2 * 24 * 3600),
                "value": {"actual": -32_000 if event_type == "adp" else -9_000, "expected": 40_000, "unit": "jobs"},
                "markets": markets,
            }
        )

    dataset = {
        "generated_at": _iso_from_ts(int(now.timestamp())),
        "kalshi": {
            "base_url": KALSHI_BASE_URL,
            "series_ticker": DEFAULT_SERIES_TICKER,
            "interval": "1m",
            "window": {"pre_minutes": 30, "post_minutes": 240},
            "horizons_minutes": [5, 30, 60, 240],
            "demo": True,
        },
        "events": events,
    }

    for event in dataset["events"]:
        event["summary"] = _summarize_event(
            event, horizons=[5, 30, 60, 240], pre_minutes=30, post_minutes=240, stability_epsilon_pp=0.5
        )
    dataset["summary"] = _summarize_dataset(dataset["events"], horizons=[5, 30, 60, 240])

    _write_json(out_path, dataset)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Kalshi candlesticks around payroll releases and export JSON.")
    parser.add_argument("--out", default="data/kalshi_impact.json", help="Output JSON path (default: data/kalshi_impact.json)")
    parser.add_argument("--demo", action="store_true", help="Write a demo dataset (no network).")
    parser.add_argument("--token", default=os.environ.get("KALSHI_TOKEN", ""), help="Kalshi bearer token (or set KALSHI_TOKEN).")
    parser.add_argument("--key-id", default=os.environ.get("KALSHI_KEY_ID", ""), help="Kalshi API key id (or set KALSHI_KEY_ID).")
    parser.add_argument(
        "--private-key-path",
        default=os.environ.get("KALSHI_PRIVATE_KEY_PATH", ""),
        help="Path to Kalshi private key .key file (or set KALSHI_PRIVATE_KEY_PATH).",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("KALSHI_BASE_URL", KALSHI_BASE_URL),
        help=f"Kalshi API base URL (default: {KALSHI_BASE_URL})",
    )
    parser.add_argument("--verbose", action="store_true", help="Print progress details.")
    parser.add_argument("--series", default=DEFAULT_SERIES_TICKER, help=f"Kalshi series ticker (default: {DEFAULT_SERIES_TICKER})")
    parser.add_argument("--start-month", default="", help="Start release month (YYYY-MM), e.g. 2025-07")
    parser.add_argument("--end-month", default="", help="End release month (YYYY-MM), e.g. 2025-12")
    parser.add_argument("--events-csv", default="", help="Optional CSV of events with actual/expected values.")
    parser.add_argument("--adp-time", default="08:15", help="ADP release time in ET (HH:MM).")
    parser.add_argument("--revelio-time", default="08:30", help="Revelio release time in ET (HH:MM).")
    parser.add_argument("--jobs-time", default="08:30", help="Jobs report time in ET (HH:MM).")
    parser.add_argument("--tz", default=DEFAULT_TZ, help=f"Timezone name (default: {DEFAULT_TZ}).")
    parser.add_argument("--tz-fallback-offset", default="-05:00", help="Fallback offset if tz database missing (default: -05:00).")
    parser.add_argument("--cache-dir", default="data/kalshi_cache", help="Cache directory for API responses.")
    parser.add_argument(
        "--adp-history-csv",
        default="ADP_NER_history.csv",
        help="Local ADP history CSV used to infer announced MoM change (default: ADP_NER_history.csv).",
    )
    parser.add_argument(
        "--revelio-national-csv",
        default="employment_national_revelio.csv",
        help="Local Revelio national totals CSV used to infer announced MoM change (default: employment_national_revelio.csv).",
    )
    parser.add_argument("--interval", default="1m", help="Candlestick granularity (e.g., 1m, 1h, 1d).")
    parser.add_argument("--horizons", default="5,30,60,240", help="Comma-separated post-release horizons in minutes.")
    parser.add_argument("--pre-minutes", type=int, default=30, help="Minutes before release to fetch.")
    parser.add_argument("--post-minutes", type=int, default=240, help="Minutes after release to fetch.")
    parser.add_argument(
        "--stability-epsilon-pp",
        type=float,
        default=0.5,
        help="Stabilization band in Yes price percentage points (default: 0.5).",
    )
    args = parser.parse_args()

    out_path = Path(args.out)
    if args.demo:
        _build_demo_dataset(out_path)
        print(f"Wrote demo dataset: {out_path}")
        return 0

    base_url = str(args.base_url).rstrip("/")

    auth: Dict[str, Any]
    token = (args.token or "").strip()
    if token:
        auth = {"kind": "bearer", "token": token}
    else:
        key_id = (args.key_id or "").strip()
        private_key_path = (args.private_key_path or "").strip()
        if key_id and private_key_path:
            auth = {"kind": "key", "key_id": key_id, "private_key_path": private_key_path}
        else:
            print(
                "Error: Kalshi credentials missing. Provide --token (KALSHI_TOKEN) OR --key-id + --private-key-path "
                "(KALSHI_KEY_ID + KALSHI_PRIVATE_KEY_PATH).",
                file=sys.stderr,
            )
            return 2

    cache_dir = Path(args.cache_dir)
    if args.verbose:
        print(
            f"Kalshi config: base_url={base_url} series={args.series} auth={auth.get('kind')}",
            flush=True,
        )

    if args.events_csv:
        events = _load_events_from_csv(Path(args.events_csv))
        if not events:
            print(f"Error: no events loaded from {args.events_csv}", file=sys.stderr)
            return 2
        # Ensure we have jobs_report_ts for market selection; infer if missing.
        by_release_month: Dict[str, int] = {}
        for event in events:
            rm = event.get("release_month")
            if not rm:
                continue
            y, m = _parse_year_month(str(rm))
            jobs_date = _first_weekday_of_month(y, m, weekday=4)
            by_release_month[str(rm)] = _to_utc_ts(jobs_date, args.jobs_time, args.tz, args.tz_fallback_offset)
        for event in events:
            rm = event.get("release_month")
            if rm and "jobs_report_ts" not in event:
                jobs_ts = by_release_month.get(str(rm))
                if jobs_ts:
                    event["jobs_report_ts"] = jobs_ts
                    event["jobs_report_iso"] = _iso_from_ts(jobs_ts)
    else:
        if not args.start_month or not args.end_month:
            print("Error: --start-month and --end-month are required (or pass --events-csv or use --demo).", file=sys.stderr)
            return 2
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
            adp_history_csv=Path(args.adp_history_csv) if args.adp_history_csv else None,
            revelio_national_csv=Path(args.revelio_national_csv) if args.revelio_national_csv else None,
        )

    pre_minutes = int(args.pre_minutes)
    post_minutes = int(args.post_minutes)
    horizons = _parse_horizons(args.horizons)
    if not horizons:
        print("Error: --horizons must contain at least one positive integer minute value.", file=sys.stderr)
        return 2
    if max(horizons) > post_minutes:
        print("Error: --post-minutes must be >= max(--horizons).", file=sys.stderr)
        return 2
    interval = str(args.interval)
    try:
        _interval_to_period_minutes(interval)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    series_ticker = str(args.series)
    stability_epsilon_pp = float(args.stability_epsilon_pp)

    print("Fetching markets…", flush=True)
    markets = _fetch_markets(base_url=base_url, auth=auth, series_ticker=series_ticker, cache_dir=cache_dir)
    if not markets:
        print("Error: no markets returned from /markets.", file=sys.stderr)
        return 1
    if args.verbose:
        kept = []
        for m in markets:
            if not isinstance(m, dict):
                continue
            st = str(m.get("series_ticker") or "")
            ticker = str(m.get("ticker") or "")
            if st:
                if st == series_ticker:
                    kept.append(m)
            elif ticker.startswith(f"{series_ticker}-"):
                kept.append(m)
        print(f"Loaded {len(markets)} markets ({len(kept)} matching {series_ticker}).", flush=True)

    jobs_markets_by_month: Dict[str, List[Dict[str, Any]]] = {}
    for event in events:
        release_month = str(event.get("release_month") or "")
        payroll_month = str(event.get("payroll_month") or "")
        if not release_month or not payroll_month:
            continue
        if release_month in jobs_markets_by_month:
            continue
        selected = _select_payroll_markets_for_payroll_month(
            markets, series_ticker=series_ticker, payroll_month=payroll_month
        )
        jobs_markets_by_month[release_month] = selected

    for event in events:
        release_month = str(event.get("release_month") or "")
        release_ts = int(event["release_ts"])
        start_ts = release_ts - pre_minutes * 60
        end_ts = release_ts + post_minutes * 60

        market_list = jobs_markets_by_month.get(release_month, [])
        if not market_list:
            event["markets"] = []
            continue

        event_markets: List[Dict[str, Any]] = []
        for i, market in enumerate(sorted(market_list, key=lambda m: str(m.get("ticker") or ""))):
            ticker = str(market.get("ticker") or "").strip()
            title = str(market.get("title") or "").strip()
            if not ticker:
                continue
            try:
                candles = _fetch_candles(
                    base_url=base_url,
                    auth=auth,
                    series_ticker=series_ticker,
                    market_ticker=ticker,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    interval=interval,
                    cache_dir=cache_dir,
                )
            except Exception as exc:
                print(f"Warning: candlesticks failed for {ticker}: {exc}", file=sys.stderr)
                candles = []

            close_ts = _market_close_ts(market)
            strike = _parse_strike(title)

            baseline_ts, baseline_yes = _pick_baseline(candles, release_ts)
            levels: Dict[str, Optional[float]] = {}
            deltas: Dict[str, Optional[float]] = {}
            for mins in horizons:
                key = f"{mins}m"
                _t, yes = _pick_level_at_or_after(candles, release_ts + mins * 60)
                levels[key] = yes
                deltas[key] = (yes - baseline_yes) if (yes is not None and baseline_yes is not None) else None

            event_markets.append(
                {
                    "ticker": ticker,
                    "title": title,
                    "strike": strike,
                    "close_ts": close_ts,
                    "close_iso": _iso_from_ts(close_ts) if close_ts else None,
                    "candles": candles,
                    "summary": {
                        "baseline_ts": baseline_ts,
                        "baseline_yes": baseline_yes,
                        "levels": levels,
                        "deltas": deltas,
                        "volume": {
                            "pre": _sum_volume(candles, start_ts, release_ts),
                            "post": _sum_volume(candles, release_ts, end_ts),
                        },
                    },
                }
            )
            if i % 10 == 0:
                time.sleep(0.1)

        event["markets"] = event_markets
        event["summary"] = _summarize_event(
            event,
            horizons=horizons,
            pre_minutes=pre_minutes,
            post_minutes=post_minutes,
            stability_epsilon_pp=stability_epsilon_pp,
        )

    dataset = {
        "generated_at": _iso_from_ts(int(time.time())),
        "kalshi": {
            "base_url": base_url,
            "series_ticker": series_ticker,
            "interval": interval,
            "window": {"pre_minutes": pre_minutes, "post_minutes": post_minutes},
            "horizons_minutes": horizons,
            "demo": False,
        },
        "events": events,
    }
    dataset["summary"] = _summarize_dataset(events, horizons=horizons)

    _write_json(out_path, dataset)
    print(f"Wrote dataset: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
