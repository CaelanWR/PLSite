from __future__ import annotations

import csv
import datetime as dt
import math
import re
import statistics
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]

DEFAULT_TZ = "America/New_York"


def _mean(values: List[float]) -> Optional[float]:
    cleaned = [v for v in values if isinstance(v, (int, float)) and math.isfinite(v)]
    if not cleaned:
        return None
    return float(statistics.fmean(cleaned))


def _median(values: List[float]) -> Optional[float]:
    cleaned = [v for v in values if isinstance(v, (int, float)) and math.isfinite(v)]
    if not cleaned:
        return None
    return float(statistics.median(cleaned))


def _pearson_r(xs: List[float], ys: List[float]) -> Optional[float]:
    cleaned: List[Tuple[float, float]] = []
    for x, y in zip(xs, ys):
        if not (isinstance(x, (int, float)) and math.isfinite(x)):
            continue
        if not (isinstance(y, (int, float)) and math.isfinite(y)):
            continue
        cleaned.append((float(x), float(y)))
    if len(cleaned) < 2:
        return None
    mx = statistics.fmean([x for x, _y in cleaned])
    my = statistics.fmean([y for _x, y in cleaned])
    sxx = 0.0
    syy = 0.0
    sxy = 0.0
    for x, y in cleaned:
        dx = x - mx
        dy = y - my
        sxx += dx * dx
        syy += dy * dy
        sxy += dx * dy
    if sxx <= 0.0 or syy <= 0.0:
        return None
    return float(sxy / math.sqrt(sxx * syy))


def _parse_horizons(value: str) -> List[int]:
    out: List[int] = []
    for part in str(value or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            minutes = int(part)
        except ValueError:
            continue
        if minutes <= 0:
            continue
        out.append(minutes)
    out = sorted(set(out))
    return out


def _parse_year_month(value: str) -> Tuple[int, int]:
    match = re.match(r"^(\d{4})-(\d{2})$", value.strip())
    if not match:
        raise ValueError(f"Invalid YYYY-MM: {value!r}")
    year = int(match.group(1))
    month = int(match.group(2))
    if month < 1 or month > 12:
        raise ValueError(f"Invalid month: {value!r}")
    return year, month


def _iter_year_months(start_ym: str, end_ym: str) -> Iterable[Tuple[int, int]]:
    sy, sm = _parse_year_month(start_ym)
    ey, em = _parse_year_month(end_ym)
    cur_y, cur_m = sy, sm
    while (cur_y, cur_m) <= (ey, em):
        yield cur_y, cur_m
        cur_m += 1
        if cur_m == 13:
            cur_m = 1
            cur_y += 1


def _ym_string(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _prev_month(year: int, month: int) -> Tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _first_weekday_of_month(year: int, month: int, weekday: int) -> dt.date:
    """weekday: Monday=0, ..., Sunday=6"""
    d = dt.date(year, month, 1)
    while d.weekday() != weekday:
        d += dt.timedelta(days=1)
    return d


def _parse_hhmm(value: str) -> Tuple[int, int]:
    match = re.match(r"^(\d{1,2}):(\d{2})$", value.strip())
    if not match:
        raise ValueError(f"Invalid HH:MM: {value!r}")
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"Invalid time: {value!r}")
    return hour, minute


def _parse_fixed_offset(value: str) -> dt.tzinfo:
    match = re.match(r"^([+-])(\d{2}):(\d{2})$", value.strip())
    if not match:
        raise ValueError(f"Invalid UTC offset (+HH:MM): {value!r}")
    sign = -1 if match.group(1) == "-" else 1
    hours = int(match.group(2))
    minutes = int(match.group(3))
    return dt.timezone(sign * dt.timedelta(hours=hours, minutes=minutes))


def _to_utc_ts(date: dt.date, hhmm: str, tz_name: str, fallback_offset: str) -> int:
    hour, minute = _parse_hhmm(hhmm)
    naive = dt.datetime.combine(date, dt.time(hour=hour, minute=minute))
    if ZoneInfo is not None:
        try:
            local = naive.replace(tzinfo=ZoneInfo(tz_name))
            return int(local.astimezone(dt.timezone.utc).timestamp())
        except Exception:
            pass
    local = naive.replace(tzinfo=_parse_fixed_offset(fallback_offset))
    return int(local.astimezone(dt.timezone.utc).timestamp())


def _iso_from_ts(ts: int) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _to_float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def _load_monthly_levels_from_csv(
    path: Path,
    *,
    month_field: str,
    level_field: str,
    month_is_date: bool = False,
    required_fields: Optional[Dict[str, str]] = None,
) -> Dict[str, float]:
    """
    Returns {YYYY-MM: level}.
    If month_is_date=True, accepts date strings like YYYY-MM-DD and truncates to YYYY-MM.
    """
    out: Dict[str, float] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if required_fields:
                ok = True
                for key, expected in required_fields.items():
                    if str(row.get(key) or "").strip() != expected:
                        ok = False
                        break
                if not ok:
                    continue
            month_raw = (row.get(month_field) or "").strip()
            if not month_raw:
                continue
            month = month_raw[:7] if month_is_date else month_raw
            if not re.match(r"^(\d{4})-(\d{2})$", month):
                continue
            level = _to_float_or_none(row.get(level_field))
            if level is None:
                continue
            out[month] = float(level)
    return out


def _month_delta(levels: Dict[str, float], month: str) -> Optional[float]:
    if month not in levels:
        return None
    y, m = _parse_year_month(month)
    py, pm = _prev_month(y, m)
    prev = _ym_string(py, pm)
    if prev not in levels:
        return None
    return float(levels[month] - levels[prev])


def _attach_announced_values(
    events: List[Dict[str, Any]],
    *,
    adp_history_csv: Optional[Path],
    revelio_national_csv: Optional[Path],
) -> None:
    """
    Adds `event["value"] = {"actual": ..., "unit": "jobs"}` when missing, using local datasets:
      - ADP: `data/raw/ADP_NER_history.csv` national SA level deltas
      - Revelio: `data/raw/employment_national_revelio.csv` SA level deltas
    """
    adp_levels: Dict[str, float] = {}
    revelio_levels: Dict[str, float] = {}

    if adp_history_csv and adp_history_csv.exists():
        # ADP file is levels; headline is MoM change.
        # Use National/U.S. NER_SA where available; fall back to NER.
        filters = {"agg_RIS": "National", "category": "U.S.", "timestep": "M"}
        sa = _load_monthly_levels_from_csv(
            adp_history_csv, month_field="date", level_field="NER_SA", month_is_date=True, required_fields=filters
        )
        raw = _load_monthly_levels_from_csv(
            adp_history_csv, month_field="date", level_field="NER", month_is_date=True, required_fields=filters
        )
        adp_levels = sa or raw

    if revelio_national_csv and revelio_national_csv.exists():
        revelio_levels = _load_monthly_levels_from_csv(
            revelio_national_csv, month_field="month", level_field="employment_sa", month_is_date=False
        )

    for event in events:
        if not isinstance(event, dict):
            continue
        if isinstance(event.get("value"), dict) and event["value"]:
            continue
        payroll_month = str(event.get("payroll_month") or "").strip()
        if not payroll_month:
            continue
        event_type = str(event.get("type") or "").strip().lower()

        actual: Optional[float] = None
        if event_type == "adp" and adp_levels:
            actual = _month_delta(adp_levels, payroll_month)
        elif event_type == "revelio" and revelio_levels:
            actual = _month_delta(revelio_levels, payroll_month)

        if actual is None:
            continue
        event["value"] = {"actual": float(actual), "expected": None, "unit": "jobs"}


def _generate_events(
    start_month: str,
    end_month: str,
    *,
    tz_name: str,
    adp_time: str,
    revelio_time: str,
    jobs_time: str,
    tz_fallback_offset: str,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    revelio_start_date = dt.date(2025, 9, 4)
    revelio_start_ts = _to_utc_ts(revelio_start_date, revelio_time, tz_name, tz_fallback_offset)
    for year, month in _iter_year_months(start_month, end_month):
        release_month = _ym_string(year, month)
        payroll_y, payroll_m = _prev_month(year, month)
        payroll_month = _ym_string(payroll_y, payroll_m)

        jobs_date = _first_weekday_of_month(year, month, weekday=4)  # Friday
        if month == 1 and jobs_date.day <= 3:
            jobs_date = jobs_date + dt.timedelta(days=7)
        adp_date = jobs_date - dt.timedelta(days=2)
        revelio_date = jobs_date - dt.timedelta(days=1)

        jobs_ts = _to_utc_ts(jobs_date, jobs_time, tz_name, tz_fallback_offset)
        adp_ts = _to_utc_ts(adp_date, adp_time, tz_name, tz_fallback_offset)
        rev_ts = _to_utc_ts(revelio_date, revelio_time, tz_name, tz_fallback_offset)

        events.append(
            {
                "id": f"{release_month}-adp",
                "type": "adp",
                "label": "ADP National Employment Report",
                "release_month": release_month,
                "payroll_month": payroll_month,
                "release_ts": adp_ts,
                "release_iso": _iso_from_ts(adp_ts),
                "jobs_report_ts": jobs_ts,
                "jobs_report_iso": _iso_from_ts(jobs_ts),
            }
        )
        events.append(
            {
                "id": f"{release_month}-revelio",
                "type": "revelio",
                "label": "Revelio Public Labor Statistics (RPLS)",
                "release_month": release_month,
                "payroll_month": payroll_month,
                "release_ts": rev_ts,
                "release_iso": _iso_from_ts(rev_ts),
                "jobs_report_ts": jobs_ts,
                "jobs_report_iso": _iso_from_ts(jobs_ts),
            }
        )
    events = [event for event in events if event.get("type") != "revelio" or int(event["release_ts"]) >= revelio_start_ts]
    events.sort(key=lambda e: int(e["release_ts"]))
    return events


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
            actual_min = row.get("actual_min") or row.get("actual_low") or row.get("actual_floor")
            actual_max = row.get("actual_max") or row.get("actual_high") or row.get("actual_ceiling")
            unit = (row.get("unit") or "jobs").strip()

            value: Optional[Dict[str, Any]] = None

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
            act_min = _to_float(actual_min)
            act_max = _to_float(actual_max)
            if act is not None or exp is not None or act_min is not None or act_max is not None:
                value = {"actual": act, "expected": exp, "unit": unit}
                if act_min is not None:
                    value["actual_min"] = act_min
                if act_max is not None:
                    value["actual_max"] = act_max

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


def _pick_baseline(candles: List[List[Optional[float]]], release_ts: int) -> Tuple[Optional[float], Optional[float]]:
    baseline_ts: Optional[float] = None
    baseline_yes: Optional[float] = None
    for ts, yes, _vol in candles:
        if ts is None or yes is None:
            continue
        if ts <= release_ts:
            baseline_ts = ts
            baseline_yes = yes
        else:
            break
    if baseline_yes is not None:
        return baseline_ts, baseline_yes
    for ts, yes, _vol in candles:
        if ts is None or yes is None:
            continue
        return ts, yes
    return None, None


def _pick_level_at_or_after(
    candles: List[List[Optional[float]]], target_ts: int
) -> Tuple[Optional[float], Optional[float]]:
    for ts, yes, _vol in candles:
        if ts is None or yes is None:
            continue
        if ts >= target_ts:
            return ts, yes
    return None, None


def _last_yes(candles: List[List[Optional[float]]]) -> Tuple[Optional[float], Optional[float]]:
    for ts, yes, _vol in reversed(candles):
        if ts is None or yes is None:
            continue
        return ts, yes
    return None, None


def _stabilization_minutes(
    candles: List[List[Optional[float]]],
    *,
    release_ts: int,
    epsilon_pp: float,
) -> Optional[int]:
    """
    Returns the first minute (>=0) after release such that all subsequent
    candles remain within +/- epsilon_pp of the last observed Yes price.
    """
    if not candles:
        return None
    _last_ts, last_yes = _last_yes(candles)
    if last_yes is None or not math.isfinite(last_yes):
        return None

    last_outside = None
    for ts, yes, _vol in candles:
        if ts is None or yes is None:
            continue
        if ts < release_ts:
            continue
        if not math.isfinite(yes):
            continue
        if abs(yes - last_yes) > epsilon_pp:
            last_outside = ts

    if last_outside is None:
        for ts, yes, _vol in candles:
            if ts is None or yes is None:
                continue
            if ts >= release_ts and math.isfinite(yes):
                return max(0, int((int(ts) - release_ts) / 60))
        return None

    for ts, yes, _vol in candles:
        if ts is None or yes is None:
            continue
        if ts <= last_outside:
            continue
        if ts >= release_ts and math.isfinite(yes):
            return max(0, int((int(ts) - release_ts) / 60))
    # Fallback: stabilize at end of window.
    return max(0, int((int(candles[-1][0] or release_ts) - release_ts) / 60))


def _enrich_market_summary(
    market: Dict[str, Any],
    *,
    release_ts: int,
    pre_minutes: int,
    post_minutes: int,
    stability_epsilon_pp: float,
) -> None:
    summary = market.get("summary")
    if not isinstance(summary, dict):
        return

    volume = summary.get("volume")
    if not isinstance(volume, dict):
        volume = {}
    pre = volume.get("pre")
    post = volume.get("post")
    pre_rate = (float(pre) / pre_minutes) if isinstance(pre, (int, float)) and pre_minutes > 0 else None
    post_rate = (float(post) / post_minutes) if isinstance(post, (int, float)) and post_minutes > 0 else None
    rate_ratio = (post_rate / pre_rate) if (pre_rate is not None and post_rate is not None and pre_rate > 0) else None
    volume["pre_rate"] = pre_rate
    volume["post_rate"] = post_rate
    volume["rate_ratio"] = rate_ratio
    summary["volume"] = volume

    candles = market.get("candles")
    if isinstance(candles, list):
        last_ts, last_yes = _last_yes(candles)
        summary["last_ts"] = last_ts
        summary["last_yes"] = last_yes
        summary["stabilization_minutes"] = _stabilization_minutes(
            candles, release_ts=release_ts, epsilon_pp=stability_epsilon_pp
        )


def _summarize_event(
    event: Dict[str, Any],
    *,
    horizons: List[int],
    pre_minutes: int,
    post_minutes: int,
    stability_epsilon_pp: float,
) -> Dict[str, Any]:
    markets = event.get("markets") if isinstance(event.get("markets"), list) else []
    release_ts = int(event.get("release_ts") or 0)

    horizon_keys = [f"{h}m" for h in horizons]
    horizon_stats: Dict[str, Any] = {}
    for key in horizon_keys:
        deltas: List[float] = []
        for market in markets:
            summary = market.get("summary") if isinstance(market, dict) else None
            if not isinstance(summary, dict):
                continue
            d = summary.get("deltas") if isinstance(summary.get("deltas"), dict) else {}
            value = d.get(key)
            if isinstance(value, (int, float)) and math.isfinite(value):
                deltas.append(float(value))
        abs_deltas = [abs(v) for v in deltas]
        horizon_stats[key] = {
            "n": len(deltas),
            "mean_delta_pp": _mean(deltas),
            "mean_abs_delta_pp": _mean(abs_deltas),
            "median_abs_delta_pp": _median(abs_deltas),
        }

    total_pre = 0.0
    total_post = 0.0
    seen_pre = False
    seen_post = False
    market_rate_ratios: List[float] = []
    stabilizations: List[float] = []

    for market in markets:
        if not isinstance(market, dict):
            continue
        _enrich_market_summary(
            market,
            release_ts=release_ts,
            pre_minutes=pre_minutes,
            post_minutes=post_minutes,
            stability_epsilon_pp=stability_epsilon_pp,
        )
        summary = market.get("summary")
        if not isinstance(summary, dict):
            continue
        volume = summary.get("volume")
        if isinstance(volume, dict):
            pre = volume.get("pre")
            post = volume.get("post")
            if isinstance(pre, (int, float)) and math.isfinite(pre):
                total_pre += float(pre)
                seen_pre = True
            if isinstance(post, (int, float)) and math.isfinite(post):
                total_post += float(post)
                seen_post = True
            rr = volume.get("rate_ratio")
            if isinstance(rr, (int, float)) and math.isfinite(rr):
                market_rate_ratios.append(float(rr))
        stab = summary.get("stabilization_minutes")
        if isinstance(stab, (int, float)) and math.isfinite(stab):
            stabilizations.append(float(stab))

    pre_total = total_pre if seen_pre else None
    post_total = total_post if seen_post else None
    pre_rate_total = (pre_total / pre_minutes) if (pre_total is not None and pre_minutes > 0) else None
    post_rate_total = (post_total / post_minutes) if (post_total is not None and post_minutes > 0) else None
    rate_ratio_total = (
        (post_rate_total / pre_rate_total)
        if (pre_rate_total is not None and post_rate_total is not None and pre_rate_total > 0)
        else None
    )

    value = event.get("value") if isinstance(event.get("value"), dict) else None
    surprise: Optional[float] = None
    if value:
        actual = value.get("actual")
        expected = value.get("expected")
        if isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
            if math.isfinite(actual) and math.isfinite(expected):
                surprise = float(actual - expected)

    direction_aligned: Optional[bool] = None
    aligned_horizon = "30m" if "30m" in horizon_stats else (horizon_keys[0] if horizon_keys else None)
    if surprise is not None and aligned_horizon:
        delta = horizon_stats.get(aligned_horizon, {}).get("mean_delta_pp")
        if isinstance(delta, (int, float)) and math.isfinite(delta) and delta != 0 and surprise != 0:
            direction_aligned = (delta > 0) == (surprise > 0)

    return {
        "market_count": len(markets),
        "horizons": horizon_stats,
        "volume": {
            "pre_total": pre_total,
            "post_total": post_total,
            "pre_rate_total": pre_rate_total,
            "post_rate_total": post_rate_total,
            "rate_ratio_total": rate_ratio_total,
            "median_market_rate_ratio": _median(market_rate_ratios),
        },
        "stabilization_minutes": {"median": _median(stabilizations), "mean": _mean(stabilizations)},
        "surprise": {"value": surprise, "aligned_horizon": aligned_horizon, "direction_aligned": direction_aligned},
    }


def _summarize_dataset(events: List[Dict[str, Any]], *, horizons: List[int]) -> Dict[str, Any]:
    horizon_keys = [f"{h}m" for h in horizons]

    def summarize_group(group_events: List[Dict[str, Any]]) -> Dict[str, Any]:
        horizon_out: Dict[str, Any] = {}
        for key in horizon_keys:
            mean_deltas: List[float] = []
            mean_abs: List[float] = []
            for event in group_events:
                summary = event.get("summary") if isinstance(event.get("summary"), dict) else None
                if not isinstance(summary, dict):
                    continue
                hs = summary.get("horizons") if isinstance(summary.get("horizons"), dict) else {}
                entry = hs.get(key) if isinstance(hs.get(key), dict) else {}
                md = entry.get("mean_delta_pp")
                ma = entry.get("mean_abs_delta_pp")
                if isinstance(md, (int, float)) and math.isfinite(md):
                    mean_deltas.append(float(md))
                if isinstance(ma, (int, float)) and math.isfinite(ma):
                    mean_abs.append(float(ma))
            horizon_out[key] = {
                "event_n": len(mean_deltas),
                "avg_event_mean_delta_pp": _mean(mean_deltas),
                "avg_event_mean_abs_delta_pp": _mean(mean_abs),
                "median_event_mean_abs_delta_pp": _median(mean_abs),
            }

        rate_ratios: List[float] = []
        stabilization_medians: List[float] = []
        surprise_vals: List[float] = []
        surprise_deltas: List[float] = []
        corr_horizon = "30m" if "30m" in horizon_keys else (horizon_keys[0] if horizon_keys else None)

        for event in group_events:
            summary = event.get("summary") if isinstance(event.get("summary"), dict) else None
            if not isinstance(summary, dict):
                continue
            volume = summary.get("volume") if isinstance(summary.get("volume"), dict) else {}
            rr = volume.get("rate_ratio_total")
            if isinstance(rr, (int, float)) and math.isfinite(rr):
                rate_ratios.append(float(rr))
            stab = summary.get("stabilization_minutes") if isinstance(summary.get("stabilization_minutes"), dict) else {}
            med = stab.get("median")
            if isinstance(med, (int, float)) and math.isfinite(med):
                stabilization_medians.append(float(med))

            surprise = summary.get("surprise") if isinstance(summary.get("surprise"), dict) else {}
            sval = surprise.get("value")
            if corr_horizon and isinstance(sval, (int, float)) and math.isfinite(sval):
                hs = summary.get("horizons") if isinstance(summary.get("horizons"), dict) else {}
                entry = hs.get(corr_horizon) if isinstance(hs.get(corr_horizon), dict) else {}
                delta = entry.get("mean_delta_pp")
                if isinstance(delta, (int, float)) and math.isfinite(delta):
                    surprise_vals.append(float(sval))
                    surprise_deltas.append(float(delta))

        return {
            "event_count": len(group_events),
            "horizons": horizon_out,
            "volume_rate_ratio_total": {"median": _median(rate_ratios), "mean": _mean(rate_ratios)},
            "stabilization_minutes": {"median": _median(stabilization_medians), "mean": _mean(stabilization_medians)},
            "surprise_delta_corr": {
                "horizon": corr_horizon,
                "n": len(surprise_vals),
                "pearson_r": _pearson_r(surprise_vals, surprise_deltas),
            },
        }

    by_type: Dict[str, List[Dict[str, Any]]] = {}
    for event in events:
        t = str(event.get("type") or "unknown").lower()
        by_type.setdefault(t, []).append(event)

    sources: Dict[str, Any] = {"all": summarize_group(events)}
    for t, group in sorted(by_type.items()):
        sources[t] = summarize_group(group)

    return {"horizons": horizon_keys, "sources": sources}
