#!/usr/bin/env python3
"""
Build a Bayesian posterior over market priors using Kalshi/Polymarket brackets.

Model (simple, source-aware):
  - Let p be the "true" probability over K bins (K = union of brackets).
  - Prior: p ~ Dirichlet(1, 1, ..., 1).
  - For each source s with observed distribution y_s:
      y_s ~ Dirichlet(kappa_s * p)
    where kappa_s is a concentration parameter capturing source reliability.

This script reads data/market_priors.json and writes data/market_priors_bayes.json.
It keeps the original schema but adds a "posterior" block to each snapshot.
If source volumes are present, it weights the sources by log(volume).
"""

from __future__ import annotations

import argparse
import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

try:
    import pymc as pm
except Exception as exc:  # pragma: no cover
    pm = None  # type: ignore
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_IN = BASE_DIR / "data" / "market_priors.json"
DEFAULT_OUT = BASE_DIR / "data" / "market_priors_bayes.json"
EPSILON = 1e-6


@dataclass(frozen=True)
class Bin:
    lower: Optional[float]
    upper: Optional[float]

    def key(self) -> str:
        return f"{self.lower if self.lower is not None else ''}|{self.upper if self.upper is not None else ''}"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out


def _safe_array(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _infer_default_width(ranges: List[Dict[str, Any]]) -> float:
    widths = []
    values = []
    for entry in ranges:
        lower = _to_float(entry.get("lower"))
        upper = _to_float(entry.get("upper"))
        if lower is not None:
            values.append(lower)
        if upper is not None:
            values.append(upper)
        if lower is None or upper is None:
            continue
        width = upper - lower
        if width > 0:
            widths.append(width)
    if widths:
        widths.sort()
        mid = len(widths) // 2
        return widths[mid] if len(widths) % 2 else (widths[mid - 1] + widths[mid]) / 2.0
    if not values:
        return 1.0
    max_abs = max(abs(v) for v in values)
    if max_abs <= 5:
        return 0.25
    if max_abs <= 20:
        return 1.0
    return 50_000.0


def _standard_bins_from_ranges(ranges: List[Dict[str, Any]]) -> Tuple[List[Bin], float]:
    width = _infer_default_width(ranges)
    values = []
    for entry in ranges:
        lower = _to_float(entry.get("lower"))
        upper = _to_float(entry.get("upper"))
        if lower is not None:
            values.append(lower)
        if upper is not None:
            values.append(upper)
        if lower is None and upper is not None:
            values.append(upper - width)
        if upper is None and lower is not None:
            values.append(lower + width)
    if not values:
        return [], width
    min_val = min(values)
    max_val = max(values)
    start = math.floor(min_val / width) * width
    end = math.ceil(max_val / width) * width
    bins = [Bin(lower=None, upper=start)]
    v = start
    while v < end - 1e-9:
        bins.append(Bin(lower=v, upper=v + width))
        v += width
    bins.append(Bin(lower=end, upper=None))
    return bins, width


def _collect_bins(sources: Dict[str, List[Dict[str, Any]]]) -> Tuple[List[Bin], float]:
    all_ranges = []
    for ranges in sources.values():
        all_ranges.extend(_safe_array(ranges))
    return _standard_bins_from_ranges(all_ranges)


def _bin_midpoints(bins: List[Bin], width: float) -> np.ndarray:
    mids = []
    for b in bins:
        if b.lower is not None and b.upper is not None:
            mids.append((b.lower + b.upper) / 2.0)
        elif b.lower is None and b.upper is not None:
            mids.append(b.upper - width / 2.0)
        elif b.upper is None and b.lower is not None:
            mids.append(b.lower + width / 2.0)
        else:
            mids.append(0.0)
    return np.asarray(mids, dtype=float)


def _expand_bounds(lower: Optional[float], upper: Optional[float], min_bound: float, max_bound: float, width: float) -> Tuple[float, float]:
    if lower is None:
        lower = min_bound - width
    if upper is None:
        upper = max_bound + width
    return lower, upper


def _normalize_ranges(ranges: List[Dict[str, Any]]) -> List[Tuple[Optional[float], Optional[float], float]]:
    cleaned = []
    total = 0.0
    for entry in ranges:
        lower = _to_float(entry.get("lower"))
        upper = _to_float(entry.get("upper"))
        prob = _to_float(entry.get("prob"))
        if prob is None or prob < 0:
            continue
        cleaned.append((lower, upper, prob))
        total += prob
    if total <= 0:
        return []
    return [(l, u, p / total) for l, u, p in cleaned]


def _vectorize_source(bins: List[Bin], ranges: List[Dict[str, Any]], width: float) -> Optional[np.ndarray]:
    normalized = _normalize_ranges(_safe_array(ranges))
    if not normalized:
        return None
    min_bound = next((b.lower for b in bins if b.lower is not None), 0.0)
    max_bound = next((b.upper for b in reversed(bins) if b.upper is not None), min_bound + width)
    totals = np.zeros(len(bins), dtype=float)
    for lower, upper, prob in normalized:
        src_lo, src_hi = _expand_bounds(lower, upper, min_bound, max_bound, width)
        src_width = src_hi - src_lo
        if src_width <= 0:
            continue
        for idx, b in enumerate(bins):
            dst_lo, dst_hi = _expand_bounds(b.lower, b.upper, min_bound, max_bound, width)
            overlap = max(0.0, min(src_hi, dst_hi) - max(src_lo, dst_lo))
            if overlap > 0:
                totals[idx] += prob * (overlap / src_width)
    total = float(totals.sum())
    if total <= 0:
        return None
    totals = totals / total
    totals = totals + EPSILON
    totals = totals / totals.sum()
    return totals


def _volume_weights(snapshot: Dict[str, Any], sources: List[str]) -> Dict[str, float]:
    meta = snapshot.get("source_meta") if isinstance(snapshot.get("source_meta"), dict) else {}
    if not meta:
        return {}
    volumes: Dict[str, float] = {}
    for source in sources:
        entry = meta.get(source) if isinstance(meta.get(source), dict) else {}
        vol = _to_float(entry.get("volume"))
        if vol is not None and vol > 0:
            volumes[source] = vol
    if len(volumes) < 2:
        return {}
    raw = {source: math.log1p(vol) for source, vol in volumes.items()}
    mean = sum(raw.values()) / len(raw)
    if mean <= 0:
        return {}
    weights: Dict[str, float] = {}
    for source in sources:
        if source in raw:
            weight = raw[source] / mean
            weight = min(max(weight, 0.5), 3.0)
        else:
            weight = 1.0
        weights[source] = float(weight)
    return weights


def _quantiles(values: np.ndarray, qs: Tuple[float, float]) -> Tuple[float, float]:
    return float(np.quantile(values, qs[0])), float(np.quantile(values, qs[1]))


def _fit_dirichlet_model(
    observations: Dict[str, np.ndarray],
    *,
    weights: Optional[Dict[str, float]] = None,
    draws: int,
    tune: int,
    chains: int,
    target_accept: float,
) -> Tuple[np.ndarray, Dict[str, float]]:
    if pm is None:  # pragma: no cover
        raise RuntimeError(f"PyMC not available: {_IMPORT_ERROR}")
    sources = list(observations.keys())
    k = len(next(iter(observations.values())))
    with pm.Model() as model:
        p = pm.Dirichlet("p", a=np.ones(k))
        kappas = {}
        for source in sources:
            weight = weights.get(source, 1.0) if weights else 1.0
            scale = 25.0 * max(weight, 0.1)
            kappa = pm.Exponential(f"kappa_{source}", lam=1 / scale)
            kappas[source] = kappa
            pm.Dirichlet(f"obs_{source}", a=kappa * p, observed=observations[source])
        trace = pm.sample(
            draws=draws,
            tune=tune,
            chains=chains,
            target_accept=target_accept,
            progressbar=False,
            compute_convergence_checks=False,
        )
    p_samples = trace.posterior["p"].stack(sample=("chain", "draw")).values
    kappa_means = {
        source: float(trace.posterior[f"kappa_{source}"].mean().values)
        for source in sources
    }
    return p_samples.T, kappa_means


def _fallback_posterior(
    observations: Dict[str, np.ndarray],
    weights: Optional[Dict[str, float]] = None,
) -> Tuple[np.ndarray, Dict[str, float]]:
    stacked = np.stack(list(observations.values()))
    if weights:
        sources = list(observations.keys())
        weight_vals = np.array([weights.get(source, 1.0) for source in sources], dtype=float)
        if np.isfinite(weight_vals).all() and weight_vals.sum() > 0:
            weight_vals = weight_vals / weight_vals.sum()
            mean = np.average(stacked, axis=0, weights=weight_vals)
            return np.expand_dims(mean, axis=0), {}
    mean = stacked.mean(axis=0, keepdims=True)
    return mean, {}


def build_snapshot_posterior(
    snapshot: Dict[str, Any],
    *,
    draws: int,
    tune: int,
    chains: int,
    target_accept: float,
) -> Optional[Dict[str, Any]]:
    sources = snapshot.get("sources") if isinstance(snapshot.get("sources"), dict) else {}
    bins, width = _collect_bins(sources)
    if not bins:
        return None
    observations: Dict[str, np.ndarray] = {}
    for source, ranges in sources.items():
        vec = _vectorize_source(bins, ranges, width)
        if vec is not None:
            observations[source] = vec
    if not observations:
        return None
    weights = _volume_weights(snapshot, list(observations.keys()))
    model_error = None
    if len(observations) == 1:
        p_samples, kappa_means = _fallback_posterior(observations, weights=weights)
        used_sampling = False
    else:
        try:
            p_samples, kappa_means = _fit_dirichlet_model(
                observations,
                weights=weights,
                draws=draws,
                tune=tune,
                chains=chains,
                target_accept=target_accept,
            )
            used_sampling = True
        except Exception as exc:
            model_error = f"{type(exc).__name__}: {exc}"
            p_samples, kappa_means = _fallback_posterior(observations, weights=weights)
            used_sampling = False

    midpoints = _bin_midpoints(bins, width)
    expected_samples = p_samples @ midpoints
    expected_mean = float(expected_samples.mean())
    expected_lo, expected_hi = _quantiles(expected_samples, (0.1, 0.9))

    posterior_bins = []
    for idx, b in enumerate(bins):
        samples = p_samples[:, idx]
        mean = float(samples.mean())
        lo, hi = _quantiles(samples, (0.1, 0.9))
        posterior_bins.append(
            {
                "lower": b.lower,
                "upper": b.upper,
                "midpoint": float(midpoints[idx]),
                "mean": mean,
                "p10": lo,
                "p90": hi,
            }
        )

    model_block = {
        "kind": "dirichlet",
        "sources": list(observations.keys()),
        "sampled": used_sampling,
        "quantiles": [0.1, 0.9],
    }
    if weights:
        model_block["volume_weights"] = weights
    if model_error:
        model_block["error"] = model_error

    return {
        "model": model_block,
        "kappa": kappa_means,
        "expected": {
            "mean": expected_mean,
            "p10": expected_lo,
            "p90": expected_hi,
        },
        "bins": posterior_bins,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Bayesian posterior for market priors.")
    parser.add_argument("--in", dest="input_path", default=str(DEFAULT_IN), help="Input market_priors.json path.")
    parser.add_argument("--out", dest="output_path", default=str(DEFAULT_OUT), help="Output market_priors_bayes.json path.")
    parser.add_argument("--draws", type=int, default=800, help="Posterior draws.")
    parser.add_argument("--tune", type=int, default=800, help="Tuning steps.")
    parser.add_argument("--chains", type=int, default=2, help="Number of chains.")
    parser.add_argument("--target-accept", type=float, default=0.9, help="Target accept for NUTS.")
    args = parser.parse_args()

    if pm is None:  # pragma: no cover
        print("PyMC is required to run this script.")
        print(f"Import error: {_IMPORT_ERROR}")
        print("Try: pip install pymc numpy arviz")
        return 1

    in_path = Path(args.input_path).expanduser()
    out_path = Path(args.output_path).expanduser()
    if not in_path.exists():
        print(f"Input file not found: {in_path}")
        return 1

    with in_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    events_out = []
    for event in _safe_array(payload.get("events")):
        snapshots_out = []
        for snap in _safe_array(event.get("snapshots")):
            posterior = build_snapshot_posterior(
                snap,
                draws=args.draws,
                tune=args.tune,
                chains=args.chains,
                target_accept=args.target_accept,
            )
            snap_out = dict(snap)
            snap_out["posterior"] = posterior
            snapshots_out.append(snap_out)

        event_out = dict(event)
        event_out["snapshots"] = snapshots_out
        events_out.append(event_out)

    out_payload = {
        "updated_at": _now_iso(),
        "events": events_out,
        "model": {
            "kind": "dirichlet",
            "notes": "Posterior computed from source distributions using a Dirichlet likelihood; kappa scaled by log(volume) when available.",
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out_payload, f, ensure_ascii=False, indent=2)
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
