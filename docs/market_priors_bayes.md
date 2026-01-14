# Market Priors Bayesian Model

This doc describes the Bayesian posterior used by `Market Priors` and how to
rebuild it locally.

## Overview

We treat the market prior as a probability distribution over outcome bins
(Kalshi/Polymarket brackets). The goal is to combine sources while keeping a
clear notion of uncertainty.

Model (simple, source-aware):

- Let `p` be the latent, true probability over `K` bins.
- Prior: `p ~ Dirichlet(1, 1, ..., 1)` (weakly-informative).
- Each source distribution is treated as a noisy observation of `p`:
  `y_s ~ Dirichlet(kappa_s * p)`
- `kappa_s` is a per-source concentration (higher = more confident).
- If both sources provide volume metadata, `kappa_s` is scaled by
  `log1p(notional_volume)` (normalized to mean 1, clamped to 0.5–3.0).

We sample the posterior of `p` and `kappa_s` with PyMC, then summarize:

- Posterior mean per bin.
- 10th/90th percentile bands per bin.
- Expected value distribution (posterior mean + p10/p90).

## Inputs and Outputs

Input:
- `data/market_priors.json` (from `scripts/fetch_market_priors.py`)

Output:
- `data/market_priors_bayes.json`
  - same structure as `market_priors.json`
  - each snapshot has a `posterior` block:
    - `bins`: `{ lower, upper, midpoint, mean, p10, p90 }`
    - `expected`: `{ mean, p10, p90 }`
    - `kappa`: per-source concentration estimates
    - `model.volume_weights`: optional per-source weights (if volume data is present)
  - each event may include `history` (expected-value series by source)

If only one source is present for a snapshot, the script falls back to a
simple average (no sampling) and still produces a posterior block.

## Running the Model

Install dependencies:

```
pip install pymc numpy arviz
```

Run:

```
python3 scripts/build_market_priors_bayes.py
```

Optional knobs:

```
python3 scripts/build_market_priors_bayes.py \\
  --draws 800 --tune 800 --chains 2 --target-accept 0.9
```

## Troubleshooting

If you see `cannot import name 'gaussian' from 'scipy.signal'`, your SciPy
version is too new for the installed PyMC. Pin SciPy below 1.12:

```
pip install \"scipy<1.12\" --upgrade
```

## How the UI Uses It

`html/priors.html` + `js/priors.js`:

- Loads `data/market_priors_bayes.json` if present.
- Falls back to `data/market_priors.json` if not.
- Shows the posterior mean as the main distribution.
- Shows posterior expected value (mean + p10/p90 band) in the summary.
- Plots the expected value over time using snapshot timestamps.
- If `event.history` is present, the time series uses that market history instead.

## Notes / Limitations

- The model assumes sources are noisy observations of the same underlying `p`.
- We do not yet model time dynamics; each snapshot is fit independently.
- Open-ended bins use a default width based on median bin width.
