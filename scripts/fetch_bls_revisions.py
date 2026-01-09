"""
BLS Revision Analyzer
Analyzes how employment estimates change as BLS releases subsequent revisions
Uses FRED API to pull different data vintages for revision analysis
"""

import pandas as pd
import requests
import time
import os
import argparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import math

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"

# Provide your FRED API key via the environment:
#   export FRED_API_KEY="..."
API_KEY = os.environ.get("FRED_API_KEY", "").strip()
if not API_KEY:
    raise ValueError("Missing FRED API key. Set FRED_API_KEY in your environment.")

# Analysis mode toggles
RUN_LEVEL_2_ANALYSIS = True      # Broad industries + totals (SA + NSA)
RUN_DETAILED_ANALYSIS = False     # Detailed industries within a supersector

# Detailed analysis parameters
DETAILED_SUPERSECTOR_CODE = "30"  # "30"=Manufacturing, "42"=Retail, etc.
DETAILED_LEVEL = 4                # Display level to analyze (typically 3 or 4)

# Date range for analysis
START_DATE = "2012-01-01"
END_DATE = "2025-10-31"

# Output paths (JSON feeds power the website explorer)
OUTPUT_DIR = DATA_DIR
OUTPUT_CSV_LEVEL2 = OUTPUT_DIR / "bls_revisions_level2.csv"
OUTPUT_JSON_LEVEL2 = OUTPUT_DIR / "bls_revisions_level2.json"
OUTPUT_CSV_DETAILED = OUTPUT_DIR / "bls_revisions_detailed.csv"
OUTPUT_JSON_DETAILED = OUTPUT_DIR / "bls_revisions_detailed.json"

# Revelio alignment exports (for BLS vs Revelio comparisons)
RUN_REVELIO_ALIGNMENT_EXPORT = True
REVELIO_REVISIONS_CSV = RAW_DIR / "bls_revisions_revelio.csv"
REVELIO_EMPLOYMENT_CSV = RAW_DIR / "employment_naics_revelio.csv"
REVELIO_NATIONAL_EMPLOYMENT_CSV_CANDIDATES = [
    RAW_DIR / "employment_national_revelio.csv",
    RAW_DIR / "employment_national_Revelio.csv"
]

def _pick_first_existing(paths: List[Path]) -> Optional[Path]:
    for path in paths:
        if path.exists():
            return path
    return None
OUTPUT_CSV_BLS_REVISIONS_REVELIO_FORMAT = RAW_DIR / "bls_revisions.csv"
OUTPUT_CSV_BLS_EMPLOYMENT_REVELIO_FORMAT = RAW_DIR / "employment_naics.csv"
OUTPUT_CSV_BLS_VS_REVELIO_REVISIONS = OUTPUT_DIR / "bls_vs_revelio_revisions.csv"
OUTPUT_JSON_BLS_VS_REVELIO_REVISIONS = OUTPUT_DIR / "bls_vs_revelio_revisions.json"
OUTPUT_CSV_BLS_VS_REVELIO_EMPLOYMENT = OUTPUT_DIR / "bls_vs_revelio_employment.csv"
OUTPUT_JSON_BLS_VS_REVELIO_EMPLOYMENT = OUTPUT_DIR / "bls_vs_revelio_employment.json"

# Map Revelio NAICS buckets -> closest CES supersector code
# (Revelio is NAICS-based; CES supersectors don't perfectly align, especially Mining vs Mining+Logging.)
REVELIO_TO_CES_SUPERSECTOR = {
    "11": None,      # Agriculture (not in CES nonfarm payrolls)
    "21": "10",      # Mining -> Mining and logging
    "22": "44",      # Utilities
    "23": "20",      # Construction
    "31-33": "30",   # Manufacturing
    "42": "41",      # Wholesale trade
    "44-45": "42",   # Retail trade
    "48-49": "43",   # Transportation and warehousing
    "51": "50",      # Information
    "52-53": "55",   # Financial activities
    "54-56": "60",   # Professional and business services
    "61-62": "65",   # Private education and health services
    "71-72": "70",   # Leisure and hospitality
    "81": "80",      # Other services
    "92": "90",      # Government
    "99": None       # Unclassified
}

# ADP (National Employment Report) history (levels)
ADP_NER_HISTORY_CSV = RAW_DIR / "ADP_NER_history.csv"

ADP_INDUSTRY_TO_SECTOR = {
    "Construction": "23",
    "Manufacturing": "31-33",
    "Natural resources and mining": "21",
    "Trade, transportation, and utilities": "TTU",
    "Information": "51",
    "Financial activities": "52-53",
    "Professional and business services": "54-56",
    "Education and health services": "61-62",
    "Leisure and hospitality": "71-72",
    "Other services": "81"
}

# =============================================================================


class OptimizedBLSAnalyzer:
    """
    Handles FRED API interactions with intelligent caching and rate limiting
    
    Key features:
    - Respects FRED's 120 requests/minute limit with adaptive delays
    - Caches responses to prevent duplicate API calls for the same data
    - Tracks failed requests to avoid retrying known errors
    
    Note: Cache is primarily for deduplication within a series, not across
    series, since each series has unique data. Low cache hit rates (10-20%)
    are expected when processing multiple series.
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.stlouisfed.org/fred"
        self.request_count = 0
        self.start_time = time.time()
        self.response_times = []
        self.vintage_cache = {}
        self.failed_requests = set()
    
    def _calculate_adaptive_delay(self):
        """Dynamically adjust delay based on API response times"""
        base_delay = 0.5
        
        # Use recent response times to calculate optimal delay
        if len(self.response_times) >= 5:
            recent_times = self.response_times[-10:]
            avg_response_time = sum(recent_times) / len(recent_times)
            adaptive_delay = max(base_delay, avg_response_time * 1.5)
            return min(adaptive_delay, 2.0)
        
        return base_delay
    
    def _get_cache_key(self, series_id: str, realtime_date: str, obs_start: str, obs_end: str):
        """Generate unique identifier for caching API responses"""
        key_data = f"{series_id}|{realtime_date}|{obs_start}|{obs_end}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _api_call_with_retry(self, endpoint: str, params: dict, max_retries=3):
        """Execute API call with retry logic and cache management"""
        params.update({'api_key': self.api_key, 'file_type': 'json'})
        
        # Check cache before making API call
        if endpoint == 'series/observations':
            cache_key = self._get_cache_key(
                params.get('series_id', ''),
                params.get('realtime_start', ''),
                params.get('observation_start', ''),
                params.get('observation_end', '')
            )
            if cache_key in self.vintage_cache:
                return self.vintage_cache[cache_key]
            if cache_key in self.failed_requests:
                return None
        
        # Execute request with exponential backoff on failures
        for attempt in range(max_retries):
            try:
                self.request_count += 1
                time.sleep(self._calculate_adaptive_delay())
                
                # Progress indicator every 50 requests
                if self.request_count % 50 == 0:
                    elapsed = time.time() - self.start_time
                    rate = self.request_count / (elapsed / 60)
                    print(f"Progress: {self.request_count} calls, {rate:.0f}/min, {len(self.vintage_cache)} cached")
                
                start_request = time.time()
                response = requests.get(
                    f"{self.base_url}/{endpoint}",
                    params=params,
                    timeout=30
                )
                request_time = time.time() - start_request
                self.response_times.append(request_time)
                
                # Keep only recent response times for adaptive delay calculation
                if len(self.response_times) > 20:
                    self.response_times = self.response_times[-20:]
                
                if response.status_code == 200:
                    result = response.json()
                    if endpoint == 'series/observations':
                        self.vintage_cache[cache_key] = result
                    return result
                elif response.status_code == 429:
                    # Rate limited - wait with exponential backoff
                    wait_time = min(20, 5 * (2 ** attempt))
                    print(f"Rate limited, waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 400:
                    # Bad request - mark as failed to avoid retrying
                    if endpoint == 'series/observations':
                        self.failed_requests.add(cache_key)
                    return None
                else:
                    return None
                    
            except Exception:
                if attempt == max_retries - 1:
                    if endpoint == 'series/observations':
                        self.failed_requests.add(cache_key)
                    return None
                else:
                    time.sleep(2 * (attempt + 1))
                    continue
        
        return None
    
    def get_data_as_of_date(self, series_id: str, realtime_date: str, obs_start: str, obs_end: str):
        """Retrieve vintage data snapshot as it existed on a specific date"""
        response = self._api_call_with_retry('series/observations', {
            'series_id': series_id,
            'observation_start': obs_start,
            'observation_end': obs_end,
            'realtime_start': realtime_date,
            'realtime_end': realtime_date
        })
        
        if not response or 'observations' not in response:
            return {}
        
        # Parse observations into dictionary
        data = {}
        for obs in response['observations']:
            if obs['value'] != '.':
                try:
                    data[obs['date']] = float(obs['value'])
                except (ValueError, TypeError):
                    continue
        return data

    def get_latest_data(self, series_id: str, obs_start: str, obs_end: str) -> Dict[str, float]:
        """
        Retrieve the latest published observations for a series.

        Why this exists:
        - Many FRED series are not ALFRED/vintage-enabled.
        - Passing realtime_start/realtime_end can yield HTTP 400 for those series.
        - For level comparisons we want the latest values, not historical vintages.
        """
        response = self._api_call_with_retry(
            "series/observations",
            {
                "series_id": series_id,
                "observation_start": obs_start,
                "observation_end": obs_end
            }
        )

        if not response or "observations" not in response:
            return {}

        data: Dict[str, float] = {}
        for obs in response["observations"]:
            if obs.get("value") == ".":
                continue
            try:
                data[str(obs.get("date"))] = float(obs.get("value"))
            except (ValueError, TypeError):
                continue
        return data

    def _calculate_vintage_dates_optimized(self, obs_months):
        """
        Calculate when each data point was released and subsequently revised

        BLS releases employment data approximately 1 week into the following month,
        then revises it in the next two monthly releases (t+1 and t+2 revisions).
        """
        vintage_requests = {}

        for obs_date in obs_months:
            # Initial release is ~6 days into following month
            release_month = obs_date + relativedelta(months=1)
            initial_release = release_month + timedelta(days=6)
            first_revision = initial_release + relativedelta(months=1)
            second_revision = initial_release + relativedelta(months=2)

            vintage_dates = [
                initial_release.strftime('%Y-%m-%d'),
                first_revision.strftime('%Y-%m-%d'),
                second_revision.strftime('%Y-%m-%d')
            ]

            # Track which observation dates are needed for each vintage
            for vintage_date in vintage_dates:
                if vintage_date not in vintage_requests:
                    vintage_requests[vintage_date] = set()
                vintage_requests[vintage_date].add(obs_date)

        return vintage_requests

    def analyze_series_revisions_optimized(self, series_id: str, start_date: str, end_date: str):
        """Build complete revision history for a single employment series"""
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        obs_months = pd.date_range(start=start_dt, end=end_dt, freq='MS')

        # Use consistent extended date range across all chunks for better caching
        global_start = (obs_months[0] - relativedelta(months=3)).strftime('%Y-%m-%d')
        global_end = (obs_months[-1] + relativedelta(months=4)).strftime('%Y-%m-%d')

        # Calculate all vintage dates upfront
        vintage_requests = self._calculate_vintage_dates_optimized(obs_months)

        # Fetch all vintage snapshots using the same date range (maximizes cache hits)
        vintage_data = {}
        for vintage_date in sorted(vintage_requests.keys()):
            data = self.get_data_as_of_date(
                series_id, vintage_date, global_start, global_end
            )
            vintage_data[vintage_date] = data

        # Process all revisions
        revisions = self._process_chunk_revisions(series_id, obs_months, vintage_data)

        return pd.DataFrame(revisions) if revisions else None

    def _process_chunk_revisions(self, series_id: str, obs_months, vintage_data):
        """
        Extract initial estimates and revisions from vintage snapshots.

        Calculates both incremental revisions (t to t1, t1 to t2) and cumulative
        revisions (t to t2) to show how estimates evolve over time.
        """
        revisions = []

        for obs_date in obs_months:
            obs_date_str = obs_date.strftime('%Y-%m-%d')

            # Calculate vintage dates: initial release ~6 days after month, then monthly revisions
            release_month = obs_date + relativedelta(months=1)
            initial_release = release_month + timedelta(days=6)
            first_revision = initial_release + relativedelta(months=1)
            second_revision = initial_release + relativedelta(months=2)

            initial_date_str = initial_release.strftime('%Y-%m-%d')
            first_rev_date_str = first_revision.strftime('%Y-%m-%d')
            second_rev_date_str = second_revision.strftime('%Y-%m-%d')

            # Pull the three estimates as of those vintages
            estimates = {}
            vintages = {}

            if initial_date_str in vintage_data and obs_date_str in vintage_data[initial_date_str]:
                estimates['t'] = vintage_data[initial_date_str][obs_date_str]
                vintages['t'] = initial_release

            if first_rev_date_str in vintage_data and obs_date_str in vintage_data[first_rev_date_str]:
                estimates['t1'] = vintage_data[first_rev_date_str][obs_date_str]
                vintages['t1'] = first_revision

            if second_rev_date_str in vintage_data and obs_date_str in vintage_data[second_rev_date_str]:
                estimates['t2'] = vintage_data[second_rev_date_str][obs_date_str]
                vintages['t2'] = second_revision

            # Build record with estimates and vintages
            if 't' in estimates:
                est_t = estimates.get('t')
                est_t1 = estimates.get('t1')
                est_t2 = estimates.get('t2')

                rec = {
                    'series_id': series_id,
                    'obs_date': obs_date,
                    'estimate_t': est_t,
                    'estimate_t1': est_t1,
                    'estimate_t2': est_t2,
                    'vintage_t': vintages.get('t'),
                    'vintage_t1': vintages.get('t1'),
                    'vintage_t2': vintages.get('t2'),
                }

                # First revision: t to t1 (incremental change after first month)
                if est_t is not None and est_t1 is not None:
                    rec['revision_1month'] = est_t1 - est_t
                    rec['revision_1month_pct'] = ((rec['revision_1month'] / est_t) * 100) if est_t else None

                # Second revision: t1 to t2 (incremental change after second month)
                if est_t1 is not None and est_t2 is not None:
                    rec['revision_2month'] = est_t2 - est_t1
                    rec['revision_2month_pct'] = ((rec['revision_2month'] / est_t1) * 100) if est_t1 else None

                # Also store as explicit incremental revision
                if est_t1 is not None and est_t2 is not None:
                    rec['revision_t1_to_t2'] = est_t2 - est_t1

                # Cumulative revision from initial to final (t to t2)
                if est_t is not None and est_t2 is not None:
                    rec['rev2_cum'] = est_t2 - est_t
                    rec['rev2_cum_pct'] = ((rec['rev2_cum'] / est_t) * 100) if est_t else None
                else:
                    rec['rev2_cum'] = None
                    rec['rev2_cum_pct'] = None

                # Incremental second-step revision (same as revision_2month, explicit alias)
                if est_t1 is not None and est_t2 is not None:
                    rev2_incr = est_t2 - est_t1
                    rec['rev2_incr'] = rev2_incr
                    rec['rev2_incr_pct'] = ((rev2_incr / est_t1) * 100) if est_t1 else None
                else:
                    rec['rev2_incr'] = None
                    rec['rev2_incr_pct'] = None

                revisions.append(rec)

        return revisions

    def build_dataset(self, series_list, start_date, end_date):
        """Compile revision data across multiple employment series"""
        print(f"\nAnalyzing {len(series_list)} series from {start_date} to {end_date}")

        all_data = []
        successful = 0

        for i, series_id in enumerate(series_list):
            print(f"{i+1}/{len(series_list)}: {series_id}")
            series_data = self.analyze_series_revisions_optimized(series_id, start_date, end_date)

            if series_data is not None and not series_data.empty:
                all_data.append(series_data)
                successful += 1

            # Brief pause between series to avoid rate limiting
            if i < len(series_list) - 1:
                time.sleep(1.5)

        if all_data:
            final_dataset = pd.concat(all_data, ignore_index=True)
            elapsed = time.time() - self.start_time
            print(f"\nCompleted: {len(final_dataset):,} records from {successful}/{len(series_list)} series in {elapsed:.0f}s")
            return final_dataset
        else:
            print("No data collected")
            return pd.DataFrame()


def _ym_from_date_str(date_str: str) -> str:
    """Convert YYYY-MM-DD -> YYYY-MM (safe for already-YYYY-MM inputs)."""
    if not date_str:
        return ""
    return date_str[:7]


def _ym_first_day(ym: str) -> datetime:
    return datetime.strptime(f"{ym}-01", "%Y-%m-%d")


def _last_day_previous_month(ym: str) -> str:
    first_day = _ym_first_day(ym)
    prev_last = first_day - timedelta(days=1)
    return prev_last.strftime("%Y-%m-%d")


def _approx_release_date_in_month(ym: str, day_offset: int = 6) -> str:
    """
    Approximate BLS/FRED 'realtime' vintage date within a release month.

    Using the 7th calendar day (offset=6) tends to land after the jobs report release window
    for most months without needing the full release calendar.
    """
    return (_ym_first_day(ym) + timedelta(days=day_offset)).strftime("%Y-%m-%d")


def parse_revelio_revision_releases(path: Path) -> Tuple[List[str], str]:
    """
    Parse Revelio revisions CSV to get:
    - sorted list of release months (YYYY-MM)
    - common start month shared by all releases (max of each-release min month)
    """
    df = pd.read_csv(path, dtype=str)
    if "release" not in df.columns or "month" not in df.columns:
        raise ValueError(f"{path} missing required columns (month, release)")

    grouped = df.groupby("release")["month"].agg(["min", "max", "count"]).reset_index()
    releases = sorted(grouped["release"].astype(str).tolist())
    common_start = grouped["min"].astype(str).max()
    return releases, common_start


def parse_revelio_employment_buckets(path: Path) -> Tuple[List[Tuple[str, str]], List[str]]:
    """
    Parse Revelio NAICS employment CSV to get:
    - list of (naics2d_code, naics2d_name) in file order
    - sorted list of months (YYYY-MM)
    """
    df = pd.read_csv(path, dtype=str)
    required = {"month", "naics2d_code", "naics2d_name"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} missing required columns: {sorted(missing)}")

    # Preserve stable ordering as it appears in the file (drop duplicates, keep first)
    buckets_df = df[["naics2d_code", "naics2d_name"]].drop_duplicates(keep="first")
    buckets = list(zip(buckets_df["naics2d_code"].tolist(), buckets_df["naics2d_name"].tolist()))
    months = sorted(df["month"].dropna().astype(str).unique().tolist())
    return buckets, months


def parse_revelio_national_employment(path: Path) -> pd.DataFrame:
    """
    Parse Revelio national employment file:
      month (YYYY-MM), employment_nsa (persons), employment_sa (persons)
    """
    df = pd.read_csv(path, dtype={"month": str})
    required = {"month", "employment_nsa", "employment_sa"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} missing required columns: {sorted(missing)}")
    df = df[["month", "employment_nsa", "employment_sa"]].copy()
    df["month"] = df["month"].astype(str).str.slice(0, 7)
    df["employment_nsa"] = pd.to_numeric(df["employment_nsa"], errors="coerce")
    df["employment_sa"] = pd.to_numeric(df["employment_sa"], errors="coerce")
    df = df.dropna(subset=["month"])
    return df


def parse_adp_ner_comparable(path: Path) -> Optional[pd.DataFrame]:
    """
    Parse ADP_NER_history.csv into a comparable long format keyed by (month, naics2d_code).

    Output columns:
      month, naics2d_code, naics2d_name, employment_nsa_adp, employment_sa_adp

    Notes:
    - ADP "National" is coded as naics2d_code = "00"
    - ADP industry buckets are mapped to either NAICS(ish) codes or synthetic "TTU"
    - Values appear to be levels (persons).
    """
    if not path.exists():
        return None

    df = pd.read_csv(path, dtype={"agg_RIS": str, "category": str, "date": str})
    required = {"agg_RIS", "category", "date", "NER", "NER_SA"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} missing required columns: {sorted(missing)}")

    df = df[df["timestep"].astype(str).str.upper().eq("M")].copy() if "timestep" in df.columns else df.copy()
    df["month"] = df["date"].astype(str).str.slice(0, 7)
    df["employment_nsa_adp"] = pd.to_numeric(df["NER"], errors="coerce")
    df["employment_sa_adp"] = pd.to_numeric(df["NER_SA"], errors="coerce")

    # National
    nat = df[df["agg_RIS"].astype(str).str.strip().eq("National")].copy()
    nat = nat.assign(naics2d_code="00", naics2d_name="Total private (ADP NER)")[[
        "month", "naics2d_code", "naics2d_name", "employment_nsa_adp", "employment_sa_adp"
    ]]

    # Industry
    ind = df[df["agg_RIS"].astype(str).str.strip().eq("Industry")].copy()
    ind["naics2d_code"] = ind["category"].map(ADP_INDUSTRY_TO_SECTOR)
    ind = ind.dropna(subset=["naics2d_code"])
    ind = ind.assign(naics2d_name=ind["category"])[[
        "month", "naics2d_code", "naics2d_name", "employment_nsa_adp", "employment_sa_adp"
    ]]

    out = pd.concat([nat, ind], ignore_index=True)
    out = out.dropna(subset=["month", "naics2d_code"])
    out = out.sort_values(["naics2d_code", "month"]).reset_index(drop=True)
    return out


def find_industry_code_for_supersector(bls_universe: pd.DataFrame, supersector_code: str) -> Optional[str]:
    """
    Locate the canonical CES industry_code (8 digits) for a supersector code.

    Prefers the common aggregate pattern 'CC000000' (e.g., 30 -> 30000000).
    Falls back to the lowest display_level within that supersector code.
    """
    if not supersector_code:
        return None

    code = str(supersector_code).zfill(2)
    target = f"{code}000000"
    if "industry_code" in bls_universe.columns:
        exact = bls_universe[bls_universe["industry_code"].astype(str) == target]
        if not exact.empty:
            return target

    candidates = bls_universe[bls_universe["supersector_code"].astype(str) == code].copy()
    if candidates.empty:
        return None

    # Prefer rows where the industry name matches the supersector label (aggregate row)
    if "industry_name" in candidates.columns and "supersector_name" in candidates.columns:
        name_match = candidates[
            candidates["industry_name"].astype(str).str.lower()
            == candidates["supersector_name"].astype(str).str.lower()
        ]
        if not name_match.empty:
            candidates = name_match

    if "display_level" in candidates.columns:
        candidates["display_level"] = pd.to_numeric(candidates["display_level"], errors="coerce")
        candidates = candidates.sort_values(["display_level", "industry_code"])
    else:
        candidates = candidates.sort_values(["industry_code"])

    return str(candidates.iloc[0]["industry_code"])


def export_bls_revisions_revelio_format(api_key: str, releases: List[str], common_start: str) -> Optional[pd.DataFrame]:
    """
    Export PAYEMS vintages in Revelio's long format:
      month (YYYY-MM), release (YYYY-MM), employment_sa (persons)

    This makes it directly joinable with bls_revisions_revelio.csv on (month, release).
    """
    analyzer = OptimizedBLSAnalyzer(api_key)
    rows: List[Dict[str, object]] = []

    observation_start = f"{common_start}-01"
    for release in releases:
        realtime_date = _approx_release_date_in_month(release, day_offset=6)
        observation_end = _last_day_previous_month(release)
        vintage = analyzer.get_data_as_of_date("PAYEMS", realtime_date, observation_start, observation_end)
        if not vintage:
            continue

        for obs_date, value in vintage.items():
            ym = _ym_from_date_str(obs_date)
            if ym < common_start:
                continue
            # PAYEMS is in thousands of persons on FRED -> convert to persons for Revelio alignment
            rows.append(
                {
                    "month": ym,
                    "release": release,
                    "employment_sa": round(float(value) * 1000) if value is not None else None
                }
            )

    if not rows:
        print("No PAYEMS vintage data collected for Revelio-format export")
        return None

    out = pd.DataFrame(rows)
    out = out.sort_values(["release", "month"])
    out.to_csv(OUTPUT_CSV_BLS_REVISIONS_REVELIO_FORMAT, index=False)
    print(f"Saved BLS (PAYEMS) Revelio-format revisions: {OUTPUT_CSV_BLS_REVISIONS_REVELIO_FORMAT}")
    return out


def export_bls_employment_naics_revelio_format(
    api_key: str,
    bls_universe: pd.DataFrame,
    buckets: List[Tuple[str, str]],
    months: List[str]
) -> Optional[pd.DataFrame]:
    """
    Export a BLS time series in the same schema as employment_naics_revelio.csv:
      month, naics2d_code, naics2d_name, employment_nsa, employment_sa

    Values are converted from thousands (FRED) to persons to match Revelio's units.
    """
    if not months:
        print("No months provided for NAICS employment export")
        return None

    analyzer = OptimizedBLSAnalyzer(api_key)
    obs_start = f"{min(months)}-01"
    obs_end = (_ym_first_day(max(months)) + relativedelta(months=1) - timedelta(days=1)).strftime("%Y-%m-%d")
    realtime_date = datetime.utcnow().strftime("%Y-%m-%d")
    used_fallback: set = set()

    def fetch_latest_with_fallback(primary: Optional[str], fallback: Optional[str]) -> Dict[str, float]:
        if not primary:
            return {}
        data = analyzer.get_latest_data(primary, obs_start, obs_end)
        if data:
            return data
        if not fallback:
            return {}
        data = analyzer.get_latest_data(fallback, obs_start, obs_end)
        if data and (primary, fallback) not in used_fallback:
            used_fallback.add((primary, fallback))
            print(f"Note: Using fallback series for latest levels: {primary} -> {fallback}")
        return data

    def series_ids_for_bucket(code: str) -> Tuple[Optional[str], Optional[str]]:
        # Special totals:
        # - "NF": Total nonfarm payroll employment (includes government) -> PAYEMS/PAYNSA
        # - "00": Total private payroll employment (ADP-comparable) -> CES/CEU 05 supersector
        if code in {"TOTAL_NONFARM", "NF"}:
            return "PAYNSA", "PAYEMS"
        if code in {"TOTAL_PRIVATE", "00"}:
            return "CEU0500000001", "CES0500000001"
        ces_code = REVELIO_TO_CES_SUPERSECTOR.get(code)
        if not ces_code:
            return None, None
        industry_code = find_industry_code_for_supersector(bls_universe, ces_code)
        if not industry_code:
            return None, None
        return f"CEU{industry_code}01", f"CES{industry_code}01"

    # Add coverage-aware totals so the website can compare on an apples-to-apples basis.
    buckets = list(buckets)
    if not any(str(code) == "NF" for code, _ in buckets):
        buckets.insert(0, ("NF", "Total nonfarm payrolls"))
    if not any(str(code) == "00" for code, _ in buckets):
        buckets.insert(0, ("00", "Total private (ex-ag; ex gov; ADP comparable)"))

    rows: List[Dict[str, object]] = []

    for naics_code, naics_name in buckets:
        nsa_series, sa_series = series_ids_for_bucket(str(naics_code))
        # For employment level comparisons we want the latest published values; do not require vintage support.
        # Some common series (notably Total Private) may not be available under the CES*/CEU* id in every FRED account,
        # so we keep a conservative fallback for totals.
        nsa = analyzer.get_latest_data(nsa_series, obs_start, obs_end) if nsa_series else {}
        if str(naics_code) == "00":
            # Total Private SA is often exposed as USPRIV in FRED.
            sa = fetch_latest_with_fallback(sa_series, "USPRIV")
        else:
            sa = analyzer.get_latest_data(sa_series, obs_start, obs_end) if sa_series else {}

        for ym in months:
            obs_date = f"{ym}-01"
            nsa_val = nsa.get(obs_date)
            sa_val = sa.get(obs_date)
            rows.append(
                {
                    "month": ym,
                    "naics2d_code": str(naics_code),
                    "naics2d_name": str(naics_name),
                    "employment_nsa": round(float(nsa_val) * 1000) if nsa_val is not None else None,
                    "employment_sa": round(float(sa_val) * 1000) if sa_val is not None else None
                }
            )

    out = pd.DataFrame(rows)
    out.to_csv(OUTPUT_CSV_BLS_EMPLOYMENT_REVELIO_FORMAT, index=False)
    print(f"Saved BLS Revelio-format employment buckets: {OUTPUT_CSV_BLS_EMPLOYMENT_REVELIO_FORMAT}")
    return out


def export_bls_vs_revelio_comparisons(
    bls_revisions: Optional[pd.DataFrame],
    bls_employment: Optional[pd.DataFrame]
) -> None:
    """
    Produce merge-ready comparison files for the website (BLS vs Revelio).

    - Revisions: joins on (month, release)
    - Employment buckets: joins on (month, naics2d_code)
    """
    def _safe_records(df: pd.DataFrame) -> List[Dict[str, object]]:
        """
        Convert a DataFrame to JSON-safe records:
        - Replaces NaN/Inf with None (including numpy float scalars)
        - Avoids pandas float columns reintroducing NaN when assigning None
        """
        records: List[Dict[str, object]] = df.to_dict(orient="records")

        for rec in records:
            for key, value in list(rec.items()):
                if value is None:
                    continue
                try:
                    as_float = float(value)
                except (TypeError, ValueError):
                    continue
                if math.isnan(as_float) or math.isinf(as_float):
                    rec[key] = None

        return records

    if REVELIO_REVISIONS_CSV.exists() and bls_revisions is not None:
        rev_revelio = pd.read_csv(REVELIO_REVISIONS_CSV)
        rev_merged = rev_revelio.merge(
            bls_revisions,
            on=["month", "release"],
            how="inner",
            suffixes=("_revelio", "_bls")
        )
        if "employment_sa_revelio" in rev_merged.columns and "employment_sa_bls" in rev_merged.columns:
            rev_merged["diff_bls_minus_revelio"] = (
                pd.to_numeric(rev_merged["employment_sa_bls"], errors="coerce")
                - pd.to_numeric(rev_merged["employment_sa_revelio"], errors="coerce")
            )
        rev_merged.to_csv(OUTPUT_CSV_BLS_VS_REVELIO_REVISIONS, index=False)
        with open(OUTPUT_JSON_BLS_VS_REVELIO_REVISIONS, "w", encoding="utf-8") as f:
            json.dump(_safe_records(rev_merged), f, allow_nan=False)
        print(f"Saved comparison: {OUTPUT_CSV_BLS_VS_REVELIO_REVISIONS}")
        print(f"Saved comparison: {OUTPUT_JSON_BLS_VS_REVELIO_REVISIONS}")

    if REVELIO_EMPLOYMENT_CSV.exists() and bls_employment is not None:
        emp_revelio = pd.read_csv(REVELIO_EMPLOYMENT_CSV, dtype={"month": str, "naics2d_code": str})

        # Optional: bring in Revelio's national total (as published).
        # When present, we use it for the "Total nonfarm" series to match Revelio's published headline.
        nat_path = _pick_first_existing(REVELIO_NATIONAL_EMPLOYMENT_CSV_CANDIDATES)
        nat = parse_revelio_national_employment(nat_path) if nat_path is not None else None

        # Build coverage-aware totals from the NAICS buckets:
        # - NF: Total nonfarm (Revelio published, when available)
        # - 00: Total private (exclude agriculture + unclassified + government) -> comparable to ADP "National"
        for col in ["employment_nsa", "employment_sa"]:
            if col in emp_revelio.columns:
                emp_revelio[col] = pd.to_numeric(emp_revelio[col], errors="coerce")

        base = emp_revelio.copy()
        base["naics2d_code"] = base["naics2d_code"].astype(str).str.strip()
        base["month"] = base["month"].astype(str).str.slice(0, 7)
        base = base.dropna(subset=["month", "naics2d_code"])

        exclude_ag = "11"
        exclude_unclassified = "99"
        government = "92"

        def _sum_for(code: str, name: str, exclude_codes: List[str]) -> pd.DataFrame:
            subset = base[~base["naics2d_code"].isin(exclude_codes)].copy()
            summed = (
                subset.groupby("month")[["employment_nsa", "employment_sa"]]
                .sum(min_count=1)
                .reset_index()
            )
            summed["naics2d_code"] = code
            summed["naics2d_name"] = name
            return summed[["month", "naics2d_code", "naics2d_name", "employment_nsa", "employment_sa"]]

        if nat is not None and not nat.empty:
            nf_rows = nat.assign(
                naics2d_code="NF",
                naics2d_name="Total nonfarm (Revelio published)"
            )[["month", "naics2d_code", "naics2d_name", "employment_nsa", "employment_sa"]]
        else:
            nf_rows = _sum_for(
                "NF",
                "Total nonfarm (computed; excludes 11 + 99)",
                [exclude_ag, exclude_unclassified],
            )
        private_rows = _sum_for(
            "00",
            "Total private (ex-ag; ex gov; ADP comparable)",
            [exclude_ag, exclude_unclassified, government],
        )

        emp_revelio = pd.concat([private_rows, nf_rows, emp_revelio], ignore_index=True)

        emp_merged = emp_revelio.merge(
            bls_employment,
            on=["month", "naics2d_code"],
            how="left",
            suffixes=("_revelio", "_bls")
        )
        # Keep Revelio's bucket naming as the canonical label
        if "naics2d_name_revelio" in emp_merged.columns:
            emp_merged = emp_merged.rename(columns={"naics2d_name_revelio": "naics2d_name"})
        if "naics2d_name_bls" in emp_merged.columns:
            emp_merged = emp_merged.drop(columns=["naics2d_name_bls"])

        # Add a synthetic TTU aggregate (ADP uses this combined bucket)
        ttu_parts = ["22", "42", "44-45", "48-49"]
        ttu_source_cols = [
            "employment_nsa_revelio",
            "employment_sa_revelio",
            "employment_nsa_bls",
            "employment_sa_bls"
        ]
        ttu_rows = []
        part_df = emp_merged[emp_merged["naics2d_code"].isin(ttu_parts)].copy()
        for col in ttu_source_cols:
            if col in part_df.columns:
                part_df[col] = pd.to_numeric(part_df[col], errors="coerce")

        for month, g in part_df.groupby("month"):
            rec = {
                "month": month,
                "naics2d_code": "TTU",
                "naics2d_name": "Trade, transportation, and utilities"
            }
            for col in ttu_source_cols:
                if col not in g.columns:
                    continue
                vals = g.set_index("naics2d_code")[col].reindex(ttu_parts)
                rec[col] = float(vals.sum()) if vals.notna().all() else None
            ttu_rows.append(rec)

        if ttu_rows:
            emp_merged = pd.concat([emp_merged, pd.DataFrame(ttu_rows)], ignore_index=True)

        # Merge ADP (National + Industry) levels if provided
        adp_df = parse_adp_ner_comparable(ADP_NER_HISTORY_CSV)
        if adp_df is not None and not adp_df.empty:
            emp_merged = emp_merged.merge(
                adp_df,
                on=["month", "naics2d_code"],
                how="left"
            )

        emp_merged.to_csv(OUTPUT_CSV_BLS_VS_REVELIO_EMPLOYMENT, index=False)
        with open(OUTPUT_JSON_BLS_VS_REVELIO_EMPLOYMENT, "w", encoding="utf-8") as f:
            json.dump(_safe_records(emp_merged), f, allow_nan=False)
        print(f"Saved comparison: {OUTPUT_CSV_BLS_VS_REVELIO_EMPLOYMENT}")
        print(f"Saved comparison: {OUTPUT_JSON_BLS_VS_REVELIO_EMPLOYMENT}")


class BLSExtractor:
    """
    Downloads and parses BLS industry classification files
    
    Provides mapping between industry codes, NAICS codes, and series IDs
    for navigating the BLS employment statistics hierarchy
    """
    
    def __init__(self):
        self.base_url = "https://download.bls.gov/pub/time.series/ce/"

    def download_bls_file(self, filename: str):
        """Fetch BLS reference file from public data repository"""
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        urls = [
            f"{self.base_url}{filename}",
            f"https://download.bls.gov/pub/time.series/ce/{filename}"
        ]
        
        for url in urls:
            try:
                response = requests.get(url, headers=headers, timeout=30)
                if response.status_code == 200:
                    return response.text
            except Exception:
                continue
        
        raise Exception("Could not download BLS industry file")

    def parse_industry_file(self, content: str):
        """Parse tab-delimited BLS industry classification format"""
        lines = content.strip().split('\n')
        industries = []
        
        for line in lines[1:]:  # Skip header row
            parts = line.split('\t')
            if len(parts) >= 5:
                try:
                    industries.append({
                        'industry_code': str(parts[0]).strip().zfill(8),
                        'naics_code': str(parts[1]).strip(),
                        'industry_name': str(parts[3]).strip(),
                        'display_level': int(parts[4]) if parts[4].isdigit() else 0
                    })
                except Exception:
                    continue
        
        return pd.DataFrame(industries)

    def get_supersector_mapping(self):
        """Map BLS supersector codes to industry names"""
        return {
            '00': 'Total nonfarm', '05': 'Total private', '06': 'Goods-producing',
            '07': 'Service-providing', '08': 'Private service-providing',
            '10': 'Mining and logging', '20': 'Construction', '30': 'Manufacturing',
            '31': 'Durable goods', '32': 'Nondurable goods',
            '40': 'Trade, transportation, and utilities', '41': 'Wholesale trade',
            '42': 'Retail trade', '43': 'Transportation and warehousing',
            '44': 'Utilities', '50': 'Information', '55': 'Financial activities',
            '60': 'Professional and business services', '65': 'Education and health services',
            '70': 'Leisure and hospitality', '80': 'Other services', '90': 'Government'
        }
    
    def get_supersector_hierarchy(self):
        """
        Map parent supersectors to their children
        
        Some supersectors like Manufacturing (30) aggregate multiple child
        supersectors (31, 32). When analyzing "30", we need to include both children.
        """
        return {
            '30': ['31', '32'],  # Manufacturing -> Durable, Nondurable
            '40': ['41', '42', '43', '44'],  # Trade/Transport/Utilities -> children
        }
    
    def get_effective_supersector_codes(self, supersector_code: str):
        """
        Get all supersector codes to search for a given code
        
        Returns the code itself plus any children if it's a parent aggregate.
        This handles cases like Manufacturing (30) which contains Durable (31)
        and Nondurable (32) goods.
        """
        hierarchy = self.get_supersector_hierarchy()
        
        if supersector_code in hierarchy:
            # Parent code with children - return all children
            return hierarchy[supersector_code]
        else:
            # Leaf code - return as-is
            return [supersector_code]

    def get_universe(self):
        """Build complete industry hierarchy with series IDs and metadata"""
        content = self.download_bls_file('ce.industry')
        df = self.parse_industry_file(content)
        supersector_map = self.get_supersector_mapping()
        
        # Add supersector information to each industry
        df['supersector_code'] = df['industry_code'].str[:2]
        df['supersector_name'] = df['supersector_code'].map(supersector_map)
        
        # Generate seasonally adjusted (SA) and not seasonally adjusted (NSA) series IDs
        df['sa_series_id'] = 'CES' + df['industry_code'] + '01'
        df['nsa_series_id'] = 'CEU' + df['industry_code'] + '01'
        
        return df.sort_values(['supersector_code', 'display_level', 'industry_code']).reset_index(drop=True)


def analyze_supersector_detailed(api_key, supersector_code, detail_level, start_date, end_date):
    """
    Analyze all detailed industries at specified level within a supersector
    
    Automatically handles parent supersectors by including all child supersectors.
    For example, requesting Manufacturing (30) will analyze both Durable (31)
    and Nondurable (32) goods.
    """
    extractor = BLSExtractor()
    bls_universe = extractor.get_universe()
    
    # Get all supersector codes to search (handles parent-child relationships)
    effective_codes = extractor.get_effective_supersector_codes(supersector_code)
    supersector_map = extractor.get_supersector_mapping()
    
    # Show available levels for this supersector
    matching_industries = bls_universe[bls_universe['supersector_code'].isin(effective_codes)]
    if not matching_industries.empty:
        available_levels = matching_industries['display_level'].value_counts().sort_index()
        print(f"\nAvailable levels for supersector {supersector_code} (searching codes: {effective_codes}):")
        for level, count in available_levels.items():
            print(f"  Level {level}: {count} industries")
        print(f"\nUsing level {detail_level} for analysis")
    
    # Filter to requested detail level
    supersector_detailed = bls_universe[
        (bls_universe['supersector_code'].isin(effective_codes)) & 
        (bls_universe['display_level'] == detail_level)
    ].copy()
    
    if supersector_detailed.empty:
        print(f"\nNo level {detail_level} industries found")
        print(f"Try a different DETAILED_LEVEL value (typically 3 or 4)")
        return None
    
    # Use the requested supersector name for the filename
    supersector_name = supersector_map.get(supersector_code, f"Supersector_{supersector_code}")
    detailed_series = supersector_detailed['nsa_series_id'].tolist()
    
    print(f"\n{'='*60}")
    print(f"Running: {supersector_name} (code {supersector_code}, level {detail_level})")
    print(f"Found {len(detailed_series)} series across codes {effective_codes}")
    print(f"{'='*60}")
    
    # Run analysis
    analyzer = OptimizedBLSAnalyzer(api_key)
    revision_dataset = analyzer.build_dataset(detailed_series, start_date, end_date)
    
    if not revision_dataset.empty:
        # Merge industry names and metadata
        revision_dataset = revision_dataset.merge(
            supersector_detailed[['nsa_series_id', 'industry_name', 'supersector_name']],
            left_on='series_id',
            right_on='nsa_series_id',
            how='left'
        )
        
        # Save to CSV
        safe_name = supersector_name.replace(' ', '_').replace(',', '').replace('/', '_')
        filename = RAW_DIR / f"bls_revisions_level{detail_level}_{supersector_code}_{safe_name}.csv"
        revision_dataset.to_csv(filename, index=False)
        print(f"Saved: {filename}")
        
        return revision_dataset
    else:
        print("No data collected")
        return None


def main():
    """Execute BLS revision analysis based on configuration settings"""
    parser = argparse.ArgumentParser(description="BLS revision analysis and Alt Compare exports.")
    parser.add_argument(
        "--alignment-only",
        action="store_true",
        help="Skip full revision analysis; only generate BLS vs Revelio alignment exports.",
    )
    parser.add_argument(
        "--employment-only",
        action="store_true",
        help="Only refresh Alt Compare employment levels (skips revision exports).",
    )
    args = parser.parse_args()

    run_level2 = RUN_LEVEL_2_ANALYSIS and not args.alignment_only and not args.employment_only
    run_detailed = RUN_DETAILED_ANALYSIS and not args.alignment_only and not args.employment_only
    run_alignment = RUN_REVELIO_ALIGNMENT_EXPORT
    run_alignment_revisions = not args.employment_only

    print("BLS Revision Analysis")
    print(f"Date range: {START_DATE} to {END_DATE}\n")
    
    extractor = BLSExtractor()
    bls_universe = extractor.get_universe()
    
    if bls_universe.empty:
        print("Could not load BLS universe")
        return {}
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    results = {}
    metadata_cache = {}

    def build_level2_manifest():
        level2 = bls_universe[bls_universe['display_level'] == 2].copy()
        manifest = []
        for _, row in level2.iterrows():
            manifest.append({
                'series_id': row['sa_series_id'],
                'industry_name': row['industry_name'],
                'supersector_name': row['supersector_name'],
                'display_level': row['display_level'],
                'seasonally_adjusted': True
            })
            manifest.append({
                'series_id': row['nsa_series_id'],
                'industry_name': row['industry_name'],
                'supersector_name': row['supersector_name'],
                'display_level': row['display_level'],
                'seasonally_adjusted': False
            })

        manifest.extend(
            [
                {
                    'series_id': 'PAYEMS',
                    'industry_name': 'Total Nonfarm (PAYEMS)',
                    'supersector_name': 'Total nonfarm',
                    'display_level': 0,
                    'seasonally_adjusted': True
                },
                {
                    'series_id': 'PAYNSA',
                    'industry_name': 'Total Nonfarm (PAYNSA)',
                    'supersector_name': 'Total nonfarm',
                    'display_level': 0,
                    'seasonally_adjusted': False
                }
            ]
        )

        # Remove duplicates while preserving metadata (take first occurrence)
        seen = set()
        deduped = []
        for item in manifest:
            if item['series_id'] not in seen:
                seen.add(item['series_id'])
                deduped.append(item)
        return deduped

    def export_dataset(df: pd.DataFrame, csv_path: Path, json_path: Path):
        if df.empty:
            print(f"Skipped export for {csv_path} (dataset empty)")
            return

        df.to_csv(csv_path, index=False)
        date_cols = [
            'obs_date', 'vintage_t', 'vintage_t1', 'vintage_t2'
        ]
        serializable = df.copy()
        for col in date_cols:
            if col in serializable.columns:
                serializable[col] = serializable[col].apply(
                    lambda x: x.isoformat() if pd.notnull(x) else None
                )
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(serializable.to_dict(orient="records"), f)
        print(f"Saved CSV: {csv_path}")
        print(f"Saved JSON: {json_path}")

    def run_manifest(manifest, label):
        analyzer = OptimizedBLSAnalyzer(API_KEY)
        series_ids = [m['series_id'] for m in manifest]
        dataset = analyzer.build_dataset(series_ids, START_DATE, END_DATE)
        if dataset.empty:
            print(f"No data collected for {label}")
            return dataset
        meta_df = pd.DataFrame(manifest)
        merged = dataset.merge(meta_df, on="series_id", how="left")
        return merged

    # Analyze broad industries (level 2) plus total nonfarm
    if run_level2:
        print(f"\n{'='*60}")
        print("Running: Level 2 + PAYNSA")
        print(f"{'='*60}")
        level2_manifest = build_level2_manifest()
        level2_data = run_manifest(level2_manifest, "Level 2")
        if not level2_data.empty:
            level2_data['scope'] = 'level2'
            export_dataset(level2_data, OUTPUT_CSV_LEVEL2, OUTPUT_JSON_LEVEL2)
            results['level_2'] = level2_data

    # Analyze detailed industries within a supersector
    if run_detailed:
        data = analyze_supersector_detailed(
            API_KEY, DETAILED_SUPERSECTOR_CODE, DETAILED_LEVEL,
            START_DATE, END_DATE
        )
        if data is not None:
            data['scope'] = f"detailed_{DETAILED_SUPERSECTOR_CODE}"
            export_dataset(data, OUTPUT_CSV_DETAILED, OUTPUT_JSON_DETAILED)
            results['detailed'] = data

    # Optional: Export BLS datasets in the same schema/date formatting as the Revelio CSVs
    if run_alignment:
        try:
            bls_rev_df = None
            bls_emp_df = None
            if run_alignment_revisions and REVELIO_REVISIONS_CSV.exists():
                releases, common_start = parse_revelio_revision_releases(REVELIO_REVISIONS_CSV)
                print(f"\nRevelio alignment: {len(releases)} releases, common start {common_start}")
                bls_rev_df = export_bls_revisions_revelio_format(API_KEY, releases, common_start)
            elif run_alignment_revisions:
                print(f"Revelio revisions file not found: {REVELIO_REVISIONS_CSV}")

            if REVELIO_EMPLOYMENT_CSV.exists():
                buckets, months = parse_revelio_employment_buckets(REVELIO_EMPLOYMENT_CSV)
                print(f"Revelio alignment: {len(buckets)} NAICS buckets, {len(months)} months")
                bls_emp_df = export_bls_employment_naics_revelio_format(API_KEY, bls_universe, buckets, months)
            else:
                print(f"Revelio employment file not found: {REVELIO_EMPLOYMENT_CSV}")

            export_bls_vs_revelio_comparisons(bls_rev_df, bls_emp_df)
        except Exception as exc:
            print(f"Revelio alignment export failed: {exc}")
    
    return results


if __name__ == "__main__":
    results = main()

    print(f"\n{'='*60}")
    print("Analysis complete")
    for key, data in results.items():
        print(f"{key}: {len(data):,} records")
    print(f"{'='*60}")
