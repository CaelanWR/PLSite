import { byId, toNumber, fetchJSONLocal, initInfoPopover } from "./ui_utils.js";

const DATA_PATH = "../data/market_priors_bayes.json";
const FALLBACK_PATH = "../data/market_priors.json";
const LIVE_REFRESH_URL = "../api/priors/refresh";
const REFRESH_MS = 5 * 60 * 1000;
const STORAGE_KEY = "marketPriorsEvent";
const PROVIDER_KEY = "marketPriorsProvider";
const LOOKBACK_KEY = "marketPriorsLookbackDays";
const BAND_KEY = "marketPriorsBand";
const FRED_KEY_STORAGE = "fredApiKey";
const FRED_OBSERVATIONS_ENDPOINT = "https://api.stlouisfed.org/fred/series/observations";

const DEFAULT_EVENTS = [
  { id: "nfp", label: "Nonfarm Payrolls (NFP)", format: "jobs", decimals: 0 },
  { id: "unemployment", label: "Unemployment Rate", format: "percent", decimals: 1 },
  { id: "cpi", label: "CPI Inflation", format: "percent", decimals: 1 },
  { id: "fed", label: "Fed Rate Decision", format: "percent", decimals: 2 }
];

const elements = {
  providerSelect: byId("priorsProviderSelect"),
  eventSelect: byId("priorsEventSelect"),
  lookbackSelect: byId("priorsLookbackSelect"),
  releaseDate: byId("priorsReleaseDate"),
  releaseLink: byId("priorsReleaseLink"),
  updatedAt: byId("priorsUpdatedAt"),
  refreshButton: byId("priorsRefresh"),
  fredKeyInput: byId("priorsFredKey"),
  fredApplyButton: byId("priorsFredApply"),
  fredStatus: byId("priorsFredStatus"),
  status: byId("priorsStatus"),
  metrics: byId("priorsMetrics"),
  benchmarkTitle: byId("priorsBenchmarkTitle"),
  benchmark: byId("priorsBenchmark"),
  distributionCanvas: byId("priorsDistributionChart"),
  trendCanvas: byId("priorsTrendChart"),
  uncertaintyCanvas: byId("priorsUncertaintyChart"),
  uncertaintySubtitle: byId("priorsUncertaintySubtitle"),
  bandSelect: byId("priorsBandSelect"),
  tableHead: byId("priorsTableHead"),
  tableBody: byId("priorsTableBody"),
  infoWrap: byId("priorsInfo"),
  infoBtn: byId("priorsInfoBtn")
};

const state = {
  data: null,
  loading: false,
  usingBayes: false,
  distributionChart: null,
  trendChart: null,
  uncertaintyChart: null,
  refreshTimer: null,
  benchmarkCache: new Map(),
  benchmarkLoading: new Set()
};

const BAND_OPTIONS = {
  "50": { low: 0.25, high: 0.75, label: "50% (p75–p25)" },
  "80": { low: 0.1, high: 0.9, label: "80% (p90–p10)" },
  "90": { low: 0.05, high: 0.95, label: "90% (p95–p05)" }
};

const fredProxyChain = [
  { name: "Direct", build: (url) => url },
  { name: "IsomorphicGit", build: (url) => `https://cors.isomorphic-git.org/${url}` },
  { name: "CorsProxyIO", build: (url) => `https://corsproxy.io/?${encodeURIComponent(url)}` },
  { name: "ThingProxy", build: (url) => `https://thingproxy.freeboard.io/fetch/${url}` }
];

const BENCHMARK_CONFIG = {
  nfp: {
    seriesId: "PAYEMS",
    label: "Last month's payroll change",
    description: "Total nonfarm payrolls (PAYEMS), SA.",
    type: "change",
    scale: 1000,
    format: "jobs"
  },
  unemployment: {
    seriesId: "UNRATE",
    label: "Last month's unemployment rate",
    description: "U-3 unemployment rate, SA.",
    type: "level",
    format: "percent"
  },
  cpi: {
    seriesId: "CPIAUCSL",
    label: "Last month's CPI (MoM)",
    description: "CPI-U monthly change, SA.",
    type: "pct_change",
    format: "percent"
  },
  fed: {
    seriesId: "DFEDTARU",
    label: "Current Fed target (upper bound)",
    description: "Upper bound of the target range.",
    type: "level",
    format: "percent"
  }
};

const storage = {
  get: (key) => {
    try {
      return localStorage.getItem(key);
    } catch (_err) {
      return null;
    }
  },
  set: (key, value) => {
    try {
      localStorage.setItem(key, value);
    } catch (_err) {
    }
  }
};

const safeArray = (value) => (Array.isArray(value) ? value : []);

const parseDate = (value) => {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const formatDate = (value) => {
  const parsed = parseDate(value);
  if (!parsed) return "--";
  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short"
  }).format(parsed);
};

const formatDateShort = (value) => {
  const parsed = parseDate(value);
  if (!parsed) return "";
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric"
  }).format(parsed);
};

const formatPct = (value, decimals = 1) => {
  if (!Number.isFinite(value)) return "--";
  return `${(value * 100).toFixed(decimals)}%`;
};

const formatJobsCompact = (value) => {
  if (!Number.isFinite(value)) return "--";
  const rounded = Math.round(value / 1000);
  return `${rounded.toLocaleString()}K`;
};

const formatRate = (value, decimals) => {
  if (!Number.isFinite(value)) return "--";
  const places = Number.isFinite(decimals) ? decimals : 1;
  return `${value.toFixed(places)}%`;
};

const formatChartPercent = (value, decimals = 0) => {
  if (!Number.isFinite(value)) return "--";
  return `${Number(value).toFixed(decimals)}%`;
};

const formatChartValue = (value, meta, decimals = 2) => {
  if (!Number.isFinite(value)) return "--";
  if (meta.format === "jobs") return formatJobsCompact(value);
  return formatRate(value, decimals);
};

const formatChartValueTooltip = (value, meta, decimals = 2) => {
  if (!Number.isFinite(value)) return "--";
  if (meta.format === "jobs") return `${formatJobsCompact(value)} jobs`;
  return formatRate(value, decimals);
};

const formatExpectedSummary = (value, meta) => {
  if (!Number.isFinite(value)) return "--";
  if (meta.format === "jobs") return `${formatJobsCompact(value)} jobs`;
  return formatRate(value, 2);
};

const formatVolume = (value) => {
  if (!Number.isFinite(value)) return "--";
  const abs = Math.abs(value);
  if (abs >= 1e9) return `${(value / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${(value / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${Math.round(value / 1e3)}K`;
  return Math.round(value).toLocaleString();
};

const getFredKey = () => storage.get(FRED_KEY_STORAGE)?.trim() || "";

const fetchFredJSON = async (url) => {
  const errors = [];
  for (const proxy of fredProxyChain) {
    try {
      const response = await fetch(proxy.build(url));
      if (!response.ok) {
        throw new Error(`${proxy.name} HTTP ${response.status}`);
      }
      return await response.json();
    } catch (error) {
      errors.push(`${proxy.name}: ${error.message}`);
    }
  }
  throw new Error(errors.join("; "));
};

const fetchFredLatest = async (seriesId, limit = 2, apiKey = getFredKey()) => {
  if (!apiKey) {
    throw new Error("FRED API key missing.");
  }
  const params = new URLSearchParams({
    series_id: seriesId,
    api_key: apiKey,
    file_type: "json",
    sort_order: "desc",
    limit: String(limit),
    observation_end: new Date().toISOString().split("T")[0]
  });
  const url = `${FRED_OBSERVATIONS_ENDPOINT}?${params.toString()}`;
  const payload = await fetchFredJSON(url);
  const observations = Array.isArray(payload.observations) ? payload.observations : [];
  const points = observations
    .map((obs) => ({
      date: obs.date,
      value: Number.parseFloat(obs.value)
    }))
    .filter((point) => Number.isFinite(point.value));
  if (!points.length) {
    throw new Error("No numeric FRED observations.");
  }
  return points;
};

const computeBenchmark = (config, points) => {
  if (!config || !points.length) return null;
  const latest = points[0];
  const prev = points[1];
  if (config.type === "change") {
    if (!prev) return null;
    return {
      value: (latest.value - prev.value) * (config.scale || 1),
      date: latest.date,
      previous: prev.value ?? null,
      previousDate: prev.date ?? null,
      delta: latest.value - prev.value
    };
  }
  if (config.type === "pct_change") {
    if (!prev || prev.value === 0) return null;
    return {
      value: ((latest.value / prev.value) - 1) * 100,
      date: latest.date,
      previous: prev.value ?? null,
      previousDate: prev.date ?? null,
      delta: latest.value - prev.value
    };
  }
  return {
    value: latest.value,
    date: latest.date,
    previous: prev?.value ?? null,
    previousDate: prev?.date ?? null,
    delta: prev ? latest.value - prev.value : null
  };
};

const normalizeProb = (value) => {
  const n = toNumber(value);
  if (!Number.isFinite(n)) return null;
  return n > 1.5 ? n / 100 : n;
};

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

const getSelectedBand = () => {
  const value = elements.bandSelect?.value || "80";
  return BAND_OPTIONS[value] || BAND_OPTIONS["80"];
};

const quantileKey = (q) => {
  const pct = Math.round(q * 100);
  return `p${String(pct).padStart(2, "0")}`;
};

const normalizeDistribution = (ranges) => {
  const cleaned = safeArray(ranges)
    .map((r) => ({
      lower: toNumber(r?.lower),
      upper: toNumber(r?.upper),
      prob: normalizeProb(r?.prob)
    }))
    .filter((r) => r.prob !== null);

  const total = cleaned.reduce((acc, r) => acc + (r.prob ?? 0), 0);
  if (total > 0) {
    cleaned.forEach((r) => {
      r.prob = (r.prob ?? 0) / total;
    });
  }
  return cleaned;
};

const rangeKey = (range) => `${range.lower ?? ""}|${range.upper ?? ""}`;

const sortRanges = (a, b) => {
  const al = Number.isFinite(a.lower) ? a.lower : -Infinity;
  const bl = Number.isFinite(b.lower) ? b.lower : -Infinity;
  if (al !== bl) return al - bl;
  const au = Number.isFinite(a.upper) ? a.upper : Infinity;
  const bu = Number.isFinite(b.upper) ? b.upper : Infinity;
  return au - bu;
};

const formatRangeLabel = (range, meta) => {
  const lower = toNumber(range?.lower);
  const upper = toNumber(range?.upper);
  if (meta.format === "jobs") {
    const fmt = (v) => `${Math.round(v / 1000).toLocaleString()}K`;
    if (Number.isFinite(lower) && Number.isFinite(upper)) return `${fmt(lower)}-${fmt(upper)}`;
    if (!Number.isFinite(lower) && Number.isFinite(upper)) return `<${fmt(upper)}`;
    if (Number.isFinite(lower) && !Number.isFinite(upper)) return `${fmt(lower)}+`;
    return "--";
  }
  const fmt = (v) => v.toFixed(meta.decimals ?? 1);
  if (Number.isFinite(lower) && Number.isFinite(upper)) return `${fmt(lower)}-${fmt(upper)}%`;
  if (!Number.isFinite(lower) && Number.isFinite(upper)) return `<${fmt(upper)}%`;
  if (Number.isFinite(lower) && !Number.isFinite(upper)) return `${fmt(lower)}%+`;
  return "--";
};

const mergeMeta = (eventData) => {
  const fallback = DEFAULT_EVENTS.find((event) => event.id === eventData?.id) || DEFAULT_EVENTS[0];
  return {
    id: eventData?.id || fallback.id,
    label: eventData?.name || fallback.label,
    format: eventData?.format || fallback.format,
    decimals: Number.isFinite(eventData?.decimals) ? eventData.decimals : fallback.decimals,
    nextRelease: eventData?.next_release || eventData?.nextRelease || null,
    snapshots: safeArray(eventData?.snapshots)
  };
};

const setStatus = (message, isError = false) => {
  if (!elements.status) return;
  elements.status.textContent = message;
  elements.status.classList.toggle("error", Boolean(isError));
};

const setFredStatus = (message, isError = false) => {
  if (!elements.fredStatus) return;
  elements.fredStatus.textContent = message;
  elements.fredStatus.classList.toggle("error", Boolean(isError));
};

const setReleaseDate = (value) => {
  if (!elements.releaseDate) return;
  const formatted = formatDate(value);
  if ("value" in elements.releaseDate) {
    elements.releaseDate.value = formatted;
  } else {
    elements.releaseDate.textContent = formatted;
  }
};

const setUpdatedAt = (value) => {
  if (!elements.updatedAt) return;
  elements.updatedAt.textContent = value ? `Updated: ${formatDate(value)}` : "Updated: --";
};

const getBinsFromSources = (sources) => {
  const map = new Map();
  Object.values(sources || {}).forEach((ranges) => {
    normalizeDistribution(ranges).forEach((range) => {
      const key = rangeKey(range);
      map.set(key, { lower: range.lower ?? null, upper: range.upper ?? null });
    });
  });
  return Array.from(map.values()).sort(sortRanges);
};

const stepForMeta = (meta) => {
  if (meta.format === "jobs") return 25000;
  if (meta.format === "percent") return 0.1;
  return 1;
};

const buildStandardBins = (bins, meta) => {
  const step = stepForMeta(meta);
  const lowers = bins.map((b) => toNumber(b.lower)).filter(Number.isFinite);
  const uppers = bins.map((b) => toNumber(b.upper)).filter(Number.isFinite);
  if (!lowers.length && !uppers.length) return [];
  const minVal = lowers.length ? Math.min(...lowers) : Math.min(...uppers) - step;
  const maxVal = uppers.length ? Math.max(...uppers) : Math.max(...lowers) + step;
  const start = Math.floor(minVal / step) * step;
  const end = Math.ceil(maxVal / step) * step;
  const out = [];
  out.push({ lower: null, upper: start });
  for (let v = start; v < end; v += step) {
    out.push({ lower: v, upper: v + step });
  }
  out.push({ lower: end, upper: null });
  return out;
};

const expandRange = (range, minBound, maxBound, step) => {
  const lower = Number.isFinite(toNumber(range.lower)) ? toNumber(range.lower) : minBound - step;
  const upper = Number.isFinite(toNumber(range.upper)) ? toNumber(range.upper) : maxBound + step;
  return { lower, upper };
};

const aggregateRangesToStandard = (ranges, standardBins, meta) => {
  const step = stepForMeta(meta);
  const clean = normalizeDistribution(ranges);
  if (!standardBins.length || !clean.length) return [];
  const minBound = standardBins.find((b) => Number.isFinite(toNumber(b.lower)))?.lower ?? 0;
  const maxBound = standardBins
    .slice()
    .reverse()
    .find((b) => Number.isFinite(toNumber(b.upper)))?.upper ?? minBound + step;
  const totals = standardBins.map(() => 0);
  for (const range of clean) {
    const prob = toNumber(range.prob);
    if (!Number.isFinite(prob) || prob <= 0) continue;
    const src = expandRange(range, minBound, maxBound, step);
    const width = src.upper - src.lower;
    if (!(width > 0)) continue;
    standardBins.forEach((bin, idx) => {
      const dst = expandRange(bin, minBound, maxBound, step);
      const overlap = Math.max(0, Math.min(src.upper, dst.upper) - Math.max(src.lower, dst.lower));
      if (overlap > 0) totals[idx] += prob * (overlap / width);
    });
  }
  const totalProb = totals.reduce((acc, v) => acc + v, 0);
  if (totalProb > 0) {
    return totals.map((value) => value / totalProb);
  }
  return totals;
};

const estimateBinWidth = (bins) => {
  const widths = bins
    .map((b) => (Number.isFinite(b.lower) && Number.isFinite(b.upper) ? b.upper - b.lower : null))
    .filter((v) => Number.isFinite(v) && v > 0);
  if (!widths.length) return 50000;
  widths.sort((a, b) => a - b);
  const mid = Math.floor(widths.length / 2);
  return widths.length % 2 ? widths[mid] : (widths[mid - 1] + widths[mid]) / 2;
};

const midpointForBin = (bin, fallbackWidth) => {
  const lower = toNumber(bin?.lower);
  const upper = toNumber(bin?.upper);
  if (Number.isFinite(lower) && Number.isFinite(upper)) return (lower + upper) / 2;
  if (!Number.isFinite(lower) && Number.isFinite(upper)) return upper - fallbackWidth / 2;
  if (Number.isFinite(lower) && !Number.isFinite(upper)) return lower + fallbackWidth / 2;
  return 0;
};

const combineDistributions = (sources) => {
  const map = new Map();
  const ingest = (sourceKey, ranges) => {
    for (const range of normalizeDistribution(ranges)) {
      const lower = toNumber(range?.lower);
      const upper = toNumber(range?.upper);
      const key = rangeKey({ lower, upper });
      const existing = map.get(key) || { lower, upper, kalshi: null, polymarket: null };
      existing[sourceKey] = toNumber(range?.prob);
      map.set(key, existing);
    }
  };

  ingest("kalshi", sources?.kalshi || []);
  ingest("polymarket", sources?.polymarket || []);

  const entries = Array.from(map.values()).map((entry) => {
    const sourcesList = [entry.kalshi, entry.polymarket].filter(Number.isFinite);
    const combinedRaw = sourcesList.length ? sourcesList.reduce((a, b) => a + b, 0) / sourcesList.length : null;
    return { ...entry, combinedRaw };
  });

  const combinedTotal = entries.reduce((acc, entry) => acc + (entry.combinedRaw ?? 0), 0);
  entries.forEach((entry) => {
    entry.combined = combinedTotal > 0 && entry.combinedRaw !== null ? entry.combinedRaw / combinedTotal : 0;
  });

  return entries.sort(sortRanges);
};

const getPosterior = (snapshot) => {
  const posterior = snapshot?.posterior;
  if (posterior?.bins?.length) return posterior;
  return null;
};

const getPosteriorBins = (snapshot, bins) => {
  const posterior = getPosterior(snapshot);
  if (posterior?.bins?.length) {
    return posterior.bins.map((bin) => ({
      lower: bin.lower ?? null,
      upper: bin.upper ?? null,
      midpoint: toNumber(bin.midpoint),
      mean: toNumber(bin.mean),
      p10: toNumber(bin.p10),
      p90: toNumber(bin.p90)
    }));
  }
  const combined = combineDistributions(snapshot?.sources || {});
  const fallbackWidth = estimateBinWidth(bins);
  return combined.map((entry) => ({
    lower: entry.lower ?? null,
    upper: entry.upper ?? null,
    midpoint: midpointForBin(entry, fallbackWidth),
    mean: toNumber(entry.combined),
    p10: null,
    p90: null
  }));
};

const expectedFromBins = (bins, meta) => {
  const width = estimateBinWidth(bins);
  let total = 0;
  bins.forEach((bin) => {
    const prob = toNumber(bin.mean);
    if (!Number.isFinite(prob)) return;
    total += prob * midpointForBin(bin, width);
  });
  return total;
};

const expectedFromRanges = (ranges, bins) => {
  const normalized = normalizeDistribution(ranges);
  if (!normalized.length) return null;
  const width = estimateBinWidth(bins);
  let total = 0;
  for (const range of normalized) {
    const prob = toNumber(range.prob);
    if (!Number.isFinite(prob)) continue;
    total += prob * midpointForBin(range, width);
  }
  return total;
};

const formatBand = (value, meta) => {
  if (!value || !Number.isFinite(value.p10) || !Number.isFinite(value.p90)) return "--";
  const lo = formatExpectedSummary(value.p10, meta);
  const hi = formatExpectedSummary(value.p90, meta);
  return `${lo} to ${hi}`;
};

const formatBandWidth = (value, meta) => {
  if (!Number.isFinite(value)) return "--";
  if (meta.format === "jobs") return `${formatJobsCompact(value)} jobs`;
  const abs = Math.abs(value);
  const decimals = abs < 0.01 ? 3 : 2;
  return formatRate(value, decimals);
};

const formatBenchmarkValue = (value, meta) => {
  if (!Number.isFinite(value)) return "--";
  if (meta.format === "jobs") {
    const rounded = Math.round(value / 1000);
    const sign = rounded > 0 ? "+" : rounded < 0 ? "-" : "";
    const abs = Math.abs(rounded).toLocaleString();
    return `${sign}${abs}K jobs`;
  }
  return formatRate(value, 2);
};

const formatFedDecision = (delta) => {
  if (!Number.isFinite(delta)) return "--";
  const bps = Math.round(delta * 100);
  if (bps === 0) return "Hold (0 bps)";
  const direction = bps > 0 ? "Hike" : "Cut";
  return `${direction} ${Math.abs(bps)} bps`;
};

const setBenchmarkTitle = (meta) => {
  if (!elements.benchmarkTitle) return;
  const config = BENCHMARK_CONFIG[meta.id];
  elements.benchmarkTitle.textContent = config?.label || "Current benchmark";
};

const renderBenchmark = (meta) => {
  if (!elements.benchmark) return;
  setBenchmarkTitle(meta);
  const config = BENCHMARK_CONFIG[meta.id];
  if (!config) {
    elements.benchmark.innerHTML = `
      <div><p class="detail-label">Latest release</p><p>--</p></div>
    `;
    return;
  }
  const apiKey = getFredKey();
  if (!apiKey) {
    elements.benchmark.innerHTML = `
      <div>
        <p class="detail-label">Latest release</p>
        <p>--</p>
        <p class="detail-note">Add your FRED API key on this page to show the latest value.</p>
        <p class="detail-note">${config.description}</p>
      </div>
    `;
    return;
  }
  const cached = state.benchmarkCache.get(meta.id);
  if (!cached) {
    elements.benchmark.innerHTML = `
      <div>
        <p class="detail-label">Latest release</p>
        <p>Loading latest…</p>
        <p class="detail-note">${config.description}</p>
      </div>
    `;
    return;
  }
  const valueLabel = formatBenchmarkValue(cached.value, meta);
  const decisionLabel = meta.id === "fed" ? formatFedDecision(cached.delta) : null;
  const decisionDate = cached.previousDate ? formatDateShort(cached.previousDate) : null;
  const decisionBlock = decisionLabel
    ? `<p class="detail-label">Last decision</p><p>${decisionLabel}${decisionDate ? ` <span class="detail-note">(${decisionDate})</span>` : ""}</p>`
    : "";
  elements.benchmark.innerHTML = `
    <div>
      <p class="detail-label">Latest release</p>
      <p>${valueLabel}</p>
      ${decisionBlock}
      <p class="detail-note">${config.description}</p>
    </div>
  `;
};

const loadBenchmarkForEvent = async (meta) => {
  const config = BENCHMARK_CONFIG[meta.id];
  if (!config) return;
  if (!getFredKey()) return;
  if (state.benchmarkCache.has(meta.id) || state.benchmarkLoading.has(meta.id)) return;
  state.benchmarkLoading.add(meta.id);
  try {
    const points = await fetchFredLatest(config.seriesId, 2);
    const benchmark = computeBenchmark(config, points);
    if (benchmark) {
      state.benchmarkCache.set(meta.id, benchmark);
    }
  } catch (_err) {
  } finally {
    state.benchmarkLoading.delete(meta.id);
    renderBenchmark(meta);
  }
};

const computeVolumeWeights = (kalshiVol, polymarketVol) => {
  if (!Number.isFinite(kalshiVol) || !Number.isFinite(polymarketVol)) return null;
  if (kalshiVol <= 0 || polymarketVol <= 0) return null;
  const rawKalshi = Math.log1p(kalshiVol);
  const rawPoly = Math.log1p(polymarketVol);
  const mean = (rawKalshi + rawPoly) / 2;
  if (!(mean > 0)) return null;
  return {
    kalshi: clamp(rawKalshi / mean, 0.5, 3.0),
    polymarket: clamp(rawPoly / mean, 0.5, 3.0)
  };
};

const getBlendWeightData = (snapshot) => {
  if (!snapshot) return null;
  const meta = snapshot?.source_meta || {};
  const kalshiVol = toNumber(meta?.kalshi?.volume);
  const polyVol = toNumber(meta?.polymarket?.volume);
  const weights = snapshot?.posterior?.model?.volume_weights || computeVolumeWeights(kalshiVol, polyVol);
  if (!weights) return null;
  return { weights, kalshiVol, polyVol };
};

const formatBlendWeightCell = (weight, volume) => {
  if (!Number.isFinite(weight)) return "--";
  const volLabel = Number.isFinite(volume) ? ` (${formatVolume(volume)})` : "";
  return `${weight.toFixed(2)}x${volLabel}`;
};

const renderMetrics = (snapshot, meta) => {
  if (!elements.metrics) return;
  if (!snapshot) {
    elements.metrics.innerHTML = `
      <div><p class=\"detail-label\">Expected (posterior mean)</p><p>--</p></div>
      <div><p class=\"detail-label\">Top range (share)</p><p>--</p></div>
      <div><p class=\"detail-label\">Uncertainty band (p90–p10)</p><p>--</p></div>
    `;
    return;
  }
  const bins = getBinsFromSources(snapshot?.sources || {});
  if (!bins.length) {
    elements.metrics.innerHTML = `
      <div><p class=\"detail-label\">Expected (posterior mean)</p><p>--</p></div>
      <div><p class=\"detail-label\">Top range (share)</p><p>--</p></div>
      <div><p class=\"detail-label\">Uncertainty band (p90–p10)</p><p>--</p></div>
    `;
    return;
  }
  const posteriorBins = getPosteriorBins(snapshot, bins);
  const standardBins = buildStandardBins(bins, meta);
  const posteriorAgg = aggregateRangesToStandard(
    posteriorBins.map((bin) => ({ ...bin, prob: bin.mean })),
    standardBins,
    meta
  );
  const posteriorExpected = getPosterior(snapshot)?.expected || null;
  const posteriorMean = Number.isFinite(posteriorExpected?.mean)
    ? posteriorExpected.mean
    : expectedFromBins(posteriorBins, meta);

  let topLabel = "--";
  let topProb = null;
  if (posteriorAgg.length) {
    let bestIdx = 0;
    let bestProb = -Infinity;
    posteriorAgg.forEach((prob, idx) => {
      if (Number.isFinite(prob) && prob > bestProb) {
        bestProb = prob;
        bestIdx = idx;
      }
    });
    if (standardBins[bestIdx] && Number.isFinite(bestProb)) {
      topLabel = formatRangeLabel(standardBins[bestIdx], meta);
      topProb = bestProb;
    }
  }

  const bandWidth = Number.isFinite(posteriorExpected?.p90) && Number.isFinite(posteriorExpected?.p10)
    ? posteriorExpected.p90 - posteriorExpected.p10
    : null;

  const rows = [
    { label: "Expected (posterior mean)", value: `${formatExpectedSummary(posteriorMean, meta)} (p10–p90: ${formatBand(posteriorExpected, meta)})` },
    { label: "Top range (share)", value: topProb !== null ? `${topLabel} (${formatPct(topProb, 1)})` : "--" },
    { label: "Uncertainty band (p90–p10)", value: Number.isFinite(bandWidth) ? formatBandWidth(bandWidth, meta) : "--" }
  ];

  elements.metrics.innerHTML = rows
    .map(({ label, value }) => `<div><p class="detail-label">${label}</p><p>${value}</p></div>`)
    .join("");
};

const renderTable = (snapshot, meta) => {
  if (!elements.tableHead || !elements.tableBody) return;
  elements.tableHead.innerHTML = `
    <tr>
      <th>Outcome range</th>
      <th>Posterior</th>
      <th>Kalshi</th>
      <th>Polymarket</th>
    </tr>
  `;

  const baseBins = getBinsFromSources(snapshot?.sources || {});
  const standardBins = buildStandardBins(baseBins, meta);
  if (!standardBins.length) {
    elements.tableBody.innerHTML = '<tr><td colspan="4">No data available.</td></tr>';
    return;
  }

  const posteriorBins = getPosteriorBins(snapshot, baseBins);
  const posteriorAgg = aggregateRangesToStandard(
    posteriorBins.map((bin) => ({ ...bin, prob: bin.mean })),
    standardBins,
    meta
  );
  const kalshiAgg = aggregateRangesToStandard(snapshot?.sources?.kalshi || [], standardBins, meta);
  const polyAgg = aggregateRangesToStandard(snapshot?.sources?.polymarket || [], standardBins, meta);

  const rows = standardBins.map((bin, idx) => {
    const label = formatRangeLabel(bin, meta);
    const posteriorLabel = Number.isFinite(posteriorAgg[idx]) ? formatPct(posteriorAgg[idx]) : "--";
    const kalshi = Number.isFinite(kalshiAgg[idx]) ? formatPct(kalshiAgg[idx]) : "--";
    const polymarket = Number.isFinite(polyAgg[idx]) ? formatPct(polyAgg[idx]) : "--";
    return `<tr><td>${label}</td><td>${posteriorLabel}</td><td>${kalshi}</td><td>${polymarket}</td></tr>`;
  });

  const blendData = getBlendWeightData(snapshot);
  const weightRow = `
    <tr>
      <td>Blend weights (notional)</td>
      <td>--</td>
      <td>${formatBlendWeightCell(blendData?.weights?.kalshi, blendData?.kalshiVol)}</td>
      <td>${formatBlendWeightCell(blendData?.weights?.polymarket, blendData?.polyVol)}</td>
    </tr>
  `;
  rows.push(weightRow);
  elements.tableBody.innerHTML = rows.join("");
};

const renderDistributionChart = (snapshot, meta, provider) => {
  if (!elements.distributionCanvas || typeof Chart === "undefined") return;
  const ctx = elements.distributionCanvas.getContext("2d");
  const sources = snapshot?.sources || {};
  const baseBins = getBinsFromSources(sources);
  const standardBins = buildStandardBins(baseBins, meta);
  if (!standardBins.length) return;

  const labels = standardBins.map((bin) => formatRangeLabel(bin, meta));
  const posteriorBins = getPosteriorBins(snapshot, baseBins);
  const posteriorAgg = aggregateRangesToStandard(
    posteriorBins.map((bin) => ({ ...bin, prob: bin.mean })),
    standardBins,
    meta
  );
  const kalshiAgg = aggregateRangesToStandard(sources.kalshi || [], standardBins, meta);
  const polyAgg = aggregateRangesToStandard(sources.polymarket || [], standardBins, meta);

  let values = posteriorAgg;
  let color = "rgba(15, 118, 110, 0.65)";
  if (provider === "kalshi") {
    values = kalshiAgg;
    color = "rgba(37, 99, 235, 0.65)";
  } else if (provider === "polymarket") {
    values = polyAgg;
    color = "rgba(124, 58, 237, 0.65)";
  }

  const config = {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: provider === "posterior" ? "Posterior mean" : provider === "kalshi" ? "Kalshi" : "Polymarket",
          data: values.map((v) => (Number.isFinite(v) ? v * 100 : 0)),
          backgroundColor: color,
          borderColor: color.replace("0.65", "0.95"),
          borderWidth: 1
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${formatChartPercent(ctx.parsed.y, 0)}`
          }
        }
      },
      scales: {
        x: { title: { display: true, text: meta.format === "jobs" ? "Payroll change (k)" : "Rate" } },
        y: {
          min: 0,
          title: { display: true, text: "Probability" },
          ticks: {
            callback: (v) => formatChartPercent(v, 0)
          }
        }
      }
    }
  };

  if (!state.distributionChart) {
    state.distributionChart = new Chart(ctx, config);
  } else {
    state.distributionChart.config.type = config.type;
    state.distributionChart.data = config.data;
    state.distributionChart.options = config.options;
    state.distributionChart.update();
  }
};

const buildExpectationSeriesFromHistory = (eventData, meta, provider, lookbackDays) => {
  const history = eventData?.history;
  const sources = history?.sources || {};
  const sourceKeys = Object.keys(sources);
  if (!sourceKeys.length) return null;
  const bySource = {};
  for (const key of sourceKeys) {
    bySource[key] = safeArray(sources[key])
      .map((point) => ({
        ts: Number(point?.ts),
        expected: toNumber(point?.expected)
      }))
      .filter((point) => Number.isFinite(point.ts) && Number.isFinite(point.expected))
      .map((point) => ({ x: point.ts * 1000, y: point.expected }));
  }

  const union = new Map();
  for (const key of Object.keys(bySource)) {
    bySource[key].forEach((point) => {
      const existing = union.get(point.x) || {};
      existing[key] = point.y;
      union.set(point.x, existing);
    });
  }
  const points = [];
  union.forEach((values, ts) => {
    let value = null;
    if (provider === "posterior") {
      const vals = Object.values(values).filter(Number.isFinite);
      if (vals.length) value = vals.reduce((a, b) => a + b, 0) / vals.length;
    } else if (provider === "kalshi" && Number.isFinite(values.kalshi)) {
      value = values.kalshi;
    } else if (provider === "polymarket" && Number.isFinite(values.polymarket)) {
      value = values.polymarket;
    }
    if (!Number.isFinite(value)) return;
    points.push({ x: ts, y: value });
  });

  if (!points.length) return null;
  points.sort((a, b) => a.x - b.x);
  const latest = points[points.length - 1].x;
  const cutoff = latest - lookbackDays * 24 * 60 * 60 * 1000;
  const filtered = points.filter((point) => point.x >= cutoff);
  return { points: filtered, bandLow: [], bandHigh: [], usedHistory: true };
};

const buildExpectationSeries = (snapshots, meta, provider, lookbackDays) => {
  const sorted = snapshots
    .map((snap) => ({ snap, ts: parseDate(snap?.as_of)?.getTime() ?? null }))
    .filter((item) => Number.isFinite(item.ts))
    .sort((a, b) => a.ts - b.ts);

  if (!sorted.length) return { points: [], bandLow: [], bandHigh: [] };
  const latest = sorted[sorted.length - 1].ts;
  const cutoff = latest - lookbackDays * 24 * 60 * 60 * 1000;

  const points = [];
  const bandLow = [];
  const bandHigh = [];
  sorted.forEach(({ snap, ts }) => {
    if (ts < cutoff) return;
    const bins = getBinsFromSources(snap?.sources || {});
    let value = null;
    let lo = null;
    let hi = null;

    if (provider === "posterior") {
      const posterior = getPosterior(snap);
      if (posterior?.expected) {
        value = toNumber(posterior.expected.mean);
        lo = toNumber(posterior.expected.p10);
        hi = toNumber(posterior.expected.p90);
      } else {
        const pbins = getPosteriorBins(snap, bins);
        value = expectedFromBins(pbins, meta);
      }
    } else if (provider === "kalshi") {
      value = expectedFromRanges(snap?.sources?.kalshi || [], bins);
    } else if (provider === "polymarket") {
      value = expectedFromRanges(snap?.sources?.polymarket || [], bins);
    }

    if (!Number.isFinite(value)) return;
    points.push({ x: ts, y: value });
    if (Number.isFinite(lo) && Number.isFinite(hi)) {
      bandLow.push({ x: ts, y: lo });
      bandHigh.push({ x: ts, y: hi });
    }
  });

  return { points, bandLow, bandHigh };
};

const buildUncertaintySeries = (snapshots, lookbackDays, band) => {
  if (!band || band.low !== 0.1 || band.high !== 0.9) return [];
  const sorted = snapshots
    .map((snap) => ({ snap, ts: parseDate(snap?.as_of)?.getTime() ?? null }))
    .filter((item) => Number.isFinite(item.ts))
    .sort((a, b) => a.ts - b.ts);

  if (!sorted.length) return [];
  const latest = sorted[sorted.length - 1].ts;
  const cutoff = latest - lookbackDays * 24 * 60 * 60 * 1000;

  const points = [];
  sorted.forEach(({ snap, ts }) => {
    if (ts < cutoff) return;
    const expected = snap?.posterior?.expected;
    const p10 = toNumber(expected?.p10);
    const p90 = toNumber(expected?.p90);
    if (!Number.isFinite(p10) || !Number.isFinite(p90)) return;
    const width = p90 - p10;
    if (!Number.isFinite(width)) return;
    points.push({ x: ts, y: width });
  });
  return points;
};

const buildUncertaintySeriesFromHistory = (eventData, provider, lookbackDays, weightMap, band) => {
  const history = eventData?.history;
  const sources = history?.sources || {};
  const sourceKeys = Object.keys(sources);
  if (!sourceKeys.length) return null;
  const lowKey = quantileKey(band.low);
  const highKey = quantileKey(band.high);

  const bySource = {};
  for (const key of sourceKeys) {
    bySource[key] = safeArray(sources[key])
      .map((point) => {
        const p10 = toNumber(point?.[lowKey]);
        const p90 = toNumber(point?.[highKey]);
        const ts = Number(point?.ts);
        if (!Number.isFinite(ts) || !Number.isFinite(p10) || !Number.isFinite(p90)) return null;
        return { x: ts * 1000, y: p90 - p10 };
      })
      .filter(Boolean);
  }

  const union = new Map();
  Object.entries(bySource).forEach(([key, series]) => {
    series.forEach((point) => {
      const existing = union.get(point.x) || {};
      existing[key] = point.y;
      union.set(point.x, existing);
    });
  });

  const points = [];
  union.forEach((values, ts) => {
    let value = null;
    if (provider === "posterior") {
      const entries = Object.entries(values).filter(([, v]) => Number.isFinite(v));
      if (entries.length) {
        let weightSum = 0;
        let total = 0;
        entries.forEach(([key, v]) => {
          const weight = toNumber(weightMap?.[key]) || 1;
          total += v * weight;
          weightSum += weight;
        });
        value = weightSum > 0 ? total / weightSum : null;
      }
    } else if (provider === "kalshi" && Number.isFinite(values.kalshi)) {
      value = values.kalshi;
    } else if (provider === "polymarket" && Number.isFinite(values.polymarket)) {
      value = values.polymarket;
    }
    if (!Number.isFinite(value)) return;
    points.push({ x: ts, y: value });
  });

  if (!points.length) return null;
  points.sort((a, b) => a.x - b.x);
  const latest = points[points.length - 1].x;
  const cutoff = latest - lookbackDays * 24 * 60 * 60 * 1000;
  const filtered = points.filter((point) => point.x >= cutoff);
  return filtered.length ? filtered : null;
};

const renderTrendChart = (eventData, snapshots, meta, provider, lookbackDays) => {
  if (!elements.trendCanvas || typeof Chart === "undefined") return;
  const ctx = elements.trendCanvas.getContext("2d");
  const historySeries = buildExpectationSeriesFromHistory(eventData, meta, provider, lookbackDays);
  const series = historySeries || buildExpectationSeries(snapshots, meta, provider, lookbackDays);
  const datasets = [];

  if (provider === "posterior" && series.bandLow.length && series.bandHigh.length) {
    datasets.push({
      label: "Posterior p10",
      data: series.bandLow,
      borderColor: "transparent",
      pointRadius: 0,
      tension: 0.2,
      fill: false
    });
    datasets.push({
      label: "Posterior p90",
      data: series.bandHigh,
      borderColor: "transparent",
      backgroundColor: "rgba(15, 118, 110, 0.12)",
      pointRadius: 0,
      tension: 0.2,
      fill: "-1"
    });
  }

  datasets.push({
    label: provider === "posterior" ? "Posterior expected" : "Expected",
    data: series.points,
    borderColor: provider === "polymarket" ? "#7c3aed" : "#0f766e",
    backgroundColor: "transparent",
    borderWidth: 2,
    pointRadius: 0,
    tension: 0.2,
    spanGaps: true
  });

  const trendDecimals = meta.id === "fed" ? 3 : 2;
  const config = {
    type: "line",
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (items) => formatDate(items?.[0]?.parsed?.x),
            label: (ctx) => `Expected: ${formatChartValueTooltip(ctx.parsed.y, meta, trendDecimals)}`
          }
        }
      },
      interaction: { mode: "nearest", intersect: false },
      scales: {
        x: {
          type: "linear",
          ticks: {
            callback: (value) => formatDateShort(value)
          }
        },
        y: {
          title: { display: true, text: meta.format === "jobs" ? "Expected payroll change" : "Expected rate" },
          ticks: {
            callback: (value) => formatChartValue(value, meta, trendDecimals)
          }
        }
      }
    }
  };

  if (!state.trendChart) {
    state.trendChart = new Chart(ctx, config);
  } else {
    state.trendChart.config.type = config.type;
    state.trendChart.data = config.data;
    state.trendChart.options = config.options;
    state.trendChart.update();
  }
};

const renderUncertaintyChart = (eventData, snapshots, snapshot, meta, provider, lookbackDays) => {
  if (!elements.uncertaintyCanvas || typeof Chart === "undefined") return;
  const ctx = elements.uncertaintyCanvas.getContext("2d");
  const band = getSelectedBand();
  const weightMap = getBlendWeightData(snapshot)?.weights || null;
  const historyPoints = buildUncertaintySeriesFromHistory(eventData, provider, lookbackDays, weightMap, band);
  const points = historyPoints || (provider === "posterior" ? buildUncertaintySeries(snapshots, lookbackDays, band) : []);
  if (elements.uncertaintySubtitle) {
    elements.uncertaintySubtitle.textContent = `Width of the posterior ${band.label} band.`;
  }

  const config = {
    type: "line",
    data: {
      datasets: [
        {
          label: "Posterior band width",
          data: points,
          borderColor: "#0f766e",
          backgroundColor: "rgba(15, 118, 110, 0.12)",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.2,
          fill: true,
          spanGaps: true
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (items) => formatDate(items?.[0]?.parsed?.x),
            label: (ctx) => `Band width (${band.label}): ${formatChartValueTooltip(ctx.parsed.y, meta, 2)}`
          }
        }
      },
      interaction: { mode: "nearest", intersect: false },
      scales: {
        x: {
          type: "linear",
          ticks: {
            callback: (value) => formatDateShort(value)
          }
        },
        y: {
          title: { display: true, text: meta.format === "jobs" ? "p90–p10 width" : "p90–p10 width" },
          ticks: {
            callback: (value) => formatChartValue(value, meta, 2)
          }
        }
      }
    }
  };

  if (!state.uncertaintyChart) {
    state.uncertaintyChart = new Chart(ctx, config);
  } else {
    state.uncertaintyChart.config.type = config.type;
    state.uncertaintyChart.data = config.data;
    state.uncertaintyChart.options = config.options;
    state.uncertaintyChart.update();
  }
};

const pickLatestSnapshot = (snapshots) => {
  const sorted = snapshots
    .map((snap) => ({ snap, ts: parseDate(snap?.as_of)?.getTime() ?? -Infinity }))
    .filter((item) => Number.isFinite(item.ts))
    .sort((a, b) => b.ts - a.ts)
    .map((item) => item.snap);
  return sorted[0] || null;
};

const getEventList = () => {
  const dataEvents = safeArray(state.data?.events);
  const map = new Map(dataEvents.map((event) => [event.id, event]));
  return DEFAULT_EVENTS.map((event) => map.get(event.id) || { id: event.id, name: event.label, format: event.format, decimals: event.decimals, snapshots: [] });
};

const populateEventSelect = () => {
  if (!elements.eventSelect) return;
  const events = getEventList();
  const selected = events.find((event) => event.id === "nfp")?.id || events[0]?.id;
  elements.eventSelect.innerHTML = events
    .map((event) => {
      const meta = mergeMeta(event);
      const isSelected = meta.id === selected;
      return `<option value="${meta.id}" ${isSelected ? "selected" : ""}>${meta.label}</option>`;
    })
    .join("");
};

const renderEvent = (eventData) => {
  const meta = mergeMeta(eventData);
  const snapshots = meta.snapshots;
  const currentSnapshot = pickLatestSnapshot(snapshots);
  const provider = elements.providerSelect?.value || "posterior";
  const lookbackDays = Number(elements.lookbackSelect?.value || 7);

  setReleaseDate(meta.nextRelease);
  renderBenchmark(meta);
  loadBenchmarkForEvent(meta);
  if (!currentSnapshot) {
    setStatus(`No recent pulls available for ${meta.label}.`, true);
    renderMetrics(null, meta);
    renderBenchmark(meta);
    renderTable({ sources: {} }, meta);
    renderDistributionChart({ sources: {} }, meta, provider);
    renderTrendChart(eventData, [], meta, provider, lookbackDays);
    return;
  }

  renderMetrics(currentSnapshot, meta);
  renderTable(currentSnapshot, meta);
  renderDistributionChart(currentSnapshot, meta, provider);
  renderTrendChart(eventData, snapshots, meta, provider, lookbackDays);
  renderUncertaintyChart(eventData, snapshots, currentSnapshot, meta, provider, lookbackDays);

  const missing = [];
  if (!safeArray(currentSnapshot?.sources?.kalshi).length) missing.push("Kalshi");
  if (!safeArray(currentSnapshot?.sources?.polymarket).length) missing.push("Polymarket");
  const historySeries = buildExpectationSeriesFromHistory(eventData, meta, provider, lookbackDays);
  const fallbackSeries = historySeries || buildExpectationSeries(snapshots, meta, provider, lookbackDays);
  const historyNote = fallbackSeries.points.length <= 1
    ? "Only one refresh — run fetch_market_priors.py again to build history."
    : "";
  const note = [missing.length ? `Missing ${missing.join(" + ")} brackets.` : "", historyNote].filter(Boolean).join(" ");
  setStatus(`Showing ${meta.label} priors. ${note}`.trim(), false);
};

const render = () => {
  const events = getEventList();
  const selectedId = elements.eventSelect?.value || events[0]?.id;
  const eventData = events.find((event) => event.id === selectedId) || events[0];
  if (!eventData) return;
  storage.set(STORAGE_KEY, eventData.id);
  renderEvent(eventData);
};

const loadData = async ({ silent = false } = {}) => {
  if (state.loading) return;
  state.loading = true;
  if (!silent) setStatus("Loading market priors...", false);
  try {
    let data = await fetchJSONLocal(DATA_PATH);
    state.usingBayes = true;
    state.data = data;
  } catch (_err) {
    try {
      const data = await fetchJSONLocal(FALLBACK_PATH);
      state.data = data;
      state.usingBayes = false;
    } catch (err) {
      setStatus("Failed to load market priors data. Check data/market_priors.json.", true);
      state.loading = false;
      return;
    }
  }
  setUpdatedAt(state.data?.updated_at || state.data?.updatedAt || null);
  populateEventSelect();
  render();
  state.loading = false;
};

const refreshLiveData = async () => {
  try {
    setStatus("Refreshing live priors...", false);
    state.benchmarkCache.clear();
    const response = await fetch(LIVE_REFRESH_URL, { method: "POST" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json().catch(() => ({}));
    if (payload?.status !== "ok") {
      throw new Error(payload?.error || "Refresh failed.");
    }
    setStatus(payload?.message || "Market priors refreshed.", false);
  } catch (_err) {
    setStatus("Live refresh unavailable. Run scripts/serve.py to enable.", true);
  } finally {
    await loadData({ silent: true });
  }
};

const startAutoRefresh = () => {
  if (state.refreshTimer) clearInterval(state.refreshTimer);
  state.refreshTimer = setInterval(() => {
    if (document.visibilityState !== "visible") return;
    loadData({ silent: true });
  }, REFRESH_MS);

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      loadData({ silent: true });
    }
  });
};

if (elements.providerSelect) {
  const stored = storage.get(PROVIDER_KEY);
  if (stored && ["posterior", "kalshi", "polymarket"].includes(stored)) {
    elements.providerSelect.value = stored;
  }
  elements.providerSelect.addEventListener("change", () => {
    storage.set(PROVIDER_KEY, elements.providerSelect.value);
    render();
  });
}

if (elements.lookbackSelect) {
  const stored = storage.get(LOOKBACK_KEY);
  if (stored && Array.from(elements.lookbackSelect.options).some((opt) => opt.value === stored)) {
    elements.lookbackSelect.value = stored;
  }
  elements.lookbackSelect.addEventListener("change", () => {
    storage.set(LOOKBACK_KEY, elements.lookbackSelect.value);
    render();
  });
}

if (elements.bandSelect) {
  const stored = storage.get(BAND_KEY);
  if (stored && BAND_OPTIONS[stored]) {
    elements.bandSelect.value = stored;
  }
  elements.bandSelect.addEventListener("change", () => {
    storage.set(BAND_KEY, elements.bandSelect.value);
    render();
  });
}

elements.eventSelect?.addEventListener("change", () => render());

elements.refreshButton?.addEventListener("click", () => refreshLiveData());

const applyFredKey = () => {
  if (!elements.fredKeyInput) return;
  const trimmed = elements.fredKeyInput.value.trim();
  if (!trimmed) {
    setFredStatus("Enter a valid FRED API key.", true);
    return;
  }
  storage.set(FRED_KEY_STORAGE, trimmed);
  setFredStatus("FRED API key saved. Loading latest releases...", false);
  state.benchmarkCache.clear();
  render();
};

if (elements.fredKeyInput) {
  elements.fredKeyInput.value = getFredKey();
  elements.fredKeyInput.addEventListener("keypress", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      applyFredKey();
    }
  });
}

if (elements.fredApplyButton) {
  elements.fredApplyButton.addEventListener("click", applyFredKey);
}

if (elements.fredStatus) {
  const hasKey = Boolean(getFredKey());
  setFredStatus(
    hasKey
      ? "Key stored locally in this browser only."
      : "Add your FRED API key to show the latest release.",
    false
  );
}

initInfoPopover({ wrap: elements.infoWrap, btn: elements.infoBtn });

populateEventSelect();
loadData();
startAutoRefresh();
