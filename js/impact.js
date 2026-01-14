import { byId, toNumber, fetchJSONLocal, initInfoPopover } from "./ui_utils.js";

const elements = {
  summaryProviderSelect: byId("impactSummaryProviderSelect"),
  sectionsWrap: byId("impactSections"),
  sourceSelect: byId("impactSourceSelect"),
  eventSelect: byId("impactEventSelect"),
  strikeSelect: byId("impactStrikeSelect"),
  horizonSelect: byId("impactHorizonSelect"),
  barModeSelect: byId("impactBarModeSelect"),
  refreshButton: byId("impactRefresh"),
  status: byId("impactStatus"),
  summaryMetrics: byId("impactSummaryMetrics"),
  summaryChart: byId("impactSummaryChart"),
  summaryVolumeChart: byId("impactSummaryVolumeChart"),
  trendCanvas: byId("impactTrendChart"),
  trendWrap: byId("impactTrendWrap"),
  trendEmpty: byId("impactTrendEmpty"),
  trendVolumeCanvas: byId("impactTrendVolumeChart"),
  trendVolumeWrap: byId("impactTrendVolumeWrap"),
  trendVolumeEmpty: byId("impactTrendVolumeEmpty"),
  trendVolumeTitle: byId("impactTrendVolumeTitle"),
  trendWindowSelect: byId("impactTrendWindowSelect"),
  trendVolumeWindowSelect: byId("impactTrendVolumeWindowSelect"),
  arbMetrics: byId("impactArbMetrics"),
  arbCanvas: byId("impactArbChart"),
  arbWrap: byId("impactArbWrap"),
  arbEmpty: byId("impactArbEmpty"),
  arbDetail: byId("impactArbDetail"),
  expectationText: byId("impactExpectationText"),
  expectationMetrics: byId("impactExpectationMetrics"),
  expectationCanvas: byId("impactExpectationChart"),
  canvas: byId("impactChart"),
  barCanvas: byId("impactBarChart"),
  volumeCanvas: byId("impactVolumeChart"),
  volumeWrap: byId("impactVolumeWrap"),
  volumeEmpty: byId("impactVolumeEmpty"),
  volumeTitle: byId("impactVolumeTitle"),
  volumeWindowSelect: byId("impactVolumeWindowSelect"),
  densityLegend: byId("impactDensityLegend"),
  densityLegendBefore: byId("impactDensityLegendBefore"),
  densityLegendAfter: byId("impactDensityLegendAfter"),
  tableHead: byId("impactTableHead"),
  tableBody: byId("impactTableBody"),
  historyHead: byId("impactHistoryTableHead"),
  historyBody: byId("impactHistoryTableBody"),
  infoWrap: byId("impactInfo"),
  infoBtn: byId("impactInfoBtn"),
  customToggle: byId("impactCustomToggle"),
  customPanel: byId("impactCustomPanel"),
  customLabel: byId("impactCustomLabel"),
  customRelease: byId("impactCustomRelease"),
  customPayroll: byId("impactCustomPayroll"),
  customActual: byId("impactCustomActual"),
  customMin: byId("impactCustomMin"),
  customMax: byId("impactCustomMax"),
  customAdd: byId("impactCustomAdd"),
  customClear: byId("impactCustomClear"),
  customCancel: byId("impactCustomCancel")
};

const state = {
  data: null,
  loaded: false,
  loading: false,
  provider: "kalshi",
  datasets: { kalshi: null, polymarket: null },
  chart: null,
  trendChart: null,
  trendVolumeChart: null,
  arbChart: null,
  summaryChart: null,
  summaryVolumeChart: null,
  barChart: null,
  expectationChart: null,
  volumeChart: null,
  blsByMonth: null
};

const PROVIDER_KEY = "impactProvider";
const SUMMARY_PROVIDER_KEY = "impactSummaryProvider";
const TOP_SECTION_KEY = "impactTopSection";
const LONGTERM_WINDOW_KEY = "impactLongtermWindowDays";
const LONGTERM_VOLUME_WINDOW_KEY = "impactLongtermVolumeWindow";
const VOLUME_WINDOW_KEY = "impactVolumeWindowMinutes";
const CUSTOM_EVENTS_KEY = "impactCustomEvents";
const ARB_WINDOW_THRESHOLD_PP = 5;
const DEFAULT_CUSTOM_FORM = {
  label: "Trump Posts Numbers",
  release_iso: "2026-01-08T17:20:00-05:00",
  payroll_month: "2025-12",
  actual_min: 40000,
  actual_max: 60000
};
const BLS_EMPLOYMENT_PATH = "../data/bls_vs_revelio_employment.json";
const BLS_NAICS_CODE = "NF";
const PROVIDER_CONFIG = {
  kalshi: { label: "Kalshi", path: "../data/kalshi_impact.json" },
  polymarket: { label: "Polymarket", path: "../data/polymarket_impact.json" }
};

const formatPct = (value) => {
  if (!Number.isFinite(value)) return "—";
  return `${value.toFixed(2)}pp`;
};

const formatProb = (value) => {
  if (!Number.isFinite(value)) return "—";
  const normalized = value > 1 ? value : value * 100;
  return `${normalized.toFixed(1)}%`;
};

const formatInt = (value) => {
  if (!Number.isFinite(value)) return "—";
  return Math.round(value).toLocaleString();
};

const formatDurationLabel = (minutes) => {
  const mins = toNumber(minutes);
  if (!Number.isFinite(mins) || mins <= 0) return "—";
  if (mins % 1440 === 0) return `${mins / 1440}d`;
  if (mins % 60 === 0) return `${mins / 60}h`;
  return `${mins}m`;
};

const formatJobsCompact = (value) => {
  if (!Number.isFinite(value)) return "—";
  const rounded = Math.round(value / 1000);
  const sign = rounded > 0 ? "+" : "";
  return `${sign}${rounded.toLocaleString()}k`;
};

const formatJobsLevelCompact = (value) => {
  if (!Number.isFinite(value)) return "—";
  const rounded = Math.round(value / 1000);
  return `${rounded.toLocaleString()}k`;
};

const formatVolumePair = (pre, post, fallback = "—") => {
  if (!Number.isFinite(pre) && !Number.isFinite(post)) return fallback;
  const preLabel = Number.isFinite(pre) ? formatInt(pre) : "—";
  const postLabel = Number.isFinite(post) ? formatInt(post) : "—";
  return `${preLabel} / ${postLabel}`;
};

const formatJobsRangeCompact = (range) => {
  if (!range || !Number.isFinite(range.min) || !Number.isFinite(range.max)) return "—";
  const fmt = (value) => {
    const rounded = Math.round(value / 1000);
    if (!Number.isFinite(rounded)) return "—";
    if (rounded === 0) return "0k";
    const sign = rounded < 0 ? "-" : "";
    return `${sign}${Math.abs(rounded).toLocaleString()}k`;
  };
  return `${fmt(range.min)}–${fmt(range.max)}`;
};

const DEFAULT_LONGTERM_WINDOW_DAYS = 7;
const MAX_LONGTERM_WINDOW_DAYS = 45;
const DEFAULT_LONGTERM_VOLUME_WINDOW_MINUTES = 6 * 60;

const expWeight = (distance, scale) => {
  const d = toNumber(distance);
  const s = toNumber(scale);
  if (!Number.isFinite(d) || !Number.isFinite(s) || s <= 0) return null;
  return Math.exp(-Math.abs(d) / s);
};

const formatRatio = (value) => {
  if (!Number.isFinite(value)) return "—";
  return `${value.toFixed(2)}×`;
};

const formatDateShort = (tsSeconds) => {
  const ts = toNumber(tsSeconds);
  if (!Number.isFinite(ts)) return "";
  const date = new Date(ts * 1000);
  if (Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "2-digit" }).format(date);
};

const formatDateTimeShort = (tsSeconds) => {
  const ts = toNumber(tsSeconds);
  if (!Number.isFinite(ts)) return "";
  const date = new Date(ts * 1000);
  if (Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat("en-US", { dateStyle: "medium", timeStyle: "short" }).format(date);
};

const hexToRgba = (hex, alpha) => {
  if (typeof hex !== "string" || !hex.startsWith("#")) return hex;
  const normalized = hex.replace("#", "");
  if (normalized.length !== 6) return hex;
  const r = Number.parseInt(normalized.slice(0, 2), 16);
  const g = Number.parseInt(normalized.slice(2, 4), 16);
  const b = Number.parseInt(normalized.slice(4, 6), 16);
  if (![r, g, b].every((v) => Number.isFinite(v))) return hex;
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
};

let announcedBandRegistered = false;

const ANNOUNCED_BAND_PLUGIN = {
  id: "announcedBand",
  beforeDatasetsDraw(chart) {
    const range = chart?.$announcedRange ?? chart?.options?.plugins?.announcedRange;
    if (!range || !Number.isFinite(range.min) || !Number.isFinite(range.max)) return;
    const { ctx, chartArea, scales } = chart;
    if (!chartArea || !scales?.x) return;
    const xMin = range.min / 1000;
    const xMax = range.max / 1000;
    if (!Number.isFinite(xMin) || !Number.isFinite(xMax)) return;
    const left = scales.x.getPixelForValue(Math.min(xMin, xMax));
    const right = scales.x.getPixelForValue(Math.max(xMin, xMax));
    if (!Number.isFinite(left) || !Number.isFinite(right)) return;
    const top = chartArea.top;
    const bottom = chartArea.bottom;
    ctx.save();
    ctx.fillStyle = "rgba(245, 158, 11, 0.18)";
    ctx.fillRect(left, top, right - left, bottom - top);
    ctx.strokeStyle = "rgba(245, 158, 11, 0.7)";
    ctx.lineWidth = 1.5;
    ctx.setLineDash([6, 4]);
    ctx.beginPath();
    ctx.moveTo(left, top);
    ctx.lineTo(left, bottom);
    ctx.moveTo(right, top);
    ctx.lineTo(right, bottom);
    ctx.stroke();
    ctx.restore();
  }
};

const signOf = (value, eps = 1e-9) => {
  const v = toNumber(value);
  if (!Number.isFinite(v)) return null;
  if (v > eps) return 1;
  if (v < -eps) return -1;
  return 0;
};

const formatCorr = (value, n) => {
  if (!Number.isFinite(value)) return "—";
  const suffix = Number.isFinite(n) ? ` (n=${n})` : "";
  return `${value.toFixed(2)}${suffix}`;
};

const getPrevMonth = (yearMonth) => {
  const [yRaw, mRaw] = String(yearMonth || "").split("-");
  const year = Number(yRaw);
  const month = Number(mRaw);
  if (!Number.isFinite(year) || !Number.isFinite(month)) return null;
  if (month === 1) return `${year - 1}-12`;
  if (month >= 2 && month <= 12) return `${year}-${String(month - 1).padStart(2, "0")}`;
  return null;
};

const getAnnouncedRange = (value) => {
  const min = toNumber(value?.actual_min ?? value?.actual_low ?? value?.min);
  const max = toNumber(value?.actual_max ?? value?.actual_high ?? value?.max);
  if (!Number.isFinite(min) || !Number.isFinite(max)) return null;
  return { min: Math.min(min, max), max: Math.max(min, max) };
};

const getAnnouncedValue = (value) => {
  const actual = toNumber(value?.actual);
  if (Number.isFinite(actual)) return actual;
  const range = getAnnouncedRange(value);
  if (!range) return null;
  return (range.min + range.max) / 2;
};

const formatAnnouncementValue = (announcement) => {
  if (!announcement) return "—";
  if (announcement.range) return formatJobsRangeCompact(announcement.range);
  if (Number.isFinite(announcement.value)) return formatJobsCompact(announcement.value);
  return "—";
};

const getEventAnnouncement = (event) => {
  const typeKey = String(event?.type || "").toLowerCase();
  if (!isAnnouncedType(typeKey)) return null;
  const range = getAnnouncedRange(event?.value);
  const value = getAnnouncedValue(event?.value);
  if (!range && !Number.isFinite(value)) return null;
  const label =
    typeKey === "adp" ? "ADP announced" : typeKey === "revelio" ? "Revelio announced" : "Custom announced";
  return { label, announcement: { range, value } };
};

const ensureBlsByMonth = async () => {
  if (state.blsByMonth) return state.blsByMonth;
  try {
    const rows = await fetchJSONLocal(BLS_EMPLOYMENT_PATH);
    const map = {};
    safeArray(rows).forEach((row) => {
      if (String(row?.naics2d_code || "") !== BLS_NAICS_CODE) return;
      const month = row?.month;
      const value = toNumber(row?.employment_sa_bls);
      if (!month || !Number.isFinite(value)) return;
      map[String(month)] = value;
    });
    state.blsByMonth = map;
    return map;
  } catch (err) {
    state.blsByMonth = null;
    return null;
  }
};

const getResolvedBlsChange = (payrollMonth, blsByMonth) => {
  if (!payrollMonth || !blsByMonth) return null;
  const prev = getPrevMonth(payrollMonth);
  if (!prev) return null;
  const current = toNumber(blsByMonth[payrollMonth]);
  const prior = toNumber(blsByMonth[prev]);
  if (!Number.isFinite(current) || !Number.isFinite(prior)) return null;
  return current - prior;
};

const computeVolumeStats = (candles, ts0, preMinutes, postMinutes) => {
  if (!Number.isFinite(ts0)) return null;
  const pre = toNumber(preMinutes) ?? 30;
  const post = toNumber(postMinutes) ?? 240;
  const preStart = ts0 - pre * 60;
  const postEnd = ts0 + post * 60;
  const volPre = sumVolumeInRange(candles, preStart, ts0);
  const volPost = sumVolumeInRange(candles, ts0, postEnd);
  const preRate = Number.isFinite(volPre) && pre > 0 ? volPre / pre : null;
  const postRate = Number.isFinite(volPost) && post > 0 ? volPost / post : null;
  const rateRatio = Number.isFinite(preRate) && preRate > 0 && Number.isFinite(postRate) ? postRate / preRate : null;
  return { pre: volPre, post: volPost, rate_ratio: rateRatio };
};

const computeAggregateVolumeStats = (markets, ts0, preMinutes, postMinutes) => {
  if (!Number.isFinite(ts0)) return null;
  const pre = toNumber(preMinutes) ?? 30;
  const post = toNumber(postMinutes) ?? 240;
  const preStart = ts0 - pre * 60;
  const postEnd = ts0 + post * 60;
  let preTotal = 0;
  let postTotal = 0;
  let seen = false;
  for (const market of safeArray(markets)) {
    const candles = safeArray(market?.candles);
    const volPre = sumVolumeInRange(candles, preStart, ts0);
    const volPost = sumVolumeInRange(candles, ts0, postEnd);
    if (Number.isFinite(volPre)) {
      preTotal += volPre;
      seen = true;
    }
    if (Number.isFinite(volPost)) {
      postTotal += volPost;
      seen = true;
    }
  }
  if (!seen) return null;
  const preRate = pre > 0 ? preTotal / pre : null;
  const postRate = post > 0 ? postTotal / post : null;
  const rateRatio = Number.isFinite(preRate) && preRate > 0 && Number.isFinite(postRate) ? postRate / preRate : null;
  return { pre: preTotal, post: postTotal, rate_ratio: rateRatio };
};

const buildVolumeSeries = (markets, ts0, preMinutes, postMinutes, windowMinutes) => {
  if (!Number.isFinite(ts0)) return { series: [], hasVolume: false };
  const pre = toNumber(preMinutes) ?? 30;
  const post = toNumber(postMinutes) ?? 240;
  const window = toNumber(windowMinutes) ?? 30;
  const startTs = ts0 - pre * 60;
  const endTs = ts0 + post * 60;
  const bucket = new Map();
  let hasVolume = false;
  for (const market of safeArray(markets)) {
    for (const candle of safeArray(market?.candles)) {
      const ts = toNumber(candle?.[0]);
      const vol = toNumber(candle?.[2]);
      if (!Number.isFinite(ts) || ts < startTs || ts > endTs) continue;
      if (!Number.isFinite(vol)) continue;
      hasVolume = true;
      const minute = Math.round((ts - ts0) / 60);
      bucket.set(minute, (bucket.get(minute) || 0) + vol);
    }
  }
  const values = [];
  for (let m = -pre; m <= post; m += 1) {
    values.push(bucket.get(m) || 0);
  }
  const series = [];
  if (window > 0) {
    const w = Math.max(1, Math.round(window));
    const prefix = [0];
    for (const v of values) prefix.push(prefix[prefix.length - 1] + v);
    for (let i = 0; i < values.length; i += 1) {
      const startIdx = Math.max(0, i - w + 1);
      const total = prefix[i + 1] - prefix[startIdx];
      series.push({ x: i - pre, y: total });
    }
  } else {
    let running = 0;
    for (let i = 0; i < values.length; i += 1) {
      running += values[i];
      series.push({ x: i - pre, y: running });
    }
  }
  return { series, hasVolume };
};

const setVolumeVisibility = (show, message = "") => {
  if (elements.volumeWrap) {
    elements.volumeWrap.style.display = show ? "block" : "none";
  }
  if (elements.volumeEmpty) {
    elements.volumeEmpty.textContent = message || "";
    elements.volumeEmpty.style.display = message ? "block" : "none";
  }
};

const renderVolumeChart = (series) => {
  if (!elements.volumeCanvas || typeof Chart === "undefined") return;
  const ctx = elements.volumeCanvas.getContext("2d");
  const config = {
    type: "line",
    data: {
      datasets: [
        {
          label: "Cumulative volume",
          data: safeArray(series),
          borderColor: "#2563eb",
          backgroundColor: "rgba(37, 99, 235, 0.18)",
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
            title: (items) => {
              if (!items?.length) return "";
              const offset = toNumber(items[0]?.parsed?.x);
              if (!Number.isFinite(offset) || !Number.isFinite(toNumber(releaseTs))) return "";
              return formatDateTimeShort(toNumber(releaseTs) + offset * 86400);
            },
            label: (item) => {
              const y = toNumber(item?.parsed?.y);
              if (!Number.isFinite(y)) return "";
              return `Expected: ${Math.round(y).toLocaleString()}k`;
            }
          }
        }
      },
      interaction: { mode: "nearest", intersect: false },
      scales: {
        x: {
          type: "linear",
          title: { display: true, text: "Minutes from release" },
          ticks: { callback: (value) => `${value}m` }
        },
        y: {
          title: { display: true, text: "Rolling volume (contracts)" },
          ticks: { callback: (value) => formatInt(value) }
        }
      }
    }
  };
  if (!state.volumeChart) {
    state.volumeChart = new Chart(ctx, config);
  } else {
    state.volumeChart.config.type = config.type;
    state.volumeChart.data = config.data;
    state.volumeChart.options = config.options;
    state.volumeChart.update();
  }
};

const pickLongtermTimeline = (markets) => {
  let best = [];
  for (const market of safeArray(markets)) {
    const candles = safeArray(market?.longterm_candles);
    if (candles.length > best.length) best = candles;
  }
  const times = best
    .map((c) => toNumber(c?.[0]))
    .filter((v) => Number.isFinite(v))
    .sort((a, b) => a - b);
  return times;
};

const computeLongtermExpectationSeries = (markets, ts0) => {
  const base = toNumber(ts0);
  if (!Number.isFinite(base)) {
    return { series: [], minX: -30, maxX: 30, hasCandles: false };
  }
  const timeline = pickLongtermTimeline(markets);
  if (!timeline.length) {
    return { series: [], minX: -30, maxX: 30, hasCandles: false };
  }
  const series = [];
  for (const t of timeline) {
    const stats = computeExpectationStatsAtTime(markets, base, t, "longterm_candles");
    if (!stats) continue;
    const x = (t - base) / 86400;
    series.push({ x, y: stats.expected / 1000 });
  }
  const xs = series.map((p) => p.x);
  const minX = xs.length ? Math.min(...xs) : -30;
  const maxX = xs.length ? Math.max(...xs) : 30;
  return { series, minX, maxX, hasCandles: true };
};

const computeLongtermVolumeSeries = (markets, ts0, windowMinutes) => {
  const base = toNumber(ts0);
  if (!Number.isFinite(base)) return { series: [], minX: -30, maxX: 30, hasVolume: false };
  const timeline = pickLongtermTimeline(markets);
  if (!timeline.length) return { series: [], minX: -30, maxX: 30, hasVolume: false };
  const volumeMaps = safeArray(markets).map((market) => {
    const map = new Map();
    for (const candle of safeArray(market?.longterm_candles)) {
      const ts = toNumber(candle?.[0]);
      const vol = toNumber(candle?.[2]);
      const price = toNumber(candle?.[1]);
      if (!Number.isFinite(ts) || !Number.isFinite(vol)) continue;
      const notional = Number.isFinite(price) ? vol * (price / 100) : vol;
      map.set(ts, notional);
    }
    return map;
  });
  const raw = [];
  let hasVolume = false;
  for (const t of timeline) {
    let total = 0;
    let seen = false;
    for (const map of volumeMaps) {
      const v = map.get(t);
      if (!Number.isFinite(v)) continue;
      total += v;
      seen = true;
    }
    if (!seen) continue;
    hasVolume = true;
    raw.push({ ts: t, value: total });
  }
  if (!raw.length) return { series: [], minX: -30, maxX: 30, hasVolume };
  raw.sort((a, b) => a.ts - b.ts);
  const window = toNumber(windowMinutes);
  const windowSec = Number.isFinite(window) && window > 0 ? window * 60 : 0;
  const series = [];
  if (windowSec > 0) {
    let startIdx = 0;
    let rolling = 0;
    for (let i = 0; i < raw.length; i += 1) {
      const current = raw[i];
      rolling += current.value;
      while (raw[i].ts - raw[startIdx].ts > windowSec) {
        rolling -= raw[startIdx].value;
        startIdx += 1;
      }
      const x = (current.ts - base) / 86400;
      series.push({ x, y: rolling });
    }
  } else {
    for (const row of raw) {
      const x = (row.ts - base) / 86400;
      series.push({ x, y: row.value });
    }
  }
  const xs = series.map((p) => p.x);
  const minX = xs.length ? Math.min(...xs) : -30;
  const maxX = xs.length ? Math.max(...xs) : 30;
  return { series, minX, maxX, hasVolume };
};

const LONGTERM_ALIGN_SECONDS = 3600;

const normalizeLongtermTimestamp = (ts) => {
  const value = toNumber(ts);
  if (!Number.isFinite(value)) return null;
  return Math.round(value / LONGTERM_ALIGN_SECONDS) * LONGTERM_ALIGN_SECONDS;
};

const buildLongtermTimeline = (markets) => {
  const times = new Set();
  for (const market of safeArray(markets)) {
    for (const candle of safeArray(market?.longterm_candles)) {
      const ts = normalizeLongtermTimestamp(candle?.[0]);
      if (!Number.isFinite(ts)) continue;
      times.add(ts);
    }
  }
  return Array.from(times).sort((a, b) => a - b);
};

const normalizeBuckets = (buckets) => {
  const normalized = [];
  let total = 0;
  for (const bucket of safeArray(buckets)) {
    const rep = toNumber(bucket?.rep);
    const p = toNumber(bucket?.p);
    if (!Number.isFinite(rep) || !Number.isFinite(p)) continue;
    normalized.push({ rep, p });
    total += p;
  }
  if (!(total > 0)) return null;
  return normalized.map((bucket) => ({ rep: bucket.rep, p: bucket.p / total }));
};

const buildTailLookup = (buckets) => {
  const items = safeArray(buckets)
    .map((b) => ({ rep: toNumber(b?.rep), p: toNumber(b?.p) }))
    .filter((b) => Number.isFinite(b.rep) && Number.isFinite(b.p))
    .sort((a, b) => a.rep - b.rep);
  if (!items.length) return null;
  const reps = items.map((b) => b.rep);
  const tails = new Array(items.length);
  let running = 0;
  for (let i = items.length - 1; i >= 0; i -= 1) {
    running += items[i].p;
    tails[i] = running;
  }
  return { reps, tails };
};

const tailAt = (lookup, x) => {
  const reps = lookup?.reps;
  const tails = lookup?.tails;
  const target = toNumber(x);
  if (!Array.isArray(reps) || !Array.isArray(tails) || !reps.length || !Number.isFinite(target)) return null;
  let lo = 0;
  let hi = reps.length - 1;
  let idx = reps.length;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (reps[mid] >= target) {
      idx = mid;
      hi = mid - 1;
    } else {
      lo = mid + 1;
    }
  }
  if (idx >= reps.length) return 0;
  return tails[idx];
};

const buildSnapshotAtTimeNearest = (markets, targetTs, candleKey = "longterm_candles") => {
  const snapshots = [];
  for (const market of safeArray(markets)) {
    const strike = market?.strike || null;
    const rep = toNumber(strike?.value);
    const kind = strike?.kind ? String(strike.kind) : null;
    const label = getMarketStrikeLabel(market);
    const candles = safeArray(market?.[candleKey]);
    const point = pickValueNearestAvailable(candles, targetTs);
    const yes = point ? toNumber(point.yes) : null;
    if (!Number.isFinite(rep) || !Number.isFinite(yes)) continue;
    snapshots.push({ label, kind, rep, p: yes / 100 });
  }
  return snapshots;
};

const buildBucketsAtTime = (markets, targetTs) => {
  const snapshots = buildSnapshotAtTimeNearest(markets, targetTs, "longterm_candles");
  const buckets = buildBucketsFromSnapshots(snapshots);
  return normalizeBuckets(buckets);
};

const computeArbGapFromBuckets = (kalshiBuckets, polymarketBuckets) => {
  const kalshiLookup = buildTailLookup(kalshiBuckets);
  const polymarketLookup = buildTailLookup(polymarketBuckets);
  if (!kalshiLookup || !polymarketLookup) return null;
  const grid = Array.from(new Set([...kalshiLookup.reps, ...polymarketLookup.reps])).sort((a, b) => a - b);
  if (!grid.length) return null;
  let maxGap = 0;
  for (const value of grid) {
    const kTail = tailAt(kalshiLookup, value);
    const pTail = tailAt(polymarketLookup, value);
    if (!Number.isFinite(kTail) || !Number.isFinite(pTail)) continue;
    const gap = Math.abs(kTail - pTail);
    if (gap > maxGap) maxGap = gap;
  }
  return maxGap;
};

const buildMispricingForKalshi = (markets, otherTail, ts) => {
  let best = null;
  for (const market of safeArray(markets)) {
    const strike = market?.strike || null;
    if (String(strike?.kind || "").toLowerCase() !== "above") continue;
    const threshold = toNumber(strike?.value);
    if (!Number.isFinite(threshold)) continue;
    const point = pickValueNearestAvailable(safeArray(market?.longterm_candles), ts);
    const actual = point ? toNumber(point.yes) / 100 : null;
    const implied = tailAt(otherTail, threshold);
    if (!Number.isFinite(actual) || !Number.isFinite(implied)) continue;
    const diff = actual - implied;
    const gap = Math.abs(diff);
    if (!best || gap > best.gap) {
      best = {
        venue: "Kalshi",
        otherVenue: "Polymarket",
        label: getMarketStrikeLabel(market),
        diff,
        gap
      };
    }
  }
  return best;
};

const buildMispricingForPolymarket = (markets, otherTail, ts) => {
  let best = null;
  for (const market of safeArray(markets)) {
    const strike = market?.strike || null;
    if (String(strike?.kind || "").toLowerCase() !== "range") continue;
    const lower = toNumber(strike?.lower);
    const upper = toNumber(strike?.upper);
    if (!Number.isFinite(lower) || !Number.isFinite(upper)) continue;
    const point = pickValueNearestAvailable(safeArray(market?.longterm_candles), ts);
    const actual = point ? toNumber(point.yes) / 100 : null;
    const impliedLower = tailAt(otherTail, lower);
    const impliedUpper = tailAt(otherTail, upper);
    if (!Number.isFinite(actual) || !Number.isFinite(impliedLower) || !Number.isFinite(impliedUpper)) continue;
    const implied = impliedLower - impliedUpper;
    if (!Number.isFinite(implied)) continue;
    const diff = actual - implied;
    const gap = Math.abs(diff);
    if (!best || gap > best.gap) {
      best = {
        venue: "Polymarket",
        otherVenue: "Kalshi",
        label: getMarketStrikeLabel(market),
        diff,
        gap
      };
    }
  }
  return best;
};

const buildMaxMispricingAtTime = (kalshiMarkets, polymarketMarkets, ts) => {
  const kalshiBuckets = buildBucketsAtTime(kalshiMarkets, ts);
  const polymarketBuckets = buildBucketsAtTime(polymarketMarkets, ts);
  if (!kalshiBuckets || !polymarketBuckets) return null;
  const kalshiTail = buildTailLookup(kalshiBuckets);
  const polymarketTail = buildTailLookup(polymarketBuckets);
  if (!kalshiTail || !polymarketTail) return null;
  const kalshiBest = buildMispricingForKalshi(kalshiMarkets, polymarketTail, ts);
  const polymarketBest = buildMispricingForPolymarket(polymarketMarkets, kalshiTail, ts);
  const bestCandidates = [kalshiBest, polymarketBest].filter(Boolean);
  if (!bestCandidates.length) return null;
  bestCandidates.sort((a, b) => b.gap - a.gap);
  const best = bestCandidates[0];
  return {
    venue: best.venue,
    otherVenue: best.otherVenue,
    label: best.label,
    gap_pp: best.gap * 100,
    diff_pp: best.diff * 100,
    direction: best.diff > 0 ? "rich" : best.diff < 0 ? "cheap" : "flat"
  };
};

const computeLongtermArbSeries = (kalshiEvent, polymarketEvent) => {
  const kalshiMarkets = safeArray(kalshiEvent?.markets);
  const polymarketMarkets = safeArray(polymarketEvent?.markets);
  const kTimeline = buildLongtermTimeline(kalshiMarkets);
  const pTimeline = buildLongtermTimeline(polymarketMarkets);
  const missingProviders = [];
  if (!kTimeline.length) missingProviders.push("Kalshi");
  if (!pTimeline.length) missingProviders.push("Polymarket");
  if (missingProviders.length) {
    return { series: [], hasCandles: false, missingProviders };
  }
  const pSet = new Set(pTimeline);
  const common = kTimeline.filter((ts) => pSet.has(ts));
  if (!common.length) return { series: [], hasCandles: true };
  const baseTs = toNumber(kalshiEvent?.release_ts);
  const fallbackBase = toNumber(polymarketEvent?.release_ts);
  const releaseTs = Number.isFinite(baseTs) ? baseTs : fallbackBase;
  if (!Number.isFinite(releaseTs)) return { series: [], hasCandles: false };
  const series = [];
  for (const ts of common) {
    const detail = buildMaxMispricingAtTime(kalshiMarkets, polymarketMarkets, ts);
    if (!detail || !Number.isFinite(detail.gap_pp)) continue;
    series.push({ x: (ts - releaseTs) / 86400, y: detail.gap_pp, detail });
  }
  series.sort((a, b) => a.x - b.x);
  return { series, hasCandles: true, releaseTs };
};

const pickMinPositive = (...values) => {
  const valid = values.filter((v) => Number.isFinite(v) && v > 0);
  if (!valid.length) return null;
  return Math.min(...valid);
};

const setTrendVisibility = (show, message = "") => {
  if (elements.trendWrap) {
    elements.trendWrap.style.display = show ? "block" : "none";
  }
  if (elements.trendEmpty) {
    elements.trendEmpty.textContent = message || "";
    elements.trendEmpty.style.display = message ? "block" : "none";
  }
};

const setTrendVolumeVisibility = (show, message = "") => {
  if (elements.trendVolumeWrap) {
    elements.trendVolumeWrap.style.display = show ? "block" : "none";
  }
  if (elements.trendVolumeTitle) {
    elements.trendVolumeTitle.style.display = show ? "block" : "none";
  }
  if (elements.trendVolumeEmpty) {
    elements.trendVolumeEmpty.textContent = message || "";
    elements.trendVolumeEmpty.style.display = message ? "block" : "none";
  }
};

const setArbVisibility = (show, message = "") => {
  if (elements.arbWrap) {
    elements.arbWrap.style.display = show ? "block" : "none";
  }
  if (elements.arbEmpty) {
    elements.arbEmpty.textContent = message || "";
    elements.arbEmpty.style.display = message ? "block" : "none";
  }
};

const renderLongtermChart = (series, minX, maxX, releaseTs, resolveOffset) => {
  if (!elements.trendCanvas || typeof Chart === "undefined") return;
  const ctx = elements.trendCanvas.getContext("2d");
  const yValues = safeArray(series)
    .map((point) => toNumber(point?.y))
    .filter((value) => Number.isFinite(value));
  const yMin = yValues.length ? Math.min(...yValues) : -100;
  const yMax = yValues.length ? Math.max(...yValues) : 100;
  const pad = yValues.length ? Math.max(5, (yMax - yMin) * 0.08) : 10;
  const releaseLine = [
    { x: 0, y: yMin - pad },
    { x: 0, y: yMax + pad }
  ];
  const resolveLine =
    Number.isFinite(toNumber(resolveOffset)) && toNumber(resolveOffset) > 0
      ? [
          { x: toNumber(resolveOffset), y: yMin - pad },
          { x: toNumber(resolveOffset), y: yMax + pad }
        ]
      : null;
  const config = {
    type: "line",
    data: {
      datasets: [
        {
          label: "Expected",
          data: safeArray(series),
          borderColor: "#0f766e",
          backgroundColor: "transparent",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.2,
          spanGaps: true
        },
        {
          label: "Release",
          data: releaseLine,
          borderColor: "rgba(15, 118, 110, 0.35)",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0,
          borderDash: [6, 4]
        },
        ...(resolveLine
          ? [
              {
                label: "Resolved",
                data: resolveLine,
                borderColor: "#ef4444",
                borderWidth: 2,
                pointRadius: 0,
                tension: 0,
                borderDash: [4, 4]
              }
            ]
          : [])
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          filter: (item) => item?.dataset?.label === "Expected",
          callbacks: {
            title: (items) => {
              const point = items?.[0];
              const offset = toNumber(point?.parsed?.x);
              if (!Number.isFinite(offset) || !Number.isFinite(toNumber(releaseTs))) return "";
              return formatDateTimeShort(toNumber(releaseTs) + offset * 86400);
            },
            label: (ctx) => {
              const value = toNumber(ctx?.parsed?.y);
              if (!Number.isFinite(value)) return "";
              return `Expected: ${value.toFixed(1)}k`;
            }
          }
        }
      },
      interaction: { mode: "nearest", intersect: false },
      scales: {
        x: {
          type: "linear",
          min: Math.floor(minX),
          max: Math.ceil(maxX),
          title: { display: true, text: "Date" },
          ticks: {
            autoSkip: true,
            maxTicksLimit: 6,
            maxRotation: 0,
            minRotation: 0,
            callback: (value) => {
              const offset = toNumber(value);
              if (!Number.isFinite(offset) || !Number.isFinite(toNumber(releaseTs))) return "";
              return formatDateShort(toNumber(releaseTs) + offset * 86400);
            }
          }
        },
        y: {
          min: yMin - pad,
          max: yMax + pad,
          title: { display: true, text: "Expected payroll change (k)" },
          ticks: { callback: (value) => `${Number(value).toFixed(0)}k` }
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

const renderLongtermVolumeChart = (series, minX, maxX, releaseTs, resolveOffset) => {
  if (!elements.trendVolumeCanvas || typeof Chart === "undefined") return;
  const ctx = elements.trendVolumeCanvas.getContext("2d");
  const yValues = safeArray(series)
    .map((point) => toNumber(point?.y))
    .filter((value) => Number.isFinite(value));
  const yMin = 0;
  const yMax = yValues.length ? Math.max(...yValues) : 1;
  const releaseLine = [
    { x: 0, y: yMin },
    { x: 0, y: yMax * 1.05 }
  ];
  const resolveLine =
    Number.isFinite(toNumber(resolveOffset)) && toNumber(resolveOffset) > 0
      ? [
          { x: toNumber(resolveOffset), y: yMin },
          { x: toNumber(resolveOffset), y: yMax * 1.05 }
        ]
      : null;
  const config = {
    type: "line",
    data: {
      datasets: [
        {
          label: "Volume",
          data: safeArray(series),
          borderColor: "#1d4ed8",
          backgroundColor: "rgba(29, 78, 216, 0.18)",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.2,
          spanGaps: true,
          fill: true
        },
        {
          label: "Release",
          data: releaseLine,
          borderColor: "rgba(37, 99, 235, 0.6)",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0,
          borderDash: [6, 4],
          fill: false
        },
        ...(resolveLine
          ? [
              {
                label: "Resolved",
                data: resolveLine,
                borderColor: "#ef4444",
                borderWidth: 2,
                pointRadius: 0,
                tension: 0,
                borderDash: [4, 4],
                fill: false
              }
            ]
          : [])
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          filter: (item) => item?.dataset?.label === "Volume",
          callbacks: {
            title: (items) => {
              const point = items?.[0];
              const offset = toNumber(point?.parsed?.x);
              if (!Number.isFinite(offset) || !Number.isFinite(toNumber(releaseTs))) return "";
              return formatDateTimeShort(toNumber(releaseTs) + offset * 86400);
            },
            label: (ctx) => {
              const value = toNumber(ctx?.parsed?.y);
              if (!Number.isFinite(value)) return "";
              return `Rolling $ volume: $${formatInt(value)}`;
            }
          }
        }
      },
      interaction: { mode: "nearest", intersect: false },
      scales: {
        x: {
          type: "linear",
          min: Math.floor(minX),
          max: Math.ceil(maxX),
          title: { display: true, text: "Date" },
          ticks: {
            autoSkip: true,
            maxTicksLimit: 6,
            maxRotation: 0,
            minRotation: 0,
            callback: (value) => {
              const offset = toNumber(value);
              if (!Number.isFinite(offset) || !Number.isFinite(toNumber(releaseTs))) return "";
              return formatDateShort(toNumber(releaseTs) + offset * 86400);
            }
          }
        },
        y: {
          min: yMin,
          max: yMax * 1.05,
          title: { display: true, text: "Notional ($)" },
          ticks: {
            callback: (value) => {
              const label = formatInt(value);
              return label === "—" ? "—" : `$${label}`;
            }
          }
        }
      }
    }
  };
  if (!state.trendVolumeChart) {
    state.trendVolumeChart = new Chart(ctx, config);
  } else {
    state.trendVolumeChart.config.type = config.type;
    state.trendVolumeChart.data = config.data;
    state.trendVolumeChart.options = config.options;
    state.trendVolumeChart.update();
  }
};

const renderArbChart = (series, minX, maxX, releaseTs, resolveOffset) => {
  if (!elements.arbCanvas || typeof Chart === "undefined") return;
  const ctx = elements.arbCanvas.getContext("2d");
  const yValues = safeArray(series)
    .map((point) => toNumber(point?.y))
    .filter((value) => Number.isFinite(value));
  const yMax = yValues.length ? Math.max(...yValues) : 5;
  const pad = Math.max(1, yMax * 0.12);
  const highlight = safeArray(series).filter((point) => toNumber(point?.y) >= ARB_WINDOW_THRESHOLD_PP);
  const releaseLine = [
    { x: 0, y: 0 },
    { x: 0, y: yMax + pad }
  ];
  const resolveLine =
    Number.isFinite(toNumber(resolveOffset)) && toNumber(resolveOffset) > 0
      ? [
          { x: toNumber(resolveOffset), y: 0 },
          { x: toNumber(resolveOffset), y: yMax + pad }
        ]
      : null;
  const thresholdLine = [
    { x: minX, y: ARB_WINDOW_THRESHOLD_PP },
    { x: maxX, y: ARB_WINDOW_THRESHOLD_PP }
  ];
  const config = {
    type: "line",
    data: {
      datasets: [
        {
          label: "Max gap",
          data: safeArray(series),
          borderColor: "#2563eb",
          backgroundColor: "transparent",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.2,
          spanGaps: true
        },
        {
          label: "Arb window",
          data: highlight,
          borderColor: "transparent",
          backgroundColor: "#1d4ed8",
          pointRadius: 3.5,
          pointHoverRadius: 4,
          showLine: false,
          order: 0
        },
        {
          label: "Threshold",
          data: thresholdLine,
          borderColor: "rgba(37, 99, 235, 0.25)",
          borderWidth: 1,
          pointRadius: 0,
          tension: 0,
          borderDash: [4, 4],
          order: 1
        },
        {
          label: "Release",
          data: releaseLine,
          borderColor: "rgba(37, 99, 235, 0.35)",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0,
          borderDash: [6, 4]
        },
        ...(resolveLine
          ? [
              {
                label: "Resolved",
                data: resolveLine,
                borderColor: "#ef4444",
                borderWidth: 2,
                pointRadius: 0,
                tension: 0,
                borderDash: [4, 4]
              }
            ]
          : [])
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          filter: (item) => item?.dataset?.label === "Max gap",
          callbacks: {
            title: (items) => {
              const point = items?.[0];
              const offset = toNumber(point?.parsed?.x);
              if (!Number.isFinite(offset) || !Number.isFinite(toNumber(releaseTs))) return "";
              return formatDateTimeShort(toNumber(releaseTs) + offset * 86400);
            },
            label: (ctx) => {
              const value = toNumber(ctx?.parsed?.y);
              if (!Number.isFinite(value)) return "";
              const detail = ctx?.raw?.detail;
              if (detail?.venue && detail?.label) {
                const gapLabel = Number.isFinite(detail.gap_pp) ? detail.gap_pp.toFixed(2) : value.toFixed(2);
                return `${detail.venue} ${detail.label} ${detail.direction} by ${gapLabel}pp vs ${detail.otherVenue}`;
              }
              return `Max gap: ${value.toFixed(2)}pp`;
            }
          }
        }
      },
      interaction: { mode: "nearest", intersect: false },
      scales: {
        x: {
          type: "linear",
          min: Math.floor(minX),
          max: Math.ceil(maxX),
          title: { display: true, text: "Date" },
          ticks: {
            autoSkip: true,
            maxTicksLimit: 6,
            maxRotation: 0,
            minRotation: 0,
            callback: (value) => {
              const offset = toNumber(value);
              if (!Number.isFinite(offset) || !Number.isFinite(toNumber(releaseTs))) return "";
              return formatDateShort(toNumber(releaseTs) + offset * 86400);
            }
          }
        },
        y: {
          min: 0,
          max: yMax + pad,
          title: { display: true, text: "Max contract mispricing (pp)" },
          ticks: { callback: (value) => `${Number(value).toFixed(1)}pp` }
        }
      }
    }
  };
  if (!state.arbChart) {
    state.arbChart = new Chart(ctx, config);
  } else {
    state.arbChart.config.type = config.type;
    state.arbChart.data = config.data;
    state.arbChart.options = config.options;
    state.arbChart.update();
  }
};

const formatIsoShort = (iso) => {
  const parsed = iso ? new Date(iso) : null;
  if (!parsed || Number.isNaN(parsed.getTime())) return "—";
  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short"
  }).format(parsed);
};

const getMarketCloseOffsetDays = (markets, releaseTs) => {
  const base = toNumber(releaseTs);
  if (!Number.isFinite(base)) return null;
  const closes = safeArray(markets)
    .map((market) => toNumber(market?.close_ts))
    .filter((ts) => Number.isFinite(ts) && ts > base);
  if (!closes.length) return null;
  const minClose = Math.min(...closes);
  return (minClose - base) / 86400;
};

const setStatus = (message, isError = false) => {
  if (!elements.status) return;
  elements.status.textContent = message;
  elements.status.classList.toggle("error", Boolean(isError));
};

const getProviderKey = () => {
  const raw = elements.summaryProviderSelect?.value || state.provider || "kalshi";
  return raw === "polymarket" ? "polymarket" : "kalshi";
};

const getSummaryProviderKey = () => {
  const raw = elements.summaryProviderSelect?.value || "kalshi";
  if (raw === "kalshi" || raw === "polymarket") return raw;
  return "kalshi";
};

const getSummaryProviders = () => {
  return [getSummaryProviderKey()];
};

const getLongtermWindowDays = () => {
  const raw = elements.trendWindowSelect?.value;
  const parsed = toNumber(raw);
  if (Number.isFinite(parsed) && parsed > 0) {
    return Math.min(parsed, MAX_LONGTERM_WINDOW_DAYS);
  }
  return DEFAULT_LONGTERM_WINDOW_DAYS;
};

const getLongtermVolumeWindowMinutes = () => {
  const raw = String(elements.trendVolumeWindowSelect?.value || "").trim();
  if (!raw) return DEFAULT_LONGTERM_VOLUME_WINDOW_MINUTES;
  const match = raw.match(/^(\d+)([hd])$/);
  if (match) {
    const value = Number(match[1]);
    if (!Number.isFinite(value) || value <= 0) return DEFAULT_LONGTERM_VOLUME_WINDOW_MINUTES;
    return match[2] === "d" ? value * 1440 : value * 60;
  }
  const parsed = toNumber(raw);
  if (Number.isFinite(parsed) && parsed > 0) return parsed;
  return DEFAULT_LONGTERM_VOLUME_WINDOW_MINUTES;
};

const getVolumeWindowMinutes = () => {
  const raw = elements.volumeWindowSelect?.value;
  const parsed = toNumber(raw);
  if (Number.isFinite(parsed) && parsed > 0) return parsed;
  return 15;
};

const getSeriesMaxX = (series) => {
  const xs = safeArray(series)
    .map((point) => toNumber(point?.x))
    .filter((x) => Number.isFinite(x));
  return xs.length ? Math.max(...xs) : 0;
};

const filterSeriesByWindow = (series, windowDays, maxPost = 0) => {
  const limit = toNumber(windowDays);
  if (!Number.isFinite(limit) || limit <= 0) return safeArray(series);
  const maxX = Number.isFinite(maxPost) && maxPost > 0 ? maxPost : 0;
  return safeArray(series).filter((point) => {
    const x = toNumber(point?.x);
    return Number.isFinite(x) && x >= -limit && x <= maxX;
  });
};

const providerMetaForDataset = (dataset) => {
  if (!dataset) return null;
  return dataset.kalshi || dataset.polymarket || null;
};

const providerMeta = () => {
  if (!state.data) return null;
  return state.data.kalshi || state.data.polymarket || null;
};

const ensureDataset = async (providerKey) => {
  const key = providerKey === "polymarket" ? "polymarket" : "kalshi";
  if (state.datasets?.[key]) return state.datasets[key];
  const cfg = PROVIDER_CONFIG[key];
  if (!cfg) return null;
  const data = await fetchJSONLocal(cfg.path);
  const customEvents = loadCustomEvents();
  if (customEvents.length) {
    data.events = mergeCustomEvents(data.events, customEvents);
  }
  state.datasets[key] = data;
  return data;
};

const TOP_SECTION_ORDER = [
  "impactDistributionCard",
  "impactExpectationCard",
  "impactTrendCard",
  "impactYesCard",
  "impactSummaryCard"
];

const applyTopSection = (topId) => {
  if (!elements.sectionsWrap) return;
  const wanted = TOP_SECTION_ORDER.includes(topId) ? topId : null;
  const ordered = wanted ? [wanted, ...TOP_SECTION_ORDER.filter((id) => id !== wanted)] : TOP_SECTION_ORDER;
  for (const id of ordered) {
    const node = document.getElementById(id);
    if (node) elements.sectionsWrap.appendChild(node);
  }
};

const initImpactInfoPopover = () => initInfoPopover({ wrap: elements.infoWrap, btn: elements.infoBtn });

const safeArray = (value) => (Array.isArray(value) ? value : []);

const pad2 = (value) => String(value).padStart(2, "0");

const formatLocalDatetimeInput = (value) => {
  const date = value instanceof Date ? value : new Date(value);
  if (!date || Number.isNaN(date.getTime())) return "";
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}T${pad2(
    date.getHours()
  )}:${pad2(date.getMinutes())}`;
};

const parseYearMonth = (value) => {
  const match = String(value || "").trim().match(/^(\d{4})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) return null;
  return { year, month };
};

const formatYearMonth = (year, month) => `${year}-${pad2(month)}`;

const prevYearMonth = (value) => {
  const parsed = parseYearMonth(value);
  if (!parsed) return null;
  let year = parsed.year;
  let month = parsed.month - 1;
  if (month < 1) {
    month = 12;
    year -= 1;
  }
  return formatYearMonth(year, month);
};

const toCustomNumber = (value) => {
  if (value == null) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const parsed = Number(raw.replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeCustomEvent = (raw) => {
  if (!raw) return null;
  const label = String(raw.label || "").trim();
  const releaseIso = String(raw.release_iso || "").trim();
  if (!label || !releaseIso) return null;
  const releaseDate = new Date(releaseIso);
  if (Number.isNaN(releaseDate.getTime())) return null;
  const releaseTs = Math.floor(releaseDate.getTime() / 1000);
  const releaseMonth =
    String(raw.release_month || "").trim() ||
    formatYearMonth(releaseDate.getFullYear(), releaseDate.getMonth() + 1);
  const payrollMonth =
    String(raw.payroll_month || "").trim() || prevYearMonth(releaseMonth) || "";
  const actual = toCustomNumber(raw?.value?.actual ?? raw.actual);
  const expected = toCustomNumber(raw?.value?.expected ?? raw.expected);
  const actualMin = toCustomNumber(raw?.value?.actual_min ?? raw.actual_min);
  const actualMax = toCustomNumber(raw?.value?.actual_max ?? raw.actual_max);
  const value = {};
  if (Number.isFinite(actual)) value.actual = actual;
  if (Number.isFinite(expected)) value.expected = expected;
  if (Number.isFinite(actualMin)) value.actual_min = actualMin;
  if (Number.isFinite(actualMax)) value.actual_max = actualMax;
  if (Object.keys(value).length) value.unit = "jobs";
  return {
    id: String(raw.id || ""),
    type: "custom",
    label,
    release_month: releaseMonth,
    payroll_month: payrollMonth,
    release_ts: releaseTs,
    release_iso: releaseIso,
    value: Object.keys(value).length ? value : null,
    __custom_local: Boolean(raw.__custom_local)
  };
};

const loadCustomEvents = () => {
  try {
    const stored = localStorage.getItem(CUSTOM_EVENTS_KEY);
    if (!stored) return [];
    const parsed = JSON.parse(stored);
    if (!Array.isArray(parsed)) return [];
    return parsed.map(normalizeCustomEvent).filter(Boolean);
  } catch (err) {
    console.warn("Unable to read custom events.", err);
    return [];
  }
};

const saveCustomEvents = (events) => {
  const cleaned = safeArray(events)
    .map((event) => ({
      id: event.id,
      type: "custom",
      label: event.label,
      release_month: event.release_month,
      payroll_month: event.payroll_month,
      release_ts: event.release_ts,
      release_iso: event.release_iso,
      value: event.value,
      __custom_local: true
    }))
    .filter((event) => event.id && event.release_iso && event.label);
  localStorage.setItem(CUSTOM_EVENTS_KEY, JSON.stringify(cleaned));
};

const pickBaseEventForCustom = (events, releaseMonth) => {
  const match = safeArray(events).find(
    (event) =>
      event?.release_month === releaseMonth &&
      String(event?.type || "").toLowerCase() !== "custom" &&
      safeArray(event?.markets).length
  );
  if (match) return match;
  return (
    safeArray(events).find(
      (event) =>
        event?.release_month === releaseMonth && String(event?.type || "").toLowerCase() !== "custom"
    ) || null
  );
};

const mergeCustomEvents = (events, customEvents) => {
  const merged = [];
  const byId = new Map();
  for (const event of safeArray(events)) {
    if (!event?.id) continue;
    byId.set(String(event.id), event);
    merged.push(event);
  }
  for (const raw of safeArray(customEvents)) {
    const custom = normalizeCustomEvent(raw);
    if (!custom?.id || byId.has(String(custom.id))) continue;
    const base = pickBaseEventForCustom(events, custom.release_month);
    const enriched = {
      ...custom,
      markets: base?.markets ? base.markets : [],
      summary: base?.summary || null,
      jobs_report_ts: base?.jobs_report_ts,
      jobs_report_iso: base?.jobs_report_iso,
      __provider: base?.__provider
    };
    byId.set(String(custom.id), enriched);
    merged.push(enriched);
  }
  return merged;
};

const getEvents = () => safeArray(state.data?.events);
const getEventsForDataset = (dataset) => safeArray(dataset?.events);

const filterEventsBySource = (events, source) => {
  if (!source || source === "all") return events;
  const wanted = String(source).toLowerCase();
  return events.filter((event) => String(event.type || "").toLowerCase() === wanted);
};

const sortEventsNewestFirst = (events) =>
  events
    .slice()
    .sort((a, b) => (toNumber(b.release_ts) ?? 0) - (toNumber(a.release_ts) ?? 0));

const isAnnouncedType = (typeKey) => typeKey === "adp" || typeKey === "revelio" || typeKey === "custom";

const buildEventLabel = (event, { includeAnnounced = true } = {}) => {
  const label = event?.label ? String(event.label) : "";
  const type = String(event.type || "").toUpperCase() || "EVENT";
  const payroll = event.payroll_month ? `Payrolls: ${event.payroll_month}` : null;
  const when = event.release_iso ? formatIsoShort(event.release_iso) : null;
  const announced = getAnnouncedValue(event?.value);
  const announcedRange = getAnnouncedRange(event?.value);
  const typeKey = String(event?.type || "").toLowerCase();
  const announcedLabel =
    includeAnnounced && isAnnouncedType(typeKey) && (announcedRange || Number.isFinite(announced))
      ? `Announced: ${announcedRange ? formatJobsRangeCompact(announcedRange) : formatJobsCompact(announced)}`
      : null;
  return [label || type, payroll, when, announcedLabel].filter(Boolean).join(" · ");
};

const getEventById = (id) => getEvents().find((event) => String(event.id) === String(id)) || null;
const getEventByIdInDataset = (dataset, id) =>
  getEventsForDataset(dataset).find((event) => String(event.id) === String(id)) || null;

const getMarketStrikeLabel = (market) => {
  const strikeLabel = market?.strike?.label ? String(market.strike.label) : "";
  if (strikeLabel) return strikeLabel;
  return market?.ticker ? String(market.ticker) : "—";
};

const sortMarketsByStrike = (markets) =>
  markets.slice().sort((a, b) => {
    const av = toNumber(a?.strike?.value);
    const bv = toNumber(b?.strike?.value);
    if (Number.isFinite(av) && Number.isFinite(bv)) return av - bv;
    return getMarketStrikeLabel(a).localeCompare(getMarketStrikeLabel(b));
  });

const fillEventOptions = () => {
  if (!elements.eventSelect) return;
  const source = elements.sourceSelect?.value ?? "all";
  const current = elements.eventSelect.value;
  const filtered = sortEventsNewestFirst(filterEventsBySource(getEvents(), source));
  if (!filtered.length) {
    elements.eventSelect.innerHTML = '<option value="">No events loaded</option>';
    return;
  }
  const customEvents = filtered.filter((event) => String(event?.type || "").toLowerCase() === "custom");
  const standardEvents = filtered.filter((event) => String(event?.type || "").toLowerCase() !== "custom");
  const buildOptions = (events) => events.map((event) => `<option value="${event.id}">${buildEventLabel(event)}</option>`).join("");
  if (source === "all" && customEvents.length) {
    const standardBlock = standardEvents.length ? `<optgroup label="Releases">${buildOptions(standardEvents)}</optgroup>` : "";
    const customBlock = `<optgroup label="Custom">${buildOptions(customEvents)}</optgroup>`;
    elements.eventSelect.innerHTML = `${standardBlock}${customBlock}`;
  } else {
    elements.eventSelect.innerHTML = buildOptions(filtered);
  }
  const allIds = filtered.map((event) => String(event.id));
  if (current && allIds.includes(String(current))) {
    elements.eventSelect.value = current;
  } else {
    const preferred = standardEvents[0]?.id || filtered[0]?.id || "";
    if (preferred) elements.eventSelect.value = preferred;
  }
};

const setCustomPanelOpen = (open) => {
  if (!elements.customPanel || !elements.customToggle) return;
  elements.customPanel.hidden = !open;
  elements.customToggle.classList.toggle("active", open);
  elements.customToggle.setAttribute("aria-expanded", String(open));
};

const clearCustomForm = () => {
  if (elements.customLabel) elements.customLabel.value = "";
  if (elements.customRelease) elements.customRelease.value = "";
  if (elements.customPayroll) elements.customPayroll.value = "";
  if (elements.customActual) elements.customActual.value = "";
  if (elements.customMin) elements.customMin.value = "";
  if (elements.customMax) elements.customMax.value = "";
};

const seedCustomForm = () => {
  if (
    !elements.customLabel ||
    !elements.customRelease ||
    !elements.customPayroll ||
    !elements.customMin ||
    !elements.customMax
  ) {
    return;
  }
  const hasValue = [
    elements.customLabel,
    elements.customRelease,
    elements.customPayroll,
    elements.customActual,
    elements.customMin,
    elements.customMax
  ]
    .filter(Boolean)
    .some((input) => String(input.value || "").trim().length);
  if (hasValue) return;
  elements.customLabel.value = DEFAULT_CUSTOM_FORM.label;
  elements.customRelease.value = formatLocalDatetimeInput(DEFAULT_CUSTOM_FORM.release_iso);
  elements.customPayroll.value = DEFAULT_CUSTOM_FORM.payroll_month;
  elements.customMin.value = String(DEFAULT_CUSTOM_FORM.actual_min);
  elements.customMax.value = String(DEFAULT_CUSTOM_FORM.actual_max);
};

const buildCustomEventId = (label, releaseMonth) => {
  const slug = String(label || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");
  const suffix = `${releaseMonth || "event"}-${slug || "custom"}-${Date.now()}`;
  return `custom-${suffix}`;
};

const buildCustomEventFromForm = () => {
  const label = String(elements.customLabel?.value || "").trim();
  if (!label) return { error: "Add a label for the custom event." };
  const releaseRaw = String(elements.customRelease?.value || "").trim();
  if (!releaseRaw) return { error: "Add a release time for the custom event." };
  const releaseDate = new Date(releaseRaw);
  if (Number.isNaN(releaseDate.getTime())) return { error: "Release time is invalid." };
  const releaseMonth = formatYearMonth(releaseDate.getFullYear(), releaseDate.getMonth() + 1);
  const releaseIso = releaseDate.toISOString();
  const releaseTs = Math.floor(releaseDate.getTime() / 1000);
  const payrollInput = String(elements.customPayroll?.value || "").trim();
  const payrollMonth = payrollInput ? (parseYearMonth(payrollInput) ? payrollInput : null) : prevYearMonth(releaseMonth);
  if (!payrollMonth) return { error: "Payroll month must be YYYY-MM." };

  const minValue = toCustomNumber(elements.customMin?.value);
  const maxValue = toCustomNumber(elements.customMax?.value);
  const actualValue = toCustomNumber(elements.customActual?.value);
  if (Number.isFinite(minValue) && Number.isFinite(maxValue) && minValue > maxValue) {
    return { error: "Range min must be less than or equal to range max." };
  }

  const value = {};
  if (Number.isFinite(minValue) || Number.isFinite(maxValue)) {
    if (Number.isFinite(minValue)) value.actual_min = minValue;
    if (Number.isFinite(maxValue)) value.actual_max = maxValue;
  } else if (Number.isFinite(actualValue)) {
    value.actual = actualValue;
  }
  if (Object.keys(value).length) value.unit = "jobs";

  return {
    event: {
      id: buildCustomEventId(label, releaseMonth),
      type: "custom",
      label,
      release_month: releaseMonth,
      payroll_month: payrollMonth,
      release_ts: releaseTs,
      release_iso: releaseIso,
      value: Object.keys(value).length ? value : null,
      __custom_local: true
    }
  };
};

const addCustomEvent = () => {
  const result = buildCustomEventFromForm();
  if (result?.error) {
    setStatus(result.error, true);
    return null;
  }
  const custom = result.event;
  const stored = loadCustomEvents();
  stored.push(custom);
  saveCustomEvents(stored);
  if (state.data) {
    state.data.events = mergeCustomEvents(state.data.events, stored);
  }
  if (elements.sourceSelect && elements.sourceSelect.value !== "all") {
    elements.sourceSelect.value = "all";
  }
  fillEventOptions();
  if (elements.eventSelect) elements.eventSelect.value = custom.id;
  clearCustomForm();
  setStatus(`Added custom event: ${custom.label}.`);
  return custom;
};

const fillStrikeOptions = (event) => {
  if (!elements.strikeSelect) return;
  const current = elements.strikeSelect.value;
  const markets = sortMarketsByStrike(safeArray(event?.markets));
  if (!markets.length) {
    elements.strikeSelect.innerHTML = '<option value="">No strikes</option>';
    return;
  }
  const options = [
    ...markets.map((market) => ({ value: market.ticker, label: getMarketStrikeLabel(market) }))
  ];
  elements.strikeSelect.innerHTML = options
    .map((opt) => `<option value="${String(opt.value)}">${String(opt.label)}</option>`)
    .join("");
  const preferred =
    (current && options.some((o) => String(o.value) === String(current)) && current) ||
    markets.find((m) => {
      const strike = m?.strike || null;
      return String(strike?.kind || "").toLowerCase() === "above" && toNumber(strike?.value) === 0;
    })?.ticker ||
    markets.find((m) => toNumber(m?.strike?.value) === 0)?.ticker ||
    markets.find((m) => String(m?.strike?.value) === "100000")?.ticker ||
    markets[0]?.ticker ||
    "";
  if (preferred) elements.strikeSelect.value = preferred;
};

const pickBaseline = (candles, releaseTs) => {
  if (!Array.isArray(candles) || !candles.length) return null;
  const ts0 = toNumber(releaseTs);
  if (!Number.isFinite(ts0)) return null;
  let candidate = null;
  for (const candle of candles) {
    const ts = toNumber(candle?.[0]);
    const yes = toNumber(candle?.[1]);
    if (!Number.isFinite(ts) || !Number.isFinite(yes)) continue;
    if (ts <= ts0) candidate = { ts, yes };
    else break;
  }
  if (candidate) return candidate;
  const first = candles.find((c) => Number.isFinite(toNumber(c?.[0])) && Number.isFinite(toNumber(c?.[1])));
  return first ? { ts: toNumber(first[0]), yes: toNumber(first[1]) } : null;
};

const pickValueAtOrAfter = (candles, targetTs) => {
  if (!Array.isArray(candles) || !candles.length) return null;
  const target = toNumber(targetTs);
  if (!Number.isFinite(target)) return null;
  for (const candle of candles) {
    const ts = toNumber(candle?.[0]);
    const yes = toNumber(candle?.[1]);
    if (!Number.isFinite(ts) || !Number.isFinite(yes)) continue;
    if (ts >= target) return { ts, yes };
  }
  return null;
};

const buildMinuteGrid = (startTs, endTs, stepSec = 60) => {
  const start = toNumber(startTs);
  const end = toNumber(endTs);
  const step = toNumber(stepSec) ?? 60;
  if (!Number.isFinite(start) || !Number.isFinite(end) || !Number.isFinite(step) || step <= 0) return [];
  if (end < start) return [];
  const out = [];
  for (let t = Math.floor(start); t <= Math.floor(end); t += step) out.push(t);
  return out;
};

const computeAggregateAlignedSeries = (event, markets, ts0, preMinutes, postMinutes, scaleJobs = 50000) => {
  const announced = getAnnouncedValue(event?.value);
  if (!Number.isFinite(announced)) return null;
  if (!Number.isFinite(ts0)) return null;

  const startTs = ts0 - (toNumber(preMinutes) ?? 30) * 60;
  const endTs = ts0 + (toNumber(postMinutes) ?? 240) * 60;
  const grid = buildMinuteGrid(startTs, endTs, 60);

  const prepared = [];
  for (const market of safeArray(markets)) {
    const strike = toNumber(market?.strike?.value);
    if (!Number.isFinite(strike)) continue;
    const sign = announced > strike ? 1 : announced < strike ? -1 : 0;
    if (!sign) continue;
    const w = expWeight(strike - announced, scaleJobs);
    if (!Number.isFinite(w) || w <= 0) continue;
    const candles = safeArray(market?.candles);
    const base = pickBaseline(candles, ts0);
    if (!base || !Number.isFinite(base.yes)) continue;
    prepared.push({ strike, sign, w, baseYes: base.yes, candles, market });
  }
  if (!prepared.length) return null;

  const series = grid.map((t) => {
    let weighted = 0;
    let denom = 0;
    for (const item of prepared) {
      const p = pickValueNearestAvailable(item.candles, t);
      if (!p || !Number.isFinite(p.yes)) continue;
      const delta = p.yes - item.baseYes;
      weighted += item.w * item.sign * delta;
      denom += item.w;
    }
    return [t, denom ? weighted / denom : null, null];
  });

  return { announced, scaleJobs, series, prepared };
};

const sumVolumeInRange = (candles, startTs, endTs) => {
  if (!Array.isArray(candles) || !candles.length) return null;
  const start = toNumber(startTs);
  const end = toNumber(endTs);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  let total = 0;
  let seen = false;
  for (const candle of candles) {
    const ts = toNumber(candle?.[0]);
    const vol = toNumber(candle?.[2]);
    if (!Number.isFinite(ts) || !Number.isFinite(vol)) continue;
    if (ts < start) continue;
    if (ts >= end) break;
    total += vol;
    seen = true;
  }
  return seen ? total : null;
};

const renderMetrics = (items) => {
  if (!elements.metrics) return;
  elements.metrics.innerHTML = items
    .map(({ label, value }) => `<div><p class="detail-label">${label}</p><p>${value}</p></div>`)
    .join("");
};

const renderSummaryMetrics = (items) => {
  if (!elements.summaryMetrics) return;
  elements.summaryMetrics.innerHTML = items
    .map(({ label, value }) => `<div><p class="detail-label">${label}</p><p>${value}</p></div>`)
    .join("");
};

const renderArbMetrics = (stats) => {
  if (!elements.arbMetrics) return;
  const items = [
    { label: "Max gap", value: Number.isFinite(stats?.max) ? formatPct(stats.max) : "—" },
    { label: "Avg gap", value: Number.isFinite(stats?.avg) ? formatPct(stats.avg) : "—" },
    { label: "Last gap", value: Number.isFinite(stats?.last) ? formatPct(stats.last) : "—" }
  ];
  elements.arbMetrics.innerHTML = items
    .map(({ label, value }) => `<div><p class="detail-label">${label}</p><p>${value}</p></div>`)
    .join("");
};

const renderArbDetail = (detail) => {
  if (!elements.arbDetail) return;
  if (!detail) {
    elements.arbDetail.textContent = "";
    return;
  }
  const gapLabel = Number.isFinite(detail.gap_pp) ? formatPct(detail.gap_pp) : "—";
  const direction = detail.direction === "rich" ? "rich" : detail.direction === "cheap" ? "cheap" : "flat";
  let tradeHint = "";
  if (direction === "rich") {
    tradeHint = ` Sell ${detail.venue}, buy ${detail.otherVenue}.`;
  } else if (direction === "cheap") {
    tradeHint = ` Buy ${detail.venue}, sell ${detail.otherVenue}.`;
  }
  elements.arbDetail.textContent = `${detail.venue} ${detail.label} ${direction} by ${gapLabel} vs ${detail.otherVenue}.${tradeHint}`;
};

const renderSummaryChart = (labels, values) => {
  if (!elements.summaryChart || typeof Chart === "undefined") return;
  const ctx = elements.summaryChart.getContext("2d");
  const colors = ["#111827", "#7c3aed"];
  const config = {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Avg |Δ Yes|",
          data: values,
          backgroundColor: colors,
          borderRadius: 8,
          borderWidth: 0
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
            label: (item) => {
              const v = toNumber(item?.parsed?.y);
              return Number.isFinite(v) ? `Avg |Δ Yes|: ${v.toFixed(2)}pp` : "—";
            }
          }
        }
      },
      scales: {
        y: {
          title: { display: true, text: "Avg |Δ Yes| (pp)" },
          ticks: { callback: (value) => `${Number(value).toFixed(1)}pp` }
        }
      }
    }
  };
  if (!state.summaryChart) {
    state.summaryChart = new Chart(ctx, config);
  } else {
    state.summaryChart.config.type = config.type;
    state.summaryChart.data = config.data;
    state.summaryChart.options = config.options;
    state.summaryChart.update();
  }
};

const renderSummaryVolumeChart = (labels, values) => {
  if (!elements.summaryVolumeChart || typeof Chart === "undefined") return;
  const ctx = elements.summaryVolumeChart.getContext("2d");
  const colors = ["#0f172a", "#6d28d9"];
  const config = {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Vol rate (post/pre)",
          data: values,
          backgroundColor: colors,
          borderRadius: 8,
          borderWidth: 0
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
            label: (item) => {
              const v = toNumber(item?.parsed?.y);
              return Number.isFinite(v) ? `Vol rate: ${v.toFixed(2)}×` : "—";
            }
          }
        }
      },
      scales: {
        y: {
          title: { display: true, text: "Vol rate (post/pre)" },
          ticks: { callback: (value) => `${Number(value).toFixed(1)}×` }
        }
      }
    }
  };
  if (!state.summaryVolumeChart) {
    state.summaryVolumeChart = new Chart(ctx, config);
  } else {
    state.summaryVolumeChart.config.type = config.type;
    state.summaryVolumeChart.data = config.data;
    state.summaryVolumeChart.options = config.options;
    state.summaryVolumeChart.update();
  }
};

const buildChartData = (candles, releaseTs) => {
  const ts0 = toNumber(releaseTs);
  if (!Number.isFinite(ts0)) return { points: [], minX: -30, maxX: 240 };
  const points = [];
  for (const candle of safeArray(candles)) {
    const ts = toNumber(candle?.[0]);
    const yes = toNumber(candle?.[1]);
    if (!Number.isFinite(ts) || !Number.isFinite(yes)) continue;
    points.push({ x: (ts - ts0) / 60, y: yes });
  }
  points.sort((a, b) => a.x - b.x);
  const xs = points.map((p) => p.x);
  const meta = providerMeta();
  const preMinutes = toNumber(meta?.window?.pre_minutes) ?? 30;
  const postMinutes = toNumber(meta?.window?.post_minutes) ?? 240;
  const minX = xs.length ? Math.min(...xs) : -preMinutes;
  const maxX = xs.length ? Math.max(...xs) : postMinutes;
  return { points, minX, maxX };
};

const renderLineChart = (label, candles, releaseTs) => {
  if (!elements.canvas || typeof Chart === "undefined") return;
  const ctx = elements.canvas.getContext("2d");
  const { points, minX, maxX } = buildChartData(candles, releaseTs);
  const config = {
    type: "line",
    data: {
      datasets: [
        {
          label,
          data: points,
          borderColor: "#2563eb",
          backgroundColor: "transparent",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.2,
          spanGaps: true
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: "bottom" } },
      interaction: { mode: "nearest", intersect: false },
      scales: {
        x: {
          type: "linear",
          min: Math.floor(minX),
          max: Math.ceil(maxX),
          title: { display: true, text: "Minutes from release" },
          ticks: { callback: (value) => `${value}m` }
        },
        y: {
          title: { display: true, text: "Yes price" },
          ticks: { callback: (value) => `${Number(value).toFixed(0)}%` }
        }
      }
    }
  };
  if (!state.chart) {
    state.chart = new Chart(ctx, config);
  } else {
    state.chart.config.type = config.type;
    state.chart.data = config.data;
    state.chart.options = config.options;
    state.chart.update();
  }
};

const renderAggregateChart = (label, candles, releaseTs) => {
  if (!elements.canvas || typeof Chart === "undefined") return;
  const ctx = elements.canvas.getContext("2d");
  const { points, minX, maxX } = buildChartData(candles, releaseTs);
  const config = {
    type: "line",
    data: {
      datasets: [
        {
          label,
          data: points,
          borderColor: "#0ea5e9",
          backgroundColor: "transparent",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.2,
          spanGaps: true
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: "bottom" } },
      interaction: { mode: "nearest", intersect: false },
      scales: {
        x: {
          type: "linear",
          min: Math.floor(minX),
          max: Math.ceil(maxX),
          title: { display: true, text: "Minutes from release" },
          ticks: { callback: (value) => `${value}m` }
        },
        y: {
          title: { display: true, text: "Announcement-aligned Δ Yes" },
          ticks: { callback: (value) => `${Number(value).toFixed(2)}pp` }
        }
      }
    }
  };
  if (!state.chart) {
    state.chart = new Chart(ctx, config);
  } else {
    state.chart.config.type = config.type;
    state.chart.data = config.data;
    state.chart.options = config.options;
    state.chart.update();
  }
};

const renderBarChart = (labels, values, datasetLabel = "Δ Yes (pp)") => {
  if (!elements.barCanvas || typeof Chart === "undefined") return;
  const ctx = elements.barCanvas.getContext("2d");
  const colors = values.map((v) => (Number.isFinite(v) && v < 0 ? "#dc2626" : "#16a34a"));
  const config = {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: datasetLabel,
          data: values,
          backgroundColor: colors,
          borderColor: colors,
          borderWidth: 1
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: {
          ticks: { callback: (v) => `${Number(v).toFixed(1)}pp` }
        }
      }
    }
  };
  if (!state.barChart) {
    state.barChart = new Chart(ctx, config);
  } else {
    state.barChart.config.type = config.type;
    state.barChart.data = config.data;
    state.barChart.options = config.options;
    state.barChart.update();
  }
};

const buildDensitySeries = (buckets, key) => {
  const points = safeArray(buckets)
    .map((b) => ({
      x: toNumber(b?.rep) != null ? toNumber(b.rep) / 1000 : null,
      y: Number.isFinite(toNumber(b?.[key])) ? toNumber(b[key]) * 100 : null
    }))
    .filter((p) => Number.isFinite(p.x) && Number.isFinite(p.y))
    .sort((a, b) => a.x - b.x);
  return points;
};

const renderDistributionDensityChart = ({ summariesByProvider, horizonLabel, announcedValue, announcedRange }) => {
  if (!elements.barCanvas || typeof Chart === "undefined") return;
  if (Chart?.register && !announcedBandRegistered) {
    Chart.register(ANNOUNCED_BAND_PLUGIN);
    announcedBandRegistered = true;
  }
  const ctx = elements.barCanvas.getContext("2d");
  const providers = Object.keys(summariesByProvider || {});
  const isMulti = providers.length > 1;
  const fillArea = !isMulti;
  const palette = {
    kalshi: "#111827",
    polymarket: "#7c3aed"
  };
  const gridColor = "rgba(148, 163, 184, 0.18)";

  const datasets = [];
  providers.forEach((provider) => {
    const summary = summariesByProvider?.[provider];
    if (!summary?.buckets?.length) return;
    const baseLabel = provider === "kalshi" ? "Kalshi" : "Polymarket";
    const color = palette[provider] || "#2563eb";
    const fillColor = fillArea ? hexToRgba(color, 0.12) : "transparent";
    const before = buildDensitySeries(summary.buckets, "p0");
    const after = buildDensitySeries(summary.buckets, "p1");
    if (before.length) {
      datasets.push({
        type: "line",
        label: isMulti ? `${baseLabel} before` : "Before",
        data: before,
        borderColor: color,
        backgroundColor: "transparent",
        borderWidth: 2,
        borderDash: [6, 4],
        pointRadius: 0,
        tension: 0.2,
        spanGaps: true,
        fill: false,
        order: 1
      });
    }
    if (after.length) {
      datasets.push({
        type: "line",
        label: isMulti ? `${baseLabel} after (${horizonLabel})` : `After (${horizonLabel})`,
        data: after,
        borderColor: color,
        backgroundColor: fillColor,
        borderWidth: 2,
        pointRadius: 0,
        tension: 0.2,
        spanGaps: true,
        fill: fillArea ? "origin" : false,
        order: 2
      });
    }
  });

  const maxY = datasets
    .flatMap((d) => safeArray(d.data).map((p) => toNumber(p?.y)))
    .filter((v) => Number.isFinite(v))
    .reduce((acc, v) => Math.max(acc, v), 0);

  const announcedX = Number.isFinite(announcedValue) ? announcedValue / 1000 : null;
  const hasRange =
    announcedRange && Number.isFinite(announcedRange.min) && Number.isFinite(announcedRange.max) && announcedRange.min !== announcedRange.max;
  if (!hasRange && Number.isFinite(announcedX) && maxY > 0) {
    datasets.push({
      type: "line",
      label: "Announced",
      data: [
        { x: announcedX, y: 0 },
        { x: announcedX, y: maxY * 1.05 }
      ],
      borderColor: "#f59e0b",
      borderWidth: 2,
      pointRadius: 0,
      tension: 0,
      spanGaps: true,
      borderDash: [4, 4],
      order: 1000
    });
  }

  const rangePayload = hasRange && maxY > 0 ? announcedRange : null;

  if (elements.densityLegend) {
    const legendColor = providers.length === 1 ? palette[providers[0]] || "#111827" : "#111827";
    elements.densityLegend.style.setProperty("--density-color", legendColor);
    if (elements.densityLegendBefore) {
      elements.densityLegendBefore.textContent = "Before";
    }
    if (elements.densityLegendAfter) {
      elements.densityLegendAfter.textContent = `After (${horizonLabel})`;
    }
    elements.densityLegend.style.display = providers.length === 1 ? "flex" : "none";
  }

  const config = {
    type: "line",
    data: {
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "nearest", intersect: false },
      plugins: {
        legend: { display: false },
        announcedRange: rangePayload
      },
      scales: {
        x: {
          type: "linear",
          title: { display: true, text: "Payroll change (k jobs)" },
          ticks: { callback: (v) => `${Number(v).toFixed(0)}k` },
          grid: { color: gridColor }
        },
        y: {
          min: 0,
          title: { display: true, text: "Probability" },
          ticks: { callback: (v) => `${Number(v).toFixed(1)}%` },
          grid: { color: gridColor }
        }
      }
    }
  };
  if (!state.barChart) {
    state.barChart = new Chart(ctx, config);
    state.barChart.$announcedRange = rangePayload;
  } else {
    state.barChart.config.type = config.type;
    state.barChart.data = config.data;
    state.barChart.options = config.options;
    state.barChart.$announcedRange = rangePayload;
    state.barChart.update();
  }
};

const horizonMinutes = (horizonKey) => {
  const match = String(horizonKey || "").match(/^(\d+)(m)$/);
  if (!match) return null;
  const minutes = Number(match[1]);
  return Number.isFinite(minutes) ? minutes : null;
};

const pickValueAtOrBefore = (candles, targetTs) => {
  if (!Array.isArray(candles) || !candles.length) return null;
  const target = toNumber(targetTs);
  if (!Number.isFinite(target)) return null;
  for (let i = candles.length - 1; i >= 0; i -= 1) {
    const candle = candles[i];
    const ts = toNumber(candle?.[0]);
    const yes = toNumber(candle?.[1]);
    if (!Number.isFinite(ts) || !Number.isFinite(yes)) continue;
    if (ts <= target) return { ts, yes };
  }
  return null;
};

const pickValueNearestAvailable = (candles, targetTs) => pickValueAtOrAfter(candles, targetTs) || pickValueAtOrBefore(candles, targetTs);

const pickValueAroundRelease = (candles, ts0, targetTs) => {
  const t = toNumber(targetTs);
  const base = toNumber(ts0);
  if (!Number.isFinite(t) || !Number.isFinite(base)) return null;
  if (t <= base) return pickValueAtOrBefore(candles, t);
  return pickValueNearestAvailable(candles, t);
};

const formatHorizonOptionLabel = (minutes) => {
  const mins = toNumber(minutes);
  if (!Number.isFinite(mins) || mins <= 0) return "+?";
  if (mins === 60) return "+1 hour";
  if (mins === 240) return "+4 hours";
  if (mins === 1440) return "+1 day";
  if (mins % 60 === 0 && mins >= 120) return `+${mins / 60} hours`;
  return `+${mins} min`;
};

const buildStrikeSnapshot = (markets, ts0, horizonTs) => {
  const out = [];
  for (const market of safeArray(markets)) {
    const strike = market?.strike || null;
    const rep = toNumber(strike?.value);
    const kind = strike?.kind ? String(strike.kind) : null;
    const label = getMarketStrikeLabel(market);
    const candles = safeArray(market?.candles);
    const base = pickBaseline(candles, ts0);
    const after = horizonTs != null ? pickValueNearestAvailable(candles, horizonTs) : null;
    const baseYes = base ? toNumber(base.yes) : null;
    const afterYes = after ? toNumber(after.yes) : null;
    out.push({
      label,
      kind,
      rep,
      baseYes,
      afterYes,
      strike,
      market
    });
  }
  return out;
};

const buildBracketDistribution = (snapshots) => {
  const buckets = [];
  for (const s of snapshots) {
    const rep = toNumber(s.rep);
    const b = toNumber(s.baseYes);
    const a = toNumber(s.afterYes);
    if (!Number.isFinite(rep) || !Number.isFinite(b) || !Number.isFinite(a)) continue;
    buckets.push({
      label: String(s.label || "—"),
      rep,
      p0: b / 100,
      p1: a / 100
    });
  }
  return buckets;
};

const buildCdfDerivedDistribution = (snapshots) => {
  const points = snapshots
    .map((s) => ({
      strike: toNumber(s?.strike?.value),
      label: String(s.label || "—"),
      p0: toNumber(s.baseYes),
      p1: toNumber(s.afterYes)
    }))
    .filter((p) => Number.isFinite(p.strike) && Number.isFinite(p.p0) && Number.isFinite(p.p1))
    .sort((a, b) => a.strike - b.strike);

  if (points.length < 2) return [];

  const gap = (i) => {
    if (i < 0 || i >= points.length - 1) return 50_000;
    const g = points[i + 1].strike - points[i].strike;
    return Number.isFinite(g) && g > 0 ? g : 50_000;
  };

  const clamp01 = (v) => Math.max(0, Math.min(1, v));

  const buckets = [];
  const leftStrike = points[0].strike;
  const leftRep = leftStrike - gap(0) / 2;
  buckets.push({
    label: `≤ ${formatJobsLevelCompact(leftStrike)}`,
    rep: leftRep,
    p0: clamp01(1 - points[0].p0 / 100),
    p1: clamp01(1 - points[0].p1 / 100)
  });

  for (let i = 0; i < points.length - 1; i += 1) {
    const lo = points[i].strike;
    const hi = points[i + 1].strike;
    const p0 = clamp01(points[i].p0 / 100 - points[i + 1].p0 / 100);
    const p1 = clamp01(points[i].p1 / 100 - points[i + 1].p1 / 100);
    buckets.push({
      label: `${formatJobsLevelCompact(lo)}–${formatJobsLevelCompact(hi)}`,
      rep: (lo + hi) / 2,
      p0,
      p1
    });
  }

  const rightStrike = points[points.length - 1].strike;
  const rightRep = rightStrike + gap(points.length - 2) / 2;
  buckets.push({
    label: `≥ ${formatJobsLevelCompact(rightStrike)}`,
    rep: rightRep,
    p0: clamp01(points[points.length - 1].p0 / 100),
    p1: clamp01(points[points.length - 1].p1 / 100)
  });

  return buckets;
};

const computeQuantileFromBuckets = (buckets, key, quantile) => {
  const rows = safeArray(buckets)
    .map((b) => ({ rep: toNumber(b?.rep), p: toNumber(b?.[key]) }))
    .filter((row) => Number.isFinite(row.rep) && Number.isFinite(row.p) && row.p >= 0)
    .sort((a, b) => a.rep - b.rep);
  if (!rows.length) return null;
  const total = rows.reduce((acc, row) => acc + row.p, 0);
  if (!(total > 0)) return null;
  const target = total * quantile;
  let running = 0;
  for (const row of rows) {
    running += row.p;
    if (running >= target) return row.rep;
  }
  return rows[rows.length - 1].rep;
};

const computeExpectationSummary = (event, markets, ts0, horizonTs) => {
  const snapshots = buildStrikeSnapshot(markets, ts0, horizonTs);
  const bracketLikes = snapshots.filter((s) => String(s.kind || "").toLowerCase() === "range");
  const aboveLikes = snapshots.filter((s) => String(s.kind || "").toLowerCase() === "above");

  const buckets =
    bracketLikes.length >= Math.max(3, Math.floor(snapshots.length * 0.6))
      ? buildBracketDistribution(bracketLikes)
      : buildCdfDerivedDistribution(aboveLikes.length ? aboveLikes : snapshots);

  if (!buckets.length) return null;

  const sum = (arr) => arr.reduce((acc, v) => acc + v, 0);
  const total0 = sum(buckets.map((b) => (Number.isFinite(b.p0) ? b.p0 : 0)));
  const total1 = sum(buckets.map((b) => (Number.isFinite(b.p1) ? b.p1 : 0)));
  if (!(total0 > 0) || !(total1 > 0)) return null;

  const exp0 = sum(buckets.map((b) => b.rep * (b.p0 / total0)));
  const exp1 = sum(buckets.map((b) => b.rep * (b.p1 / total1)));
  const repricing = exp1 - exp0;

  const top0 = buckets
    .map((b) => ({ label: b.label, p: b.p0 / total0 }))
    .sort((a, b) => b.p - a.p)[0];
  const top1 = buckets
    .map((b) => ({ label: b.label, p: b.p1 / total1 }))
    .sort((a, b) => b.p - a.p)[0];

  const top2sum = (ps) => ps.slice(0, 2).reduce((acc, p) => acc + p, 0);
  const conf0 = top2sum(buckets.map((b) => b.p0 / total0).sort((a, b) => b - a));
  const conf1 = top2sum(buckets.map((b) => b.p1 / total1).sort((a, b) => b - a));
  const median0 = computeQuantileFromBuckets(buckets, "p0", 0.5);
  const p25 = computeQuantileFromBuckets(buckets, "p0", 0.25);
  const p75 = computeQuantileFromBuckets(buckets, "p0", 0.75);

  return {
    exp0,
    exp1,
    repricing,
    top0,
    top1,
    conf0,
    conf1,
    median0,
    p25,
    p75,
    buckets
  };
};

const buildSnapshotAtTime = (markets, ts0, targetTs, candleKey = "candles") => {
  const snapshots = [];
  for (const market of safeArray(markets)) {
    const strike = market?.strike || null;
    const rep = toNumber(strike?.value);
    const kind = strike?.kind ? String(strike.kind) : null;
    const label = getMarketStrikeLabel(market);
    const candles = safeArray(market?.[candleKey]);
    const point = pickValueAroundRelease(candles, ts0, targetTs);
    const yes = point ? toNumber(point.yes) : null;
    if (!Number.isFinite(rep) || !Number.isFinite(yes)) continue;
    snapshots.push({ label, kind, rep, p: yes / 100 });
  }
  return snapshots;
};

const buildBucketsFromSnapshots = (snapshots) => {
  const bracketLikes = snapshots.filter((s) => String(s.kind || "").toLowerCase() === "range");
  const aboveLikes = snapshots.filter((s) => String(s.kind || "").toLowerCase() === "above");

  if (bracketLikes.length >= Math.max(3, Math.floor(snapshots.length * 0.6))) {
    return bracketLikes.map((s) => ({ label: s.label, rep: s.rep, p: s.p }));
  }

  const points = (aboveLikes.length ? aboveLikes : snapshots)
    .map((s) => ({ strike: s.rep, p: s.p }))
    .filter((p) => Number.isFinite(p.strike) && Number.isFinite(p.p))
    .sort((a, b) => a.strike - b.strike);
  if (points.length < 2) return [];
  const clamp01 = (v) => Math.max(0, Math.min(1, v));
  const gap = (i) => {
    if (i < 0 || i >= points.length - 1) return 50_000;
    const g = points[i + 1].strike - points[i].strike;
    return Number.isFinite(g) && g > 0 ? g : 50_000;
  };
  const out = [];
  const leftStrike = points[0].strike;
  out.push({ rep: leftStrike - gap(0) / 2, p: clamp01(1 - points[0].p) });
  for (let i = 0; i < points.length - 1; i += 1) {
    const lo = points[i].strike;
    const hi = points[i + 1].strike;
    out.push({ rep: (lo + hi) / 2, p: clamp01(points[i].p - points[i + 1].p) });
  }
  const rightStrike = points[points.length - 1].strike;
  out.push({ rep: rightStrike + gap(points.length - 2) / 2, p: clamp01(points[points.length - 1].p) });
  return out;
};

const computeExpectationStatsAtTime = (markets, ts0, targetTs, candleKey = "candles") => {
  const snapshots = buildSnapshotAtTime(markets, ts0, targetTs, candleKey);
  const buckets = buildBucketsFromSnapshots(snapshots);
  if (!buckets.length) return null;
  const total = buckets.reduce((acc, b) => acc + (Number.isFinite(b.p) ? b.p : 0), 0);
  if (!(total > 0)) return null;
  const expected = buckets.reduce((acc, b) => acc + b.rep * (b.p / total), 0);
  if (!Number.isFinite(expected)) return null;
  return { expected };
};

const computeExpectationAtTime = (markets, ts0, targetTs) => {
  const stats = computeExpectationStatsAtTime(markets, ts0, targetTs, "candles");
  return stats ? stats.expected : null;
};

const computeExpectationSeries = (event, markets, ts0, preMinutes, postMinutes) => {
  const base = toNumber(ts0);
  const pre = toNumber(preMinutes) ?? 30;
  const post = toNumber(postMinutes) ?? 240;
  if (!Number.isFinite(base)) return [];
  const startTs = base - pre * 60;
  const endTs = base + post * 60;
  const grid = buildMinuteGrid(startTs, endTs, 60);
  return grid
    .map((t) => {
      const expected = computeExpectationAtTime(markets, base, t);
      return Number.isFinite(expected) ? { x: (t - base) / 60, y: expected / 1000 } : null;
    })
    .filter(Boolean);
};

const buildConstantSeries = (series, value) => {
  if (!Number.isFinite(value)) return [];
  const base = safeArray(series).map((point) => toNumber(point?.x)).filter((x) => Number.isFinite(x));
  if (!base.length) return [];
  return base.map((x) => ({ x, y: value / 1000 }));
};

const renderExpectationChart = (seriesByProvider, summariesByProvider) => {
  if (!elements.expectationCanvas || typeof Chart === "undefined") return;
  const ctx = elements.expectationCanvas.getContext("2d");
  const palette = {
    kalshi: { color: "#111827", label: "Kalshi expected" },
    polymarket: { color: "#7c3aed", label: "Polymarket expected" }
  };
  const providers = Object.keys(seriesByProvider || {});
  const datasets = [];
  providers.forEach((key) => {
    const series = safeArray(seriesByProvider?.[key]);
    const color = palette[key]?.color || "#111827";
    const summary = summariesByProvider?.[key];
    if (providers.length === 1 && summary) {
      const p75 = buildConstantSeries(series, toNumber(summary.p75));
      const p25 = buildConstantSeries(series, toNumber(summary.p25));
      const median = buildConstantSeries(series, toNumber(summary.median0));
      if (p75.length && p25.length) {
        datasets.push({
          label: "__band_high__",
          data: p75,
          borderColor: "transparent",
          backgroundColor: "transparent",
          borderWidth: 0,
          pointRadius: 0,
          tension: 0,
          spanGaps: true,
          order: 0
        });
        datasets.push({
          label: "__band__",
          data: p25,
          borderColor: "transparent",
          backgroundColor: hexToRgba(color, 0.12),
          borderWidth: 0,
          pointRadius: 0,
          tension: 0,
          spanGaps: true,
          fill: "-1",
          order: 0
        });
      }
      if (median.length) {
        datasets.push({
          label: "__median__",
          data: median,
          borderColor: hexToRgba(color, 0.5),
          backgroundColor: "transparent",
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0,
          spanGaps: true,
          borderDash: [4, 4],
          order: 1
        });
      }
    }

    datasets.push({
      label: palette[key]?.label || `${key} expected`,
      data: series,
      borderColor: color,
      backgroundColor: "transparent",
      borderWidth: 2,
      pointRadius: 0,
      tension: 0.2,
      spanGaps: true,
      order: 2
    });
  });
  const config = {
    type: "line",
    data: {
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: providers.length > 1,
          position: "bottom",
          labels: {
            filter: (item, chartData) => {
              const label = chartData.datasets[item.datasetIndex]?.label || "";
              return !String(label).startsWith("__");
            }
          }
        }
      },
      interaction: { mode: "nearest", intersect: false },
      scales: {
        x: {
          type: "linear",
          title: { display: true, text: "Minutes from release" },
          ticks: { callback: (value) => `${value}m` }
        },
        y: {
          title: { display: true, text: "Expected (k jobs)" },
          ticks: { callback: (value) => `${Number(value).toFixed(0)}k` }
        }
      }
    }
  };
  if (!state.expectationChart) {
    state.expectationChart = new Chart(ctx, config);
  } else {
    state.expectationChart.config.type = config.type;
    state.expectationChart.data = config.data;
    state.expectationChart.options = config.options;
    state.expectationChart.update();
  }
};

const renderExpectationSummary = (event, summary, horizonKey, options = {}) => {
  if (!elements.expectationText || !elements.expectationMetrics) return;
  const { resolvedBls = null } = options || {};
  if (!summary) {
    elements.expectationText.textContent = "No distribution data available for this event/horizon.";
    elements.expectationMetrics.innerHTML = "";
    return;
  }

  const repricingText = Number.isFinite(summary.repricing) ? formatJobsCompact(summary.repricing) : "—";
  const sentence = `Market expected ${formatJobsLevelCompact(summary.exp0)} before the release and repriced to ${formatJobsLevelCompact(summary.exp1)} by ${horizonKey} (${repricingText}).`;
  elements.expectationText.textContent = sentence;

  const eventAnnouncement = getEventAnnouncement(event);
  const metrics = [
    { label: "Expected (before)", value: formatJobsLevelCompact(summary.exp0) },
    ...(eventAnnouncement
      ? [{ label: eventAnnouncement.label, value: formatAnnouncementValue(eventAnnouncement.announcement) }]
      : []),
    { label: `Expected (after ${horizonKey})`, value: formatJobsLevelCompact(summary.exp1) },
    { label: "Resolved (BLS)", value: Number.isFinite(resolvedBls) ? formatJobsCompact(resolvedBls) : "—" },
    {
      label: "Change in expectations",
      value: repricingText
    }
  ];
  elements.expectationMetrics.innerHTML = metrics
    .map(({ label, value }) => `<div><p class="detail-label">${label}</p><p>${value}</p></div>`)
    .join("");
};

const renderExpectationSummaryMulti = (event, summariesByProvider, horizonKey, options = {}) => {
  if (!elements.expectationText || !elements.expectationMetrics) return;
  const { resolvedBls = null, primaryProvider = null } = options || {};
  const providers = Object.keys(summariesByProvider || {});
  const hasAny = providers.some((p) => summariesByProvider?.[p] && Number.isFinite(toNumber(summariesByProvider[p].exp0)));
  if (!hasAny) {
    elements.expectationText.textContent = "No distribution data available for this event/horizon.";
    elements.expectationMetrics.innerHTML = "";
    return;
  }

  const parts = [];
  for (const p of providers) {
    const s = summariesByProvider?.[p];
    if (!s) continue;
    const label = p === "kalshi" ? "Kalshi" : "Polymarket";
    parts.push(`${label}: ${formatJobsLevelCompact(s.exp0)} → ${formatJobsLevelCompact(s.exp1)} (${formatJobsCompact(s.repricing)})`);
  }
  elements.expectationText.textContent = parts.join(" | ");

  const eventAnnouncement = getEventAnnouncement(event);
  const primaryKey = providers.includes(primaryProvider) ? primaryProvider : providers[0];
  const primarySummary = primaryKey ? summariesByProvider?.[primaryKey] : null;
  const repricingText = Number.isFinite(toNumber(primarySummary?.repricing))
    ? formatJobsCompact(toNumber(primarySummary?.repricing))
    : "—";
  const items = [
    { label: "Expected (before)", value: formatJobsLevelCompact(primarySummary?.exp0) },
    ...(eventAnnouncement
      ? [{ label: eventAnnouncement.label, value: formatAnnouncementValue(eventAnnouncement.announcement) }]
      : []),
    { label: `Expected (after ${horizonKey})`, value: formatJobsLevelCompact(primarySummary?.exp1) },
    { label: "Resolved (BLS)", value: Number.isFinite(resolvedBls) ? formatJobsCompact(resolvedBls) : "—" },
    {
      label: "Change in expectations",
      value: repricingText
    }
  ];
  elements.expectationMetrics.innerHTML = items
    .map(({ label, value }) => `<div><p class="detail-label">${label}</p><p>${value}</p></div>`)
    .join("");
};

const fillHorizonOptions = () => {
  if (!elements.horizonSelect) return;
  const meta = providerMeta();
  const minutes = safeArray(meta?.horizons_minutes)
    .map(toNumber)
    .filter((v) => Number.isFinite(v) && v > 0)
    .sort((a, b) => a - b);
  if (!minutes.length) return;

  const current = elements.horizonSelect.value || "120m";
  elements.horizonSelect.innerHTML = minutes
    .map((m) => `<option value="${m}m">${formatHorizonOptionLabel(m)}</option>`)
    .join("");

  const preferred =
    (current && minutes.some((m) => `${m}m` === String(current)) && current) ||
    (minutes.includes(120) && "120m") ||
    (minutes.includes(30) && "30m") ||
    `${minutes[0]}m`;
  elements.horizonSelect.value = preferred;
};

const pearsonR = (xs, ys) => {
  const pairs = [];
  for (let i = 0; i < xs.length; i += 1) {
    const x = toNumber(xs[i]);
    const y = toNumber(ys[i]);
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
    pairs.push([x, y]);
  }
  if (pairs.length < 2) return null;
  const mx = pairs.reduce((acc, [x]) => acc + x, 0) / pairs.length;
  const my = pairs.reduce((acc, [, y]) => acc + y, 0) / pairs.length;
  let sxx = 0;
  let syy = 0;
  let sxy = 0;
  for (const [x, y] of pairs) {
    const dx = x - mx;
    const dy = y - my;
    sxx += dx * dx;
    syy += dy * dy;
    sxy += dx * dy;
  }
  if (sxx <= 0 || syy <= 0) return null;
  return sxy / Math.sqrt(sxx * syy);
};

const computeSourceSummaryFallback = (events, sourceKey, horizonKey) => {
  const filtered = filterEventsBySource(events, sourceKey);
  const meta = providerMeta();
  const preMinutes = toNumber(meta?.window?.pre_minutes) ?? 30;
  const postMinutes = toNumber(meta?.window?.post_minutes) ?? 240;

  const perEventMean = [];
  const perEventMeanAbs = [];
  const perEventRateRatio = [];
  const perEventStab = [];
  const surprises = [];
  const deltas = [];

  for (const event of filtered) {
    const markets = safeArray(event?.markets);
    const deltasHere = markets
      .map((m) => toNumber(m?.summary?.deltas?.[horizonKey]))
      .filter((v) => Number.isFinite(v));
    if (deltasHere.length) {
      const mean = deltasHere.reduce((a, b) => a + b, 0) / deltasHere.length;
      const meanAbs = deltasHere.reduce((a, b) => a + Math.abs(b), 0) / deltasHere.length;
      perEventMean.push(mean);
      perEventMeanAbs.push(meanAbs);
    }

    const preTotals = markets
      .map((m) => toNumber(m?.summary?.volume?.pre))
      .filter((v) => Number.isFinite(v));
    const postTotals = markets
      .map((m) => toNumber(m?.summary?.volume?.post))
      .filter((v) => Number.isFinite(v));
    if (preTotals.length && postTotals.length && preMinutes > 0 && postMinutes > 0) {
      const preTotal = preTotals.reduce((a, b) => a + b, 0);
      const postTotal = postTotals.reduce((a, b) => a + b, 0);
      const preRate = preTotal / preMinutes;
      const postRate = postTotal / postMinutes;
      if (preRate > 0 && Number.isFinite(postRate)) perEventRateRatio.push(postRate / preRate);
    }

    const stabs = markets
      .map((m) => toNumber(m?.summary?.stabilization_minutes))
      .filter((v) => Number.isFinite(v));
    if (stabs.length) {
      stabs.sort((a, b) => a - b);
      perEventStab.push(stabs[Math.floor(stabs.length / 2)]);
    }

    const actual = toNumber(event?.value?.actual);
    const expected = toNumber(event?.value?.expected);
    const surprise = Number.isFinite(actual) && Number.isFinite(expected) ? actual - expected : null;
    if (Number.isFinite(surprise) && deltasHere.length) {
      surprises.push(surprise);
      deltas.push(deltasHere.reduce((a, b) => a + b, 0) / deltasHere.length);
    }
  }

  const avg = (arr) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null);
  const median = (arr) => {
    if (!arr.length) return null;
    const sorted = arr.slice().sort((a, b) => a - b);
    return sorted[Math.floor(sorted.length / 2)];
  };

  return {
    event_count: filtered.length,
    horizons: {
      [horizonKey]: {
        avg_event_mean_delta_pp: avg(perEventMean),
        avg_event_mean_abs_delta_pp: avg(perEventMeanAbs)
      }
    },
    volume_rate_ratio_total: { median: median(perEventRateRatio) },
    stabilization_minutes: { median: median(perEventStab) },
    surprise_delta_corr: { n: surprises.length, pearson_r: pearsonR(surprises, deltas) }
  };
};

const getSourceSummary = (sourceKey, horizonKey, dataset = null, eventsOverride = null) => {
  if (eventsOverride) return computeSourceSummaryFallback(eventsOverride, sourceKey, horizonKey);
  const baseDataset = dataset || state.data;
  const summary = baseDataset?.summary?.sources?.[sourceKey];
  if (summary) return summary;
  const events = baseDataset ? getEventsForDataset(baseDataset) : getEvents();
  return computeSourceSummaryFallback(events, sourceKey, horizonKey);
};

const renderSourceSummary = (sourceKey, horizonKey, { dataset = null, eventsOverride = null } = {}) => {
  if (!elements.summaryMetrics) return;
  const baseEvents = eventsOverride || (dataset ? getEventsForDataset(dataset) : getEvents());
  const revelioReleaseMonths = new Set(
    baseEvents
      .filter((event) => String(event?.type || "").toLowerCase() === "revelio")
      .map((event) => String(event?.release_month || ""))
      .filter(Boolean)
  );
  const adpEvents = baseEvents.filter((event) => {
    if (String(event?.type || "").toLowerCase() !== "adp") return false;
    const releaseMonth = String(event?.release_month || "");
    if (!releaseMonth) return false;
    return !revelioReleaseMonths.size || revelioReleaseMonths.has(releaseMonth);
  });
  const revEvents = baseEvents.filter((event) => String(event?.type || "").toLowerCase() === "revelio");

  const adpSummary = computeSourceSummaryFallback(adpEvents, "adp", horizonKey);
  const revSummary = computeSourceSummaryFallback(revEvents, "revelio", horizonKey);
  const adpAlign = computeAlignmentSummary(adpEvents, "adp", horizonKey);
  const revAlign = computeAlignmentSummary(revEvents, "revelio", horizonKey);
  const pickAbs = (summary) => {
    const horizon = summary?.horizons?.[horizonKey] || summary?.horizons?.[String(horizonKey)] || null;
    return (
      toNumber(horizon?.avg_event_mean_abs_delta_pp) ??
      toNumber(horizon?.mean_abs_delta_pp) ??
      toNumber(horizon?.avg_event_mean_abs_delta)
    );
  };
  const pickVol = (summary) => {
    return (
      toNumber(summary?.volume_rate_ratio_total?.median) ??
      toNumber(summary?.volume?.median_market_rate_ratio)
    );
  };
  const formatAbsWithN = (value, summary) => {
    if (!Number.isFinite(value)) return "—";
    const n = toNumber(summary?.event_count);
    const suffix = Number.isFinite(n) ? ` (n=${formatInt(n)})` : "";
    return `${formatPct(value)}${suffix}`;
  };
  const formatAligned = (align) => {
    if (!align || !Number.isFinite(toNumber(align.share))) return "—";
    const n = Number.isFinite(toNumber(align.n)) ? ` (n=${formatInt(toNumber(align.n))})` : "";
    return `${(toNumber(align.share) * 100).toFixed(0)}%${n}`;
  };

  const adpAbs = pickAbs(adpSummary);
  const revAbs = pickAbs(revSummary);
  const adpVol = pickVol(adpSummary);
  const revVol = pickVol(revSummary);
  renderSummaryMetrics([
    { label: `ADP |Δ Yes| (${horizonKey})`, value: formatAbsWithN(adpAbs, adpSummary) },
    { label: `Revelio |Δ Yes| (${horizonKey})`, value: formatAbsWithN(revAbs, revSummary) },
    { label: `ADP aligned share (${horizonKey})`, value: formatAligned(adpAlign) },
    { label: `Revelio aligned share (${horizonKey})`, value: formatAligned(revAlign) },
    {
      label: "ADP vol rate (post/pre)",
      value: Number.isFinite(adpVol) ? formatRatio(adpVol) : "—"
    },
    {
      label: "Revelio vol rate (post/pre)",
      value: Number.isFinite(revVol) ? formatRatio(revVol) : "—"
    }
  ]);
  renderSummaryChart(["ADP", "Revelio"], [
    Number.isFinite(adpAbs) ? adpAbs : null,
    Number.isFinite(revAbs) ? revAbs : null
  ]);
  renderSummaryVolumeChart(["ADP", "Revelio"], [
    Number.isFinite(adpVol) ? adpVol : null,
    Number.isFinite(revVol) ? revVol : null
  ]);
};

const renderTable = (event, horizonKey) => {
  if (!elements.tableHead || !elements.tableBody) return;
  const markets = sortMarketsByStrike(safeArray(event?.markets));
  if (!markets.length) {
    elements.tableHead.innerHTML = "";
    elements.tableBody.innerHTML = "";
    return;
  }

  elements.tableHead.innerHTML = `
    <tr>
      <th>Strike</th>
      <th>Baseline</th>
      <th>${horizonKey}</th>
      <th>Δ</th>
      <th>Vol (pre)</th>
      <th>Vol (post)</th>
    </tr>
  `;

  const ts0 = toNumber(event?.release_ts);
  const meta = providerMeta();
  const preMinutes = toNumber(meta?.window?.pre_minutes) ?? 30;
  const postMinutes = toNumber(meta?.window?.post_minutes) ?? 240;
  const minutes = horizonMinutes(horizonKey);
  const horizonTs = Number.isFinite(ts0) && Number.isFinite(minutes) ? ts0 + minutes * 60 : null;
  const preStart = Number.isFinite(ts0) ? ts0 - preMinutes * 60 : null;
  const postEnd = Number.isFinite(ts0) ? ts0 + postMinutes * 60 : null;

  const rows = markets.map((market) => {
    const candles = safeArray(market?.candles);
    const baseline = pickBaseline(candles, ts0);
    const level = horizonTs != null ? pickValueAtOrAfter(candles, horizonTs) : null;
    const delta =
      baseline && level && Number.isFinite(baseline.yes) && Number.isFinite(level.yes)
        ? level.yes - baseline.yes
        : null;
    const volPre = preStart != null && ts0 != null ? sumVolumeInRange(candles, preStart, ts0) : null;
    const volPost = ts0 != null && postEnd != null ? sumVolumeInRange(candles, ts0, postEnd) : null;
    return `
      <tr>
        <td>${getMarketStrikeLabel(market)}</td>
        <td>${baseline ? formatProb(baseline.yes) : "—"}</td>
        <td>${level ? formatProb(level.yes) : "—"}</td>
        <td>${Number.isFinite(delta) ? formatPct(delta) : "—"}</td>
        <td>${Number.isFinite(volPre) ? formatInt(volPre) : "—"}</td>
        <td>${Number.isFinite(volPost) ? formatInt(volPost) : "—"}</td>
      </tr>
    `;
  });

  elements.tableBody.innerHTML = rows.join("");
};

const renderHistoryTable = (events, sourceKey, horizonKey, showProvider = false) => {
  if (!elements.historyHead || !elements.historyBody) return;
  const minutes = horizonMinutes(horizonKey);
  if (!Number.isFinite(minutes)) {
    elements.historyHead.innerHTML = "";
    elements.historyBody.innerHTML = "";
    return;
  }

  const filtered = sortEventsNewestFirst(filterEventsBySource(events, sourceKey));
  if (!filtered.length) {
    elements.historyHead.innerHTML = "";
    elements.historyBody.innerHTML = "";
    return;
  }

  elements.historyHead.innerHTML = `
    <tr>
      ${showProvider ? "<th>Provider</th>" : ""}
      <th>Release</th>
      <th>Payroll</th>
      <th>Expected (pre)</th>
      <th>Announced - Expected</th>
      <th>Expected (${horizonKey})</th>
      <th>Repricing</th>
      <th>Aligned?</th>
    </tr>
  `;

  const rows = filtered.map((event) => {
    const ts0 = toNumber(event?.release_ts);
    const horizonTs = Number.isFinite(ts0) ? ts0 + minutes * 60 : null;
    const summary = computeExpectationSummary(event, safeArray(event?.markets), ts0, horizonTs);
    const exp0 = summary?.exp0;
    const exp1 = summary?.exp1;
    const repricing = summary?.repricing;
    const typeKey = String(event?.type || "").toLowerCase();
    const announced = getAnnouncedValue(event?.value);
    const surpriseAnnounced =
      isAnnouncedType(typeKey) && Number.isFinite(announced) && Number.isFinite(exp0) ? announced - exp0 : null;
    const surpriseLabel = Number.isFinite(surpriseAnnounced) ? formatJobsCompact(surpriseAnnounced) : "—";
    const sSurprise = signOf(surpriseAnnounced);
    const sReprice = signOf(repricing);
    const aligned =
      sSurprise === null || sReprice === null || sSurprise === 0 || sReprice === 0 ? null : sSurprise === sReprice;
    const alignedLabel = aligned === null ? "—" : aligned ? "Yes" : "No";
    const providerLabel = event.__provider === "polymarket" ? "Polymarket" : event.__provider === "kalshi" ? "Kalshi" : "—";

    return `
      <tr>
        ${showProvider ? `<td>${providerLabel}</td>` : ""}
        <td>${event.release_month || "—"}</td>
        <td>${event.payroll_month || "—"}</td>
        <td>${Number.isFinite(exp0) ? formatJobsLevelCompact(exp0) : "—"}</td>
        <td>${surpriseLabel}</td>
        <td>${Number.isFinite(exp1) ? formatJobsLevelCompact(exp1) : "—"}</td>
        <td>${Number.isFinite(repricing) ? formatJobsCompact(repricing) : "—"}</td>
        <td>${alignedLabel}</td>
      </tr>
    `;
  });

  elements.historyBody.innerHTML = rows.join("");
};

const computeAlignmentSummary = (events, sourceKey, horizonKey) => {
  const minutes = horizonMinutes(horizonKey);
  if (!Number.isFinite(minutes)) return null;
  const filtered = filterEventsBySource(events, sourceKey);
  const alignedEvents = [];
  const repricingSigned = [];
  for (const event of filtered) {
    const ts0 = toNumber(event?.release_ts);
    if (!Number.isFinite(ts0)) continue;
    const horizonTs = ts0 + minutes * 60;
    const summary = computeExpectationSummary(event, safeArray(event?.markets), ts0, horizonTs);
    const exp0 = summary?.exp0;
    const exp1 = summary?.exp1;
    if (!Number.isFinite(exp0) || !Number.isFinite(exp1)) continue;
    const repricing = exp1 - exp0;

    const typeKey = String(event?.type || "").toLowerCase();
    const announced = getAnnouncedValue(event?.value);
    if (!isAnnouncedType(typeKey) || !Number.isFinite(announced)) continue;

    const surprise = announced - exp0;
    const sSurprise = signOf(surprise);
    const sReprice = signOf(repricing);
    if (sSurprise === null || sReprice === null || sSurprise === 0 || sReprice === 0) continue;
    const isAligned = sSurprise === sReprice;
    alignedEvents.push(isAligned);
    repricingSigned.push(sSurprise * repricing);
  }

  const n = alignedEvents.length;
  if (!n) return { n: 0, aligned: 0, share: null, avg_signed_repricing: null };
  const aligned = alignedEvents.filter(Boolean).length;
  const share = aligned / n;
  const avgSigned = repricingSigned.reduce((a, b) => a + b, 0) / repricingSigned.length;
  return { n, aligned, share, avg_signed_repricing: avgSigned };
};

const computeArbStats = (series) => {
  const values = safeArray(series)
    .map((point) => toNumber(point?.y))
    .filter((value) => Number.isFinite(value));
  if (!values.length) return { max: null, avg: null, last: null };
  const max = Math.max(...values);
  const avg = values.reduce((a, b) => a + b, 0) / values.length;
  const last = values[values.length - 1];
  return { max, avg, last };
};

const updateArbSection = async (eventId) => {
  if (!elements.arbMetrics && !elements.arbCanvas) return;
  if (!eventId) {
    renderArbMetrics(null);
    renderArbDetail(null);
    setArbVisibility(false, "Select an event to see the arb window.");
    return;
  }
  try {
    const [kalshiData, polymarketData] = await Promise.all([ensureDataset("kalshi"), ensureDataset("polymarket")]);
    const kalshiEvent = kalshiData ? getEventByIdInDataset(kalshiData, eventId) : null;
    const polymarketEvent = polymarketData ? getEventByIdInDataset(polymarketData, eventId) : null;
    if (!kalshiEvent || !polymarketEvent) {
      renderArbMetrics(null);
      renderArbDetail(null);
      setArbVisibility(false, "Arb window needs both Kalshi and Polymarket data for this event.");
      return;
    }
    const arb = computeLongtermArbSeries(kalshiEvent, polymarketEvent);
    if (!arb.hasCandles) {
      const missing = arb.missingProviders || [];
      let message = "Long-term series not loaded.";
      if (missing.length === 1) {
        message =
          missing[0] === "Kalshi"
            ? "Long-term series not loaded for Kalshi. Re-run scripts/fetch_kalshi_impact.py with --longterm-days."
            : "Long-term series not loaded for Polymarket. Re-run scripts/fetch_polymarket_impact.py with --longterm-days.";
      } else if (missing.length > 1) {
        message =
          "Long-term series not loaded for Kalshi or Polymarket. Re-run scripts/fetch_kalshi_impact.py and scripts/fetch_polymarket_impact.py with --longterm-days.";
      }
      renderArbMetrics(null);
      renderArbDetail(null);
      setArbVisibility(false, message);
      return;
    }
    if (!arb.series.length) {
      renderArbMetrics(null);
      renderArbDetail(null);
      setArbVisibility(false, "No overlapping long-term prices between Kalshi and Polymarket.");
      return;
    }
    const windowDays = getLongtermWindowDays();
    const seriesMax = Math.max(0, getSeriesMaxX(arb.series));
    const kClose = getMarketCloseOffsetDays(kalshiEvent?.markets, kalshiEvent?.release_ts);
    const pClose = getMarketCloseOffsetDays(polymarketEvent?.markets, polymarketEvent?.release_ts);
    const closeOffset = pickMinPositive(kClose, pClose);
    const maxPost = Number.isFinite(closeOffset) ? Math.min(seriesMax, closeOffset) : seriesMax;
    const filtered = filterSeriesByWindow(arb.series, windowDays, maxPost);
    if (!filtered.length) {
      renderArbMetrics(null);
      renderArbDetail(null);
      setArbVisibility(false, "No overlapping long-term prices in this window.");
      return;
    }
    setArbVisibility(true);
    renderArbChart(filtered, -windowDays, maxPost, arb.releaseTs, closeOffset);
    renderArbMetrics(computeArbStats(filtered));
    renderArbDetail(filtered[filtered.length - 1]?.detail || null);
  } catch (err) {
    console.error(err);
    renderArbMetrics(null);
    renderArbDetail(null);
    setArbVisibility(false, "Unable to compute arb window.");
  }
};

const updateView = async () => {
  try {
  const provider = getProviderKey();
  const eventId = elements.eventSelect?.value;
  const event = eventId ? getEventById(eventId) : null;
  if (!event) {
    const cfg = PROVIDER_CONFIG[provider] || PROVIDER_CONFIG.kalshi;
    setStatus(`No events loaded. Generate ${cfg.path}.`, true);
    return;
  }

  fillStrikeOptions(event);
  await updateArbSection(event.id);

  const ticker = elements.strikeSelect?.value;
  const horizonKey = elements.horizonSelect?.value || "120m";
  const sourceKey = elements.sourceSelect?.value || "all";
  const markets = safeArray(event.markets);

  const ts0 = toNumber(event.release_ts);
  const minutes = horizonMinutes(horizonKey);
  const horizonTs = Number.isFinite(ts0) && Number.isFinite(minutes) ? ts0 + minutes * 60 : null;
  const horizonLabel = formatHorizonOptionLabel(minutes);

  const value = event.value || null;
  const announcedRange = getAnnouncedRange(value);
  const announcedValue = getAnnouncedValue(value);
  const actual = announcedValue;
  const expected = toNumber(value?.expected);
  const surprise = Number.isFinite(actual) && Number.isFinite(expected) ? actual - expected : null;
  const typeKey = String(event?.type || "").toLowerCase();
  const actualLabel = isAnnouncedType(typeKey) ? (announcedRange ? "Announced range" : "Announced") : "Actual";
  const showAnnouncedPrimary = isAnnouncedType(typeKey) && (announcedRange || Number.isFinite(actual));
  const primaryMetric = { label: "Event", value: buildEventLabel(event, { includeAnnounced: false }) };

  const meta = providerMeta();
  const preMinutes = toNumber(meta?.window?.pre_minutes) ?? 30;
  const postMinutes = toNumber(meta?.window?.post_minutes) ?? 240;

  const expectationSummary = computeExpectationSummary(event, markets, ts0, horizonTs);
  const blsByMonth = await ensureBlsByMonth();
  const resolvedBls = getResolvedBlsChange(event?.payroll_month, blsByMonth);
  const summaryProviders = getSummaryProviders();
  const datasets = await Promise.all(summaryProviders.map((p) => ensureDataset(p)));
  const summariesByProvider = {};
  const seriesByProvider = {};
  for (let i = 0; i < summaryProviders.length; i += 1) {
    const p = summaryProviders[i];
    const dataset = datasets[i];
    const matched = dataset ? getEventByIdInDataset(dataset, event.id) : null;
    if (!matched) continue;
    const providerMarkets = safeArray(matched?.markets);
    const pm = providerMetaForDataset(dataset);
    const pre = toNumber(pm?.window?.pre_minutes) ?? 30;
    const post = toNumber(pm?.window?.post_minutes) ?? 240;
    const pTs0 = toNumber(matched?.release_ts);
    const pMinutes = horizonMinutes(horizonKey);
    const pHorizonTs = Number.isFinite(pTs0) && Number.isFinite(pMinutes) ? pTs0 + pMinutes * 60 : null;
    const summary = computeExpectationSummary(matched, providerMarkets, pTs0, pHorizonTs);
    if (summary) summariesByProvider[p] = summary;
    seriesByProvider[p] = computeExpectationSeries(matched, providerMarkets, pTs0, pre, post);
  }

  const combinedEvents = summaryProviders
    .map((p, idx) => ({ p, dataset: datasets[idx] }))
    .filter((row) => row.dataset)
    .flatMap(({ p, dataset }) => getEventsForDataset(dataset).map((evt) => ({ ...evt, __provider: p })));

  if (summaryProviders.length > 1) {
    renderExpectationSummaryMulti(event, summariesByProvider, horizonKey, {
      resolvedBls,
      primaryProvider: getProviderKey()
    });
  } else {
    const only = summaryProviders[0];
    renderExpectationSummary(event, summariesByProvider[only] || null, horizonKey, {
      resolvedBls
    });
  }
  renderExpectationChart(seriesByProvider, summariesByProvider);

  renderHistoryTable(combinedEvents, sourceKey, horizonKey, summaryProviders.length > 1);
  const summaryDataset = summaryProviders.length === 1 ? datasets[0] : null;
  const summaryEvents = summaryProviders.length > 1 ? combinedEvents : null;

  if (String(ticker) === "__AGG__") {
    const agg = computeAggregateAlignedSeries(event, markets, ts0, preMinutes, postMinutes, 50000);
    const aggSeries = safeArray(agg?.series);
    const aggLevel =
      horizonTs != null
        ? toNumber(aggSeries.find((row) => Number.isFinite(toNumber(row?.[0])) && toNumber(row[0]) >= horizonTs)?.[1])
        : null;

    const eventVolume = {
      pre: toNumber(event?.summary?.volume?.pre_total),
      post: toNumber(event?.summary?.volume?.post_total),
      rate: toNumber(event?.summary?.volume?.rate_ratio_total)
    };
    if (!Number.isFinite(eventVolume.pre) && !Number.isFinite(eventVolume.post)) {
      const fallback = computeAggregateVolumeStats(markets, ts0, preMinutes, postMinutes);
      if (fallback) {
        eventVolume.pre = fallback.pre;
        eventVolume.post = fallback.post;
        eventVolume.rate = Number.isFinite(eventVolume.rate) ? eventVolume.rate : fallback.rate_ratio;
      }
    }
    const volumeMetrics = provider === "kalshi"
      ? [
          {
            label: "Volume (pre/post)",
            value: formatVolumePair(eventVolume.pre, eventVolume.post, "—")
          },
          {
            label: "Vol rate (post/pre)",
            value: Number.isFinite(eventVolume.rate) ? formatRatio(eventVolume.rate) : "—"
          }
        ]
      : [];

    renderMetrics([
      primaryMetric,
      { label: `Aggregate move (${horizonKey})`, value: Number.isFinite(aggLevel) ? formatPct(aggLevel) : "—" },
      ...(Number.isFinite(resolvedBls) ? [{ label: "Resolved (BLS)", value: formatJobsCompact(resolvedBls) }] : []),
      ...(showAnnouncedPrimary ? [] : [{ label: actualLabel, value: Number.isFinite(actual) ? formatInt(actual) : "—" }]),
      { label: "Expected", value: Number.isFinite(expected) ? formatInt(expected) : "—" },
      { label: "Surprise", value: Number.isFinite(surprise) ? formatInt(surprise) : "—" },
      ...volumeMetrics
    ]);

    renderAggregateChart("Aggregate (announcement-weighted)", aggSeries, ts0);

    const barMarkets = sortMarketsByStrike(markets).filter((m) => Number.isFinite(toNumber(m?.strike?.value)));
    const barMode = elements.barModeSelect?.value || "distribution";
    if (barMode === "distribution") {
      renderDistributionDensityChart({
        summariesByProvider,
        horizonLabel,
        announcedValue: showAnnouncedPrimary ? actual : null,
        announcedRange: showAnnouncedPrimary ? announcedRange : null
      });
    } else {
      const barLabels = barMarkets.map((m) => getMarketStrikeLabel(m));
      const barValues = barMarkets.map((m) => {
        const strike = toNumber(m?.strike?.value);
        const sign = Number.isFinite(actual) && Number.isFinite(strike) ? (actual > strike ? 1 : actual < strike ? -1 : 0) : 0;
        const summaryDelta = toNumber(m?.summary?.deltas?.[horizonKey]);
        return sign && Number.isFinite(summaryDelta) ? sign * summaryDelta : null;
      });
      renderBarChart(barLabels, barValues, "Aligned Δ Yes (pp)");
    }

    renderTable(event, horizonKey);
    if (elements.volumeTitle) {
      const volWindow = getVolumeWindowMinutes();
      elements.volumeTitle.textContent = `Rolling volume (last ${formatDurationLabel(volWindow)}, sum across strikes)`;
    }
    if (provider !== "kalshi") {
      setVolumeVisibility(false, "Volume timeline available for Kalshi only.");
    } else {
      const volumeWindow = getVolumeWindowMinutes();
      const volumeData = buildVolumeSeries(markets, ts0, preMinutes, postMinutes, volumeWindow);
      if (!volumeData.hasVolume) {
        setVolumeVisibility(false, "No volume data available for this event.");
      } else {
        setVolumeVisibility(true);
        renderVolumeChart(volumeData.series);
      }
    }
    const longterm = computeLongtermExpectationSeries(markets, ts0);
    if (!longterm.hasCandles) {
      const scriptHint =
        provider === "polymarket"
          ? "Long-term series not loaded. Re-run scripts/fetch_polymarket_impact.py with --longterm-days."
          : "Long-term series not loaded. Re-run scripts/fetch_kalshi_impact.py with --longterm-days.";
      setTrendVisibility(false, scriptHint);
      setTrendVolumeVisibility(false, scriptHint);
    } else if (!longterm.series.length) {
      setTrendVisibility(false, "No long-term expectation data available for this event.");
      setTrendVolumeVisibility(false, "No long-term expectation data available for this event.");
    } else {
      const windowDays = getLongtermWindowDays();
      const seriesMax = Math.max(0, getSeriesMaxX(longterm.series));
      const closeOffset = getMarketCloseOffsetDays(markets, ts0);
      const maxPost = Number.isFinite(closeOffset) ? Math.min(seriesMax, closeOffset) : seriesMax;
      const filteredSeries = filterSeriesByWindow(longterm.series, windowDays, maxPost);
      setTrendVisibility(true);
      renderLongtermChart(filteredSeries, -windowDays, maxPost, ts0, closeOffset);
      if (provider !== "kalshi") {
        setTrendVolumeVisibility(false, "Long-term volume is available for Kalshi only.");
      } else {
        const volumeWindow = getLongtermVolumeWindowMinutes();
        if (elements.trendVolumeTitle) {
          elements.trendVolumeTitle.textContent = `Rolling $ volume (notional, last ${formatDurationLabel(volumeWindow)})`;
        }
        const volumeSeries = computeLongtermVolumeSeries(markets, ts0, volumeWindow);
        const volumeSeriesMax = Math.max(0, getSeriesMaxX(volumeSeries.series));
        const volumeMaxPost = Number.isFinite(closeOffset) ? Math.min(volumeSeriesMax, closeOffset) : volumeSeriesMax;
        const filteredVolume = filterSeriesByWindow(volumeSeries.series, windowDays, volumeMaxPost);
        if (!volumeSeries.hasVolume || !filteredVolume.length) {
          setTrendVolumeVisibility(false, "No long-term volume data available for this event.");
        } else {
          setTrendVolumeVisibility(true);
          renderLongtermVolumeChart(filteredVolume, -windowDays, volumeMaxPost, ts0, closeOffset);
        }
      }
    }
    renderSourceSummary(sourceKey, horizonKey, { dataset: summaryDataset, eventsOverride: summaryEvents });
    setStatus(`Loaded aggregate view (${barMarkets.length} strikes).`);
    return;
  }

  const market = markets.find((m) => String(m?.ticker) === String(ticker)) || markets[0] || null;
  const candles = safeArray(market?.candles);
  const candlePoints = candles.filter((c) => Number.isFinite(toNumber(c?.[0])) && Number.isFinite(toNumber(c?.[1])));
  const baseline = pickBaseline(candles, ts0);
  const level = horizonTs != null ? pickValueNearestAvailable(candles, horizonTs) : null;
  const delta =
    baseline && level && Number.isFinite(baseline.yes) && Number.isFinite(level.yes)
      ? level.yes - baseline.yes
      : null;

  const marketVolume = {
    pre: toNumber(market?.summary?.volume?.pre),
    post: toNumber(market?.summary?.volume?.post),
    rate: toNumber(market?.summary?.volume?.rate_ratio)
  };
  if (!Number.isFinite(marketVolume.pre) && !Number.isFinite(marketVolume.post)) {
    const fallback = computeVolumeStats(candles, ts0, preMinutes, postMinutes);
    if (fallback) {
      marketVolume.pre = fallback.pre;
      marketVolume.post = fallback.post;
      marketVolume.rate = Number.isFinite(marketVolume.rate) ? marketVolume.rate : fallback.rate_ratio;
    }
  }
  const volumeMetrics = provider === "kalshi"
    ? [
        {
          label: "Volume (pre/post)",
          value: formatVolumePair(marketVolume.pre, marketVolume.post, "—")
        },
        {
          label: "Vol rate (post/pre)",
          value: Number.isFinite(marketVolume.rate) ? formatRatio(marketVolume.rate) : "—"
        }
      ]
    : [];

  renderMetrics([
    primaryMetric,
    { label: "Strike", value: market ? getMarketStrikeLabel(market) : "—" },
    { label: `Δ Yes (${horizonKey})`, value: Number.isFinite(delta) ? formatPct(delta) : "—" },
    { label: "Baseline", value: baseline ? formatProb(baseline.yes) : "—" },
    { label: horizonKey, value: level ? formatProb(level.yes) : "—" },
    ...(Number.isFinite(resolvedBls) ? [{ label: "Resolved (BLS)", value: formatJobsCompact(resolvedBls) }] : []),
    ...(showAnnouncedPrimary ? [] : [{ label: actualLabel, value: Number.isFinite(actual) ? formatInt(actual) : "—" }]),
    { label: "Expected", value: Number.isFinite(expected) ? formatInt(expected) : "—" },
    { label: "Surprise", value: Number.isFinite(surprise) ? formatInt(surprise) : "—" },
    ...volumeMetrics
  ]);

  const label = `${getMarketStrikeLabel(market)} · ${String(event.type || "").toUpperCase()}`;
  renderLineChart(label, candles, ts0);
  const longterm = computeLongtermExpectationSeries(markets, ts0);
  if (!longterm.hasCandles) {
    const scriptHint =
      provider === "polymarket"
        ? "Long-term series not loaded. Re-run scripts/fetch_polymarket_impact.py with --longterm-days."
        : "Long-term series not loaded. Re-run scripts/fetch_kalshi_impact.py with --longterm-days.";
    setTrendVisibility(false, scriptHint);
    setTrendVolumeVisibility(false, scriptHint);
  } else if (!longterm.series.length) {
    setTrendVisibility(false, "No long-term expectation data available for this event.");
    setTrendVolumeVisibility(false, "No long-term expectation data available for this event.");
  } else {
    const windowDays = getLongtermWindowDays();
    const seriesMax = Math.max(0, getSeriesMaxX(longterm.series));
    const closeOffset = getMarketCloseOffsetDays(markets, ts0);
    const maxPost = Number.isFinite(closeOffset) ? Math.min(seriesMax, closeOffset) : seriesMax;
    const filteredSeries = filterSeriesByWindow(longterm.series, windowDays, maxPost);
    setTrendVisibility(true);
    renderLongtermChart(filteredSeries, -windowDays, maxPost, ts0, closeOffset);
    if (provider !== "kalshi") {
      setTrendVolumeVisibility(false, "Long-term volume is available for Kalshi only.");
    } else {
      const volumeWindow = getLongtermVolumeWindowMinutes();
      if (elements.trendVolumeTitle) {
        elements.trendVolumeTitle.textContent = `Rolling $ volume (notional, last ${formatDurationLabel(volumeWindow)})`;
      }
      const volumeSeries = computeLongtermVolumeSeries(markets, ts0, volumeWindow);
      const volumeSeriesMax = Math.max(0, getSeriesMaxX(volumeSeries.series));
      const volumeMaxPost = Number.isFinite(closeOffset) ? Math.min(volumeSeriesMax, closeOffset) : volumeSeriesMax;
      const filteredVolume = filterSeriesByWindow(volumeSeries.series, windowDays, volumeMaxPost);
      if (!volumeSeries.hasVolume || !filteredVolume.length) {
        setTrendVolumeVisibility(false, "No long-term volume data available for this event.");
      } else {
        setTrendVolumeVisibility(true);
        renderLongtermVolumeChart(filteredVolume, -windowDays, volumeMaxPost, ts0, closeOffset);
      }
    }
  }

  const barMode = elements.barModeSelect?.value || "distribution";
  if (barMode === "delta") {
    const barLabels = sortMarketsByStrike(markets).map((m) => getMarketStrikeLabel(m));
    const barValues = sortMarketsByStrike(markets).map((m) => {
      const summaryDelta = toNumber(m?.summary?.deltas?.[horizonKey]);
      if (Number.isFinite(summaryDelta)) return summaryDelta;
      const series = safeArray(m?.candles);
      const base = pickBaseline(series, ts0);
      const horizon = horizonTs != null ? pickValueNearestAvailable(series, horizonTs) : null;
      return base && horizon ? horizon.yes - base.yes : null;
    });
    renderBarChart(barLabels, barValues);
  } else if (barMode === "distribution") {
    renderDistributionDensityChart({
      summariesByProvider,
      horizonLabel,
      announcedValue: showAnnouncedPrimary ? actual : null,
      announcedRange: showAnnouncedPrimary ? announcedRange : null
    });
  }

  renderTable(event, horizonKey);
  if (elements.volumeTitle) {
    const volWindow = getVolumeWindowMinutes();
    elements.volumeTitle.textContent = `Rolling volume (last ${formatDurationLabel(volWindow)}, sum across strikes)`;
  }
  if (provider !== "kalshi") {
    setVolumeVisibility(false, "Volume timeline available for Kalshi only.");
  } else if (!market) {
    setVolumeVisibility(false, "No volume data available for this event.");
  } else {
    const volumeWindow = getVolumeWindowMinutes();
    const volumeData = buildVolumeSeries(markets, ts0, preMinutes, postMinutes, volumeWindow);
    if (!volumeData.hasVolume) {
      setVolumeVisibility(false, "No volume data available for this event.");
    } else {
      setVolumeVisibility(true);
      renderVolumeChart(volumeData.series);
    }
  }
  renderSourceSummary(sourceKey, horizonKey, { dataset: summaryDataset, eventsOverride: summaryEvents });
  if (provider === "polymarket" && market && candlePoints.length === 0) {
    const usedFallback = Boolean(market?.fetch?.used_fallback);
    setStatus(
      `Polymarket has no price history in this window${usedFallback ? " (even after widening the query window)" : ""}. Try re-running scripts/fetch_polymarket_impact.py with larger --fallback-pre-days/--fallback-post-days.`,
      true
    );
    return;
  }
  setStatus(`Loaded ${markets.length} strikes for ${String(event.type || "").toUpperCase()}.`);
  } catch (err) {
    console.error(err);
    setStatus(`Unable to update view. (${err?.message || err})`, true);
  }
};

const ensureLoaded = async () => {
  if (state.loading) return;
  const provider = getProviderKey();
  const cfg = PROVIDER_CONFIG[provider] || PROVIDER_CONFIG.kalshi;
  // If provider switched, force reload.
  if (state.loaded && state.provider === provider && state.data) return;

  state.loading = true;
  setStatus(`Loading ${cfg.label} impact dataset…`);
  try {
    state.data = await fetchJSONLocal(cfg.path);
    const customEvents = loadCustomEvents();
    if (customEvents.length) {
      state.data.events = mergeCustomEvents(state.data.events, customEvents);
    }
    state.datasets[provider] = state.data;
    state.loaded = true;
    state.provider = provider;
    setStatus("Loaded dataset.");
  } catch (err) {
    setStatus(
      `Unable to load dataset. Generate ${cfg.path}. (${err.message})`,
      true
    );
  } finally {
    state.loading = false;
  }
};

const attachEvents = () => {
  if (elements.summaryProviderSelect) {
    const stored = localStorage.getItem(SUMMARY_PROVIDER_KEY);
    if (stored && (stored === "kalshi" || stored === "polymarket")) {
      elements.summaryProviderSelect.value = stored;
    } else if (stored === "both") {
      elements.summaryProviderSelect.value = "kalshi";
    }
    state.provider = getProviderKey();
    elements.summaryProviderSelect.addEventListener("change", async () => {
      localStorage.setItem(SUMMARY_PROVIDER_KEY, getSummaryProviderKey());
      state.provider = getProviderKey();
      state.loaded = false;
      state.data = null;
      await ensureLoaded();
      fillHorizonOptions();
      fillEventOptions();
      void updateView();
    });
  }
  if (elements.sourceSelect) {
    elements.sourceSelect.addEventListener("change", () => {
      fillEventOptions();
      void updateView();
    });
  }
  if (elements.eventSelect) {
    elements.eventSelect.addEventListener("change", () => void updateView());
  }
  if (elements.strikeSelect) {
    elements.strikeSelect.addEventListener("change", () => void updateView());
  }
  if (elements.horizonSelect) {
    elements.horizonSelect.addEventListener("change", () => void updateView());
  }
  if (elements.barModeSelect) {
    elements.barModeSelect.addEventListener("change", () => void updateView());
  }
  if (elements.volumeWindowSelect) {
    const stored = localStorage.getItem(VOLUME_WINDOW_KEY);
    if (stored) {
      const hasOption = Array.from(elements.volumeWindowSelect.options).some((opt) => opt.value === stored);
      if (hasOption) elements.volumeWindowSelect.value = stored;
    }
    elements.volumeWindowSelect.addEventListener("change", () => {
      localStorage.setItem(VOLUME_WINDOW_KEY, elements.volumeWindowSelect.value);
      void updateView();
    });
  }
  if (elements.trendWindowSelect) {
    const stored = localStorage.getItem(LONGTERM_WINDOW_KEY);
    if (stored) {
      const hasOption = Array.from(elements.trendWindowSelect.options).some((opt) => opt.value === stored);
      if (hasOption) elements.trendWindowSelect.value = stored;
    }
    elements.trendWindowSelect.addEventListener("change", () => {
      localStorage.setItem(LONGTERM_WINDOW_KEY, elements.trendWindowSelect.value);
      void updateView();
    });
  }
  if (elements.trendVolumeWindowSelect) {
    const stored = localStorage.getItem(LONGTERM_VOLUME_WINDOW_KEY);
    if (stored) elements.trendVolumeWindowSelect.value = stored;
    elements.trendVolumeWindowSelect.addEventListener("change", () => {
      localStorage.setItem(LONGTERM_VOLUME_WINDOW_KEY, elements.trendVolumeWindowSelect.value);
      void updateView();
    });
  }
  if (elements.refreshButton) {
    elements.refreshButton.addEventListener("click", async () => {
      state.loaded = false;
      state.data = null;
      await ensureLoaded();
      fillHorizonOptions();
      fillEventOptions();
      void updateView();
    });
  }
  if (elements.customToggle) {
    elements.customToggle.addEventListener("click", () => {
      const isOpen = elements.customPanel ? !elements.customPanel.hidden : false;
      setCustomPanelOpen(!isOpen);
    });
  }
  if (elements.customAdd) {
    elements.customAdd.addEventListener("click", () => {
      const custom = addCustomEvent();
      if (custom) void updateView();
    });
  }
  if (elements.customClear) {
    elements.customClear.addEventListener("click", () => clearCustomForm());
  }
  if (elements.customCancel) {
    elements.customCancel.addEventListener("click", () => {
      clearCustomForm();
      setCustomPanelOpen(false);
    });
  }
};

const init = async () => {
  initImpactInfoPopover();
  attachEvents();
  setCustomPanelOpen(false);
  seedCustomForm();
  await ensureLoaded();
  fillHorizonOptions();
  fillEventOptions();
  applyTopSection(localStorage.getItem(TOP_SECTION_KEY) || "");
  void updateView();
};

init();
