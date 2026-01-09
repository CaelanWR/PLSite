import { byId, toNumber, fetchJSONLocal, initInfoPopover } from "./ui_utils.js";

const elements = {
  providerSelect: byId("impactProviderSelect"),
  summaryProviderSelect: byId("impactSummaryProviderSelect"),
  sectionSelect: byId("impactSectionSelect"),
  sectionsWrap: byId("impactSections"),
  sourceSelect: byId("impactSourceSelect"),
  eventSelect: byId("impactEventSelect"),
  strikeSelect: byId("impactStrikeSelect"),
  horizonSelect: byId("impactHorizonSelect"),
  barModeSelect: byId("impactBarModeSelect"),
  refreshButton: byId("impactRefresh"),
  status: byId("impactStatus"),
  summaryMetrics: byId("impactSummaryMetrics"),
  metrics: byId("impactMetrics"),
  expectationText: byId("impactExpectationText"),
  expectationMetrics: byId("impactExpectationMetrics"),
  expectationCanvas: byId("impactExpectationChart"),
  alignmentMetrics: byId("impactAlignmentMetrics"),
  canvas: byId("impactChart"),
  barCanvas: byId("impactBarChart"),
  tableHead: byId("impactTableHead"),
  tableBody: byId("impactTableBody"),
  historyHead: byId("impactHistoryTableHead"),
  historyBody: byId("impactHistoryTableBody"),
  infoWrap: byId("impactInfo"),
  infoBtn: byId("impactInfoBtn")
};

const state = {
  data: null,
  loaded: false,
  loading: false,
  provider: "kalshi",
  datasets: { kalshi: null, polymarket: null },
  chart: null,
  barChart: null,
  expectationChart: null
};

const PROVIDER_KEY = "impactProvider";
const SUMMARY_PROVIDER_KEY = "impactSummaryProvider";
const TOP_SECTION_KEY = "impactTopSection";
const PROVIDER_CONFIG = {
  kalshi: { label: "Kalshi", path: "data/kalshi_impact.json" },
  polymarket: { label: "Polymarket", path: "data/polymarket_impact.json" }
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

const formatIsoShort = (iso) => {
  const parsed = iso ? new Date(iso) : null;
  if (!parsed || Number.isNaN(parsed.getTime())) return "—";
  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short"
  }).format(parsed);
};

const setStatus = (message, isError = false) => {
  if (!elements.status) return;
  elements.status.textContent = message;
  elements.status.classList.toggle("error", Boolean(isError));
};

const getProviderKey = () => {
  const raw = elements.providerSelect?.value || state.provider || "kalshi";
  return raw === "polymarket" ? "polymarket" : "kalshi";
};

const getSummaryProviderKey = () => {
  const raw = elements.summaryProviderSelect?.value || "same";
  if (raw === "kalshi" || raw === "polymarket" || raw === "both") return raw;
  return "same";
};

const getSummaryProviders = () => {
  const chosen = getSummaryProviderKey();
  if (chosen === "both") return ["kalshi", "polymarket"];
  if (chosen === "kalshi" || chosen === "polymarket") return [chosen];
  return [getProviderKey()];
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
  state.datasets[key] = data;
  return data;
};

const TOP_SECTION_ORDER = [
  "impactYesCard",
  "impactExpectationCard",
  "impactAlignmentCard",
  "impactDistributionCard",
  "impactHistoryCard",
  "impactStrikesCard"
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

const buildEventLabel = (event) => {
  const type = String(event.type || "").toUpperCase() || "EVENT";
  const payroll = event.payroll_month ? `Payrolls: ${event.payroll_month}` : null;
  const when = event.release_iso ? formatIsoShort(event.release_iso) : null;
  const announced = toNumber(event?.value?.actual);
  const typeKey = String(event?.type || "").toLowerCase();
  const announcedLabel =
    Number.isFinite(announced) && (typeKey === "adp" || typeKey === "revelio")
      ? `Announced: ${formatJobsCompact(announced)}`
      : null;
  return [type, payroll, when, announcedLabel].filter(Boolean).join(" · ");
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
  const filtered = sortEventsNewestFirst(filterEventsBySource(getEvents(), source));
  if (!filtered.length) {
    elements.eventSelect.innerHTML = '<option value="">No events loaded</option>';
    return;
  }
  elements.eventSelect.innerHTML = filtered
    .map((event) => `<option value="${event.id}">${buildEventLabel(event)}</option>`)
    .join("");
};

const fillStrikeOptions = (event) => {
  if (!elements.strikeSelect) return;
  const current = elements.strikeSelect.value;
  const markets = sortMarketsByStrike(safeArray(event?.markets));
  if (!markets.length) {
    elements.strikeSelect.innerHTML = '<option value="">No strikes</option>';
    return;
  }
  const announced = toNumber(event?.value?.actual);
  const hasStrikes = markets.some((m) => Number.isFinite(toNumber(m?.strike?.value)));
  const showAggregate = Number.isFinite(announced) && hasStrikes;
  const options = [
    ...(showAggregate ? [{ value: "__AGG__", label: "Aggregate (announcement-weighted)" }] : []),
    ...markets.map((market) => ({ value: market.ticker, label: getMarketStrikeLabel(market) }))
  ];
  elements.strikeSelect.innerHTML = options
    .map((opt) => `<option value="${String(opt.value)}">${String(opt.label)}</option>`)
    .join("");
  const preferred =
    (current && options.some((o) => String(o.value) === String(current)) && current) ||
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
  const announced = toNumber(event?.value?.actual);
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
      const p = pickValueAtOrAfter(item.candles, t);
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

const renderDistributionBarChart = (labels, beforeValues, afterValues, horizonLabel) => {
  if (!elements.barCanvas || typeof Chart === "undefined") return;
  const ctx = elements.barCanvas.getContext("2d");
  const config = {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Before",
          data: beforeValues,
          backgroundColor: "rgba(37, 99, 235, 0.5)",
          borderColor: "rgba(37, 99, 235, 0.9)",
          borderWidth: 1
        },
        {
          label: `After (${horizonLabel})`,
          data: afterValues,
          backgroundColor: "rgba(220, 38, 38, 0.45)",
          borderColor: "rgba(220, 38, 38, 0.85)",
          borderWidth: 1
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "bottom" } },
      scales: {
        y: {
          min: 0,
          max: 100,
          title: { display: true, text: "Probability" },
          ticks: { callback: (v) => `${Number(v).toFixed(0)}%` }
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
  return pickValueAtOrAfter(candles, t);
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

  return {
    exp0,
    exp1,
    repricing,
    top0,
    top1,
    conf0,
    conf1,
    buckets
  };
};

const computeExpectationAtTime = (markets, ts0, targetTs) => {
  const snapshots = [];
  for (const market of safeArray(markets)) {
    const strike = market?.strike || null;
    const rep = toNumber(strike?.value);
    const kind = strike?.kind ? String(strike.kind) : null;
    const label = getMarketStrikeLabel(market);
    const candles = safeArray(market?.candles);
    const point = pickValueAroundRelease(candles, ts0, targetTs);
    const yes = point ? toNumber(point.yes) : null;
    if (!Number.isFinite(rep) || !Number.isFinite(yes)) continue;
    snapshots.push({ label, kind, rep, p: yes / 100 });
  }

  const bracketLikes = snapshots.filter((s) => String(s.kind || "").toLowerCase() === "range");
  const aboveLikes = snapshots.filter((s) => String(s.kind || "").toLowerCase() === "above");

  const buckets =
    bracketLikes.length >= Math.max(3, Math.floor(snapshots.length * 0.6))
      ? bracketLikes.map((s) => ({ label: s.label, rep: s.rep, p: s.p }))
      : (() => {
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
        })();

  if (!buckets.length) return null;
  const total = buckets.reduce((acc, b) => acc + (Number.isFinite(b.p) ? b.p : 0), 0);
  if (!(total > 0)) return null;
  const expected = buckets.reduce((acc, b) => acc + b.rep * (b.p / total), 0);
  return Number.isFinite(expected) ? expected : null;
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

const renderExpectationChart = (seriesByProvider) => {
  if (!elements.expectationCanvas || typeof Chart === "undefined") return;
  const ctx = elements.expectationCanvas.getContext("2d");
  const palette = {
    kalshi: { color: "#111827", label: "Kalshi expected" },
    polymarket: { color: "#7c3aed", label: "Polymarket expected" }
  };
  const providers = Object.keys(seriesByProvider || {});
  const datasets = providers.map((key) => ({
    label: palette[key]?.label || `${key} expected`,
    data: safeArray(seriesByProvider?.[key]),
    borderColor: palette[key]?.color || "#111827",
    backgroundColor: "transparent",
    borderWidth: 2,
    pointRadius: 0,
    tension: 0.2,
    spanGaps: true
  }));
  const config = {
    type: "line",
    data: {
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: providers.length > 1, position: "bottom" } },
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

const renderExpectationSummary = (event, summary, horizonKey) => {
  if (!elements.expectationText || !elements.expectationMetrics) return;
  if (!summary) {
    elements.expectationText.textContent = "No distribution data available for this event/horizon.";
    elements.expectationMetrics.innerHTML = "";
    return;
  }

  const typeKey = String(event?.type || "").toLowerCase();
  const announced = toNumber(event?.value?.actual);
  const announcementLabel =
    (typeKey === "adp" || typeKey === "revelio") && Number.isFinite(announced) ? formatJobsCompact(announced) : "—";

  const repricingText = Number.isFinite(summary.repricing) ? formatJobsCompact(summary.repricing) : "—";
  const sentence = `Market expected ${formatJobsLevelCompact(summary.exp0)} before the release and repriced to ${formatJobsLevelCompact(summary.exp1)} by ${horizonKey} (${repricingText}).`;
  elements.expectationText.textContent = sentence;

  const metrics = [
    { label: "Expected (before)", value: formatJobsLevelCompact(summary.exp0) },
    ...(typeKey === "adp" || typeKey === "revelio"
      ? [{ label: "Announced change", value: announcementLabel }]
      : []),
    { label: `Expected (${horizonKey})`, value: formatJobsLevelCompact(summary.exp1) },
    { label: "Repricing", value: repricingText },
    { label: "Top bracket (before)", value: `${summary.top0.label} (${(summary.top0.p * 100).toFixed(0)}%)` },
    { label: `Top bracket (${horizonKey})`, value: `${summary.top1.label} (${(summary.top1.p * 100).toFixed(0)}%)` },
    { label: "Concentration (top 2, before)", value: `${(summary.conf0 * 100).toFixed(0)}%` },
    { label: `Concentration (top 2, ${horizonKey})`, value: `${(summary.conf1 * 100).toFixed(0)}%` }
  ];
  elements.expectationMetrics.innerHTML = metrics
    .map(({ label, value }) => `<div><p class="detail-label">${label}</p><p>${value}</p></div>`)
    .join("");
};

const renderExpectationSummaryMulti = (event, summariesByProvider, horizonKey) => {
  if (!elements.expectationText || !elements.expectationMetrics) return;
  const providers = Object.keys(summariesByProvider || {});
  const hasAny = providers.some((p) => summariesByProvider?.[p] && Number.isFinite(toNumber(summariesByProvider[p].exp0)));
  if (!hasAny) {
    elements.expectationText.textContent = "No distribution data available for this event/horizon.";
    elements.expectationMetrics.innerHTML = "";
    return;
  }

  const typeKey = String(event?.type || "").toLowerCase();
  const announced = toNumber(event?.value?.actual);
  const announcedLabel =
    (typeKey === "adp" || typeKey === "revelio") && Number.isFinite(announced) ? formatJobsCompact(announced) : null;

  const parts = [];
  for (const p of providers) {
    const s = summariesByProvider?.[p];
    if (!s) continue;
    const label = p === "kalshi" ? "Kalshi" : "Polymarket";
    parts.push(`${label}: ${formatJobsLevelCompact(s.exp0)} → ${formatJobsLevelCompact(s.exp1)} (${formatJobsCompact(s.repricing)})`);
  }
  elements.expectationText.textContent = parts.join(" | ");

  const items = [];
  if (announcedLabel) items.push({ label: "Announced change", value: announcedLabel });
  for (const p of providers) {
    const s = summariesByProvider?.[p];
    if (!s) continue;
    const label = p === "kalshi" ? "Kalshi" : "Polymarket";
    items.push({ label: `${label} expected (before)`, value: formatJobsLevelCompact(s.exp0) });
    items.push({ label: `${label} expected (${horizonKey})`, value: formatJobsLevelCompact(s.exp1) });
    items.push({ label: `${label} repricing`, value: formatJobsCompact(s.repricing) });
  }
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

  const current = elements.horizonSelect.value || "30m";
  elements.horizonSelect.innerHTML = minutes
    .map((m) => `<option value="${m}m">${formatHorizonOptionLabel(m)}</option>`)
    .join("");

  const preferred =
    (current && minutes.some((m) => `${m}m` === String(current)) && current) ||
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

const getSourceSummary = (sourceKey, horizonKey) => {
  const summary = state.data?.summary?.sources?.[sourceKey];
  if (summary) return summary;
  return computeSourceSummaryFallback(getEvents(), sourceKey, horizonKey);
};

const renderSourceSummary = (sourceKey, horizonKey) => {
  if (!elements.summaryMetrics) return;
  const summary = getSourceSummary(sourceKey, horizonKey);
  const horizon = summary?.horizons?.[horizonKey] || summary?.horizons?.[String(horizonKey)] || null;

  const meanDelta =
    toNumber(horizon?.avg_event_mean_delta_pp) ?? toNumber(horizon?.mean_delta_pp) ?? toNumber(horizon?.avg_event_mean_delta);
  const meanAbs =
    toNumber(horizon?.avg_event_mean_abs_delta_pp) ??
    toNumber(horizon?.mean_abs_delta_pp) ??
    toNumber(horizon?.avg_event_mean_abs_delta);

  const volMedian =
    toNumber(summary?.volume_rate_ratio_total?.median) ?? toNumber(summary?.volume?.median_market_rate_ratio);
  const stabMedian = toNumber(summary?.stabilization_minutes?.median);
  const corrR = toNumber(summary?.surprise_delta_corr?.pearson_r);
  const corrN = toNumber(summary?.surprise_delta_corr?.n);

  renderSummaryMetrics([
    { label: "Events", value: formatInt(toNumber(summary?.event_count) ?? 0) },
    { label: `Avg Δ Yes (${horizonKey})`, value: Number.isFinite(meanDelta) ? formatPct(meanDelta) : "—" },
    { label: `Avg |Δ Yes| (${horizonKey})`, value: Number.isFinite(meanAbs) ? formatPct(meanAbs) : "—" },
    { label: "Median vol rate (post/pre)", value: Number.isFinite(volMedian) ? formatRatio(volMedian) : "—" },
    { label: "Median stabilize (min)", value: Number.isFinite(stabMedian) ? formatInt(stabMedian) : "—" },
    { label: "Surprise ↔ Δ corr", value: Number.isFinite(corrR) ? formatCorr(corrR, corrN) : "—" }
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
    const announced = toNumber(event?.value?.actual);
    const surpriseAnnounced =
      (typeKey === "adp" || typeKey === "revelio") && Number.isFinite(announced) && Number.isFinite(exp0) ? announced - exp0 : null;
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
    const announced = toNumber(event?.value?.actual);
    if (!(typeKey === "adp" || typeKey === "revelio") || !Number.isFinite(announced)) continue;

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

const renderAlignmentSummary = (sourceKey, horizonKey, events, providers) => {
  if (!elements.alignmentMetrics) return;
  const inputEvents = Array.isArray(events) ? events : getEvents();
  const chosenProviders = Array.isArray(providers) && providers.length ? providers : [getProviderKey()];
  const fmtShare = (value) => (Number.isFinite(toNumber(value)) ? `${(toNumber(value) * 100).toFixed(0)}%` : "—");

  const combined = computeAlignmentSummary(inputEvents, sourceKey, horizonKey);
  if (!combined) return;

  const items = [];
  if (chosenProviders.length > 1) {
    for (const p of chosenProviders) {
      const subset = inputEvents.filter((e) => e.__provider === p);
      const s = computeAlignmentSummary(subset, sourceKey, horizonKey);
      if (!s) continue;
      const label = p === "kalshi" ? "Kalshi" : "Polymarket";
      items.push({ label: `${label} aligned share`, value: `${fmtShare(s.share)} (n=${formatInt(toNumber(s.n) ?? 0)})` });
    }
    items.push({ label: "Combined aligned share", value: `${fmtShare(combined.share)} (n=${formatInt(toNumber(combined.n) ?? 0)})` });
    items.push({
      label: `Combined avg repricing in announcement direction (${horizonKey})`,
      value: Number.isFinite(toNumber(combined.avg_signed_repricing)) ? formatJobsCompact(toNumber(combined.avg_signed_repricing)) : "—"
    });
  } else {
    items.push({ label: "Events with signal", value: formatInt(toNumber(combined.n) ?? 0) });
    items.push({ label: "Aligned", value: formatInt(toNumber(combined.aligned) ?? 0) });
    items.push({ label: "Aligned share", value: fmtShare(combined.share) });
    items.push({
      label: `Avg repricing in announcement direction (${horizonKey})`,
      value: Number.isFinite(toNumber(combined.avg_signed_repricing)) ? formatJobsCompact(toNumber(combined.avg_signed_repricing)) : "—"
    });
  }
  elements.alignmentMetrics.innerHTML = items
    .map(({ label, value }) => `<div><p class="detail-label">${label}</p><p>${value}</p></div>`)
    .join("");
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

  const ticker = elements.strikeSelect?.value;
  const horizonKey = elements.horizonSelect?.value || "30m";
  const sourceKey = elements.sourceSelect?.value || "all";
  const markets = safeArray(event.markets);

  const ts0 = toNumber(event.release_ts);
  const minutes = horizonMinutes(horizonKey);
  const horizonTs = Number.isFinite(ts0) && Number.isFinite(minutes) ? ts0 + minutes * 60 : null;

  const value = event.value || null;
  const actual = toNumber(value?.actual);
  const expected = toNumber(value?.expected);
  const surprise = Number.isFinite(actual) && Number.isFinite(expected) ? actual - expected : null;
  const typeKey = String(event?.type || "").toLowerCase();
  const actualLabel = typeKey === "adp" || typeKey === "revelio" ? "Announced" : "Actual";
  const showAnnouncedPrimary = (typeKey === "adp" || typeKey === "revelio") && Number.isFinite(actual);
  const primaryMetric = showAnnouncedPrimary
    ? { label: "Announced change", value: formatJobsCompact(actual) }
    : { label: "Event", value: buildEventLabel(event) };

  const meta = providerMeta();
  const preMinutes = toNumber(meta?.window?.pre_minutes) ?? 30;
  const postMinutes = toNumber(meta?.window?.post_minutes) ?? 240;

  const expectationSummary = computeExpectationSummary(event, markets, ts0, horizonTs);
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

  if (summaryProviders.length > 1) {
    renderExpectationSummaryMulti(event, summariesByProvider, horizonKey);
  } else {
    const only = summaryProviders[0];
    renderExpectationSummary(event, summariesByProvider[only] || null, horizonKey);
  }
  renderExpectationChart(seriesByProvider);

  const combinedEvents = summaryProviders
    .map((p, idx) => ({ p, dataset: datasets[idx] }))
    .filter((row) => row.dataset)
    .flatMap(({ p, dataset }) => getEventsForDataset(dataset).map((evt) => ({ ...evt, __provider: p })));
  renderHistoryTable(combinedEvents, sourceKey, horizonKey, summaryProviders.length > 1);
  renderAlignmentSummary(sourceKey, horizonKey, combinedEvents, summaryProviders);

  if (String(ticker) === "__AGG__") {
    const agg = computeAggregateAlignedSeries(event, markets, ts0, preMinutes, postMinutes, 50000);
    const aggSeries = safeArray(agg?.series);
    const aggLevel =
      horizonTs != null
        ? toNumber(aggSeries.find((row) => Number.isFinite(toNumber(row?.[0])) && toNumber(row[0]) >= horizonTs)?.[1])
        : null;

    renderMetrics([
      primaryMetric,
      { label: `Aggregate move (${horizonKey})`, value: Number.isFinite(aggLevel) ? formatPct(aggLevel) : "—" },
      ...(showAnnouncedPrimary ? [] : [{ label: actualLabel, value: Number.isFinite(actual) ? formatInt(actual) : "—" }]),
      { label: "Expected", value: Number.isFinite(expected) ? formatInt(expected) : "—" },
      { label: "Surprise", value: Number.isFinite(surprise) ? formatInt(surprise) : "—" }
    ]);

    renderAggregateChart("Aggregate (announcement-weighted)", aggSeries, ts0);

    const barMarkets = sortMarketsByStrike(markets).filter((m) => Number.isFinite(toNumber(m?.strike?.value)));
    const barMode = elements.barModeSelect?.value || "distribution";
    if (barMode === "distribution" && expectationSummary?.buckets?.length) {
      const labels = expectationSummary.buckets.map((b) => b.label);
      const beforeVals = expectationSummary.buckets.map((b) => (Number.isFinite(b.p0) ? b.p0 * 100 : null));
      const afterVals = expectationSummary.buckets.map((b) => (Number.isFinite(b.p1) ? b.p1 * 100 : null));
      renderDistributionBarChart(labels, beforeVals, afterVals, horizonKey);
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
    renderSourceSummary(sourceKey, horizonKey);
    setStatus(`Loaded aggregate view (${barMarkets.length} strikes).`);
    return;
  }

  const market = markets.find((m) => String(m?.ticker) === String(ticker)) || markets[0] || null;
  const candles = safeArray(market?.candles);
  const candlePoints = candles.filter((c) => Number.isFinite(toNumber(c?.[0])) && Number.isFinite(toNumber(c?.[1])));
  const baseline = pickBaseline(candles, ts0);
  const level = horizonTs != null ? pickValueAtOrAfter(candles, horizonTs) : null;
  const delta =
    baseline && level && Number.isFinite(baseline.yes) && Number.isFinite(level.yes)
      ? level.yes - baseline.yes
      : null;

  renderMetrics([
    primaryMetric,
    { label: "Strike", value: market ? getMarketStrikeLabel(market) : "—" },
    { label: `Δ Yes (${horizonKey})`, value: Number.isFinite(delta) ? formatPct(delta) : "—" },
    { label: "Baseline", value: baseline ? formatProb(baseline.yes) : "—" },
    { label: horizonKey, value: level ? formatProb(level.yes) : "—" },
    ...(showAnnouncedPrimary ? [] : [{ label: actualLabel, value: Number.isFinite(actual) ? formatInt(actual) : "—" }]),
    { label: "Expected", value: Number.isFinite(expected) ? formatInt(expected) : "—" },
    { label: "Surprise", value: Number.isFinite(surprise) ? formatInt(surprise) : "—" }
  ]);

  const label = `${getMarketStrikeLabel(market)} · ${String(event.type || "").toUpperCase()}`;
  renderLineChart(label, candles, ts0);

  const barMode = elements.barModeSelect?.value || "distribution";
  if (barMode === "delta") {
    const barLabels = sortMarketsByStrike(markets).map((m) => getMarketStrikeLabel(m));
    const barValues = sortMarketsByStrike(markets).map((m) => {
      const summaryDelta = toNumber(m?.summary?.deltas?.[horizonKey]);
      if (Number.isFinite(summaryDelta)) return summaryDelta;
      const series = safeArray(m?.candles);
      const base = pickBaseline(series, ts0);
      const horizon = horizonTs != null ? pickValueAtOrAfter(series, horizonTs) : null;
      return base && horizon ? horizon.yes - base.yes : null;
    });
    renderBarChart(barLabels, barValues);
  } else if (expectationSummary?.buckets?.length) {
    const labels = expectationSummary.buckets.map((b) => b.label);
    const beforeVals = expectationSummary.buckets.map((b) => (Number.isFinite(b.p0) ? b.p0 * 100 : null));
    const afterVals = expectationSummary.buckets.map((b) => (Number.isFinite(b.p1) ? b.p1 * 100 : null));
    renderDistributionBarChart(labels, beforeVals, afterVals, horizonKey);
  }

  renderTable(event, horizonKey);
  renderSourceSummary(sourceKey, horizonKey);
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
  if (elements.providerSelect) {
    const stored = localStorage.getItem(PROVIDER_KEY);
    if (stored && (stored === "kalshi" || stored === "polymarket")) {
      elements.providerSelect.value = stored;
    }
    state.provider = getProviderKey();
    elements.providerSelect.addEventListener("change", async () => {
      localStorage.setItem(PROVIDER_KEY, getProviderKey());
      state.loaded = false;
      state.data = null;
      await ensureLoaded();
      fillHorizonOptions();
      fillEventOptions();
      void updateView();
    });
  }
  if (elements.summaryProviderSelect) {
    const stored = localStorage.getItem(SUMMARY_PROVIDER_KEY);
    if (stored && (stored === "same" || stored === "kalshi" || stored === "polymarket" || stored === "both")) {
      elements.summaryProviderSelect.value = stored;
    }
    elements.summaryProviderSelect.addEventListener("change", () => {
      localStorage.setItem(SUMMARY_PROVIDER_KEY, getSummaryProviderKey());
      void updateView();
    });
  }
  if (elements.sectionSelect) {
    elements.sectionSelect.addEventListener("change", () => {
      const targetId = elements.sectionSelect.value;
      if (!targetId) return;
      localStorage.setItem(TOP_SECTION_KEY, targetId);
      applyTopSection(targetId);
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
};

const init = async () => {
  initImpactInfoPopover();
  attachEvents();
  await ensureLoaded();
  fillHorizonOptions();
  fillEventOptions();
  applyTopSection(localStorage.getItem(TOP_SECTION_KEY) || "");
  void updateView();
};

init();
