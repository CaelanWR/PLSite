import { byId, toNumber, fetchJSONLocal } from "./ui_utils.js";

const DATA_PATH = "../data/market_priors.json";
const REFRESH_MS = 5 * 60 * 1000;
const STORAGE_KEY = "marketPriorsEvent";

const DEFAULT_EVENTS = [
  { id: "nfp", label: "Nonfarm Payrolls (NFP)", format: "jobs", decimals: 0 },
  { id: "unemployment", label: "Unemployment Rate", format: "percent", decimals: 1 },
  { id: "cpi", label: "CPI Inflation", format: "percent", decimals: 1 },
  { id: "fed", label: "Fed Rate Decision", format: "percent", decimals: 2 }
];

const HISTORY_PRESETS = [
  { key: "4w", label: "4 weeks ago", days: 28, color: "#9ca3af" },
  { key: "2w", label: "2 weeks ago", days: 14, color: "#6b7280" },
  { key: "1w", label: "1 week ago", days: 7, color: "#4b5563" },
  { key: "current", label: "Current", days: 0, color: "#2563eb" }
];

const elements = {
  eventSelect: byId("priorsEventSelect"),
  releaseDate: byId("priorsReleaseDate"),
  updatedAt: byId("priorsUpdatedAt"),
  refreshButton: byId("priorsRefresh"),
  status: byId("priorsStatus"),
  metrics: byId("priorsMetrics"),
  barCanvas: byId("priorsBarChart"),
  evolutionCanvas: byId("priorsEvolutionChart"),
  tableHead: byId("priorsTableHead"),
  tableBody: byId("priorsTableBody")
};

const state = {
  data: null,
  loading: false,
  barChart: null,
  evolutionChart: null,
  refreshTimer: null
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

const formatPct = (value) => {
  if (!Number.isFinite(value)) return "--";
  return `${(value * 100).toFixed(1)}%`;
};

const formatJobs = (value) => {
  if (!Number.isFinite(value)) return "--";
  const rounded = Math.round(value / 1000);
  return `${rounded.toLocaleString()}K jobs`;
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

const formatStdDev = (value, meta) => {
  if (!Number.isFinite(value)) return "--";
  if (meta.format === "jobs") return formatJobsCompact(value);
  return formatRate(value, meta.decimals);
};

const normalizeProb = (value) => {
  const n = toNumber(value);
  if (!Number.isFinite(n)) return null;
  return n > 1.5 ? n / 100 : n;
};

const normalizeDistribution = (ranges) => {
  const cleaned = safeArray(ranges)
    .map((r) => ({
      lower: toNumber(r?.lower),
      upper: toNumber(r?.upper),
      prob: normalizeProb(r?.prob),
      volume: toNumber(r?.volume),
      midpoint: toNumber(r?.midpoint)
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

const combineDistributions = (kalshi, polymarket) => {
  const map = new Map();
  const ingest = (source, ranges) => {
    for (const range of safeArray(ranges)) {
      const lower = toNumber(range?.lower);
      const upper = toNumber(range?.upper);
      const key = rangeKey({ lower, upper });
      const existing = map.get(key) || { lower, upper, kalshi: null, polymarket: null };
      existing[source] = toNumber(range?.prob);
      map.set(key, existing);
    }
  };

  ingest("kalshi", kalshi);
  ingest("polymarket", polymarket);

  const entries = Array.from(map.values()).map((entry) => {
    const sources = [entry.kalshi, entry.polymarket].filter(Number.isFinite);
    const combinedRaw = sources.length ? sources.reduce((a, b) => a + b, 0) / sources.length : null;
    return { ...entry, combinedRaw };
  });

  const combinedTotal = entries.reduce((acc, entry) => acc + (entry.combinedRaw ?? 0), 0);
  entries.forEach((entry) => {
    entry.combined = combinedTotal > 0 && entry.combinedRaw !== null ? entry.combinedRaw / combinedTotal : 0;
  });

  return entries.sort(sortRanges);
};

const defaultMetaFor = (id) =>
  DEFAULT_EVENTS.find((event) => event.id === id) || DEFAULT_EVENTS[0];

const mergeMeta = (eventData) => {
  const fallback = defaultMetaFor(eventData?.id);
  return {
    id: eventData?.id || fallback.id,
    label: eventData?.name || fallback.label,
    format: eventData?.format || fallback.format,
    decimals: Number.isFinite(eventData?.decimals) ? eventData.decimals : fallback.decimals,
    nextRelease: eventData?.next_release || eventData?.nextRelease || null,
    snapshots: safeArray(eventData?.snapshots)
  };
};

const formatValue = (value, meta) => {
  if (meta.format === "jobs") return formatJobs(value);
  return formatRate(value, meta.decimals);
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

const pickLatestSnapshot = (snapshots) => {
  const sorted = snapshots
    .map((snap) => ({ snap, ts: parseDate(snap?.as_of)?.getTime() ?? -Infinity }))
    .filter((item) => Number.isFinite(item.ts))
    .sort((a, b) => a.ts - b.ts);
  return sorted.length ? sorted[sorted.length - 1].snap : null;
};

const pickClosestSnapshot = (snapshots, targetTs) => {
  let best = null;
  let bestDiff = Infinity;
  for (const snap of snapshots) {
    const ts = parseDate(snap?.as_of)?.getTime();
    if (!Number.isFinite(ts)) continue;
    const diff = Math.abs(ts - targetTs);
    if (diff < bestDiff) {
      best = snap;
      bestDiff = diff;
    }
  }
  return best;
};

const buildCombinedSnapshot = (snapshot) => {
  const sources = snapshot?.sources || snapshot || {};
  const kalshi = normalizeDistribution(sources?.kalshi);
  const polymarket = normalizeDistribution(sources?.polymarket);
  const combined = combineDistributions(kalshi, polymarket);
  return { kalshi, polymarket, combined };
};

const computeDefaultWidth = (ranges) => {
  const widths = ranges
    .map((r) => {
      const lower = toNumber(r.lower);
      const upper = toNumber(r.upper);
      if (!Number.isFinite(lower) || !Number.isFinite(upper)) return null;
      const w = upper - lower;
      return Number.isFinite(w) && w > 0 ? w : null;
    })
    .filter((v) => Number.isFinite(v))
    .sort((a, b) => a - b);
  if (!widths.length) return 1;
  return widths[Math.floor(widths.length / 2)];
};

const midpointForRange = (range, defaultWidth) => {
  const lower = toNumber(range.lower);
  const upper = toNumber(range.upper);
  if (Number.isFinite(lower) && Number.isFinite(upper)) return (lower + upper) / 2;
  if (Number.isFinite(range.midpoint)) return range.midpoint;
  if (Number.isFinite(lower) && Number.isFinite(defaultWidth)) return lower + defaultWidth / 2;
  if (Number.isFinite(upper) && Number.isFinite(defaultWidth)) return upper - defaultWidth / 2;
  return null;
};

const computeStats = (ranges) => {
  const cleaned = safeArray(ranges).filter((r) => Number.isFinite(r.combined));
  const total = cleaned.reduce((acc, r) => acc + (r.combined ?? 0), 0);
  if (!cleaned.length || total <= 0) return null;

  const width = computeDefaultWidth(cleaned);
  const withMid = cleaned
    .map((r) => ({ ...r, midpoint: midpointForRange(r, width) }))
    .filter((r) => Number.isFinite(r.midpoint));

  if (!withMid.length) return null;

  const mean = withMid.reduce((acc, r) => acc + r.midpoint * r.combined, 0);
  const variance = withMid.reduce((acc, r) => acc + ((r.midpoint - mean) ** 2) * r.combined, 0);
  const std = Math.sqrt(Math.max(variance, 0));

  let cumulative = 0;
  let median = null;
  for (const r of cleaned) {
    cumulative += r.combined ?? 0;
    if (cumulative >= 0.5) {
      median = r;
      break;
    }
  }

  const mode = cleaned.slice().sort((a, b) => (b.combined ?? 0) - (a.combined ?? 0))[0] || null;

  return { mean, std, median, mode };
};

const setStatus = (message, isError = false) => {
  if (!elements.status) return;
  elements.status.textContent = message;
  elements.status.classList.toggle("error", Boolean(isError));
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

const renderMetrics = (stats, meta) => {
  if (!elements.metrics) return;
  if (!stats) {
    elements.metrics.innerHTML = `
      <div><p class="detail-label">Expected value (mean)</p><p>--</p></div>
      <div><p class="detail-label">Median expectation</p><p>--</p></div>
      <div><p class="detail-label">Most likely (mode)</p><p>--</p></div>
      <div><p class="detail-label">Uncertainty (std dev)</p><p>--</p></div>
    `;
    return;
  }
  const medianLabel = stats.median ? formatRangeLabel(stats.median, meta) : "--";
  const modeLabel = stats.mode ? formatRangeLabel(stats.mode, meta) : "--";
  const rows = [
    { label: "Expected value (mean)", value: formatValue(stats.mean, meta) },
    { label: "Median expectation", value: medianLabel },
    { label: "Most likely (mode)", value: modeLabel },
    { label: "Uncertainty (std dev)", value: formatStdDev(stats.std, meta) }
  ];
  elements.metrics.innerHTML = rows
    .map(({ label, value }) => `<div><p class="detail-label">${label}</p><p>${value}</p></div>`)
    .join("");
};

const renderTable = (ranges, meta) => {
  if (!elements.tableHead || !elements.tableBody) return;
  elements.tableHead.innerHTML = `
    <tr>
      <th>Outcome range</th>
      <th>Kalshi</th>
      <th>Polymarket</th>
      <th>Combined</th>
    </tr>
  `;

  if (!ranges.length) {
    elements.tableBody.innerHTML = '<tr><td colspan="4">No data available.</td></tr>';
    return;
  }

  const rows = ranges.map((range) => {
    const label = formatRangeLabel(range, meta);
    const kalshi = Number.isFinite(range.kalshi) ? formatPct(range.kalshi) : "--";
    const polymarket = Number.isFinite(range.polymarket) ? formatPct(range.polymarket) : "--";
    const combined = Number.isFinite(range.combined) ? formatPct(range.combined) : "--";
    return `<tr><td>${label}</td><td>${kalshi}</td><td>${polymarket}</td><td>${combined}</td></tr>`;
  });
  elements.tableBody.innerHTML = rows.join("");
};

const renderBarChart = (labels, values) => {
  if (!elements.barCanvas || typeof Chart === "undefined") return;
  const ctx = elements.barCanvas.getContext("2d");
  const config = {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Combined",
          data: values,
          backgroundColor: "rgba(37, 99, 235, 0.6)",
          borderColor: "rgba(37, 99, 235, 0.95)",
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

const renderEvolutionChart = (labels, datasets) => {
  if (!elements.evolutionCanvas || typeof Chart === "undefined") return;
  const ctx = elements.evolutionCanvas.getContext("2d");
  const config = {
    type: "line",
    data: {
      labels,
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: "bottom" } },
      interaction: { mode: "index", intersect: false },
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
  if (!state.evolutionChart) {
    state.evolutionChart = new Chart(ctx, config);
  } else {
    state.evolutionChart.config.type = config.type;
    state.evolutionChart.data = config.data;
    state.evolutionChart.options = config.options;
    state.evolutionChart.update();
  }
};

const renderCharts = (current, history, meta) => {
  const labels = current.map((range) => formatRangeLabel(range, meta));
  const values = current.map((range) => (range.combined ?? 0) * 100);
  renderBarChart(labels, values);

  const rangeMap = new Map(current.map((range, idx) => [rangeKey(range), idx]));
  const datasets = history.map((series) => {
    const data = labels.map(() => null);
    for (const range of series.ranges) {
      const idx = rangeMap.get(rangeKey(range));
      if (idx === undefined) continue;
      data[idx] = (range.combined ?? 0) * 100;
    }
    return {
      label: series.label,
      data,
      borderColor: series.color,
      backgroundColor: "transparent",
      borderWidth: series.key === "current" ? 3 : 2,
      pointRadius: 0,
      tension: 0.2,
      spanGaps: true
    };
  });
  renderEvolutionChart(labels, datasets);
};

const renderEvent = (eventData) => {
  const meta = mergeMeta(eventData);
  const snapshots = meta.snapshots;
  const currentSnapshot = pickLatestSnapshot(snapshots);
  if (!currentSnapshot) {
    setStatus(`No snapshots available for ${meta.label}.`, true);
    renderMetrics(null, meta);
    renderTable([], meta);
    renderBarChart([], []);
    renderEvolutionChart([], []);
    return;
  }

  const combined = buildCombinedSnapshot(currentSnapshot);
  if (!combined.kalshi.length && !combined.polymarket.length) {
    setStatus(`No bracket markets available for ${meta.label}.`, true);
    renderMetrics(null, meta);
    renderTable([], meta);
    renderBarChart([], []);
    renderEvolutionChart([], []);
    return;
  }

  const currentRanges = combined.combined;
  const stats = computeStats(currentRanges);
  renderMetrics(stats, meta);
  renderTable(currentRanges, meta);
  setReleaseDate(meta.nextRelease);

  const currentDate = parseDate(currentSnapshot?.as_of);
  const history = [];
  if (currentDate) {
    for (const preset of HISTORY_PRESETS) {
      const targetTs = currentDate.getTime() - preset.days * 24 * 60 * 60 * 1000;
      const snap = preset.key === "current" ? currentSnapshot : pickClosestSnapshot(snapshots, targetTs);
      if (!snap) continue;
      const snapshotCombined = buildCombinedSnapshot(snap).combined;
      history.push({ ...preset, ranges: snapshotCombined });
    }
  }

  renderCharts(currentRanges, history, meta);

  const missing = [];
  if (!combined.kalshi.length) missing.push("Kalshi");
  if (!combined.polymarket.length) missing.push("Polymarket");
  const missingHistory = HISTORY_PRESETS.filter((preset) => !history.find((item) => item.key === preset.key)).map(
    (preset) => preset.label
  );
  const notes = [];
  if (missing.length) notes.push(`Missing ${missing.join(" + ")} brackets.`);
  if (missingHistory.length) notes.push(`Missing history: ${missingHistory.join(", ")}.`);
  const base = missing.length
    ? `Showing available sources for ${meta.label}.`
    : `Showing combined distribution for ${meta.label}.`;
  setStatus(notes.length ? `${base} ${notes.join(" ")}` : base, false);
};

const getEventList = () => {
  const dataEvents = safeArray(state.data?.events);
  const map = new Map(dataEvents.map((event) => [event.id, event]));
  return DEFAULT_EVENTS.map((event) => map.get(event.id) || { id: event.id, name: event.label, format: event.format, decimals: event.decimals, snapshots: [] });
};

const populateEventSelect = () => {
  if (!elements.eventSelect) return;
  const events = getEventList();
  const stored = storage.get(STORAGE_KEY);
  const selected = stored || events[0]?.id;
  elements.eventSelect.innerHTML = events
    .map((event) => {
      const meta = mergeMeta(event);
      const isSelected = meta.id === selected;
      return `<option value="${meta.id}" ${isSelected ? "selected" : ""}>${meta.label}</option>`;
    })
    .join("");
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
    const data = await fetchJSONLocal(DATA_PATH);
    state.data = data;
    setUpdatedAt(data?.updated_at || data?.updatedAt || null);
    populateEventSelect();
    render();
  } catch (err) {
    setStatus("Failed to load market priors data. Check the data/market_priors.json file.", true);
  } finally {
    state.loading = false;
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

elements.eventSelect?.addEventListener("change", () => render());
elements.refreshButton?.addEventListener("click", () => loadData());

populateEventSelect();
loadData();
startAutoRefresh();
