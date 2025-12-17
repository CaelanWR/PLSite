const byId = (id) => document.getElementById(id);

const elements = {
  sectorSelect: byId("altSectorSelect"),
  seasonalitySelect: byId("altSeasonalitySelect"),
  transformSelect: byId("altTransformSelect"),
  rangeSelect: byId("altRangeSelect"),
  refreshButton: byId("altRefresh"),
  hint: byId("altHint"),
  status: byId("altStatus"),
  metrics: byId("altMetrics"),
  accuracy: byId("altAccuracy"),
  canvas: byId("altChart"),
  gapCanvas: byId("altGapChart"),

  revMetricSelect: byId("altRevMetricSelect"),
  revRangeSelect: byId("altRevRangeSelect"),
  revRefreshButton: byId("altRevRefresh"),
  revStatus: byId("altRevStatus"),
  revMetrics: byId("altRevMetrics"),
  revTableHead: byId("altRevTableHead"),
  revTableBody: byId("altRevTableBody"),
  revCanvas: byId("altRevChart"),
  revGapCanvas: byId("altRevGapChart")
};

const state = {
  employment: null,
  revisions: null,
  loaded: false,
  loading: false,
  seasonalityForced: false,
  lastTotalSeasonality: "sa",
  chart: null,
  gapChart: null,
  revChart: null,
  revGapChart: null
};

const parseMonth = (value) => {
  const match = String(value ?? "").trim().match(/^(\d{4})-(\d{2})/);
  if (!match) return null;
  const year = Number(match[1]);
  const monthIndex = Number(match[2]) - 1;
  if (!Number.isFinite(year) || !Number.isFinite(monthIndex)) return null;
  return new Date(Date.UTC(year, monthIndex, 1));
};

const toNumber = (value) => {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" && value.trim() === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const formatPeople = (value) => {
  if (!Number.isFinite(value)) return "—";
  return Math.round(value).toLocaleString();
};

const formatDelta = (value) => {
  if (!Number.isFinite(value)) return "—";
  const rounded = Math.round(value);
  const sign = rounded > 0 ? "+" : "";
  return `${sign}${rounded.toLocaleString()}`;
};

const formatPct = (value) => {
  if (!Number.isFinite(value)) return "—";
  return `${value.toFixed(1)}%`;
};

const pctDiff = (a, b) => {
  if (!Number.isFinite(a) || !Number.isFinite(b) || b === 0) return null;
  return ((a - b) / b) * 100;
};

const segmentColor = (ctx) => {
  const y0 = ctx?.p0?.parsed?.y;
  const y1 = ctx?.p1?.parsed?.y;
  const y = Number.isFinite(y1) ? y1 : y0;
  if (!Number.isFinite(y)) return "#94a3b8";
  return y >= 0 ? "#16a34a" : "#dc2626";
};

const setStatus = (message, isError = false) => {
  if (!elements.status) return;
  elements.status.textContent = message;
  elements.status.classList.toggle("error", Boolean(isError));
};

const setRevStatus = (message, isError = false) => {
  if (!elements.revStatus) return;
  elements.revStatus.textContent = message;
  elements.revStatus.classList.toggle("error", Boolean(isError));
};

const initInfoPopover = () => {
  const wrap = byId("altInfo");
  const btn = byId("altInfoBtn");
  if (!wrap || !btn) return;

  const setOpen = (open) => {
    wrap.classList.toggle("show", Boolean(open));
    btn.setAttribute("aria-expanded", open ? "true" : "false");
  };

  btn.addEventListener("click", (event) => {
    event.preventDefault();
    setOpen(!wrap.classList.contains("show"));
  });

  document.addEventListener("click", (event) => {
    if (!wrap.classList.contains("show")) return;
    if (wrap.contains(event.target)) return;
    setOpen(false);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    if (!wrap.classList.contains("show")) return;
    setOpen(false);
  });
};

const setHint = (message) => {
  if (!elements.hint) return;
  elements.hint.textContent = message;
};

const fetchJSONLocal = async (path) => {
  const response = await fetch(path, { cache: "no-cache" });
  if (!response.ok) {
    throw new Error(`${path}: HTTP ${response.status}`);
  }
  return await response.json();
};

const sliceMonthSeries = (series, rangeKey) => {
  if (!Array.isArray(series) || !series.length) return [];
  if (!rangeKey || rangeKey === "max") return series;
  const now = series[series.length - 1].date;
  const start = new Date(now);
  if (rangeKey.endsWith("y")) {
    start.setUTCFullYear(start.getUTCFullYear() - Number(rangeKey.replace("y", "")));
  } else if (rangeKey.endsWith("m")) {
    start.setUTCMonth(start.getUTCMonth() - Number(rangeKey.replace("m", "")));
  } else {
    return series;
  }
  return series.filter((p) => p.date >= start);
};

const resolveName = (row, code) => {
  const candidates = [
    row.naics2d_name,
    row.naics2d_name_revelio,
    row.naics2d_name_x,
    row.naics2d_name_y
  ]
    .map((v) => String(v ?? "").trim())
    .filter(Boolean);
  if (candidates.length) return candidates[0];
  if (code === "00") return "Total private (ADP comparable)";
  if (code === "NF") return "Total nonfarm (Revelio published)";
  return code;
};

const getField = (row, key) => {
  if (row && Object.prototype.hasOwnProperty.call(row, key)) return row[key];
  const fallback = key
    .replace(/_revelio$/, "_x")
    .replace(/_bls$/, "_y");
  if (row && Object.prototype.hasOwnProperty.call(row, fallback)) return row[fallback];
  return null;
};

const prepareEmployment = (raw) => {
  const byCode = new Map();
  const meta = new Map();

  raw.forEach((row) => {
    const code = String(row.naics2d_code ?? "").trim();
    const month = String(row.month ?? "").trim();
    if (!code || !month) return;
    const date = parseMonth(month);
    if (!date) return;

    if (!meta.has(code)) {
      meta.set(code, { code, name: resolveName(row, code) });
    }

    const point = {
      month,
      date,
      sa: {
        bls: toNumber(getField(row, "employment_sa_bls")),
        revelio: toNumber(getField(row, "employment_sa_revelio")),
        adp: toNumber(getField(row, "employment_sa_adp"))
      },
      nsa: {
        bls: toNumber(getField(row, "employment_nsa_bls")),
        revelio: toNumber(getField(row, "employment_nsa_revelio")),
        adp: toNumber(getField(row, "employment_nsa_adp"))
      }
    };

    if (!byCode.has(code)) byCode.set(code, []);
    byCode.get(code).push(point);
  });

  const sectors = Array.from(meta.values()).map((sector) => {
    const points = (byCode.get(sector.code) || []).slice().sort((a, b) => a.date - b.date);
    const hasBLS = points.some((p) => Number.isFinite(p.sa.bls) || Number.isFinite(p.nsa.bls));
    const hasRevelio = points.some((p) => Number.isFinite(p.sa.revelio) || Number.isFinite(p.nsa.revelio));
    const hasADP = points.some((p) => Number.isFinite(p.sa.adp) || Number.isFinite(p.nsa.adp));
    return { ...sector, points, hasBLS, hasRevelio, hasADP };
  });

  const sortKey = (sector) => {
    if (sector.code === "00") return `0-00-${sector.name}`;
    if (sector.code === "NF") return `0-01-${sector.name}`;
    return `1-${sector.name}`;
  };
  sectors.sort((a, b) => sortKey(a).localeCompare(sortKey(b)));

  return { sectors, byCode };
};

const prepareRevisions = (raw) => {
  const byMonth = new Map();
  raw.forEach((row) => {
    const month = String(row.month ?? "").trim();
    const release = String(row.release ?? "").trim();
    if (!month || !release) return;
    const date = parseMonth(month);
    if (!date) return;
    const bls = toNumber(row.employment_sa_bls);
    const revelio = toNumber(row.employment_sa_revelio);
    if (!byMonth.has(month)) byMonth.set(month, []);
    byMonth.get(month).push({ month, date, release, bls, revelio });
  });

  const series = [];
  byMonth.forEach((rows, month) => {
    const sorted = rows.slice().sort((a, b) => a.release.localeCompare(b.release));
    const first = sorted[0];
    const last = sorted[sorted.length - 1];
    series.push({
      month,
      date: first.date,
      firstRelease: first.release,
      lastRelease: last.release,
      initial: { bls: first.bls, revelio: first.revelio },
      final: { bls: last.bls, revelio: last.revelio },
      revision: {
        bls: Number.isFinite(last.bls) && Number.isFinite(first.bls) ? last.bls - first.bls : null,
        revelio:
          Number.isFinite(last.revelio) && Number.isFinite(first.revelio) ? last.revelio - first.revelio : null
      }
    });
  });
  series.sort((a, b) => a.date - b.date);
  return { series };
};

const ensureLoaded = async () => {
  if (state.loaded || state.loading) return;
  state.loading = true;
  setStatus("Loading comparison datasets…");
  setRevStatus("Loading revision dataset…");
  try {
    const [employmentRaw, revisionsRaw] = await Promise.all([
      fetchJSONLocal("data/bls_vs_revelio_employment.json"),
      fetchJSONLocal("data/bls_vs_revelio_revisions.json")
    ]);
    state.employment = prepareEmployment(employmentRaw);
    state.revisions = prepareRevisions(revisionsRaw);
    state.loaded = true;
    setStatus("Loaded datasets.");
    setRevStatus("Loaded dataset.");
  } catch (err) {
    setStatus(
      `Unable to load datasets. Re-run fetch script to generate JSON. (${err.message})`,
      true
    );
    setRevStatus(
      `Unable to load revisions dataset. (${err.message})`,
      true
    );
  } finally {
    state.loading = false;
  }
};

const fillSectorOptions = () => {
  if (!elements.sectorSelect || !state.employment) return;
  const options = state.employment.sectors
    .filter((s) => s.hasBLS || s.hasRevelio || s.hasADP)
    .map((s) => `<option value="${s.code}">${s.name}</option>`)
    .join("");
  elements.sectorSelect.innerHTML = options;

  const preferred =
    state.employment.sectors.find((s) => s.code === "00")?.code ||
    state.employment.sectors.find((s) => s.code === "NF")?.code ||
    state.employment.sectors[0]?.code ||
    null;
  if (preferred) elements.sectorSelect.value = preferred;
};

const makeLine = (label, values, color, dashed = false) => ({
  label,
  data: values,
  borderColor: color,
  backgroundColor: "transparent",
  borderWidth: 2,
  pointRadius: 0,
  tension: 0.2,
  spanGaps: true,
  borderDash: dashed ? [6, 4] : undefined
});

const transformSeries = (points, accessor, transform) => {
  const out = [];
  const basePoint = transform === "index" ? points.find((p) => Number.isFinite(accessor(p))) || null : null;
  const baseValue = basePoint ? accessor(basePoint) : null;
  for (let i = 0; i < points.length; i += 1) {
    const current = accessor(points[i]);
    if (!Number.isFinite(current)) {
      out.push(null);
      continue;
    }
    if (transform === "level") {
      out.push(current);
      continue;
    }
    if (transform === "index") {
      out.push(Number.isFinite(baseValue) && baseValue !== 0 ? (current / baseValue) * 100 : null);
      continue;
    }
    if (transform === "mom" || transform === "momPct") {
      const prev = i > 0 ? accessor(points[i - 1]) : null;
      if (!Number.isFinite(prev)) {
        out.push(null);
      } else if (transform === "mom") {
        out.push(current - prev);
      } else {
        out.push(prev !== 0 ? ((current / prev) - 1) * 100 : null);
      }
      continue;
    }
    if (transform === "yoy" || transform === "yoyPct") {
      const prev = i >= 12 ? accessor(points[i - 12]) : null;
      if (!Number.isFinite(prev)) {
        out.push(null);
      } else if (transform === "yoy") {
        out.push(current - prev);
      } else {
        out.push(prev !== 0 ? ((current / prev) - 1) * 100 : null);
      }
      continue;
    }
    out.push(current);
  }
  return out;
};

const renderMetrics = (items) => {
  if (!elements.metrics) return;
  elements.metrics.innerHTML = items
    .map(
      ({ label, value }) =>
        `<div><p class="detail-label">${label}</p><p>${value}</p></div>`
    )
    .join("");
};

const mean = (values) => {
  if (!values.length) return null;
  return values.reduce((sum, v) => sum + v, 0) / values.length;
};

const pearsonCorrelation = (xs, ys) => {
  if (xs.length < 2) return null;
  const mx = mean(xs);
  const my = mean(ys);
  if (!Number.isFinite(mx) || !Number.isFinite(my)) return null;
  let cov = 0;
  let vx = 0;
  let vy = 0;
  for (let i = 0; i < xs.length; i += 1) {
    const dx = xs[i] - mx;
    const dy = ys[i] - my;
    cov += dx * dy;
    vx += dx * dx;
    vy += dy * dy;
  }
  if (vx === 0 || vy === 0) return null;
  return cov / Math.sqrt(vx * vy);
};

const computeAccuracyStats = (labels, baseline, candidate, { allowSignMatch = false } = {}) => {
  const xs = [];
  const ys = [];
  const months = [];
  for (let i = 0; i < baseline.length; i += 1) {
    const x = baseline[i];
    const y = candidate[i];
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
    xs.push(x);
    ys.push(y);
    months.push(labels[i]);
  }
  if (!xs.length) return null;

  const errors = xs.map((x, i) => ys[i] - x);
  const absErrors = errors.map((e) => Math.abs(e));
  const mae = mean(absErrors);
  const bias = mean(errors);
  const rmse = Math.sqrt(mean(errors.map((e) => e * e)) ?? 0);
  const corr = pearsonCorrelation(xs, ys);

  let signMatch = null;
  if (allowSignMatch) {
    let count = 0;
    let match = 0;
    for (let i = 0; i < xs.length; i += 1) {
      const sx = Math.sign(xs[i]);
      const sy = Math.sign(ys[i]);
      if (!Number.isFinite(sx) || !Number.isFinite(sy)) continue;
      count += 1;
      if (sx === sy) match += 1;
    }
    signMatch = count ? (match / count) * 100 : null;
  }

  let avgAbsPctGap = null;
  // Useful only for level comparisons.
  if (xs.every((v) => v > 0)) {
    const gaps = xs.map((x, i) => ((ys[i] - x) / x) * 100).filter((v) => Number.isFinite(v));
    avgAbsPctGap = mean(gaps.map((v) => Math.abs(v)));
  }

  return {
    n: xs.length,
    months,
    mae,
    bias,
    rmse,
    corr,
    signMatch,
    avgAbsPctGap
  };
};

const formatStat = (value, { unit, signed = false } = {}) => {
  if (!Number.isFinite(value)) return "—";
  if (unit === "Percent") return `${value.toFixed(2)}${signed ? "pp" : "%"}`;
  if (unit === "Index") return value.toFixed(2);
  return signed ? formatDelta(value) : formatPeople(value);
};

const renderAccuracy = ({ unitLabel, transformLabel, rangeLabel, stats }) => {
  if (!elements.accuracy) return;
  const cards = [];
  const sources = [
    { key: "revelio", title: "Revelio vs BLS", tone: "good" },
    { key: "adp", title: "ADP vs BLS", tone: "neutral" }
  ];

  sources.forEach(({ key, title, tone }) => {
    const s = stats[key];
    if (!s) return;
    const start = s.months[0];
    const end = s.months[s.months.length - 1];
    const allowPctGap = transformLabel === "Level";
    const allowSignMatch = transformLabel.includes("MoM") || transformLabel.includes("YoY");
    cards.push(`<section class="accuracy-card accuracy-card-${tone}">
        <header>
          <h3>${title}</h3>
          <p>${start} → ${end} • ${s.n.toLocaleString()} months • ${rangeLabel}</p>
        </header>
        <div class="accuracy-stats">
          <div><p class="detail-label">Correlation</p><p>${Number.isFinite(s.corr) ? s.corr.toFixed(2) : "—"}</p></div>
          <div><p class="detail-label">MAE</p><p>${formatStat(s.mae, { unit: unitLabel })}</p></div>
          <div><p class="detail-label">RMSE</p><p>${formatStat(s.rmse, { unit: unitLabel })}</p></div>
          <div><p class="detail-label">Bias</p><p>${formatStat(s.bias, { unit: unitLabel, signed: true })}</p></div>
          ${
            allowPctGap
              ? `<div><p class="detail-label">Avg |% gap|</p><p>${Number.isFinite(s.avgAbsPctGap) ? s.avgAbsPctGap.toFixed(2) + "%" : "—"}</p></div>`
              : ""
          }
          ${
            allowSignMatch
              ? `<div><p class="detail-label">Direction match</p><p>${Number.isFinite(s.signMatch) ? s.signMatch.toFixed(0) + "%" : "—"}</p></div>`
              : ""
          }
        </div>
      </section>`);
  });

  if (!cards.length) {
    elements.accuracy.innerHTML = `<section class="accuracy-card"><header><h3>Accuracy</h3><p>No overlapping months with BLS for this selection.</p></header></section>`;
    return;
  }
  elements.accuracy.innerHTML = cards.join("");
};

const updateEmploymentView = () => {
  if (!state.employment || !elements.sectorSelect) return;
  const sectorCode = elements.sectorSelect.value;
  const sector = state.employment.sectors.find((s) => s.code === sectorCode) || null;
  if (!sector) return;

  const isTotal = sector.code === "00" || sector.code === "NF";
  const rangeKey = elements.rangeSelect?.value || "3y";
  const transform = elements.transformSelect?.value || "level";
  const baseline = "bls";

  let seasonality = elements.seasonalitySelect?.value || "sa";
  if (!isTotal) {
    if (!state.seasonalityForced) {
      state.lastTotalSeasonality = seasonality;
    }
    seasonality = "nsa";
    if (elements.seasonalitySelect) elements.seasonalitySelect.value = "nsa";
    if (elements.seasonalitySelect) elements.seasonalitySelect.disabled = true;
    state.seasonalityForced = true;
    setHint("Selecting a sector switches to NSA automatically (SA is only available for totals).");
  } else {
    if (elements.seasonalitySelect) elements.seasonalitySelect.disabled = false;
    if (state.seasonalityForced) {
      seasonality = state.lastTotalSeasonality || "sa";
      if (elements.seasonalitySelect) elements.seasonalitySelect.value = seasonality;
      state.seasonalityForced = false;
    }
    setHint("Totals: Total private is ADP comparable; Total nonfarm uses Revelio published totals when available.");
  }

  if (elements.seasonalitySelect && !isTotal) {
    elements.seasonalitySelect.value = "nsa";
  }

  const windowed = sliceMonthSeries(sector.points, rangeKey);
  const labels = windowed.map((p) => p.month);
  const latest = windowed[windowed.length - 1] || null;

  const available = {
    bls: windowed.some((p) => Number.isFinite(p.sa.bls) || Number.isFinite(p.nsa.bls)),
    revelio: windowed.some((p) => Number.isFinite(p.sa.revelio) || Number.isFinite(p.nsa.revelio)),
    adp: windowed.some((p) => Number.isFinite(p.sa.adp) || Number.isFinite(p.nsa.adp))
  };

  const seriesKey = seasonality === "nsa" ? "nsa" : "sa";
  const unitIsPct = transform.endsWith("Pct");
  const unitLabel =
    transform === "level"
      ? "Persons"
      : transform === "index"
        ? "Index"
        : unitIsPct
          ? "Percent"
          : "Persons";

  const valueFormatter =
    unitIsPct || transform === "index" ? (v) => (Number.isFinite(v) ? v.toFixed(1) : "—") : formatPeople;

  const seriesData = {
    sa: {
      bls: available.bls ? transformSeries(windowed, (p) => p.sa.bls, transform) : [],
      revelio: available.revelio ? transformSeries(windowed, (p) => p.sa.revelio, transform) : [],
      adp: available.adp ? transformSeries(windowed, (p) => p.sa.adp, transform) : []
    },
    nsa: {
      bls: available.bls ? transformSeries(windowed, (p) => p.nsa.bls, transform) : [],
      revelio: available.revelio ? transformSeries(windowed, (p) => p.nsa.revelio, transform) : [],
      adp: available.adp ? transformSeries(windowed, (p) => p.nsa.adp, transform) : []
    }
  };

  const metricRows = [
    { label: "Sector", value: sector.name },
    { label: "Transform", value: elements.transformSelect?.selectedOptions?.[0]?.textContent || transform },
    { label: "Seasonality", value: seasonality.toUpperCase() },
    { label: "Latest month", value: latest?.month || "—" }
  ];

  const addLatest = (label, arr) => metricRows.push({ label, value: valueFormatter(arr[arr.length - 1]) });
  if (available.bls) addLatest("BLS", seriesData[seriesKey].bls);
  if (available.revelio) addLatest("Revelio", seriesData[seriesKey].revelio);
  if (available.adp) addLatest("ADP", seriesData[seriesKey].adp);
  renderMetrics(metricRows);

  const datasets = [];
  if (available.bls) datasets.push(makeLine("BLS", seriesData[seriesKey].bls, "#2563eb"));
  if (available.revelio) datasets.push(makeLine("Revelio", seriesData[seriesKey].revelio, "#16a34a"));
  if (available.adp) datasets.push(makeLine("ADP", seriesData[seriesKey].adp, "#7c3aed"));

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: "bottom" } },
    interaction: { mode: "index", intersect: false },
    scales: {
      y: {
        ticks: {
          callback: (value) => {
            if (unitLabel === "Percent") return `${Number(value).toFixed(1)}%`;
            if (unitLabel === "Index") return Number(value).toFixed(1);
            return Number(value).toLocaleString();
          }
        }
      }
    }
  };

  if (elements.canvas && typeof Chart !== "undefined") {
    const ctx = elements.canvas.getContext("2d");
    const config = { type: "line", data: { labels, datasets }, options: chartOptions };
    if (!state.chart) {
      state.chart = new Chart(ctx, config);
    } else {
      state.chart.config.type = config.type;
      state.chart.data = config.data;
      state.chart.options = config.options;
      state.chart.update();
    }
  }

  const gapDatasets = [];
  gapDatasets.push({
    label: "__zero__",
    data: labels.map(() => 0),
    borderColor: "rgba(148, 163, 184, 0.9)",
    borderWidth: 3,
    pointRadius: 0,
    tension: 0,
    spanGaps: true,
    order: 1000
  });

  const addGap = (label, values, color) => {
    gapDatasets.push({
      label,
      data: values,
      borderWidth: 2,
      pointRadius: 0,
      tension: 0.2,
      spanGaps: true,
      borderColor: color,
      borderDash: undefined
    });
  };

  const baselineSeriesFor = (k) => {
    if (baseline === "bls") return seriesData[k].bls;
    if (baseline === "revelio") return seriesData[k].revelio;
    return seriesData[k].adp;
  };

  const gapSeries = (k, targetKey) => {
    const baseArr = baselineSeriesFor(k);
    const otherArr = seriesData[k][targetKey];
    return labels.map((_, i) => pctDiff(otherArr[i], baseArr[i]));
  };

  const include = (key) => key !== baseline && available[key];

  if (include("revelio")) addGap("Revelio vs BLS", gapSeries(seriesKey, "revelio"), "#16a34a");
  if (include("adp")) addGap("ADP vs BLS", gapSeries(seriesKey, "adp"), "#7c3aed");

  if (elements.gapCanvas && typeof Chart !== "undefined") {
    const ctx = elements.gapCanvas.getContext("2d");
    const config = {
      type: "line",
      data: { labels, datasets: gapDatasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: "bottom",
            labels: {
              filter: (item, chartData) => chartData.datasets[item.datasetIndex]?.label !== "__zero__"
            }
          }
        },
        interaction: { mode: "index", intersect: false },
        scales: {
          y: {
            ticks: { callback: (value) => `${Number(value).toFixed(1)}%` },
            grid: {
              color: (ctx) =>
                ctx?.tick?.value === 0 ? "rgba(148, 163, 184, 0.9)" : "rgba(226, 232, 240, 0.8)",
              lineWidth: (ctx) => (ctx?.tick?.value === 0 ? 1.5 : 1)
            }
          }
        }
      }
    };
    if (!state.gapChart) {
      state.gapChart = new Chart(ctx, config);
    } else {
      state.gapChart.config.type = config.type;
      state.gapChart.data = config.data;
      state.gapChart.options = config.options;
      state.gapChart.update();
    }
  }

  const transformLabel = elements.transformSelect?.selectedOptions?.[0]?.textContent || transform;
  const rangeLabel = elements.rangeSelect?.selectedOptions?.[0]?.textContent || rangeKey;
  const allowSignMatch = transform === "mom" || transform === "momPct" || transform === "yoy" || transform === "yoyPct";

  const stats = {
    revelio:
      available.bls && available.revelio
        ? computeAccuracyStats(labels, seriesData[seriesKey].bls, seriesData[seriesKey].revelio, {
            allowSignMatch
          })
        : null,
    adp:
      available.bls && available.adp
        ? computeAccuracyStats(labels, seriesData[seriesKey].bls, seriesData[seriesKey].adp, { allowSignMatch })
        : null
  };

  renderAccuracy({ unitLabel, transformLabel, rangeLabel, stats });

  setStatus(`Showing ${windowed.length.toLocaleString()} months • Units: ${unitLabel} • Accuracy uses overlapping months with BLS.`);
};

const renderRevMetrics = (items) => {
  if (!elements.revMetrics) return;
  elements.revMetrics.innerHTML = items
    .map(({ label, value }) => `<div><p class="detail-label">${label}</p><p>${value}</p></div>`)
    .join("");
};

const renderRevTable = (headHtml, rowsHtml) => {
  if (!elements.revTableHead || !elements.revTableBody) return;
  elements.revTableHead.innerHTML = headHtml;
  elements.revTableBody.innerHTML = rowsHtml;
};

const updateRevisionsView = () => {
  if (!state.revisions) return;
  const metric = elements.revMetricSelect?.value || "revision";
  const rangeKey = elements.revRangeSelect?.value || "3y";

  const series = sliceMonthSeries(state.revisions.series, rangeKey);
  const labels = series.map((p) => p.month);
  const latest = series[series.length - 1] || null;

  const valuesBLS = series.map((p) => (metric === "revision" ? p.revision.bls : p.final.bls));
  const valuesRevelio = series.map((p) => (metric === "revision" ? p.revision.revelio : p.final.revelio));
  const pct = series.map((p) => pctDiff(metric === "revision" ? p.revision.bls : p.final.bls, metric === "revision" ? p.revision.revelio : p.final.revelio));

  renderRevMetrics([
    { label: "Series", value: "Total nonfarm payrolls (SA)" },
    { label: "Latest month", value: latest?.month || "—" },
    { label: "First release", value: latest?.firstRelease || "—" },
    { label: "Last release", value: latest?.lastRelease || "—" },
    { label: "BLS final", value: formatPeople(latest?.final?.bls) },
    { label: "Revelio final", value: formatPeople(latest?.final?.revelio) },
    { label: "BLS Δ (final–first)", value: formatDelta(latest?.revision?.bls) },
    { label: "Revelio Δ (final–first)", value: formatDelta(latest?.revision?.revelio) }
  ]);

  if (elements.revCanvas && typeof Chart !== "undefined") {
    const ctx = elements.revCanvas.getContext("2d");
    const config = {
      type: "line",
      data: {
        labels,
        datasets: [
          makeLine(metric === "revision" ? "BLS revision (final–first)" : "BLS final level", valuesBLS, "#2563eb"),
          makeLine(metric === "revision" ? "Revelio revision (final–first)" : "Revelio final level", valuesRevelio, "#16a34a")
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { position: "bottom" } },
        interaction: { mode: "index", intersect: false },
        scales: { y: { ticks: { callback: (value) => Number(value).toLocaleString() } } }
      }
    };
    if (!state.revChart) {
      state.revChart = new Chart(ctx, config);
    } else {
      state.revChart.config.type = config.type;
      state.revChart.data = config.data;
      state.revChart.options = config.options;
      state.revChart.update();
    }
  }

  if (elements.revGapCanvas && typeof Chart !== "undefined") {
    const ctx = elements.revGapCanvas.getContext("2d");
    const config = {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "% gap (BLS–Revelio)",
            data: pct,
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.2,
            spanGaps: true,
            borderColor: "#16a34a"
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { position: "bottom" } },
        interaction: { mode: "index", intersect: false },
        scales: {
          y: {
            ticks: { callback: (value) => `${Number(value).toFixed(1)}%` },
            grid: {
              color: (ctx) =>
                ctx?.tick?.value === 0 ? "rgba(148, 163, 184, 0.9)" : "rgba(226, 232, 240, 0.8)",
              lineWidth: (ctx) => (ctx?.tick?.value === 0 ? 1.5 : 1)
            }
          }
        }
      }
    };
    if (!state.revGapChart) {
      state.revGapChart = new Chart(ctx, config);
    } else {
      state.revGapChart.config.type = config.type;
      state.revGapChart.data = config.data;
      state.revGapChart.options = config.options;
      state.revGapChart.update();
    }
  }

  const head = `<tr>
      <th>Month</th>
      <th>BLS final</th>
      <th>Revelio final</th>
      <th>BLS Δ</th>
      <th>Revelio Δ</th>
      <th>% gap (final)</th>
    </tr>`;
  const rows = series.slice(-10).reverse();
  const rowHtml =
    rows.length === 0
      ? `<tr><td colspan="6">No data available.</td></tr>`
      : rows
          .map(
            (p) => `<tr>
              <td>${p.month}</td>
              <td>${formatPeople(p.final.bls)}</td>
              <td>${formatPeople(p.final.revelio)}</td>
              <td>${formatDelta(p.revision.bls)}</td>
              <td>${formatDelta(p.revision.revelio)}</td>
              <td>${formatPct(pctDiff(p.final.bls, p.final.revelio))}</td>
            </tr>`
          )
          .join("");
  renderRevTable(head, rowHtml);
  setRevStatus(`Showing ${series.length.toLocaleString()} months.`);
};

const init = async () => {
  initInfoPopover();
  await ensureLoaded();
  if (!state.loaded) return;

  fillSectorOptions();

  const onChange = () => {
    updateEmploymentView();
    updateRevisionsView();
  };

  elements.sectorSelect?.addEventListener("change", onChange);
  elements.seasonalitySelect?.addEventListener("change", () => {
    state.seasonalityForced = false;
    const sectorCode = elements.sectorSelect?.value || "";
    const isTotal = sectorCode === "00" || sectorCode === "NF";
    if (isTotal && elements.seasonalitySelect?.value) {
      state.lastTotalSeasonality = elements.seasonalitySelect.value;
    }
    onChange();
  });
  elements.transformSelect?.addEventListener("change", onChange);
  elements.rangeSelect?.addEventListener("change", onChange);
  elements.refreshButton?.addEventListener("click", () => {
    state.loaded = false;
    state.loading = false;
    state.employment = null;
    state.revisions = null;
    ensureLoaded().then(() => {
      if (!state.loaded) return;
      fillSectorOptions();
      onChange();
    });
  });

  elements.revMetricSelect?.addEventListener("change", onChange);
  elements.revRangeSelect?.addEventListener("change", onChange);
  elements.revRefreshButton?.addEventListener("click", onChange);

  onChange();
};

init();
