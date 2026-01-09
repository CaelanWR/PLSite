const baseTrendsURL =
  "https://trends.google.com:443/trends/embed/explore/TIMESERIES?req=";
const FRED_OBSERVATIONS_ENDPOINT =
  "https://api.stlouisfed.org/fred/series/observations";

const states = [
  { name: "United States", code: "US", geo: "US" },
  { name: "Alabama", code: "AL", geo: "US-AL" },
  { name: "Alaska", code: "AK", geo: "US-AK" },
  { name: "Arizona", code: "AZ", geo: "US-AZ" },
  { name: "Arkansas", code: "AR", geo: "US-AR" },
  { name: "California", code: "CA", geo: "US-CA" },
  { name: "Colorado", code: "CO", geo: "US-CO" },
  { name: "Connecticut", code: "CT", geo: "US-CT" },
  { name: "Delaware", code: "DE", geo: "US-DE" },
  { name: "District of Columbia", code: "DC", geo: "US-DC" },
  { name: "Florida", code: "FL", geo: "US-FL" },
  { name: "Georgia", code: "GA", geo: "US-GA" },
  { name: "Hawaii", code: "HI", geo: "US-HI" },
  { name: "Idaho", code: "ID", geo: "US-ID" },
  { name: "Illinois", code: "IL", geo: "US-IL" },
  { name: "Indiana", code: "IN", geo: "US-IN" },
  { name: "Iowa", code: "IA", geo: "US-IA" },
  { name: "Kansas", code: "KS", geo: "US-KS" },
  { name: "Kentucky", code: "KY", geo: "US-KY" },
  { name: "Louisiana", code: "LA", geo: "US-LA" },
  { name: "Maine", code: "ME", geo: "US-ME" },
  { name: "Maryland", code: "MD", geo: "US-MD" },
  { name: "Massachusetts", code: "MA", geo: "US-MA" },
  { name: "Michigan", code: "MI", geo: "US-MI" },
  { name: "Minnesota", code: "MN", geo: "US-MN" },
  { name: "Mississippi", code: "MS", geo: "US-MS" },
  { name: "Missouri", code: "MO", geo: "US-MO" },
  { name: "Montana", code: "MT", geo: "US-MT" },
  { name: "Nebraska", code: "NE", geo: "US-NE" },
  { name: "Nevada", code: "NV", geo: "US-NV" },
  { name: "New Hampshire", code: "NH", geo: "US-NH" },
  { name: "New Jersey", code: "NJ", geo: "US-NJ" },
  { name: "New Mexico", code: "NM", geo: "US-NM" },
  { name: "New York", code: "NY", geo: "US-NY" },
  { name: "North Carolina", code: "NC", geo: "US-NC" },
  { name: "North Dakota", code: "ND", geo: "US-ND" },
  { name: "Ohio", code: "OH", geo: "US-OH" },
  { name: "Oklahoma", code: "OK", geo: "US-OK" },
  { name: "Oregon", code: "OR", geo: "US-OR" },
  { name: "Pennsylvania", code: "PA", geo: "US-PA" },
  { name: "Rhode Island", code: "RI", geo: "US-RI" },
  { name: "South Carolina", code: "SC", geo: "US-SC" },
  { name: "South Dakota", code: "SD", geo: "US-SD" },
  { name: "Tennessee", code: "TN", geo: "US-TN" },
  { name: "Texas", code: "TX", geo: "US-TX" },
  { name: "Utah", code: "UT", geo: "US-UT" },
  { name: "Vermont", code: "VT", geo: "US-VT" },
  { name: "Virginia", code: "VA", geo: "US-VA" },
  { name: "Washington", code: "WA", geo: "US-WA" },
  { name: "West Virginia", code: "WV", geo: "US-WV" },
  { name: "Wisconsin", code: "WI", geo: "US-WI" },
  { name: "Wyoming", code: "WY", geo: "US-WY" }
];

let currentRange = "today 12-m";
let currentState = states[0];

const buildTrendsSrc = () => {
  const req = {
    comparisonItem: [
      {
        keyword: "unemployment",
        geo: currentState.geo,
        time: currentRange
      }
    ],
    category: 0,
    property: ""
  };
  return `${baseTrendsURL}${encodeURIComponent(JSON.stringify(req))}&tz=-240`;
};

const trendsFrame = document.getElementById("trendsFrame");
const pills = document.querySelectorAll(".pill-switch .pill");
const stateInput = document.getElementById("stateInput");
const stateStatus = document.getElementById("stateStatus");

const refreshChart = () => {
  if (trendsFrame) {
    trendsFrame.src = buildTrendsSrc();
  }
};

const setTimescale = (range) => {
  currentRange = range || currentRange;
  refreshChart();
  pills.forEach((pill) => {
    pill.classList.toggle("active", pill.dataset.range === range);
  });
};

const findState = (value) => {
  if (!value) return states[0];
  const normalized = value.trim().toLowerCase();
  return (
    states.find(
      (state) =>
        state.name.toLowerCase() === normalized ||
        state.code.toLowerCase() === normalized
    ) || null
  );
};

pills.forEach((pill) => {
  pill.addEventListener("click", () => {
    const { range } = pill.dataset;
    setTimescale(range);
  });
});

if (stateInput) {
  stateInput.addEventListener("change", () => {
    const match = findState(stateInput.value);
    if (match) {
      currentState = match;
      stateInput.value = match.name;
      if (stateStatus) {
        stateStatus.textContent = "";
      }
      refreshChart();
    } else {
      if (stateStatus) {
        stateStatus.textContent = "State not recognized. Please use full name or code.";
      }
      stateInput.value = currentState.name;
    }
  });
  stateInput.value = currentState.name;
}

setTimescale(currentRange);

const fredProxyChain = [
  { name: "Direct", build: (url) => url },
  {
    name: "IsomorphicGit",
    build: (url) => `https://cors.isomorphic-git.org/${url}`
  },
  {
    name: "CorsProxyIO",
    build: (url) => `https://corsproxy.io/?${encodeURIComponent(url)}`
  },
  {
    name: "ThingProxy",
    build: (url) => `https://thingproxy.freeboard.io/fetch/${url}`
  }
];

const fredSeries = [
  {
    id: "UNRATE",
    label: "Unemployment Rate (U-3)",
    units: "Percent",
    description:
      "Headline unemployment rate for civilian labor force (16 years and over).",
    decimals: 1
  },
  {
    id: "CIVPART",
    label: "Labor Force Participation Rate",
    units: "Percent",
    description:
    "Share of the civilian population that is either working or actively searching for work.",
    decimals: 1
  },
  {
    id: "PAYEMS",
    label: "Total Nonfarm Payroll Employment",
    units: "Thousands of persons",
    description:
      "Total nonfarm payroll employment from the Establishment Survey.",
    decimals: 0
  },
  {
    id: "JTSJOL",
    label: "Job Openings (JOLTS)",
    units: "Level, thousands",
    description:
      "Total number of job openings from the Job Openings and Labor Turnover Survey.",
    decimals: 0
  },
  {
    id: "CES0500000003",
    label: "Average Hourly Earnings (Production & Nonsupervisory)",
    units: "Dollars",
    description:
      "Average hourly earnings for production and nonsupervisory employees in the private sector.",
    decimals: 2
  },
  {
    id: "CPIAUCSL",
    label: "Consumer Price Index (All Urban Consumers)",
    units: "Index 1982-84=100",
    description:
      "Headline CPI for all items for all urban consumers (seasonally adjusted).",
    decimals: 1
  },
  {
    id: "ICNSA",
    label: "Initial Unemployment Claims (NSA)",
    units: "Claims",
    description: "Initial claims for unemployment insurance, not seasonally adjusted.",
    decimals: 0
  },
  {
    id: "HOUST",
    label: "Housing Starts",
    units: "Thousands of units",
    description:
      "Privately owned housing units started, seasonally adjusted annual rate.",
    decimals: 0
  }
];

const rateSeries = [
  {
    id: "EFFR",
    label: "Effective Federal Funds Rate",
    units: "Percent",
    description: "Volume-weighted median of overnight federal funds transactions.",
    decimals: 2,
    color: "#2563eb",
    showCard: false
  },
  {
    id: "DFEDTARU",
    label: "Fed Funds Target (Upper)",
    units: "Percent",
    description: "Upper bound of the target range set by the FOMC.",
    decimals: 2,
    color: "#7c3aed",
    showCard: false
  },
  {
    id: "DPRIME",
    label: "Bank Prime Loan Rate",
    units: "Percent",
    description: "Prime lending rate posted by major U.S. banks.",
    decimals: 2,
    color: "#fb923c",
    showCard: false
  },
  {
    id: "DGS2",
    label: "2-Year Treasury Yield",
    units: "Percent",
    description: "Constant maturity yield on 2-year U.S. Treasuries.",
    decimals: 2,
    color: "#10b981"
  },
  {
    id: "DGS5",
    label: "5-Year Treasury Yield",
    units: "Percent",
    description: "Constant maturity yield on 5-year U.S. Treasuries.",
    decimals: 2,
    color: "#1d4ed8",
    showCard: false
  },
  {
    id: "DGS3MO",
    label: "3-Month Treasury Yield",
    units: "Percent",
    description: "Constant maturity yield on 3-month U.S. Treasuries.",
    decimals: 2,
    color: "#0e7490",
    showCard: false
  },
  {
    id: "DGS30",
    label: "30-Year Treasury Yield",
    units: "Percent",
    description: "Constant maturity yield on 30-year U.S. Treasuries.",
    decimals: 2,
    color: "#a16207",
    showCard: false
  },
  {
    id: "DGS10",
    label: "10-Year Treasury Yield",
    units: "Percent",
    description: "Constant maturity yield on 10-year U.S. Treasuries.",
    decimals: 2,
    color: "#dc2626"
  }
];

const YIELD_LONG_ID = "DGS10";
const YIELD_SHORT_ID = "DGS2";
const POLICY_SERIES = ["EFFR", "DFEDTARU", "DPRIME"];
const SPREAD_OPTIONS = [
  {
    id: "10s2s",
    label: "10Y - 2Y",
    long: "DGS10",
    short: "DGS2",
    longLabel: "10-year yield",
    shortLabel: "2-year yield"
  },
  {
    id: "10s3m",
    label: "10Y - 3M",
    long: "DGS10",
    short: "DGS3MO",
    longLabel: "10-year yield",
    shortLabel: "3-month yield"
  },
  {
    id: "30s10s",
    label: "30Y - 10Y",
    long: "DGS30",
    short: "DGS10",
    longLabel: "30-year yield",
    shortLabel: "10-year yield"
  },
  {
    id: "5s2s",
    label: "5Y - 2Y",
    long: "DGS5",
    short: "DGS2",
    longLabel: "5-year yield",
    shortLabel: "2-year yield"
  },
  {
    id: "5s3m",
    label: "5Y - 3M",
    long: "DGS5",
    short: "DGS3MO",
    longLabel: "5-year yield",
    shortLabel: "3-month yield"
  }
];

const getActiveSpread = () =>
  SPREAD_OPTIONS.find((option) => option.id === rateState.spreadId) ||
  SPREAD_OPTIONS[0];

const fredElements = {
  apiKeyInput: document.getElementById("fredApiKey"),
  applyKeyButton: document.getElementById("fredApplyKey"),
  seriesSelect: document.getElementById("fredSeriesSelect"),
  rangeSelect: document.getElementById("fredRangeSelect"),
  refreshButton: document.getElementById("fredRefresh"),
  status: document.getElementById("fredStatus"),
  chartCanvas: document.getElementById("fredChart"),
  latestValue: document.getElementById("fredLatestValue"),
  latestDate: document.getElementById("fredLatestDate"),
  units: document.getElementById("fredUnits"),
  sourceLink: document.getElementById("fredSourceLink"),
  description: document.getElementById("fredDescription")
};

const rateElements = {
  board: document.getElementById("rateBoard"),
  cardsWrapper: document.getElementById("rateCards"),
  status: document.getElementById("rateStatus"),
  rangeButtons: document.querySelectorAll("[data-rate-range]"),
  policyCanvas: document.getElementById("policyRatesChart"),
  policyEffrValue: document.getElementById("policyEffrValue"),
  policyTargetValue: document.getElementById("policyTargetValue"),
  policyPrimeValue: document.getElementById("policyPrimeValue"),
  yieldPairSelect: document.getElementById("yieldPairSelect"),
  yieldCanvas: document.getElementById("yieldSpreadChart"),
  yieldLongLabel: document.getElementById("yieldLongLabel"),
  yieldShortLabel: document.getElementById("yieldShortLabel"),
  yieldTenValue: document.getElementById("yieldTenValue"),
  yieldTwoValue: document.getElementById("yieldTwoValue"),
  yieldSpreadValue: document.getElementById("yieldSpreadValue")
};

const revisionSources = [
  {
    id: "level2",
    label: "Level 2 + totals",
    path: "data/bls_revisions_level2.json"
  }
];

const revisionSeriesOptions = [
  { id: "PAYEMS", label: "Total Nonfarm (SA)" },
  { id: "PAYNSA", label: "Total Nonfarm (NSA)" }
];

const revisionElements = {
  toggle: document.getElementById("revisionToggle"),
  status: document.getElementById("revisionStatus"),
  chartCanvas: document.getElementById("revisionChart"),
  subtitle: document.getElementById("revisionChartSubtitle"),
  range: document.getElementById("revisionRange"),
  supersectorSelect: document.getElementById("revisionSupersectorSelect"),
  stats: document.getElementById("revisionStats"),
  tableBody: document.getElementById("revisionTableBody"),
  deltaTableBody: document.getElementById("revisionDeltaTableBody")
};

const revelioElements = {
  viewSelect: document.getElementById("revelioViewSelect"),
  metricSelect: document.getElementById("revelioMetricSelect"),
  sectorSelect: document.getElementById("revelioSectorSelect"),
  seasonalitySelect: document.getElementById("revelioSeasonalitySelect"),
  rangeSelect: document.getElementById("revelioRangeSelect"),
  refreshButton: document.getElementById("revelioRefresh"),
  hint: document.getElementById("revelioHint"),
  status: document.getElementById("revelioStatus"),
  canvas: document.getElementById("revelioChart"),
  pctCanvas: document.getElementById("revelioPctChart"),
  metrics: document.getElementById("revelioMetrics"),
  tableHead: document.getElementById("revelioTableHead"),
  tableBody: document.getElementById("revelioTableBody")
};
const fredState = {
  apiKey: "",
  seriesId: fredSeries[0]?.id ?? "",
  range: fredElements.rangeSelect?.value ?? "5y",
  chart: null
};

const rateState = {
  range: "6m",
  cards: new Map(),
  policyChart: null,
  yieldChart: null,
  spreadId: SPREAD_OPTIONS[0].id,
  lastData: {}
};

const revisionState = {
  datasetId: revisionSources[0]?.id ?? null,
  datasets: {},
  selectedSeries: revisionSeriesOptions[0]?.id ?? null,
  range: "24m",
  supersector: "Total nonfarm",
  supersectorCatalog: null,
  chart: null,
  loading: false
};

const revelioState = {
  view: "levels",
  metric: "level",
  sector: null,
  seasonality: "sa",
  range: "5y",
  loaded: false,
  loading: false,
  chart: null,
  pctChart: null,
  employment: null,
  revisions: null
};

const storedFredKey = localStorage.getItem("fredApiKey");
if (storedFredKey && fredElements.apiKeyInput) {
  fredState.apiKey = storedFredKey;
  fredElements.apiKeyInput.value = storedFredKey;
}

const updateFredStatus = (message, isError = false) => {
  if (!fredElements.status) return;
  fredElements.status.textContent = message;
  fredElements.status.classList.toggle("error", Boolean(isError));
};

const fetchFredJSON = async (url) => {
  const attempts = [];
  for (const proxy of fredProxyChain) {
    const proxiedUrl = proxy.build(url);
    try {
      const response = await fetch(proxiedUrl);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      return await response.json();
    } catch (error) {
      attempts.push(`${proxy.name}: ${error.message}`);
    }
  }
  throw new Error(attempts.join("; "));
};

const getCurrentFredSeries = () =>
  fredSeries.find((series) => series.id === fredState.seriesId) ||
  fredSeries[0];

const populateSeriesOptions = () => {
  if (!fredElements.seriesSelect) return;
  fredElements.seriesSelect.innerHTML = fredSeries
    .map(
      (series) =>
        `<option value="${series.id}">${series.label}</option>`
    )
    .join("");
  fredElements.seriesSelect.value = fredState.seriesId;
};

const applySeriesDetails = () => {
  const series = getCurrentFredSeries();
  if (fredElements.units) {
    fredElements.units.textContent = series.units;
  }
  if (fredElements.description) {
    fredElements.description.textContent = series.description;
  }
  if (fredElements.sourceLink) {
    fredElements.sourceLink.href = `https://fred.stlouisfed.org/series/${series.id}`;
  }
};

const computeObservationStart = (rangeKey) => {
  if (!rangeKey || rangeKey === "max") {
    return null;
  }
  const match = rangeKey.match(/(\d+)([my])/i);
  if (!match) {
    return null;
  }
  const value = Number.parseInt(match[1], 10);
  if (!Number.isFinite(value)) {
    return null;
  }
  const unit = match[2].toLowerCase();
  const start = new Date();
  if (unit === "m") {
    start.setMonth(start.getMonth() - value);
  } else {
    start.setFullYear(start.getFullYear() - value);
  }
  return start.toISOString().split("T")[0];
};

const fetchSeriesObservations = async (seriesId, rangeKey, apiKey = fredState.apiKey) => {
  if (!apiKey) {
    throw new Error("FRED API key missing.");
  }
  const params = new URLSearchParams({
    series_id: seriesId,
    api_key: apiKey,
    file_type: "json",
    observation_end: new Date().toISOString().split("T")[0]
  });
  const start = computeObservationStart(rangeKey);
  if (start) {
    params.append("observation_start", start);
  }
  const url = `${FRED_OBSERVATIONS_ENDPOINT}?${params.toString()}`;
  const payload = await fetchFredJSON(url);
  if (!Array.isArray(payload.observations) || payload.observations.length === 0) {
    throw new Error("No observations returned.");
  }
  const points = payload.observations
    .map((obs) => ({
      date: obs.date,
      value: Number.parseFloat(obs.value)
    }))
    .filter((point) => Number.isFinite(point.value));
  if (!points.length) {
    throw new Error("Observations were not numeric.");
  }
  return points;
};

const formatValue = (value, decimals = 2) => {
  if (!Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: decimals
  }).format(value);
};

const ensureRateCards = () => {
  if (!rateElements.cardsWrapper || rateState.cards.size || !rateSeries.length) {
    return;
  }
  rateSeries.forEach((series) => {
    if (series.showCard === false) {
      return;
    }
    const card = document.createElement("article");
    card.className = "rate-card";
    card.dataset.series = series.id;

    const header = document.createElement("div");
    header.className = "rate-card-header";
    const titleWrap = document.createElement("div");
    const title = document.createElement("p");
    title.className = "rate-card-title";
    title.textContent = series.label;
    const units = document.createElement("p");
    units.className = "rate-card-units";
    units.textContent = series.units;
    titleWrap.appendChild(title);
    titleWrap.appendChild(units);

    const value = document.createElement("div");
    value.className = "rate-card-value";
    value.textContent = "—";

    header.appendChild(titleWrap);
    header.appendChild(value);

    const canvas = document.createElement("canvas");
    canvas.height = 90;
    canvas.className = "rate-sparkline";

    const meta = document.createElement("div");
    meta.className = "rate-card-meta";
    const dateEl = document.createElement("span");
    dateEl.className = "rate-card-date";
    dateEl.textContent = "—";
    const desc = document.createElement("span");
    desc.className = "rate-card-blurb";
    desc.textContent = series.description;
    meta.appendChild(dateEl);
    meta.appendChild(desc);

    card.appendChild(header);
    card.appendChild(canvas);
    card.appendChild(meta);
    rateElements.cardsWrapper.appendChild(card);

    const color = series.color || "#2563eb";
    rateState.cards.set(series.id, {
      valueEl: value,
      dateEl,
      canvas,
      chart: null,
      color,
      fillColor: `${color}33`
    });
  });
};

const updateRateCard = (series, points) => {
  const card = rateState.cards.get(series.id);
  if (!card || !points.length) return;
  const latest = points[points.length - 1];
  card.valueEl.textContent = formatValue(latest.value, series.decimals ?? 2);
  card.dateEl.textContent = latest.date || "—";
  const labels = points.map((point) => point.date);
  const data = points.map((point) => point.value);
  if (!card.chart) {
    card.chart = new Chart(card.canvas.getContext("2d"), {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            data,
            borderColor: card.color,
            backgroundColor: card.fillColor,
            borderWidth: 2,
            tension: 0.2,
            pointRadius: 0,
            fill: true
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { display: false },
          y: { display: false }
        },
        elements: {
          point: { radius: 0 }
        }
      }
    });
  } else {
    card.chart.data.labels = labels;
    card.chart.data.datasets[0].data = data;
    card.chart.data.datasets[0].borderColor = card.color;
    card.chart.data.datasets[0].backgroundColor = card.fillColor;
    card.chart.update();
  }
};

const updateRateStatus = (message, isError = false) => {
  if (!rateElements.status) return;
  rateElements.status.textContent = message;
  rateElements.status.classList.toggle("error", Boolean(isError));
};

const setRevisionStatus = (message, isError = false) => {
  if (!revisionElements.status) return;
  revisionElements.status.textContent = message;
  revisionElements.status.classList.toggle("error", Boolean(isError));
};

const setRevelioStatus = (message, isError = false) => {
  if (!revelioElements.status) return;
  revelioElements.status.textContent = message;
  revelioElements.status.classList.toggle("error", Boolean(isError));
};

const setRevelioHint = (message) => {
  if (!revelioElements.hint) return;
  revelioElements.hint.textContent = message;
};

const renderYieldFromCache = () => {
  const spread = getActiveSpread();
  const longPoints = rateState.lastData[spread.long];
  const shortPoints = rateState.lastData[spread.short];
  if (longPoints && shortPoints) {
    updateYieldSpreadChart(longPoints, shortPoints);
  }
};

const updatePolicyStats = (effr, target, prime) => {
  if (rateElements.policyEffrValue) {
    rateElements.policyEffrValue.textContent = formatValue(effr, 2);
  }
  if (rateElements.policyTargetValue) {
    rateElements.policyTargetValue.textContent = formatValue(target, 2);
  }
  if (rateElements.policyPrimeValue) {
    rateElements.policyPrimeValue.textContent = formatValue(prime, 2);
  }
};

const updateYieldStats = (longValue, shortValue) => {
  const active = getActiveSpread();
  if (rateElements.yieldLongLabel) {
    rateElements.yieldLongLabel.textContent = active.longLabel;
  }
  if (rateElements.yieldShortLabel) {
    rateElements.yieldShortLabel.textContent = active.shortLabel;
  }
  if (rateElements.yieldTenValue) {
    rateElements.yieldTenValue.textContent = formatValue(longValue, 2);
  }
  if (rateElements.yieldTwoValue) {
    rateElements.yieldTwoValue.textContent = formatValue(shortValue, 2);
  }
  if (rateElements.yieldSpreadValue) {
    const spread = Number.isFinite(longValue) && Number.isFinite(shortValue)
      ? longValue - shortValue
      : Number.NaN;
    rateElements.yieldSpreadValue.textContent = Number.isFinite(spread)
      ? `${formatValue(spread, 2)} pts`
      : "—";
  }
};

const updateYieldSpreadChart = (longPoints, shortPoints) => {
  if (!longPoints?.length || !shortPoints?.length) return;
  const active = getActiveSpread();
  const shortMap = new Map(shortPoints.map((point) => [point.date, point.value]));
  const labels = [];
  const tenData = [];
  const twoData = [];
  const spreadData = [];
  longPoints.forEach((point) => {
    const twoVal = shortMap.get(point.date);
    if (!Number.isFinite(twoVal)) return;
    labels.push(point.date);
    tenData.push(point.value);
    twoData.push(twoVal);
    spreadData.push(point.value - twoVal);
  });
  if (!labels.length) return;
  const latestTen = tenData[tenData.length - 1];
  const latestTwo = twoData[twoData.length - 1];
  updateYieldStats(latestTen, latestTwo);
  if (!rateElements.yieldCanvas || typeof Chart === "undefined") {
    return;
  }
  const ctx = rateElements.yieldCanvas.getContext("2d");
  if (!rateState.yieldChart) {
    rateState.yieldChart = new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: active.longLabel,
            data: tenData,
            borderColor: "#2563eb",
            backgroundColor: "rgba(37, 99, 235, 0.1)",
            borderWidth: 2,
            tension: 0.2,
            fill: false,
            pointRadius: 0,
            yAxisID: "y"
          },
          {
            label: active.shortLabel,
            data: twoData,
            borderColor: "#10b981",
            backgroundColor: "rgba(16, 185, 129, 0.15)",
            borderWidth: 2,
            tension: 0.2,
            fill: false,
            pointRadius: 0,
            yAxisID: "y"
          },
          {
            label: `${active.label} spread`,
            data: spreadData,
            borderColor: "#f97316",
            backgroundColor: "rgba(249, 115, 22, 0.2)",
            borderWidth: 1.5,
            tension: 0.2,
            fill: true,
            pointRadius: 0,
            yAxisID: "spread"
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "bottom" }
        },
        scales: {
          y: {
            position: "left",
            ticks: { maxTicksLimit: 6 }
          },
          spread: {
            position: "right",
            grid: { drawOnChartArea: false },
            ticks: { maxTicksLimit: 5 }
          }
        }
      }
    });
  } else {
    rateState.yieldChart.data.labels = labels;
    rateState.yieldChart.data.datasets[0].data = tenData;
    rateState.yieldChart.data.datasets[1].data = twoData;
    rateState.yieldChart.data.datasets[2].data = spreadData;
    rateState.yieldChart.data.datasets[0].label = active.longLabel;
    rateState.yieldChart.data.datasets[1].label = active.shortLabel;
    rateState.yieldChart.data.datasets[2].label = `${active.label} spread`;
    rateState.yieldChart.update();
  }
};

const updatePolicyChart = (datasets) => {
  if (!POLICY_SERIES.every((id) => datasets[id]?.length)) return;
  const maps = POLICY_SERIES.map((id) =>
    new Map(datasets[id].map((point) => [point.date, point.value]))
  );
  const labelSet = new Set(maps[0].keys());
  maps[1].forEach((_, key) => labelSet.add(key));
  maps[2].forEach((_, key) => labelSet.add(key));
  const labels = Array.from(labelSet).sort((a, b) => new Date(a) - new Date(b));
  const effrData = [];
  const targetData = [];
  const primeData = [];
  const filteredLabels = [];
  labels.forEach((label) => {
    const effrVal = maps[0].get(label);
    const targetVal = maps[1].get(label);
    const primeVal = maps[2].get(label);
    if (
      !Number.isFinite(effrVal) ||
      !Number.isFinite(targetVal) ||
      !Number.isFinite(primeVal)
    ) {
      return;
    }
    filteredLabels.push(label);
    effrData.push(effrVal);
    targetData.push(targetVal);
    primeData.push(primeVal);
  });
  if (!filteredLabels.length) {
    updatePolicyStats(
      effrData[effrData.length - 1],
      targetData[targetData.length - 1],
      primeData[primeData.length - 1]
    );
    return;
  }
  if (!rateElements.policyCanvas || typeof Chart === "undefined") {
    updatePolicyStats(
      effrData[effrData.length - 1],
      targetData[targetData.length - 1],
      primeData[primeData.length - 1]
    );
    return;
  }
  const ctx = rateElements.policyCanvas.getContext("2d");
  if (!rateState.policyChart) {
    rateState.policyChart = new Chart(ctx, {
      type: "line",
      data: {
        labels: filteredLabels,
        datasets: [
          {
            label: "Effective fed funds",
            data: effrData,
            borderColor: "#2563eb",
            borderWidth: 2,
            tension: 0.2,
            fill: false,
            pointRadius: 0
          },
          {
            label: "Target (upper)",
            data: targetData,
            borderColor: "#7c3aed",
            borderWidth: 2,
            tension: 0.2,
            fill: false,
            pointRadius: 0
          },
          {
            label: "Bank prime",
            data: primeData,
            borderColor: "#fb923c",
            borderWidth: 2,
            tension: 0.2,
            fill: false,
            pointRadius: 0
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "bottom" }
        },
        scales: {
          y: {
            ticks: { maxTicksLimit: 6 }
          }
        }
      }
    });
  } else {
    rateState.policyChart.data.labels = filteredLabels;
    rateState.policyChart.data.datasets[0].data = effrData;
    rateState.policyChart.data.datasets[1].data = targetData;
    rateState.policyChart.data.datasets[2].data = primeData;
    rateState.policyChart.update();
  }
  updatePolicyStats(
    effrData[effrData.length - 1],
    targetData[targetData.length - 1],
    primeData[primeData.length - 1]
  );
};

const updateFredChart = (points, series) => {
  if (!fredElements.chartCanvas || typeof Chart === "undefined") {
    updateFredStatus("Chart.js failed to load.", true);
    return;
  }
  const labels = points.map((point) => point.date);
  const data = points.map((point) => point.value);
  if (!fredState.chart) {
    fredState.chart = new Chart(fredElements.chartCanvas.getContext("2d"), {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: series.label,
            data,
            borderColor: "#2563eb",
            backgroundColor: "rgba(37, 99, 235, 0.15)",
            borderWidth: 2,
            tension: 0.25,
            pointRadius: 0,
            fill: true
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
          mode: "index",
          intersect: false
        },
        plugins: {
          legend: { display: false }
        },
        scales: {
          x: {
            ticks: { maxTicksLimit: 6 }
          },
          y: {
            ticks: { maxTicksLimit: 6 }
          }
        }
      }
    });
  } else {
    fredState.chart.data.labels = labels;
    fredState.chart.data.datasets[0].data = data;
    fredState.chart.data.datasets[0].label = series.label;
    fredState.chart.update();
  }
};

const updateFredMeta = (points, series) => {
  const latest = points[points.length - 1];
  if (fredElements.latestValue) {
    fredElements.latestValue.textContent = formatValue(
      latest.value,
      series.decimals ?? 2
    );
  }
  if (fredElements.latestDate) {
    fredElements.latestDate.textContent = latest.date || "—";
  }
};

const loadFredSeries = async () => {
  const currentSeries = getCurrentFredSeries();
  if (!fredState.apiKey) {
    updateFredStatus("Enter your FRED API key to load data.", true);
    return;
  }

  updateFredStatus(`Fetching ${currentSeries.label}…`);
  fredElements.refreshButton?.setAttribute("disabled", "disabled");

  try {
    const points = await fetchSeriesObservations(currentSeries.id, fredState.range);
    updateFredChart(points, currentSeries);
    updateFredMeta(points, currentSeries);
    updateFredStatus(
      `Loaded ${points.length} observations (${currentSeries.label}).`
    );
  } catch (error) {
    updateFredStatus(`Unable to load series: ${error.message}`, true);
  } finally {
    fredElements.refreshButton?.removeAttribute("disabled");
  }
};

const loadRateDashboard = async () => {
  if (!rateElements.cardsWrapper || !rateSeries.length) return;
  ensureRateCards();
  if (!fredState.apiKey) {
    updateRateStatus("Enter your FRED API key to load the rate monitor.", true);
    return;
  }
  updateRateStatus("Loading interest rate series…");
  try {
    const results = {};
    await Promise.all(
      rateSeries.map(async (series) => {
        const points = await fetchSeriesObservations(series.id, rateState.range);
        results[series.id] = points;
        updateRateCard(series, points);
      })
    );
    rateState.lastData = results;
    const spread = getActiveSpread();
    if (results[spread.long] && results[spread.short]) {
      updateYieldSpreadChart(results[spread.long], results[spread.short]);
    }
    updatePolicyChart(results);
    updateRateStatus("Interest rates updated.");
  } catch (error) {
    updateRateStatus(`Unable to load rate data: ${error.message}`, true);
  }
};

const updateRateButtons = () => {
  rateElements.rangeButtons.forEach((button) => {
    button.classList.toggle("active", button.dataset.rateRange === rateState.range);
  });
};

const setRateRange = (range) => {
  if (!range || rateState.range === range) return;
  rateState.range = range;
  updateRateButtons();
  loadRateDashboard();
};

const applyFredKey = () => {
  if (!fredElements.apiKeyInput) return;
  const trimmed = fredElements.apiKeyInput.value.trim();
  if (!trimmed) {
    updateFredStatus("Please provide a valid API key.", true);
    return;
  }
  fredState.apiKey = trimmed;
  localStorage.setItem("fredApiKey", trimmed);
  updateFredStatus("API key applied. Loading data…");
  loadFredSeries();
  loadRateDashboard();
};

if (fredElements.applyKeyButton) {
  fredElements.applyKeyButton.addEventListener("click", applyFredKey);
}

if (fredElements.apiKeyInput) {
  fredElements.apiKeyInput.addEventListener("keypress", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      applyFredKey();
    }
  });
}

if (fredElements.seriesSelect) {
  populateSeriesOptions();
  fredElements.seriesSelect.addEventListener("change", (event) => {
    fredState.seriesId = event.target.value;
    applySeriesDetails();
    loadFredSeries();
  });
}

if (fredElements.rangeSelect) {
  fredElements.rangeSelect.addEventListener("change", (event) => {
    fredState.range = event.target.value;
    loadFredSeries();
  });
}

if (fredElements.refreshButton) {
  fredElements.refreshButton.addEventListener("click", loadFredSeries);
}

applySeriesDetails();

if (rateElements.rangeButtons.length) {
  rateElements.rangeButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const { rateRange } = button.dataset;
      setRateRange(rateRange);
    });
  });
  updateRateButtons();
}

if (rateElements.yieldPairSelect) {
  rateElements.yieldPairSelect.value = rateState.spreadId;
  rateElements.yieldPairSelect.addEventListener("change", (event) => {
    rateState.spreadId = event.target.value;
    renderYieldFromCache();
  });
}

ensureRateCards();

const toNumber = (value) => {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const formatEmployment = (value) => {
  const num = toNumber(value);
  if (!Number.isFinite(num)) return "—";
  return num.toLocaleString();
};

const formatDelta = (value) => {
  const num = toNumber(value);
  if (!Number.isFinite(num)) return "—";
  const formatted = num.toLocaleString(undefined, {
    minimumFractionDigits: Math.abs(num) < 1 ? 2 : 0,
    maximumFractionDigits: Math.abs(num) < 1 ? 2 : 0
  });
  return num > 0 ? `+${formatted}` : formatted;
};

const formatPeople = (value) => {
  const num = toNumber(value);
  if (!Number.isFinite(num)) return "—";
  return Math.round(num).toLocaleString();
};

const formatPeopleDelta = (value) => {
  const num = toNumber(value);
  if (!Number.isFinite(num)) return "—";
  const formatted = Math.round(Math.abs(num)).toLocaleString();
  return num > 0 ? `+${formatted}` : `-${formatted}`;
};

const formatMonthLabel = (date) =>
  date
    ? new Intl.DateTimeFormat("en-US", { month: "short", year: "numeric" }).format(date)
    : "—";

const formatVintageDate = (value) => {
  if (!value) return "—";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric"
  }).format(date);
};

const revisionRangeMeta = {
  "12m": { label: "12M", months: 12 },
  "24m": { label: "24M", months: 24 },
  "5y": { label: "5Y", years: 5 },
  max: { label: "Max" }
};

const sliceRevisionWindow = (records, rangeKey) => {
  if (!Array.isArray(records) || !records.length) return [];
  const meta = revisionRangeMeta[rangeKey] || revisionRangeMeta["24m"];
  if (!meta || rangeKey === "max") return records.slice();
  const last = records[records.length - 1]?.obsDate;
  if (!last) return records.slice();
  const start = new Date(last);
  if (meta.years) {
    start.setFullYear(start.getFullYear() - meta.years);
  } else if (meta.months) {
    start.setMonth(start.getMonth() - meta.months);
  }
  return records.filter((record) => record.obsDate && record.obsDate >= start);
};

const sliceMonthSeries = (points, rangeKey) => {
  if (!Array.isArray(points) || !points.length) return [];
  if (rangeKey === "max") return points.slice();
  const last = points[points.length - 1]?.date;
  if (!last) return points.slice();
  const start = new Date(last);
  if (rangeKey === "10y") start.setFullYear(start.getFullYear() - 10);
  else if (rangeKey === "5y") start.setFullYear(start.getFullYear() - 5);
  else start.setFullYear(start.getFullYear() - 1);
  return points.filter((point) => point.date && point.date >= start);
};

const parseMonth = (ym) => {
  if (!ym) return null;
  const date = new Date(`${ym}-01T00:00:00`);
  return Number.isNaN(date.getTime()) ? null : date;
};

const safeJSONParseLoose = (text) => {
  const sanitized = text
    .replace(/\bNaN\b/g, "null")
    .replace(/\bInfinity\b/g, "null")
    .replace(/\b-Infinity\b/g, "null");
  return JSON.parse(sanitized);
};

const fetchJSONLoose = async (path) => {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const text = await response.text();
  return safeJSONParseLoose(text);
};

const prepareRevisionDataset = (raw, datasetId) => {
  const seriesMap = new Map();
  const metaMap = new Map();

  raw.forEach((item) => {
    const seriesId = item.series_id;
    if (!seriesId) return;
    const obsDate = item.obs_date ? new Date(item.obs_date) : null;
    const record = {
      seriesId,
      obsDate,
      estimate_t: toNumber(item.estimate_t),
      estimate_t1: toNumber(item.estimate_t1),
      estimate_t2: toNumber(item.estimate_t2),
      rev1: toNumber(item.revision_1month),
      rev2: toNumber(item.rev2_cum ?? item.revision_2month ?? item.rev2_incr),
      raw: item
    };
    if (!seriesMap.has(seriesId)) {
      seriesMap.set(seriesId, []);
    }
    seriesMap.get(seriesId).push(record);

    if (!metaMap.has(seriesId)) {
      const labels = [];
      if (item.industry_name) {
        labels.push(item.industry_name);
      } else if (item.supersector_name) {
        labels.push(item.supersector_name);
      } else {
        labels.push(seriesId);
      }
      const isNSA =
        item.seasonally_adjusted === false || seriesId.startsWith("CEU") || seriesId.endsWith("NSA");
      const isSA = item.seasonally_adjusted === true || seriesId.startsWith("CES") || seriesId.endsWith("SA");
      if (isSA) {
        labels.push("(SA)");
      } else if (isNSA) {
        labels.push("(NSA)");
      }
      metaMap.set(seriesId, {
        label: labels.join(" "),
        industry: item.industry_name || seriesId,
        supersector: item.supersector_name || "—",
        datasetId
      });
    }
  });

  seriesMap.forEach((records) => {
    records.sort((a, b) => {
      if (!a.obsDate || !b.obsDate) return 0;
      return a.obsDate.getTime() - b.obsDate.getTime();
    });
  });

  return { seriesMap, metaMap };
};

const isRevisionSeriesSA = (seriesId, exampleRaw) => {
  if (seriesId === "PAYEMS") return true;
  if (seriesId === "PAYNSA") return false;
  if (typeof exampleRaw?.seasonally_adjusted === "boolean") {
    return exampleRaw.seasonally_adjusted === true;
  }
  if (seriesId.startsWith("CES")) return true;
  if (seriesId.startsWith("CEU")) return false;
  if (seriesId.endsWith("SA")) return true;
  if (seriesId.endsWith("NSA")) return false;
  return true;
};

const swapRevisionSeasonality = (seriesId, targetSA) => {
  if (!seriesId) return null;
  if (seriesId === "PAYEMS") return targetSA ? "PAYEMS" : "PAYNSA";
  if (seriesId === "PAYNSA") return targetSA ? "PAYEMS" : "PAYNSA";
  if (targetSA && seriesId.startsWith("CEU")) return `CES${seriesId.slice(3)}`;
  if (!targetSA && seriesId.startsWith("CES")) return `CEU${seriesId.slice(3)}`;
  return seriesId;
};

const buildRevisionSupersectorCatalog = (dataset) => {
  const nsaBySuper = new Map();

  dataset.metaMap.forEach((meta, seriesId) => {
    const records = dataset.seriesMap.get(seriesId);
    const exampleRaw = records?.[0]?.raw ?? null;
    const isSA = isRevisionSeriesSA(seriesId, exampleRaw);
    if (isSA) return;
    const supersector =
      (meta.supersector || meta.industry || "Other").toString().trim() || "Other";

    const existing = nsaBySuper.get(supersector);
    const score = (() => {
      if (seriesId === "PAYNSA") return 100;
      if (meta.industry && meta.supersector && meta.industry === meta.supersector) return 90;
      return 10;
    })();
    if (!existing || score > existing.score) {
      nsaBySuper.set(supersector, {
        seriesId,
        label: meta.label || seriesId,
        score
      });
    }
  });

  const list = (map) =>
    Array.from(map.entries())
      .map(([name, value]) => ({ name, seriesId: value.seriesId, label: value.label }))
      .sort((a, b) => a.name.localeCompare(b.name));

  return { list: list(nsaBySuper) };
};

const renderRevisionSupersectorSelect = (dataset) => {
  if (!revisionElements.supersectorSelect) return;
  if (!revisionState.supersectorCatalog) {
    revisionState.supersectorCatalog = buildRevisionSupersectorCatalog(dataset);
  }
  const options = revisionState.supersectorCatalog.list;

  revisionElements.supersectorSelect.innerHTML = options
    .map((entry) => `<option value="${entry.name}">${entry.name}</option>`)
    .join("");

  const currentMeta = dataset.metaMap.get(revisionState.selectedSeries);
  const currentSuper = (currentMeta?.supersector || currentMeta?.industry || revisionState.supersector).toString();
  revisionState.supersector = currentSuper;
  revisionElements.supersectorSelect.value = currentSuper;
};

const selectRevisionSupersector = (supersectorName) => {
  const dataset = revisionState.datasets[revisionState.datasetId];
  if (!dataset) return;
  if (!revisionState.supersectorCatalog) {
    revisionState.supersectorCatalog = buildRevisionSupersectorCatalog(dataset);
  }
  const options = revisionState.supersectorCatalog.list;
  const match = options.find((entry) => entry.name === supersectorName) || null;
  if (!match) return;
  revisionState.supersector = match.name;
  if (match.name.toLowerCase() === "total nonfarm") {
    const wantSA = revisionElements.toggle?.querySelector("[data-revision-series=\"PAYEMS\"]")?.classList.contains("active");
    revisionState.selectedSeries = wantSA ? "PAYEMS" : "PAYNSA";
  } else {
    revisionState.selectedSeries = match.seriesId;
  }
  renderRevisionView();
};

const updateRevisionChart = (records) => {
  if (!revisionElements.chartCanvas || typeof Chart === "undefined") return;
  const windowed = records;
  const labels = windowed.map((record) => formatMonthLabel(record.obsDate));
  const buildSeries = (key, label, color) => ({
    label,
    data: windowed.map((record) => record[key] ?? null),
    borderColor: color.border,
    backgroundColor: color.fill,
    borderWidth: 2,
    tension: 0.2,
    spanGaps: true,
    pointRadius: 0,
    pointHitRadius: 8
  });
  const datasets = [
    buildSeries("estimate_t", "Initial", { border: "#94a3b8", fill: "rgba(148, 163, 184, 0.18)" }),
    buildSeries("estimate_t1", "1st rev", { border: "#2563eb", fill: "rgba(37, 99, 235, 0.14)" }),
    buildSeries("estimate_t2", "Final", { border: "#16a34a", fill: "rgba(22, 163, 74, 0.12)" })
  ];

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: "bottom",
        labels: {
          boxWidth: 10,
          boxHeight: 10,
          padding: 14,
          font: { size: 12 }
        }
      }
    },
    interaction: { mode: "index", intersect: false },
    scales: {
      y: {
        ticks: {
          callback: (value) => Number(value).toLocaleString()
        }
      }
    }
  };

  if (!revisionState.chart) {
    revisionState.chart = new Chart(revisionElements.chartCanvas.getContext("2d"), {
      type: "line",
      data: { labels, datasets },
      options
    });
  } else {
    revisionState.chart.options = options;
    revisionState.chart.data.labels = labels;
    revisionState.chart.data.datasets = datasets;
    revisionState.chart.update();
  }
};

const updateRevisionSubtitle = (dataset, records) => {
  if (!revisionElements.subtitle) return;
  const rangeMeta = revisionRangeMeta[revisionState.range] || revisionRangeMeta["24m"];
  const seriesOption =
    revisionSeriesOptions.find((option) => option.id === revisionState.selectedSeries) || null;
  const source =
    revisionSources.find((src) => src.id === revisionState.datasetId) || revisionSources[0];
  const meta = dataset?.metaMap?.get(revisionState.selectedSeries) || null;

  const start = records[0]?.obsDate;
  const end = records[records.length - 1]?.obsDate;
  const latestRaw = records[records.length - 1]?.raw || null;

  const pieces = [];
  pieces.push(`${seriesOption ? seriesOption.label : meta?.label || revisionState.selectedSeries} (${revisionState.selectedSeries})`);
  if (meta?.supersector) pieces.push(meta.supersector);
  if (source?.label) pieces.push(`Dataset: ${source.label}`);
  if (start && end) pieces.push(`${rangeMeta.label}: ${formatMonthLabel(start)}–${formatMonthLabel(end)}`);
  if (latestRaw) {
    pieces.push(
      `Vintages: ${formatVintageDate(latestRaw.vintage_t)} / ${formatVintageDate(latestRaw.vintage_t1)} / ${formatVintageDate(latestRaw.vintage_t2)}`
    );
  }

  revisionElements.subtitle.textContent = pieces.join(" · ");
};

const updateRevisionStats = (records) => {
  if (!revisionElements.stats) return;
  if (!records.length) {
    revisionElements.stats.innerHTML = "<p>No revisions available.</p>";
    return;
  }
  const latest = records[records.length - 1];
  const stats = [
    ["Latest month", formatMonthLabel(latest.obsDate)],
    ["Initial estimate", formatEmployment(latest.estimate_t)],
    ["First revision", formatEmployment(latest.estimate_t1)],
    ["Final estimate", formatEmployment(latest.estimate_t2)],
    ["Δ 1 month", formatDelta(latest.rev1 ?? (latest.estimate_t1 ?? 0) - (latest.estimate_t ?? 0))],
    ["Δ 2 months", formatDelta(latest.rev2 ?? (latest.estimate_t2 ?? 0) - (latest.estimate_t ?? 0))]
  ];
  revisionElements.stats.innerHTML = stats
    .map(
      ([label, value]) =>
        `<div class="revision-stat"><span>${label}</span><span title="${value}">${value}</span></div>`
    )
    .join("");
};

const updateRevisionTable = (records) => {
  if (!revisionElements.tableBody) return;
  if (!records.length) {
    revisionElements.tableBody.innerHTML =
      '<tr><td colspan="6">No data loaded yet.</td></tr>';
    return;
  }
  const rows = records.slice(-6).reverse();
  revisionElements.tableBody.innerHTML = rows
    .map((row) => {
      const delta1 =
        row.rev1 ??
        (row.estimate_t1 != null && row.estimate_t != null
          ? row.estimate_t1 - row.estimate_t
          : null);
      const delta2 =
        row.rev2 ??
        (row.estimate_t2 != null && row.estimate_t != null
          ? row.estimate_t2 - row.estimate_t
          : null);
      return `
        <tr>
          <td>${formatMonthLabel(row.obsDate)}</td>
          <td>${formatEmployment(row.estimate_t)}</td>
          <td>${formatEmployment(row.estimate_t1)}</td>
          <td>${formatEmployment(row.estimate_t2)}</td>
          <td>${formatDelta(delta1)}</td>
          <td>${formatDelta(delta2)}</td>
        </tr>
      `;
    })
    .join("");
};

const updateRevisionDeltaTable = (records) => {
  if (!revisionElements.deltaTableBody) return;
  if (!records.length) {
    revisionElements.deltaTableBody.innerHTML =
      '<tr><td colspan="2">No data yet.</td></tr>';
    return;
  }
  const rows = records.slice(-10).reverse();
  revisionElements.deltaTableBody.innerHTML = rows
    .map((row) => {
      const delta =
        row.rev2 ??
        (row.estimate_t2 != null && row.estimate_t != null
          ? row.estimate_t2 - row.estimate_t
          : null);
      return `
        <tr>
          <td>${formatMonthLabel(row.obsDate)}</td>
          <td>${formatDelta(delta)}</td>
        </tr>
      `;
    })
    .join("");
};

const renderRevisionView = () => {
  const dataset = revisionState.datasets[revisionState.datasetId];
  if (!dataset || !revisionState.selectedSeries || !dataset.seriesMap.has(revisionState.selectedSeries)) {
    if (revisionState.chart) {
      revisionState.chart.destroy();
      revisionState.chart = null;
    }
    if (revisionElements.subtitle) {
      revisionElements.subtitle.textContent = "";
    }
    if (revisionElements.stats) {
      revisionElements.stats.innerHTML =
        "<p>Run the revision script to generate the JSON dataset, then refresh this page.</p>";
    }
    if (revisionElements.tableBody) {
      revisionElements.tableBody.innerHTML =
        '<tr><td colspan="6">Dataset not available yet.</td></tr>';
    }
    return;
  }
  const records = dataset.seriesMap.get(revisionState.selectedSeries);
  const windowed = sliceRevisionWindow(records, revisionState.range);
  const viewRecords = windowed.length ? windowed : records;
  renderRevisionSupersectorSelect(dataset);
  updateRevisionSubtitle(dataset, viewRecords);
  updateRevisionChart(viewRecords);
  updateRevisionStats(viewRecords);
  updateRevisionTable(viewRecords);
  updateRevisionDeltaTable(viewRecords);
};

const loadRevisionDataset = async () => {
  const source = revisionSources.find((src) => src.id === revisionState.datasetId);
  if (!source) return;
  if (revisionState.loading) return;
  revisionState.loading = true;
  setRevisionStatus(`Loading ${source.label}…`);
  try {
    const response = await fetch(source.path, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const raw = await response.json();
    if (!Array.isArray(raw) || !raw.length) {
      throw new Error("Dataset is empty");
    }
    revisionState.datasets[revisionState.datasetId] = prepareRevisionDataset(
      raw,
      revisionState.datasetId
    );
    revisionState.supersectorCatalog = null;
    const prepared = revisionState.datasets[revisionState.datasetId];
    if (prepared && revisionState.selectedSeries && !prepared.seriesMap.has(revisionState.selectedSeries)) {
      revisionState.selectedSeries =
        revisionSeriesOptions.find((option) => prepared.seriesMap.has(option.id))?.id ??
        Array.from(prepared.seriesMap.keys())[0] ??
        revisionState.selectedSeries;
    }
    setRevisionStatus(
      `Loaded ${raw.length.toLocaleString()} records from ${source.label}.`
    );
    renderRevisionView();
  } catch (error) {
    setRevisionStatus(`Unable to load ${source.label}: ${error.message}`, true);
    revisionState.selectedSeries = null;
    renderRevisionView();
  } finally {
    revisionState.loading = false;
  }
};

const initRevisionExplorer = () => {
  if (!revisionState.datasetId || !revisionElements.toggle || !revisionSeriesOptions.length) {
    return;
  }
  Array.from(revisionElements.toggle.querySelectorAll("button")).forEach((button) => {
    const seriesId = button.dataset.revisionSeries;
    button.addEventListener("click", () => {
      if (revisionState.selectedSeries === seriesId) return;
      const targetSA = seriesId === "PAYEMS";
      if (revisionState.supersector?.toLowerCase?.() === "total nonfarm") {
        revisionState.selectedSeries = targetSA ? "PAYEMS" : "PAYNSA";
      } else {
        revisionState.selectedSeries = swapRevisionSeasonality(revisionState.selectedSeries, targetSA) || revisionState.selectedSeries;
      }
      revisionElements.toggle
        .querySelectorAll("button")
        .forEach((btn) => btn.classList.toggle("active", btn === button));
      const dataset = revisionState.datasets[revisionState.datasetId];
      if (dataset) {
        revisionState.supersectorCatalog = null;
        renderRevisionSupersectorSelect(dataset);
        if (revisionState.supersector?.toLowerCase?.() === "total nonfarm") {
          renderRevisionView();
          return;
        }
        selectRevisionSupersector(revisionState.supersector);
        return;
      }
      renderRevisionView();
    });
  });

  if (revisionElements.range) {
    Array.from(revisionElements.range.querySelectorAll("button")).forEach((button) => {
      const rangeKey = button.dataset.revisionRange;
      button.addEventListener("click", () => {
        if (!rangeKey || revisionState.range === rangeKey) return;
        revisionState.range = rangeKey;
        revisionElements.range
          .querySelectorAll("button")
          .forEach((btn) => btn.classList.toggle("active", btn === button));
        renderRevisionView();
      });
    });
  }

  if (revisionElements.supersectorSelect) {
    revisionElements.supersectorSelect.addEventListener("change", () => {
      selectRevisionSupersector(revisionElements.supersectorSelect.value);
    });
  }

  loadRevisionDataset();
};

const buildRevelioSectorOptions = (employment) => {
  if (!revelioElements.sectorSelect) return;
  const comparable = [];
  const revelioOnly = [];

  employment.sectors.forEach((sector) => {
    const hasBLS = sector.hasBLS;
    (hasBLS ? comparable : revelioOnly).push(sector);
  });

  const sectorSortKey = (sector) => {
    if (sector.code === "00") return `0-00-${sector.name}`;
    if (sector.code === "NF") return `0-01-${sector.name}`;
    return `1-${sector.name}`;
  };

  const makeOptions = (sectors) =>
    sectors
      .sort((a, b) => sectorSortKey(a).localeCompare(sectorSortKey(b)))
      .map((sector) => `<option value="${sector.code}">${sector.name}</option>`)
      .join("");

  revelioElements.sectorSelect.innerHTML = "";
  if (comparable.length) {
    const group = document.createElement("optgroup");
    group.label = "Comparable sectors";
    group.innerHTML = makeOptions(comparable);
    revelioElements.sectorSelect.appendChild(group);
  }
  if (revelioOnly.length) {
    const group = document.createElement("optgroup");
    group.label = "Revelio-only (no BLS match)";
    group.innerHTML = makeOptions(revelioOnly);
    revelioElements.sectorSelect.appendChild(group);
  }
};

const prepareRevelioEmployment = (raw) => {
  const byCode = new Map();
  const sectorMeta = new Map();

  raw.forEach((row) => {
    const code = String(row.naics2d_code ?? "").trim();
    const month = String(row.month ?? "").trim();
    if (!code || !month) return;
    const date = parseMonth(month);
    if (!date) return;

    const name = String(row.naics2d_name ?? code).trim();
    if (!sectorMeta.has(code)) {
      sectorMeta.set(code, { code, name });
    }

    const saRevelio = toNumber(row.employment_sa_revelio);
    const nsaRevelio = toNumber(row.employment_nsa_revelio);
    const saBLS = toNumber(row.employment_sa_bls);
    const nsaBLS = toNumber(row.employment_nsa_bls);
    const saADP = toNumber(row.employment_sa_adp);
    const nsaADP = toNumber(row.employment_nsa_adp);

    if (!byCode.has(code)) byCode.set(code, []);
    byCode.get(code).push({
      month,
      date,
      sa: { revelio: saRevelio, bls: saBLS, adp: saADP },
      nsa: { revelio: nsaRevelio, bls: nsaBLS, adp: nsaADP }
    });
  });

  const sectors = Array.from(sectorMeta.values()).map((sector) => {
    const points = (byCode.get(sector.code) || []).slice().sort((a, b) => a.date - b.date);
    const hasBLS = points.some((p) => Number.isFinite(p.sa.bls) || Number.isFinite(p.nsa.bls));
    const hasADP = points.some((p) => Number.isFinite(p.sa.adp) || Number.isFinite(p.nsa.adp));
    return { ...sector, hasBLS, hasADP, points };
  });

  sectors.sort((a, b) => a.name.localeCompare(b.name));
  return { sectors, byCode };
};

const prepareRevelioRevisions = (raw) => {
  const byMonth = new Map();
  raw.forEach((row) => {
    const month = String(row.month ?? "").trim();
    const release = String(row.release ?? "").trim();
    if (!month || !release) return;
    const valueR = toNumber(row.employment_sa_revelio);
    const valueB = toNumber(row.employment_sa_bls);
    if (!byMonth.has(month)) byMonth.set(month, []);
    byMonth.get(month).push({ release, revelio: valueR, bls: valueB });
  });

  const series = [];
  Array.from(byMonth.entries()).forEach(([month, entries]) => {
    const date = parseMonth(month);
    if (!date) return;
    entries.sort((a, b) => a.release.localeCompare(b.release));
    const first = entries[0];
    const last = entries[entries.length - 1];
    if (!first || !last) return;
    const initialR = first.revelio;
    const finalR = last.revelio;
    const initialB = first.bls;
    const finalB = last.bls;
    series.push({
      month,
      date,
      firstRelease: first.release,
      lastRelease: last.release,
      initial: { revelio: initialR, bls: initialB },
      final: { revelio: finalR, bls: finalB },
      revision: {
        revelio: Number.isFinite(finalR) && Number.isFinite(initialR) ? finalR - initialR : null,
        bls: Number.isFinite(finalB) && Number.isFinite(initialB) ? finalB - initialB : null
      }
    });
  });
  series.sort((a, b) => a.date - b.date);
  return { series };
};

const ensureRevelioLoaded = async () => {
  if (revelioState.loaded || revelioState.loading) return;
  revelioState.loading = true;
  setRevelioStatus("Loading BLS vs Revelio comparison data…");
  try {
    const [employmentRaw, revisionsRaw] = await Promise.all([
      fetchJSONLoose("data/bls_vs_revelio_employment.json"),
      fetchJSONLoose("data/bls_vs_revelio_revisions.json")
    ]);
    revelioState.employment = prepareRevelioEmployment(employmentRaw);
    revelioState.revisions = prepareRevelioRevisions(revisionsRaw);
    buildRevelioSectorOptions(revelioState.employment);
    const preferredTotal =
      revelioState.employment.sectors.find((s) => s.code === "00" && s.hasBLS)?.code ||
      revelioState.employment.sectors.find((s) => s.code === "NF" && s.hasBLS)?.code ||
      null;
    revelioState.sector =
      revelioState.sector ||
      preferredTotal ||
      revelioState.employment.sectors.find((s) => s.hasBLS)?.code ||
      revelioState.employment.sectors[0]?.code ||
      null;
    if (revelioElements.sectorSelect && revelioState.sector) {
      revelioElements.sectorSelect.value = revelioState.sector;
    }
    if (revelioElements.seasonalitySelect) {
      revelioElements.seasonalitySelect.value = "both";
      revelioState.seasonality = "both";
    }
    if (revelioState.employment.sectors.some((s) => s.code === "NF")) {
      setRevelioHint(
        "Defaults to Total private (ADP comparable). Selecting a sector switches to NSA automatically."
      );
    } else {
      setRevelioHint("Defaults to Total (SA + NSA). Selecting a sector switches to NSA automatically.");
    }
    setRevelioStatus("Loaded BLS vs Revelio data.");
    revelioState.loaded = true;
  } catch (error) {
    setRevelioStatus(
      `Unable to load BLS vs Revelio datasets. Re-run fetch script to generate JSON. (${error.message})`,
      true
    );
  } finally {
    revelioState.loading = false;
  }
};

const renderRevelioMetrics = (items) => {
  if (!revelioElements.metrics) return;
  revelioElements.metrics.innerHTML = items
    .map(
      ({ label, value }) =>
        `<div><p class="detail-label">${label}</p><p>${value}</p></div>`
    )
    .join("");
};

const renderRevelioTable = (view, rows) => {
  if (!revelioElements.tableHead || !revelioElements.tableBody) return;
  if (!rows.length) {
    revelioElements.tableHead.innerHTML = "";
    revelioElements.tableBody.innerHTML = '<tr><td colspan="6">No data available.</td></tr>';
    return;
  }

  if (view === "revisions") {
    revelioElements.tableHead.innerHTML = `
      <tr>
        <th>Month</th>
        <th>First release</th>
        <th>Last release</th>
        <th>BLS final</th>
        <th>Revelio final</th>
        <th>Gap (BLS–Revelio)</th>
        <th>BLS Δ</th>
        <th>Revelio Δ</th>
      </tr>
    `;
    revelioElements.tableBody.innerHTML = rows
      .map((row) => {
        const gap =
          Number.isFinite(row.final.bls) && Number.isFinite(row.final.revelio)
            ? row.final.bls - row.final.revelio
            : null;
        return `
          <tr>
            <td>${row.month}</td>
            <td>${row.firstRelease}</td>
            <td>${row.lastRelease}</td>
            <td>${formatPeople(row.final.bls)}</td>
            <td>${formatPeople(row.final.revelio)}</td>
            <td>${formatPeopleDelta(gap)}</td>
            <td>${formatPeopleDelta(row.revision.bls)}</td>
            <td>${formatPeopleDelta(row.revision.revelio)}</td>
          </tr>
        `;
      })
      .join("");
    return;
  }

  const hasAdp = rows.some((row) => Number.isFinite(toNumber(row.adp)));

  revelioElements.tableHead.innerHTML = `
    <tr>
      <th>Month</th>
      <th>BLS</th>
      <th>Revelio</th>
      ${hasAdp ? "<th>ADP</th>" : ""}
      <th>Gap (BLS–Revelio)</th>
      ${hasAdp ? "<th>Gap (BLS–ADP)</th>" : ""}
    </tr>
  `;
  revelioElements.tableBody.innerHTML = rows
    .map((row) => {
      const bls = row.bls;
      const rev = row.revelio;
      const gap =
        Number.isFinite(bls) && Number.isFinite(rev) ? bls - rev : null;
      const adp = toNumber(row.adp);
      const gapAdp =
        Number.isFinite(bls) && Number.isFinite(adp) ? bls - adp : null;
      return `
        <tr>
          <td>${row.month}</td>
          <td>${formatPeople(bls)}</td>
          <td>${formatPeople(rev)}</td>
          ${hasAdp ? `<td>${formatPeople(adp)}</td>` : ""}
          <td>${formatPeopleDelta(gap)}</td>
          ${hasAdp ? `<td>${formatPeopleDelta(gapAdp)}</td>` : ""}
        </tr>
      `;
    })
    .join("");
};

const renderRevelioChart = (config) => {
  if (!revelioElements.canvas || typeof Chart === "undefined") return;
  const ctx = revelioElements.canvas.getContext("2d");
  if (!revelioState.chart) {
    revelioState.chart = new Chart(ctx, config);
  } else {
    revelioState.chart.config.type = config.type;
    revelioState.chart.data = config.data;
    revelioState.chart.options = config.options;
    revelioState.chart.update();
  }
};

const renderRevelioPctChart = (config) => {
  if (!revelioElements.pctCanvas || typeof Chart === "undefined") return;
  const ctx = revelioElements.pctCanvas.getContext("2d");
  if (!revelioState.pctChart) {
    revelioState.pctChart = new Chart(ctx, config);
  } else {
    revelioState.pctChart.config.type = config.type;
    revelioState.pctChart.data = config.data;
    revelioState.pctChart.options = config.options;
    revelioState.pctChart.update();
  }
};

const pctDiff = (bls, revelio) => {
  const b = toNumber(bls);
  const r = toNumber(revelio);
  if (!Number.isFinite(b) || !Number.isFinite(r) || r === 0) return null;
  return ((b - r) / r) * 100;
};

const segmentColor = (ctx) => {
  const y0 = ctx?.p0?.parsed?.y;
  const y1 = ctx?.p1?.parsed?.y;
  const y = Number.isFinite(y1) ? y1 : y0;
  if (!Number.isFinite(y)) return "#94a3b8";
  return y >= 0 ? "#16a34a" : "#dc2626";
};

const updateRevelioView = async () => {
  if (!revelioElements.viewSelect) return;
  await ensureRevelioLoaded();
  if (!revelioState.loaded) return;

  revelioState.view = revelioElements.viewSelect.value || revelioState.view;
  revelioState.metric = revelioElements.metricSelect?.value || revelioState.metric;
  revelioState.seasonality = revelioElements.seasonalitySelect?.value || revelioState.seasonality;
  revelioState.range = revelioElements.rangeSelect?.value || revelioState.range;
  revelioState.sector = revelioElements.sectorSelect?.value || revelioState.sector;

  if (revelioState.view === "revisions") {
    if (revelioElements.seasonalitySelect) {
      revelioElements.seasonalitySelect.value = "sa";
      revelioElements.seasonalitySelect.disabled = true;
    }
    if (revelioElements.sectorSelect) {
      revelioElements.sectorSelect.disabled = true;
    }
    if (revelioElements.metricSelect) {
      revelioElements.metricSelect.disabled = false;
      if (revelioElements.metricSelect.value === "level") {
        revelioElements.metricSelect.value = "revision";
        revelioState.metric = "revision";
      }
    }
  } else {
    if (revelioElements.seasonalitySelect) revelioElements.seasonalitySelect.disabled = false;
    if (revelioElements.sectorSelect) revelioElements.sectorSelect.disabled = false;
    if (revelioElements.metricSelect) {
      revelioElements.metricSelect.value = "level";
      revelioElements.metricSelect.disabled = true;
    }
  }

  if (revelioState.view === "levels") {
    const sector = revelioState.employment.sectors.find((s) => s.code === revelioState.sector) || null;
    if (!sector) return;
    const isTotal = sector.code === "00" || sector.code === "NF";
    if (!isTotal) {
      setRevelioHint("Selecting a sector switches to NSA automatically (SA is only available for totals).");
    } else if (revelioState.employment.sectors.some((s) => s.code === "NF")) {
      setRevelioHint("Totals: Total private is ADP comparable; Total nonfarm includes government.");
    }
    if (revelioElements.seasonalitySelect) {
      const bothOpt = revelioElements.seasonalitySelect.querySelector('option[value="both"]');
      const saOpt = revelioElements.seasonalitySelect.querySelector('option[value="sa"]');
      const nsaOpt = revelioElements.seasonalitySelect.querySelector('option[value="nsa"]');
      if (bothOpt) bothOpt.disabled = !isTotal;
      if (saOpt) saOpt.disabled = !isTotal;
      if (nsaOpt) nsaOpt.disabled = false;
      revelioElements.seasonalitySelect.disabled = !isTotal;
    }

    // Explorer is built around Total SA/NSA; sector comparisons are NSA-only.
    if (!isTotal) {
      revelioState.seasonality = "nsa";
      if (revelioElements.seasonalitySelect) {
        revelioElements.seasonalitySelect.value = "nsa";
      }
    }

    const windowed = sliceMonthSeries(sector.points, revelioState.range);
    const labels = windowed.map((p) => p.month);

    const latest = windowed[windowed.length - 1] || null;
    const adpAvailable = windowed.some((p) => Number.isFinite(p.sa.adp) || Number.isFinite(p.nsa.adp));
    if (isTotal && revelioState.seasonality === "both") {
      const saGap =
        latest && Number.isFinite(latest.sa.bls) && Number.isFinite(latest.sa.revelio)
          ? latest.sa.bls - latest.sa.revelio
          : null;
      const nsaGap =
        latest && Number.isFinite(latest.nsa.bls) && Number.isFinite(latest.nsa.revelio)
          ? latest.nsa.bls - latest.nsa.revelio
          : null;
      const saGapAdp =
        latest && Number.isFinite(latest.sa.bls) && Number.isFinite(latest.sa.adp)
          ? latest.sa.bls - latest.sa.adp
          : null;
      const nsaGapAdp =
        latest && Number.isFinite(latest.nsa.bls) && Number.isFinite(latest.nsa.adp)
          ? latest.nsa.bls - latest.nsa.adp
          : null;
      renderRevelioMetrics([
        { label: "Sector", value: sector.name },
        { label: "Latest BLS (SA)", value: latest ? formatPeople(latest.sa.bls) : "—" },
        { label: "Latest Revelio (SA)", value: latest ? formatPeople(latest.sa.revelio) : "—" },
        { label: "Gap (SA)", value: formatPeopleDelta(saGap) },
        ...(adpAvailable ? [{ label: "Latest ADP (SA)", value: latest ? formatPeople(latest.sa.adp) : "—" }] : []),
        ...(adpAvailable ? [{ label: "Gap vs ADP (SA)", value: formatPeopleDelta(saGapAdp) }] : []),
        { label: "Latest BLS (NSA)", value: latest ? formatPeople(latest.nsa.bls) : "—" },
        { label: "Latest Revelio (NSA)", value: latest ? formatPeople(latest.nsa.revelio) : "—" },
        { label: "Gap (NSA)", value: formatPeopleDelta(nsaGap) },
        ...(adpAvailable ? [{ label: "Latest ADP (NSA)", value: latest ? formatPeople(latest.nsa.adp) : "—" }] : []),
        ...(adpAvailable ? [{ label: "Gap vs ADP (NSA)", value: formatPeopleDelta(nsaGapAdp) }] : [])
      ]);
    } else {
      const seriesKey = revelioState.seasonality === "nsa" ? "nsa" : "sa";
      const adpGap =
        latest && Number.isFinite(latest[seriesKey].bls) && Number.isFinite(latest[seriesKey].adp)
          ? latest[seriesKey].bls - latest[seriesKey].adp
          : null;
      renderRevelioMetrics([
        { label: "Sector", value: sector.name },
        { label: "Latest BLS", value: latest ? formatPeople(latest[seriesKey].bls) : "—" },
        { label: "Latest Revelio", value: latest ? formatPeople(latest[seriesKey].revelio) : "—" },
        {
          label: "Latest gap (BLS–Revelio)",
          value:
            latest && Number.isFinite(latest[seriesKey].bls) && Number.isFinite(latest[seriesKey].revelio)
              ? formatPeopleDelta(latest[seriesKey].bls - latest[seriesKey].revelio)
              : "—"
        },
        ...(adpAvailable ? [{ label: "Latest ADP", value: latest ? formatPeople(latest[seriesKey].adp) : "—" }] : []),
        ...(adpAvailable ? [{ label: "Latest gap (BLS–ADP)", value: formatPeopleDelta(adpGap) }] : [])
      ]);
    }

    const makeLine = (label, data, color, dashed = false) => ({
      label,
      data,
      borderColor: color,
      backgroundColor: `${color}1f`,
      borderWidth: 2,
      pointRadius: 0,
      tension: 0.2,
      spanGaps: true,
      borderDash: dashed ? [6, 4] : undefined
    });

    const datasets =
      isTotal && revelioState.seasonality === "both"
        ? [
            makeLine("BLS (SA)", windowed.map((p) => p.sa.bls ?? null), "#2563eb"),
            makeLine("Revelio (SA)", windowed.map((p) => p.sa.revelio ?? null), "#16a34a"),
            ...(adpAvailable ? [makeLine("ADP (SA)", windowed.map((p) => p.sa.adp ?? null), "#7c3aed")] : []),
            makeLine("BLS (NSA)", windowed.map((p) => p.nsa.bls ?? null), "#2563eb", true),
            makeLine("Revelio (NSA)", windowed.map((p) => p.nsa.revelio ?? null), "#16a34a", true),
            ...(adpAvailable ? [makeLine("ADP (NSA)", windowed.map((p) => p.nsa.adp ?? null), "#7c3aed", true)] : [])
          ]
        : (() => {
            const seriesKey = revelioState.seasonality === "nsa" ? "nsa" : "sa";
            return [
              makeLine("BLS", windowed.map((p) => p[seriesKey].bls ?? null), "#2563eb"),
              makeLine("Revelio", windowed.map((p) => p[seriesKey].revelio ?? null), "#16a34a"),
              ...(adpAvailable ? [makeLine("ADP", windowed.map((p) => p[seriesKey].adp ?? null), "#7c3aed")] : [])
            ];
          })();

    renderRevelioChart({
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
          y: { ticks: { callback: (value) => Number(value).toLocaleString() } }
        }
      }
    });

    // Percent difference chart (BLS - Revelio), colored by direction.
    const pctDatasets =
      isTotal && revelioState.seasonality === "both"
        ? [
            {
              label: "SA % diff",
              data: windowed.map((p) => pctDiff(p.sa.bls, p.sa.revelio)),
              borderWidth: 2,
              pointRadius: 0,
              tension: 0.2,
              spanGaps: true,
              segment: { borderColor: segmentColor },
              borderColor: "#16a34a"
            },
            ...(adpAvailable
              ? [
                  {
                    label: "SA % diff vs ADP",
                    data: windowed.map((p) => pctDiff(p.sa.bls, p.sa.adp)),
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.2,
                    spanGaps: true,
                    segment: { borderColor: segmentColor },
                    borderColor: "#7c3aed"
                  }
                ]
              : []),
            {
              label: "NSA % diff",
              data: windowed.map((p) => pctDiff(p.nsa.bls, p.nsa.revelio)),
              borderWidth: 2,
              pointRadius: 0,
              tension: 0.2,
              spanGaps: true,
              borderDash: [6, 4],
              segment: { borderColor: segmentColor },
              borderColor: "#16a34a"
            }
            ,
            ...(adpAvailable
              ? [
                  {
                    label: "NSA % diff vs ADP",
                    data: windowed.map((p) => pctDiff(p.nsa.bls, p.nsa.adp)),
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.2,
                    spanGaps: true,
                    borderDash: [6, 4],
                    segment: { borderColor: segmentColor },
                    borderColor: "#7c3aed"
                  }
                ]
              : [])
          ]
        : (() => {
            const seriesKey = revelioState.seasonality === "nsa" ? "nsa" : "sa";
            return [
              {
                label: "% diff",
                data: windowed.map((p) => pctDiff(p[seriesKey].bls, p[seriesKey].revelio)),
                borderWidth: 2,
                pointRadius: 0,
                tension: 0.2,
                spanGaps: true,
                segment: { borderColor: segmentColor },
                borderColor: "#16a34a"
              },
              ...(adpAvailable
                ? [
                    {
                      label: "% diff vs ADP",
                      data: windowed.map((p) => pctDiff(p[seriesKey].bls, p[seriesKey].adp)),
                      borderWidth: 2,
                      pointRadius: 0,
                      tension: 0.2,
                      spanGaps: true,
                      segment: { borderColor: segmentColor },
                      borderColor: "#7c3aed"
                    }
                  ]
                : [])
            ];
          })();

    renderRevelioPctChart({
      type: "line",
      data: { labels, datasets: pctDatasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { position: "bottom" } },
        interaction: { mode: "index", intersect: false },
        scales: {
          y: {
            ticks: {
              callback: (value) => `${Number(value).toFixed(1)}%`
            }
          }
        }
      }
    });

    if (isTotal && revelioState.seasonality === "both") {
      const rows = windowed.slice(-8).reverse().map((p) => ({
        month: p.month,
        sa: { bls: p.sa.bls, revelio: p.sa.revelio },
        nsa: { bls: p.nsa.bls, revelio: p.nsa.revelio },
        adp: { sa: p.sa.adp, nsa: p.nsa.adp }
      }));
      if (revelioElements.tableHead && revelioElements.tableBody) {
        revelioElements.tableHead.innerHTML = `
          <tr>
            <th>Month</th>
            <th>BLS SA</th>
            <th>Revelio SA</th>
            <th>Gap SA</th>
            ${adpAvailable ? "<th>ADP SA</th><th>Gap vs ADP SA</th>" : ""}
            <th>BLS NSA</th>
            <th>Revelio NSA</th>
            <th>Gap NSA</th>
            ${adpAvailable ? "<th>ADP NSA</th><th>Gap vs ADP NSA</th>" : ""}
          </tr>
        `;
        revelioElements.tableBody.innerHTML = rows
          .map((row) => {
            const gapSa =
              Number.isFinite(row.sa.bls) && Number.isFinite(row.sa.revelio)
                ? row.sa.bls - row.sa.revelio
                : null;
            const gapNsa =
              Number.isFinite(row.nsa.bls) && Number.isFinite(row.nsa.revelio)
                ? row.nsa.bls - row.nsa.revelio
                : null;
            const gapSaAdp =
              adpAvailable && Number.isFinite(row.sa.bls) && Number.isFinite(row.adp.sa)
                ? row.sa.bls - row.adp.sa
                : null;
            const gapNsaAdp =
              adpAvailable && Number.isFinite(row.nsa.bls) && Number.isFinite(row.adp.nsa)
                ? row.nsa.bls - row.adp.nsa
                : null;
            return `
              <tr>
                <td>${row.month}</td>
                <td>${formatPeople(row.sa.bls)}</td>
                <td>${formatPeople(row.sa.revelio)}</td>
                <td>${formatPeopleDelta(gapSa)}</td>
                ${adpAvailable ? `<td>${formatPeople(row.adp.sa)}</td><td>${formatPeopleDelta(gapSaAdp)}</td>` : ""}
                <td>${formatPeople(row.nsa.bls)}</td>
                <td>${formatPeople(row.nsa.revelio)}</td>
                <td>${formatPeopleDelta(gapNsa)}</td>
                ${adpAvailable ? `<td>${formatPeople(row.adp.nsa)}</td><td>${formatPeopleDelta(gapNsaAdp)}</td>` : ""}
              </tr>
            `;
          })
          .join("");
      }
    } else {
      const seriesKey = revelioState.seasonality === "nsa" ? "nsa" : "sa";
      const tableRows = windowed.slice(-8).reverse().map((p) => ({
        month: p.month,
        bls: p[seriesKey].bls,
        revelio: p[seriesKey].revelio,
        adp: p[seriesKey].adp
      }));
      renderRevelioTable("levels", tableRows);
    }
    return;
  }

  const series = sliceMonthSeries(revelioState.revisions.series, revelioState.range);
  const labels = series.map((row) => row.month);
  const blsRev = series.map((row) => row.revision.bls ?? null);
  const revRev = series.map((row) => row.revision.revelio ?? null);
  const blsFinal = series.map((row) => row.final.bls ?? null);
  const revFinal = series.map((row) => row.final.revelio ?? null);
  const latest = series[series.length - 1] || null;
  renderRevelioMetrics([
    { label: "Series", value: "Total nonfarm payrolls (SA)" },
    { label: "Latest month", value: latest?.month || "—" },
    { label: "First release", value: latest?.firstRelease || "—" },
    { label: "Last release", value: latest?.lastRelease || "—" },
    { label: "BLS final", value: latest ? formatPeople(latest.final.bls) : "—" },
    { label: "Revelio final", value: latest ? formatPeople(latest.final.revelio) : "—" },
    { label: "BLS Δ (final–first)", value: latest ? formatPeopleDelta(latest.revision.bls) : "—" },
    { label: "Revelio Δ (final–first)", value: latest ? formatPeopleDelta(latest.revision.revelio) : "—" }
  ]);

  const showingRevision = revelioState.metric !== "level";
  renderRevelioChart({
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: showingRevision ? "BLS revision (final–first)" : "BLS final level",
          data: showingRevision ? blsRev : blsFinal,
          borderColor: "#2563eb",
          backgroundColor: "rgba(37, 99, 235, 0.12)",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.2,
          spanGaps: true
        },
        {
          label: showingRevision ? "Revelio revision (final–first)" : "Revelio final level",
          data: showingRevision ? revRev : revFinal,
          borderColor: "#16a34a",
          backgroundColor: "rgba(22, 163, 74, 0.10)",
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
      interaction: { mode: "index", intersect: false },
      scales: {
        y: { ticks: { callback: (value) => Number(value).toLocaleString() } }
      }
    }
  });

  // For revisions view, show percent gap on the chosen metric (revision size or final level).
  const pctLabels = labels;
  const pctSeries =
    showingRevision
      ? series.map((row) => pctDiff(row.revision.bls, row.revision.revelio))
      : series.map((row) => pctDiff(row.final.bls, row.final.revelio));

  renderRevelioPctChart({
    type: "line",
    data: {
      labels: pctLabels,
      datasets: [
        {
          label: "% diff (BLS–Revelio)",
          data: pctSeries,
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.2,
          spanGaps: true,
          segment: { borderColor: segmentColor },
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
          ticks: {
            callback: (value) => `${Number(value).toFixed(1)}%`
          }
        }
      }
    }
  });

  renderRevelioTable("revisions", series.slice(-10).reverse());
};

const initRevelioExplorer = () => {
  if (!revelioElements.viewSelect || !revelioElements.canvas) return;
  setRevelioStatus("Loading BLS vs Revelio datasets…");

  const onControlChange = () => {
    updateRevelioView();
  };

  revelioElements.viewSelect.addEventListener("change", onControlChange);
  revelioElements.metricSelect?.addEventListener("change", onControlChange);
  revelioElements.sectorSelect?.addEventListener("change", onControlChange);
  revelioElements.seasonalitySelect?.addEventListener("change", onControlChange);
  revelioElements.rangeSelect?.addEventListener("change", onControlChange);
  revelioElements.refreshButton?.addEventListener("click", onControlChange);

  updateRevelioView();
};

initRevisionExplorer();
initRevelioExplorer();

if (fredState.apiKey) {
  loadFredSeries();
  loadRateDashboard();
} else {
  updateFredStatus("Enter your FRED API key to start charting.", true);
  updateRateStatus("Add your FRED API key to unlock interest rate charts.", true);
}
