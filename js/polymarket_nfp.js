export const PolymarketNFP = (() => {
  const CLOB_URL = "https://clob.polymarket.com";
  const GAMMA_URL = "https://gamma-api.polymarket.com";

  const state = {
    tracking: false,
    intervalId: null,
    currentMarket: null,
    markets: [],
    dataPoints: [],
    startTimeMs: null,
    priceChart: null
  };

  const elements = {
    providerPanel: null,
    status: null,
    marketSelect: null,
    intervalSelect: null,
    startBtn: null,
    stopBtn: null,
    exportBtn: null,
    marketName: null,
    dataPoints: null,
    duration: null,
    chartCanvas: null,
    pricesBody: null
  };

  const toNumber = (value) => {
    if (value === null || value === undefined) return null;
    if (typeof value === "number") return Number.isFinite(value) ? value : null;
    const s = String(value).trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  };

  const parseJsonArrayish = (value) => {
    if (!value) return null;
    if (Array.isArray(value)) return value;
    if (typeof value !== "string") return null;
    const trimmed = value.trim();
    if (!trimmed) return null;
    try {
      const parsed = JSON.parse(trimmed);
      return Array.isArray(parsed) ? parsed : null;
    } catch (_err) {
      return null;
    }
  };

  const formatPct = (value) => {
    if (!Number.isFinite(value)) return "—";
    return `${(value * 100).toFixed(1)}%`;
  };

  const formatDuration = (ms) => {
    if (!Number.isFinite(ms) || ms < 0) return "—";
    const s = Math.floor(ms / 1000);
    const minutes = Math.floor(s / 60);
    const seconds = s % 60;
    return `${minutes}m ${seconds}s`;
  };

  const isNFPMarket = (market) => {
    const question = String(market?.question || market?.title || "").toLowerCase();
    const slug = String(market?.slug || "").toLowerCase();
    return (
      question.includes("how many jobs added") ||
      slug.startsWith("how-many-jobs-added-in-") ||
      question.includes("jobs added in")
    );
  };

  const parseTimestamp = (value) => {
    const n = toNumber(value);
    if (Number.isFinite(n)) return n > 1e12 ? n : n * 1000;
    const s = String(value || "").trim();
    if (!s) return null;
    const t = Date.parse(s);
    return Number.isFinite(t) ? t : null;
  };

  const pickMostRelevantMarket = (markets) => {
    const scored = markets
      .map((m) => {
        const volume = toNumber(m?.volume) ?? toNumber(m?.liquidity) ?? 0;
        const end = parseTimestamp(m?.end_date ?? m?.close_time ?? m?.closeTime ?? m?.resolved_time);
        const now = Date.now();
        const soon = Number.isFinite(end) ? Math.max(0, end - now) : null;
        return { market: m, volume, soon };
      })
      .sort((a, b) => {
        if (Number.isFinite(a.soon) && Number.isFinite(b.soon) && a.soon !== b.soon) {
          return a.soon - b.soon;
        }
        if (a.volume !== b.volume) return b.volume - a.volume;
        return String(a.market?.slug || "").localeCompare(String(b.market?.slug || ""));
      });
    return scored[0]?.market ?? null;
  };

  const extractOutcomeTokenMap = (market) => {
    const outcomesRaw =
      parseJsonArrayish(market?.outcomes) ||
      parseJsonArrayish(market?.outcomeLabels) ||
      parseJsonArrayish(market?.outcome_labels);
    const tokenIdsRaw =
      parseJsonArrayish(market?.clobTokenIds) ||
      parseJsonArrayish(market?.clobTokenIDs) ||
      parseJsonArrayish(market?.clob_token_ids) ||
      parseJsonArrayish(market?.tokens);

    const outcomes =
      (Array.isArray(outcomesRaw) ? outcomesRaw : [])
        .map((o) => (typeof o === "string" ? o : o?.name ?? o?.label ?? null))
        .filter(Boolean) || [];

    const tokenIds =
      (Array.isArray(tokenIdsRaw) ? tokenIdsRaw : [])
        .map((t) => {
          if (typeof t === "string") return t;
          if (typeof t === "number") return String(t);
          return t?.token_id ?? t?.tokenId ?? t?.id ?? null;
        })
        .filter(Boolean) || [];

    if (outcomes.length && outcomes.length === tokenIds.length) {
      const map = {};
      outcomes.forEach((outcome, idx) => {
        map[outcome] = String(tokenIds[idx]);
      });
      return { outcomes, tokenIds, map };
    }

    // Fallback: try `tokens` array of objects with `outcome` + `token_id`.
    if (Array.isArray(market?.tokens)) {
      const map = {};
      const outcomeLabels = [];
      const tokenList = [];
      for (const t of market.tokens) {
        const label = t?.outcome ?? t?.label ?? t?.name ?? null;
        const id = t?.token_id ?? t?.tokenId ?? t?.id ?? null;
        if (!label || !id) continue;
        outcomeLabels.push(String(label));
        tokenList.push(String(id));
        map[String(label)] = String(id);
      }
      if (outcomeLabels.length) {
        return { outcomes: outcomeLabels, tokenIds: tokenList, map };
      }
    }

    return { outcomes: [], tokenIds: [], map: {} };
  };

  const apiFetchJson = async (url) => {
    const response = await fetch(url, { cache: "no-cache" });
    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(text || `HTTP ${response.status}`);
    }
    return await response.json();
  };

  const fetchNFPMarkets = async () => {
    const limit = 200;
    const maxPages = 8;
    let offset = 0;
    const out = [];
    for (let page = 0; page < maxPages; page += 1) {
      const url = `${GAMMA_URL}/markets?active=true&limit=${limit}&offset=${offset}`;
      const payload = await apiFetchJson(url);
      const markets = Array.isArray(payload) ? payload : payload?.markets || payload?.data || [];
      if (!Array.isArray(markets) || !markets.length) break;
      out.push(...markets.filter(isNFPMarket));
      if (markets.length < limit) break;
      offset += limit;
    }
    return out;
  };

  const midpointUrl = (tokenId) =>
    `${CLOB_URL}/midpoint?token_id=${encodeURIComponent(String(tokenId))}`;

  const fetchMidpoint = async (tokenId) => {
    const payload = await apiFetchJson(midpointUrl(tokenId));
    const raw = payload?.mid ?? payload?.midpoint ?? payload?.price ?? 0;
    return toNumber(raw) ?? 0;
  };

  const getAllPrices = async (tokenIds) => {
    const ids = (Array.isArray(tokenIds) ? tokenIds : []).map(String);
    const values = await Promise.all(ids.map((id) => fetchMidpoint(id).catch(() => 0)));
    const out = {};
    ids.forEach((id, idx) => {
      out[id] = values[idx] ?? 0;
    });
    return out;
  };

  const setStatus = (message, isError = false) => {
    if (!elements.status) return;
    elements.status.textContent = message;
    elements.status.classList.toggle("error", Boolean(isError));
  };

  const updateButtons = () => {
    if (elements.startBtn) elements.startBtn.disabled = state.tracking;
    if (elements.stopBtn) elements.stopBtn.disabled = !state.tracking;
    if (elements.exportBtn) elements.exportBtn.disabled = !state.dataPoints.length;
  };

  const updateMarketSelect = () => {
    if (!elements.marketSelect) return;
    if (!state.markets.length) {
      elements.marketSelect.innerHTML = '<option value="">No markets found</option>';
      return;
    }
    elements.marketSelect.innerHTML = state.markets
      .map((m) => {
        const slug = String(m.slug || "");
        const question = String(m.question || m.title || slug);
        return `<option value="${slug}">${question}</option>`;
      })
      .join("");
    const current = state.currentMarket?.slug;
    if (current) elements.marketSelect.value = current;
  };

  const updatePricesTable = (dataPoint) => {
    if (!elements.pricesBody || !state.currentMarket) return;
    elements.pricesBody.innerHTML = "";
    const outcomes = state.currentMarket.outcomes || [];
    outcomes.forEach((label) => {
      const price = dataPoint?.outcomes?.[label];
      const row = document.createElement("tr");
      row.innerHTML = `<td>${label}</td><td>${formatPct(toNumber(price))}</td>`;
      elements.pricesBody.appendChild(row);
    });
  };

  const ensureChart = () => {
    if (state.priceChart || !elements.chartCanvas || typeof Chart === "undefined") return;
    const ctx = elements.chartCanvas.getContext("2d");
    if (!ctx) return;
    state.priceChart = new Chart(ctx, {
      type: "line",
      data: { datasets: [] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: "right",
            labels: { boxWidth: 10, font: { size: 10 } }
          }
        },
        interaction: { mode: "nearest", intersect: false },
        scales: {
          x: {
            type: "linear",
            ticks: {
              callback: (value) => {
                const t = Number(value);
                if (!Number.isFinite(t)) return "";
                const d = new Date(t);
                if (Number.isNaN(d.getTime())) return "";
                return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
              }
            },
            title: { display: true, text: "Time" }
          },
          y: {
            min: 0,
            max: 1,
            ticks: { callback: (v) => `${Math.round(Number(v) * 100)}%` },
            title: { display: true, text: "Probability" }
          }
        }
      }
    });
  };

  const palette = [
    "#16a34a",
    "#dc2626",
    "#2563eb",
    "#ea580c",
    "#7c3aed",
    "#0891b2",
    "#ca8a04",
    "#db2777"
  ];

  const updateChart = () => {
    if (!state.priceChart || !state.currentMarket) return;
    const outcomes = state.currentMarket.outcomes || [];
    const datasets = outcomes.map((label, idx) => ({
      label,
      data: state.dataPoints.map((dp) => ({ x: dp.ts_ms, y: toNumber(dp.outcomes?.[label]) })),
      borderColor: palette[idx % palette.length],
      backgroundColor: "transparent",
      borderWidth: 2,
      pointRadius: 1,
      spanGaps: true,
      tension: 0.2
    }));
    state.priceChart.data.datasets = datasets;
    state.priceChart.update("none");
  };

  const updateUI = (dataPoint) => {
    if (elements.marketName && state.currentMarket) {
      elements.marketName.textContent = state.currentMarket.question || state.currentMarket.slug || "—";
    }
    if (elements.dataPoints) {
      elements.dataPoints.textContent = String(state.dataPoints.length);
    }
    if (elements.duration) {
      const duration = state.startTimeMs ? Date.now() - state.startTimeMs : null;
      elements.duration.textContent = formatDuration(duration);
    }
    updatePricesTable(dataPoint);
  };

  const collectDataPoint = async () => {
    if (!state.currentMarket) return;
    const now = Date.now();
    const pricesByToken = await getAllPrices(state.currentMarket.tokenIds);
    const dp = { ts_ms: now, iso: new Date(now).toISOString(), outcomes: {} };
    state.currentMarket.outcomes.forEach((label) => {
      const tokenId = state.currentMarket.map[label];
      dp.outcomes[label] = tokenId ? pricesByToken[String(tokenId)] ?? 0 : 0;
    });
    state.dataPoints.push(dp);
    updateUI(dp);
    updateChart();
    updateButtons();
  };

  const stopTracking = () => {
    if (state.intervalId) {
      clearInterval(state.intervalId);
      state.intervalId = null;
    }
    state.tracking = false;
    setStatus("Tracking stopped.");
    updateButtons();
  };

  const startTracking = async (intervalSeconds = 60) => {
    if (state.tracking) return;
    if (!state.currentMarket) {
      setStatus("No Polymarket NFP market selected.", true);
      return;
    }
    const interval = toNumber(intervalSeconds);
    if (!Number.isFinite(interval) || interval <= 0) {
      setStatus("Invalid interval.", true);
      return;
    }
    state.tracking = true;
    state.startTimeMs = Date.now();
    state.dataPoints = [];
    setStatus(`Tracking: ${state.currentMarket.question}`);
    updateButtons();
    await collectDataPoint();
    state.intervalId = setInterval(() => {
      collectDataPoint().catch((err) => setStatus(`Error: ${err.message}`, true));
    }, interval * 1000);
  };

  const exportData = () => {
    if (!state.currentMarket) return;
    const data = {
      provider: "polymarket",
      market: state.currentMarket,
      start_time: state.startTimeMs ? new Date(state.startTimeMs).toISOString() : null,
      end_time: new Date().toISOString(),
      data_points: state.dataPoints
    };
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `nfp_${state.currentMarket.slug || "polymarket"}_${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const setCurrentMarketBySlug = (slug) => {
    const wanted = String(slug || "").trim();
    const picked = state.markets.find((m) => String(m.slug || "") === wanted) || null;
    if (!picked) return;
    const { outcomes, tokenIds, map } = extractOutcomeTokenMap(picked);
    state.currentMarket = {
      question: String(picked.question || picked.title || wanted),
      slug: String(picked.slug || wanted),
      condition_id: picked.condition_id ?? picked.conditionId ?? null,
      outcomes,
      tokenIds,
      map
    };
    setStatus(`Selected: ${state.currentMarket.question}`);
    updateMarketSelect();
    updateButtons();
    updateChart();
  };

  const refreshMarketList = async () => {
    setStatus("Finding active Polymarket NFP markets…");
    const markets = await fetchNFPMarkets();
    state.markets = markets;
    const picked = pickMostRelevantMarket(markets);
    if (!picked) {
      state.currentMarket = null;
      updateMarketSelect();
      setStatus("No active NFP market found.", true);
      return;
    }
    setCurrentMarketBySlug(picked.slug);
  };

  const init = () => {
    elements.status = document.getElementById("nfpStatus");
    elements.marketSelect = document.getElementById("nfpMarketSelect");
    elements.intervalSelect = document.getElementById("nfpInterval");
    elements.startBtn = document.getElementById("nfpStartBtn");
    elements.stopBtn = document.getElementById("nfpStopBtn");
    elements.exportBtn = document.getElementById("nfpExportBtn");
    elements.marketName = document.getElementById("nfpMarketName");
    elements.dataPoints = document.getElementById("nfpDataPoints");
    elements.duration = document.getElementById("nfpDuration");
    elements.chartCanvas = document.getElementById("nfpPriceChart");
    elements.pricesBody = document.getElementById("nfpPricesTableBody");

    ensureChart();
    updateButtons();

    if (elements.marketSelect) {
      elements.marketSelect.addEventListener("change", () => {
        stopTracking();
        setCurrentMarketBySlug(elements.marketSelect.value);
      });
    }
    if (elements.startBtn) {
      elements.startBtn.addEventListener("click", () => {
        const interval = toNumber(elements.intervalSelect?.value) ?? 60;
        startTracking(interval).catch((err) => setStatus(`Error: ${err.message}`, true));
      });
    }
    if (elements.stopBtn) {
      elements.stopBtn.addEventListener("click", stopTracking);
    }
    if (elements.exportBtn) {
      elements.exportBtn.addEventListener("click", exportData);
    }

    // Lazy-load markets only if the panel exists on the page.
    if (elements.status || elements.marketSelect) {
      refreshMarketList().catch((err) => setStatus(`Error: ${err.message}`, true));
    }
  };

  return {
    CLOB_URL,
    GAMMA_URL,
    state,
    init,
    refreshMarketList,
    startTracking,
    stopTracking
  };
})();

