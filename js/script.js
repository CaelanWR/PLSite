const topics = [
  { label: "Labor Market", query: "labor market", isPrivate: false },
  { label: "Unemployment", query: "unemployment", isPrivate: false },
  { label: "BLS Jobs Report", query: "\"BLS jobs report\"", isPrivate: false },
  { label: "Wage Growth", query: "\"wage growth\"", isPrivate: false },
  { label: "Job Openings", query: "\"job openings\"", isPrivate: false },
  { label: "JOLTS", query: "\"JOLTS report\"", isPrivate: false },
  { label: "ADP Employment", query: "\"ADP employment\"", isPrivate: true },
  { label: "Revelio Labs", query: "\"Revelio Labs\"", isPrivate: true },
  { label: "Horsefly Analytics", query: "\"Horsefly Analytics\" labor market", isPrivate: true },
  { label: "Lightcast", query: "\"Lightcast\" labor market", isPrivate: true }
];

const sourcePriority = new Map(
  [
    ["The Wall Street Journal", 5],
    ["Wall Street Journal", 5],
    ["Financial Times", 5],
    ["Bloomberg", 5],
    ["Reuters", 4],
    ["The New York Times", 4],
    ["New York Times", 4],
    ["The Economist", 4],
    ["CNBC", 3],
    ["Forbes", 3],
    ["Fortune", 3],
    ["Quartz", 2],
    ["Business Insider", 2]
  ].map(([source, weight]) => [source.toLowerCase(), weight])
);

const feedEl = document.getElementById("feed");
const statusEl = document.getElementById("status");
const filterInput = document.getElementById("filterInput");
const sortSelect = document.getElementById("sortSelect");
const refreshButton = document.getElementById("refreshButton");
const loadMoreButton = document.getElementById("loadMoreButton");
const articleTemplate = document.getElementById("article-card-template");
const regionSelect = document.getElementById("regionSelect");
const priorityToggle = document.getElementById("priorityToggle");
const privateToggle = document.getElementById("privateToggle");

const SAVED_ARTICLES_KEY = "market_pulse_saved_articles_v1";

const saveModal = document.getElementById("saveArticleModal");
const closeSaveModalBtn = document.getElementById("closeSaveArticle");
const saveArticleForm = document.getElementById("saveArticleForm");
const saveArticleTitleEl = document.getElementById("saveArticleTitle");
const saveArticleTagsInput = document.getElementById("saveArticleTags");
const saveArticleNoteInput = document.getElementById("saveArticleNote");
const saveArticleStatus = document.getElementById("saveArticleStatus");

// Google News RSS "editions" are country+language specific. "Regions" below are either
// a single edition or a lightweight aggregation of a few editions.
const regions = {
  global: {
    label: "Global (English)",
    editions: [{ label: "US", hl: "en-US", gl: "US", ceid: "US:en" }]
  },
  us: {
    label: "United States",
    editions: [{ label: "US", hl: "en-US", gl: "US", ceid: "US:en" }]
  },
  ca: {
    label: "Canada",
    editions: [{ label: "CA", hl: "en-CA", gl: "CA", ceid: "CA:en" }]
  },
  uk: {
    label: "United Kingdom",
    editions: [{ label: "GB", hl: "en-GB", gl: "GB", ceid: "GB:en" }]
  },
  au: {
    label: "Australia",
    editions: [{ label: "AU", hl: "en-AU", gl: "AU", ceid: "AU:en" }]
  },
  in: {
    label: "India",
    editions: [{ label: "IN", hl: "en-IN", gl: "IN", ceid: "IN:en" }]
  },
  sg: {
    label: "Singapore",
    editions: [{ label: "SG", hl: "en-SG", gl: "SG", ceid: "SG:en" }]
  },
  eu: {
    label: "Europe (UK + Ireland)",
    editions: [
      { label: "GB", hl: "en-GB", gl: "GB", ceid: "GB:en" },
      { label: "IE", hl: "en-IE", gl: "IE", ceid: "IE:en" }
    ]
  },
  apac: {
    label: "APAC (AU + IN + SG)",
    editions: [
      { label: "AU", hl: "en-AU", gl: "AU", ceid: "AU:en" },
      { label: "IN", hl: "en-IN", gl: "IN", ceid: "IN:en" },
      { label: "SG", hl: "en-SG", gl: "SG", ceid: "SG:en" }
    ]
  }
};

const initialRegion = regionSelect?.value || "global";

const state = {
  articles: [],
  loading: false,
  lastUpdated: null,
  selectedRegion: initialRegion,
  prioritizeTopSources: true,
  includePrivateFeeds: true,
  sortMode: sortSelect?.value || "relevance",
  renderLimit: 60,
  savedArticleKeys: new Set(),
  pendingSaveArticle: null
};

const proxyChain = [
  {
    name: "AllOrigins",
    build: (url) => `https://api.allorigins.win/raw?url=${encodeURIComponent(url)}`
  },
  {
    name: "CorsProxyIO",
    build: (url) => `https://corsproxy.io/?${encodeURIComponent(url)}`
  },
  {
    name: "ThingProxy",
    build: (url) => `https://thingproxy.freeboard.io/fetch/${url}`
  },
  {
    name: "IsomorphicGit",
    build: (url) => `https://cors.isomorphic-git.org/${url}`
  },
  {
    name: "JinaReader",
    build: (url) => {
      const parsed = new URL(url);
      const path = `${parsed.host}${parsed.pathname}${parsed.search}${parsed.hash}`;
      return `https://r.jina.ai/http://${path}`;
    }
  }
];

const fetchWithFallback = async (url) => {
  const attempts = [];

  for (const proxy of proxyChain) {
    try {
      const response = await fetch(proxy.build(url));
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const text = await response.text();
      return { text, proxy: proxy.name };
    } catch (err) {
      attempts.push(`${proxy.name}: ${err.message}`);
    }
  }

  throw new Error(attempts.join("; "));
};

const sanitizeSummary = (snippet = "") => {
  const parsed = new DOMParser().parseFromString(snippet, "text/html");
  return parsed.body.textContent?.trim().replace(/\s+/g, " ") ?? "";
};

const formatDate = (isoString) => {
  const date = isoString ? new Date(isoString) : null;
  if (!date || Number.isNaN(date)) {
    return "Unknown date";
  }
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  }).format(date);
};

const parseRSSItems = (xmlString, topicLabel) => {
  const doc = new DOMParser().parseFromString(xmlString, "text/xml");
  const parserError = doc.querySelector("parsererror");
  if (parserError) {
    throw new Error("Unable to parse feed.");
  }

  const items = Array.from(doc.querySelectorAll("item"));
  return items.map((item) => {
    const title = item.querySelector("title")?.textContent?.trim() ?? "";
    const link = item.querySelector("link")?.textContent?.trim() ?? "";
    const summary = sanitizeSummary(
      item.querySelector("description")?.textContent ?? ""
    );
    const source =
      item.querySelector("source")?.textContent?.trim() ??
      title.split(" - ").pop() ??
      "Unknown source";
    const pubDate = item.querySelector("pubDate")?.textContent ?? "";
    return {
      title,
      link,
      summary,
      source,
      pubDate,
      topic: topicLabel,
      topics: [topicLabel]
    };
  });
};

const normalizeLinkKey = (link = "", fallback = "") => {
  if (link) {
    try {
      const url = new URL(link);
      url.hash = "";
      // Strip common tracking parameters so the same story doesn't appear twice.
      const trackingPrefixes = ["utm_", "fbclid", "gclid", "mc_cid", "mc_eid"];
      for (const key of Array.from(url.searchParams.keys())) {
        const lower = key.toLowerCase();
        if (trackingPrefixes.some((prefix) => lower.startsWith(prefix))) {
          url.searchParams.delete(key);
        }
      }
      return url.toString();
    } catch {
      return link.trim();
    }
  }
  return fallback.toLowerCase();
};

const loadSavedArticles = () => {
  try {
    const raw = localStorage.getItem(SAVED_ARTICLES_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

const saveSavedArticles = (articles) => {
  localStorage.setItem(SAVED_ARTICLES_KEY, JSON.stringify(articles));
};

const refreshSavedKeys = () => {
  const keys = new Set();
  loadSavedArticles().forEach((a) => {
    const key = a?.key;
    if (key) keys.add(String(key));
  });
  state.savedArticleKeys = keys;
};

const splitTags = (value) =>
  String(value || "")
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);

const openSaveModal = (article) => {
  if (!saveModal || !saveArticleForm) return;
  state.pendingSaveArticle = article;
  if (saveArticleTitleEl) saveArticleTitleEl.textContent = article.title || "Untitled";
  if (saveArticleTagsInput) saveArticleTagsInput.value = (article.topics || []).join(", ");
  if (saveArticleNoteInput) saveArticleNoteInput.value = "";
  if (saveArticleStatus) saveArticleStatus.textContent = "";
  saveModal.classList.remove("hidden");
  setTimeout(() => saveArticleTagsInput?.focus(), 0);
};

const closeSaveModal = () => {
  saveModal?.classList.add("hidden");
  state.pendingSaveArticle = null;
  saveArticleForm?.reset();
  if (saveArticleStatus) saveArticleStatus.textContent = "";
};

const upsertSavedArticle = (article, extraTags, note) => {
  const saved = loadSavedArticles();
  const key = normalizeLinkKey(article.link, article.title);
  const existingIdx = saved.findIndex((a) => String(a.key) === String(key));

  const mergedTags = Array.from(
    new Set([...(article.topics || []), ...(extraTags || [])].filter(Boolean))
  );

  const payload = {
    id: existingIdx >= 0 ? saved[existingIdx].id : (crypto.randomUUID ? crypto.randomUUID() : Date.now().toString()),
    key,
    title: article.title,
    url: article.link,
    source: article.source,
    pubDate: article.pubDate,
    summary: article.summary,
    tags: mergedTags,
    note: note || "",
    savedAt: new Date().toISOString()
  };

  if (existingIdx >= 0) {
    saved[existingIdx] = { ...saved[existingIdx], ...payload };
  } else {
    saved.push(payload);
  }
  saveSavedArticles(saved);
  refreshSavedKeys();
};

const buildTagRules = () => [
  { tag: "Revelio", re: /\brevelio\b|\brpls\b|public\s+labor\s+statistics/i },
  { tag: "ADP", re: /\badp\b|national employment report|pay insights/i },
  { tag: "BLS", re: /\bbls\b|bureau of labor statistics|employment situation|jobs report|nonfarm payroll/i },
  { tag: "JOLTS", re: /\bjolts\b|job openings and labor turnover|quits rate|hires rate/i },
  { tag: "Claims", re: /jobless claims|initial claims|continuing claims/i },
  { tag: "Wages", re: /wage growth|average hourly earnings|compensation|eci\b/i },
  { tag: "Inflation", re: /\bcpi\b|inflation|price pressures|ppi\b/i },
  { tag: "Openings", re: /job openings|vacancies|vacancy rate/i },
  { tag: "Layoffs", re: /layoffs|job cuts|reductions? in force|\brif\b/i },
  { tag: "Hiring", re: /\bhiring\b|new hires|recruiting/i },
  { tag: "Revisions", re: /revisions?|benchmark revision|annual benchmark/i }
];

const inferTags = (article) => {
  const text = [article.title, article.summary, article.source].join(" ").toLowerCase();
  const tags = new Set();
  buildTagRules().forEach(({ tag, re }) => {
    if (re.test(text)) tags.add(tag);
  });
  // Preserve the feed topic tag as well.
  (article.topics || [article.topic]).forEach((t) => tags.add(t));
  return Array.from(tags);
};

const splitTerms = (query) =>
  String(query || "")
    .toLowerCase()
    .split(/[\s,]+/)
    .map((t) => t.trim())
    .filter(Boolean);

const computeRelevanceScore = (article, query) => {
  const priority = getSourcePriority(article.source);
  const ts = new Date(article.pubDate).getTime() || 0;
  const hoursAgo = ts ? (Date.now() - ts) / (1000 * 60 * 60) : 1e9;
  const recency = Math.max(0, 72 - hoursAgo); // boost within ~3 days

  const tags = Array.isArray(article.topics) ? article.topics : [article.topic];
  const tagBoost = tags.length * 1.25;

  const haystack = [article.title, article.summary, article.source, tags.join(" ")].join(" ").toLowerCase();
  const terms = splitTerms(query);
  const termHits = terms.length
    ? terms.reduce((acc, term) => (haystack.includes(term) ? acc + 6 : acc), 0)
    : 0;

  const revelioBoost = /\brevelio\b|\brpls\b/.test(haystack) ? 8 : 0;
  const privateBoost = /\badp\b|horsefly|lightcast/.test(haystack) ? 2 : 0;

  return priority * 8 + recency + tagBoost + termHits + revelioBoost + privateBoost;
};

const mergeArticlesByLink = (articles) => {
  const merged = new Map();
  articles.forEach((article) => {
    const key = normalizeLinkKey(article.link, article.title);
    const existing = merged.get(key);
    if (existing) {
      const combinedTopics = new Set([...(existing.topics || []), article.topic]);
      existing.topics = Array.from(combinedTopics);
      existing.topic = existing.topics[0];
      if (!existing.summary && article.summary) {
        existing.summary = article.summary;
      }
      if (!existing.source && article.source) {
        existing.source = article.source;
      }
      if (!existing.pubDate && article.pubDate) {
        existing.pubDate = article.pubDate;
      }
    } else {
      merged.set(key, {
        ...article,
        topics: article.topics?.length ? [...article.topics] : [article.topic]
      });
    }
  });
  return Array.from(merged.values()).map((article) => ({
    ...article,
    topics: inferTags(article)
  }));
};

const getActiveEditions = () => {
  const region = regions[state.selectedRegion] ?? regions.global;
  return region.editions?.length ? region.editions : regions.global.editions;
};

const buildRSSUrl = (rawQuery, edition) => {
  const encodedQuery = encodeURIComponent(rawQuery);
  return `https://news.google.com/rss/search?hl=${edition.hl}&gl=${edition.gl}&ceid=${edition.ceid}&q=${encodedQuery}`;
};

const fetchTopicArticles = async (topic) => {
  const editions = getActiveEditions();
  const results = await Promise.allSettled(
    editions.map(async (edition) => {
      const rssUrl = buildRSSUrl(topic.query, edition);
      const { text } = await fetchWithFallback(rssUrl);
      const items = parseRSSItems(text, topic.label);
      return items.map((item) => ({
        ...item,
        edition: edition.label
      }));
    })
  );

  const aggregated = [];
  const errors = [];
  results.forEach((result, idx) => {
    const editionLabel = editions[idx]?.label ?? "Edition";
    if (result.status === "fulfilled") {
      aggregated.push(...result.value);
    } else {
      errors.push(`${editionLabel}: ${result.reason?.message ?? "Error"}`);
    }
  });
  if (errors.length) {
    const err = new Error(errors.join("; "));
    err.name = "EditionFetchError";
    throw err;
  }
  return aggregated;
};

const getSourcePriority = (sourceName = "") =>
  sourcePriority.get(sourceName.toLowerCase()) ?? 0;

const getSortedArticles = () => {
  const items = [...state.articles];
  items.sort((a, b) => {
    const dateA = new Date(a.pubDate).getTime() || 0;
    const dateB = new Date(b.pubDate).getTime() || 0;
    const mode = state.sortMode || "recent";
    const keyword = filterInput?.value?.trim() || "";

    if (mode === "relevance") {
      const scoreA = computeRelevanceScore(a, keyword);
      const scoreB = computeRelevanceScore(b, keyword);
      if (scoreA !== scoreB) return scoreB - scoreA;
      return dateB - dateA;
    }

    if (mode === "top" || state.prioritizeTopSources) {
      const priorityA = getSourcePriority(a.source);
      const priorityB = getSourcePriority(b.source);
      if (priorityA !== priorityB) return priorityB - priorityA;
    }
    return dateB - dateA;
  });
  return items;
};

const refreshArticles = async () => {
  if (state.loading) return;
  state.renderLimit = 60;
  const activeTopics = topics.filter(
    (topic) => state.includePrivateFeeds || !topic.isPrivate
  );
  if (!activeTopics.length) {
    state.articles = [];
    renderArticles();
    statusEl.textContent = "No topics selected. Enable at least one feed.";
    return;
  }

  state.loading = true;
  statusEl.textContent = "Fetching the latest labor market coverage…";
  refreshButton.disabled = true;
  refreshButton.textContent = "Loading…";
  feedEl.innerHTML = "";

  const results = await Promise.allSettled(activeTopics.map(fetchTopicArticles));
  const aggregated = [];
  const errors = [];

  results.forEach((result, idx) => {
    if (result.status === "fulfilled") {
      aggregated.push(...result.value);
    } else {
      const topicLabel = activeTopics[idx]?.label ?? "Unknown topic";
      errors.push(`${topicLabel}: ${result.reason?.message ?? "Error"}`);
    }
  });

  const mergedArticles = mergeArticlesByLink(aggregated);
  state.articles = mergedArticles;
  state.lastUpdated = new Date();
  state.loading = false;

  const regionObj = regions[state.selectedRegion] ?? regions.global;
  const regionName = regionObj.label ?? "Selected region";
  const editionLabel =
    regionObj.editions?.length && regionObj.editions.length > 1
      ? ` (${regionObj.editions.map((e) => e.label).join(", ")})`
      : "";
  const scopeLabel = state.includePrivateFeeds ? "All sources" : "Public sources only";
  const statusMessage =
    aggregated.length > 0
      ? `Showing ${aggregated.length} articles for ${regionName}${editionLabel} (${scopeLabel}) — updated ${state.lastUpdated.toLocaleTimeString()}.`
      : `No articles available for ${regionName}${editionLabel} (${scopeLabel}) at the moment.`;
  const errorMessage =
    errors.length > 0
      ? ` Some feeds could not be loaded (${errors.join("; ")}).`
      : "";
  statusEl.textContent = `${statusMessage}${errorMessage}`;

  refreshButton.disabled = false;
  refreshButton.textContent = "Refresh";

  renderArticles();
};

const renderArticles = () => {
  const keyword = filterInput.value.trim().toLowerCase();
  const sortedArticles = getSortedArticles();
  const filtered = sortedArticles.filter((article) => {
    if (!keyword) return true;
    const haystack = [
      article.title,
      article.summary,
      article.source,
      Array.isArray(article.topics) ? article.topics.join(" ") : article.topic
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(keyword);
  });

  feedEl.innerHTML = "";

  if (!filtered.length) {
    feedEl.innerHTML =
      '<div class="empty-state">No articles match that filter yet. Try another keyword.</div>';
    if (loadMoreButton) loadMoreButton.style.display = "none";
    return;
  }

  const visible = filtered.slice(0, state.renderLimit);
  const fragment = document.createDocumentFragment();
  visible.forEach((article) => {
    const clone = articleTemplate.content.cloneNode(true);
    const pillsContainer = clone.querySelector(".topic-pills");
    if (pillsContainer) {
      pillsContainer.innerHTML = "";
      (article.topics || [article.topic]).forEach((topicLabel) => {
        const pill = document.createElement("span");
        pill.className = "topic-pill";
        pill.textContent = topicLabel;
        pill.title = `Filter by "${topicLabel}"`;
        pill.addEventListener("click", () => {
          filterInput.value = topicLabel;
          state.renderLimit = 60;
          renderArticles();
        });
        pillsContainer.appendChild(pill);
      });
    }
    clone.querySelector(".source").textContent = article.source;
    clone.querySelector("time").textContent = formatDate(article.pubDate);

    const titleEl = clone.querySelector(".card-title");
    titleEl.textContent = article.title;
    titleEl.href = article.link;

    const summaryEl = clone.querySelector(".summary");
    summaryEl.textContent = article.summary || "No summary available.";

    clone.querySelector(".read-more").href = article.link;

    const saveBtn = clone.querySelector(".save-article");
    if (saveBtn) {
      const key = normalizeLinkKey(article.link, article.title);
      const alreadySaved = state.savedArticleKeys.has(key);
      saveBtn.textContent = alreadySaved ? "Saved" : "Save";
      saveBtn.classList.toggle("saved", alreadySaved);
      saveBtn.disabled = alreadySaved;
      if (!alreadySaved) {
        saveBtn.addEventListener("click", () => openSaveModal(article));
      }
    }

    fragment.appendChild(clone);
  });

  feedEl.appendChild(fragment);

  if (loadMoreButton) {
    const remaining = filtered.length - visible.length;
    loadMoreButton.style.display = remaining > 0 ? "inline-flex" : "none";
    loadMoreButton.textContent = remaining > 0 ? `Load more (${remaining.toLocaleString()} left)` : "Load more";
  }
};

const updatePriorityButton = () => {
  if (!priorityToggle) return;
  const label = state.prioritizeTopSources ? "On" : "Off";
  priorityToggle.textContent = `Top sources: ${label}`;
  priorityToggle.setAttribute("aria-pressed", String(state.prioritizeTopSources));
  priorityToggle.classList.toggle("active", state.prioritizeTopSources);
};

const updatePrivateButton = () => {
  if (!privateToggle) return;
  const label = state.includePrivateFeeds ? "On" : "Off";
  privateToggle.textContent = `Private data: ${label}`;
  privateToggle.setAttribute("aria-pressed", String(state.includePrivateFeeds));
  privateToggle.classList.toggle("active", state.includePrivateFeeds);
};

filterInput.addEventListener("input", renderArticles);
refreshButton.addEventListener("click", refreshArticles);
if (sortSelect) {
  sortSelect.addEventListener("change", () => {
    state.sortMode = sortSelect.value;
    state.renderLimit = 60;
    renderArticles();
  });
}
if (loadMoreButton) {
  loadMoreButton.addEventListener("click", () => {
    state.renderLimit += 60;
    renderArticles();
  });
}
if (regionSelect) {
  regionSelect.addEventListener("change", () => {
    state.selectedRegion = regionSelect.value;
    refreshArticles();
  });
}
if (priorityToggle) {
  priorityToggle.addEventListener("click", () => {
    state.prioritizeTopSources = !state.prioritizeTopSources;
    updatePriorityButton();
    if (state.sortMode !== "relevance") {
      state.sortMode = state.prioritizeTopSources ? "top" : "recent";
      if (sortSelect) sortSelect.value = state.sortMode;
    }
    renderArticles();
  });
}
if (privateToggle) {
  privateToggle.addEventListener("click", () => {
    state.includePrivateFeeds = !state.includePrivateFeeds;
    updatePrivateButton();
    refreshArticles();
  });
}
updatePriorityButton();
updatePrivateButton();
if (regionSelect) {
  regionSelect.value = state.selectedRegion;
}
if (sortSelect) {
  sortSelect.value = state.sortMode;
}

// Save modal handlers (saves to Links page library)
if (closeSaveModalBtn) closeSaveModalBtn.addEventListener("click", closeSaveModal);
if (saveModal) {
  saveModal.addEventListener("click", (event) => {
    if (event.target === saveModal) closeSaveModal();
  });
}
if (saveArticleForm) {
  saveArticleForm.addEventListener("submit", (event) => {
    event.preventDefault();
    const article = state.pendingSaveArticle;
    if (!article) return;
    const tags = splitTags(saveArticleTagsInput?.value || "");
    const note = (saveArticleNoteInput?.value || "").trim();
    upsertSavedArticle(article, tags, note);
    closeSaveModal();
    renderArticles();
  });
}

refreshSavedKeys();

refreshArticles().catch((err) => {
  statusEl.textContent = `Unable to load articles: ${err.message}`;
  refreshButton.disabled = false;
  refreshButton.textContent = "Refresh";
  state.loading = false;
});
