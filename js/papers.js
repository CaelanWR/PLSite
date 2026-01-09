const PROXY_CHAIN = [
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

const SOURCES = [
  {
    id: "arxiv",
    label: "arXiv Econ/Fin",
    type: "arxiv",
    url:
      "https://export.arxiv.org/api/query?search_query=all:%22labor%20market%22+OR+cat:q-fin.EC+OR+cat:econ.EM+OR+cat:econ.GN&sortBy=submittedDate&sortOrder=descending&max_results=75"
  },
  {
    id: "nber",
    label: "NBER",
    type: "rss",
    url: "https://www.nber.org/rss/new.xml"
  }
];

const dom = {
  list: document.getElementById("paperList"),
  status: document.getElementById("papersStatus"),
  filter: document.getElementById("paperFilter"),
  refresh: document.getElementById("papersRefresh"),
  sourceFilters: document.getElementById("sourceFilters"),
  laborToggle: document.getElementById("laborToggle")
};

const state = {
  items: [],
  filter: "",
  loading: false,
  errors: [],
  activeSources: new Set(SOURCES.map((source) => source.id)),
  laborOnly: true
};

const LABOR_KEYWORDS = [
  "labor",
  "employment",
  "unemployment",
  "wages",
  "job",
  "workforce",
  "labor market",
  "occupational",
  "hiring",
  "earnings",
  "labor force"
];

const fetchWithProxies = async (url, timeoutMs = 6000, disableProxy = false) => {
  const attempts = [];
  const chain = disableProxy ? [PROXY_CHAIN[0]] : PROXY_CHAIN;
  for (const proxy of chain) {
    const proxied = proxy.build(url);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort("timeout"), timeoutMs);
    try {
      const response = await fetch(proxied, { signal: controller.signal });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      clearTimeout(timeout);
      return await response.text();
    } catch (error) {
      clearTimeout(timeout);
      if (error.name === "AbortError") {
        attempts.push(`${proxy.name}: timed out`);
      } else {
        attempts.push(`${proxy.name}: ${error.message}`);
      }
    }
  }
  throw new Error(attempts.join("; "));
};

const decodeHtml = (input = "") => {
  const textarea = document.createElement("textarea");
  textarea.innerHTML = input;
  return textarea.value;
};

const cleanSummary = (text = "", maxLength = 500) => {
  const cleaned = text.replace(/\s+/g, " ").trim();
  if (cleaned.length <= maxLength) return cleaned;
  return `${cleaned.slice(0, maxLength)}…`;
};

const parseArxivFeed = (xmlText) => {
  const doc = new DOMParser().parseFromString(xmlText, "application/xml");
  const entries = Array.from(doc.querySelectorAll("entry"));
  return entries.map((entry) => {
    const title = entry.querySelector("title")?.textContent?.trim() || "Untitled";
    const summary = entry.querySelector("summary")?.textContent || "";
    const published = entry.querySelector("published")?.textContent;
    const link =
      entry.querySelector('link[rel="alternate"]')?.getAttribute("href") ||
      entry.querySelector("id")?.textContent ||
      "#";
    const authors = Array.from(entry.querySelectorAll("author > name")).map((node) =>
      node.textContent.trim()
    );
    return {
      id: entry.querySelector("id")?.textContent || link,
      title: decodeHtml(title),
      summary: cleanSummary(summary),
      published: published ? new Date(published) : new Date(),
      link,
      authors,
      source: "arxiv"
    };
  });
};

const parseRssFeed = (xmlText, sourceId) => {
  const doc = new DOMParser().parseFromString(xmlText, "application/xml");
  const items = Array.from(doc.querySelectorAll("item"));
  return items.map((item) => {
    const title = item.querySelector("title")?.textContent?.trim() || "Untitled";
    const summary =
      item.querySelector("description")?.textContent ||
      item.querySelector("content\\:encoded")?.textContent ||
      "";
    const dateText = item.querySelector("pubDate")?.textContent;
    const link = item.querySelector("link")?.textContent || "#";
    const authors = [];
    const creator = item.querySelector("dc\\:creator");
    if (creator?.textContent) {
      authors.push(creator.textContent.trim());
    }
    return {
      id: item.querySelector("guid")?.textContent || link,
      title: decodeHtml(title),
      summary: cleanSummary(decodeHtml(summary)),
      published: dateText ? new Date(dateText) : new Date(),
      link: decodeHtml(link),
      authors,
      source: sourceId
    };
  });
};

const fetchSource = async (source) => {
  const payload = await fetchWithProxies(source.url, 6000, source.disableProxy);
  switch (source.type) {
    case "arxiv":
      return parseArxivFeed(payload);
    case "rss":
    default:
      return parseRssFeed(payload, source.id);
  }
};

const formatDate = (date) =>
  new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short"
  }).format(date);

const setStatus = (message, isError = false) => {
  if (!dom.status) return;
  dom.status.textContent = message;
  dom.status.classList.toggle("error", Boolean(isError));
};

const renderSourceFilters = () => {
  if (!dom.sourceFilters) return;
  dom.sourceFilters.innerHTML = "";
  SOURCES.forEach((source) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "source-pill";
    button.dataset.source = source.id;
    button.textContent = source.label;
    if (state.activeSources.has(source.id)) {
      button.classList.add("active");
    }
    button.addEventListener("click", () => {
      if (state.activeSources.has(source.id) && state.activeSources.size === 1) {
        return;
      }
      if (state.activeSources.has(source.id)) {
        state.activeSources.delete(source.id);
      } else {
        state.activeSources.add(source.id);
      }
      button.classList.toggle("active", state.activeSources.has(source.id));
      renderPaperList();
    });
    dom.sourceFilters.appendChild(button);
  });
};

const sortedItems = () =>
  state.items
    .filter((item) => state.activeSources.has(item.source))
    .filter((item) => {
      if (state.laborOnly) {
        const haystack = `${item.title} ${item.summary}`.toLowerCase();
        const matchesLabor = LABOR_KEYWORDS.some((keyword) =>
          haystack.includes(keyword.toLowerCase())
        );
        if (!matchesLabor) {
          return false;
        }
      }
      if (!state.filter) return true;
      const haystack = `${item.title} ${item.summary} ${item.authors.join(" ")} ${item.source}`.toLowerCase();
      return haystack.includes(state.filter.toLowerCase());
    })
    .sort((a, b) => b.published.getTime() - a.published.getTime());

const renderPaperList = () => {
  if (!dom.list) return;
  const results = sortedItems();
  dom.list.innerHTML = "";
  if (!results.length) {
    dom.list.innerHTML =
      '<div class="empty-state">No papers match your filters yet. Try broadening the keywords or enabling more sources.</div>';
    return;
  }
  const fragment = document.createDocumentFragment();
  results.forEach((item, index) => {
    const card = document.createElement("article");
    card.className = "paper-card";
    card.dataset.source = item.source;
    const authors = item.authors.length ? item.authors.join(", ") : "Various authors";
    card.innerHTML = `
      <div class="paper-meta">
        <span>${formatDate(item.published)}</span>
        <span>•</span>
        <span>${authors}</span>
        <span>•</span>
        <span>${SOURCES.find((src) => src.id === item.source)?.label || item.source}</span>
      </div>
      <h3 class="paper-title">${item.title}</h3>
      <p class="paper-summary">${item.summary}</p>
      <div class="paper-actions">
        <span class="paper-rank">#${index + 1}</span>
        <a href="${item.link}" target="_blank" rel="noopener">Read paper →</a>
      </div>
    `;
    fragment.appendChild(card);
  });
  dom.list.appendChild(fragment);
  const note = `${results.length} papers from ${state.activeSources.size} sources`;
  setStatus(`Showing ${note}. ${state.errors.length ? "Some sources had issues." : ""}`, state.errors.length > 0);
};

const loadPapers = async () => {
  if (state.loading) return;
  state.loading = true;
  state.errors = [];
  setStatus("Pulling latest working papers…");
  try {
    const responses = await Promise.allSettled(SOURCES.map((source) => fetchSource(source)));
    const collected = [];
    responses.forEach((result, index) => {
      const source = SOURCES[index];
      if (result.status === "fulfilled") {
        collected.push(...result.value);
      } else {
        state.errors.push(`${source.label}: ${result.reason?.message || "Failed to fetch"}`);
      }
    });
    state.items = collected;
    if (!collected.length) {
      setStatus("No papers could be fetched. Try refreshing.", true);
    } else {
      renderPaperList();
    }
  } catch (error) {
    setStatus(`Unable to load research: ${error.message}`, true);
  } finally {
    state.loading = false;
  }
  if (state.errors.length) {
    setStatus(
      `Loaded ${state.items.length} papers. Issues: ${state.errors.join(" • ")}`,
      true
    );
  }
};

if (dom.filter) {
  dom.filter.addEventListener("input", (event) => {
    state.filter = event.target.value.trim();
    renderPaperList();
  });
}

if (dom.refresh) {
  dom.refresh.addEventListener("click", loadPapers);
}

if (dom.laborToggle) {
  dom.laborToggle.addEventListener("click", () => {
    state.laborOnly = !state.laborOnly;
    dom.laborToggle.classList.toggle("active", state.laborOnly);
    dom.laborToggle.textContent = state.laborOnly ? "Labor econ only" : "Show all topics";
    renderPaperList();
  });
  dom.laborToggle.textContent = state.laborOnly ? "Labor econ only" : "Show all topics";
}

renderSourceFilters();
loadPapers();
