const LINKS_KEY = "market_pulse_links_v1";
const SAVED_ARTICLES_KEY = "market_pulse_saved_articles_v1";
const LEGACY_LINKS_KEY = "labor_links_custom";

const byId = (id) => document.getElementById(id);

const elements = {
  filter: byId("linksFilter"),
  savedArticles: byId("savedArticles"),
  linksContainer: byId("customLinks"),

  openModalButton: byId("toggleLinkForm"),
  linkModal: byId("linkModal"),
  closeModalButton: byId("closeLinkForm"),
  form: byId("linkForm"),
  formStatus: byId("linkFormStatus"),
  title: byId("linkTitle"),
  url: byId("linkUrl"),
  tags: byId("linkTags"),
  description: byId("linkDescription")
};

const loadJSON = (key, fallback) => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    return parsed ?? fallback;
  } catch {
    return fallback;
  }
};

const saveJSON = (key, value) => {
  localStorage.setItem(key, JSON.stringify(value));
};

const normalizeLinkKey = (link = "", fallback = "") => {
  if (link) {
    try {
      const url = new URL(link);
      url.hash = "";
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

const splitTags = (value) =>
  String(value || "")
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);

const openModal = () => {
  if (!elements.linkModal) return;
  elements.linkModal.classList.remove("hidden");
  if (elements.formStatus) elements.formStatus.textContent = "";
  setTimeout(() => elements.title?.focus(), 0);
};

const closeModal = () => {
  elements.linkModal?.classList.add("hidden");
  elements.form?.reset();
  if (elements.formStatus) elements.formStatus.textContent = "";
};

const loadLinks = () => {
  const parsed = loadJSON(LINKS_KEY, []);
  return Array.isArray(parsed) ? parsed : [];
};

const saveLinks = (links) => saveJSON(LINKS_KEY, links);

const loadSavedArticles = () => {
  const parsed = loadJSON(SAVED_ARTICLES_KEY, []);
  return Array.isArray(parsed) ? parsed : [];
};

const saveSavedArticles = (articles) => saveJSON(SAVED_ARTICLES_KEY, articles);

const applyFilter = (items, filterValue, toSearchText) => {
  const q = String(filterValue || "").trim().toLowerCase();
  if (!q) return items;
  return items.filter((item) => toSearchText(item).includes(q));
};

const renderTags = (tags = []) => {
  const safeTags = Array.isArray(tags) ? tags : [];
  if (!safeTags.length) return "";
  return `<div class="topic-pills">${safeTags
    .map((tag) => `<span class="topic-pill" data-tag="${tag.replace(/\"/g, "&quot;")}">${tag}</span>`)
    .join("")}</div>`;
};

const renderSavedArticles = () => {
  if (!elements.savedArticles) return;
  const filterValue = elements.filter?.value || "";

  const saved = loadSavedArticles()
    .slice()
    .sort((a, b) => (new Date(b.savedAt).getTime() || 0) - (new Date(a.savedAt).getTime() || 0));

  const filtered = applyFilter(saved, filterValue, (item) => {
    return [
      item.title,
      item.source,
      item.summary,
      item.url,
      Array.isArray(item.tags) ? item.tags.join(" ") : ""
    ]
      .join(" ")
      .toLowerCase();
  });

  if (!filtered.length) {
    elements.savedArticles.innerHTML =
      '<p class="empty-state">No saved articles yet. Save one from the News Feed to build your library.</p>';
    return;
  }

  const fragment = document.createDocumentFragment();
  filtered.forEach((article) => {
    const card = document.createElement("article");
    card.className = "custom-card";
    const dateLabel = article.pubDate ? new Date(article.pubDate).toLocaleDateString() : "—";
    card.innerHTML = `
      <header>
        <h3>${article.title || "Untitled"}</h3>
        <button class="remove-link" data-type="article" data-id="${article.id}" type="button">Remove</button>
      </header>
      ${renderTags(article.tags)}
      <p class="detail-label">${article.source || "Unknown source"} • ${dateLabel}</p>
      <p>${article.summary || "No summary saved."}</p>
      <a href="${article.url}" target="_blank" rel="noopener">Open article →</a>
    `;
    fragment.appendChild(card);
  });

  elements.savedArticles.innerHTML = "";
  elements.savedArticles.appendChild(fragment);
};

const renderLinks = () => {
  if (!elements.linksContainer) return;
  const filterValue = elements.filter?.value || "";

  const links = loadLinks().slice().sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
  const filtered = applyFilter(links, filterValue, (link) => {
    return [link.title, link.description, link.url, Array.isArray(link.tags) ? link.tags.join(" ") : ""]
      .join(" ")
      .toLowerCase();
  });

  if (!filtered.length) {
    elements.linksContainer.innerHTML =
      '<p class="empty-state">No links yet. Click “Add link” to create one.</p>';
    return;
  }

  const fragment = document.createDocumentFragment();
  filtered.forEach((link) => {
    const card = document.createElement("article");
    card.className = "custom-card";
    card.innerHTML = `
      <header>
        <h3>${link.title || "Untitled"}</h3>
        <button class="remove-link" data-type="link" data-id="${link.id}" type="button">Remove</button>
      </header>
      ${renderTags(link.tags)}
      <p>${link.description || ""}</p>
      <a href="${link.url}" target="_blank" rel="noopener">Visit site →</a>
    `;
    fragment.appendChild(card);
  });

  elements.linksContainer.innerHTML = "";
  elements.linksContainer.appendChild(fragment);
};

const renderAll = () => {
  renderSavedArticles();
  renderLinks();
};

const migrateLegacyLinks = () => {
  const already = localStorage.getItem(LINKS_KEY);
  const legacyRaw = localStorage.getItem(LEGACY_LINKS_KEY);
  if (already || !legacyRaw) return;
  try {
    const legacy = JSON.parse(legacyRaw);
    if (!Array.isArray(legacy) || !legacy.length) return;
    const migrated = legacy
      .filter((l) => l && (l.title || l.url))
      .map((l) => ({
        id: l.id || (crypto.randomUUID ? crypto.randomUUID() : Date.now().toString()),
        title: String(l.title || "Untitled"),
        url: String(l.url || ""),
        description: String(l.description || ""),
        tags: [],
        createdAt: Date.now()
      }))
      .filter((l) => l.url);
    if (migrated.length) {
      saveLinks(migrated);
    }
  } catch {
    // ignore
  }
};

const handleAddLink = (event) => {
  event.preventDefault();

  const title = elements.title?.value?.trim() || "";
  const url = elements.url?.value?.trim() || "";
  const description = elements.description?.value?.trim() || "";
  const tags = splitTags(elements.tags?.value || "");

  if (!title || !url) {
    if (elements.formStatus) elements.formStatus.textContent = "Title and URL are required.";
    return;
  }

  let normalizedUrl = url;
  try {
    normalizedUrl = new URL(url).toString();
  } catch {
    if (elements.formStatus) elements.formStatus.textContent = "Please enter a valid URL.";
    return;
  }

  const links = loadLinks();
  links.push({
    id: crypto.randomUUID ? crypto.randomUUID() : Date.now().toString(),
    title,
    url: normalizedUrl,
    description,
    tags,
    createdAt: Date.now()
  });
  saveLinks(links);
  closeModal();
  renderLinks();
};

const handleRemove = (event) => {
  const button = event.target.closest("button.remove-link");
  if (!button) return;
  const id = button.dataset.id;
  const type = button.dataset.type;
  if (!id || !type) return;

  const confirmed = window.confirm("Remove this item?");
  if (!confirmed) return;

  if (type === "link") {
    const next = loadLinks().filter((l) => l.id !== id);
    saveLinks(next);
    renderLinks();
    return;
  }
  if (type === "article") {
    const next = loadSavedArticles().filter((a) => a.id !== id);
    saveSavedArticles(next);
    renderSavedArticles();
  }
};

const handleTagClick = (event) => {
  const pill = event.target.closest(".topic-pill");
  if (!pill || !elements.filter) return;
  const tag = pill.dataset.tag;
  if (!tag) return;
  elements.filter.value = tag;
  renderAll();
};

if (elements.openModalButton) elements.openModalButton.addEventListener("click", openModal);
if (elements.closeModalButton) elements.closeModalButton.addEventListener("click", closeModal);
if (elements.linkModal) {
  elements.linkModal.addEventListener("click", (event) => {
    if (event.target === elements.linkModal) closeModal();
  });
}
if (elements.form) elements.form.addEventListener("submit", handleAddLink);
if (elements.filter) elements.filter.addEventListener("input", renderAll);

document.addEventListener("click", (event) => {
  if (event.target.closest(".topic-pill")) handleTagClick(event);
});
if (elements.linksContainer) elements.linksContainer.addEventListener("click", handleRemove);
if (elements.savedArticles) elements.savedArticles.addEventListener("click", handleRemove);

// Ensure correct block is shown on load
migrateLegacyLinks();
renderAll();
