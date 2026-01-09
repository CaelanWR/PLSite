export const byId = (id) => document.getElementById(id);

export const toNumber = (value) => {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" && value.trim() === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

export const fetchJSONLocal = async (path) => {
  const response = await fetch(path, { cache: "no-cache" });
  if (!response.ok) {
    throw new Error(`${path}: HTTP ${response.status}`);
  }
  return await response.json();
};

export const initInfoPopover = ({ wrap, btn }) => {
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
