const PROXY_CHAIN = [
  { name: "Direct", build: (url) => url },
  {
    name: "IsomorphicGit",
    build: (url) => `https://cors.isomorphic-git.org/${url}`
  },
  {
    name: "CorsProxyIO",
    build: (url) => `https://corsproxy.io/?${url}`
  },
  {
    name: "ThingProxy",
    build: (url) => `https://thingproxy.freeboard.io/fetch/${url}`
  }
];

const FALLBACK_EVENTS = [
  {
    title: "Employment Situation (Jobs Report)",
    source: "Bureau of Labor Statistics",
    date: "2024-08-02T12:30:00Z",
    link: "https://www.bls.gov/bls/news-release/empsit.htm",
    description:
      "Headline unemployment rate, payrolls, and participation data.",
    location: "Washington, DC",
    category: "jobs"
  },
  {
    title: "ADP National Employment Report",
    source: "ADP Research Institute",
    date: "2024-07-31T12:15:00Z",
    link: "https://adpemploymentreport.com/",
    description:
      "Private payroll estimates and pay insights ahead of the BLS release.",
    location: "Online release",
    category: "jobs"
  },
  {
    title: "JOLTS Job Openings",
    source: "Bureau of Labor Statistics",
    date: "2024-08-06T14:00:00Z",
    link: "https://www.bls.gov/jlt/",
    description: "Vacancies, hires, and quits across industries.",
    location: "Washington, DC",
    category: "jobs"
  }
];

const STORAGE_KEY = "labor_calendar_custom_events";
const ADMIN_PASS_KEY = "labor_calendar_admin_passphrase";
const DEFAULT_TIME_ZONE = "America/New_York";
const FRED_RELEASE_SEARCH_ENDPOINT =
  "https://api.stlouisfed.org/fred/releases/search";
const FRED_RELEASE_DATES_ENDPOINT =
  "https://api.stlouisfed.org/fred/release/dates";
const FED_CALENDAR_URL =
  "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm";

const autoSources = [
  {
    id: "bls-emp",
    label: "Employment Situation",
    type: "fredRelease",
    searchText: "Employment Situation",
    title: "Employment Situation (Jobs Report)",
    source: "Bureau of Labor Statistics",
    link: "https://www.bls.gov/bls/news-release/empsit.htm",
    description:
      "Nonfarm payrolls, unemployment rate, participation, and wages.",
    time: "08:30",
    tzOffset: "-05:00",
    location: "Washington, DC",
    limit: 24,
    category: "jobs",
    fallback: {
      type: "monthlyWeekday",
      weekday: 5,
      startDay: 1,
      monthsAhead: 24,
      note: "Used first-Friday rule while BLS feed is offline.",
      attribution: "Estimated: first Friday of each month at 8:30 ET."
    }
  },
  {
    id: "adp",
    label: "ADP National Employment Report",
    type: "adp",
    title: "ADP National Employment Report",
    source: "ADP Research Institute",
    link: "https://adpemploymentreport.com/",
    description:
      "Private payroll gauge released ahead of the Employment Situation.",
    time: "08:15",
    tzOffset: "-05:00",
    horizonMonths: 24,
    category: "jobs"
  },
  {
    id: "revelio",
    label: "Revelio Public Labor Statistics (RPLS)",
    type: "revelio",
    searchText: "Employment Situation",
    title: "Revelio Public Labor Statistics (RPLS)",
    source: "Revelio Labs",
    link: "https://www.reveliolabs.com/public-labor-statistics/",
    description:
      "Aggregated workforce flows, openings, and attrition signals published ahead of Jobs Friday.",
    time: "09:00",
    tzOffset: "-05:00",
    location: "Online release",
    limit: 24,
    category: "jobs",
    fallback: {
      type: "monthlyWeekday",
      weekday: 5,
      startDay: 1,
      monthsAhead: 24,
      shiftDays: -1,
      note: "Assuming RPLS lands the day before Employment Situation.",
      attribution: "Estimated: Thursday before Jobs Friday (9:00 ET)."
    }
  },
  {
    id: "jolts",
    label: "JOLTS",
    type: "fredRelease",
    searchText: "Job Openings and Labor Turnover Survey",
    title: "JOLTS Job Openings",
    source: "Bureau of Labor Statistics",
    link: "https://www.bls.gov/jlt/",
    description:
      "Job openings, hires, quits, and layoffs across industries.",
    time: "10:00",
    tzOffset: "-05:00",
    location: "Washington, DC",
    limit: 24,
    category: "jobs",
    fallback: {
      type: "monthlyWeekday",
      weekday: 2,
      startDay: 1,
      monthsAhead: 24,
      note: "Used first-Tuesday cadence for JOLTS.",
      attribution: "Estimated: first Tuesday of each month at 10:00 ET."
    }
  },
  {
    id: "cpi",
    label: "Consumer Price Index",
    type: "fredRelease",
    searchText: "Consumer Price Index",
    title: "Consumer Price Index (CPI)",
    source: "Bureau of Labor Statistics",
    link: "https://www.bls.gov/cpi/",
    description:
      "Monthly CPI release covering inflation for urban consumers.",
    location: "Washington, DC",
    time: "08:30",
    tzOffset: "-05:00",
    location: "Washington, DC",
    limit: 24,
    category: "inflation",
    fallback: {
      type: "monthlyWeekday",
      weekday: 3,
      startDay: 10,
      monthsAhead: 24,
      note: "Used second-week Wednesday cadence for CPI.",
      attribution: "Estimated: first Wednesday on or after the 10th at 8:30 ET."
    }
  },
  {
    id: "claims",
    label: "Initial Jobless Claims",
    type: "weeklyClaims",
    title: "Initial Jobless Claims",
    source: "Employment and Training Administration (DOL)",
    link: "https://oui.doleta.gov/unemploy/claims.asp",
    description:
      "Weekly initial unemployment insurance claims (seasonally adjusted).",
    time: "08:30",
    tzOffset: "-05:00",
    weeks: 104,
    category: "claims"
  },
  {
    id: "fed",
    label: "FOMC decisions",
    type: "manualList",
    title: "FOMC Interest Rate Decision",
    source: "Federal Reserve Board",
    link: "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
    description:
      "Scheduled Federal Open Market Committee policy announcement (meetings marked * include a Summary of Economic Projections).",
    time: "14:00",
    tzOffset: "-05:00",
    category: "fed",
    dates: [
      "2024-09-18",
      "2024-11-07",
      "2024-12-18",
      "2025-01-29",
      "2025-03-19",
      "2025-04-30",
      "2025-06-18",
      "2025-07-30",
      "2025-09-17",
      "2025-11-06",
      "2025-12-09",
      "2026-01-28",
      "2026-03-18",
      "2026-04-29",
      "2026-06-17",
      "2026-07-29",
      "2026-09-16",
      "2026-10-28",
      "2026-12-09",
      "2027-01-27",
      "2027-03-17",
      "2027-04-28",
      "2027-06-09",
      "2027-07-28",
      "2027-09-15",
      "2027-10-27",
      "2027-12-08"
    ],
    note: "Using published 2024-2027 FOMC meeting schedule.",
    attribution: "FOMC calendar (manual upload)"
  }
];

const calendarList = document.getElementById("calendarList");
const calendarGrid = document.getElementById("calendarGrid");
const addForm = document.getElementById("addReleaseForm");
const toggleAddFormButton = document.getElementById("toggleAddForm");
const formStatus = document.getElementById("formStatus");
const autoRefreshButton = document.getElementById("autoRefresh");
const autoStatusEl = document.getElementById("autoStatus");
const calendarFredKeyInput = document.getElementById("calendarFredKey");
const calendarFredApplyButton = document.getElementById("calendarFredApply");
const viewToggleButtons = document.querySelectorAll(".view-switch .toggle-btn");
const calendarMonthLabel = document.getElementById("calendarMonthLabel");
const calendarMonthNav = document.getElementById("calendarMonthNav");
const calendarPrevMonth = document.getElementById("calendarPrevMonth");
const calendarNextMonth = document.getElementById("calendarNextMonth");

const initialGridMonth = new Date();
initialGridMonth.setDate(1);

const state = {
  autoEvents: [],
  autoLoading: false,
  autoNotes: [],
  viewMode: "grid",
  gridMonth: initialGridMonth
};

const CATEGORY_LABELS = {
  jobs: "Labor",
  inflation: "Inflation",
  wages: "Wages",
  claims: "Claims",
  fed: "Fed"
};

const WEEKDAY_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MAX_EVENTS_PER_DAY = 3;
const releaseIdCache = new Map();

const getStoredFredKey = () => localStorage.getItem("fredApiKey") ?? "";

const setStoredFredKey = (value) => {
  localStorage.setItem("fredApiKey", value);
  if (calendarFredKeyInput) {
    calendarFredKeyInput.value = value;
  }
};

if (calendarFredKeyInput) {
  calendarFredKeyInput.value = getStoredFredKey();
}

const setAutoStatus = (message, isError = false, notes = []) => {
  if (!autoStatusEl) return;
  autoStatusEl.innerHTML = "";
  autoStatusEl.classList.toggle("error", Boolean(isError));
  const textSpan = document.createElement("span");
  textSpan.textContent = message;
  autoStatusEl.appendChild(textSpan);
  if (notes.length) {
    const infoWrapper = document.createElement("span");
    infoWrapper.className = "auto-info";
    const button = document.createElement("button");
    button.type = "button";
    button.className = "info-pill";
    button.setAttribute("aria-label", "Show cadence notes");
    button.textContent = "i";
    const detail = document.createElement("span");
    detail.className = "auto-info-details";
    detail.textContent = notes.join(" • ");
    button.addEventListener("click", () => {
      infoWrapper.classList.toggle("show");
    });
    infoWrapper.appendChild(button);
    infoWrapper.appendChild(detail);
    autoStatusEl.appendChild(infoWrapper);
  }
};

const fetchWithProxies = async (url, asJson = false) => {
  const attempts = [];
  for (const proxy of PROXY_CHAIN) {
    const proxiedUrl = proxy.build(url);
    try {
      const response = await fetch(proxiedUrl);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      return asJson ? await response.json() : await response.text();
    } catch (error) {
      attempts.push(`${proxy.name}: ${error.message}`);
    }
  }
  throw new Error(attempts.join("; "));
};

const fetchJsonWithProxies = (url) => fetchWithProxies(url, true);

const formatDateTimeLong = (isoString) => {
  const date = isoString ? new Date(isoString) : null;
  if (!date || Number.isNaN(date)) {
    return "Date TBD";
  }
  return new Intl.DateTimeFormat("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
      hour: "numeric",
      minute: "2-digit",
      timeZoneName: "short"
    }).format(date);
};

const formatTimeShort = (isoString) => {
  const date = isoString ? new Date(isoString) : null;
  if (!date || Number.isNaN(date)) {
    return "Time TBD";
  }
  return new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short"
  }).format(date);
};

const loadCustomEvents = () => {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed;
  } catch {
    return [];
  }
};

const getAdminPassphrase = () => localStorage.getItem(ADMIN_PASS_KEY) || "";

const setAdminPassphrase = (value) => {
  localStorage.setItem(ADMIN_PASS_KEY, value);
};

const saveCustomEvents = (events) => {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(events));
};

const dedupeEvents = (events) => {
  const map = new Map();
  events.forEach((event) => {
    const key = `${event.title}-${event.date}`;
    if (!map.has(key)) {
      map.set(key, event);
    }
  });
  return Array.from(map.values());
};

const _parseIsoDateParts = (dateStr) => {
  if (!dateStr) return null;
  const parsed = new Date(dateStr);
  const isoDate = Number.isNaN(parsed.getTime())
    ? String(dateStr)
    : parsed.toISOString().split("T")[0];
  const match = isoDate.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  return { isoDate, year: Number(match[1]), month: Number(match[2]), day: Number(match[3]) };
};

const _parseTimeParts = (timeStr) => {
  const match = String(timeStr || "").match(/^(\d{2}):(\d{2})$/);
  if (!match) return null;
  return { hour: Number(match[1]), minute: Number(match[2]) };
};

const _tzOffsetMinutes = (date, timeZone) => {
  try {
    const dtf = new Intl.DateTimeFormat("en-US", {
      timeZone,
      hour12: false,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit"
    });
    const parts = dtf.formatToParts(date);
    const values = {};
    parts.forEach((part) => {
      if (part.type !== "literal") values[part.type] = part.value;
    });
    const utc = Date.UTC(
      Number(values.year),
      Number(values.month) - 1,
      Number(values.day),
      Number(values.hour),
      Number(values.minute),
      Number(values.second)
    );
    return (utc - date.getTime()) / 60000;
  } catch {
    return null;
  }
};

const _isoFromTimeZone = (isoDate, timeStr, timeZone) => {
  const dateParts = _parseIsoDateParts(isoDate);
  const timeParts = _parseTimeParts(timeStr);
  if (!dateParts || !timeParts) return null;
  const utcGuess = new Date(Date.UTC(
    dateParts.year,
    dateParts.month - 1,
    dateParts.day,
    timeParts.hour,
    timeParts.minute,
    0
  ));
  const offsetMinutes = _tzOffsetMinutes(utcGuess, timeZone);
  if (offsetMinutes === null) return null;
  const ts = Date.UTC(
    dateParts.year,
    dateParts.month - 1,
    dateParts.day,
    timeParts.hour,
    timeParts.minute,
    0
  ) - offsetMinutes * 60 * 1000;
  return new Date(ts).toISOString();
};

const combineDateAndTime = (
  dateStr,
  time = "08:30",
  tzOffset = "-05:00",
  timeZone = DEFAULT_TIME_ZONE
) => {
  if (!dateStr) return null;
  const parsed = new Date(dateStr);
  const isoDate = Number.isNaN(parsed.getTime())
    ? String(dateStr)
    : parsed.toISOString().split("T")[0];
  const zonedIso = timeZone ? _isoFromTimeZone(isoDate, time, timeZone) : null;
  if (zonedIso) return zonedIso;
  return `${isoDate}T${time}:00${tzOffset}`;
};

const getWeekdayOnOrAfter = (year, month, startDay, weekday) => {
  const date = new Date(Date.UTC(year, month, startDay));
  while (date.getUTCDay() !== weekday) {
    date.setUTCDate(date.getUTCDate() + 1);
    if (date.getUTCMonth() !== month) return null;
  }
  return date;
};

const generateMonthlyWeekdaySchedule = (source, fallback) => {
  const items = [];
  const monthsAhead = fallback.monthsAhead ?? 6;
  const now = new Date();

  for (let i = 0; i < monthsAhead + 2; i += 1) {
    const base = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + i, 1));
    const target = getWeekdayOnOrAfter(
      base.getUTCFullYear(),
      base.getUTCMonth(),
      fallback.startDay ?? 1,
      fallback.weekday ?? 5
    );
    if (!target) continue;
    if (fallback.addWeeks) {
      target.setUTCDate(target.getUTCDate() + fallback.addWeeks * 7);
    }
    if (fallback.shiftDays) {
      target.setUTCDate(target.getUTCDate() + fallback.shiftDays);
    }
    const iso = combineDateAndTime(
      target.toISOString().split("T")[0],
      source.time,
      source.tzOffset
    );
    if (new Date(iso) < now) continue;
    items.push({
      title: source.title,
      source: source.source,
      link: source.link,
      description: source.description,
      location: source.location,
      date: iso,
      category: source.category,
      attribution:
        fallback.attribution ||
        "Estimated cadence derived from historical schedule."
    });
    if (items.length >= monthsAhead) break;
  }

  if (items.length) {
    state.autoNotes.push(
      `${source.label}: ${fallback.note || "Dates estimated via cadence rule."}`
    );
  }

  return items;
};

const generateManualListSchedule = (source, fallbackConfig) => {
  const fallback = fallbackConfig || source;
  const now = new Date();
  const items = (fallback?.dates || [])
    .map((dateStr) => {
      const iso = combineDateAndTime(dateStr, source.time, source.tzOffset);
      return {
        title: source.title,
        source: source.source,
        link: source.link,
        description: source.description,
        location: source.location,
        date: iso,
        category: source.category,
        attribution:
          fallback?.attribution ||
          "Manual schedule provided for upcoming FOMC meetings."
      };
    })
    .filter((event) => event.date && new Date(event.date) >= now);

  if (items.length) {
    state.autoNotes.push(
      `${source.label}: ${fallback?.note || "Using manual list of meeting dates."}`
    );
  }
  return items;
};

const fallbackGenerators = {
  monthlyWeekday: generateMonthlyWeekdaySchedule,
  manualList: generateManualListSchedule
};

const generateFallbackEvents = (source, message) => {
  if (!source.fallback) {
    throw new Error(message);
  }
  const generator = fallbackGenerators[source.fallback.type];
  if (!generator) {
    throw new Error(message);
  }
  const events = generator(source, source.fallback);
  if (!events.length) {
    throw new Error(message);
  }
  return events;
};

const getBaseEvents = () =>
  state.autoEvents.length ? state.autoEvents : FALLBACK_EVENTS;

const getAllEvents = () => dedupeEvents([...getBaseEvents(), ...loadCustomEvents()]);

const expandRecurringEvents = (events) => {
  const expanded = [];
  const now = new Date();
  const horizon = new Date();
  horizon.setFullYear(horizon.getFullYear() + 1);

  events.forEach((event) => {
    if (!event.recurring) {
      expanded.push(event);
      return;
    }

    const baseDate = new Date(event.date);
    if (Number.isNaN(baseDate)) {
      expanded.push(event);
      return;
    }

    let nextDate = new Date(baseDate);
    while (nextDate < now) {
      nextDate.setMonth(nextDate.getMonth() + 1);
    }

    while (nextDate <= horizon) {
      expanded.push({
        ...event,
        date: nextDate.toISOString()
      });
      nextDate = new Date(nextDate);
      nextDate.setMonth(nextDate.getMonth() + 1);
    }
  });

  return expanded;
};

const buildUpcomingEvents = () =>
  expandRecurringEvents(getAllEvents()).sort(
    (a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()
  );

const renderCalendarList = (upcoming) => {
  if (!calendarList) return;
  if (!upcoming.length) {
    calendarList.innerHTML =
      '<li class="calendar-item">No releases on the schedule yet. Check back soon.</li>';
    return;
  }

  const fragment = document.createDocumentFragment();
  upcoming.forEach((event) => {
    const li = document.createElement("li");
    li.className = "calendar-item";
    if (event.category) {
      li.dataset.category = event.category;
    }
    const formatted = formatDateTimeLong(event.date);
    const dateObj = new Date(event.date);
    const day = Number.isNaN(dateObj) ? "" : dateObj.getDate();
    const month = Number.isNaN(dateObj)
      ? ""
      : dateObj.toLocaleString("en-US", { month: "short" }).toUpperCase();
    li.innerHTML = `
      <header>
        <div class="calendar-when">
          <span class="calendar-month">${month}</span>
          <span class="calendar-day">${day}</span>
        </div>
        <div class="calendar-title">
          <h3>${event.title}</h3>
          <div class="calendar-source">${event.source}${
            event.location ? ` • ${event.location}` : ""
          }</div>
          <time datetime="${event.date}" class="calendar-time">${formatted}</time>
        </div>
      </header>
      <p>${event.description}</p>
      <a href="${event.link}" target="_blank" rel="noopener">View details →</a>
      ${event.attribution ? `<p class="calendar-note">${event.attribution}</p>` : ""}
    `;
    fragment.appendChild(li);
  });

  calendarList.innerHTML = "";
  calendarList.appendChild(fragment);
};

const renderCalendarGrid = (upcoming) => {
  if (!calendarGrid) return;
  calendarGrid.innerHTML = "";
  const month = new Date(state.gridMonth);
  if (Number.isNaN(month)) {
    month.setDate(1);
  }
  const monthStart = new Date(month.getFullYear(), month.getMonth(), 1);
  const monthEnd = new Date(month.getFullYear(), month.getMonth() + 1, 0);
  const startOffset = monthStart.getDay();
  const totalDays = monthEnd.getDate();
  const labelFormatter = new Intl.DateTimeFormat("en-US", {
    month: "long",
    year: "numeric"
  });
  if (calendarMonthLabel) {
    calendarMonthLabel.textContent = labelFormatter.format(monthStart);
  }

  const eventsByDay = upcoming.reduce((acc, event) => {
    const dateObj = new Date(event.date);
    if (
      Number.isNaN(dateObj) ||
      dateObj.getFullYear() !== monthStart.getFullYear() ||
      dateObj.getMonth() !== monthStart.getMonth()
    ) {
      return acc;
    }
    const day = dateObj.getDate();
    if (!acc[day]) {
      acc[day] = [];
    }
    acc[day].push(event);
    acc[day].sort(
      (a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()
    );
    return acc;
  }, {});

  const header = document.createElement("div");
  header.className = "calendar-grid-header";
  WEEKDAY_LABELS.forEach((label) => {
    const span = document.createElement("span");
    span.textContent = label;
    header.appendChild(span);
  });

  const body = document.createElement("div");
  body.className = "calendar-grid-body";

  const totalCells = Math.ceil((startOffset + totalDays) / 7) * 7;
  for (let cellIndex = 0; cellIndex < totalCells; cellIndex += 1) {
    const dayNumber = cellIndex - startOffset + 1;
    if (dayNumber < 1 || dayNumber > totalDays) {
      const emptyCell = document.createElement("div");
      emptyCell.className = "day-cell empty";
      body.appendChild(emptyCell);
      continue;
    }
    const dayCell = document.createElement("div");
    dayCell.className = "day-cell";

    const dayHeader = document.createElement("div");
    dayHeader.className = "day-number";
    dayHeader.textContent = dayNumber;
    dayCell.appendChild(dayHeader);

    const dayEventsWrap = document.createElement("div");
    dayEventsWrap.className = "day-events";
    const dayEvents = eventsByDay[dayNumber] || [];

    dayEvents.slice(0, MAX_EVENTS_PER_DAY).forEach((event) => {
      const eventDiv = document.createElement("div");
      eventDiv.className = "grid-event";
      if (event.category) {
        eventDiv.dataset.category = event.category;
      }
      const title = document.createElement("p");
      title.className = "grid-event-title";
      title.textContent = event.title;
      eventDiv.appendChild(title);
      const time = document.createElement("span");
      time.className = "grid-event-time";
      time.textContent = formatTimeShort(event.date);
      eventDiv.appendChild(time);
      dayEventsWrap.appendChild(eventDiv);
    });

    if (dayEvents.length > MAX_EVENTS_PER_DAY) {
      const more = document.createElement("div");
      more.className = "more-indicator";
      more.textContent = `+${dayEvents.length - MAX_EVENTS_PER_DAY} more`;
      dayEventsWrap.appendChild(more);
    }

    dayCell.appendChild(dayEventsWrap);
    body.appendChild(dayCell);
  }

  calendarGrid.appendChild(header);
  calendarGrid.appendChild(body);
};

const updateViewVisibility = () => {
  const isGrid = state.viewMode === "grid";
  if (calendarList) {
    calendarList.classList.toggle("view-hidden", isGrid);
  }
  if (calendarGrid) {
    calendarGrid.classList.toggle("view-hidden", !isGrid);
  }
  if (calendarMonthNav) {
    calendarMonthNav.classList.toggle("view-hidden", !isGrid);
  }
  viewToggleButtons.forEach((button) => {
    const isActive = button.dataset.view === state.viewMode;
    button.classList.toggle("active", isActive);
  });
};

const renderCalendar = () => {
  const upcoming = buildUpcomingEvents();
  renderCalendarList(upcoming);
  renderCalendarGrid(upcoming);
  updateViewVisibility();
};

const resetForm = () => {
  if (!addForm) return;
  addForm.reset();
};

const toggleFormVisibility = () => {
  if (!addForm) return;
  addForm.classList.toggle("hidden");
  if (formStatus) {
    formStatus.textContent = "";
  }
};

const handleFormSubmit = (event) => {
  event.preventDefault();
  if (!addForm) return;
  const pass = document.getElementById("adminPass").value.trim();
  const storedPass = getAdminPassphrase();
  if (!storedPass) {
    if (!pass) {
      formStatus.textContent = "Set a passphrase to enable admin edits.";
      return;
    }
    setAdminPassphrase(pass);
  } else if (pass !== storedPass) {
    formStatus.textContent = "Incorrect passphrase.";
    return;
  }

  const newEvent = {
    title: document.getElementById("releaseTitle").value.trim(),
    source: document.getElementById("releaseSource").value.trim(),
    date: document.getElementById("releaseDate").value,
    link: document.getElementById("releaseLink").value.trim(),
    location: document.getElementById("releaseLocation").value.trim(),
    description: document
      .getElementById("releaseDescription")
      .value.trim(),
    recurring: document.getElementById("releaseRecurring").checked
  };

  if (
    !newEvent.title ||
    !newEvent.source ||
    !newEvent.date ||
    !newEvent.link ||
    !newEvent.description
  ) {
    formStatus.textContent = "Please fill in all required fields.";
    return;
  }

  const customEvents = loadCustomEvents();
  customEvents.push(newEvent);
  saveCustomEvents(customEvents);
  formStatus.textContent = "Release added.";
  addForm.classList.add("hidden");
  resetForm();
  renderCalendar();
};

const ensureFredKey = () => {
  const key = getStoredFredKey();
  if (!key) {
    throw new Error("FRED API key required to fetch BLS releases.");
  }
  return key;
};

const fetchFredReleaseId = async (searchText, apiKey) => {
  if (releaseIdCache.has(searchText)) {
    return releaseIdCache.get(searchText);
  }
  const params = new URLSearchParams({
    api_key: apiKey,
    file_type: "json",
    search_text: searchText,
    limit: "20"
  });
  const url = `${FRED_RELEASE_SEARCH_ENDPOINT}?${params.toString()}`;
  const data = await fetchJsonWithProxies(url);
  const releases = data.releases ?? [];
  const match =
    releases.find((release) =>
      release.name.toLowerCase().includes(searchText.toLowerCase())
    ) || releases[0];
  if (!match) {
    throw new Error(`No FRED release found for “${searchText}”.`);
  }
  releaseIdCache.set(searchText, match.id || match.release_id);
  return match.id || match.release_id;
};

const fetchFredReleaseDates = async (releaseId, apiKey, limit = 6) => {
  const params = new URLSearchParams({
    api_key: apiKey,
    file_type: "json",
    release_id: releaseId,
    sort_order: "asc",
    include_release_dates_with_no_data: "true",
    limit: String(limit * 3)
  });
  const url = `${FRED_RELEASE_DATES_ENDPOINT}?${params.toString()}`;
  const data = await fetchJsonWithProxies(url);
  return data.release_dates ?? [];
};

const fetchFredReleaseEvents = async (source) => {
  try {
    const apiKey = ensureFredKey();
    const releaseId = await fetchFredReleaseId(source.searchText, apiKey);
    const dates = await fetchFredReleaseDates(
      releaseId,
      apiKey,
      source.limit ?? 6
    );
    const now = new Date();
    const future = dates
      .map((entry) => entry.date || entry.release_date)
      .filter(Boolean)
      .map((dateStr) => new Date(`${dateStr}T00:00:00Z`))
      .filter((dateObj) => dateObj >= now)
      .slice(0, source.limit ?? 6);

    if (!future.length) {
      throw new Error("No upcoming release dates returned.");
    }

    return future.map((dateObj) => {
      const iso = combineDateAndTime(
        dateObj.toISOString().split("T")[0],
        source.time,
        source.tzOffset
      );
      return {
        title: source.title,
        source: source.source,
        link: source.link,
        description: source.description,
        location: source.location,
        date: iso,
        category: source.category,
        attribution: "Source: FRED release calendar."
      };
    });
  } catch (error) {
    if (source.fallback) {
      return generateFallbackEvents(source, error.message);
    }
    throw error;
  }
};

const fetchRevelioEvents = async (source) => {
  try {
    const apiKey = ensureFredKey();
    const releaseId = await fetchFredReleaseId(source.searchText, apiKey);
    const dates = await fetchFredReleaseDates(
      releaseId,
      apiKey,
      source.limit ?? 6
    );
    const now = new Date();
    const future = dates
      .map((entry) => entry.date || entry.release_date)
      .filter(Boolean)
      .map((dateStr) => new Date(`${dateStr}T00:00:00Z`))
      .filter((dateObj) => dateObj >= now)
      .slice(0, source.limit ?? 6);

    if (!future.length) {
      throw new Error("No upcoming release dates returned.");
    }

    return future.map((dateObj) => {
      const adjusted = new Date(dateObj);
      adjusted.setUTCDate(adjusted.getUTCDate() - 1);
      const iso = combineDateAndTime(
        adjusted.toISOString().split("T")[0],
        source.time,
        source.tzOffset
      );
      return {
        title: source.title,
        source: source.source,
        link: source.link,
        description: source.description,
        location: source.location,
        date: iso,
        category: source.category,
        attribution: "Source: inferred from BLS schedule (one day prior)."
      };
    });
  } catch (error) {
    if (source.fallback) {
      return generateFallbackEvents(source, error.message);
    }
    throw error;
  }
};

const generateAdpSchedule = (config) => {
  const events = [];
  const now = new Date();
  const months = config.horizonMonths ?? 8;
  for (let i = 0; i < months; i += 1) {
    const firstDay = new Date(now.getFullYear(), now.getMonth() + i, 1);
    const releaseDate = new Date(firstDay);
    while (releaseDate.getDay() !== 3) {
      releaseDate.setDate(releaseDate.getDate() + 1);
    }
    const iso = combineDateAndTime(
      releaseDate.toISOString().split("T")[0],
      config.time,
      config.tzOffset
    );
    if (new Date(iso) >= now) {
      events.push({
        title: config.title,
        source: config.source,
        link: config.link,
        description: config.description,
        location: config.location || "Online release",
        date: iso,
        category: config.category,
        attribution: "Source: ADPemploymentreport.com"
      });
    }
  }
  return events;
};

const generateWeeklyClaims = (config) => {
  const events = [];
  const weeks = config.weeks ?? 8;
  const now = new Date();
  const nextThursday = new Date(now);
  nextThursday.setDate(nextThursday.getDate() + ((11 - nextThursday.getDay()) % 7));
  nextThursday.setHours(0, 0, 0, 0);

  for (let i = 0; i < weeks; i += 1) {
    const date = new Date(nextThursday);
    date.setDate(nextThursday.getDate() + i * 7);
    const iso = combineDateAndTime(
      date.toISOString().split("T")[0],
      config.time,
      config.tzOffset
    );
    events.push({
      title: config.title,
      source: config.source,
      link: config.link,
      description: `${config.description} (week ending ${date
        .toISOString()
        .split("T")[0]}).`,
      location: config.location || "Washington, DC",
      date: iso,
      category: config.category,
      attribution: "Source: U.S. Department of Labor"
    });
  }
  return events;
};

const fetchFedMeetings = async (config) => {
  let html;
  try {
    html = await fetchWithProxies(FED_CALENDAR_URL);
  } catch (error) {
    if (config.fallback) {
      return generateFallbackEvents(
        config,
        `Fed site unavailable (${error.message}).`
      );
    }
    throw new Error(`Fed site unavailable (${error.message}).`);
  }
  const doc = new DOMParser().parseFromString(html, "text/html");
  const textNodes = Array.from(doc.querySelectorAll("p, li, td, strong, h3")).map(
    (node) => node.textContent.trim()
  );
  const regex =
    /([A-Za-z]+)\s(\d{1,2})(?:\s?(?:–|-|—)\s?(\d{1,2}))?,\s(20\d{2})/g;
  const seen = new Set();
  const events = [];
  const now = new Date();

  textNodes.forEach((text) => {
    let match;
    while ((match = regex.exec(text)) !== null) {
      const [, month, day] = match;
      const year = match[4];
      const key = `${month}-${year}`;
      if (seen.has(key)) continue;
      const iso = combineDateAndTime(
        `${month} ${day}, ${year}`,
        config.time,
        config.tzOffset
      );
      if (Number.isNaN(new Date(iso).getTime()) || new Date(iso) < now) {
        continue;
      }
      seen.add(key);
      events.push({
        title: config.title,
        source: config.source,
        link: config.link,
        description: `${config.description} (${text}).`,
        location: config.location || "Washington, DC",
        date: iso,
        category: config.category,
        attribution: "Source: FederalReserve.gov"
      });
    }
  });

  if (!events.length) {
    if (config.fallback) {
      return generateFallbackEvents(config, "Fed meeting dates not found.");
    }
    throw new Error("Fed meeting dates not found.");
  }
  return events.slice(0, 10);
};

const fetchAutoSource = async (source) => {
  switch (source.type) {
    case "fredRelease":
      return fetchFredReleaseEvents(source);
    case "revelio":
      return fetchRevelioEvents(source);
    case "adp":
      return generateAdpSchedule(source);
    case "weeklyClaims":
      return generateWeeklyClaims(source);
    case "fedMeetings":
      return fetchFedMeetings(source);
    case "manualList":
      return generateManualListSchedule(source);
    default:
      return [];
  }
};

const loadAutomaticEvents = async () => {
  if (state.autoLoading) return;
  state.autoLoading = true;
  state.autoNotes = [];
  setAutoStatus("Refreshing release schedule…");
  try {
    const results = await Promise.allSettled(
      autoSources.map((source) => fetchAutoSource(source))
    );
    const combined = [];
    const errors = [];
    results.forEach((result, index) => {
      if (result.status === "fulfilled" && result.value.length) {
        combined.push(...result.value);
      } else {
        const reason =
          result.status === "rejected"
            ? result.reason?.message
            : "No data returned";
        errors.push(`${autoSources[index].label}: ${reason}`);
      }
    });

    state.autoEvents = combined;
    const statusParts = [];
    let isError = false;
    if (combined.length) {
      statusParts.push(`Loaded ${combined.length} release slots.`);
    } else {
      statusParts.push("No automated releases available yet.");
    }
    if (errors.length) {
      isError = true;
      statusParts.push(`Issues: ${errors.join(" • ")}`);
    }
    setAutoStatus(statusParts.join(" / "), isError, state.autoNotes);
  } catch (error) {
    setAutoStatus(`Unable to refresh releases: ${error.message}`, true);
  } finally {
    state.autoLoading = false;
    renderCalendar();
  }
};

if (calendarFredApplyButton && calendarFredKeyInput) {
  calendarFredApplyButton.addEventListener("click", () => {
    const key = calendarFredKeyInput.value.trim();
    if (!key) {
      setAutoStatus("Enter a valid FRED API key.", true);
      return;
    }
    setStoredFredKey(key);
    setAutoStatus("FRED API key saved. Updating schedule…");
    loadAutomaticEvents();
  });
}

if (autoRefreshButton) {
  autoRefreshButton.addEventListener("click", loadAutomaticEvents);
}

if (toggleAddFormButton) {
  toggleAddFormButton.addEventListener("click", toggleFormVisibility);
}

if (addForm) {
  addForm.addEventListener("submit", handleFormSubmit);
}

if (viewToggleButtons.length) {
  viewToggleButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const view = button.dataset.view;
      if (!view || state.viewMode === view) return;
      state.viewMode = view;
      renderCalendar();
    });
  });
}

const adjustGridMonth = (delta) => {
  const next = new Date(state.gridMonth);
  next.setMonth(next.getMonth() + delta);
  next.setDate(1);
  state.gridMonth = next;
  renderCalendar();
};

if (calendarPrevMonth) {
  calendarPrevMonth.addEventListener("click", () => adjustGridMonth(-1));
}

if (calendarNextMonth) {
  calendarNextMonth.addEventListener("click", () => adjustGridMonth(1));
}

renderCalendar();
loadAutomaticEvents();
