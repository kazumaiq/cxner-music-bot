const appState = {
  activeTab: "home",
  filterArtist: "all",
  releases: [
    {
      id: "r1",
      title: "Eternal Lowrider",
      artist: "gostlxne",
      date: "2026-02-13",
      cover: "https://images.unsplash.com/photo-1464822759023-fed622ff2c3b?auto=format&fit=crop&w=900&q=80",
      links: {
        spotify: "https://open.spotify.com",
        apple: "https://music.apple.com",
        vk: "https://vk.com/music",
        yandex: "https://music.yandex.ru"
      }
    },
    {
      id: "r2",
      title: "Psycho Runner",
      artist: "rvincarnatixn",
      date: "2026-02-05",
      cover: "https://images.unsplash.com/photo-1508261305438-4d6f7f4ef7d1?auto=format&fit=crop&w=900&q=80",
      links: {
        spotify: "https://open.spotify.com",
        apple: "https://music.apple.com",
        vk: "https://vk.com/music",
        yandex: "https://music.yandex.ru"
      }
    },
    {
      id: "r3",
      title: "Distortion",
      artist: "zeepoon",
      date: "2026-02-08",
      cover: "https://images.unsplash.com/photo-1487180144351-b8472da7d491?auto=format&fit=crop&w=900&q=80",
      links: {
        spotify: "https://open.spotify.com",
        apple: "https://music.apple.com",
        vk: "https://vk.com/music",
        yandex: "https://music.yandex.ru"
      }
    },
    {
      id: "r4",
      title: "Night Engine",
      artist: "demyanovxx",
      date: "2026-02-01",
      cover: "https://images.unsplash.com/photo-1511379938547-c1f69419868d?auto=format&fit=crop&w=900&q=80",
      links: {
        spotify: "https://open.spotify.com",
        apple: "https://music.apple.com",
        vk: "https://vk.com/music",
        yandex: "https://music.yandex.ru"
      }
    },
    {
      id: "r5",
      title: "Abyss Drift",
      artist: "kazumaiq",
      date: "2026-01-29",
      cover: "https://images.unsplash.com/photo-1473186578172-c141e6798cf4?auto=format&fit=crop&w=900&q=80",
      links: {
        spotify: "https://open.spotify.com",
        apple: "https://music.apple.com",
        vk: "https://vk.com/music",
        yandex: "https://music.yandex.ru"
      }
    },
    {
      id: "r6",
      title: "Lost In Space",
      artist: "rvincarnatixn",
      date: "2026-01-26",
      cover: "https://images.unsplash.com/photo-1529429611278-7bb32d19e4a5?auto=format&fit=crop&w=900&q=80",
      links: {
        spotify: "https://open.spotify.com",
        apple: "https://music.apple.com",
        vk: "https://vk.com/music",
        yandex: "https://music.yandex.ru"
      }
    }
  ],
  artists: [],
  cabinet: {
    approved: false,
    releases: [],
    updatedAt: ""
  }
};

const LABEL_ARTISTS = [
  {
    name: "MVRTX",
    monthlyListeners: 389622,
    avatar: "assets/artists/mvrtx.png",
    profile: ""
  },
  {
    name: "MC LONE",
    monthlyListeners: 348861,
    avatar: "assets/artists/mc-lone.png",
    profile: ""
  },
  {
    name: "Balekajon",
    monthlyListeners: 259760,
    avatar: "assets/artists/balekajon.png",
    profile: ""
  },
  {
    name: "TendyOne",
    monthlyListeners: 257991,
    avatar: "assets/artists/tendyone.png",
    profile: ""
  },
  {
    name: "Hxlkart",
    monthlyListeners: 191340,
    avatar: "assets/artists/hxlkart.png",
    profile: ""
  },
  {
    name: "STAROX",
    monthlyListeners: 139396,
    avatar: "assets/artists/starox.png",
    profile: ""
  },
  {
    name: "Cerrera D'Ark",
    monthlyListeners: 77254,
    avatar: "assets/artists/cerrera-dark.png",
    profile: ""
  }
];

const HAS_DOM = typeof window !== "undefined" && typeof document !== "undefined";
const tg = HAS_DOM ? (window.Telegram?.WebApp ?? null) : null;
const DATE_PATTERN = /^(\d{2})\.(\d{2})\.(\d{4})$/;
const CABINET_USERS_URL = "data/cabinet-users.json";
const CABINET_RELEASES_URL = "data/releases-public.json";
const CABINET_REFRESH_MS = 15000;
const lazyObserver = typeof IntersectionObserver === "function"
  ? new IntersectionObserver(
    (entries, observer) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) {
          return;
        }
        const img = entry.target;
        if (img.dataset.src) {
          img.addEventListener(
            "error",
            () => {
              if (img.dataset.fallback) {
                img.src = img.dataset.fallback;
              }
            },
            { once: true }
          );
          img.src = img.dataset.src;
          img.removeAttribute("data-src");
          img.addEventListener(
            "load",
            () => img.classList.add("loaded"),
            { once: true }
          );
        }
        observer.unobserve(img);
      });
    },
    { rootMargin: "220px 0px" }
  )
  : null;

function initTelegramWebApp() {
  if (!tg) {
    return;
  }

  tg.ready();
  tg.expand();
  tg.enableClosingConfirmation?.();

  const user = tg.initDataUnsafe?.user;
  if (user) {
    const badge = document.getElementById("userBadge");
    const username = user.username ? `@${user.username}` : user.first_name || "Профиль";
    badge.textContent = username;
  }

  const params = tg.themeParams || {};
  const root = document.documentElement;
  if (params.bg_color) {
    root.style.setProperty("--bg", params.bg_color);
  }
  if (params.secondary_bg_color) {
    root.style.setProperty("--surface", params.secondary_bg_color);
  }
  if (params.text_color) {
    root.style.setProperty("--text", params.text_color);
  }
}

function safeOpenLink(url) {
  if (!url) {
    return;
  }
  if (tg?.openLink) {
    tg.openLink(url);
    return;
  }
  window.open(url, "_blank", "noopener,noreferrer");
}

function showToast(text) {
  const toast = document.getElementById("toast");
  toast.textContent = text;
  toast.classList.remove("hidden");
  window.clearTimeout(showToast._timer);
  showToast._timer = window.setTimeout(() => {
    toast.classList.add("hidden");
  }, 2600);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function observeLazyImages(scope = document) {
  if (!lazyObserver) {
    scope.querySelectorAll("img[data-src]").forEach((img) => {
      img.src = img.dataset.src;
      img.addEventListener(
        "error",
        () => {
          if (img.dataset.fallback) {
            img.src = img.dataset.fallback;
          }
        },
        { once: true }
      );
      img.classList.add("loaded");
    });
    return;
  }
  scope.querySelectorAll("img[data-src]").forEach((img) => lazyObserver.observe(img));
}

function formatDate(dateIso) {
  const date = new Date(dateIso);
  if (Number.isNaN(date.getTime())) {
    return dateIso;
  }
  return date.toLocaleDateString("ru-RU", {
    day: "2-digit",
    month: "short",
    year: "numeric"
  });
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString("ru-RU");
}

function limitText(value, maxLen) {
  const text = normalizeText(value);
  if (!text) {
    return "";
  }
  return text.length > maxLen ? text.slice(0, maxLen) : text;
}

function getByteLength(text) {
  if (typeof TextEncoder !== "undefined") {
    return new TextEncoder().encode(text).length;
  }
  try {
    return unescape(encodeURIComponent(text)).length;
  } catch {
    return String(text).length;
  }
}

function getCurrentUserId() {
  const user = tg?.initDataUnsafe?.user;
  if (!user || !user.id) {
    return "";
  }
  return String(user.id);
}

function getCabinetLocalKey(userId) {
  return `cxrner_cabinet_active_${userId}`;
}

function isCabinetActiveLocal(userId) {
  if (!userId) {
    return false;
  }
  try {
    return window.localStorage.getItem(getCabinetLocalKey(userId)) === "1";
  } catch {
    return false;
  }
}

function setCabinetActiveLocal(userId, active) {
  if (!userId) {
    return;
  }
  try {
    if (active) {
      window.localStorage.setItem(getCabinetLocalKey(userId), "1");
    } else {
      window.localStorage.removeItem(getCabinetLocalKey(userId));
    }
  } catch {
    // ignore storage errors
  }
}

function getStatusMeta(status) {
  const normalized = String(status || "on_upload");
  const map = {
    on_upload: { text: "На отгрузке", emoji: "🕓" },
    moderation: { text: "На модерации", emoji: "🧠" },
    approved: { text: "Одобрено", emoji: "✅" },
    rejected: { text: "Отклонено", emoji: "❌" },
    needs_fix: { text: "На исправлении", emoji: "✏️" },
    deleted: { text: "Удалён", emoji: "🗑" }
  };
  return map[normalized] || { text: normalized, emoji: "⏳" };
}

async function loadJsonSafe(url) {
  try {
    const res = await fetch(`${url}?t=${Date.now()}`, { cache: "no-store" });
    if (!res.ok) {
      return null;
    }
    return await res.json();
  } catch {
    return null;
  }
}

function renderCabinetSummary(releases) {
  const el = document.getElementById("cabinetSummary");
  if (!releases.length) {
    el.classList.add("hidden");
    el.innerHTML = "";
    return;
  }

  const counts = {
    total: releases.length,
    approved: releases.filter((r) => r.status === "approved").length,
    moderation: releases.filter((r) => r.status === "moderation").length,
    pending: releases.filter((r) => r.status === "on_upload").length
  };

  el.innerHTML = `
    <div class="cabinet-metric">
      <span class="cabinet-metric-value">${counts.total}</span>
      <span class="cabinet-metric-label">Всего релизов</span>
    </div>
    <div class="cabinet-metric">
      <span class="cabinet-metric-value">${counts.pending}</span>
      <span class="cabinet-metric-label">На отгрузке</span>
    </div>
    <div class="cabinet-metric">
      <span class="cabinet-metric-value">${counts.moderation}</span>
      <span class="cabinet-metric-label">На модерации</span>
    </div>
    <div class="cabinet-metric">
      <span class="cabinet-metric-value">${counts.approved}</span>
      <span class="cabinet-metric-label">Одобрено</span>
    </div>
  `;
  el.classList.remove("hidden");
}

function renderCabinetList(releases) {
  const list = document.getElementById("cabinetList");
  if (!releases.length) {
    list.innerHTML = `
      <article class="cabinet-item">
        <p class="cabinet-item-title">Пока нет релизов</p>
        <p class="cabinet-item-meta">Отправьте первую анкету во вкладке «Анкета».</p>
      </article>
    `;
    return;
  }

  const sorted = [...releases].sort((a, b) => {
    const aTime = new Date(a.submission_time || 0).getTime();
    const bTime = new Date(b.submission_time || 0).getTime();
    return bTime - aTime;
  });

  list.innerHTML = sorted.map((rel) => {
    const meta = getStatusMeta(rel.status);
    const typeText = rel.type || "релиз";
    const dateText = rel.date || "—";
    const reason = rel.reject_reason
      ? `<p class="cabinet-item-meta">Причина: ${escapeHtml(rel.reject_reason)}</p>`
      : "";
    const upc = rel.upc
      ? `<p class="cabinet-item-meta">UPC: ${escapeHtml(rel.upc)}</p>`
      : "";

    return `
      <article class="cabinet-item">
        <div class="cabinet-item-head">
          <p class="cabinet-item-title">${escapeHtml(rel.name || "Без названия")}</p>
          <span class="status-chip status-${escapeHtml(rel.status || "on_upload")}">
            ${meta.emoji} ${escapeHtml(meta.text)}
          </span>
        </div>
        <p class="cabinet-item-meta">${escapeHtml(typeText)} • ${escapeHtml(dateText)} • ${escapeHtml(rel.genre || "—")}</p>
        <p class="cabinet-item-meta">Артист: ${escapeHtml(rel.nick || "—")}</p>
        ${upc}
        ${reason}
      </article>
    `;
  }).join("");
}

async function refreshCabinet() {
  const bindCard = document.getElementById("cabinetBindCard");
  const statusCard = document.getElementById("cabinetStatusCard");
  const statusText = document.getElementById("cabinetStatusText");
  const userId = getCurrentUserId();

  if (!userId) {
    bindCard.classList.add("hidden");
    statusCard.classList.remove("hidden");
    statusText.textContent = "Откройте Mini App внутри Telegram, чтобы использовать кабинет.";
    document.getElementById("cabinetSummary").classList.add("hidden");
    document.getElementById("cabinetList").innerHTML = "";
    return;
  }

  const [cabinetJson, releasesJson] = await Promise.all([
    loadJsonSafe(CABINET_USERS_URL),
    loadJsonSafe(CABINET_RELEASES_URL)
  ]);

  const serverApproved = Boolean(cabinetJson?.users?.[userId]?.approved);
  const localApproved = isCabinetActiveLocal(userId);
  const approved = serverApproved || localApproved;
  appState.cabinet.approved = approved;
  appState.cabinet.updatedAt = releasesJson?.updated_at || "";

  if (!approved) {
    bindCard.classList.remove("hidden");
    statusCard.classList.remove("hidden");
    statusText.textContent = "Кабинет не активирован. Нажмите «Подтвердить вход».";
    document.getElementById("cabinetSummary").classList.add("hidden");
    document.getElementById("cabinetList").innerHTML = "";
    return;
  }

  bindCard.classList.add("hidden");
  statusCard.classList.remove("hidden");
  statusText.textContent = "Кабинет активен. Статусы синхронизируются с ботом и модерацией.";

  const userReleases = releasesJson?.users?.[userId] || [];
  const visible = userReleases.filter((rel) => !rel.user_deleted);
  appState.cabinet.releases = visible;
  renderCabinetSummary(visible);
  renderCabinetList(visible);
}

function activateCabinet() {
  const userId = getCurrentUserId();
  if (!userId) {
    showToast("Откройте Mini App из Telegram.");
    return;
  }

  const payload = {
    action: "cabinet_activate",
    source: "mini_app",
    submitted_at: new Date().toISOString(),
    user: tg?.initDataUnsafe?.user || null
  };

  if (tg?.sendData) {
    tg.sendData(JSON.stringify(payload));
    setCabinetActiveLocal(userId, true);
    showToast("Запрос на активацию отправлен. Обновляем кабинет...");
    refreshCabinet();
    return;
  }

  showToast("Привязка кабинета доступна только внутри Telegram-бота.");
}

function buildArtistsCatalog() {
  // Порядок важен: от большего числа слушателей к меньшему.
  appState.artists = LABEL_ARTISTS.map((artist) => ({ ...artist }));
}

function renderStats() {
  document.getElementById("statReleases").textContent = String(appState.releases.length);
  document.getElementById("statArtists").textContent = String(appState.artists.length);
}

function renderFreshReleases() {
  const container = document.getElementById("freshList");
  const list = appState.releases.slice(0, 6);
  container.innerHTML = list.map((rel) => `
    <article class="release-card glass">
      <img class="lazy" data-src="${rel.cover}" alt="${escapeHtml(rel.title)}" loading="lazy" decoding="async">
      <div>
        <p class="release-title">${escapeHtml(rel.title)}</p>
        <p class="release-artist">${escapeHtml(rel.artist)}</p>
      </div>
      <button class="btn btn-neon" data-listen="${rel.id}" type="button">Слушать</button>
    </article>
  `).join("");
  observeLazyImages(container);
}

function renderArtistFilter() {
  const select = document.getElementById("artistFilter");
  const releaseArtists = [...new Set(appState.releases.map((rel) => rel.artist))]
    .sort((a, b) => a.localeCompare(b));
  const options = [
    { value: "all", label: "Все артисты" },
    ...releaseArtists.map((name) => ({ value: name, label: name }))
  ];
  select.innerHTML = options
    .map((opt) => `<option value="${escapeHtml(opt.value)}">${escapeHtml(opt.label)}</option>`)
    .join("");
  select.value = appState.filterArtist;
}

function renderReleasesGrid() {
  const grid = document.getElementById("releaseGrid");
  const list = appState.filterArtist === "all"
    ? appState.releases
    : appState.releases.filter((rel) => rel.artist === appState.filterArtist);

  grid.innerHTML = list.map((rel) => `
    <article class="release-card glass">
      <img class="lazy" data-src="${rel.cover}" alt="${escapeHtml(rel.title)}" loading="lazy" decoding="async">
      <div>
        <p class="release-title">${escapeHtml(rel.title)}</p>
        <p class="release-artist">${escapeHtml(rel.artist)}</p>
        <p class="release-date">${formatDate(rel.date)}</p>
      </div>
      <button class="btn btn-ghost" data-open-release="${rel.id}" type="button">Открыть релиз</button>
    </article>
  `).join("");
  observeLazyImages(grid);
}

function renderArtists() {
  const grid = document.getElementById("artistGrid");
  grid.innerHTML = appState.artists
    .map((artist) => `
      <article class="artist-card glass">
        <div class="artist-head">
          <img class="artist-avatar lazy" data-src="${artist.avatar}" alt="${escapeHtml(artist.name)}" loading="lazy" decoding="async">
          <div>
            <p class="artist-name">${escapeHtml(artist.name)}</p>
            <p class="artist-meta">Слушателей в месяц: ${formatNumber(artist.monthlyListeners)}</p>
          </div>
        </div>
        <button class="btn btn-ghost" data-open-artist="${escapeHtml(artist.name)}" data-artist-link="${escapeHtml(artist.profile || "")}" type="button">Открыть профиль</button>
      </article>
    `)
    .join("");
  observeLazyImages(grid);
}

function openReleaseModal(releaseId) {
  const release = appState.releases.find((item) => item.id === releaseId);
  if (!release) {
    return;
  }

  const body = document.getElementById("modalBody");
  body.innerHTML = `
    <img class="cover-img loaded" src="${release.cover}" alt="${escapeHtml(release.title)}">
    <h3>${escapeHtml(release.title)}</h3>
    <p class="release-artist">${escapeHtml(release.artist)}</p>
    <p class="release-date">${formatDate(release.date)}</p>
    <div class="stream-grid">
      <button class="btn btn-neon" data-stream-url="${release.links.spotify}" type="button">Spotify</button>
      <button class="btn btn-neon" data-stream-url="${release.links.apple}" type="button">Apple Music</button>
      <button class="btn btn-neon" data-stream-url="${release.links.vk}" type="button">VK Music</button>
      <button class="btn btn-neon" data-stream-url="${release.links.yandex}" type="button">Yandex Music</button>
    </div>
  `;

  document.getElementById("releaseModal").classList.remove("hidden");
}

function closeReleaseModal() {
  document.getElementById("releaseModal").classList.add("hidden");
}

function switchTab(tabId) {
  appState.activeTab = tabId;
  document.querySelectorAll(".view").forEach((view) => {
    view.classList.toggle("active", view.id === tabId);
  });
  document.querySelectorAll(".nav-item").forEach((item) => {
    item.classList.toggle("active", item.dataset.tab === tabId);
  });
  if (tabId === "cabinet") {
    refreshCabinet();
  }
  syncMainButton();
}

function normalizeText(value) {
  return String(value ?? "").trim();
}

function parseRuDate(dateText) {
  const match = DATE_PATTERN.exec(dateText);
  if (!match) {
    return null;
  }
  const day = Number(match[1]);
  const month = Number(match[2]);
  const year = Number(match[3]);
  const date = new Date(year, month - 1, day, 12, 0, 0, 0);
  if (
    date.getFullYear() !== year ||
    date.getMonth() !== month - 1 ||
    date.getDate() !== day
  ) {
    return null;
  }
  return date;
}

function isHttpUrl(text) {
  try {
    const url = new URL(text);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

function updateTracklistVisibility() {
  const typeSelect = document.getElementById("releaseType");
  const wrap = document.getElementById("tracklistFieldWrap");
  const field = document.getElementById("tracklistField");
  const isAlbum = typeSelect.value === "альбом";
  wrap.classList.toggle("visible", isAlbum);
  field.required = isAlbum;
  if (!isAlbum) {
    field.value = "";
  }
}

function buildSubmitPayload(form) {
  const formData = new FormData(form);
  const values = {
    type: limitText(formData.get("type"), 20),
    name: limitText(formData.get("name"), 160),
    subname: limitText(formData.get("subname"), 90) || ".",
    has_lyrics: limitText(formData.get("has_lyrics"), 60),
    nick: limitText(formData.get("nick"), 90),
    fio: limitText(formData.get("fio"), 130),
    date: limitText(formData.get("date"), 20),
    version: limitText(formData.get("version"), 90) || "Оригинал",
    genre: limitText(formData.get("genre"), 90),
    link: limitText(formData.get("link"), 320),
    yandex: limitText(formData.get("yandex"), 320) || ".",
    mat: limitText(formData.get("mat"), 20),
    promo: limitText(formData.get("promo"), 260) || ".",
    comment: limitText(formData.get("comment"), 260) || ".",
    tracklist: limitText(formData.get("tracklist"), 260) || ".",
    tg: limitText(formData.get("tg"), 180)
  };

  const errors = [];
  if (!values.type) {
    errors.push("Выберите тип релиза.");
  }
  if (!values.name) {
    errors.push("Введите название релиза.");
  }
  if (!values.has_lyrics) {
    errors.push("Укажите, есть ли слова в релизе.");
  }
  if (!values.nick) {
    errors.push("Введите ник исполнителя.");
  }
  if (!values.fio) {
    errors.push("Введите ФИО исполнителя.");
  }
  if (!values.date) {
    errors.push("Укажите дату релиза.");
  }
  if (!values.genre) {
    errors.push("Введите жанр.");
  }
  if (!values.link) {
    errors.push("Добавьте ссылку на файлы.");
  }
  if (!values.tg) {
    errors.push("Укажите Telegram для связи.");
  }
  if (!values.mat) {
    errors.push("Выберите, есть ли ненормативная лексика.");
  }

  const parsedDate = parseRuDate(values.date);
  if (!parsedDate) {
    errors.push("Дата должна быть в формате ДД.ММ.ГГГГ.");
  } else {
    const minDays = values.type === "альбом" ? 7 : 3;
    const minDate = new Date();
    minDate.setHours(0, 0, 0, 0);
    minDate.setDate(minDate.getDate() + minDays);
    if (parsedDate < minDate) {
      errors.push(`Дата релиза должна быть минимум через ${minDays} дней.`);
    }
  }

  if (values.link && !isHttpUrl(values.link)) {
    errors.push("Ссылка на файлы должна начинаться с http:// или https://.");
  }
  if (values.yandex !== "." && !isHttpUrl(values.yandex)) {
    errors.push("Поле Яндекс Музыка: укажите URL или точку.");
  }
  if (values.type === "альбом" && values.tracklist === ".") {
    errors.push("Для альбома обязательно заполните Tracklist.");
  }

  if (values.type !== "альбом") {
    values.tracklist = ".";
  }

  const payload = {
    action: "webapp_release_submit",
    source: "mini_app",
    version: 2,
    submitted_at: new Date().toISOString(),
    user: tg?.initDataUnsafe?.user || null,
    form: values
  };

  const payloadJson = JSON.stringify(payload);
  const payloadBytes = getByteLength(payloadJson);
  if (payloadBytes > 3800) {
    errors.push("Анкета слишком большая. Сократите промо, комментарий и tracklist.");
  }

  if (errors.length) {
    return { errors, payload: null, payloadJson: "" };
  }

  return { errors: [], payload, payloadJson };
}

function submitReleaseForm(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const result = buildSubmitPayload(form);

  if (result.errors.length) {
    tg?.HapticFeedback?.notificationOccurred?.("error");
    showToast(result.errors[0]);
    return;
  }

  if (tg?.sendData) {
    tg.sendData(result.payloadJson || JSON.stringify(result.payload));
    tg.HapticFeedback?.notificationOccurred?.("success");
    showToast("Анкета отправлена. Проверьте «Кабинет» и чат с ботом.");
    form.reset();
    updateTracklistVisibility();
    syncMainButton();
    switchTab("cabinet");
    return;
  } else {
    showToast("Откройте Mini App через кнопку «Открыть приложение» в боте.");
    return;
  }
}

function syncMainButton() {
  if (!tg?.MainButton) {
    return;
  }
  const canShow = appState.activeTab === "submit";
  tg.MainButton.setParams({ color: "#8154ff", text_color: "#ffffff", is_visible: canShow });
  tg.MainButton.setText("Отправить анкету");
  tg.MainButton.offClick(handleMainButtonClick);
  tg.MainButton.onClick(handleMainButtonClick);
  if (canShow && !document.getElementById("submitForm").checkValidity()) {
    tg.MainButton.setText("Заполните анкету");
  }
  if (canShow) {
    tg.MainButton.show();
  } else {
    tg.MainButton.hide();
  }
}

function handleMainButtonClick() {
  document.getElementById("submitForm").requestSubmit();
}

function wireEvents() {
  document.addEventListener("click", (event) => {
    const navBtn = event.target.closest(".nav-item");
    if (navBtn) {
      switchTab(navBtn.dataset.tab);
      return;
    }

    const gotoBtn = event.target.closest("[data-goto]");
    if (gotoBtn) {
      switchTab(gotoBtn.dataset.goto);
      return;
    }

    const listenBtn = event.target.closest("[data-listen]");
    if (listenBtn) {
      const release = appState.releases.find((item) => item.id === listenBtn.dataset.listen);
      if (release) {
        safeOpenLink(release.links.spotify);
      }
      return;
    }

    const releaseBtn = event.target.closest("[data-open-release]");
    if (releaseBtn) {
      openReleaseModal(releaseBtn.dataset.openRelease);
      return;
    }

    const streamBtn = event.target.closest("[data-stream-url]");
    if (streamBtn) {
      safeOpenLink(streamBtn.dataset.streamUrl);
      return;
    }

    const artistBtn = event.target.closest("[data-open-artist]");
    if (artistBtn) {
      const directLink = artistBtn.dataset.artistLink;
      if (directLink) {
        safeOpenLink(directLink);
      } else {
        showToast(`Профиль ${artistBtn.dataset.openArtist} пока без ссылки.`);
      }
      return;
    }

    const contactBtn = event.target.closest("[data-link]");
    if (contactBtn) {
      safeOpenLink(contactBtn.dataset.link);
      return;
    }

    if (event.target.closest("[data-close-modal]") || event.target.closest("#modalCloseBtn")) {
      closeReleaseModal();
      return;
    }
  });

  document.getElementById("artistFilter").addEventListener("change", (event) => {
    appState.filterArtist = event.target.value;
    renderReleasesGrid();
  });

  const form = document.getElementById("submitForm");
  form.addEventListener("submit", submitReleaseForm);
  form.addEventListener("input", syncMainButton);
  form.addEventListener("change", syncMainButton);

  document.getElementById("releaseType").addEventListener("change", () => {
    updateTracklistVisibility();
    syncMainButton();
  });

  const cabinetActivateBtn = document.getElementById("cabinetActivateBtn");
  if (cabinetActivateBtn) {
    cabinetActivateBtn.addEventListener("click", activateCabinet);
  }

  document.getElementById("userBadge").addEventListener("click", () => {
    safeOpenLink("https://t.me/cxrnermusic");
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeReleaseModal();
    }
  });
}

function setupParallax() {
  const layerA = document.querySelector(".layer-a");
  const layerB = document.querySelector(".layer-b");
  if (!layerA || !layerB) {
    return;
  }

  let targetX = 0;
  let targetY = 0;
  let ticking = false;

  function paint() {
    layerA.style.transform = `translate3d(${targetX * 14}px, ${targetY * 14}px, 0)`;
    layerB.style.transform = `translate3d(${targetX * -12}px, ${targetY * -12}px, 0)`;
    ticking = false;
  }

  window.addEventListener("pointermove", (event) => {
    targetX = ((event.clientX / window.innerWidth) - 0.5) * 2;
    targetY = ((event.clientY / window.innerHeight) - 0.5) * 2;
    if (!ticking) {
      ticking = true;
      requestAnimationFrame(paint);
    }
  });
}

function hideLoader() {
  document.getElementById("loader").classList.add("hidden");
  document.getElementById("app").classList.remove("hidden");
}

function bootstrap() {
  initTelegramWebApp();
  buildArtistsCatalog();
  renderStats();
  renderFreshReleases();
  renderArtistFilter();
  renderReleasesGrid();
  renderArtists();
  wireEvents();
  setupParallax();
  updateTracklistVisibility();
  syncMainButton();
  observeLazyImages(document);
  refreshCabinet();
  window.setInterval(() => {
    if (appState.activeTab === "cabinet") {
      refreshCabinet();
    }
  }, CABINET_REFRESH_MS);

  window.setTimeout(hideLoader, 550);
}

if (HAS_DOM) {
  window.addEventListener("load", bootstrap);
} else {
  // If hosting starts this file with Node.js, run Python bot as a fallback launcher.
  if (typeof process !== "undefined" && process?.versions?.node) {
    // eslint-disable-next-line no-console
    console.info("Mini App script started in Node.js, switching to Python bot launcher...");
    try {
      // eslint-disable-next-line global-require
      const path = require("node:path");
      // eslint-disable-next-line global-require
      const cp = require("node:child_process");

      const projectRoot = path.resolve(__dirname, "..");
      const mainPy = path.join(projectRoot, "main.py");
      const candidates = [process.env.PYTHON_BIN, "python3", "python"].filter(Boolean);

      let pythonBin = null;
      for (const bin of candidates) {
        const check = cp.spawnSync(bin, ["--version"], { stdio: "ignore" });
        if (check.status === 0) {
          pythonBin = bin;
          break;
        }
      }

      if (!pythonBin) {
        // eslint-disable-next-line no-console
        console.error("Python runtime not found. Set PYTHON_BIN or configure Python app start.");
        process.exit(1);
      }

      // eslint-disable-next-line no-console
      console.info(`Starting bot: ${pythonBin} -u ${mainPy}`);
      const child = cp.spawn(pythonBin, ["-u", mainPy], {
        cwd: projectRoot,
        stdio: "inherit",
        env: process.env
      });

      const forwardSignal = (signal) => {
        try {
          child.kill(signal);
        } catch {
          // ignore
        }
      };
      process.on("SIGTERM", () => forwardSignal("SIGTERM"));
      process.on("SIGINT", () => forwardSignal("SIGINT"));
      child.on("exit", (code) => process.exit(typeof code === "number" ? code : 1));
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error("Failed to start Python bot from Node launcher:", err);
      process.exit(1);
    }
  }
}
