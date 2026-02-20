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
let tg = HAS_DOM ? (window.Telegram?.WebApp ?? null) : null;
const DATE_PATTERN = /^(\d{2})\.(\d{2})\.(\d{4})$/;
const SUPABASE_CONFIG_URL = "data/supabase-config.json";
const CABINET_REFRESH_MS = 15000;
const supabaseRuntime = {
  url: "",
  anonKey: "",
  schema: "public",
  formsTable: "cxrner_forms",
  usersTable: "cxrner_users",
  releasesTable: "cxrner_public_releases",
  botApiBaseUrl: ""
};
let supabaseConfigLoaded = false;
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

function getTelegramWebApp() {
  if (!HAS_DOM) {
    return null;
  }
  if (window.Telegram?.WebApp) {
    tg = window.Telegram.WebApp;
  }
  return tg;
}

function getLaunchUserFromUrl() {
  if (!HAS_DOM) {
    return null;
  }
  const sources = [];
  if (window.location.hash) {
    sources.push(window.location.hash.replace(/^#/, ""));
  }
  if (window.location.search) {
    sources.push(window.location.search.replace(/^\?/, ""));
  }

  for (const rawSource of sources) {
    try {
      const params = new URLSearchParams(rawSource);
      const tgData = params.get("tgWebAppData");
      if (!tgData) {
        continue;
      }
      const tgParams = new URLSearchParams(tgData);
      const rawUser = tgParams.get("user");
      if (!rawUser) {
        continue;
      }
      const parsed = JSON.parse(rawUser);
      if (parsed && parsed.id) {
        return parsed;
      }
    } catch {
      // ignore malformed launch params
    }
  }
  return null;
}

function getTelegramUser() {
  const sdkUser = getTelegramWebApp()?.initDataUnsafe?.user;
  if (sdkUser && sdkUser.id) {
    return sdkUser;
  }
  return getLaunchUserFromUrl();
}

function getTelegramInitData() {
  const sdk = getTelegramWebApp();
  if (sdk?.initData) {
    return String(sdk.initData);
  }
  if (!HAS_DOM) {
    return "";
  }
  const sources = [];
  if (window.location.hash) {
    sources.push(window.location.hash.replace(/^#/, ""));
  }
  if (window.location.search) {
    sources.push(window.location.search.replace(/^\?/, ""));
  }
  for (const rawSource of sources) {
    try {
      const params = new URLSearchParams(rawSource);
      const tgData = params.get("tgWebAppData");
      if (tgData) {
        return tgData;
      }
    } catch {
      // ignore malformed params
    }
  }
  return "";
}

async function ensureSupabaseConfig() {
  if (supabaseConfigLoaded) {
    return;
  }
  supabaseConfigLoaded = true;
  try {
    const res = await fetch(`${SUPABASE_CONFIG_URL}?t=${Date.now()}`, { cache: "no-store" });
    if (!res.ok) {
      return;
    }
    const cfg = await res.json().catch(() => ({}));
    if (cfg && typeof cfg === "object") {
      supabaseRuntime.url = normalizeText(cfg.url || cfg.supabaseUrl || supabaseRuntime.url);
      supabaseRuntime.anonKey = normalizeText(cfg.anonKey || cfg.supabaseAnonKey || supabaseRuntime.anonKey);
      supabaseRuntime.schema = normalizeText(cfg.schema || supabaseRuntime.schema) || "public";
      supabaseRuntime.formsTable = normalizeText(cfg.formsTable || supabaseRuntime.formsTable) || "cxrner_forms";
      supabaseRuntime.usersTable = normalizeText(cfg.usersTable || supabaseRuntime.usersTable) || "cxrner_users";
      supabaseRuntime.releasesTable = normalizeText(cfg.releasesTable || supabaseRuntime.releasesTable) || "cxrner_public_releases";
      supabaseRuntime.botApiBaseUrl = normalizeText(
        cfg.botApiBaseUrl || cfg.bot_api_base_url || cfg.apiBaseUrl || supabaseRuntime.botApiBaseUrl
      );
    }
  } catch {
    // ignore runtime config fetch errors
  }
}

function hasSupabaseRuntime() {
  return Boolean(supabaseRuntime.url && supabaseRuntime.anonKey);
}

function supabaseRestUrl(pathAndQuery) {
  const base = String(supabaseRuntime.url || "").replace(/\/+$/, "");
  return `${base}/rest/v1/${pathAndQuery}`;
}

function miniappApiBaseUrl() {
  const base = normalizeText(supabaseRuntime.botApiBaseUrl || "");
  if (base) return base.replace(/\/+$/, "");
  if (typeof window !== "undefined" && window.location && window.location.origin) {
    return String(window.location.origin).replace(/\/+$/, "");
  }
  return "";
}

async function postMiniappApi(pathname, payload) {
  const base = miniappApiBaseUrl();
  if (!base) {
    throw new Error("botApiBaseUrl не настроен");
  }
  const url = `${base}${pathname.startsWith("/") ? pathname : `/${pathname}`}`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload || {})
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data?.ok) {
    const details = data?.errors?.[0] || data?.reason || data?.error || `${res.status} ${res.statusText}`;
    throw new Error(String(details));
  }
  return data;
}

async function supabaseSelectRows(tableName, selectExpr, filters = [], orderExpr = "", limit = 0) {
  if (!hasSupabaseRuntime()) {
    return [];
  }
  const query = [];
  query.push(`select=${encodeURIComponent(selectExpr)}`);
  filters.forEach((row) => {
    const key = normalizeText(row?.key);
    const op = normalizeText(row?.op || "eq");
    const value = normalizeText(row?.value);
    if (!key || !value) {
      return;
    }
    query.push(`${encodeURIComponent(key)}=${encodeURIComponent(`${op}.${value}`)}`);
  });
  if (orderExpr) {
    query.push(`order=${encodeURIComponent(orderExpr)}`);
  }
  if (limit > 0) {
    query.push(`limit=${Number(limit)}`);
  }
  const url = supabaseRestUrl(`${tableName}?${query.join("&")}`);
  const headers = {
    apikey: supabaseRuntime.anonKey,
    Authorization: `Bearer ${supabaseRuntime.anonKey}`
  };
  if (supabaseRuntime.schema && supabaseRuntime.schema !== "public") {
    headers["Accept-Profile"] = supabaseRuntime.schema;
  }
  const res = await fetch(url, { headers, cache: "no-store" });
  if (!res.ok) {
    throw new Error(`supabase ${tableName}: ${res.status}`);
  }
  const json = await res.json().catch(() => []);
  return Array.isArray(json) ? json : [];
}

function mapFormStatusToCabinetStatus(status) {
  const normalized = normalizeText(status).toLowerCase();
  if (normalized === "approved") {
    return "approved";
  }
  if (normalized === "rejected") {
    return "rejected";
  }
  if (normalized === "on_moderation") {
    return "moderation";
  }
  return "on_upload";
}

function mapSupabaseFormToCabinetRelease(row) {
  const payload = row?.form_payload && typeof row.form_payload === "object" ? row.form_payload : {};
  const releaseType = normalizeText(row?.release_type || payload?.release_type || payload?.type).toLowerCase();
  const typeText = ["album", "альбом"].includes(releaseType) ? "альбом" : "сингл";
  return {
    id: normalizeText(row?.id || row?.form_id || payload?.id || `${row?.telegram_id || "u"}_${row?.submission_key || row?.created_at || Date.now()}`),
    type: typeText,
    name: normalizeText(payload?.name || row?.track_name),
    nick: normalizeText(payload?.nick || row?.artist_name),
    date: normalizeText(payload?.date || row?.release_date || ""),
    genre: normalizeText(payload?.genre || row?.genre || ""),
    status: mapFormStatusToCabinetStatus(row?.status),
    reject_reason: normalizeText(row?.reject_reason || payload?.reject_reason || ""),
    upc: normalizeText(row?.upc || payload?.upc || ""),
    submission_time: normalizeText(row?.submission_key || row?.created_at || payload?.submission_time || ""),
    moderation_time: normalizeText(row?.updated_at || payload?.moderation_time || ""),
    user_deleted: false
  };
}

async function loadCabinetFromSupabase(userId) {
  await ensureSupabaseConfig();
  if (!hasSupabaseRuntime()) {
    return { ok: false, error: "SUPABASE_NOT_CONFIGURED", approved: false, releases: [] };
  }
  const [userRows, formRows, approvedRows] = await Promise.all([
    supabaseSelectRows(
      supabaseRuntime.usersTable,
      "telegram_id,username,first_name,cabinet_active,created_at,updated_at",
      [{ key: "telegram_id", value: userId }],
      "updated_at.desc",
      1
    ).catch(() => []),
    supabaseSelectRows(
      supabaseRuntime.formsTable,
      "id,form_id,telegram_id,artist_name,track_name,genre,release_type,status,reject_reason,upc,submission_key,created_at,updated_at,form_payload",
      [{ key: "telegram_id", value: userId }],
      "created_at.desc",
      300
    ).catch(() => []),
    supabaseSelectRows(
      supabaseRuntime.releasesTable,
      "form_id,telegram_id,artist_name,track_name,genre,release_type,status,approved_at,updated_at,release_data",
      [{ key: "telegram_id", value: userId }, { key: "status", value: "approved" }],
      "approved_at.desc",
      300
    ).catch(() => [])
  ]);
  const userRow = Array.isArray(userRows) && userRows.length ? userRows[0] : null;
  const releaseMap = new Map();
  (Array.isArray(formRows) ? formRows : []).forEach((row) => {
    const mapped = mapSupabaseFormToCabinetRelease(row);
    releaseMap.set(mapped.id, mapped);
  });
  (Array.isArray(approvedRows) ? approvedRows : []).forEach((row) => {
    const data = row?.release_data && typeof row.release_data === "object" ? row.release_data : {};
    const id = normalizeText(row?.form_id || data?.supabase_form_id || `${userId}_${row?.approved_at || Date.now()}`);
    if (releaseMap.has(id)) {
      const prev = releaseMap.get(id);
      releaseMap.set(id, { ...prev, status: "approved" });
      return;
    }
    releaseMap.set(id, {
      id,
      type: normalizeText(data?.type || row?.release_type) || "сингл",
      name: normalizeText(data?.name || row?.track_name),
      nick: normalizeText(data?.nick || row?.artist_name),
      date: normalizeText(data?.date || ""),
      genre: normalizeText(data?.genre || row?.genre || ""),
      status: "approved",
      reject_reason: "",
      upc: normalizeText(data?.upc || ""),
      submission_time: normalizeText(data?.submission_time || row?.approved_at || ""),
      moderation_time: normalizeText(data?.moderation_time || row?.updated_at || ""),
      user_deleted: false
    });
  });
  const releases = Array.from(releaseMap.values());
  return {
    ok: true,
    approved: Boolean(userRow?.cabinet_active),
    releases,
    updatedAt: userRow?.updated_at || new Date().toISOString()
  };
}

function initTelegramWebApp() {
  const tgApp = getTelegramWebApp();
  if (!tgApp) {
    console.error("Telegram.WebApp is undefined");
    return;
  }

  tgApp.ready();
  tgApp.expand();
  tgApp.enableClosingConfirmation?.();

  const user = getTelegramUser();
  if (user) {
    const badge = document.getElementById("userBadge");
    const username = user.username ? `@${user.username}` : user.first_name || "Профиль";
    badge.textContent = username;
  }

  const params = tgApp.themeParams || {};
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

function logWebAppSendDiagnostics(reason = "submit_form") {
  const tgApp = getTelegramWebApp();
  if (!tgApp) {
    console.error("Telegram.WebApp is undefined");
    return null;
  }
  try {
    // Required diagnostics for Mini App sendData chain.
    console.log("SEND DATA TRIGGERED");
    console.log(Telegram.WebApp.initData);
    console.log(Telegram.WebApp.initDataUnsafe);
    console.log(`[WEBAPP_DIAG] reason=${reason}`);
  } catch (e) {
    console.error("[WEBAPP_DIAG] diagnostics log failed", e);
  }
  return tgApp;
}

function safeOpenLink(url) {
  if (!url) {
    return;
  }
  const tgApp = getTelegramWebApp();
  if (tgApp?.openLink) {
    tgApp.openLink(url);
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
  const user = getTelegramUser();
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
    deleted: { text: "Удалено", emoji: "🗑" }
  };
  return map[normalized] || { text: normalized, emoji: "⏳" };
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
    statusText.textContent = "Mini App открыт без авторизации Telegram. Запускайте его через кнопку в боте.";
    document.getElementById("cabinetSummary").classList.add("hidden");
    document.getElementById("cabinetList").innerHTML = "";
    return;
  }

  const supabaseResult = await loadCabinetFromSupabase(userId).catch(() => ({
    ok: false,
    approved: false,
    releases: [],
    updatedAt: ""
  }));

  const serverApproved = Boolean(supabaseResult?.approved);
  const localApproved = isCabinetActiveLocal(userId);
  const approved = serverApproved || localApproved;
  appState.cabinet.approved = approved;
  appState.cabinet.updatedAt = supabaseResult?.updatedAt || "";

  if (!supabaseResult?.ok) {
    bindCard.classList.remove("hidden");
    statusCard.classList.remove("hidden");
    statusText.textContent = "Нет подключения к Supabase. Проверьте data/supabase-config.json.";
    document.getElementById("cabinetSummary").classList.add("hidden");
    document.getElementById("cabinetList").innerHTML = "";
    return;
  }

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
  statusText.textContent = "Кабинет активен. Статусы синхронизируются с Supabase и ботом.";

  const userReleases = Array.isArray(supabaseResult?.releases) ? supabaseResult.releases : [];
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
    version: 3,
    telegram_id: userId,
    init_data: getTelegramInitData(),
    request_id: `cab_${Date.now()}`,
    submitted_at: new Date().toISOString(),
    user: getTelegramUser() || null
  };

  const tgApp = getTelegramWebApp();
  if (tgApp?.sendData) {
    tgApp.sendData(JSON.stringify(payload));
    setCabinetActiveLocal(userId, true);
    showToast("Запрос на активацию отправлен. Обновляем кабинет...");
    refreshCabinet();
    return;
  }

  showToast("Привязка кабинета доступна только внутри Telegram-бота.");
}

function requestCabinetSync() {
  const userId = getCurrentUserId();
  if (!userId) {
    return;
  }
  const tgApp = getTelegramWebApp();
  if (!tgApp?.sendData) {
    return;
  }
  const payload = {
    action: "cabinet_sync_request",
    source: "mini_app",
    version: 3,
    telegram_id: userId,
    init_data: getTelegramInitData(),
    request_id: `sync_${Date.now()}`,
    submitted_at: new Date().toISOString(),
    user: getTelegramUser() || null
  };
  tgApp.sendData(JSON.stringify(payload));
}

function buildArtistsCatalog() {
  // Порядок важен: от большего числа слушателей к меньшему.
  appState.artists = LABEL_ARTISTS.map((artist) => ({ ...artist }));
}

function renderStats() {
  document.getElementById("statReleases").textContent = String(appState.releases.length);
  document.getElementById("statArtists").textContent = String(appState.artists.length);
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
    requestCabinetSync();
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

function normalizeReleaseTypeValue(value) {
  const raw = normalizeText(value).toLowerCase();
  if (!raw) {
    return "";
  }
  if (raw === "album" || raw === "альбом") {
    return "album";
  }
  if (raw === "single" || raw === "singl" || raw === "сингл" || raw === "сингал" || raw === "сингел") {
    return "single";
  }
  return "";
}

function updateTracklistVisibility() {
  const typeSelect = document.getElementById("releaseType");
  const wrap = document.getElementById("tracklistFieldWrap");
  const field = document.getElementById("tracklistField");
  const isAlbum = normalizeReleaseTypeValue(typeSelect.value) === "album";
  wrap.classList.toggle("visible", isAlbum);
  field.required = isAlbum;
  if (!isAlbum) {
    field.value = "";
  }
}

function buildSubmitPayload(form) {
  const formData = new FormData(form);
  const normalizedType = normalizeReleaseTypeValue(formData.get("type"));
  const telegramId = getCurrentUserId();
  const releaseType = normalizedType === "album" ? "album" : "single";

  const values = {
    type: normalizedType,
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
  if (!telegramId) {
    errors.push("Не удалось определить telegram_id. Откройте Mini App через бота.");
  }
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
    const minDays = values.type === "album" ? 7 : 3;
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
  if (values.type === "album" && values.tracklist === ".") {
    errors.push("Для альбома обязательно заполните Tracklist.");
  }

  if (values.type !== "album") {
    values.tracklist = ".";
  }

  const formPayload = {
    ...values,
    artist_name: values.nick,
    track_name: values.name,
    release_type: releaseType,
    telegram_id: telegramId,
    telegram_contact: values.tg
  };

  const payload = {
    action: "webapp_release_submit",
    source: "mini_app",
    version: 3,
    request_id: `rel_${Date.now()}`,
    submitted_at: new Date().toISOString(),
    telegram_id: telegramId,
    init_data: getTelegramInitData(),
    user: getTelegramUser() || null,
    form: formPayload
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

async function submitReleaseForm(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const result = buildSubmitPayload(form);
  const tgApp = logWebAppSendDiagnostics("submit_form");

  if (result.errors.length) {
    tgApp?.HapticFeedback?.notificationOccurred?.("error");
    showToast(result.errors[0]);
    return;
  }

  const afterSuccess = () => {
    tgApp?.HapticFeedback?.notificationOccurred?.("success");
    showToast("Анкета отправлена. Проверьте «Кабинет» и чат с ботом.");
    form.reset();
    updateTracklistVisibility();
    syncMainButton();
    switchTab("cabinet");
  };

  if (!tgApp?.sendData) {
    console.error("Telegram.WebApp.sendData unavailable");
    showToast("Ошибка: Mini App открыт не через Telegram WebApp.");
    return;
  }

  try {
    tgApp.sendData(result.payloadJson || JSON.stringify(result.payload));
  } catch (e) {
    console.error("Telegram sendData failed:", e);
    tgApp?.HapticFeedback?.notificationOccurred?.("error");
    showToast(`Ошибка отправки: ${normalizeText(e?.message || e) || "попробуйте позже"}`);
    return;
  }

  afterSuccess();
}

function sendDiagnosticTestPayload() {
  const tgApp = logWebAppSendDiagnostics("diag_button");
  if (!tgApp?.sendData) {
    console.error("Telegram.WebApp.sendData unavailable");
    showToast("Ошибка: Telegram.WebApp undefined.");
    return;
  }
  try {
    tgApp.sendData("test анкета");
    tgApp.HapticFeedback?.notificationOccurred?.("success");
    showToast("Тест sendData отправлен.");
  } catch (e) {
    console.error("[WEBAPP_DIAG] test sendData failed", e);
    tgApp.HapticFeedback?.notificationOccurred?.("error");
    showToast("Ошибка тестовой отправки sendData.");
  }
}

function syncMainButton() {
  const tgApp = getTelegramWebApp();
  if (!tgApp?.MainButton) {
    return;
  }
  const canShow = appState.activeTab === "submit";
  tgApp.MainButton.setParams({ color: "#8154ff", text_color: "#ffffff", is_visible: canShow });
  tgApp.MainButton.setText("Отправить анкету");
  tgApp.MainButton.offClick(handleMainButtonClick);
  tgApp.MainButton.onClick(handleMainButtonClick);
  if (canShow && !document.getElementById("submitForm").checkValidity()) {
    tgApp.MainButton.setText("Заполните анкету");
  }
  if (canShow) {
    tgApp.MainButton.show();
  } else {
    tgApp.MainButton.hide();
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
  const diagBtn = document.getElementById("diagSendDataBtn");
  if (diagBtn) {
    diagBtn.addEventListener("click", sendDiagnosticTestPayload);
  }

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
  // If hosting starts this file with Node.js, jump directly to the Node bot runtime.
  if (typeof process !== "undefined" && process?.versions?.node) {
    try {
      // eslint-disable-next-line global-require
      const path = require("node:path");
      // eslint-disable-next-line global-require
      const fs = require("node:fs");
      // eslint-disable-next-line global-require
      const cp = require("node:child_process");

      const projectRoot = path.resolve(__dirname, "..");
      const nodeFallbackBot = path.join(projectRoot, "node_bot.js");
      if (!fs.existsSync(nodeFallbackBot)) {
        // eslint-disable-next-line no-console
        console.error(`Node fallback bot not found: ${nodeFallbackBot}`);
        process.exit(1);
      }
      // eslint-disable-next-line no-console
      console.info("Mini App script started in Node.js, launching node_bot.js...");
      const child = cp.spawn(process.execPath, [nodeFallbackBot], {
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
      child.on("error", (error) => {
        // eslint-disable-next-line no-console
        console.error("Failed to start Node fallback bot:", error);
        process.exit(1);
      });
      child.on("exit", (code) => process.exit(typeof code === "number" ? code : 1));
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error("Failed to start Node bot from webapp launcher:", err);
      process.exit(1);
    }
  }
}


