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
const CABINET_USERS_URL = "data/cabinet-users.json";
const CABINET_RELEASES_URL = "data/releases-public.json";
const BOT_API_CONFIG_URL = "data/supabase-config.json";
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
let runtimeBotApiBaseUrl = "";

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
    const username = user.username ? `@${user.username}` : user.first_name || "РџСЂРѕС„РёР»СЊ";
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

function logWebAppSendDiagnostics(reason = "submit") {
  if (!HAS_DOM || !window.Telegram?.WebApp) {
    console.error("Telegram.WebApp is undefined");
    return null;
  }
  const tgApp = window.Telegram.WebApp;
  try {
    console.log("SEND DATA TRIGGERED");
    console.log(Telegram.WebApp.initData);
    console.log(Telegram.WebApp.initDataUnsafe);
    console.log(`[WEBAPP_DIAG] reason=${reason} href=${window.location.href}`);
  } catch (e) {
    console.error("[WEBAPP_DIAG] log error", e);
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
    on_upload: { text: "РќР° РѕС‚РіСЂСѓР·РєРµ", emoji: "рџ•“" },
    moderation: { text: "РќР° РјРѕРґРµСЂР°С†РёРё", emoji: "рџ§ " },
    approved: { text: "РћРґРѕР±СЂРµРЅРѕ", emoji: "вњ…" },
    rejected: { text: "РћС‚РєР»РѕРЅРµРЅРѕ", emoji: "вќЊ" },
    needs_fix: { text: "РќР° РёСЃРїСЂР°РІР»РµРЅРёРё", emoji: "вњЏпёЏ" },
    deleted: { text: "РЈРґР°Р»С‘РЅ", emoji: "рџ—‘" }
  };
  return map[normalized] || { text: normalized, emoji: "вЏі" };
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

function normalizeUrlBase(value) {
  const raw = normalizeText(value);
  if (!raw) {
    return "";
  }
  return raw.replace(/\/+$/, "");
}

function readRuntimeBotApiBaseUrl() {
  if (runtimeBotApiBaseUrl) {
    return runtimeBotApiBaseUrl;
  }
  try {
    const direct = normalizeUrlBase(window.CXRNER_BOT_API_BASE || window.__CXRNER_BOT_API_BASE__);
    if (direct) {
      runtimeBotApiBaseUrl = direct;
      return runtimeBotApiBaseUrl;
    }
    const params = new URLSearchParams(window.location.search || "");
    const qp = normalizeUrlBase(params.get("bot_api_base") || params.get("botApiBaseUrl"));
    if (qp) {
      runtimeBotApiBaseUrl = qp;
      return runtimeBotApiBaseUrl;
    }
  } catch {
    // ignore
  }
  return "";
}

function joinUrl(base, part) {
  return `${base.replace(/\/+$/, "")}/${String(part || "").replace(/^\/+/, "")}`;
}

async function initRuntimeConfig() {
  readRuntimeBotApiBaseUrl();
  if (runtimeBotApiBaseUrl) {
    console.info(`[WEBAPP_API] base=${runtimeBotApiBaseUrl} (from runtime)`);
    return;
  }
  const cfg = await loadJsonSafe(BOT_API_CONFIG_URL);
  const fromCfg = normalizeUrlBase(
    cfg?.botApiBaseUrl ||
    cfg?.bot_api_base_url ||
    cfg?.apiBaseUrl ||
    cfg?.api_base_url ||
    ""
  );
  if (fromCfg) {
    runtimeBotApiBaseUrl = fromCfg;
    console.info(`[WEBAPP_API] base=${runtimeBotApiBaseUrl} (from ${BOT_API_CONFIG_URL})`);
  } else {
    console.info("[WEBAPP_API] base is not configured (sendData only)");
  }
}

async function postToBotApi(endpoint, body) {
  const base = readRuntimeBotApiBaseUrl();
  if (!base) {
    return { ok: false, skipped: true, error: "BOT_API_BASE_URL is empty" };
  }
  const url = joinUrl(base, endpoint);
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body)
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok || !json?.ok) {
      const msg = normalizeText(json?.error) || `HTTP ${res.status}`;
      return { ok: false, error: msg, status: res.status, response: json };
    }
    return { ok: true, data: json };
  } catch (error) {
    return { ok: false, error: normalizeText(error?.message || error || "fetch failed") };
  }
}

async function submitViaBotApi(payload, reason = "submit_form") {
  const tgApp = getTelegramWebApp();
  const body = {
    reason,
    source: "mini_app",
    submitted_at: new Date().toISOString(),
    payload,
    initData: tgApp?.initData || "",
    initDataUnsafe: tgApp?.initDataUnsafe || null,
    user: getTelegramUser() || null
  };
  const out = await postToBotApi("/api/webapp/submit", body);
  if (!out.ok) {
    console.warn("[WEBAPP_API] submit failed:", out.error || "unknown");
  } else {
    console.info("[WEBAPP_API] submit ok:", out.data);
  }
  return out;
}

async function runBotApiDiag(text = "test Р°РЅРєРµС‚Р°") {
  const tgApp = getTelegramWebApp();
  const body = {
    text,
    source: "mini_app",
    sent_at: new Date().toISOString(),
    initData: tgApp?.initData || "",
    initDataUnsafe: tgApp?.initDataUnsafe || null,
    user: getTelegramUser() || null
  };
  const out = await postToBotApi("/api/webapp/test", body);
  if (!out.ok) {
    console.warn("[WEBAPP_API] diag failed:", out.error || "unknown");
  } else {
    console.info("[WEBAPP_API] diag ok:", out.data);
  }
  return out;
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
      <span class="cabinet-metric-label">Р’СЃРµРіРѕ СЂРµР»РёР·РѕРІ</span>
    </div>
    <div class="cabinet-metric">
      <span class="cabinet-metric-value">${counts.pending}</span>
      <span class="cabinet-metric-label">РќР° РѕС‚РіСЂСѓР·РєРµ</span>
    </div>
    <div class="cabinet-metric">
      <span class="cabinet-metric-value">${counts.moderation}</span>
      <span class="cabinet-metric-label">РќР° РјРѕРґРµСЂР°С†РёРё</span>
    </div>
    <div class="cabinet-metric">
      <span class="cabinet-metric-value">${counts.approved}</span>
      <span class="cabinet-metric-label">РћРґРѕР±СЂРµРЅРѕ</span>
    </div>
  `;
  el.classList.remove("hidden");
}

function renderCabinetList(releases) {
  const list = document.getElementById("cabinetList");
  if (!releases.length) {
    list.innerHTML = `
      <article class="cabinet-item">
        <p class="cabinet-item-title">РџРѕРєР° РЅРµС‚ СЂРµР»РёР·РѕРІ</p>
        <p class="cabinet-item-meta">РћС‚РїСЂР°РІСЊС‚Рµ РїРµСЂРІСѓСЋ Р°РЅРєРµС‚Сѓ РІРѕ РІРєР»Р°РґРєРµ В«РђРЅРєРµС‚Р°В».</p>
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
    const typeText = rel.type || "СЂРµР»РёР·";
    const dateText = rel.date || "вЂ”";
    const reason = rel.reject_reason
      ? `<p class="cabinet-item-meta">РџСЂРёС‡РёРЅР°: ${escapeHtml(rel.reject_reason)}</p>`
      : "";
    const upc = rel.upc
      ? `<p class="cabinet-item-meta">UPC: ${escapeHtml(rel.upc)}</p>`
      : "";

    return `
      <article class="cabinet-item">
        <div class="cabinet-item-head">
          <p class="cabinet-item-title">${escapeHtml(rel.name || "Р‘РµР· РЅР°Р·РІР°РЅРёСЏ")}</p>
          <span class="status-chip status-${escapeHtml(rel.status || "on_upload")}">
            ${meta.emoji} ${escapeHtml(meta.text)}
          </span>
        </div>
        <p class="cabinet-item-meta">${escapeHtml(typeText)} вЂў ${escapeHtml(dateText)} вЂў ${escapeHtml(rel.genre || "вЂ”")}</p>
        <p class="cabinet-item-meta">РђСЂС‚РёСЃС‚: ${escapeHtml(rel.nick || "вЂ”")}</p>
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
    statusText.textContent = "Mini App РѕС‚РєСЂС‹С‚ Р±РµР· Р°РІС‚РѕСЂРёР·Р°С†РёРё Telegram. Р—Р°РїСѓСЃРєР°Р№С‚Рµ РµРіРѕ С‚РѕР»СЊРєРѕ С‡РµСЂРµР· РєРЅРѕРїРєСѓ В«РћС‚РєСЂС‹С‚СЊ РїСЂРёР»РѕР¶РµРЅРёРµВ» РІ С‡Р°С‚Рµ СЃ Р±РѕС‚РѕРј.";
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
    statusText.textContent = "РљР°Р±РёРЅРµС‚ РЅРµ Р°РєС‚РёРІРёСЂРѕРІР°РЅ. РќР°Р¶РјРёС‚Рµ В«РџРѕРґС‚РІРµСЂРґРёС‚СЊ РІС…РѕРґВ».";
    document.getElementById("cabinetSummary").classList.add("hidden");
    document.getElementById("cabinetList").innerHTML = "";
    return;
  }

  bindCard.classList.add("hidden");
  statusCard.classList.remove("hidden");
  statusText.textContent = "РљР°Р±РёРЅРµС‚ Р°РєС‚РёРІРµРЅ. РЎС‚Р°С‚СѓСЃС‹ СЃРёРЅС…СЂРѕРЅРёР·РёСЂСѓСЋС‚СЃСЏ СЃ Р±РѕС‚РѕРј Рё РјРѕРґРµСЂР°С†РёРµР№.";

  const userReleases = releasesJson?.users?.[userId] || [];
  const visible = userReleases.filter((rel) => !rel.user_deleted);
  appState.cabinet.releases = visible;
  renderCabinetSummary(visible);
  renderCabinetList(visible);
}

function activateCabinet() {
  const userId = getCurrentUserId();
  if (!userId) {
    showToast("РћС‚РєСЂРѕР№С‚Рµ Mini App РёР· Telegram.");
    return;
  }

  const payload = {
    action: "cabinet_activate",
    source: "mini_app",
    submitted_at: new Date().toISOString(),
    user: getTelegramUser() || null
  };

  const tgApp = getTelegramWebApp();
  if (tgApp?.sendData) {
    tgApp.sendData(JSON.stringify(payload));
    setCabinetActiveLocal(userId, true);
    showToast("Р—Р°РїСЂРѕСЃ РЅР° Р°РєС‚РёРІР°С†РёСЋ РѕС‚РїСЂР°РІР»РµРЅ. РћР±РЅРѕРІР»СЏРµРј РєР°Р±РёРЅРµС‚...");
    refreshCabinet();
    return;
  }

  showToast("РџСЂРёРІСЏР·РєР° РєР°Р±РёРЅРµС‚Р° РґРѕСЃС‚СѓРїРЅР° С‚РѕР»СЊРєРѕ РІРЅСѓС‚СЂРё Telegram-Р±РѕС‚Р°.");
}

function buildArtistsCatalog() {
  // РџРѕСЂСЏРґРѕРє РІР°Р¶РµРЅ: РѕС‚ Р±РѕР»СЊС€РµРіРѕ С‡РёСЃР»Р° СЃР»СѓС€Р°С‚РµР»РµР№ Рє РјРµРЅСЊС€РµРјСѓ.
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
    { value: "all", label: "Р’СЃРµ Р°СЂС‚РёСЃС‚С‹" },
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
      <button class="btn btn-ghost" data-open-release="${rel.id}" type="button">РћС‚РєСЂС‹С‚СЊ СЂРµР»РёР·</button>
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
            <p class="artist-meta">РЎР»СѓС€Р°С‚РµР»РµР№ РІ РјРµСЃСЏС†: ${formatNumber(artist.monthlyListeners)}</p>
          </div>
        </div>
        <button class="btn btn-ghost" data-open-artist="${escapeHtml(artist.name)}" data-artist-link="${escapeHtml(artist.profile || "")}" type="button">РћС‚РєСЂС‹С‚СЊ РїСЂРѕС„РёР»СЊ</button>
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

function normalizeReleaseTypeValue(value) {
  const raw = normalizeText(value).toLowerCase();
  if (!raw) {
    return "";
  }
  if (raw === "album" || raw === "Р°Р»СЊР±РѕРј" || raw === "СЂВ°СЂВ»СЃСљСЂВ±СЂС•СЂС") {
    return "album";
  }
  if (raw === "single" || raw === "singl" || raw === "СЃРёРЅРіР»" || raw === "СЃРёРЅРіР°Р»" || raw === "СЃРёРЅРіРµР»" || raw === "СЃСљСЃС“СЂР…СЂС–СЂВ»") {
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
  const values = {
    type: normalizedType,
    name: limitText(formData.get("name"), 160),
    subname: limitText(formData.get("subname"), 90) || ".",
    has_lyrics: limitText(formData.get("has_lyrics"), 60),
    nick: limitText(formData.get("nick"), 90),
    fio: limitText(formData.get("fio"), 130),
    date: limitText(formData.get("date"), 20),
    version: limitText(formData.get("version"), 90) || "РћСЂРёРіРёРЅР°Р»",
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
    errors.push("Р’С‹Р±РµСЂРёС‚Рµ С‚РёРї СЂРµР»РёР·Р°.");
  }
  if (!values.name) {
    errors.push("Р’РІРµРґРёС‚Рµ РЅР°Р·РІР°РЅРёРµ СЂРµР»РёР·Р°.");
  }
  if (!values.has_lyrics) {
    errors.push("РЈРєР°Р¶РёС‚Рµ, РµСЃС‚СЊ Р»Рё СЃР»РѕРІР° РІ СЂРµР»РёР·Рµ.");
  }
  if (!values.nick) {
    errors.push("Р’РІРµРґРёС‚Рµ РЅРёРє РёСЃРїРѕР»РЅРёС‚РµР»СЏ.");
  }
  if (!values.fio) {
    errors.push("Р’РІРµРґРёС‚Рµ Р¤РРћ РёСЃРїРѕР»РЅРёС‚РµР»СЏ.");
  }
  if (!values.date) {
    errors.push("РЈРєР°Р¶РёС‚Рµ РґР°С‚Сѓ СЂРµР»РёР·Р°.");
  }
  if (!values.genre) {
    errors.push("Р’РІРµРґРёС‚Рµ Р¶Р°РЅСЂ.");
  }
  if (!values.link) {
    errors.push("Р”РѕР±Р°РІСЊС‚Рµ СЃСЃС‹Р»РєСѓ РЅР° С„Р°Р№Р»С‹.");
  }
  if (!values.tg) {
    errors.push("РЈРєР°Р¶РёС‚Рµ Telegram РґР»СЏ СЃРІСЏР·Рё.");
  }
  if (!values.mat) {
    errors.push("Р’С‹Р±РµСЂРёС‚Рµ, РµСЃС‚СЊ Р»Рё РЅРµРЅРѕСЂРјР°С‚РёРІРЅР°СЏ Р»РµРєСЃРёРєР°.");
  }

  const parsedDate = parseRuDate(values.date);
  if (!parsedDate) {
    errors.push("Р”Р°С‚Р° РґРѕР»Р¶РЅР° Р±С‹С‚СЊ РІ С„РѕСЂРјР°С‚Рµ Р”Р”.РњРњ.Р“Р“Р“Р“.");
  } else {
    const minDays = values.type === "album" ? 7 : 3;
    const minDate = new Date();
    minDate.setHours(0, 0, 0, 0);
    minDate.setDate(minDate.getDate() + minDays);
    if (parsedDate < minDate) {
      errors.push(`Р”Р°С‚Р° СЂРµР»РёР·Р° РґРѕР»Р¶РЅР° Р±С‹С‚СЊ РјРёРЅРёРјСѓРј С‡РµСЂРµР· ${minDays} РґРЅРµР№.`);
    }
  }

  if (values.link && !isHttpUrl(values.link)) {
    errors.push("РЎСЃС‹Р»РєР° РЅР° С„Р°Р№Р»С‹ РґРѕР»Р¶РЅР° РЅР°С‡РёРЅР°С‚СЊСЃСЏ СЃ http:// РёР»Рё https://.");
  }
  if (values.yandex !== "." && !isHttpUrl(values.yandex)) {
    errors.push("РџРѕР»Рµ РЇРЅРґРµРєСЃ РњСѓР·С‹РєР°: СѓРєР°Р¶РёС‚Рµ URL РёР»Рё С‚РѕС‡РєСѓ.");
  }
  if (values.type === "album" && values.tracklist === ".") {
    errors.push("Р”Р»СЏ Р°Р»СЊР±РѕРјР° РѕР±СЏР·Р°С‚РµР»СЊРЅРѕ Р·Р°РїРѕР»РЅРёС‚Рµ Tracklist.");
  }

  if (values.type !== "album") {
    values.tracklist = ".";
  }

  const userId = getCurrentUserId();
  const payload = {
    action: "webapp_release_submit",
    source: "mini_app",
    version: 2,
    submitted_at: new Date().toISOString(),
    telegram_id: userId,
    user: getTelegramUser() || null,
    form: { ...values, telegram_id: userId }
  };

  const payloadJson = JSON.stringify(payload);
  const payloadBytes = getByteLength(payloadJson);
  if (payloadBytes > 3800) {
    errors.push("РђРЅРєРµС‚Р° СЃР»РёС€РєРѕРј Р±РѕР»СЊС€Р°СЏ. РЎРѕРєСЂР°С‚РёС‚Рµ РїСЂРѕРјРѕ, РєРѕРјРјРµРЅС‚Р°СЂРёР№ Рё tracklist.");
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

  const apiResult = await submitViaBotApi(result.payload, "submit_form");
  if (apiResult.ok) {
    tgApp?.HapticFeedback?.notificationOccurred?.("success");
    if (apiResult.data?.duplicate) {
      showToast("Дубликат анкеты: уже есть в системе.");
    } else {
      showToast("Анкета отправлена в модерацию.");
    }
    form.reset();
    updateTracklistVisibility();
    syncMainButton();
    switchTab("cabinet");
    return;
  }

  if (tgApp?.sendData) {
    try {
      tgApp.sendData(result.payloadJson || JSON.stringify(result.payload));
      tgApp.HapticFeedback?.notificationOccurred?.("success");
      showToast("Анкета передана в Telegram. Ожидайте ответ бота в чате.");
      form.reset();
      updateTracklistVisibility();
      syncMainButton();
      switchTab("cabinet");
      return;
    } catch (e) {
      console.error("[WEBAPP_DIAG] sendData failed", e);
    }
  } else {
    console.error("Telegram.WebApp.sendData unavailable");
  }

  tgApp?.HapticFeedback?.notificationOccurred?.("error");
  showToast(`Ошибка отправки: ${apiResult.error || "канал недоступен"}`);
}

async function sendDiagnosticTestPayload() {
  const tgApp = logWebAppSendDiagnostics("diag_button");
  const apiResult = await runBotApiDiag("test анкета");
  if (apiResult.ok) {
    tgApp?.HapticFeedback?.notificationOccurred?.("success");
    showToast("Тест API отправлен в модерацию.");
    return;
  }
  if (tgApp?.sendData) {
    try {
      tgApp.sendData("test анкета");
      tgApp.HapticFeedback?.notificationOccurred?.("success");
      showToast("Тест sendData отправлен.");
      return;
    } catch (e) {
      console.error("[WEBAPP_DIAG] test sendData failed", e);
    }
  } else {
    console.error("Telegram.WebApp.sendData unavailable");
  }
  tgApp?.HapticFeedback?.notificationOccurred?.("error");
  showToast(`Ошибка теста: ${apiResult.error || "канал недоступен"}`);
}

function syncMainButton() {
  const tgApp = getTelegramWebApp();
  if (!tgApp?.MainButton) {
    return;
  }
  const canShow = appState.activeTab === "submit";
  tgApp.MainButton.setParams({ color: "#8154ff", text_color: "#ffffff", is_visible: canShow });
  tgApp.MainButton.setText("РћС‚РїСЂР°РІРёС‚СЊ Р°РЅРєРµС‚Сѓ");
  tgApp.MainButton.offClick(handleMainButtonClick);
  tgApp.MainButton.onClick(handleMainButtonClick);
  if (canShow && !document.getElementById("submitForm").checkValidity()) {
    tgApp.MainButton.setText("Р—Р°РїРѕР»РЅРёС‚Рµ Р°РЅРєРµС‚Сѓ");
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
        showToast(`РџСЂРѕС„РёР»СЊ ${artistBtn.dataset.openArtist} РїРѕРєР° Р±РµР· СЃСЃС‹Р»РєРё.`);
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

async function bootstrap() {
  initTelegramWebApp();
  await initRuntimeConfig();
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
  window.addEventListener("load", () => {
    bootstrap().catch((err) => {
      console.error("Mini App bootstrap failed:", err);
    });
  });
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

