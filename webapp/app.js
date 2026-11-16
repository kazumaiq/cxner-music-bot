(() => {
  'use strict';

  const CLIENT_BUILD_VERSION = '2026-11-16-ui-v14';
  const $ = (s, root = document) => root.querySelector(s);
  const $$ = (s, root = document) => Array.from(root.querySelectorAll(s));

  let tg = window.Telegram?.WebApp || null;

  const DSP_META = {
    yandex: { label: 'Яндекс Музыка', color: '#FFCC00' },
    vk: { label: 'VK Музыка', color: '#0077FF' },
    spotify: { label: 'Spotify', color: '#1DB954' },
    apple: { label: 'Apple Music', color: '#FA243C' },
    youtube: { label: 'YouTube Music', color: '#FF0000' },
    deezer: { label: 'Deezer', color: '#A238FF' },
    zvuk: { label: 'Звук', color: '#6E4AFF' },
    soundcloud: { label: 'SoundCloud', color: '#FF5500' }
  };

  const ICON_PATHS = {
    home: '<path d="M3 10.5L12 3l9 7.5"/><path d="M5 9.5V20a1 1 0 0 0 1 1h4v-6h4v6h4a1 1 0 0 0 1-1V9.5"/>',
    music: '<path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/>',
    upload: '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>',
    plus: '<line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>',
    user: '<path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>',
    menu: '<line x1="4" y1="7" x2="20" y2="7"/><line x1="4" y1="12" x2="20" y2="12"/><line x1="4" y1="17" x2="20" y2="17"/>',
    grid: '<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/>',
    bell: '<path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/>',
    search: '<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>',
    link: '<path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>',
    shield: '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><path d="m9 12 2 2 4-4"/>',
    trophy: '<path d="M6 9H4.5a2.5 2.5 0 0 1 0-5H6"/><path d="M18 9h1.5a2.5 2.5 0 0 0 0-5H18"/><path d="M4 22h16"/><path d="M10 14.66V17c0 .55-.47.98-.97 1.21C7.85 18.75 7 20.24 7 22"/><path d="M14 14.66V17c0 .55.47.98.97 1.21C16.15 18.75 17 20.24 17 22"/><path d="M18 2H6v7a6 6 0 0 0 12 0V2Z"/>',
    calendar: '<rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/>',
    activity: '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>',
    cube: '<path d="m21 16-9 5-9-5V8l9-5 9 5v8z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/>',
    settings: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9c.26.6.85 1 1.51 1H21a2 2 0 0 1 0 4h-.09c-.66 0-1.25.4-1.51 1z"/>',
    users: '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>',
    heart: '<path d="M19 14c1.49-1.46 3-3.21 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.76 0-3 .5-4.5 2-1.5-1.5-2.74-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.3 1.5 4.05 3 5.5l7 7Z"/>',
    star: '<polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>',
    check: '<polyline points="20 6 9 17 4 12"/>',
    zap: '<polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>'
  };

  const state = {
    screen: 'home',
    history: ['home'],
    isAdmin: false,
    authFailed: false,
    homeData: null,
    catalogReleases: [],
    musicTab: 'catalog',
    catalogGenre: 'all',
    catalogSearch: '',
    catalogSort: 'newest',
    catalogFavOnly: false,
    wizardStep: 1,
    profileTab: 'overview',
    profileData: null,
    referralsData: null,
    analyticsData: null,
    dropTab: 'packs',
    dropState: null,
    dropInvFilter: 'all',
    fastOpen: false,
    arenaCaseId: 'case_001_archive',
    arenaResult: null,
    fuseSelectedIds: [],
    adminTab: 'overview',
    adminData: null,
    adminModStatus: 'all',
    adminModSource: 'all',
    adminModSearch: '',
    adminSearchQuery: '',
    adminSearchResults: null,
    myLinks: [],
    editingLink: null,
    rankSort: 'likes',
    currentReleaseId: null,
    likedIds: new Set(safeParse(localStorage.getItem('cxrner_likes'), [])),
    favoriteIds: new Set(safeParse(localStorage.getItem('cxrner_favs'), [])),
    recentViewedIds: safeParse(localStorage.getItem('cxrner_recent'), [])
  };

  let toastTimer = null;
  let canvasAnimId = null;
  let searchRafId = null;

  function safeParse(raw, fallback) {
    try { return raw ? JSON.parse(raw) : fallback; } catch { return fallback; }
  }

  function esc(v) {
    return String(v ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function pluralRu(n, one, few, many) {
    const abs = Math.abs(Math.floor(Number(n) || 0));
    const mod100 = abs % 100;
    const mod10 = abs % 10;
    if (mod100 >= 11 && mod100 <= 19) return many;
    if (mod10 === 1) return one;
    if (mod10 >= 2 && mod10 <= 4) return few;
    return many;
  }

  function formatNumber(n) {
    return Number(n || 0).toLocaleString('ru-RU');
  }

  function relTime(iso) {
    const ts = Date.parse(String(iso || ''));
    if (!Number.isFinite(ts)) return 'недавно';
    const diffSec = Math.max(0, Math.floor((Date.now() - ts) / 1000));
    if (diffSec < 60) return 'только что';
    const m = Math.floor(diffSec / 60);
    if (m < 60) return `${m}м назад`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}ч назад`;
    const d = Math.floor(h / 24);
    return `${d}д назад`;
  }

  function applyTelegramSafeArea() {
    const sysTop = Number(tg?.safeAreaInset?.top || 0);
    const contentTop = Number(tg?.contentSafeAreaInset?.top || 0);
    const sysBottom = Number(tg?.safeAreaInset?.bottom || 0);
    const totalTop = Math.max(6, sysTop + contentTop);
    document.documentElement.style.setProperty('--app-top-pad', `${totalTop}px`);
    document.documentElement.style.setProperty('--app-bottom-pad', `${Math.max(10, sysBottom)}px`);
  }

  function haptic(type = 'light') {
    try {
      if (type === 'success' || type === 'error' || type === 'warning') {
        tg?.HapticFeedback?.notificationOccurred?.(type);
      } else {
        tg?.HapticFeedback?.impactOccurred?.(type);
      }
    } catch {}
  }

  function toast(msg) {
    const el = $('#toast');
    if (!el || !msg) return;
    el.textContent = String(msg);
    el.classList.remove('is-hidden');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => el.classList.add('is-hidden'), 3000);
  }

  function getInitData() {
    return tg?.initData || '';
  }

  async function api(path, opts = {}) {
    const headers = { ...(opts.headers || {}) };
    const initData = getInitData();
    if (initData) headers['x-telegram-init-data'] = initData;
    const req = { method: opts.method || 'GET', headers };
    if (opts.body !== undefined) {
      headers['content-type'] = 'application/json';
      req.body = JSON.stringify({ ...opts.body, init_data: initData });
    }
    const res = await fetch(path, req);
    const data = await res.json().catch(() => ({ ok: false, error: `HTTP ${res.status}` }));
    if (!res.ok || data.ok === false) {
      const err = new Error(data.error || `Ошибка сервера (${res.status})`);
      err.status = res.status;
      throw err;
    }
    return data;
  }

  function generateCoverSvg(title = 'CXRNER', artist = '') {
    const palette = [
      ['#6D28D9', '#22D3EE'],
      ['#BE185D', '#7C3AED'],
      ['#0F766E', '#3B82F6'],
      ['#D97706', '#DC2626'],
      ['#4338CA', '#EC4899']
    ];
    const seed = [...String(title + artist)].reduce((a, c) => a + c.charCodeAt(0), 0);
    const [c1, c2] = palette[seed % palette.length];
    const letter = esc((String(title || 'C').trim()[0] || 'C').toUpperCase());
    const sub = esc(String(artist || 'CXRNER ARCHIVE').slice(0, 18).toUpperCase());
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 300 300"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop stop-color="${c1}"/><stop offset="1" stop-color="${c2}"/></linearGradient><radialGradient id="s" cx="50%" cy="46%" r="55%"><stop stop-color="#0B0816" stop-opacity="0.15"/><stop offset="1" stop-color="#0B0816" stop-opacity="0.88"/></radialGradient></defs><rect width="300" height="300" fill="url(#g)"/><rect width="300" height="300" fill="url(#s)"/><circle cx="150" cy="142" r="88" fill="none" stroke="rgba(255,255,255,0.22)" stroke-width="1.5"/><circle cx="150" cy="142" r="56" fill="rgba(11,8,22,0.38)" stroke="rgba(255,255,255,0.2)" stroke-width="1.2"/><text x="150" y="160" text-anchor="middle" fill="#FFFFFF" font-family="Arial,sans-serif" font-weight="800" font-size="60">${letter}</text><text x="150" y="266" text-anchor="middle" fill="rgba(255,255,255,0.75)" font-family="monospace" font-size="13" letter-spacing="2">${sub}</text></svg>`;
    return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
  }

  function coverUrl(item) {
    const raw = item?.cover || item?.cover_url || item?.release_data?.cover_url || item?.form_payload?.cover_url || '';
    return /^https?:\/\//i.test(raw) || raw.startsWith('data:image/')
      ? raw
      : generateCoverSvg(item?.title || item?.track_name || item?.name, item?.artist || item?.artist_name || item?.nick);
  }

  window.__cxrCoverErr = function (img, title, artist) {
    if (!img || img.dataset.fallbackDone) return;
    img.dataset.fallbackDone = '1';
    const t = title || img.dataset.coverTitle || 'CXRNER';
    const a = artist || img.dataset.coverArtist || '';
    img.src = generateCoverSvg(t, a);
  };

  function renderIcons(root = document) {
    $$('[data-icon]', root).forEach((el) => {
      const p = ICON_PATHS[el.dataset.icon];
      if (p && !el.querySelector('svg')) el.innerHTML = `<svg viewBox="0 0 24 24">${p}</svg>`;
    });
  }

  function statusText(status) {
    const s = String(status || '').toLowerCase();
    if (s === 'approved' || s === 'published') return 'Одобрено';
    if (s === 'moderation' || s === 'on_moderation') return 'На модерации';
    if (s === 'pending' || s === 'on_upload') return 'На отгрузке';
    if (s === 'rejected') return 'Отклонено';
    if (s === 'needfix') return 'Правки';
    if (s === 'deleted') return 'Удалено';
    return s || 'В работе';
  }

  function statusCss(status) {
    const s = String(status || '').toLowerCase();
    if (s === 'approved' || s === 'published') return 'approved';
    if (s === 'rejected' || s === 'deleted') return 'rejected';
    if (s === 'needfix') return 'needfix';
    return 'moderation';
  }

  function rarityLabel(r) {
    return ({ common: 'COMMON', rare: 'RARE', epic: 'EPIC', legendary: 'LEGENDARY', mythic: 'MYTHIC', special_reward: 'VOUCHER' }[r] || String(r || 'COMMON').toUpperCase());
  }

  function openExternal(url) {
    if (!url) return;
    try {
      if (tg?.openLink && /^https?:\/\//i.test(url)) {
        if (url.startsWith('https://t.me/')) tg.openTelegramLink(url);
        else tg.openLink(url);
        return;
      }
    } catch {}
    window.open(url, '_blank', 'noopener');
  }

  function copyText(value, label = 'Скопировано!') {
    if (!value) return;
    navigator.clipboard?.writeText(String(value))
      .then(() => { haptic('success'); toast(label); })
      .catch(() => { haptic('light'); toast(String(value)); });
  }

  function syncWalletState(wallet) {
    if (!wallet || typeof wallet !== 'object') return;
    if (state.homeData) state.homeData.wallet = wallet;
    if (state.dropState) state.dropState.wallet = wallet;
  }

  function updateTopbar() {
    const u = state.homeData?.user || tg?.initDataUnsafe?.user || {};
    const wallet = state.dropState?.wallet || state.homeData?.wallet;
    if (wallet) $('#topBalance').textContent = formatNumber(wallet.balance || 0);

    const unread = (state.homeData?.notifications || []).filter((n) => !n.read_at).length;
    const badge = $('#notifBadge');
    badge.textContent = unread > 9 ? '9+' : String(unread);
    badge.classList.toggle('is-hidden', unread === 0);

    const dropDot = $('#dockDropBadge');
    const canScan = Boolean(state.dropState?.daily_scan?.can_claim ?? state.homeData?.can_daily_scan);
    if (dropDot) dropDot.classList.toggle('is-hidden', !canScan);

    const photo = state.profileData?.artist_profile?.avatar_url || u.photo_url || tg?.initDataUnsafe?.user?.photo_url || '';
    const img = $('#topAvatar');
    const fb = $('#topAvatarFallback');
    fb.textContent = String(u.first_name || u.username || 'C')[0].toUpperCase();
    if (photo && !photo.startsWith('data:text')) {
      img.src = photo;
      img.onload = () => { img.classList.add('is-loaded'); fb.classList.add('is-hidden'); };
      img.onerror = () => { img.classList.remove('is-loaded'); fb.classList.remove('is-hidden'); };
    } else {
      img.classList.remove('is-loaded');
      fb.classList.remove('is-hidden');
    }

    const adminMenuBtn = $('#moreAdminBtn');
    if (adminMenuBtn) adminMenuBtn.classList.toggle('is-hidden', !state.isAdmin);
  }

  function switchScreen(name, pushHistory = true) {
    if (!name || !$(`#${name}`)) return;
    state.screen = name;
    if (pushHistory && state.history[state.history.length - 1] !== name) {
      state.history.push(name);
      if (state.history.length > 12) state.history.shift();
    }
    $$('.screen').forEach((s) => s.classList.toggle('is-active', s.id === name));
    $$('#bottomNav .nav-item').forEach((b) => b.classList.toggle('is-active', b.dataset.screen === name));
    window.scrollTo({ top: 0, behavior: 'instant' });
    haptic('light');
    try {
      if (name === 'home') tg?.BackButton?.hide();
      else tg?.BackButton?.show();
    } catch {}

    if (name === 'profile') loadProfile();
    else if (name === 'drop') loadDropScreen();
    else if (name === 'link') loadLinkEditor();
    else if (name === 'verification') renderVerification();
    else if (name === 'notifications') renderNotifications();
    else if (name === 'ranking') loadRanking();
    else if (name === 'calendar') loadCalendar();
    else if (name === 'activity') loadActivity();
    else if (name === 'admin') loadAdmin();
    else if (name === 'catalog') setMusicTab(state.musicTab || 'catalog');
  }

  function setMusicTab(tab = 'catalog') {
    state.musicTab = tab;
    $$('#musicModeTabs .seg-btn').forEach((b) => b.classList.toggle('is-active', b.dataset.musicTab === tab));
    $('#musicCatalogPane')?.classList.toggle('is-hidden', tab !== 'catalog');
    $('#musicRankingPane')?.classList.toggle('is-hidden', tab !== 'ranking');
    $('#musicCalendarPane')?.classList.toggle('is-hidden', tab !== 'calendar');
    if (tab === 'catalog') renderCatalog();
    else if (tab === 'ranking') loadRanking();
    else if (tab === 'calendar') loadCalendar();
  }

  function releaseCardHtml(item, horizontal = false) {
    const title = item.title || item.track_name || item.name || 'Без названия';
    const artist = item.artist || item.artist_name || item.nick || 'CXRNER Artist';
    const genre = item.genre || 'Music';
    const cover = esc(coverUrl(item));
    const st = statusCss(item.status);
    return `<article class="release-card" data-release-id="${esc(item.id)}" style="${horizontal ? 'min-width:150px;max-width:156px' : ''}">
      <div class="cover-wrap">
        <img src="${cover}" alt="" loading="lazy" decoding="async" data-cover-title="${esc(title)}" data-cover-artist="${esc(artist)}" onerror="window.__cxrCoverErr(this)">
        <span class="status-badge status-${esc(st)}">${esc(statusText(item.status))}</span>
      </div>
      <div class="rc-body">
        <div class="rc-title">${esc(title)}</div>
        <div class="rc-artist">${esc(artist)}</div>
        <div class="rc-foot">
          <span class="genre-pill">${esc(genre)}</span>
          <span class="mono" style="font-size:11px;color:var(--muted)">♥ ${Number(item.likes || 0)}</span>
        </div>
      </div>
    </article>`;
  }

  /* ==================== HOME ==================== */
  function renderLiveStrip(targetEl) {
    if (!targetEl) return;
    const drops = state.dropState?.recent_drops || state.homeData?.recent_drops || [];
    const rarityColors = { common: '#94A3B8', rare: '#22D3EE', epic: '#A855F7', legendary: '#F59E0B', mythic: '#F43F5E', special_reward: '#10B981' };
    if (!drops.length) {
      targetEl.classList.add('is-hidden');
      return;
    }
    targetEl.classList.remove('is-hidden');
    targetEl.innerHTML = drops.map((d) => {
      const c = rarityColors[d.rarity] || '#A855F7';
      return `<div class="ticker-chip" data-screen="drop">
        <span class="tc-dot" style="background:${c}22;color:${c};border:1px solid ${c}66">◈</span>
        <span class="tc-meta">
          <strong>${esc(d.display_name || 'Коллекционер')} · ${esc(d.item_name)}</strong>
          <small>${esc(rarityLabel(d.rarity))} · ${relTime(d.created_at)}</small>
        </span>
      </div>`;
    }).join('');
  }

  function renderHome() {
    const data = state.homeData || {};
    renderLiveStrip($('#homeLiveStrip'));

    const adminBox = $('#homeAdminEntry');
    if (adminBox) {
      adminBox.innerHTML = state.isAdmin
        ? `<button class="admin-entry-banner" data-screen="admin" type="button">
            <span class="aeb-left">
              <span class="aeb-badge">👑</span>
              <span class="aeb-copy">
                <strong>Панель управления CEO / Админ</strong>
                <small>Бот live, модерация анкет сайта и Mini App, юзеры</small>
              </span>
            </span>
            <span class="aeb-pill">Открыть ›</span>
          </button>`
        : '';
    }

    const tgUser = tg?.initDataUnsafe?.user || {};
    const u = data.user || tgUser;
    const uname = String(u.username || tgUser.username || '').toLowerCase();
    const isCeo = uname === 'kazumaiq' || state.isAdmin;
    const isVerif = data.verification_status === 'verified' || isCeo;
    const stats = data.stats || {};
    const forms = data.forms || [];
    const lastForm = forms[0];

    if (state.authFailed && !getInitData()) {
      $('#homeUserSummary').innerHTML = `<div class="glass-card" style="border-color:rgba(245,158,11,0.45)">
        <strong>Предпросмотр без Telegram авторизации</strong>
        <p class="muted-sm">Каталог открыт. Кабинет артиста, DROP и админка загружаются автоматически при открытии внутри Telegram.</p>
      </div>`;
    } else if (forms.length === 0 && !(stats.releases > 0)) {
      $('#homeUserSummary').innerHTML = `<div class="onboard-card">
        <div class="hero-top-row">
          <div>
            <div class="eyebrow">ДЛЯ НОВЫХ АРТИСТОВ</div>
            <h2 style="margin-top:4px">Привет, ${esc(u.first_name || u.username || 'артист')}!</h2>
          </div>
          ${isCeo ? '<span class="ceo-badge">👑 CEO</span>' : ''}
        </div>
        <div class="onboard-steps">
          <div class="onboard-step">
            <div class="onboard-num">1</div>
            <div class="onboard-copy">
              <strong>Отправь трек</strong>
              <span>Нажми «Выпустить трек» выше или «+» внизу — 5 простых шагов.</span>
            </div>
          </div>
          <div class="onboard-step">
            <div class="onboard-num">2</div>
            <div class="onboard-copy">
              <strong>Отслеживай модерацию</strong>
              <span>Статус обновляется прямо в этом боте и в твоём кабинете на сайте.</span>
            </div>
          </div>
          <div class="onboard-step">
            <div class="onboard-num">3</div>
            <div class="onboard-copy">
              <strong>Сделай Smart Link</strong>
              <span>После одобрения получишь UPC и красивую ссылку на все площадки.</span>
            </div>
          </div>
        </div>
        <div class="wrap-gap">
          <button class="btn btn-outline btn-sm" style="flex:1" data-screen="link" type="button">🔗 Smart Link</button>
          <button class="btn btn-outline btn-sm" style="flex:1" data-action="open-friends" type="button">🤝 Друзья +75 CXP</button>
        </div>
      </div>`;
    } else {
      $('#homeUserSummary').innerHTML = `<div class="glass-card">
        <div class="hero-top-row">
          <div>
            <div class="eyebrow">ТВОЙ КАБИНЕТ</div>
            <h2 style="margin-top:4px">${esc(u.first_name || u.username || 'Артист')}</h2>
          </div>
          <div class="wrap-gap" style="justify-content:flex-end">
            ${isCeo ? '<span class="ceo-badge">👑 CEO</span>' : ''}
            ${isVerif ? '<span class="verified-pill">✓ VERIFIED</span>' : ''}
          </div>
        </div>
        <div class="muted-sm">${lastForm ? `Последний релиз: <b style="color:#fff">${esc(lastForm.track_name || 'Без названия')}</b> — ${esc(statusText(lastForm.status))}` : 'Все твои треки и смарт-ссылки под рукой.'}</div>
        <div class="kpi-grid-4">
          <div class="kpi-box"><strong>${stats.releases || 0}</strong><span>${pluralRu(stats.releases || 0, 'Релиз', 'Релиза', 'Релизов')}</span></div>
          <div class="kpi-box"><strong>${stats.moderation || 0}</strong><span>Модерация</span></div>
          <div class="kpi-box"><strong>${stats.links || 0}</strong><span>Ссылки</span></div>
          <div class="kpi-box"><strong>${data.inventory_count || 0}</strong><span>Артефакты</span></div>
        </div>
        <div class="wrap-gap">
          <button class="btn btn-outline btn-xs" style="flex:1" data-screen="link" type="button">🔗 Smart Link</button>
          <button class="btn btn-outline btn-xs" style="flex:1" data-action="open-friends" type="button">🤝 Друзья +75</button>
          <button class="btn btn-outline btn-xs" style="flex:1" data-screen="profile" type="button">Профиль</button>
        </div>
      </div>`;
    }

    const latest = data.latest?.length ? data.latest : state.catalogReleases.slice(0, 8);
    const popular = data.popular?.length ? data.popular : state.catalogReleases.slice(0, 8);
    $('#homeLatestRow').innerHTML = latest.length
      ? latest.map((r) => releaseCardHtml(r, true)).join('')
      : '<div class="empty-box" style="width:100%">Каталог скоро пополнится новыми релизами.</div>';
    $('#homePopularRow').innerHTML = popular.length
      ? popular.map((r) => releaseCardHtml(r, true)).join('')
      : '<div class="empty-box" style="width:100%">Пока нет данных чарта.</div>';

    const news = data.news || [];
    $('#homeNewsList').innerHTML = news.length
      ? news.map((n) => `<article class="glass-card">
          <div class="hero-top-row">
            <strong>${esc(n.title)}</strong>
            <span class="mono muted-sm">${new Date(n.published_at || n.created_at || Date.now()).toLocaleDateString('ru-RU')}</span>
          </div>
          <p class="muted-sm">${esc(n.body)}</p>
        </article>`).join('')
      : '<div class="empty-box">Свежих новостей пока нет.</div>';
  }

  /* ==================== CATALOG ==================== */
  function renderCatalog() {
    const items = state.catalogReleases || [];
    const genres = ['all', ...new Set(items.map((x) => x.genre).filter(Boolean)).values()].slice(0, 14);
    $('#catalogGenreChips').innerHTML = genres.map((g) => `<button class="btn-chip ${state.catalogGenre === g ? 'is-active' : ''}" data-cat-genre="${esc(g)}" type="button">${g === 'all' ? 'Все жанры' : esc(g)}</button>`).join('');

    const q = state.catalogSearch.trim().toLowerCase();
    let filtered = items.filter((item) => {
      if (state.catalogGenre !== 'all' && item.genre !== state.catalogGenre) return false;
      if (state.catalogFavOnly && !state.favoriteIds.has(String(item.id))) return false;
      if (!q) return true;
      return `${item.title || ''} ${item.artist || ''} ${item.genre || ''}`.toLowerCase().includes(q);
    });

    if (state.catalogSort === 'popular') {
      filtered = [...filtered].sort((a, b) => ((b.likes || 0) + (b.favorites || 0)) - ((a.likes || 0) + (a.favorites || 0)));
    } else if (state.catalogSort === 'title') {
      filtered = [...filtered].sort((a, b) => String(a.title || '').localeCompare(String(b.title || ''), 'ru'));
    }

    const total = filtered.length;
    $('#catalogCount').textContent = `${total} ${pluralRu(total, 'релиз', 'релиза', 'релизов')}`;
    $('#catalogFavOnly').classList.toggle('is-active', state.catalogFavOnly);
    $('#catalogGrid').innerHTML = total
      ? filtered.map((item) => releaseCardHtml(item, false)).join('')
      : '<div class="empty-box" style="grid-column:1/-1">Ничего не найдено по фильтрам.</div>';
  }

  /* ==================== MODALS & RELEASE DETAIL ==================== */
  function showModal(html) {
    $('#modalBody').innerHTML = html;
    $('#releaseModal').classList.remove('is-hidden');
    renderIcons($('#releaseModal'));
  }

  function closeModal() {
    $('#releaseModal').classList.add('is-hidden');
    if (canvasAnimId) { cancelAnimationFrame(canvasAnimId); canvasAnimId = null; }
  }

  async function openRelease(id) {
    const item = [...state.catalogReleases, ...(state.homeData?.latest || []), ...(state.profileData?.releases || [])]
      .find((r) => String(r.id) === String(id));
    if (!item) return;
    state.currentReleaseId = String(id);

    state.recentViewedIds = [String(id), ...state.recentViewedIds.filter((x) => x !== String(id))].slice(0, 12);
    localStorage.setItem('cxrner_recent', JSON.stringify(state.recentViewedIds));

    const links = item.dsp_links || item.form_payload?.dsp_links || {};
    const dspButtons = Object.entries(links)
      .filter(([, u]) => /^https?:\/\//i.test(String(u || '')))
      .map(([k, u]) => {
        const meta = DSP_META[k] || { label: k.toUpperCase(), color: '#22D3EE' };
        return `<a class="dsp-btn" href="${esc(u)}" target="_blank" rel="noopener"><span class="dsp-dot" style="background:${meta.color}"></span><span>${esc(meta.label)}</span></a>`;
      }).join('');

    const smartLink = item.smart_link || item.form_payload?.smart_link || '';
    const liked = state.likedIds.has(String(id));
    const fav = state.favoriteIds.has(String(id));
    const title = item.title || item.track_name || 'Без названия';
    const artist = item.artist || item.artist_name || 'Артист';

    showModal(`<div class="row-gap" style="align-items:flex-start">
        <img src="${esc(coverUrl(item))}" alt="" data-cover-title="${esc(title)}" data-cover-artist="${esc(artist)}" onerror="window.__cxrCoverErr(this)" style="width:94px;height:94px;border-radius:16px;object-fit:cover;flex-shrink:0">
        <div style="min-width:0;flex:1">
          <div class="eyebrow">${esc(item.genre || 'CXRNER MUSIC')}</div>
          <h2 style="margin-top:4px">${esc(title)}</h2>
          <p class="muted" style="margin-top:3px">${esc(artist)}</p>
          <span class="status-badge status-${esc(statusCss(item.status))}" style="position:static;display:inline-block;margin-top:8px">${esc(statusText(item.status))}</span>
        </div>
      </div>
      ${item.description ? `<p class="muted-sm">${esc(item.description)}</p>` : ''}
      <div class="wrap-gap">
        <button class="btn ${liked ? 'btn-primary' : 'btn-outline'} btn-sm" data-action="engage" data-kind="like" data-id="${esc(id)}" type="button">♥ ${Number(item.likes || 0)}</button>
        <button class="btn ${fav ? 'btn-cyan' : 'btn-outline'} btn-sm" data-action="engage" data-kind="favorite" data-id="${esc(id)}" type="button">★ В избранное</button>
        ${item.telegram_id ? `<button class="btn btn-outline btn-sm" data-action="open-artist" data-id="${esc(item.telegram_id)}" type="button">Артист</button>` : ''}
        ${smartLink ? `<button class="btn btn-success btn-sm" data-external="${esc(smartLink)}" type="button">Смарт-ссылка ↗</button>` : ''}
      </div>
      ${dspButtons ? `<div class="dsp-list">${dspButtons}</div>` : '<p class="muted-sm">Ссылки на стриминги появятся после индексации.</p>'}
      <div class="glass-card" style="padding:12px">
        <h3>Комментарии</h3>
        <form id="modalCommentForm" class="row-gap">
          <input name="body" placeholder="Напиши отзыв о релизе…" required>
          <button class="btn btn-primary btn-sm" type="submit">Ок</button>
        </form>
        <div id="modalCommentsList" class="stack-list"><p class="muted-sm">Загрузка…</p></div>
      </div>`);

    api('/api/miniapp/platform/listen', { method: 'POST', body: { release_id: String(id), seconds_played: 15 } }).catch(() => {});
    loadComments(id);
  }

  async function loadComments(releaseId) {
    try {
      const res = await api(`/api/miniapp/platform/comments?release_id=${encodeURIComponent(releaseId)}`);
      const box = $('#modalCommentsList');
      if (!box) return;
      const list = res.comments || [];
      box.innerHTML = list.length
        ? list.map((c) => `<div style="padding:8px 0;border-bottom:1px solid var(--border)">
            <div class="hero-top-row">
              <strong style="font-size:12px">${esc(c.author?.first_name || c.author?.username || `ID ${c.telegram_id}`)}</strong>
              <span class="muted-sm">${relTime(c.created_at)}</span>
            </div>
            <p style="font-size:13px;margin-top:3px">${esc(c.body)}</p>
          </div>`).join('')
        : '<p class="muted-sm">Будь первым, кто оставит отзыв.</p>';
    } catch {}
  }

  async function openArtistModal(telegramId) {
    showModal('<p class="muted">Загружаем карточку артиста…</p>');
    try {
      const res = await api(`/api/miniapp/platform/artist?telegram_id=${encodeURIComponent(telegramId)}`);
      const a = res.artist || {};
      const name = a.display_name || `Артист #${telegramId}`;
      const badges = (res.badges || []).map((b) => `<span class="btn-chip is-active">${esc(b.name)}</span>`).join('');
      const socials = Object.entries(a.socials || {}).filter(([, v]) => v).map(([k, v]) => `<a class="btn btn-outline btn-xs" href="${esc(v)}" target="_blank">${esc(k.toUpperCase())}</a>`).join('');
      showModal(`<div class="row-gap">
          <div class="profile-avatar-xl" style="width:64px;height:64px;border-radius:18px;font-size:22px">${a.avatar_url ? `<img src="${esc(a.avatar_url)}" onerror="this.style.display='none'">` : esc(name[0].toUpperCase())}</div>
          <div>
            <h2>${esc(name)}</h2>
            <p class="muted-sm">${res.followers || 0} ${pluralRu(res.followers || 0, 'подписчик', 'подписчика', 'подписчиков')}</p>
          </div>
        </div>
        ${a.bio ? `<p class="muted-sm">${esc(a.bio)}</p>` : ''}
        ${badges ? `<div class="wrap-gap">${badges}</div>` : ''}
        ${socials ? `<div class="wrap-gap">${socials}</div>` : ''}
        <div class="row-gap">
          <button class="btn btn-primary btn-sm" data-action="follow-artist" data-id="${esc(telegramId)}" type="button">Подписаться</button>
          <button class="btn btn-outline btn-sm" data-copy="${esc(res.share_url)}" type="button">Скопировать ссылку</button>
        </div>`);
    } catch (e) {
      showModal(`<p class="muted">${esc(e.message)}</p>`);
    }
  }

  /* ==================== RELEASE WIZARD ==================== */
  function setWizardStep(step) {
    state.wizardStep = Math.max(1, Math.min(5, step));
    $$('.wizard-pane').forEach((p) => p.classList.toggle('is-hidden', Number(p.dataset.step) !== state.wizardStep));
    $$('#wizardStepper .step-dot').forEach((d) => {
      const s = Number(d.dataset.step);
      d.classList.toggle('is-active', s === state.wizardStep);
      d.classList.toggle('is-done', s < state.wizardStep);
    });
    $('#wizPrevBtn').classList.toggle('is-hidden', state.wizardStep === 1);
    $('#wizNextBtn').classList.toggle('is-hidden', state.wizardStep === 5);
    $('#wizSubmitBtn').classList.toggle('is-hidden', state.wizardStep !== 5);
    if (state.wizardStep === 5) renderWizardReview();
  }

  function validateWizardStep(step) {
    if (step === 1) {
      if (!$('#wizType').value) return toast('Выбери формат релиза') || false;
      if (!$('#wizName').value.trim()) return toast('Введи название релиза') || false;
      if (!$('#wizDate').value.trim()) return toast('Укажи планируемую дату релиза') || false;
    } else if (step === 2) {
      if (!$('#wizNick').value.trim()) return toast('Введи никнейм артиста') || false;
    } else if (step === 3) {
      const link = $('#wizLink').value.trim();
      if (!/^https?:\/\//i.test(link)) return toast('Нужна корректная HTTP(S)-ссылка на аудио') || false;
    }
    return true;
  }

  function renderWizardReview() {
    const fd = new FormData($('#releaseWizardForm'));
    const pairs = [
      ['Формат', fd.get('type')],
      ['Название', `${fd.get('name') || ''} ${fd.get('version') ? `(${fd.get('version')})` : ''}`],
      ['Жанр', fd.get('genre') || 'Не указан'],
      ['Дата релиза', fd.get('date')],
      ['Артист', fd.get('nick')],
      ['Аудиоссылка', fd.get('link')]
    ];
    $('#wizReviewBox').innerHTML = pairs.map(([k, v]) => `<div><span class="muted-sm">${esc(k)}:</span> <strong>${esc(v || '—')}</strong></div>`).join('');
  }

  /* ==================== CXRNER DROP (60 FPS ROULETTE & 3D) ==================== */
  function packArtSvg(index) {
    const colors = [
      ['#22D3EE', '#1D4ED8'],
      ['#C084FC', '#5B21B6'],
      ['#FDE68A', '#EA580C']
    ][index % 3];
    return `<svg viewBox="0 0 96 96" width="76" height="76" fill="none">
      <defs>
        <linearGradient id="pkG${index}" x1="12" y1="8" x2="84" y2="88" gradientUnits="userSpaceOnUse">
          <stop stop-color="${colors[0]}"/><stop offset="1" stop-color="${colors[1]}"/>
        </linearGradient>
      </defs>
      <path d="M48 10L82 28V68L48 86L14 68V28L48 10Z" fill="rgba(11,8,22,0.45)" stroke="url(#pkG${index})" stroke-width="3"/>
      <path d="M14 28L48 46L82 28M48 46V86" stroke="url(#pkG${index})" stroke-width="2.5"/>
      <circle cx="48" cy="46" r="11" fill="url(#pkG${index})" opacity="0.9"/>
      <path d="M48 39L54 46L48 53L42 46L48 39Z" fill="#0B0816"/>
    </svg>`;
  }

  function specimenGlyphSvg(shape, color = '#22D3EE', size = 44) {
    const s = String(shape || 'box');
    let body = `<rect x="14" y="14" width="36" height="36" rx="8" stroke="${esc(color)}" stroke-width="2.8" fill="${esc(color)}22"/>`;
    if (s === 'torus') body = `<circle cx="32" cy="32" r="17" stroke="${esc(color)}" stroke-width="5" fill="none"/><circle cx="32" cy="32" r="7" fill="${esc(color)}66"/>`;
    else if (s === 'cylinder') body = `<ellipse cx="32" cy="32" rx="18" ry="18" stroke="${esc(color)}" stroke-width="3" fill="${esc(color)}22"/><circle cx="32" cy="32" r="5" fill="#fff"/>`;
    else if (s === 'octahedron' || s === 'dodecahedron' || s === 'icosahedron') {
      body = `<polygon points="32,8 54,28 32,56 10,28" stroke="${esc(color)}" stroke-width="2.8" fill="${esc(color)}28"/><polyline points="10,28 32,36 54,28" stroke="${esc(color)}" stroke-width="1.8" fill="none"/>`;
    }
    return `<svg viewBox="0 0 64 64" width="${size}" height="${size}">${body}</svg>`;
  }

  function startInteractive3dCanvas(canvasId, specimen) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    if (canvasAnimId) cancelAnimationFrame(canvasAnimId);

    const ctx = canvas.getContext('2d');
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const w = canvas.clientWidth || 320;
    const h = canvas.clientHeight || 196;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    const color = specimen?.model_color || '#22D3EE';
    const shape = String(specimen?.shape || 'octahedron');
    let verts = [];
    let edges = [];

    if (shape === 'octahedron' || shape === 'dodecahedron' || shape === 'icosahedron') {
      verts = [[0, 1.38, 0], [0, -1.38, 0], [1.18, 0, 0], [-1.18, 0, 0], [0, 0, 1.18], [0, 0, -1.18]];
      edges = [[0, 2], [0, 3], [0, 4], [0, 5], [1, 2], [1, 3], [1, 4], [1, 5], [2, 4], [4, 3], [3, 5], [5, 2]];
    } else if (shape === 'torus') {
      for (let i = 0; i < 16; i += 1) {
        const a = (i / 16) * Math.PI * 2;
        verts.push([Math.cos(a) * 1.18, Math.sin(a * 2) * 0.25, Math.sin(a) * 1.18]);
        edges.push([i, (i + 1) % 16]);
        verts.push([Math.cos(a) * 0.72, Math.sin(a * 2) * 0.16, Math.sin(a) * 0.72]);
      }
      for (let i = 0; i < 16; i += 1) {
        edges.push([i * 2, i * 2 + 1]);
        edges.push([i * 2 + 1, ((i + 1) % 16) * 2 + 1]);
      }
    } else {
      verts = [[-0.95, -0.95, -0.95], [0.95, -0.95, -0.95], [0.95, 0.95, -0.95], [-0.95, 0.95, -0.95], [-0.95, -0.95, 0.95], [0.95, -0.95, 0.95], [0.95, 0.95, 0.95], [-0.95, 0.95, 0.95]];
      edges = [[0, 1], [1, 2], [2, 3], [3, 0], [4, 5], [5, 6], [6, 7], [7, 4], [0, 4], [1, 5], [2, 6], [3, 7], [0, 6], [2, 4]];
    }

    let rx = 0.45, ry = 0.25, drag = false, lx = 0, ly = 0;
    const onDown = (e) => { drag = true; const p = e.touches?.[0] || e; lx = p.clientX; ly = p.clientY; };
    const onMove = (e) => { if (!drag) return; const p = e.touches?.[0] || e; ry += (p.clientX - lx) * 0.015; rx += (p.clientY - ly) * 0.015; lx = p.clientX; ly = p.clientY; };
    const onUp = () => { drag = false; };
    canvas.addEventListener('pointerdown', onDown, { passive: true });
    window.addEventListener('pointermove', onMove, { passive: true });
    window.addEventListener('pointerup', onUp, { passive: true });

    let frame = 0;
    const draw = () => {
      if (!document.getElementById(canvasId)) return;
      frame += 1;
      if (!drag) ry += 0.016;
      ctx.clearRect(0, 0, w, h);

      const cx = w / 2, cy = h / 2;
      const glow = ctx.createRadialGradient(cx, cy, 4, cx, cy, 92);
      glow.addColorStop(0, `${color}55`);
      glow.addColorStop(1, 'rgba(11,8,22,0)');
      ctx.fillStyle = glow;
      ctx.fillRect(0, 0, w, h);

      ctx.strokeStyle = 'rgba(255,255,255,0.08)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.ellipse(cx, cy, 74, 24, -0.15, 0, Math.PI * 2);
      ctx.stroke();

      const cosY = Math.cos(ry), sinY = Math.sin(ry), cosX = Math.cos(rx), sinX = Math.sin(rx);
      const scale = Math.min(w, h) * 0.27;
      const proj = verts.map(([x, y, z]) => {
        const x1 = x * cosY - z * sinY;
        const z1 = x * sinY + z * cosY;
        const y2 = y * cosX - z1 * sinX;
        const z2 = y * sinX + z1 * cosX;
        const p = 3.1 / (3.1 + z2);
        return [cx + x1 * scale * p, cy + y2 * scale * p, z2];
      });

      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.beginPath();
      for (const [a, b] of edges) {
        if (!proj[a] || !proj[b]) continue;
        ctx.moveTo(proj[a][0], proj[a][1]);
        ctx.lineTo(proj[b][0], proj[b][1]);
      }
      ctx.stroke();

      ctx.fillStyle = '#FFFFFF';
      for (const [px, py] of proj) {
        ctx.beginPath();
        ctx.arc(px, py, 2.3, 0, Math.PI * 2);
        ctx.fill();
      }
      canvasAnimId = requestAnimationFrame(draw);
    };
    draw();
  }

  async function loadDropScreen() {
    try {
      const ds = await api('/api/miniapp/drop/state');
      state.dropState = ds;
      updateTopbar();
      renderDropScreen();
    } catch (e) {
      $('#dropBody').innerHTML = `<div class="glass-card"><p class="muted">${esc(e.message)}</p></div>`;
    }
  }

  function renderDropScreen() {
    const ds = state.dropState;
    if (!ds) return;
    renderLiveStrip($('#dropLiveFeed'));

    const daily = ds.daily_scan || {};
    const inv = ds.inventory || [];
    const streak = Number(daily.streak || 0);

    $('#dropHeader').innerHTML = `
      <div class="subscreen-bar">
        <strong style="text-align:left;font-family:'Unbounded',sans-serif;font-size:18px">CXRNER DROP</strong>
        <button class="btn-chip ${state.fastOpen ? 'is-active' : ''}" data-action="toggle-fast-open" type="button">⚡ Быстро: ${state.fastOpen ? 'ВКЛ' : 'ВЫКЛ'}</button>
      </div>
      <button class="drop-banner-quest ${daily.can_claim ? '' : 'is-claimed'}" data-action="drop-daily" ${daily.can_claim ? '' : 'disabled'} type="button">
        <span class="dbq-ico">${daily.can_claim ? '🎁' : '✅'}</span>
        <span class="dbq-copy">
          <strong>${daily.can_claim ? `Забери ежедневный скан (+${35 + Math.min(35, streak * 5)} CXP)` : 'Ежедневный скан получен'}</strong>
          <small>${daily.can_claim ? `Серия: ${streak}/7 ${pluralRu(streak, 'день', 'дня', 'дней')} подряд` : 'Следующее сканирование откроется завтра'}</small>
        </span>
        <span class="dbq-chevron">›</span>
      </button>
      <div class="seg-tabs seg-tabs-5">
        <button class="seg-btn ${state.dropTab === 'packs' ? 'is-active' : ''}" data-drop-tab="packs" type="button">Паки</button>
        <button class="seg-btn ${state.dropTab === 'arena' ? 'is-active' : ''}" data-drop-tab="arena" type="button">Арена</button>
        <button class="seg-btn ${state.dropTab === 'fuse' ? 'is-active' : ''}" data-drop-tab="fuse" type="button">Крафт</button>
        <button class="seg-btn ${state.dropTab === 'inventory' ? 'is-active' : ''}" data-drop-tab="inventory" type="button">Склад (${inv.length})</button>
        <button class="seg-btn ${state.dropTab === 'leaderboard' ? 'is-active' : ''}" data-drop-tab="leaderboard" type="button">Топ</button>
      </div>`;

    renderDropBody();
  }

  function renderDropBody() {
    const ds = state.dropState;
    if (!ds) return;
    const body = $('#dropBody');
    const cases = ds.cases || [];
    const inv = ds.inventory || [];
    const themeClasses = ['pack-cyan', 'pack-violet', 'pack-amber'];

    if (state.dropTab === 'packs') {
      body.innerHTML = `
        <div class="sec-head" style="margin-bottom:8px">
          <div><h2>Новые наборы</h2><p class="sec-sub">+10% кэшбэк CXP на счёт с каждого открытия</p></div>
        </div>
        <div class="packs-grid">
          ${cases.map((c, idx) => `<div class="pack-card ${themeClasses[idx % 3]} ${idx === 2 ? 'pack-wide' : ''}" data-action="open-case" data-case-id="${esc(c.id)}">
            <span class="pack-ribbon">${idx === 2 ? 'MYTHIC X4' : `+10% КЭШБЭК`}</span>
            <div class="pack-art">${packArtSvg(idx)}</div>
            <div class="pack-info">
              <strong>${esc(c.name)}</strong>
              <small>${esc(c.subtitle)}</small>
              <span class="pack-price-pill">⚡ ${c.price_cxp} CXP</span>
            </div>
          </div>`).join('')}
        </div>
        <form id="dropClaimForm" class="glass-card" style="margin-top:12px">
          <div>
            <h3>Промокод или Gift-код</h3>
            <p class="muted-sm">Например: <code class="mono">WELCOME50</code>, <code class="mono">DROP100</code>, <code class="mono">CXRNER2026</code></p>
          </div>
          <div class="row-gap">
            <input name="code" placeholder="Введи код…" required>
            <button class="btn btn-primary btn-sm" type="submit">Забрать</button>
          </div>
        </form>`;
      return;
    }

    if (state.dropTab === 'arena') {
      const last = state.arenaResult;
      body.innerHTML = `<div class="arena-stage">
        <div class="hero-top-row">
          <div><div class="eyebrow">1V1 SPECIMEN SHOWDOWN</div><h2 style="margin-top:3px">Арена Дуэлей</h2></div>
          <span class="pill-live">+80% CXP ЗА ПОБЕДУ</span>
        </div>
        <p class="muted-sm">Выбери набор и сразись с Vault Bot! Победитель забирает ОБА выпавших артефакта и бонусные CXP.</p>
        <div class="wrap-gap">
          ${cases.map((c) => `<button class="btn-chip ${state.arenaCaseId === c.id ? 'is-active' : ''}" data-action="select-arena-case" data-case-id="${esc(c.id)}" type="button">${esc(c.name.split('—')[0].trim())} · ${c.price_cxp} CXP</button>`).join('')}
        </div>
        ${last ? `<div class="arena-versus-bar">
          <div class="arena-fighter">
            <span class="muted-sm">Твой бросок</span>
            <strong style="font-size:12px">${esc(last.player_item?.item_name || '—')}</strong>
            <span class="rarity-pill rarity-${esc(last.player_item?.rarity || 'common')}" style="margin:0 auto">${rarityLabel(last.player_item?.rarity)}</span>
          </div>
          <div class="arena-vs">VS</div>
          <div class="arena-fighter">
            <span class="muted-sm">Vault Bot</span>
            <strong style="font-size:12px">${esc(last.bot_item?.item_name || '—')}</strong>
            <span class="rarity-pill rarity-${esc(last.bot_item?.rarity || 'common')}" style="margin:0 auto">${rarityLabel(last.bot_item?.rarity)}</span>
          </div>
        </div>
        <div class="status-note" style="text-align:center;font-weight:800">${last.won ? `🏆 ПОБЕДА! Забрал 2 артефакта и +${last.reward_cxp} CXP!` : '⚡ В этот раз победил Vault Bot. Попробуй реванш!'}</div>` : ''}
        <button id="arenaStartBtn" class="btn btn-amber btn-block" data-action="drop-battle" type="button">⚔️ Начать дуэль 1v1</button>
      </div>`;
      return;
    }

    if (state.dropTab === 'fuse') {
      const fusable = inv.filter((x) => ['common', 'rare', 'epic', 'legendary'].includes(x.rarity));
      body.innerHTML = `<div class="glass-card">
        <div class="hero-top-row">
          <div><h3>Синтез 5 → 1</h3><p class="muted-sm">Выбери 5 артефактов одной редкости и скрафти предмет классом выше.</p></div>
          <span class="mono" id="fuseCounter" style="color:var(--cyan);font-weight:800">${state.fuseSelectedIds.length}/5</span>
        </div>
        <button id="fuseSubmitBtn" class="btn btn-primary btn-block" data-action="drop-fuse-submit" ${state.fuseSelectedIds.length === 5 ? '' : 'disabled'} type="button">⚗️ Синтезировать (${state.fuseSelectedIds.length}/5)</button>
        <div class="inventory-grid">${fusable.length ? fusable.map((it) => `
          <div class="item-card ${state.fuseSelectedIds.includes(String(it.id)) ? 'is-selected' : ''}" data-action="toggle-fuse-item" data-id="${esc(it.id)}">
            <div class="item-preview-box">${specimenGlyphSvg(it.shape, it.model_color, 44)}</div>
            <span class="rarity-pill rarity-${esc(it.rarity)}">${rarityLabel(it.rarity)}</span>
            <strong style="font-size:12px">${esc(it.item_name)}</strong>
          </div>`).join('') : '<div class="empty-box" style="grid-column:1/-1">Нужно минимум 5 предметов Common–Legendary.</div>'}</div>
      </div>`;
      return;
    }

    if (state.dropTab === 'inventory') {
      const rarities = ['all', 'common', 'rare', 'epic', 'legendary', 'mythic', 'special_reward'];
      const filteredInv = state.dropInvFilter === 'all' ? inv : inv.filter((x) => x.rarity === state.dropInvFilter);
      body.innerHTML = `
        <div class="chip-scroll" style="margin-bottom:8px">
          ${rarities.map((r) => `<button class="btn-chip ${state.dropInvFilter === r ? 'is-active' : ''}" data-action="set-inv-filter" data-rarity="${r}" type="button">${r === 'all' ? `Все (${inv.length})` : rarityLabel(r)}</button>`).join('')}
        </div>
        <div class="inventory-grid">
          ${filteredInv.length ? filteredInv.map((it) => `<div class="item-card" data-action="inspect-item" data-id="${esc(it.id)}">
            <div class="item-preview-box">${specimenGlyphSvg(it.shape, it.model_color, 48)}</div>
            <div class="hero-top-row">
              <span class="rarity-pill rarity-${esc(it.rarity)}">${rarityLabel(it.rarity)}</span>
              ${it.is_pinned ? '<span style="font-size:11px">📌</span>' : ''}
            </div>
            <strong style="font-size:12px;line-height:1.25">${esc(it.item_name)}</strong>
            <small class="mono muted-sm">#${String(it.serial_number || 1).padStart(3, '0')} · ${esc(it.specimen_code)}</small>
          </div>`).join('') : '<div class="empty-box" style="grid-column:1/-1">В этой категории пока пусто. Открой набор во вкладке «Паки»!</div>'}
        </div>`;
      return;
    }

    const lb = ds.leaderboard || [];
    body.innerHTML = `<div class="stack-list">${lb.length ? lb.map((r, i) => `
      <div class="list-row">
        <strong class="mono" style="width:26px;text-align:center;color:${i < 3 ? '#FDE68A' : 'var(--muted)'}">#${i + 1}</strong>
        <div class="list-meta">
          <strong>${esc(r.display_name)} ${r.is_ceo ? '👑' : ''}</strong>
          <small>${r.items_count} ${pluralRu(r.items_count, 'артефакт', 'артефакта', 'артефактов')}</small>
        </div>
        <strong class="mono" style="color:var(--cyan)">${formatNumber(r.score)} XP</strong>
      </div>`).join('') : '<div class="empty-box">Рейтинг формируется…</div>'}</div>`;
  }

  function showSpecimenRevealModal(it, cashback = 0, caseId = null) {
    const salvageVal = { common: 15, rare: 30, epic: 55, legendary: 120, mythic: 250, special_reward: 500 }[it.rarity] || 15;
    showModal(`
      <div class="hero-top-row">
        <span class="rarity-pill rarity-${esc(it.rarity)}">${rarityLabel(it.rarity)}</span>
        <span class="mono muted-sm">${esc(it.specimen_code)} · #${String(it.serial_number || 1).padStart(3, '0')}</span>
      </div>
      <canvas id="dropInspectCanvas" class="drop-3d-canvas"></canvas>
      <div>
        <h2>${esc(it.item_name)}</h2>
        <p class="muted-sm" style="margin-top:4px">${esc(it.description || it.collection_name || 'Экземпляр CXRNER DROP')}</p>
        ${cashback ? `<p class="muted-sm" style="color:#6EE7B7;margin-top:4px">⚡ Кэшбэк +${cashback} CXP зачислен</p>` : ''}
      </div>
      ${it.claim_code ? `<div class="status-note">🎁 Код ваучера: <strong class="mono">${esc(it.claim_code)}</strong> <button class="link-btn" data-copy="${esc(it.claim_code)}" type="button">Копировать</button></div>` : ''}
      <div class="wrap-gap">
        ${caseId ? `<button class="btn btn-primary btn-sm" data-action="replay-case" data-case-id="${esc(caseId)}" type="button">🎲 Открыть ещё</button>` : ''}
        <button class="btn btn-outline btn-sm" data-action="pin-item" data-id="${esc(it.id)}" type="button">${it.is_pinned ? 'Открепить' : '📌 На витрину'}</button>
        <button class="btn btn-outline btn-sm" data-action="gift-item" data-id="${esc(it.id)}" type="button">🎁 Подарить</button>
        <button class="btn btn-danger btn-sm" data-action="salvage-item" data-id="${esc(it.id)}" type="button">Продать +${salvageVal} CXP</button>
      </div>`);
    requestAnimationFrame(() => startInteractive3dCanvas('dropInspectCanvas', it));
  }

  function inspectDropItem(itemId) {
    const it = (state.dropState?.inventory || []).find((x) => String(x.id) === String(itemId));
    if (it) showSpecimenRevealModal(it);
  }

  async function handleOpenCase(caseId) {
    try {
      haptic('medium');
      const res = await api('/api/miniapp/drop/open', { method: 'POST', body: { case_id: caseId } });
      syncWalletState(res.wallet);
      if (state.dropState) {
        state.dropState.inventory = [res.item, ...(state.dropState.inventory || [])];
      }
      updateTopbar();

      if (state.fastOpen) {
        haptic('success');
        showSpecimenRevealModal(res.item, res.cashback, caseId);
        renderDropScreen();
        return;
      }

      const pool = state.dropState?.specimens || [res.item];
      const winnerIdx = 26;
      const tiles = [];
      for (let i = 0; i < 34; i += 1) {
        tiles.push(i === winnerIdx ? res.item : pool[Math.floor(Math.random() * pool.length)]);
      }

      const overlay = $('#unsealOverlay');
      $('#unsealStage').innerHTML = `
        <div class="hero-top-row">
          <div><div class="eyebrow">РАСПАКОВКА КАПСУЛЫ</div><h2 style="margin-top:3px">CXRNER DROP</h2></div>
          <button class="btn btn-outline btn-xs" id="unsealSkipBtn" type="button">Пропустить ⚡</button>
        </div>
        <div class="roulette-viewport">
          <div class="roulette-needle"></div>
          <div class="roulette-track" id="rouletteTrack">
            ${tiles.map((t) => `<div class="roulette-tile rarity-${esc(t.rarity)}">
              ${specimenGlyphSvg(t.shape, t.model_color, 44)}
              <span>${esc(t.item_name)}</span>
            </div>`).join('')}
          </div>
        </div>
        <p class="muted-sm" style="text-align:center">Крутим генератор артефактов…</p>`;
      overlay.classList.remove('is-hidden');

      let finished = false;
      const finishUnseal = () => {
        if (finished) return;
        finished = true;
        overlay.classList.add('is-hidden');
        haptic('success');
        renderDropScreen();
        showSpecimenRevealModal(res.item, res.cashback, caseId);
      };

      $('#unsealSkipBtn').onclick = finishUnseal;

      requestAnimationFrame(() => {
        const track = $('#rouletteTrack');
        const vp = $('.roulette-viewport');
        if (!track || !vp) { finishUnseal(); return; }
        const stepPx = 108;
        const centerOffset = vp.clientWidth / 2 - 50;
        const targetX = winnerIdx * stepPx - centerOffset + (Math.random() * 14 - 7);
        track.style.transition = 'transform 2.55s cubic-bezier(0.08, 0.90, 0.18, 1)';
        track.style.transform = `translate3d(-${Math.max(0, targetX)}px, 0, 0)`;
        setTimeout(() => { if (!finished) haptic('light'); }, 650);
        setTimeout(() => { if (!finished) haptic('light'); }, 1350);
        setTimeout(() => { if (!finished) haptic('light'); }, 1950);
        setTimeout(finishUnseal, 2750);
      });
    } catch (e) {
      haptic('error');
      toast(e.message);
    }
  }

  /* ==================== PROFILE & FRIENDS ("ПУТЬ ЧЕМПИОНА") ==================== */
  async function loadProfile() {
    try {
      const [p, refs, an] = await Promise.all([
        api('/api/miniapp/platform/profile'),
        api('/api/miniapp/platform/referrals').catch(() => null),
        api('/api/miniapp/platform/analytics').catch(() => null)
      ]);
      state.profileData = p;
      state.referralsData = refs;
      state.analyticsData = an;
      state.isAdmin = Boolean(p.is_admin || state.isAdmin);
      updateTopbar();
      renderProfile();
    } catch (e) {
      $('#profileHeaderCard').innerHTML = `<div class="glass-card"><p class="muted">${esc(e.message)}</p></div>`;
    }
  }

  function renderProfile() {
    const p = state.profileData;
    if (!p) return;
    const u = p.user || {};
    const ap = p.artist_profile || {};
    const tgUser = tg?.initDataUnsafe?.user || {};
    const displayName = ap.display_name || u.first_name || tgUser.first_name || tgUser.username || 'Артист';
    const uname = String(u.username || tgUser.username || '').toLowerCase();
    const isCeo = uname === 'kazumaiq' || state.isAdmin;
    const isVerif = state.homeData?.verification_status === 'verified' || isCeo;
    const frame = ap.equipped_frame || 'default';
    const avatar = ap.avatar_url || u.photo_url || tgUser.photo_url || '';
    const bannerStyle = ap.banner_url ? `background-image:url('${esc(ap.banner_url)}')` : '';

    $('#profileRelCount').textContent = String((p.releases || []).length);
    $$('#profileTabs .seg-btn').forEach((b) => b.classList.toggle('is-active', b.dataset.ptab === state.profileTab));

    $('#profileHeaderCard').innerHTML = `<div class="profile-banner-card">
      <div class="profile-cover-bg" style="${bannerStyle}"></div>
      <div class="profile-top-bar">
        <div class="profile-avatar-xl frame-${esc(frame)}">
          ${avatar ? `<img src="${esc(avatar)}" alt="" data-fallback-char="${esc(displayName[0].toUpperCase())}" onerror="this.style.display='none';this.parentElement.textContent=this.dataset.fallbackChar||'C'">` : esc(displayName[0].toUpperCase())}
        </div>
        <div class="profile-btn-pair wrap-gap" style="justify-content:flex-end">
          <button class="btn btn-outline btn-xs" data-action="edit-profile" type="button">Настроить ID</button>
          <button class="btn btn-outline btn-xs" data-screen="link" type="button">Smart Link</button>
          <button class="btn btn-outline btn-xs" data-screen="verification" type="button">Верификация</button>
        </div>
      </div>
      <div style="margin-top:10px">
        <div class="wrap-gap" style="align-items:center">
          <h2>${esc(displayName)}</h2>
          ${isCeo ? '<span class="ceo-badge">👑 CEO</span>' : ''}
          ${isVerif ? '<span class="verified-pill">✓ VERIFIED</span>' : ''}
        </div>
        <div class="muted-sm" style="margin-top:3px">${u.username ? `@${esc(u.username)}` : `ID ${esc(u.telegram_id || '')}`}${ap.city ? ` · ${esc(ap.city)}` : ''}</div>
        ${ap.bio ? `<p class="muted-sm" style="margin-top:7px">${esc(ap.bio)}</p>` : ''}
      </div>
      ${state.isAdmin ? `<button class="btn btn-amber btn-block" data-screen="admin" type="button" style="margin-top:12px">👑 Открыть Админ-центр CXRNER</button>` : ''}
    </div>`;

    renderProfileTabBody();
  }

  function renderProfileTabBody() {
    const p = state.profileData;
    const refs = state.referralsData;
    const an = state.analyticsData;
    if (!p) return;
    const body = $('#profileTabBody');

    if (state.profileTab === 'overview') {
      const releases = p.releases || [];
      const pinned = (state.dropState?.inventory || []).filter((i) => i.is_pinned).slice(0, 4);
      const roles = (p.artist_profile?.music_roles || []).length ? p.artist_profile.music_roles : ['Artist'];
      const st = p.stats || {};
      body.innerHTML = `<div class="stack-list">
        <div class="glass-card">
          <div class="hero-top-row"><h3>Роли и сводка</h3><span class="muted-sm">${p.user?.telegram_id || ''}</span></div>
          <div class="wrap-gap">${roles.map((r) => `<span class="btn-chip is-active">${esc(r)}</span>`).join('')}</div>
          <div class="kpi-grid-4">
            <div class="kpi-box"><strong>${releases.length}</strong><span>${pluralRu(releases.length, 'Заявка', 'Заявки', 'Заявок')}</span></div>
            <div class="kpi-box"><strong>${st.likes || 0}</strong><span>${pluralRu(st.likes || 0, 'Лайк', 'Лайка', 'Лайков')}</span></div>
            <div class="kpi-box"><strong>${refs?.total || 0}</strong><span>${pluralRu(refs?.total || 0, 'Друг', 'Друга', 'Друзей')}</span></div>
            <div class="kpi-box"><strong>${(p.badges || []).length}</strong><span>Бейджи</span></div>
          </div>
        </div>
        <div class="glass-card">
          <div class="hero-top-row">
            <div><h3>Витрина CXRNER DROP</h3><p class="muted-sm">Закрепляй редкие артефакты из Склада.</p></div>
            <button class="btn btn-outline btn-xs" data-screen="drop" type="button">DROP ↗</button>
          </div>
          ${pinned.length ? `<div class="inventory-grid">${pinned.map((it) => `<div class="item-card" data-action="inspect-item" data-id="${esc(it.id)}"><div class="item-preview-box">${specimenGlyphSvg(it.shape, it.model_color, 40)}</div><span class="rarity-pill rarity-${esc(it.rarity)}">${rarityLabel(it.rarity)}</span><strong style="font-size:11px">${esc(it.item_name)}</strong></div>`).join('')}</div>` : '<p class="muted-sm">Закреплённых артефактов нет — нажми «📌 На витрину» в DROP.</p>'}
        </div>
      </div>`;
      return;
    }

    if (state.profileTab === 'friends') {
      const invited = refs?.invited || [];
      const milestones = refs?.milestones || [
        { step: 1, required: 1, title: '1 друг', reward_type: 'cxp', reward_cxp: 75, unlocked: false, claimed: false },
        { step: 2, required: 2, title: '2 друга', reward_type: 'cxp', reward_cxp: 125, unlocked: false, claimed: false },
        { step: 3, required: 3, title: '3 друга', reward_type: 'pack', reward_cxp: 50, unlocked: false, claimed: false },
        { step: 4, required: 4, title: '4 друга', reward_type: 'cxp', reward_cxp: 200, unlocked: false, claimed: false },
        { step: 5, required: 5, title: '5 друзей', reward_type: 'pack', reward_cxp: 0, unlocked: false, claimed: false }
      ];
      const inviteUrl = refs?.miniapp_invite_url || refs?.invite_url || `https://t.me/${refs?.bot_username || 'moder_cxrner_bot'}?start=${refs?.ref_code || ''}`;
      body.innerHTML = `<div class="stack-list">
        <div class="invite-hero-card">
          <h3>Приглашай друзей и получай +75 CXP!</h3>
          <p>Друг получит <b>+50 CXP</b> приветственного бонуса сразу после перехода по ссылке.</p>
          <div class="row-gap">
            <button class="btn btn-white btn-sm" style="flex:1" data-action="share-invite" data-url="${esc(inviteUrl)}" type="button">📲 Пригласить друга</button>
            <button class="btn btn-glass-lite btn-sm" data-copy="${esc(inviteUrl)}" type="button">📋 Копия</button>
          </div>
        </div>

        <div class="champion-card">
          <div class="champion-head">
            <h3>Путь чемпиона</h3>
            <p>Пригласи 5 друзей и забери наборы DROP + до 450 CXP!</p>
          </div>
          <div class="champion-steps">
            ${milestones.map((m) => {
              const cls = m.claimed ? 'is-claimed' : m.unlocked ? 'is-unlocked' : '';
              return `<button class="champ-slot ${cls}" data-action="claim-milestone" data-step="${m.step}" type="button">
                <strong>${m.claimed ? '✓' : m.reward_type === 'pack' ? '🎁' : `${m.step}`}</strong>
                <small>${m.claimed ? 'Забрано' : m.unlocked ? 'Забрать!' : m.reward_type === 'pack' ? (m.step === 5 ? '2× Pack' : 'Pack') : `+${m.reward_cxp}`}</small>
              </button>`;
            }).join('')}
          </div>
          <div class="champ-progress-dots">
            ${milestones.map((m) => `<div class="champ-bar ${invited.length >= m.required ? 'is-fill' : ''}"></div>`).join('')}
          </div>
        </div>

        <div class="glass-card">
          <div class="hero-top-row">
            <h3>Приглашённые друзья (${invited.length})</h3>
            <span class="mono" style="color:#6EE7B7;font-size:12px">+${formatNumber(refs?.total_bonus_earned || 0)} CXP</span>
          </div>
          ${invited.length ? `<div class="stack-list">${invited.map((f) => `<div class="list-row" style="cursor:default">
            <div class="profile-avatar-xl" style="width:40px;height:40px;border-radius:12px;font-size:15px">${f.photo_url ? `<img src="${esc(f.photo_url)}" onerror="this.style.display='none'">` : esc((f.display_name || 'F')[0].toUpperCase())}</div>
            <div class="list-meta">
              <strong>${esc(f.display_name)}</strong>
              <small>${f.username ? `@${esc(f.username)}` : `ID ${esc(f.invited_id)}`} · ${relTime(f.created_at)}</small>
            </div>
            <span class="mono" style="color:#6EE7B7;font-size:12px;font-weight:800">+${Number(f.bonus || 75)} CXP</span>
          </div>`).join('')}</div>` : '<p class="muted-sm">Пока никого нет. Отправь ссылку друзьям-музыкантам выше!</p>'}
        </div>
      </div>`;
      return;
    }

    if (state.profileTab === 'releases') {
      const releases = p.releases || [];
      body.innerHTML = releases.length ? `<div class="stack-list">${releases.map((r) => `<div class="glass-card">
        <div class="row-gap">
          <img class="list-thumb" src="${esc(coverUrl(r))}" alt="" data-cover-title="${esc(r.track_name || '')}" data-cover-artist="${esc(r.artist_name || '')}" onerror="window.__cxrCoverErr(this)">
          <div class="list-meta">
            <strong>${esc(r.track_name || 'Без названия')}</strong>
            <small>${esc(r.artist_name || 'Артист')} · ${esc(statusText(r.status))}</small>
          </div>
          ${r.upc ? `<span class="btn-chip mono" style="font-size:10px">UPC ${esc(r.upc)}</span>` : ''}
        </div>
        ${r.reject_reason ? `<p class="status-note" style="background:rgba(244,63,94,0.14);border-color:rgba(244,63,94,0.4);color:#FDA4AF">${esc(r.reject_reason)}</p>` : ''}
        <div class="wrap-gap">
          <button class="btn btn-outline btn-xs" data-action="edit-release-media" data-id="${esc(r.id)}" type="button">✏️ Медиа</button>
          <button class="btn btn-outline btn-xs" data-screen="link" type="button">🔗 Смарт-ссылка</button>
          <button class="btn btn-danger btn-xs" data-action="delete-release" data-id="${esc(r.id)}" type="button">Удалить</button>
        </div>
      </div>`).join('')}</div>` : '<div class="empty-box">У тебя ещё нет отправленных релизов.</div>';
      return;
    }

    const totals = an?.totals || { streams: 0, revenue: 0, seconds: 0 };
    const vals = [3, 6, 4, 9, 7, 11, Math.max(4, (totals.streams || 0) % 15 + 4)];
    const maxV = Math.max(...vals, 1);
    const days = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];
    body.innerHTML = `<div class="stack-list">
      <div class="kpi-grid-4" style="grid-template-columns:repeat(2,1fr)">
        <div class="kpi-box"><strong>${totals.streams || 0}</strong><span>Прослушивания</span></div>
        <div class="kpi-box"><strong>${Number(totals.revenue || 0).toFixed(2)} ₽</strong><span>Роялти</span></div>
        <div class="kpi-box"><strong>${Math.round((totals.seconds || 0) / 60)} м</strong><span>Время в плеере</span></div>
        <div class="kpi-box"><strong>${(an?.payouts || []).length}</strong><span>Выплаты</span></div>
      </div>
      <div class="glass-card">
        <h3>Динамика активности</h3>
        <div class="svg-bars">${vals.map((v, i) => `<div class="svg-bar-col"><div class="svg-bar-fill" style="height:${Math.round((v / maxV) * 64) + 6}px"></div><small>${days[i]}</small></div>`).join('')}</div>
      </div>
    </div>`;
  }

  /* ==================== SMART LINKS / VERIF / NOTIF / RANK / CAL / ACTIVITY ==================== */
  async function loadLinkEditor() {
    try {
      const res = await api('/api/miniapp/link?mine=1');
      state.myLinks = res.links || [];
      $('#myLinksList').innerHTML = state.myLinks.length
        ? state.myLinks.map((l) => `<div class="glass-card">
            <div class="hero-top-row">
              <div><strong>${esc(l.title)}</strong><p class="muted-sm">${esc(l.artist)} · /${esc(l.slug)}</p></div>
              <button class="btn btn-outline btn-xs" data-external="${esc(l.public_url)}" type="button">Открыть ↗</button>
            </div>
            <div class="wrap-gap">
              <button class="btn btn-outline btn-xs" data-copy="${esc(l.public_url)}" type="button">Копия URL</button>
              <button class="btn btn-outline btn-xs" data-action="edit-link" data-id="${esc(l.id)}" type="button">Изменить</button>
              <button class="btn btn-danger btn-xs" data-action="delete-link" data-id="${esc(l.id)}" type="button">Удалить</button>
            </div>
          </div>`).join('')
        : '<div class="empty-box">Смарт-ссылок пока нет.</div>';
    } catch {}
  }

  function renderVerification() {
    const st = state.homeData?.verification_status || 'not_verified';
    if (st === 'verified') {
      $('#verificationStateBox').innerHTML = '<div class="glass-card"><h2>✓ Профиль верифицирован</h2><p class="muted">Твой CXRNER ID отмечен официальной галочкой лейбла.</p></div>';
      return;
    }
    if (st === 'pending_review') {
      $('#verificationStateBox').innerHTML = '<div class="glass-card"><h2>⏳ Заявка на проверке</h2><p class="muted">Администрация рассмотрит её в разделе «Юзеры & Верификация» и пришлёт уведомление.</p></div>';
      return;
    }
    $('#verificationStateBox').innerHTML = `<form id="verificationForm" class="glass-card">
      <h3>Запрос галочки Verified</h3>
      <p class="muted-sm">Подтверди свой сценический профиль через ссылки на официальные площадки.</p>
      <label class="field"><span>Сценическое имя *</span><input name="stage_name" required placeholder="CXRNER"></label>
      <label class="field"><span>Яндекс Музыка / Spotify URL *</span><input name="yandex_url" type="url" required placeholder="https://music.yandex.ru/artist/…"></label>
      <label class="field"><span>Контакт Telegram</span><input name="contact_telegram" placeholder="@username"></label>
      <label class="field"><span>Комментарий модератору</span><textarea name="evidence_notes" rows="2" placeholder="Дополнительные ссылки или пруфы…"></textarea></label>
      <button class="btn btn-primary btn-block" type="submit">Отправить заявку ✓</button>
    </form>`;
  }

  function renderNotifications() {
    const list = state.homeData?.notifications || [];
    $('#notificationsList').innerHTML = list.length
      ? list.map((n) => `<div class="glass-card" style="${n.read_at ? '' : 'border-color:rgba(34,211,238,0.48)'}">
          <div class="hero-top-row">
            <strong>${esc(n.title)}</strong>
            <span class="muted-sm">${relTime(n.created_at)}</span>
          </div>
          <p class="muted-sm">${esc(n.body)}</p>
        </div>`).join('')
      : '<div class="empty-box">Новых уведомлений нет.</div>';
  }

  async function loadRanking() {
    try {
      const res = await api(`/api/miniapp/platform/rankings?sort=${encodeURIComponent(state.rankSort)}`);
      const rows = res.rankings || [];
      const html = rows.length
        ? rows.map((r, i) => `<div class="list-row" data-action="open-artist" data-id="${esc(r.telegram_id)}">
            <strong class="mono" style="width:28px;text-align:center;color:${i < 3 ? '#FDE68A' : 'var(--muted)'}">#${i + 1}</strong>
            <div class="list-meta"><strong>${esc(r.artist)}</strong><small>${r.releases} ${pluralRu(r.releases, 'релиз', 'релиза', 'релизов')} · ${r.listens} прослушиваний</small></div>
            <strong class="mono" style="color:var(--cyan)">♥ ${r.likes}</strong>
          </div>`).join('')
        : '<div class="empty-box">Чарт пуст.</div>';
      if ($('#rankingList')) $('#rankingList').innerHTML = html;
      if ($('#musicRankingList')) $('#musicRankingList').innerHTML = html;
    } catch {}
  }

  async function loadCalendar() {
    try {
      const res = await api('/api/miniapp/platform/calendar');
      const rows = res.releases || [];
      const html = rows.length
        ? rows.slice(0, 40).map((r) => `<div class="list-row" data-release-id="${esc(r.id)}">
            <img class="list-thumb" src="${esc(coverUrl(r))}" alt="" data-cover-title="${esc(r.title || '')}" data-cover-artist="${esc(r.artist || '')}" onerror="window.__cxrCoverErr(this)">
            <div class="list-meta"><strong>${esc(r.title)}</strong><small>${esc(r.artist)}</small></div>
            <span class="btn-chip mono" style="font-size:11px">${esc(r.date || 'Скоро')}</span>
          </div>`).join('')
        : '<div class="empty-box">Календарь пуст.</div>';
      if ($('#calendarList')) $('#calendarList').innerHTML = html;
      if ($('#musicCalendarList')) $('#musicCalendarList').innerHTML = html;
    } catch {}
  }

  async function loadActivity() {
    try {
      const res = await api('/api/miniapp/platform/activity');
      const items = res.items || [];
      $('#activityList').innerHTML = items.length
        ? items.map((x) => `<div class="glass-card"><div class="hero-top-row"><strong>${esc(x.title)}</strong><span class="muted-sm">${relTime(x.created_at)}</span></div><p class="muted-sm">${esc(x.body)}</p></div>`).join('')
        : '<div class="empty-box">Событий пока нет.</div>';
    } catch {}
  }

  /* ==================== ADMIN CONTROL CENTER ==================== */
  async function loadAdmin() {
    if (!state.isAdmin) {
      $('#adminTabBody').innerHTML = '<div class="empty-box">Доступ только для администраторов CXRNER.</div>';
      return;
    }
    if (!state.adminData) $('#adminTabBody').innerHTML = '<div class="glass-card"><p class="muted">Загружаем телеметрию бота, сайта и Mini App…</p></div>';
    try {
      const data = await api('/api/miniapp/platform/admin/overview');
      state.adminData = data;
      const pendingMod = (data.stats?.on_upload || 0) + (data.stats?.needfix || 0) + (data.stats?.moderation || 0);
      $('#admModCount').textContent = String(data.stats?.moderation || pendingMod || 0);
      renderAdminTab();
    } catch (e) {
      $('#adminTabBody').innerHTML = `<div class="glass-card"><p class="muted">${esc(e.message)}</p></div>`;
    }
  }

  function fmtUptime(sec) {
    const s = Math.max(0, Number(sec || 0));
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    return h > 0 ? `${h}ч ${m}м` : `${m}м ${s % 60}с`;
  }

  function renderAdminTab() {
    $$('#adminTabs .seg-btn').forEach((b) => b.classList.toggle('is-active', b.dataset.atab === state.adminTab));
    const d = state.adminData;
    const body = $('#adminTabBody');
    if (!d) return;

    const st = d.stats || {};
    const bh = d.bot_health || {};
    const src = st.source_breakdown || { web: 0, mini_app: 0, bot: 0 };
    const totalSrc = Math.max(1, (src.web || 0) + (src.mini_app || 0) + (src.bot || 0));

    if (state.adminTab === 'overview') {
      const pollOk = bh.polling?.ok !== false;
      const pinOk = bh.moderation?.can_pin_messages === true;
      body.innerHTML = `<div class="stack-list">
        <div class="admin-hero-card">
          <div class="hero-top-row">
            <div><div class="eyebrow">LIVE BOT & SERVER</div><h3 style="margin-top:3px">Build ${esc(bh.build || 'ui-v8')}</h3></div>
            <span class="pill-live">${pollOk ? '● POLLING OK' : '⚠ POLL ERR'}</span>
          </div>
          <div class="muted-sm">Commit: <code class="mono">${esc(bh.commit || 'local')}</code> · Uptime: <b>${fmtUptime(bh.uptime_sec)}</b></div>
          <div class="muted-sm">Мод-чат: <b>${esc(bh.moderation?.chat_title || 'Группа')}</b> · @${esc(bh.moderation?.bot_username || 'bot')} · Закреп: ${pinOk ? '✅' : '⚠️'}</div>
          <div class="wrap-gap">
            <span class="btn-chip is-active">Supabase: ${bh.supabase_enabled ? 'ON' : 'OFF'}</span>
            <span class="btn-chip">Верификаций ждут: ${st.verifications_pending || 0}</span>
          </div>
        </div>

        <div class="kpi-grid-4" style="grid-template-columns:repeat(3,1fr)">
          <div class="kpi-box"><strong>${st.users || 0}</strong><span>Юзеры (+${st.new_today || 0})</span></div>
          <div class="kpi-box"><strong>${st.releases || 0}</strong><span>В каталоге</span></div>
          <div class="kpi-box"><strong style="color:#FDE68A">${st.moderation || 0}</strong><span>На проверке</span></div>
          <div class="kpi-box"><strong>${st.links || 0}</strong><span>Смарт-ссылки</span></div>
          <div class="kpi-box"><strong>${st.views || 0}</strong><span>Стримы</span></div>
          <div class="kpi-box"><strong style="color:var(--cyan)">${st.drop_total_items || 0}</strong><span>DROP предм.</span></div>
        </div>

        <div class="glass-card">
          <div class="hero-top-row"><h3>Источники анкет (${st.forms_total || totalSrc})</h3><span class="muted-sm">Сайт · Mini App · Бот</span></div>
          <div class="source-bar-track">
            <div class="sbt-web" style="width:${Math.round(((src.web || 0) / totalSrc) * 100)}%"></div>
            <div class="sbt-mini" style="width:${Math.round(((src.mini_app || 0) / totalSrc) * 100)}%"></div>
            <div class="sbt-bot" style="width:${Math.round(((src.bot || 0) / totalSrc) * 100)}%"></div>
          </div>
          <div class="wrap-gap">
            <span class="btn-chip">🌐 Сайт: ${src.web || 0}</span>
            <span class="btn-chip">📱 Mini App: ${src.mini_app || 0}</span>
            <span class="btn-chip">🤖 Бот: ${src.bot || 0}</span>
          </div>
          <div class="wrap-gap" style="margin-top:4px">
            <span class="btn-chip" style="color:#FDE68A">Отгрузка: ${st.on_upload || 0}</span>
            <span class="btn-chip" style="color:#6EE7B7">Одобрено: ${st.approved || 0}</span>
            <span class="btn-chip" style="color:#7DD3FC">Правки: ${st.needfix || 0}</span>
            <span class="btn-chip" style="color:#FDA4AF">Откл.: ${st.rejected || 0}</span>
          </div>
        </div>
      </div>`;
      return;
    }

    if (state.adminTab === 'moderation') {
      const forms = d.forms || [];
      const q = state.adminModSearch.trim().toLowerCase();
      const filtered = forms.filter((f) => {
        if (state.adminModSource !== 'all' && f.source !== state.adminModSource) return false;
        const fs = String(f.status || '').toLowerCase();
        if (state.adminModStatus === 'pending' && !['on_upload', 'pending', 'moderation', 'on_moderation', 'needfix'].includes(fs)) return false;
        if (state.adminModStatus === 'approved' && !['approved', 'published'].includes(fs)) return false;
        if (state.adminModStatus === 'rejected' && fs !== 'rejected') return false;
        if (!q) return true;
        return `${f.track_name || ''} ${f.artist_name || ''} ${f.username || ''} ${f.upc || ''}`.toLowerCase().includes(q);
      }).slice(0, 60);

      body.innerHTML = `<div class="stack-list">
        <div class="search-bar">
          <span class="ico-wrap" data-icon="search"></span>
          <input id="adminModFilterInput" type="search" value="${esc(state.adminModSearch)}" placeholder="Трек, артист, UPC…">
        </div>
        <div class="chip-scroll">
          ${[['all', 'Все'], ['pending', 'Ждут решения'], ['approved', 'Одобрены'], ['rejected', 'Отклонены']].map(([k, lbl]) =>
            `<button class="btn-chip ${state.adminModStatus === k ? 'is-active' : ''}" data-action="adm-mod-status" data-val="${k}" type="button">${lbl}</button>`).join('')}
        </div>
        <div class="chip-scroll">
          ${[['all', 'Все каналы'], ['web', '🌐 Сайт'], ['mini_app', '📱 Mini App'], ['bot', '🤖 Бот']].map(([k, lbl]) =>
            `<button class="btn-chip ${state.adminModSource === k ? 'is-active' : ''}" data-action="adm-mod-source" data-val="${k}" type="button">${lbl}</button>`).join('')}
        </div>
        ${filtered.length ? filtered.map((f) => `<div class="mod-form-card">
          <div class="row-gap">
            <img class="list-thumb" src="${esc(coverUrl(f))}" alt="" data-cover-title="${esc(f.track_name)}" data-cover-artist="${esc(f.artist_name)}" onerror="window.__cxrCoverErr(this)">
            <div class="list-meta">
              <strong>${esc(f.track_name)}</strong>
              <small>${esc(f.artist_name)} ${f.username ? `(@${esc(f.username)})` : ''} · ${f.source === 'web' ? '🌐 Сайт' : f.source === 'mini_app' ? '📱 Mini App' : '🤖 Бот'}</small>
            </div>
            <span class="status-badge status-${esc(statusCss(f.status))}" style="position:static">${esc(statusText(f.status))}</span>
          </div>
          ${f.upc ? `<div class="mono muted-sm">UPC: <b>${esc(f.upc)}</b></div>` : ''}
          ${f.reject_reason ? `<div class="muted-sm" style="color:#FDA4AF">${esc(f.reject_reason)}</div>` : ''}
          <div class="mod-form-actions">
            <button class="btn btn-success btn-xs" data-action="adm-moderate" data-act="approve" data-id="${esc(f.id)}" data-uid="${esc(f.telegram_id)}" data-track="${esc(f.track_name)}" type="button">✅ Принять</button>
            <button class="btn btn-outline btn-xs" data-action="adm-moderate" data-act="moderate" data-id="${esc(f.id)}" data-uid="${esc(f.telegram_id)}" data-track="${esc(f.track_name)}" type="button">🧠 Модер.</button>
            <button class="btn btn-outline btn-xs" data-action="adm-moderate-prompt" data-act="needfix" data-id="${esc(f.id)}" data-uid="${esc(f.telegram_id)}" data-track="${esc(f.track_name)}" type="button">✏️ Правки</button>
            <button class="btn btn-danger btn-xs" data-action="adm-moderate-prompt" data-act="reject" data-id="${esc(f.id)}" data-uid="${esc(f.telegram_id)}" data-track="${esc(f.track_name)}" type="button">❌ Откл.</button>
            <button class="btn btn-amber btn-xs" data-action="adm-moderate-prompt" data-act="upc" data-id="${esc(f.id)}" data-uid="${esc(f.telegram_id)}" data-track="${esc(f.track_name)}" type="button">📦 UPC</button>
          </div>
        </div>`).join('') : '<div class="empty-box">Анкеты по фильтру не найдены.</div>'}
      </div>`;
      renderIcons(body);
      return;
    }

    if (state.adminTab === 'users') {
      const verifs = d.verifications || [];
      const pendingVerifs = verifs.filter((v) => v.status === 'pending_review');
      const users = (d.users || []).slice(0, 60);
      body.innerHTML = `<div class="stack-list">
        <div class="search-bar">
          <span class="ico-wrap" data-icon="search"></span>
          <input id="adminSearchInput" type="search" value="${esc(state.adminSearchQuery)}" placeholder="Глобальный поиск по базе (от 2 букв)…">
        </div>
        <div id="adminSearchResultsBox">${renderAdminSearchHtml()}</div>
        ${pendingVerifs.length ? `<div class="glass-card" style="border-color:rgba(245,158,11,0.4)">
          <h3>Заявки на верификацию (${pendingVerifs.length})</h3>
          ${pendingVerifs.map((v) => `<div style="padding:8px 0;border-bottom:1px solid var(--border)">
            <strong>${esc(v.stage_name || 'Артист')}</strong> <span class="muted-sm">${esc(v.contact_telegram || '')}</span>
            <p class="muted-sm" style="margin-top:3px">${esc(v.evidence_notes || '')} ${(v.dsp_links || []).map((u) => esc(u)).join(', ')}</p>
            <div class="row-gap" style="margin-top:7px">
              <button class="btn btn-success btn-xs" data-action="adm-verify" data-decision="approve" data-id="${esc(v.id)}" data-uid="${esc(v.user_id)}" data-stage="${esc(v.stage_name)}" type="button">✅ Одобрить</button>
              <button class="btn btn-danger btn-xs" data-action="adm-verify" data-decision="reject" data-id="${esc(v.id)}" data-uid="${esc(v.user_id)}" data-stage="${esc(v.stage_name)}" type="button">❌ Отклонить</button>
            </div>
          </div>`).join('')}
        </div>` : ''}
        <div class="glass-card">
          <h3>Пользователи (${(d.users || []).length})</h3>
          <p class="muted-sm">Нажми на пользователя, чтобы начислить CXP или изменить роль.</p>
          <div class="stack-list">
            ${users.map((u) => `<div class="list-row" data-action="adm-inspect-user" data-uid="${esc(u.telegram_id)}">
              <div class="list-meta">
                <strong>${esc(u.first_name || u.username || `ID ${u.telegram_id}`)} ${u.verification_status === 'verified' ? '✓' : ''}</strong>
                <small>@${esc(u.username || '—')} · ${esc(u.role || 'artist')} · ${esc(u.status || 'active')}</small>
              </div>
              <span class="btn-chip mono" style="font-size:10px">⚡ ${formatNumber(u.wallet_balance || 0)}</span>
            </div>`).join('')}
          </div>
        </div>
      </div>`;
      renderIcons(body);
      return;
    }

    const newsList = d.news || [];
    const commentsList = (d.comments || []).slice(0, 20);
    body.innerHTML = `<div class="stack-list">
      <form id="adminNewsForm" class="glass-card">
        <h3>Опубликовать новость лейбла</h3>
        <input name="title" placeholder="Заголовок новости" required>
        <textarea name="body" rows="2" placeholder="Текст для главной витрины…" required></textarea>
        <button class="btn btn-primary btn-sm" type="submit">Опубликовать новость</button>
      </form>
      <form id="adminBroadcastForm" class="glass-card">
        <h3>Рассылка в Mini App</h3>
        <input name="title" placeholder="Заголовок уведомления" required>
        <textarea name="body" rows="2" placeholder="Сообщение пользователям…" required></textarea>
        <select name="audience" class="select-ui"><option value="all">Всем пользователям</option><option value="artists">Артистам</option><option value="active">Активным</option><option value="new">Новым за 30 дней</option></select>
        <button class="btn btn-cyan btn-sm" type="submit">Отправить уведомления 📢</button>
      </form>
      ${newsList.length ? `<div class="glass-card">
        <h3>Новости (${newsList.length})</h3>
        ${newsList.slice(0, 8).map((n) => `<div class="hero-top-row" style="padding:6px 0;border-bottom:1px solid var(--border)">
          <span style="font-size:12px;font-weight:700">${esc(n.title)}</span>
          <button class="btn btn-danger btn-xs" data-action="adm-delete-entity" data-entity="news" data-id="${esc(n.id)}" type="button">Удалить</button>
        </div>`).join('')}
      </div>` : ''}
      ${commentsList.length ? `<div class="glass-card">
        <h3>Последние комментарии (${commentsList.length})</h3>
        ${commentsList.map((c) => `<div class="hero-top-row" style="padding:6px 0;border-bottom:1px solid var(--border);gap:8px">
          <div class="list-meta"><strong>ID ${esc(c.telegram_id)}</strong><small>${esc(c.body)}</small></div>
          <button class="btn btn-danger btn-xs" data-action="adm-delete-entity" data-entity="comment" data-id="${esc(c.id)}" type="button">✕</button>
        </div>`).join('')}
      </div>` : ''}
    </div>`;
  }

  function renderAdminSearchHtml() {
    if (!state.adminSearchResults) return '';
    return state.adminSearchResults.map((g) => g.items.length
      ? `<div class="glass-card" style="margin-bottom:8px"><strong>${esc(g.type.toUpperCase())} (${g.items.length})</strong>${g.items.slice(0, 6).map((x) => `<p class="muted-sm">${esc(x.track_name || x.username || x.first_name || x.title || x.body || x.slug)}</p>`).join('')}</div>`
      : '').join('');
  }

  function openAdminUserModal(uid) {
    const u = (state.adminData?.users || []).find((x) => String(x.telegram_id) === String(uid));
    if (!u) return;
    showModal(`
      <div>
        <div class="eyebrow">ADMIN USER INSPECTOR</div>
        <h2 style="margin-top:4px">${esc(u.first_name || u.username || `ID ${uid}`)}</h2>
        <p class="muted-sm">TG ID: <code class="mono">${esc(uid)}</code> · @${esc(u.username || '—')}</p>
      </div>
      <div class="kpi-grid-4" style="grid-template-columns:repeat(3,1fr)">
        <div class="kpi-box"><strong>${formatNumber(u.wallet_balance || 0)}</strong><span>CXP</span></div>
        <div class="kpi-box"><strong>${u.inventory_count || 0}</strong><span>DROP</span></div>
        <div class="kpi-box"><strong>${u.friends_count || 0}</strong><span>Друзья</span></div>
      </div>
      <div class="glass-card" style="padding:12px">
        <h3>Начислить / списать CXP</h3>
        <div class="wrap-gap">
          <button class="btn btn-success btn-xs" data-action="adm-give-cxp" data-uid="${esc(uid)}" data-delta="100" type="button">+100 CXP</button>
          <button class="btn btn-success btn-xs" data-action="adm-give-cxp" data-uid="${esc(uid)}" data-delta="500" type="button">+500 CXP</button>
          <button class="btn btn-amber btn-xs" data-action="adm-give-cxp" data-uid="${esc(uid)}" data-delta="1000" type="button">+1 000 CXP</button>
          <button class="btn btn-danger btn-xs" data-action="adm-give-cxp" data-uid="${esc(uid)}" data-delta="-100" type="button">-100 CXP</button>
        </div>
      </div>
      <form id="adminUserUpdateForm" class="glass-card" style="padding:12px">
        <input type="hidden" name="telegram_id" value="${esc(uid)}">
        <label class="field"><span>Роль</span>
          <select name="role" class="select-ui">
            <option value="artist" ${u.role === 'artist' ? 'selected' : ''}>artist</option>
            <option value="moderator" ${u.role === 'moderator' ? 'selected' : ''}>moderator</option>
            <option value="admin" ${u.role === 'admin' ? 'selected' : ''}>admin</option>
          </select>
        </label>
        <label class="field"><span>Статус доступа</span>
          <select name="status" class="select-ui">
            <option value="active" ${u.status === 'active' ? 'selected' : ''}>active</option>
            <option value="blocked" ${u.status === 'blocked' ? 'selected' : ''}>blocked</option>
          </select>
        </label>
        <button class="btn btn-primary btn-sm" type="submit">Сохранить настройки юзера</button>
      </form>`);
  }

  /* ==================== BOOTSTRAP ==================== */
  async function loadAll() {
    applyTelegramSafeArea();
    const cachedHome = safeParse(sessionStorage.getItem('cxrner_home_v4'), null);
    const cachedCat = safeParse(sessionStorage.getItem('cxrner_cat_v4'), null);
    if (cachedHome) {
      state.homeData = cachedHome;
      state.isAdmin = Boolean(cachedHome.is_admin);
    }
    if (cachedCat) state.catalogReleases = cachedCat;
    if (cachedHome || cachedCat) {
      renderIcons();
      updateTopbar();
      renderHome();
      renderCatalog();
      $('#loader').classList.add('is-hidden');
      $('#appShell').classList.remove('is-hidden');
    }

    const urlParams = new URLSearchParams(window.location.search);
    const startParam = tg?.initDataUnsafe?.start_param || urlParams.get('startapp') || urlParams.get('ref') || '';

    try {
      const [home, cat, ds] = await Promise.all([
        api('/api/miniapp/platform/home').catch((err) => { state.authFailed = err.status === 403; return null; }),
        api('/api/miniapp/platform/catalog').catch(() => null),
        api('/api/miniapp/drop/state').catch(() => null)
      ]);
      if (home) {
        if (home.build_version && home.build_version !== CLIENT_BUILD_VERSION) {
          const reloadKey = `cxr_reloaded_${home.build_version}`;
          if (!sessionStorage.getItem(reloadKey)) {
            sessionStorage.setItem(reloadKey, '1');
            try {
              const nextUrl = new URL(window.location.href);
              nextUrl.searchParams.set('v', home.build_version);
              window.location.replace(nextUrl.toString());
              return;
            } catch {}
          }
        }
        state.homeData = home;
        state.isAdmin = Boolean(home.is_admin);
        sessionStorage.setItem('cxrner_home_v4', JSON.stringify(home));
      }
      if (cat) {
        state.catalogReleases = cat.releases || [];
        sessionStorage.setItem('cxrner_cat_v4', JSON.stringify(state.catalogReleases));
      }
      if (ds) state.dropState = ds;

      if (/^ref[_-]?\d{4,20}$/i.test(startParam) && getInitData()) {
        api('/api/miniapp/platform/referrals', { method: 'POST', body: { action: 'register_ref', referrer_id: startParam } })
          .then((r) => { if (r?.ref_result?.recorded) toast('🎁 Бонус +50 CXP за приглашение начислен!'); })
          .catch(() => {});
      }
    } finally {
      renderIcons();
      updateTopbar();
      renderHome();
      renderCatalog();
      $('#loader').classList.add('is-hidden');
      $('#appShell').classList.remove('is-hidden');

      const deepScreen = urlParams.get('screen');
      const deepArtist = urlParams.get('artist');
      const deepRelease = urlParams.get('release');
      if (deepScreen && $(`#${deepScreen}`)) switchScreen(deepScreen, false);
      if (deepArtist) openArtistModal(deepArtist);
      else if (deepRelease) openRelease(deepRelease);
    }
  }

  /* ==================== GLOBAL EVENTS ==================== */
  document.addEventListener('click', async (e) => {
    const scr = e.target.closest('[data-screen]');
    if (scr && !e.target.closest('[data-action]')) {
      switchScreen(scr.dataset.screen);
      return;
    }

    const relCard = e.target.closest('[data-release-id]');
    if (relCard && !e.target.closest('button,a')) {
      openRelease(relCard.dataset.releaseId);
      return;
    }

    const copyBtn = e.target.closest('[data-copy]');
    if (copyBtn) {
      copyText(copyBtn.dataset.copy);
      return;
    }

    const extBtn = e.target.closest('[data-external]');
    if (extBtn) {
      openExternal(extBtn.dataset.external);
      return;
    }

    const gChip = e.target.closest('[data-cat-genre]');
    if (gChip) {
      state.catalogGenre = gChip.dataset.catGenre;
      renderCatalog();
      return;
    }

    const preset = e.target.closest('[data-genre]');
    if (preset) {
      $('#wizGenre').value = preset.dataset.genre;
      $$('#wizGenrePresets .preset-chip').forEach((c) => c.classList.toggle('is-active', c === preset));
      return;
    }

    const pTab = e.target.closest('[data-ptab]');
    if (pTab) {
      state.profileTab = pTab.dataset.ptab;
      $$('#profileTabs .seg-btn').forEach((b) => b.classList.toggle('is-active', b === pTab));
      renderProfileTabBody();
      return;
    }

    const dTab = e.target.closest('[data-drop-tab]');
    if (dTab) {
      state.dropTab = dTab.dataset.dropTab;
      $$('#dropHeader .seg-btn').forEach((b) => b.classList.toggle('is-active', b === dTab));
      renderDropBody();
      return;
    }

    const aTab = e.target.closest('[data-atab]');
    if (aTab) {
      state.adminTab = aTab.dataset.atab;
      renderAdminTab();
      return;
    }

    const mTab = e.target.closest('[data-music-tab]');
    if (mTab) {
      setMusicTab(mTab.dataset.musicTab);
      return;
    }

    const rSort = e.target.closest('[data-rank-sort]');
    if (rSort) {
      state.rankSort = rSort.dataset.rankSort;
      $$('#ranking .seg-btn, #musicRankSortTabs .seg-btn').forEach((b) => b.classList.toggle('is-active', b.dataset.rankSort === state.rankSort));
      loadRanking();
      return;
    }

    const actEl = e.target.closest('[data-action]');
    if (!actEl) return;
    const action = actEl.dataset.action;

    if (action === 'open-ranking-tab') {
      state.musicTab = 'ranking';
      switchScreen('catalog');
      return;
    }

    if (action === 'open-friends') {
      state.profileTab = 'friends';
      switchScreen('profile');
      return;
    }

    if (action === 'share-invite') {
      const url = actEl.dataset.url;
      const shareText = encodeURIComponent('Залетай в CXRNER MUSIC! Получи +50 CXP, открывай кейсы DROP и выпускай треки на 120+ площадок');
      const tgShare = `https://t.me/share/url?url=${encodeURIComponent(url)}&text=${shareText}`;
      openExternal(tgShare);
      return;
    }

    if (action === 'claim-milestone') {
      const step = Number(actEl.dataset.step);
      try {
        haptic('medium');
        const res = await api('/api/miniapp/platform/referrals', { method: 'POST', body: { action: 'claim_milestone', step } });
        haptic('success');
        syncWalletState(res.wallet);
        if (state.dropState && Array.isArray(res.inventory)) state.dropState.inventory = res.inventory;
        updateTopbar();
        toast(res.bonus_items?.length ? `🎁 Получено ${res.bonus_items.length} набор(а) DROP!` : `⚡ Получено +${res.reward_cxp} CXP!`);
        await loadProfile();
      } catch (err) {
        haptic('error');
        toast(err.message);
      }
      return;
    }

    if (action === 'engage') {
      const rid = actEl.dataset.id, kind = actEl.dataset.kind;
      const set = kind === 'like' ? state.likedIds : state.favoriteIds;
      const active = !set.has(rid);
      if (active) set.add(rid); else set.delete(rid);
      localStorage.setItem(kind === 'like' ? 'cxrner_likes' : 'cxrner_favs', JSON.stringify([...set]));
      haptic('light');
      const found = state.catalogReleases.find((r) => String(r.id) === rid);
      if (found && kind === 'like') found.likes = Math.max(0, Number(found.likes || 0) + (active ? 1 : -1));
      openRelease(rid);
      api('/api/miniapp/platform/engagement', { method: 'POST', body: { release_id: rid, kind, active } }).catch(() => {});
      return;
    }

    if (action === 'open-artist') { openArtistModal(actEl.dataset.id); return; }

    if (action === 'follow-artist') {
      try {
        await api('/api/miniapp/platform/follow', { method: 'POST', body: { following_id: actEl.dataset.id, active: true } });
        toast('Подписка оформлена');
      } catch (err) { toast(err.message); }
      return;
    }

    if (action === 'toggle-fast-open') {
      state.fastOpen = !state.fastOpen;
      renderDropScreen();
      return;
    }

    if (action === 'open-case') { handleOpenCase(actEl.dataset.caseId); return; }
    if (action === 'replay-case') { closeModal(); handleOpenCase(actEl.dataset.caseId); return; }
    if (action === 'inspect-item') { inspectDropItem(actEl.dataset.id); return; }
    if (action === 'set-inv-filter') { state.dropInvFilter = actEl.dataset.rarity; renderDropBody(); return; }
    if (action === 'select-arena-case') { state.arenaCaseId = actEl.dataset.caseId; renderDropBody(); return; }

    if (action === 'drop-daily') {
      try {
        const res = await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'daily_scan' } });
        haptic('success');
        toast(`⚡ Скан: +${res.reward_cxp} CXP (серия ${res.streak}/7)`);
        await loadDropScreen();
      } catch (err) { toast(err.message); }
      return;
    }

    if (action === 'pin-item') {
      try {
        await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'pin_item', item_id: actEl.dataset.id } });
        toast('Витрина обновлена');
        closeModal();
        await loadDropScreen();
      } catch (err) { toast(err.message); }
      return;
    }

    if (action === 'salvage-item') {
      try {
        const res = await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'salvage', item_id: actEl.dataset.id } });
        haptic('success');
        toast(`+${res.gained_cxp} CXP на баланс`);
        closeModal();
        await loadDropScreen();
      } catch (err) { toast(err.message); }
      return;
    }

    if (action === 'gift-item') {
      try {
        const res = await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'create_gift', item_id: actEl.dataset.id } });
        showModal(`<h3>🎁 Подарок создан</h3><p class="muted-sm">Передай другу код:</p><div class="status-note mono">${esc(res.gift_code)}</div><button class="btn btn-primary btn-block" data-copy="${esc(res.gift_code)}" type="button">Скопировать Gift-код</button>`);
        await loadDropScreen();
      } catch (err) { toast(err.message); }
      return;
    }

    if (action === 'toggle-fuse-item') {
      const id = String(actEl.dataset.id);
      if (state.fuseSelectedIds.includes(id)) {
        state.fuseSelectedIds = state.fuseSelectedIds.filter((x) => x !== id);
        actEl.classList.remove('is-selected');
      } else if (state.fuseSelectedIds.length < 5) {
        state.fuseSelectedIds.push(id);
        actEl.classList.add('is-selected');
      } else {
        toast('Выбери ровно 5 артефактов');
        return;
      }
      const cntEl = $('#fuseCounter');
      const btnEl = $('#fuseSubmitBtn');
      if (cntEl) cntEl.textContent = `${state.fuseSelectedIds.length}/5`;
      if (btnEl) {
        btnEl.disabled = state.fuseSelectedIds.length !== 5;
        btnEl.textContent = `⚗️ Синтезировать (${state.fuseSelectedIds.length}/5)`;
      }
      return;
    }

    if (action === 'drop-fuse-submit') {
      try {
        const res = await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'fuse', item_ids: state.fuseSelectedIds } });
        state.fuseSelectedIds = [];
        haptic('success');
        await loadDropScreen();
        showSpecimenRevealModal(res.forged);
      } catch (err) { toast(err.message); }
      return;
    }

    if (action === 'drop-battle') {
      try {
        haptic('medium');
        const res = await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'battle', case_id: state.arenaCaseId } });
        state.arenaResult = res;
        syncWalletState(res.wallet);
        if (state.dropState) {
          state.dropState.inventory = res.inventory;
        }
        updateTopbar();
        renderDropBody();
        haptic(res.won ? 'success' : 'error');
      } catch (err) { toast(err.message); }
      return;
    }

    if (action === 'edit-profile') {
      const ap = state.profileData?.artist_profile || {};
      showModal(`<h3>Настройка CXRNER ID</h3>
        <form id="editProfileForm" class="stack-list">
          <label class="field"><span>Сценическое имя</span><input name="display_name" value="${esc(ap.display_name || '')}"></label>
          <label class="field"><span>Био</span><textarea name="bio" rows="3">${esc(ap.bio || '')}</textarea></label>
          <label class="field"><span>Город</span><input name="city" value="${esc(ap.city || '')}"></label>
          <label class="field"><span>Рамка аватара</span>
            <select name="equipped_frame" class="select-ui">
              <option value="default" ${ap.equipped_frame === 'default' ? 'selected' : ''}>Стандартная</option>
              <option value="neon" ${ap.equipped_frame === 'neon' ? 'selected' : ''}>Cyan Neon</option>
              <option value="royal" ${ap.equipped_frame === 'royal' ? 'selected' : ''}>Purple Royal</option>
              <option value="gold" ${ap.equipped_frame === 'gold' ? 'selected' : ''}>Founder Gold</option>
            </select>
          </label>
          <button class="btn btn-primary btn-block" type="submit">Сохранить CXRNER ID</button>
        </form>`);
      return;
    }

    if (action === 'edit-release-media') {
      const rel = (state.profileData?.releases || []).find((r) => String(r.id) === String(actEl.dataset.id));
      if (!rel) return;
      showModal(`<h3>Медиа релиза</h3>
        <form id="editReleaseMediaForm" class="stack-list">
          <input type="hidden" name="release_id" value="${esc(rel.id)}">
          <label class="field"><span>URL обложки (HTTPS)</span><input name="cover_url" value="${esc(rel.form_payload?.cover_url || '')}"></label>
          <label class="field"><span>Смарт-ссылка</span><input name="smart_link" value="${esc(rel.form_payload?.smart_link || '')}"></label>
          <button class="btn btn-primary btn-block" type="submit">Сохранить</button>
        </form>`);
      return;
    }

    if (action === 'delete-release') {
      try {
        await api('/api/miniapp/platform/release', { method: 'POST', body: { release_id: actEl.dataset.id, action: 'delete' } });
        toast('Релиз удалён');
        await loadProfile();
      } catch (err) { toast(err.message); }
      return;
    }

    if (action === 'edit-link') {
      const l = state.myLinks.find((x) => String(x.id) === String(actEl.dataset.id));
      if (!l) return;
      $('#linkEditId').value = l.id;
      $('#linkTitle').value = l.title || '';
      $('#linkArtist').value = l.artist || '';
      $('#linkSlug').value = l.slug || '';
      $('#linkTheme').value = l.theme || 'aurora';
      $('#linkCoverUrl').value = l.cover_url || '';
      ['yandex', 'vk', 'spotify', 'apple', 'youtube', 'soundcloud'].forEach((k) => {
        const inp = $(`[name="pl_${k}"]`);
        if (inp) inp.value = l.platforms?.[k] || '';
      });
      $('#linkFormTitle').textContent = 'Редактировать ссылку';
      $('#linkResetBtn').classList.remove('is-hidden');
      window.scrollTo({ top: 0, behavior: 'smooth' });
      return;
    }

    if (action === 'delete-link') {
      try {
        await api('/api/miniapp/link', { method: 'POST', body: { action: 'delete', link_id: actEl.dataset.id } });
        toast('Ссылка удалена');
        loadLinkEditor();
      } catch (err) { toast(err.message); }
      return;
    }

    /* ADMIN ACTIONS */
    if (action === 'adm-mod-status') { state.adminModStatus = actEl.dataset.val; renderAdminTab(); return; }
    if (action === 'adm-mod-source') { state.adminModSource = actEl.dataset.val; renderAdminTab(); return; }

    if (action === 'adm-moderate') {
      try {
        await api('/api/miniapp/platform/admin/moderate', {
          method: 'POST',
          body: { form_id: actEl.dataset.id, action: actEl.dataset.act, telegram_id: actEl.dataset.uid, track_name: actEl.dataset.track }
        });
        haptic('success');
        toast('Статус анкеты обновлён и синхронизирован с группой!');
        await loadAdmin();
      } catch (err) { toast(err.message); }
      return;
    }

    if (action === 'adm-moderate-prompt') {
      const act = actEl.dataset.act;
      const titles = { reject: 'Причина отклонения', needfix: 'Что нужно исправить', upc: 'Присвоить UPC релиза' };
      const fieldNames = { reject: 'reject_reason', needfix: 'moderator_comment', upc: 'upc' };
      showModal(`<h3>${titles[act]}</h3>
        <form id="adminModPromptForm" class="stack-list">
          <input type="hidden" name="form_id" value="${esc(actEl.dataset.id)}">
          <input type="hidden" name="action" value="${esc(act === 'upc' ? 'approve' : act)}">
          <input type="hidden" name="telegram_id" value="${esc(actEl.dataset.uid)}">
          <input type="hidden" name="track_name" value="${esc(actEl.dataset.track)}">
          <input type="hidden" name="field_key" value="${fieldNames[act]}">
          <label class="field"><span>${esc(actEl.dataset.track)}</span>
            <input name="val" required placeholder="${act === 'upc' ? 'Например: 5059460192831' : 'Укажи текст…'}">
          </label>
          <button class="btn btn-primary btn-block" type="submit">Применить</button>
        </form>`);
      return;
    }

    if (action === 'adm-verify') {
      try {
        await api('/api/miniapp/platform/admin/verification', {
          method: 'POST',
          body: { id: actEl.dataset.id, user_id: actEl.dataset.uid, stage_name: actEl.dataset.stage, decision: actEl.dataset.decision }
        });
        haptic('success');
        toast(actEl.dataset.decision === 'approve' ? 'Верификация одобрена!' : 'Заявка отклонена');
        await loadAdmin();
      } catch (err) { toast(err.message); }
      return;
    }

    if (action === 'adm-inspect-user') { openAdminUserModal(actEl.dataset.uid); return; }

    if (action === 'adm-give-cxp') {
      try {
        await api('/api/miniapp/platform/admin/user-bonus', {
          method: 'POST',
          body: { telegram_id: actEl.dataset.uid, delta_cxp: Number(actEl.dataset.delta) }
        });
        haptic('success');
        toast(`Баланс изменён на ${actEl.dataset.delta} CXP`);
        await loadAdmin();
        openAdminUserModal(actEl.dataset.uid);
      } catch (err) { toast(err.message); }
      return;
    }

    if (action === 'adm-delete-entity') {
      try {
        await api('/api/miniapp/platform/admin/action', {
          method: 'POST',
          body: { entity: actEl.dataset.entity, action: 'delete', id: actEl.dataset.id }
        });
        toast('Удалено');
        await loadAdmin();
      } catch (err) { toast(err.message); }
    }
  });

  $('#catalogFavOnly')?.addEventListener('click', () => { state.catalogFavOnly = !state.catalogFavOnly; renderCatalog(); });
  $('#catalogSort')?.addEventListener('change', (e) => { state.catalogSort = e.target.value; renderCatalog(); });
  $('#catalogSearchInput')?.addEventListener('input', (e) => {
    state.catalogSearch = e.target.value;
    $('#catalogSearchClear').classList.toggle('is-hidden', !state.catalogSearch);
    cancelAnimationFrame(searchRafId);
    searchRafId = requestAnimationFrame(() => renderCatalog());
  });
  $('#catalogSearchClear')?.addEventListener('click', () => {
    state.catalogSearch = '';
    $('#catalogSearchInput').value = '';
    $('#catalogSearchClear').classList.add('is-hidden');
    renderCatalog();
  });

  $('#wizNextBtn')?.addEventListener('click', () => { if (validateWizardStep(state.wizardStep)) setWizardStep(state.wizardStep + 1); });
  $('#wizPrevBtn')?.addEventListener('click', () => setWizardStep(state.wizardStep - 1));
  $('#wizCoverUrl')?.addEventListener('input', (e) => {
    const u = e.target.value.trim();
    const wrap = $('#wizCoverPreviewWrap');
    const img = $('#wizCoverPreview');
    if (/^https?:\/\//i.test(u)) {
      img.src = u;
      img.onerror = () => wrap.classList.add('is-hidden');
      wrap.classList.remove('is-hidden');
    } else wrap.classList.add('is-hidden');
  });

  $('#closeReleaseModal')?.addEventListener('click', closeModal);
  $('#releaseModal')?.addEventListener('click', (e) => { if (e.target.id === 'releaseModal') closeModal(); });
  $('#adminRefreshBtn')?.addEventListener('click', () => loadAdmin());

  $('#markAllReadBtn')?.addEventListener('click', async () => {
    try {
      await api('/api/miniapp/platform/notifications/read', { method: 'POST', body: {} });
      (state.homeData?.notifications || []).forEach((n) => { n.read_at = new Date().toISOString(); });
      updateTopbar();
      renderNotifications();
      toast('Все уведомления прочитаны');
    } catch {}
  });

  $('#linkUpcLookupBtn')?.addEventListener('click', async () => {
    const upc = $('#linkUpcLookup').value.trim();
    if (!upc) return;
    try {
      const res = await api(`/api/miniapp/link/lookup?upc=${encodeURIComponent(upc)}`);
      const r = res.release;
      $('#linkTitle').value = r.title || '';
      $('#linkArtist').value = r.artist || '';
      $('#linkCoverUrl').value = r.cover || '';
      Object.entries(r.platforms || {}).forEach(([k, v]) => { const el = $(`[name="pl_${k}"]`); if (el) el.value = v; });
      toast(`Подставлено: ${r.title}`);
    } catch (e) { toast(e.message); }
  });

  $('#linkResetBtn')?.addEventListener('click', () => {
    $('#linkEditorForm').reset();
    $('#linkEditId').value = '';
    $('#linkFormTitle').textContent = 'Создать смарт-ссылку';
    $('#linkResetBtn').classList.add('is-hidden');
  });

  document.addEventListener('input', async (e) => {
    if (e.target.id === 'adminModFilterInput') {
      state.adminModSearch = e.target.value;
      renderAdminTab();
      const inp = $('#adminModFilterInput');
      if (inp) { inp.focus(); inp.setSelectionRange(inp.value.length, inp.value.length); }
      return;
    }
    if (e.target.id === 'adminSearchInput') {
      state.adminSearchQuery = e.target.value;
      const q = e.target.value.trim();
      if (q.length < 2) {
        state.adminSearchResults = null;
        const box = $('#adminSearchResultsBox');
        if (box) box.innerHTML = '';
        return;
      }
      try {
        const res = await api(`/api/miniapp/platform/admin/search?q=${encodeURIComponent(q)}`);
        state.adminSearchResults = res.results || [];
        const box = $('#adminSearchResultsBox');
        if (box) box.innerHTML = renderAdminSearchHtml();
      } catch {}
    }
  });

  document.addEventListener('submit', async (e) => {
    if (e.target.id === 'releaseWizardForm') {
      e.preventDefault();
      if (!validateWizardStep(3)) return;
      const btn = $('#wizSubmitBtn');
      btn.disabled = true;
      btn.textContent = 'Отправка…';
      try {
        const fd = new FormData(e.target);
        const u = tg?.initDataUnsafe?.user || {};
        await api('/api/webapp/submit', {
          method: 'POST',
          body: {
            source: 'mini_app',
            init_data: getInitData(),
            telegram_id: u.id || '',
            username: u.username || '',
            type: fd.get('type'),
            name: fd.get('name'),
            version: fd.get('version'),
            genre: fd.get('genre'),
            date: fd.get('date'),
            nick: fd.get('nick'),
            fio: fd.get('fio'),
            tg: fd.get('tg'),
            lyrics: fd.get('lyrics'),
            link: fd.get('link'),
            cover_url: fd.get('cover_url'),
            tt_time: fd.get('tt_time'),
            yandex: fd.get('yandex'),
            promo: fd.get('promo'),
            comment: fd.get('comment'),
            tiktok: $('#wizTiktok').checked ? 'Да' : 'Нет',
            explicit: $('#wizExplicit').checked ? 'Да' : 'Нет',
            karaoke: $('#wizKaraoke').checked ? 'Да' : 'Нет'
          }
        });
        haptic('success');
        toast('Релиз отправлен и закреплён на модерации! 🚀');
        e.target.reset();
        $('#wizCoverPreviewWrap').classList.add('is-hidden');
        setWizardStep(1);
        await loadAll();
        switchScreen('home');
      } catch (err) { toast(err.message); }
      finally { btn.disabled = false; btn.textContent = 'Отправить релиз 🚀'; }
      return;
    }

    if (e.target.id === 'editProfileForm') {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        await api('/api/miniapp/platform/artist/profile', {
          method: 'POST',
          body: {
            profile: {
              display_name: fd.get('display_name'),
              bio: fd.get('bio'),
              city: fd.get('city'),
              equipped_frame: fd.get('equipped_frame') || 'default'
            }
          }
        });
        closeModal();
        toast('CXRNER ID обновлён');
        await loadProfile();
      } catch (err) { toast(err.message); }
      return;
    }

    if (e.target.id === 'modalCommentForm') {
      e.preventDefault();
      const body = String(new FormData(e.target).get('body') || '').trim();
      if (!body) return;
      try {
        await api('/api/miniapp/platform/comments', { method: 'POST', body: { release_id: state.currentReleaseId, body } });
        e.target.reset();
        loadComments(state.currentReleaseId);
      } catch (err) { toast(err.message); }
      return;
    }

    if (e.target.id === 'editReleaseMediaForm') {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        await api('/api/miniapp/platform/release', {
          method: 'POST',
          body: { release_id: fd.get('release_id'), cover_url: fd.get('cover_url'), smart_link: fd.get('smart_link') }
        });
        closeModal();
        toast('Медиа релиза обновлены');
        loadProfile();
      } catch (err) { toast(err.message); }
      return;
    }

    if (e.target.id === 'dropClaimForm') {
      e.preventDefault();
      const code = String(new FormData(e.target).get('code') || '').trim();
      try {
        const res = await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'claim_code', code } });
        e.target.reset();
        haptic('success');
        toast(res.type === 'promo' ? `Промокод активирован: +${res.cxp} CXP!` : `Подарок получен: ${res.item?.item_name}!`);
        await loadDropScreen();
      } catch (err) { toast(err.message); }
      return;
    }

    if (e.target.id === 'linkEditorForm') {
      e.preventDefault();
      const fd = new FormData(e.target);
      const platforms = {};
      ['yandex', 'vk', 'spotify', 'apple', 'youtube', 'soundcloud'].forEach((k) => {
        const v = String(fd.get(`pl_${k}`) || '').trim();
        if (v) platforms[k] = v;
      });
      const id = String(fd.get('id') || '').trim();
      try {
        await api('/api/miniapp/link', {
          method: 'POST',
          body: {
            action: id ? 'update' : 'create',
            link_id: id || undefined,
            title: fd.get('title'),
            artist: fd.get('artist'),
            slug: fd.get('slug'),
            theme: fd.get('theme'),
            cover_url: fd.get('cover_url'),
            platforms
          }
        });
        e.target.reset();
        $('#linkEditId').value = '';
        $('#linkFormTitle').textContent = 'Создать смарт-ссылку';
        $('#linkResetBtn').classList.add('is-hidden');
        toast('Смарт-ссылка сохранена!');
        loadLinkEditor();
      } catch (err) { toast(err.message); }
      return;
    }

    if (e.target.id === 'verificationForm') {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        await api('/api/miniapp/platform/verification', { method: 'POST', body: Object.fromEntries(fd.entries()) });
        toast('Заявка на верификацию отправлена!');
        if (state.homeData) state.homeData.verification_status = 'pending_review';
        renderVerification();
      } catch (err) { toast(err.message); }
      return;
    }

    if (e.target.id === 'adminModPromptForm') {
      e.preventDefault();
      const fd = new FormData(e.target);
      const body = {
        form_id: fd.get('form_id'),
        action: fd.get('action'),
        telegram_id: fd.get('telegram_id'),
        track_name: fd.get('track_name')
      };
      body[fd.get('field_key')] = fd.get('val');
      try {
        await api('/api/miniapp/platform/admin/moderate', { method: 'POST', body });
        closeModal();
        haptic('success');
        toast('Решение применено и отправлено в модерацию!');
        await loadAdmin();
      } catch (err) { toast(err.message); }
      return;
    }

    if (e.target.id === 'adminUserUpdateForm') {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        await api('/api/miniapp/platform/admin/user-bonus', {
          method: 'POST',
          body: { telegram_id: fd.get('telegram_id'), role: fd.get('role'), status: fd.get('status') }
        });
        haptic('success');
        closeModal();
        toast('Профиль пользователя обновлён');
        await loadAdmin();
      } catch (err) { toast(err.message); }
      return;
    }

    if (e.target.id === 'adminNewsForm') {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        await api('/api/miniapp/platform/admin/action', {
          method: 'POST',
          body: { entity: 'news', action: 'create', values: { title: fd.get('title'), body: fd.get('body'), published: true, published_at: new Date().toISOString() } }
        });
        e.target.reset();
        toast('Новость опубликована');
        await loadAdmin();
      } catch (err) { toast(err.message); }
      return;
    }

    if (e.target.id === 'adminBroadcastForm') {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        const res = await api('/api/miniapp/platform/admin/broadcast', {
          method: 'POST',
          body: { title: fd.get('title'), body: fd.get('body'), audience: fd.get('audience') }
        });
        e.target.reset();
        toast(`Доставлено уведомлений: ${res.delivered}`);
      } catch (err) { toast(err.message); }
    }
  });

  window.addEventListener('load', () => {
    tg = window.Telegram?.WebApp || tg;
    try {
      tg?.ready();
      tg?.expand();
      tg?.setHeaderColor?.('#0B0816');
      tg?.setBackgroundColor?.('#0B0816');
      tg?.disableVerticalSwipes?.();
      tg?.onEvent?.('safeAreaChanged', applyTelegramSafeArea);
      tg?.onEvent?.('contentSafeAreaChanged', applyTelegramSafeArea);
      tg?.onEvent?.('fullscreenChanged', applyTelegramSafeArea);
      tg?.onEvent?.('viewportChanged', applyTelegramSafeArea);
      tg?.BackButton?.onClick(() => {
        if (!$('#unsealOverlay').classList.contains('is-hidden')) {
          $('#unsealSkipBtn')?.click();
        } else if (!$('#releaseModal').classList.contains('is-hidden')) {
          closeModal();
        } else if (state.screen !== 'home') {
          state.history.pop();
          const prev = state.history[state.history.length - 1] || 'home';
          switchScreen(prev === state.screen ? 'home' : prev, false);
        }
      });
    } catch {}
    applyTelegramSafeArea();
    setTimeout(applyTelegramSafeArea, 120);
    setTimeout(applyTelegramSafeArea, 450);
    loadAll();
  });
})();
