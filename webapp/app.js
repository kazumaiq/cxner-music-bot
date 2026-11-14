(() => {
  'use strict';

  let tg = window.Telegram?.WebApp || null;
  const API_PREFIX = location.pathname.startsWith('/miniapp') ? '/miniapp' : '';
  const apiUrl = (path) => (/^https?:\/\//i.test(path) ? path : API_PREFIX && path.startsWith('/api/') ? API_PREFIX + path : path);

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
  const esc = (v) =>
    String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  const ICON_PATHS = {
    home: '<path d="m3 10 9-7 9 7v10a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1z"/><path d="M9 21v-7h6v7"/>',
    music: '<path d="M9 18V5l11-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="17" cy="16" r="3"/>',
    upload: '<path d="M12 16V4m0 0L7 9m5-5 5 5"/><path d="M5 14v5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-5"/>',
    user: '<circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0 1 16 0"/>',
    menu: '<circle cx="5" cy="12" r="1"/><circle cx="12" cy="12" r="1"/><circle cx="19" cy="12" r="1"/>',
    search: '<circle cx="11" cy="11" r="7"/><path d="m20 20-4-4"/>',
    close: '<path d="m6 6 12 12M18 6 6 18"/>',
    arrow: '<path d="M5 12h14m-6-6 6 6-6 6"/>',
    heart: '<path d="M20.8 8.6c0 5.4-8.8 10.4-8.8 10.4S3.2 14 3.2 8.6A4.6 4.6 0 0 1 12 6.4a4.6 4.6 0 0 1 8.8 2.2Z"/>',
    star: '<path d="m12 3 2.8 5.7 6.2.9-4.5 4.4 1.1 6.2-5.6-3-5.6 3 1.1-6.2L3 9.6l6.2-.9z"/>',
    send: '<path d="m22 2-7 20-4-9-9-4Z"/><path d="M22 2 11 13"/>',
    bell: '<path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9"/><path d="M10.3 21a1.9 1.9 0 0 0 3.4 0"/>',
    cube: '<path d="m21 16-9 5-9-5V8l9-5 9 5z"/><path d="m3.3 7 8.7 5 8.7-5M12 22V12"/>',
    link: '<path d="M10 13a5 5 0 0 0 7.5.5l3-3a5 5 0 0 0-7-7l-1.7 1.7"/><path d="M14 11a5 5 0 0 0-7.5-.5l-3 3a5 5 0 0 0 7 7l1.7-1.7"/>',
    shield: '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><path d="m9 12 2 2 4-4"/>',
    trophy: '<path d="M6 9H4a2 2 0 0 1-2-2V5h4m12 4h2a2 2 0 0 0 2-2V5h-4"/><path d="M6 4h12v6a6 6 0 0 1-12 0V4z"/><path d="M12 16v5m-4 0h8"/>',
    calendar: '<rect x="3" y="4" width="18" height="18" rx="3"/><path d="M16 2v4M8 2v4M3 10h18"/>',
    bolt: '<path d="M13 2 3 14h8l-1 8 10-12h-8l1-8z"/>',
    external: '<path d="M14 3h7v7m-11 4L21 3"/><path d="M19 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h6"/>',
    telegram: '<path d="m22 4-3 16-5-3-3 3v-5L22 4 2 11l6 2"/>',
    mail: '<rect x="3" y="5" width="18" height="14" rx="2"/><path d="m3 7 9 6 9-6"/>'
  };

  const DSP_META = {
    yandex: {
      name: 'Яндекс Музыка',
      bg: 'linear-gradient(135deg,#FFDE2E,#FFBC0D)',
      fg: '#0A0A0D',
      svg: '<svg viewBox="0 0 22 22" fill="currentColor"><path d="M21.79,8.39l-.09-.43-3.63-.63,2.11-2.85-.25-.27-3.1,1.49.39-3.95-.32-.18-1.89,3.19L12.91,0h-.37l.5,4.6L7.71.33l-.45.13,4.11,5.16L3.24,2.91l-.37.41,7.26,4.13L.11,8.29l-.11.62,10.41,1.13L1.73,17.22l.37.5,10.33-5.62-2.05,9.9h.63l3.96-9.31,2.41,7.29.43-.33-.99-7.41,3.76,4.27.25-.39-2.88-5.29,4.02,1.49.04-.45-3.6-2.66,3.4-.82Z"/></svg>'
    },
    vk: {
      name: 'VK Музыка',
      bg: '#0D0E14',
      fg: '#FFFFFF',
      svg: '<svg viewBox="0 0 101 100" fill="none" style="transform:scale(1.45)"><path fill-rule="evenodd" clip-rule="evenodd" d="M7.48 7.03C.66 13.85.46 24.7.45 46c19.09 0 31.01-8.2 41.46-15.38C50.08 25 57.35 20 66.45 20c13.55 0 25.38 9.58 31.65 14.66.88.72 1.66 1.34 2.31 1.84C100.21 21.31 99.14 12.75 93.42 7.03 86.39 0 75.08 0 52.45 0h-4C25.83 0 14.51 0 7.48 7.03Z" fill="#0077FF"/><path fill-rule="evenodd" clip-rule="evenodd" d="M.45 46c19.09 0 31.01-8.2 41.46-15.38C50.08 25 57.35 20 66.45 20c13.55 0 25.38 9.58 31.65 14.66.88.72 1.66 1.34 2.31 1.84.04 3.47.04 7.29.04 11.5v4c0 22.63 0 33.94-7.03 40.97-6.72 6.72-17.35 7.02-38.03 7.03h-9.88c-20.68-.01-31.31-.31-38.03-7.03C.45 85.94.45 74.63.45 52v-4c0-.68 0-1.34 0-2Z" fill="#F45FFF"/><path d="M77.95 50.98c0-2.48 2.03-4.48 4.5-4.48s4.5 2 4.5 4.48v18.05c0 2.47-2.03 4.47-4.5 4.47s-4.5-2-4.5-4.47V50.98Zm-16-11.5c0-2.48 2.03-4.48 4.5-4.48s4.5 2 4.5 4.48v41.05c0 2.47-2.03 4.47-4.5 4.47s-4.5-2-4.5-4.47V39.48Zm-16 11.5c0-2.48 2.03-4.48 4.5-4.48s4.5 2 4.5 4.48v18.05c0 2.47-2.03 4.47-4.5 4.47s-4.5-2-4.5-4.47V50.98Zm-16 6.5c0-2.48 2.03-4.48 4.5-4.48s4.5 2 4.5 4.48v5.05c0 2.47-2.03 4.47-4.5 4.47s-4.5-2-4.5-4.47v-5.05Zm-16 0c0-2.48 2.03-4.48 4.5-4.48s4.5 2 4.5 4.48v5.05c0 2.47-2.03 4.47-4.5 4.47s-4.5-2-4.5-4.47v-5.05Z" fill="#FFF"/></svg>'
    },
    spotify: {
      name: 'Spotify',
      bg: 'linear-gradient(135deg,#1ED760,#169C46)',
      fg: '#06150B',
      svg: '<svg viewBox="68 145 370 245" fill="currentColor"><path d="M406.6 231.1c-5.2 0-8.4-1.3-12.9-3.9-71.2-42.5-198.5-52.7-280.9-29.7-3.6 1-8.1 2.6-12.9 2.6-13.2 0-23.3-10.3-23.3-23.6 0-13.6 8.4-21.3 17.4-23.9 35.2-10.3 74.6-15.2 117.5-15.2 73 0 149.5 15.2 205.4 47.8 7.8 4.5 12.9 10.7 12.9 22.6 0 13.6-11 23.3-23.2 23.3zm-31 76.2c-5.2 0-8.7-2.3-12.3-4.2-62.5-37-155.7-51.9-238.6-29.4-4.8 1.3-7.4 2.6-11.9 2.6-10.7 0-19.4-8.7-19.4-19.4s5.2-17.8 15.5-20.7c27.8-7.8 56.2-13.6 97.8-13.6 64.9 0 127.6 16.1 177 45.5 8.1 4.8 11.3 11 11.3 19.7-.1 10.8-8.5 19.5-19.4 19.5zm-26.9 65.6c-4.2 0-6.8-1.3-10.7-3.6-62.4-37.6-135-39.2-206.7-24.5-3.9 1-9 2.6-11.9 2.6-9.7 0-15.8-7.7-15.8-15.8 0-10.3 6.1-15.2 13.6-16.8 81.9-18.1 165.6-16.5 237 26.2 6.1 3.9 9.7 7.4 9.7 16.5s-7.1 15.4-15.2 15.4z"/></svg>'
    },
    apple: {
      name: 'Apple Music',
      bg: 'linear-gradient(135deg,#FB5C74,#FA233B)',
      fg: '#FFFFFF',
      svg: '<svg viewBox="3.5 2.5 16.5 18.5" fill="currentColor"><path d="M17.571 10.114v5.712c0 .417-.058.827-.244 1.206-.29.59-.76.962-1.388 1.14-.35.1-.706.157-1.07.173-.95.045-1.773-.6-1.943-1.536a1.88 1.88 0 011.038-2.022c.323-.16.67-.25 1.018-.324.378-.082.758-.153 1.134-.24.274-.063.457-.23.51-.516a.904.904 0 00.02-.193c0-1.815 0-3.63-.002-5.443a.725.725 0 00-.026-.185c-.04-.15-.15-.243-.304-.234-.16.01-.318.035-.475.066-.76.15-1.52.303-2.28.456l-2.325.47-1.374.278c-.016.003-.032.01-.048.013-.277.077-.377.203-.39.49-.002.042 0 .086 0 .13-.002 2.602 0 5.204-.003 7.805 0 .42-.047.836-.215 1.227-.278.64-.77 1.04-1.434 1.233-.35.1-.71.16-1.075.172-.96.036-1.755-.6-1.92-1.544-.14-.812.23-1.685 1.154-2.075.357-.15.73-.232 1.108-.31.287-.06.575-.116.86-.177.383-.083.583-.323.6-.714v-.15c0-2.96 0-5.922.002-8.882 0-.123.013-.25.042-.37.07-.285.273-.448.546-.518.255-.066.515-.112.774-.165.733-.15 1.466-.296 2.2-.444l2.27-.46c.67-.134 1.34-.27 2.01-.403.22-.043.442-.088.663-.106.31-.025.523.17.554.482.008.073.012.148.012.223.002 1.91.002 3.822 0 5.732z"/></svg>'
    },
    youtube: {
      name: 'YouTube Music',
      bg: 'linear-gradient(135deg,#FF1A1A,#CC0000)',
      fg: '#FFFFFF',
      svg: '<svg viewBox="2.5 2.5 19 19" fill="currentColor"><path d="M12 3.2a8.8 8.8 0 1 0 0 17.6 8.8 8.8 0 0 0 0-17.6Zm0 15.8a7 7 0 1 1 0-14 7 7 0 0 1 0 14Zm-2.32-10.54v7.08L15.82 12 9.68 8.46Z"/></svg>'
    },
    deezer: {
      name: 'Deezer',
      bg: 'linear-gradient(135deg,#A335FF,#7B1FA2)',
      fg: '#FFFFFF',
      svg: '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M.693 10.024c.381 0 .693-1.256.693-2.807 0-1.55-.312-2.807-.693-2.807C.312 4.41 0 5.666 0 7.217s.312 2.808.693 2.808ZM21.038 1.56c-.364 0-.684.805-.91 2.096C19.765 1.446 19.184 0 18.526 0c-.78 0-1.464 2.036-1.784 5-.312-2.158-.788-3.536-1.325-3.536-.745 0-1.386 2.704-1.62 6.472-.442-1.932-1.083-3.145-1.793-3.145s-1.35 1.213-1.793 3.145c-.242-3.76-.874-6.463-1.628-6.463-.537 0-1.013 1.378-1.325 3.535C6.938 2.036 6.262 0 5.474 0c-.658 0-1.247 1.447-1.602 3.665-.217-1.291-.546-2.105-.91-2.105-.675 0-1.221 2.807-1.221 6.272 0 3.466.546 6.273 1.221 6.273.277 0 .537-.476.736-1.273.32 2.928.996 4.938 1.776 4.938.606 0 1.143-1.204 1.507-3.11.251 3.622.875 6.195 1.602 6.195.46 0 .875-1.023 1.187-2.677C10.142 21.6 11 24 12.004 24c1.005 0 1.863-2.4 2.235-5.822.312 1.654.727 2.677 1.186 2.677.728 0 1.352-2.573 1.603-6.195.364 1.906.9 3.11 1.507 3.11.78 0 1.455-2.01 1.775-4.938.208.797.46 1.273.737 1.273.675 0 1.22-2.807 1.22-6.273-.008-3.457-.553-6.272-1.23-6.272ZM23.307 10.024c.381 0 .693-1.256.693-2.807 0-1.55-.312-2.807-.693-2.807-.381 0-.693 1.256-.693 2.807s.312 2.808.693 2.808Z"/></svg>'
    },
    zvuk: {
      name: 'Звук',
      bg: 'linear-gradient(135deg,#1E202B,#08090C)',
      fg: '#FFFFFF',
      svg: '<svg viewBox="11 11 93 93" fill="currentColor"><path fill-rule="evenodd" clip-rule="evenodd" d="M48.76 97.73c.28 1.37.42 2.05 0 2.53-.43.48-1.14.42-2.57.29-7.97-.71-12.43-2.17-15.92-3.62-5.8-2.4-10.41-7.01-12.82-12.82-1.44-3.48-2.9-7.95-3.62-15.92-.12-1.42-.19-2.14.29-2.56.48-.43 1.16-.29 2.53-.01 16.1 3.34 28.78 16.01 32.11 32.11Zm15.42 3.11c-1.11.05-1.67.08-2.05-.24-.38-.31-.46-.89-.62-2.04C58.17 74.92 39.46 56.22 15.82 52.88c-1.15-.17-1.73-.25-2.04-.63-.32-.38-.29-.93-.24-2.04.22-4.64.66-8.29 1.22-11.26.19-.98.28-1.47.67-1.76.38-.3.88-.26 1.89-.19 32.06 2.41 57.65 28.01 60.06 60.07.08 1 .12 1.5-.18 1.89-.3.38-.79.48-1.77.66-2.97.57-6.62 1-11.25 1.22Zm28.74-9.81c-1.26 1.38-1.89 2.07-2.66 1.84-.77-.23-.93-1.25-1.26-3.29C83.69 56.68 57.71 30.69 24.81 25.39c-2.04-.33-3.07-.5-3.29-1.26-.23-.77.46-1.4 1.84-2.66 2.01-1.84 4.34-3.34 6.91-4.4 4.86-2.01 11.61-4.06 27.11-4.06 14.62 0 21.45 1.82 26.26 3.71-4.44 3.2-7.34 8.42-7.34 14.31 0 9.74 7.9 17.63 17.63 17.63h7.21c.15 2.46.23 5.23.23 8.34 0 15.51-2.05 22.26-4.06 27.12-1.06 2.56-2.56 4.9-4.39 6.91Z"/></svg>'
    },
    soundcloud: {
      name: 'SoundCloud',
      bg: 'linear-gradient(135deg,#FF7700,#FF3300)',
      fg: '#FFFFFF',
      svg: '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M24 14.17c-.05 1.79-1.61 3.16-3.4 3.16h-8.18a.68.68 0 0 1-.68-.68V7.86c0-.3.17-.57.45-.72 0 0 .75-.51 2.33-.51a5.4 5.4 0 0 1 2.77.75 5.44 5.44 0 0 1 2.57 3.54 3.3 3.3 0 0 1 4.14 3.25ZM10.72 8.42c.25 2.98.43 5.7 0 8.67a.26.26 0 0 1-.53 0c-.4-2.94-.22-5.72 0-8.67a.26.26 0 0 1 .53 0Zm-1.65 1.03c.29 2.66.37 4.98 0 7.65a.28.28 0 0 1-.55 0c-.33-2.63-.26-5.02 0-7.65a.28.28 0 0 1 .55 0Zm-1.66-.26c.27 2.73.39 5.17 0 7.91a.27.27 0 0 1-.53 0c-.38-2.69-.26-5.21 0-7.91a.27.27 0 0 1 .53 0Zm-1.65.77a26 26 0 0 1 0 7.15.27.27 0 0 1-.54 0 28 28 0 0 1 0-7.15.28.28 0 0 1 .54 0Zm-1.67 1.77c.42 1.86.23 3.5-.03 5.39a.26.26 0 0 1-.51 0c-.21-1.86-.4-3.55 0-5.39a.27.27 0 0 1 .54 0Z"/></svg>'
    }
  };

  const state = {
    screen: 'home',
    history: ['home'],
    releases: [],
    homeData: null,
    profile: null,
    profileTab: 'overview',
    dropState: null,
    dropTab: 'cases',
    fusionSelection: [],
    search: '',
    genre: 'all',
    sort: 'newest',
    onlyFavorites: false,
    step: 1,
    currentReleaseId: '',
    links: [],
    editingLink: null
  };

  function initData() {
    return tg?.initData || new URLSearchParams(location.search).get('initData') || '';
  }

  function applyTelegramSafeArea() {
    const contentTop = Number(tg?.contentSafeAreaInset?.top || 0);
    const sysTop = Number(tg?.safeAreaInset?.top || 0);
    const sysBottom = Number(tg?.safeAreaInset?.bottom || 0);
    const totalTop = Math.max(8, contentTop + sysTop + (tg?.isFullscreen ? 6 : 0));
    document.documentElement.style.setProperty('--app-top-pad', `${totalTop}px`);
    document.documentElement.style.setProperty('--app-bottom-pad', `${Math.max(10, sysBottom)}px`);
  }

  function syncBackButton() {
    if (!tg?.BackButton) return;
    const modalOpen = !$('#releaseModal').classList.contains('is-hidden');
    if (modalOpen || state.screen !== 'home') tg.BackButton.show();
    else tg.BackButton.hide();
  }

  function haptic(type = 'light') {
    try { tg?.HapticFeedback?.impactOccurred(type); } catch {}
  }

  function toast(text) {
    const el = $('#toast');
    if (!el) return;
    el.textContent = text;
    el.classList.remove('is-hidden');
    clearTimeout(window.__toastTimer);
    window.__toastTimer = setTimeout(() => el.classList.add('is-hidden'), 3200);
  }

  async function api(url, options = {}) {
    const headers = { ...(options.headers || {}), 'x-telegram-init-data': initData() };
    const cfg = { ...options, headers };
    if (options.body && typeof options.body !== 'string') {
      headers['content-type'] = 'application/json';
      cfg.body = JSON.stringify(options.body);
    }
    const response = await fetch(apiUrl(url), cfg);
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) {
      throw new Error(data.error || 'Не удалось выполнить запрос');
    }
    return data;
  }

  function generateCoverSvg(title, artist, genre) {
    const seed = `${title || 'CXRNER'}:${artist || ''}:${genre || ''}`;
    let h = 2166136261;
    for (let i = 0; i < seed.length; i += 1) {
      h ^= seed.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    const palettes = [
      ['#3B0764', '#0B0F1A', '#00F0FF'],
      ['#1E1B4B', '#090D16', '#C084FC'],
      ['#450A0A', '#0B0912', '#F59E0B'],
      ['#064E3B', '#070B12', '#34D399'],
      ['#311042', '#0A0814', '#F43F5E'],
      ['#0C4A6E', '#080B12', '#38BDF8']
    ];
    const pal = palettes[Math.abs(h) % palettes.length];
    const letters = esc((String(title || 'CX').replace(/[^A-Za-zА-Яа-я0-9]/g, '').slice(0, 2) || 'CX').toUpperCase());
    const ringR = 52 + (Math.abs(h >> 4) % 36);
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 320"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="${pal[0]}"/><stop offset="100%" stop-color="${pal[1]}"/></linearGradient></defs><rect width="320" height="320" fill="url(#g)"/><circle cx="160" cy="160" r="118" fill="none" stroke="rgba(255,255,255,0.12)" stroke-width="1.5"/><circle cx="160" cy="160" r="${ringR}" fill="none" stroke="${pal[2]}" stroke-opacity="0.48" stroke-width="2"/><line x1="28" y1="28" x2="88" y2="28" stroke="${pal[2]}" stroke-width="2" stroke-opacity="0.7"/><text x="28" y="294" fill="rgba(255,255,255,0.45)" font-family="monospace" font-size="13" letter-spacing="2">CXRNER // ARCHIVE</text><text x="160" y="178" text-anchor="middle" fill="#FFFFFF" font-family="sans-serif" font-weight="800" font-size="76" letter-spacing="-2">${letters}</text></svg>`;
    return 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svg);
  }

  function coverUrl(item) {
    const raw = String(item?.cover || item?.cover_url || item?.raw?.cover_url || '').trim();
    if (raw && !raw.endsWith('general.jpg')) return raw;
    return generateCoverSvg(item?.title || item?.track_name || item?.name, item?.artist || item?.artist_name, item?.genre);
  }

  function renderIcons(root = document) {
    $$('[data-icon]', root).forEach((el) => {
      const p = ICON_PATHS[el.dataset.icon];
      if (p && !el.querySelector('svg')) el.innerHTML = `<svg viewBox="0 0 24 24">${p}</svg>`;
    });
    $$('[data-nav-icon]', root).forEach(() => {});
    $$('.nav-ico[data-icon], .nav-ico-fab[data-icon], .q-ico[data-icon], .more-ico[data-icon]', root).forEach((el) => {
      const p = ICON_PATHS[el.dataset.icon];
      if (p && !el.querySelector('svg')) el.innerHTML = `<svg viewBox="0 0 24 24">${p}</svg>`;
    });
  }

  function statusText(s) {
    return ({
      approved: 'Одобрено',
      published: 'Опубликовано',
      rejected: 'Отклонено',
      needs_fix: 'Нужны правки',
      moderation: 'На проверке',
      on_moderation: 'На проверке',
      on_upload: 'На отгрузке',
      pending: 'В очереди'
    }[String(s || '').toLowerCase()] || s || 'Статус');
  }

  function updateTopbar() {
    const wallet = state.dropState?.wallet || state.homeData?.wallet || state.profile?.cabinet?.wallet || {};
    const bal = typeof wallet.balance === 'number' ? wallet.balance : '—';
    const cxpEl = $('#topbarCxpValue');
    if (cxpEl) cxpEl.textContent = typeof bal === 'number' ? Number(bal).toLocaleString('ru-RU') : bal;

    const u = state.profile?.user || state.homeData?.user || tg?.initDataUnsafe?.user || {};
    const ep = state.profile?.artist_profile || state.homeData?.site_profile || {};
    const uname = String(u.username || '').toLowerCase();
    const isCeo = uname === 'kazumaiq' || Boolean(state.profile?.is_admin || state.homeData?.is_admin);
    const avaBtn = $('#topbarAvatarBtn');
    if (avaBtn) avaBtn.classList.toggle('is-ceo', isCeo);

    const img = $('#topbarAvatarImg');
    const mono = $('#topbarAvatarFallback');
    const photo = ep.avatar_url || u.photo_url || '';
    if (photo) {
      img.src = photo;
      img.classList.remove('is-hidden');
      mono.classList.add('is-hidden');
      img.onerror = () => {
        img.classList.add('is-hidden');
        mono.textContent = (ep.display_name || u.first_name || 'CX').slice(0, 2).toUpperCase();
        mono.classList.remove('is-hidden');
      };
    } else {
      img.classList.add('is-hidden');
      mono.textContent = (ep.display_name || u.first_name || 'CX').slice(0, 2).toUpperCase();
      mono.classList.remove('is-hidden');
    }

    const notifs = state.homeData?.notifications || state.profile?.notifications || [];
    const unread = notifs.some((n) => !n.read_at && !n.is_read);
    $('#topbarBellDot')?.classList.toggle('is-hidden', !unread);
  }

  function switchScreen(screen, pushHistory = true) {
    if (!screen || !$('#' + screen)) return;
    if (pushHistory && state.screen !== screen) state.history.push(screen);
    state.screen = screen;
    $$('.screen').forEach((x) => x.classList.toggle('is-active', x.id === screen));
    const navMap = { drop: 'more', link: 'more', verification: 'more', notifications: 'more', ranking: 'more', calendar: 'more', activity: 'more', admin: 'more' };
    const navTarget = navMap[screen] || screen;
    $$('.nav-item').forEach((x) => x.classList.toggle('is-active', x.dataset.screen === navTarget));
    window.scrollTo({ top: 0, behavior: 'smooth' });
    syncBackButton();
    renderIcons();

    if (screen === 'catalog') renderCatalog();
    if (screen === 'profile') loadProfile();
    if (screen === 'drop') loadDropScreen();
    if (screen === 'link') loadLinkEditor();
    if (screen === 'verification') renderVerification();
    if (screen === 'notifications') renderNotifications();
    if (screen === 'ranking') loadRanking();
    if (screen === 'calendar') loadCalendar();
    if (screen === 'activity') loadActivity();
    if (screen === 'admin') loadAdmin();
    if (screen === 'submit' && state.step === 5) renderWizardSummary();
  }

  function releaseCardHtml(item, horizontal = false) {
    const cover = esc(coverUrl(item));
    return `<article class="release-card" data-release-id="${esc(item.id)}" style="${horizontal ? 'min-width:154px' : ''}">
      <div class="cover-wrap">
        <img src="${cover}" alt="${esc(item.title)}" loading="lazy">
        <span class="status-badge status-${esc(item.status)}">${esc(statusText(item.status))}</span>
      </div>
      <div class="release-meta">
        <h3>${esc(item.title)}</h3>
        <p class="artist-line">${esc(item.artist)}</p>
        <div class="release-foot">
          <span class="genre-pill">${esc(item.genre || 'Music')}</span>
          <span>♥ ${Number(item.likes || 0)}</span>
        </div>
      </div>
    </article>`;
  }

  /* ==================== 01. HOME ==================== */
  function renderHome() {
    const hd = state.homeData || {};
    const u = hd.user || tg?.initDataUnsafe?.user || {};
    const ep = hd.site_profile || {};
    const uname = String(u.username || '').toLowerCase();
    const isCeo = uname === 'kazumaiq';
    const isAdmin = Boolean(hd.is_admin || state.profile?.is_admin);

    const name = ep.display_name || u.first_name || u.username || 'артист';
    $('#homeGreeting').textContent = `Привет, ${name}`;

    const forms = Array.isArray(hd.forms) ? hd.forms : [];
    const latestOwn = forms[0];
    $('#homeLastRelease').textContent = latestOwn
      ? `Последний релиз: ${latestOwn.track_name || 'Без названия'} (${statusText(latestOwn.status)})`
      : 'Готов к новому релизу и коллекционированию';

    const badges = [];
    if (isCeo) badges.push('<span class="role-badge badge-ceo">👑 FOUNDER & CEO</span>');
    else if (isAdmin) badges.push('<span class="role-badge badge-ceo">ADMIN</span>');
    if (hd.verification_status === 'verified' || isCeo) badges.push('<span class="role-badge badge-verified">✓ VERIFIED</span>');
    else badges.push('<span class="role-badge badge-artist">ARTIST</span>');
    $('#homeRoleBadges').innerHTML = badges.join('');

    $('#homeStatReleases').textContent = String(hd.stats?.releases ?? forms.length ?? 0);
    $('#homeStatModeration').textContent = String(hd.stats?.moderation ?? 0);
    $('#homeStatLinks').textContent = String(hd.stats?.links ?? 0);
    $('#homeStatItems').textContent = String(hd.inventory_count ?? 0);

    let comp = 40;
    if (ep.display_name || u.first_name) comp += 15;
    if (ep.bio) comp += 15;
    if (ep.avatar_url || u.photo_url) comp += 15;
    if (forms.length > 0) comp += 15;
    comp = Math.min(100, isCeo ? 100 : comp);
    $('#homeCompletionPct').textContent = `${comp}%`;
    $('#homeCompletionFill').style.width = `${comp}%`;

    const banners = [];
    if (hd.can_daily_scan) {
      banners.push(`<button class="promo-banner" data-action="quick-daily-scan" type="button">
        <div><b>📡 Daily Archive Scan доступен</b><small>Забери ежедневный бонус CXP для открытия кейсов</small></div>
        <span class="promo-pill">+35 CXP</span>
      </button>`);
    }
    $('#homeActionBanners').innerHTML = banners.join('');

    const latest = Array.isArray(hd.latest) && hd.latest.length ? hd.latest : state.releases;
    const pop = Array.isArray(hd.popular) && hd.popular.length ? hd.popular : [...latest].sort((a, b) => (b.likes || 0) - (a.likes || 0));

    if (latest[0]) {
      $('#homeTickerText').textContent = `LIVE // ${latest[0].artist} — «${latest[0].title}» · В каталоге ${state.releases.length} релизов · CXRNER DROP активен`;
    }

    $('#homeReleases').innerHTML = latest.length
      ? latest.slice(0, 10).map((r) => releaseCardHtml(r, true)).join('')
      : '<p class="muted-text">Одобренные релизы скоро появятся.</p>';

    $('#homePopular').innerHTML = pop.length
      ? pop.slice(0, 8).map((r) => releaseCardHtml(r, true)).join('')
      : '';

    const artistMap = new Map();
    for (const r of state.releases) {
      if (!r.telegram_id || artistMap.has(String(r.telegram_id))) continue;
      artistMap.set(String(r.telegram_id), { id: String(r.telegram_id), name: r.artist, releases: 1 });
    }
    const artists = [...artistMap.values()].slice(0, 10);
    $('#homeArtists').innerHTML = artists.length
      ? artists
          .map(
            (a) => `<button class="artist-chip-card" data-artist-id="${esc(a.id)}" type="button">
              <span class="artist-ava">${esc(a.name.slice(0, 2).toUpperCase())}</span>
              <span><b>${esc(a.name)}</b><small>Профиль ↗</small></span>
            </button>`
          )
          .join('')
      : '<p class="muted-text">Создатели каталога загружаются.</p>';

    const news = Array.isArray(hd.news) ? hd.news : [];
    $('#homeNews').innerHTML = news.length
      ? news
          .map(
            (n) => `<article class="news-card"><b>${esc(n.title)}</b><p>${esc(n.body)}</p><small>${new Date(n.published_at || n.created_at || Date.now()).toLocaleDateString('ru-RU')}</small></article>`
          )
          .join('')
      : '<div class="news-card"><b>CXRNER MINI APP 3.0</b><p>Обновлены навигация, 5-шаговый мастер релиза, CXRNER DROP Arena и официальные иконки площадок.</p><small>СЕГОДНЯ</small></div>';
  }

  /* ==================== 02. CATALOG ==================== */
  function getFavoritesSet() {
    try { return new Set(JSON.parse(localStorage.getItem('cxrner_fav_ids') || '[]')); } catch { return new Set(); }
  }
  function getRecentViewed() {
    try { return JSON.parse(localStorage.getItem('cxrner_recent_views') || '[]'); } catch { return []; }
  }
  function pushRecentViewed(rel) {
    try {
      const prev = getRecentViewed().filter((x) => x.id !== rel.id);
      prev.unshift({ id: rel.id, title: rel.title, artist: rel.artist, cover: coverUrl(rel) });
      localStorage.setItem('cxrner_recent_views', JSON.stringify(prev.slice(0, 8)));
    } catch {}
  }

  function renderCatalog() {
    const genres = ['all', ...new Set(state.releases.map((r) => String(r.genre || '').trim()).filter(Boolean))].slice(0, 14);
    $('#genreChips').innerHTML = genres
      .map((g) => `<button class="chip-btn ${state.genre === g ? 'is-active' : ''}" data-genre-chip="${esc(g)}" type="button">${esc(g === 'all' ? 'Все жанры' : g)}</button>`)
      .join('');

    const favSet = getFavoritesSet();
    const q = state.search.trim().toLowerCase();
    let list = state.releases.filter((r) => {
      if (state.genre !== 'all' && r.genre !== state.genre) return false;
      if (state.onlyFavorites && !favSet.has(String(r.id))) return false;
      if (!q) return true;
      return `${r.title} ${r.artist} ${r.genre}`.toLowerCase().includes(q);
    });

    if (state.sort === 'title') list = [...list].sort((a, b) => String(a.title).localeCompare(String(b.title), 'ru'));
    if (state.sort === 'popular') list = [...list].sort((a, b) => (b.likes || 0) - (a.likes || 0));

    $('#catalogCount').textContent = `${list.length} релизов`;
    $('#catalogFavToggle').classList.toggle('is-active', state.onlyFavorites);

    const recent = getRecentViewed();
    $('#recentViewedWrap').classList.toggle('is-hidden', !recent.length);
    $('#recentViewedList').innerHTML = recent
      .map(
        (x) => `<button class="mini-rail-card" data-release-id="${esc(x.id)}" type="button">
          <img src="${esc(x.cover)}" alt=""><span><b>${esc(x.title)}</b><small style="font-size:10px;color:var(--muted)">${esc(x.artist)}</small></span>
        </button>`
      )
      .join('');

    $('#catalogGrid').innerHTML = list.length
      ? list.map((r) => releaseCardHtml(r, false)).join('')
      : '<div class="glass-card" style="grid-column:1/-1;text-align:center">Ничего не найдено по фильтрам.</div>';
  }

  function openModal(html) {
    $('#modalContent').innerHTML = html;
    $('#releaseModal').classList.remove('is-hidden');
    renderIcons($('#releaseModal'));
    syncBackButton();
  }
  function closeModal() {
    $('#releaseModal').classList.add('is-hidden');
    $('#modalContent').innerHTML = '';
    syncBackButton();
  }

  async function openRelease(id) {
    const rel = state.releases.find((r) => String(r.id) === String(id)) || state.homeData?.latest?.find((r) => String(r.id) === String(id));
    if (!rel) return;
    state.currentReleaseId = String(rel.id);
    pushRecentViewed(rel);

    const dspEntries = Object.entries(rel.dsp_links || {}).filter(([, url]) => url && String(url).length > 4);
    const dspHtml = dspEntries
      .map(([k, url]) => {
        const meta = DSP_META[k.toLowerCase()] || { name: k, bg: 'linear-gradient(135deg,#8737FF,#5B21B6)', fg: '#fff', svg: '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 3v10.55A4 4 0 1 0 14 17V7h4V3h-6Z"/></svg>' };
        return `<a class="dsp-btn" href="${esc(url)}" target="_blank" rel="noopener" data-track-listen="${esc(rel.id)}">
          <span class="dsp-logo-box" style="background:${meta.bg};color:${meta.fg}">${meta.svg}</span>
          <span><b>${esc(meta.name)}</b><small>OFFICIAL DSP</small></span>
          <span class="dsp-cta">Слушать ↗</span>
        </a>`;
      })
      .join('');

    const favSet = getFavoritesSet();
    const isFav = favSet.has(String(rel.id));

    openModal(`
      <div class="stack-gap" style="padding-top:6px">
        <div style="display:flex;gap:14px;align-items:center">
          <img src="${esc(coverUrl(rel))}" alt="" style="width:94px;height:94px;border-radius:16px;object-fit:cover;border:1px solid var(--line)">
          <div style="min-width:0;flex:1">
            <span class="status-badge status-${esc(rel.status)}" style="position:static">${esc(statusText(rel.status))}</span>
            <h2 style="font-family:'Unbounded',sans-serif;font-size:18px;margin:6px 0 2px">${esc(rel.title)}</h2>
            <button data-artist-id="${esc(rel.telegram_id)}" style="font-size:13px;font-weight:700;color:var(--cyan)" type="button">${esc(rel.artist)} ↗</button>
            <p class="muted-text mono-num" style="font-size:11px">${esc([rel.genre, rel.type, rel.date].filter(Boolean).join(' · '))}</p>
          </div>
        </div>
        ${rel.description ? `<p style="margin:0;font-size:13px;color:#CBD5E1">${esc(rel.description)}</p>` : ''}
        <div class="cta-row">
          <button class="btn btn-ghost" data-engage="like" data-release-id="${esc(rel.id)}" type="button">♥ Лайк (${Number(rel.likes || 0)})</button>
          <button class="btn btn-ghost" data-engage="favorite" data-release-id="${esc(rel.id)}" type="button">${isFav ? '★ В избранном' : '☆ В избранное'}</button>
        </div>
        ${rel.smart_link ? `<a class="btn btn-primary full-w" href="${esc(rel.smart_link)}" target="_blank" rel="noopener">Открыть Smart Link ↗</a>` : ''}
        ${dspHtml ? `<div class="mini-label">ОФИЦИАЛЬНЫЕ ПЛОЩАДКИ</div><div class="dsp-list">${dspHtml}</div>` : ''}
        <div class="mini-label">КОММЕНТАРИИ</div>
        <form id="modalCommentForm" style="display:flex;gap:8px">
          <input class="input-box" name="body" required placeholder="Написать отзыв на трек…" style="flex:1">
          <button class="btn btn-primary btn-sm" type="submit">Ок</button>
        </form>
        <div id="modalCommentsList" class="stack-gap"><p class="muted-text">Загружаем комментарии…</p></div>
      </div>
    `);

    loadComments(rel.id);
  }

  async function loadComments(releaseId) {
    try {
      const data = await api(`/api/miniapp/platform/comments?release_id=${encodeURIComponent(releaseId)}`);
      const list = $('#modalCommentsList');
      if (!list) return;
      const rows = data.comments || [];
      if (!rows.length) {
        list.innerHTML = '<p class="muted-text">Пока нет комментариев — будь первым.</p>';
        return;
      }
      const myUid = String(state.profile?.user?.telegram_id || tg?.initDataUnsafe?.user?.id || '');
      const isAdmin = Boolean(state.profile?.is_admin || state.homeData?.is_admin);
      list.innerHTML = rows
        .map((c) => {
          const author = c.author?.first_name || c.author?.username || `ID ${c.telegram_id}`;
          return `<div class="row-item" style="align-items:flex-start">
            <div style="flex:1">
              <b>${c.is_pinned ? '📌 ' : ''}${esc(author)}</b>
              <p style="margin:3px 0 0;font-size:12px;color:#E2E8F0">${esc(c.body)}</p>
            </div>
            <div style="display:flex;gap:5px">
              ${isAdmin ? `<button class="btn btn-ghost btn-sm" data-pin-comment="${esc(c.id)}" data-pinned="${!c.is_pinned}" type="button">📌</button>` : ''}
              ${String(c.telegram_id) === myUid ? `<button class="btn btn-danger btn-sm" data-del-comment="${esc(c.id)}" type="button">✕</button>` : ''}
            </div>
          </div>`;
        })
        .join('');
    } catch {
      const list = $('#modalCommentsList');
      if (list) list.innerHTML = '<p class="muted-text">Комментарии временно недоступны.</p>';
    }
  }

  async function openArtistModal(artistId) {
    if (!artistId) return;
    openModal('<div class="skeleton" style="height:180px"></div>');
    try {
      const data = await api(`/api/miniapp/platform/artist?telegram_id=${encodeURIComponent(artistId)}`);
      const a = data.artist || {};
      const badges = data.badges || [];
      const tracks = state.releases.filter((r) => String(r.telegram_id) === String(artistId));
      openModal(`
        <div class="stack-gap" style="padding-top:6px">
          <div style="display:flex;align-items:center;gap:12px">
            <div class="id-avatar-box" style="width:62px;height:62px;margin:0">
              ${a.avatar_url ? `<img src="${esc(a.avatar_url)}" alt="">` : `<span class="avatar-mono" style="font-size:18px">${esc((a.display_name || 'AR').slice(0, 2).toUpperCase())}</span>`}
            </div>
            <div style="flex:1;min-width:0">
              <h2 class="id-name">${esc(a.display_name || 'Артист CXRNER')}</h2>
              <small class="mono-num muted-text">Подписчиков: ${Number(data.followers || 0)} · Релизов: ${tracks.length}</small>
            </div>
          </div>
          ${a.bio ? `<p class="id-bio" style="margin:0">${esc(a.bio)}</p>` : ''}
          ${badges.length ? `<div class="pill-row">${badges.map((b) => `<span class="tag-pill">${esc(b.name)}</span>`).join('')}</div>` : ''}
          <button class="btn btn-primary full-w" data-follow-artist="${esc(artistId)}" type="button">Подписаться</button>
          ${tracks.length ? `<div class="mini-label">РЕЛИЗЫ АРТИСТА</div><div class="catalog-grid">${tracks.map((t) => releaseCardHtml(t, false)).join('')}</div>` : ''}
        </div>
      `);
    } catch (err) {
      toast(err.message);
      closeModal();
    }
  }

  /* ==================== 03. RELEASE WIZARD ==================== */
  function setStep(step) {
    state.step = Math.max(1, Math.min(5, step));
    $$('#wizardStepper .w-step').forEach((el) => {
      const n = Number(el.dataset.goStep);
      el.classList.toggle('is-current', n === state.step);
      el.classList.toggle('is-done', n < state.step);
    });
    $$('#releaseForm .form-step').forEach((el) => {
      el.classList.toggle('is-current', Number(el.dataset.formStep) === state.step);
    });
    const isAlbum = $('#releaseForm [name="type"]')?.value === 'album';
    $$('#releaseForm .album-only').forEach((el) => el.classList.toggle('is-hidden', !isAlbum));
    if (state.step === 5) renderWizardSummary();
  }

  function validateStep() {
    const active = $(`#releaseForm .form-step[data-form-step="${state.step}"]`);
    if (!active) return true;
    const fields = $$('input, select, textarea', active).filter((f) => !f.closest('.is-hidden'));
    for (const f of fields) {
      if (!f.checkValidity()) {
        f.reportValidity();
        f.focus();
        return false;
      }
    }
    return true;
  }

  function renderWizardSummary() {
    const f = $('#releaseForm');
    if (!f) return;
    const fd = new FormData(f);
    const rows = [
      ['Формат', fd.get('type') === 'album' ? 'Альбом / EP' : 'Сингл'],
      ['Название', `${fd.get('name') || '—'}${fd.get('subname') ? ` (${fd.get('subname')})` : ''}`],
      ['Жанр', fd.get('genre') || '—'],
      ['Дата', fd.get('date') || '—'],
      ['Артист', fd.get('nick') || '—'],
      ['Правообладатель', fd.get('fio') || '—'],
      ['Telegram', fd.get('tg') || '—'],
      ['Файлы', fd.get('link') || '—']
    ];
    $('#wizardSummaryBox').innerHTML = rows.map(([k, v]) => `<div class="summary-row"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join('');
  }

  function updateWizardCoverPreview(url) {
    const img = $('#wizardCoverPreviewImg');
    const ph = $('#wizardCoverPreviewPlaceholder');
    if (!img || !ph) return;
    if (/^https?:\/\//i.test(String(url || '').trim())) {
      img.src = url.trim();
      img.classList.remove('is-hidden');
      ph.classList.add('is-hidden');
    } else {
      img.classList.add('is-hidden');
      ph.classList.remove('is-hidden');
    }
  }

  async function submitReleaseForm(e) {
    e.preventDefault();
    if (!validateStep()) return;
    if (state.step < 5) { setStep(state.step + 1); return; }

    const form = $('#releaseForm');
    const fd = new FormData(form);
    const val = (k, fb = '.') => String(fd.get(k) || fb).trim() || fb;
    const btn = $('#wizardSubmitBtn');
    if (btn) { btn.disabled = true; btn.textContent = 'Отправка…'; }

    const payload = {
      action: 'webapp_release_submit',
      source: 'mini_app',
      version: 3,
      submitted_at: new Date().toISOString(),
      telegram_id: state.profile?.user?.telegram_id || tg?.initDataUnsafe?.user?.id || null,
      user: state.profile?.user || tg?.initDataUnsafe?.user || null,
      form: {
        type: val('type', 'single'),
        name: val('name', ''),
        subname: val('subname'),
        has_lyrics: val('has_lyrics', 'Да'),
        nick: val('nick', ''),
        fio: val('fio', ''),
        date: val('date', ''),
        version: val('version'),
        genre: val('genre', ''),
        link: val('link', ''),
        cover_url: val('cover_url', ''),
        smart_link: val('smart_link', ''),
        yandex: val('yandex'),
        mat: val('mat', 'Нет'),
        promo: val('promo'),
        comment: val('comment'),
        tracklist: val('tracklist'),
        tg: val('tg', '')
      }
    };

    try {
      await api('/api/webapp/submit', {
        method: 'POST',
        body: { reason: 'submit_form', source: 'mini_app', submitted_at: new Date().toISOString(), payload, initData: initData(), user: tg?.initDataUnsafe?.user || null }
      });
      localStorage.removeItem('cxrner_release_draft_v3');
      form.classList.add('is-hidden');
      $('#wizardStepper').classList.add('is-hidden');
      $('#wizardSuccessCard').classList.remove('is-hidden');
      haptic('medium');
      toast('Релиз отправлен в модерацию!');
    } catch (err) {
      toast('Ошибка отправки: ' + err.message);
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = 'Отправить релиз'; }
    }
  }

  /* ==================== 04. PROFILE & CXRNER ID ==================== */
  async function loadProfile() {
    try {
      const data = await api('/api/miniapp/platform/profile');
      state.profile = data;
      updateTopbar();
      renderProfile();
    } catch (err) {
      $('#profileContent').innerHTML = `<div class="glass-card">${esc(err.message)}</div>`;
    }
  }

  function renderProfile() {
    const p = state.profile;
    if (!p) return;
    const u = p.user || {};
    const ep = p.artist_profile || {};
    const uname = String(u.username || '').replace(/^@/, '');
    const isCeo = uname.toLowerCase() === 'kazumaiq';
    const frameClass = isCeo ? 'frame-ceo' : ep.equipped_frame === 'cyan' ? 'frame-cyan' : ep.equipped_frame === 'purple' ? 'frame-purple' : '';
    const display = ep.display_name || u.first_name || uname || 'Artist';
    const avatar = ep.avatar_url || u.photo_url || '';
    const bannerStyle = ep.banner_url ? `background-image:url('${esc(ep.banner_url)}')` : '';

    const releases = p.releases || [];
    const tabs = [
      { id: 'overview', label: 'CXRNER ID' },
      { id: 'releases', label: `Релизы (${releases.length})` },
      { id: 'referrals', label: 'Друзья' },
      { id: 'analytics', label: 'Аналитика' }
    ];

    let tabBody = '';
    if (state.profileTab === 'overview') {
      const roles = Array.isArray(ep.music_roles) && ep.music_roles.length ? ep.music_roles : ['Artist'];
      const genres = Array.isArray(ep.genres) ? ep.genres : [];
      tabBody = `
        <div class="glass-card stack-gap">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <span class="eyebrow-tag">КАТЕГОРИИ И РОЛИ</span>
            <button class="btn btn-cyan btn-sm" id="openVerificationQuickBtn" data-screen="verification" type="button">Верификация</button>
          </div>
          <div class="pill-row">
            ${roles.map((r) => `<span class="tag-pill">${esc(r)}</span>`).join('')}
            ${genres.map((g) => `<span class="tag-pill" style="color:var(--cyan)">#${esc(g)}</span>`).join('')}
          </div>
          <div class="dash-grid" style="margin:6px 0 0">
            <div class="dash-stat"><b class="mono-num">${releases.length}</b><small>Заявок</small></div>
            <div class="dash-stat"><b class="mono-num">${Number(p.stats?.likes || 0)}</b><small>Лайков</small></div>
            <div class="dash-stat"><b class="mono-num">${Number(p.stats?.favorites || 0)}</b><small>Избранное</small></div>
            <div class="dash-stat"><b class="mono-num">${Number(p.badges?.length || 0)}</b><small>Бейджей</small></div>
          </div>
        </div>
        <div class="glass-card">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
            <b>CXRNER DROP &amp; Единый аккаунт</b>
            <button class="btn btn-ghost btn-sm" data-screen="drop" type="button">Открыть DROP ↗</button>
          </div>
          <p class="muted-text">Telegram ID <span class="mono-num">${esc(u.telegram_id)}</span> синхронизирован с сайтом cxrnermusic.vercel.app.</p>
        </div>
      `;
    } else if (state.profileTab === 'releases') {
      tabBody = releases.length
        ? releases
            .map((r) => {
              const pl = r.form_payload || {};
              return `<div class="row-item" style="flex-direction:column;align-items:stretch;gap:8px">
                <div style="display:flex;align-items:center;justify-content:space-between;gap:10px">
                  <div style="min-width:0"><b>${esc(r.track_name || pl.name || 'Без названия')}</b><small>${esc(r.artist_name || pl.nick || '')}${r.upc ? ` · UPC ${esc(r.upc)}` : ''}</small></div>
                  <span class="status-badge status-${esc(r.status)}" style="position:static">${esc(statusText(r.status))}</span>
                </div>
                ${r.reject_reason ? `<small style="color:#FDA4AF">Причина: ${esc(r.reject_reason)}</small>` : ''}
                <div style="display:flex;gap:7px;flex-wrap:wrap">
                  <button class="btn btn-ghost btn-sm" data-edit-own-release="${esc(r.id)}" type="button">Медиа / Ссылка</button>
                  <button class="btn btn-cyan btn-sm" data-quick-link-release="${esc(r.id)}" type="button">Создать LINK</button>
                  <button class="btn btn-danger btn-sm" data-delete-own-release="${esc(r.id)}" type="button">Удалить</button>
                </div>
              </div>`;
            })
            .join('')
        : '<div class="glass-card" style="text-align:center"><p class="muted-text">У тебя пока нет отправленных релизов.</p><button class="btn btn-primary btn-sm" data-screen="submit" style="margin-top:10px" type="button">Отправить первый релиз</button></div>';
    } else if (state.profileTab === 'referrals') {
      const refUrl = `https://t.me/cxrnermusic_bot?start=ref_${encodeURIComponent(u.telegram_id || '')}`;
      tabBody = `<div class="glass-card stack-gap">
        <span class="eyebrow-tag">РЕФЕРАЛЬНАЯ ПРОГРАММА</span>
        <h3 style="margin:0;font-family:'Unbounded',sans-serif;font-size:15px">Приглашай друзей в CXRNER</h3>
        <p class="muted-text">Отправь личную ссылку артистам — получай бонусы CXP за активные подключения.</p>
        <input class="input-box mono-num" readonly value="${esc(refUrl)}">
        <div class="cta-row">
          <button class="btn btn-primary" data-copy-text="${esc(refUrl)}" type="button">Скопировать ссылку</button>
          <a class="btn btn-cyan" href="https://t.me/share/url?url=${encodeURIComponent(refUrl)}&text=${encodeURIComponent('Залетай в CXRNER MUSIC!')}" target="_blank" rel="noopener">Поделиться ↗</a>
        </div>
      </div>`;
    } else if (state.profileTab === 'analytics') {
      tabBody = '<div id="profileAnalyticsBox" class="glass-card"><p class="muted-text">Загружаем аналитику прослушиваний и роялти…</p></div>';
    }

    $('#profileContent').innerHTML = `
      <div class="id-hero ${frameClass}">
        <div class="id-banner" style="${bannerStyle}"></div>
        <div class="id-body">
          <div class="id-top-row">
            <div class="id-avatar-box">
              ${avatar ? `<img src="${esc(avatar)}" alt="" onerror="this.remove()">` : `<span class="avatar-mono" style="font-size:20px">${esc(display.slice(0, 2).toUpperCase())}</span>`}
            </div>
            <div style="display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end">
              <button class="btn btn-ghost btn-sm" id="openProfileEditorBtn" type="button">Настроить ID</button>
              <a class="btn btn-cyan btn-sm" href="https://cxrnermusic.vercel.app/profile/${encodeURIComponent(uname || u.telegram_id)}" target="_blank" rel="noopener">На сайте ↗</a>
            </div>
          </div>
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
            <h1 class="id-name">${esc(display)}</h1>
            ${isCeo ? '<span class="role-badge badge-ceo">👑 FOUNDER &amp; CEO</span>' : ''}
          </div>
          <div class="id-handle">@${esc(uname || u.telegram_id)}${ep.city || ep.country ? ` · ${esc([ep.city, ep.country].filter(Boolean).join(', '))}` : ''}</div>
          ${ep.bio ? `<p class="id-bio">${esc(ep.bio)}</p>` : ''}
        </div>
      </div>

      <div class="tab-strip">
        ${tabs.map((t) => `<button class="tab-pill ${state.profileTab === t.id ? 'is-active' : ''}" data-profile-tab="${t.id}" type="button">${esc(t.label)}</button>`).join('')}
      </div>
      ${tabBody}
    `;

    if (state.profileTab === 'analytics') {
      api('/api/miniapp/platform/analytics')
        .then((an) => {
          const box = $('#profileAnalyticsBox');
          if (!box) return;
          box.innerHTML = `<div class="dash-grid" style="grid-template-columns:repeat(3,1fr);margin-bottom:10px">
            <div class="dash-stat"><b class="mono-num">${Number(an.totals?.streams || 0)}</b><small>Стримов</small></div>
            <div class="dash-stat"><b class="mono-num">${Number(an.totals?.revenue || 0)} ₽</b><small>Доход</small></div>
            <div class="dash-stat"><b class="mono-num">${Number((an.payouts || []).length)}</b><small>Выплат</small></div>
          </div><p class="muted-text">Данные синхронизируются с отчётами дистрибуции.</p>`;
        })
        .catch(() => {});
    }
  }

  function compressImageFile(file, maxW, maxH, square = false) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onerror = () => reject(new Error('Не удалось прочитать файл'));
      reader.onload = () => {
        const img = new Image();
        img.onerror = () => reject(new Error('Неверный формат изображения'));
        img.onload = () => {
          const canvas = document.createElement('canvas');
          const ctx = canvas.getContext('2d');
          let sx = 0, sy = 0, sw = img.width, sh = img.height;
          if (square) {
            const side = Math.min(img.width, img.height);
            sx = (img.width - side) / 2;
            sy = (img.height - side) / 2;
            sw = side;
            sh = side;
            canvas.width = maxW;
            canvas.height = maxH;
          } else {
            const ratio = Math.min(maxW / img.width, maxH / img.height, 1);
            canvas.width = Math.round(img.width * ratio);
            canvas.height = Math.round(img.height * ratio);
          }
          ctx.drawImage(img, sx, sy, sw, sh, 0, 0, canvas.width, canvas.height);
          let out = canvas.toDataURL('image/webp', 0.82);
          if (!out.startsWith('data:image/webp')) out = canvas.toDataURL('image/jpeg', 0.84);
          resolve(out);
        };
        img.src = String(reader.result);
      };
      reader.readAsDataURL(file);
    });
  }

  function openProfileEditorModal() {
    const p = state.profile || {};
    const u = p.user || {};
    const ep = p.artist_profile || {};
    openModal(`
      <form id="profileEditorForm" class="stack-gap" style="padding-top:6px">
        <span class="eyebrow-tag">STUDIO CUSTOMIZER</span>
        <h2 style="font-family:'Unbounded',sans-serif;font-size:17px;margin:0">Редактировать CXRNER ID</h2>

        <div class="upload-box">
          <img id="editAvatarThumb" class="upload-thumb" src="${esc(ep.avatar_url || u.photo_url || 'assets/logo.png')}" alt="">
          <div style="flex:1;min-width:0">
            <b style="font-size:12px">Аватар (квадрат)</b>
            <input type="hidden" name="avatar_url" id="editAvatarVal" value="${esc(ep.avatar_url || u.photo_url || '')}">
            <div style="display:flex;gap:6px;margin-top:6px">
              <label class="btn btn-cyan btn-sm" style="cursor:pointer">Загрузить фото<input type="file" id="editAvatarFile" accept="image/*" class="is-hidden"></label>
              <button type="button" class="btn btn-ghost btn-sm" id="editAvatarUrlBtn">URL</button>
            </div>
          </div>
        </div>

        <div class="upload-box">
          <img id="editBannerThumb" class="upload-thumb banner-thumb" src="${esc(ep.banner_url || 'assets/logo.png')}" alt="">
          <div style="flex:1;min-width:0">
            <b style="font-size:12px">Баннер профиля (обложка)</b>
            <input type="hidden" name="banner_url" id="editBannerVal" value="${esc(ep.banner_url || '')}">
            <div style="display:flex;gap:6px;margin-top:6px">
              <label class="btn btn-cyan btn-sm" style="cursor:pointer">Загрузить фото<input type="file" id="editBannerFile" accept="image/*" class="is-hidden"></label>
              <button type="button" class="btn btn-ghost btn-sm" id="editBannerUrlBtn">URL</button>
            </div>
          </div>
        </div>

        <label style="display:grid;gap:5px;font-size:12px">Отображаемое имя
          <input class="input-box" name="display_name" required value="${esc(ep.display_name || u.first_name || '')}">
        </label>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
          <label style="display:grid;gap:5px;font-size:12px">Город<input class="input-box" name="city" value="${esc(ep.city || '')}" placeholder="Moscow"></label>
          <label style="display:grid;gap:5px;font-size:12px">Страна<input class="input-box" name="country" value="${esc(ep.country || '')}" placeholder="RU"></label>
        </div>
        <label style="display:grid;gap:5px;font-size:12px">О себе / Био
          <textarea class="input-box" name="bio" rows="3" placeholder="Продюсер, артист, саунд-дизайнер…">${esc(ep.bio || '')}</textarea>
        </label>
        <label style="display:grid;gap:5px;font-size:12px">Роли (через запятую)
          <input class="input-box" name="music_roles" value="${esc((ep.music_roles || ['Artist']).join(', '))}">
        </label>
        <label style="display:grid;gap:5px;font-size:12px">Жанры (через запятую)
          <input class="input-box" name="genres" value="${esc((ep.genres || []).join(', '))}" placeholder="Phonk, Trap, Electronic">
        </label>
        <label style="display:grid;gap:5px;font-size:12px">Рамка карточки
          <select class="input-box" name="equipped_frame">
            <option value="default" ${ep.equipped_frame === 'default' ? 'selected' : ''}>Obsidian Standard</option>
            <option value="cyan" ${ep.equipped_frame === 'cyan' ? 'selected' : ''}>Electric Cyan Neon</option>
            <option value="purple" ${ep.equipped_frame === 'purple' ? 'selected' : ''}>Ultraviolet Glow</option>
          </select>
        </label>
        <button class="btn btn-primary full-w" type="submit">Сохранить профиль</button>
      </form>
    `);
  }

  /* ==================== 05. CXRNER DROP ARENA & 3D ==================== */
  async function loadDropScreen() {
    try {
      const data = await api('/api/miniapp/drop/state');
      state.dropState = data;
      updateTopbar();
      renderDropScreen();
    } catch (err) {
      $('#dropContent').innerHTML = `<div class="glass-card">Ошибка загрузки DROP: ${esc(err.message)}</div>`;
    }
  }

  function renderDropScreen() {
    const d = state.dropState;
    if (!d) return;
    const w = d.wallet || { balance: 150 };
    const inv = d.inventory || [];
    const cases = d.cases || [];

    const tabs = [
      { id: 'cases', label: 'Кейсы' },
      { id: 'battle', label: 'Дуэль 1v1' },
      { id: 'fusion', label: 'Синтез 5→1' },
      { id: 'inventory', label: `Склад (${inv.length})` },
      { id: 'codes', label: 'Коды & Топ' }
    ];

    let body = '';
    if (state.dropTab === 'cases') {
      body = `
        ${d.daily_scan?.can_claim ? `<button class="promo-banner" data-drop-daily type="button">
          <div><b>📡 Ежедневный Daily Scan готов!</b><small>Серия дней: ${Number(d.daily_scan.streak || 0)} · +35..65 CXP бесплатно</small></div>
          <span class="promo-pill">ЗАБРАТЬ</span>
        </button>` : `<div class="row-item"><div><b>📡 Daily Archive Scan выполнен</b><small>Следующее пополнение энергии позже</small></div><span class="tag-pill">OK</span></div>`}
        <div class="case-grid">
          ${cases
            .map(
              (c) => `<div class="case-card">
                <div class="case-card-top">
                  <div class="case-pill-icon" style="border-color:${esc(c.accent)};color:${esc(c.accent)}">${esc(c.id.includes('001') ? '001' : c.id.includes('002') ? '002' : '003')}</div>
                  <div style="flex:1;min-width:0">
                    <b style="font-size:14px">${esc(c.name)}</b>
                    <small class="mono-num muted-text" style="font-size:10px">${esc(c.subtitle)}</small>
                  </div>
                  <span class="mono-num" style="color:var(--cyan);font-weight:800">${Number(c.price_cxp)} CXP</span>
                </div>
                <p class="muted-text" style="margin:0">${esc(c.description)}</p>
                <div style="display:flex;gap:8px">
                  <button class="btn btn-primary" style="flex:1" data-open-case="${esc(c.id)}" type="button">Открыть кейс · ${Number(c.price_cxp)} CXP</button>
                </div>
              </div>`
            )
            .join('')}
        </div>
      `;
    } else if (state.dropTab === 'battle') {
      body = `<div class="glass-card stack-gap">
        <span class="eyebrow-tag">PVP // ARCHIVE BOT</span>
        <h3 style="margin:0;font-family:'Unbounded',sans-serif;font-size:16px">Дуэль 1v1 за артефакты</h3>
        <p class="muted-text">Выбери ставку. У кого редкость и балл выше — тот забирает Оба артефакта + 80% кэшбек CXP!</p>
        <div class="stack-gap">
          ${cases.map((c) => `<button class="btn btn-cyan full-w" data-battle-case="${esc(c.id)}" type="button">⚔ Дуэль: ${esc(c.name)} (${c.price_cxp} CXP)</button>`).join('')}
        </div>
      </div>`;
    } else if (state.dropTab === 'fusion') {
      const fusable = inv.filter((i) => ['common', 'rare', 'epic', 'legendary'].includes(i.rarity));
      body = `<div class="glass-card stack-gap">
        <span class="eyebrow-tag">КРАФТ 5 → 1</span>
        <h3 style="margin:0;font-family:'Unbounded',sans-serif;font-size:15px">Синтез редкости (${state.fusionSelection.length}/5)</h3>
        <p class="muted-text">Выбери 5 артефактов одной редкости и скрафти экземпляр следующей ступени.</p>
        <button class="btn btn-primary full-w" id="runFusionBtn" ${state.fusionSelection.length === 5 ? '' : 'disabled'} type="button">Синтезировать (${state.fusionSelection.length}/5)</button>
      </div>
      <div class="inv-grid">
        ${fusable.length ? fusable.map((it) => {
          const sel = state.fusionSelection.includes(String(it.id));
          return `<button class="inv-card rarity-${esc(it.rarity)} ${sel ? 'is-selected' : ''}" data-toggle-fuse="${esc(it.id)}" type="button">
            <span class="rarity-chip rc-${esc(it.rarity)}">${esc(it.rarity)}</span>
            <b style="font-size:12px">${esc(it.item_name)}</b>
            <small class="mono-num muted-text">${esc(it.specimen_code)} · #${Number(it.serial_number || 1)}</small>
          </button>`;
        }).join('') : '<div class="glass-card" style="grid-column:1/-1">Нужно минимум 5 предметов Common–Legendary.</div>'}
      </div>`;
    } else if (state.dropTab === 'inventory') {
      body = inv.length
        ? `<div class="inv-grid">${inv
            .map(
              (it) => `<button class="inv-card rarity-${esc(it.rarity)}" data-inspect-item="${esc(it.id)}" type="button">
                <div style="display:flex;justify-content:space-between;align-items:center">
                  <span class="rarity-chip rc-${esc(it.rarity)}">${esc(it.rarity)}</span>
                  <span class="mono-num" style="font-size:10px;color:var(--dim)">#${Number(it.serial_number || 1)}</span>
                </div>
                <b style="font-size:12px;line-height:1.25">${esc(it.item_name)}</b>
                <small class="mono-num muted-text" style="font-size:10px">${esc(it.specimen_code || 'SP-001')}${it.is_pinned ? ' · 📌' : ''}</small>
              </button>`
            )
            .join('')}</div>`
        : '<div class="glass-card" style="text-align:center"><p class="muted-text">Склад пуст — открой первый кейс во вкладке «Кейсы»!</p></div>';
    } else {
      body = `
        <form id="dropClaimForm" class="glass-card stack-gap">
          <span class="eyebrow-tag">ПРОМОКОДЫ И ПОДАРКИ</span>
          <b style="font-size:14px">Активировать промокод или GIFT-код</b>
          <input class="input-box mono-num" name="code" required placeholder="CXRNER2026 или GIFT-...">
          <button class="btn btn-primary full-w" type="submit">Активировать</button>
        </form>
        <div class="glass-card stack-gap">
          <span class="eyebrow-tag">ТОП КОЛЛЕКЦИОНЕРОВ</span>
          ${(d.leaderboard || []).slice(0, 10).map((row, i) => `
            <div class="row-item">
              <div><b>#${i + 1} ${row.is_ceo ? '👑 ' : ''}${esc(row.display_name)}</b><small>${Number(row.items_count)} артефактов</small></div>
              <span class="mono-num" style="color:var(--cyan);font-weight:700">${Number(row.score)} PTS</span>
            </div>
          `).join('') || '<p class="muted-text">Рейтинг формируется…</p>'}
        </div>
      `;
    }

    $('#dropContent').innerHTML = `
      <div class="drop-hero">
        <div class="drop-balance-row">
          <div>
            <span class="eyebrow-tag">CXRNER DROP ARENA</span>
            <div class="drop-balance-val mono-num">${Number(w.balance || 0).toLocaleString('ru-RU')} <span style="font-size:14px;color:var(--cyan)">CXP</span></div>
          </div>
          <div style="text-align:right">
            <small class="muted-text mono-num" style="display:block">Склад: ${inv.length} шт</small>
            <small class="muted-text mono-num" style="display:block">Открыто: ${(d.history || []).length}</small>
          </div>
        </div>
      </div>

      <div class="tab-strip">
        ${tabs.map((t) => `<button class="tab-pill ${state.dropTab === t.id ? 'is-active' : ''}" data-drop-tab="${t.id}" type="button">${esc(t.label)}</button>`).join('')}
      </div>
      ${body}
    `;
  }

  function startInteractive3dCanvas(canvas, colorHex = '#00F0FF', mode = 'model', shape = 'octahedron') {
    if (!canvas) return () => {};
    const ctx = canvas.getContext('2d');
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const resize = () => {
      canvas.width = Math.max(200, canvas.clientWidth * dpr);
      canvas.height = Math.max(180, canvas.clientHeight * dpr);
    };
    resize();

    let rx = 0.45, ry = 0.65, vx = 0, vy = 0.014, drag = false, px = 0, py = 0, active = true;
    const verts = shape === 'box'
      ? [[-1,-1,-1],[1,-1,-1],[1,1,-1],[-1,1,-1],[-1,-1,1],[1,-1,1],[1,1,1],[-1,1,1]]
      : shape === 'cylinder'
      ? Array.from({ length: 16 }, (_, i) => {
          const a = ((i % 8) / 8) * Math.PI * 2;
          return [Math.cos(a) * 1.05, i < 8 ? -0.9 : 0.9, Math.sin(a) * 1.05];
        })
      : [[0, 1.35, 0], [0, -1.35, 0], [1.15, 0, 0], [-1.15, 0, 0], [0, 0, 1.15], [0, 0, -1.15]];

    const edges = shape === 'box'
      ? [[0,1],[1,2],[2,3],[3,0],[4,5],[5,6],[6,7],[7,4],[0,4],[1,5],[2,6],[3,7]]
      : shape === 'cylinder'
      ? [...Array.from({ length: 8 }, (_, i) => [i, (i + 1) % 8]), ...Array.from({ length: 8 }, (_, i) => [8 + i, 8 + ((i + 1) % 8)]), ...Array.from({ length: 8 }, (_, i) => [i, i + 8])]
      : [[0,2],[0,3],[0,4],[0,5],[1,2],[1,3],[1,4],[1,5],[2,4],[4,3],[3,5],[5,2]];

    const onDown = (e) => { drag = true; px = e.clientX || e.touches?.[0]?.clientX || 0; py = e.clientY || e.touches?.[0]?.clientY || 0; };
    const onMove = (e) => {
      if (!drag) return;
      const cx = e.clientX || e.touches?.[0]?.clientX || 0;
      const cy = e.clientY || e.touches?.[0]?.clientY || 0;
      vy = (cx - px) * 0.012;
      vx = (cy - py) * 0.012;
      ry += vy;
      rx += vx;
      px = cx;
      py = cy;
    };
    const onUp = () => { drag = false; };
    canvas.addEventListener('pointerdown', onDown);
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp);

    function frame() {
      if (!active || !document.body.contains(canvas)) return;
      const w = canvas.width, h = canvas.height;
      ctx.clearRect(0, 0, w, h);
      if (!drag) { ry += 0.013; rx += 0.004; }

      const cx = w / 2, cy = h / 2, sc = Math.min(w, h) * 0.27;
      if (mode === 'capsule') {
        ctx.strokeStyle = 'rgba(255,255,255,0.28)';
        ctx.lineWidth = 2 * dpr;
        ctx.strokeRect(cx - sc * 0.85, cy - sc * 1.1, sc * 1.7, sc * 2.2);
        ctx.strokeStyle = colorHex;
        ctx.beginPath();
        ctx.ellipse(cx, cy, sc * 0.68, sc * 0.22, ry * 0.35, 0, Math.PI * 2);
        ctx.stroke();
      }

      const sinX = Math.sin(rx), cosX = Math.cos(rx), sinY = Math.sin(ry), cosY = Math.cos(ry);
      const pts = verts.map(([x, y, z]) => {
        const nx = x * cosY - z * sinY;
        const nz = x * sinY + z * cosY;
        const ny = y * cosX - nz * sinX;
        const fz = y * sinX + nz * cosX;
        const p = 3.2 / (3.2 - fz * 0.45);
        return [cx + nx * sc * p, cy + ny * sc * p];
      });

      ctx.strokeStyle = colorHex;
      ctx.shadowColor = colorHex;
      ctx.shadowBlur = 12 * dpr;
      ctx.lineWidth = 2.2 * dpr;
      ctx.beginPath();
      for (const [a, b] of edges) {
        if (!pts[a] || !pts[b]) continue;
        ctx.moveTo(pts[a][0], pts[a][1]);
        ctx.lineTo(pts[b][0], pts[b][1]);
      }
      ctx.stroke();
      ctx.shadowBlur = 0;

      requestAnimationFrame(frame);
    }
    requestAnimationFrame(frame);

    return () => {
      active = false;
      window.removeEventListener('pointermove', onMove);
      window.removeEventListener('pointerup', onUp);
    };
  }

  function inspectDropItem(item, justWon = false) {
    let viewMode = 'model';
    const salvageVal = { common: 15, rare: 30, epic: 55, legendary: 120, mythic: 250, special_reward: 500 }[item.rarity] || 15;
    openModal(`
      <div class="stack-gap" style="padding-top:6px">
        <div style="display:flex;justify-content:space-between;align-items:center">
          <span class="rarity-chip rc-${esc(item.rarity)}">${esc(item.rarity)} SPECIMEN</span>
          <span class="mono-num muted-text">#${String(item.serial_number || 1).padStart(5, '0')} · ${esc(item.specimen_code || 'SP-001')}</span>
        </div>
        <h2 style="font-family:'Unbounded',sans-serif;font-size:18px;margin:0;text-align:center">${esc(item.item_name)}</h2>
        ${justWon ? '<div style="text-align:center;color:var(--emerald);font-family:\'JetBrains Mono\',monospace;font-size:11px">✓ ЭКЗЕМПЛЯР ДОБАВЛЕН НА СКЛАД</div>' : ''}
        <div class="tab-strip">
          <button class="tab-pill is-active" data-cap-mode="model" type="button">3D Модель</button>
          <button class="tab-pill" data-cap-mode="capsule" type="button">Вакуум-капсула</button>
        </div>
        <div class="capsule-stage">
          <canvas id="specimen3dCanvas"></canvas>
          <span class="capsule-badge-overlay">${esc(item.specimen_code || 'SP-001')}</span>
          <span class="capsule-hint-overlay">ВРАЩАЙ ПАЛЬЦЕМ 360°</span>
        </div>
        <p class="muted-text" style="text-align:center;margin:0">${esc(item.description || '')}</p>
        ${item.claim_code ? `<div class="summary-row"><span>Ваучер код</span><b class="mono-num" style="color:var(--cyan)">${esc(item.claim_code)}</b></div>` : ''}
        <div class="cta-row">
          <button class="btn btn-ghost" data-item-pin="${esc(item.id)}" type="button">${item.is_pinned ? 'Открепить' : '📌 Закрепить'}</button>
          <button class="btn btn-cyan" data-item-gift="${esc(item.id)}" type="button">🎁 Подарить</button>
        </div>
        <button class="btn btn-danger full-w" data-item-salvage="${esc(item.id)}" type="button">⚡ Распылить в энергию (+${salvageVal} CXP)</button>
      </div>
    `);

    const canvas = $('#specimen3dCanvas');
    let stopCanvas = startInteractive3dCanvas(canvas, item.model_color || '#00F0FF', viewMode, item.shape || 'octahedron');
    $$('[data-cap-mode]', $('#modalContent')).forEach((btn) => {
      btn.addEventListener('click', () => {
        $$('[data-cap-mode]', $('#modalContent')).forEach((b) => b.classList.remove('is-active'));
        btn.classList.add('is-active');
        viewMode = btn.dataset.capMode;
        stopCanvas();
        stopCanvas = startInteractive3dCanvas(canvas, item.model_color || '#00F0FF', viewMode, item.shape || 'octahedron');
      });
    });
  }

  async function handleOpenCase(caseId) {
    openModal(`
      <div class="unseal-stage">
        <span class="eyebrow-tag">UNSEALING PROTOCOL ACTIVE</span>
        <h2 style="font-family:'Unbounded',sans-serif;font-size:17px;margin:0" id="unsealStatusText">Вскрытие вакуум-пломбы…</h2>
        <div class="progress-track"><div id="unsealBar" class="progress-fill" style="width:22%"></div></div>
        <p class="mono-num muted-text" id="unsealPercent">22% // VERIFYING ARCHIVE</p>
      </div>
    `);
    try {
      const apiPromise = api('/api/miniapp/drop/open', { method: 'POST', body: { case_id: caseId } });
      await new Promise((r) => setTimeout(r, 380));
      if ($('#unsealBar')) { $('#unsealBar').style.width = '68%'; $('#unsealStatusText').textContent = 'Идентификация редкости…'; $('#unsealPercent').textContent = '68% // SCANNING SPECIMEN'; }
      const res = await apiPromise;
      await new Promise((r) => setTimeout(r, 320));
      haptic('heavy');
      await loadDropScreen();
      inspectDropItem(res.item, true);
    } catch (err) {
      closeModal();
      toast(err.message);
    }
  }

  /* ==================== 06. CXRNER LINK STUDIO ==================== */
  async function loadLinkEditor() {
    const box = $('#linkContent');
    if (!box) return;
    try {
      const data = await api('/api/miniapp/link?mine=1');
      state.links = data.links || [];
      const cur = state.editingLink || {};
      const plats = cur.platforms || {};
      box.innerHTML = `
        <form id="linkEditorForm" class="glass-form">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <h2 style="margin:0;font-size:15px">${cur.id ? 'Редактировать ссылку' : 'Новая смарт-ссылка'}</h2>
            ${cur.id ? '<button type="button" class="btn btn-ghost btn-sm" id="resetLinkFormBtn">Создать новую</button>' : ''}
          </div>
          <input type="hidden" name="id" value="${esc(cur.id || '')}">
          <div style="display:flex;gap:8px">
            <input class="input-box mono-num" id="upcLookupInput" placeholder="Найти релиз по UPC…" style="flex:1">
            <button type="button" class="btn btn-cyan btn-sm" id="upcLookupBtn">UPC</button>
          </div>
          <label>Название трека / альбома<input name="title" required value="${esc(cur.title || '')}" placeholder="Midnight Signal"></label>
          <label>Артист<input name="artist" required value="${esc(cur.artist || '')}" placeholder="CXRNER Artist"></label>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
            <label>Slug ссылки<input name="slug" class="mono-num" value="${esc(cur.slug || '')}" placeholder="artist-track"></label>
            <label>Тема<select name="theme">
              ${['aurora', 'obsidian', 'cyber', 'neon', 'chrome'].map((t) => `<option value="${t}" ${cur.theme === t ? 'selected' : ''}>${t.toUpperCase()}</option>`).join('')}
            </select></label>
          </div>
          <label>Обложка URL<input name="cover_url" type="url" value="${esc(cur.cover_url || '')}" placeholder="https://.../cover.jpg"></label>
          <div class="mini-label">ССЫЛКИ НА ПЛОЩАДКИ</div>
          ${['yandex', 'vk', 'spotify', 'apple', 'youtube', 'deezer', 'zvuk', 'soundcloud']
            .map((k) => {
              const m = DSP_META[k];
              return `<div style="display:flex;align-items:center;gap:8px">
                <span class="dsp-logo-box" style="width:34px;height:34px;border-radius:10px;background:${m.bg};color:${m.fg}">${m.svg}</span>
                <input class="input-box" name="pl_${k}" value="${esc(plats[k] || '')}" placeholder="${esc(m.name)} URL" style="min-height:36px">
              </div>`;
            })
            .join('')}
          <button class="btn btn-primary full-w" type="submit">${cur.id ? 'Сохранить изменения' : 'Опубликовать CXRNER LINK'}</button>
        </form>

        <div class="mini-label" style="margin-top:8px">МОИ СМАРТ-ССЫЛКИ (${state.links.length})</div>
        <div class="stack-gap">
          ${state.links.length ? state.links.map((l) => `
            <div class="row-item" style="flex-direction:column;align-items:stretch;gap:8px">
              <div style="display:flex;justify-content:space-between;align-items:center;gap:8px">
                <div><b>${esc(l.title)}</b><small class="mono-num">/link/${esc(l.slug)} · ${esc(l.artist)}</small></div>
                <a class="btn btn-cyan btn-sm" href="${esc(l.public_url)}" target="_blank" rel="noopener">Открыть ↗</a>
              </div>
              <div style="display:flex;gap:7px">
                <button class="btn btn-ghost btn-sm" data-copy-text="${esc(l.public_url)}" type="button">Копировать</button>
                <button class="btn btn-ghost btn-sm" data-edit-link="${esc(l.id)}" type="button">Редактировать</button>
                <button class="btn btn-danger btn-sm" data-delete-link="${esc(l.id)}" type="button">Удалить</button>
              </div>
            </div>
          `).join('') : '<div class="glass-card"><p class="muted-text">Создай свою первую смарт-ссылку выше.</p></div>'}
        </div>
      `;
    } catch (err) {
      box.innerHTML = `<div class="glass-card">${esc(err.message)}</div>`;
    }
  }

  /* ==================== 07. OTHER SCREENS ==================== */
  function renderVerification() {
    const status = state.homeData?.verification_status || 'not_verified';
    $('#verificationContent').innerHTML = `
      <div class="glass-card">
        <span class="eyebrow-tag">ТЕКУЩИЙ СТАТУС</span>
        <div style="margin-top:6px;display:flex;align-items:center;justify-content:space-between">
          <b>${status === 'verified' ? '✓ Артист верифицирован' : status === 'pending_review' ? '⏳ Заявка на проверке' : 'Заявка не отправлена'}</b>
          <span class="role-badge ${status === 'verified' ? 'badge-verified' : 'badge-artist'}">${esc(status.toUpperCase())}</span>
        </div>
      </div>
      <form id="verificationForm" class="glass-form">
        <h2>Заявка на верификацию</h2>
        <p class="form-desc">Укажи свой псевдоним и ссылки на карточки артиста для получения галочки.</p>
        <label>Сценический псевдоним<input name="stage_name" required placeholder="Твой ник артиста"></label>
        <label>Яндекс Музыка<input name="yandex_url" type="url" placeholder="https://music.yandex.ru/artist/..."></label>
        <label>Spotify<input name="spotify_url" type="url" placeholder="https://open.spotify.com/artist/..."></label>
        <label>VK Музыка<input name="vk_url" type="url" placeholder="https://vk.com/artist/..."></label>
        <label>Apple Music<input name="apple_url" type="url" placeholder="https://music.apple.com/artist/..."></label>
        <label>Комментарий<textarea name="notes" rows="2" placeholder="Ссылки на релизы или подтверждение прав"></textarea></label>
        <button class="btn btn-primary full-w" type="submit">Отправить на проверку</button>
      </form>
    `;
  }

  function renderNotifications() {
    const items = state.homeData?.notifications || state.profile?.notifications || [];
    $('#notificationsContent').innerHTML = items.length
      ? items
          .map(
            (n) => `<div class="glass-card">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
                <b style="font-size:13px">${esc(n.title)}</b>
                ${!n.read_at && !n.is_read ? '<span class="role-badge badge-verified">NEW</span>' : ''}
              </div>
              <p class="muted-text" style="margin:0">${esc(n.body || n.message || '')}</p>
              <small class="mono-num muted-text" style="display:block;margin-top:6px;font-size:10px">${new Date(n.created_at || Date.now()).toLocaleString('ru-RU')}</small>
            </div>`
          )
          .join('')
      : '<div class="glass-card" style="text-align:center"><p class="muted-text">Уведомлений пока нет.</p></div>';
  }

  async function loadRanking() {
    try {
      const sort = $('#rankingSort')?.value || 'likes';
      const data = await api(`/api/miniapp/platform/rankings?sort=${encodeURIComponent(sort)}`);
      $('#rankingContent').innerHTML = (data.rankings || []).length
        ? data.rankings
            .map(
              (r, idx) => `<button class="row-item" data-artist-id="${esc(r.telegram_id)}" type="button">
                <div><b>#${idx + 1} ${esc(r.artist)}</b><small>Релизов: ${Number(r.releases || 0)} · Лайков: ${Number(r.likes || 0)}</small></div>
                <span class="mono-num" style="color:var(--cyan);font-weight:700">${Number(r[sort] || 0)}</span>
              </button>`
            )
            .join('')
        : '<div class="glass-card">Рейтинг пока пуст.</div>';
    } catch (err) {
      $('#rankingContent').innerHTML = `<div class="glass-card">${esc(err.message)}</div>`;
    }
  }

  async function loadCalendar() {
    try {
      const data = await api('/api/miniapp/platform/calendar');
      $('#calendarContent').innerHTML = (data.releases || []).length
        ? data.releases
            .slice(0, 30)
            .map(
              (r) => `<button class="row-item" data-release-id="${esc(r.id)}" type="button">
                <div><b>${esc(r.title)}</b><small>${esc(r.artist)} · ${esc(r.genre || '')}</small></div>
                <span class="tag-pill mono-num">${esc(String(r.date || r.release_date || '').slice(0, 10))}</span>
              </button>`
            )
            .join('')
        : '<div class="glass-card">Даты релизов не найдены.</div>';
    } catch (err) {
      $('#calendarContent').innerHTML = `<div class="glass-card">${esc(err.message)}</div>`;
    }
  }

  async function loadActivity() {
    try {
      const data = await api('/api/miniapp/platform/activity');
      $('#activityContent').innerHTML = (data.items || []).length
        ? data.items
            .map(
              (x) => `<div class="row-item">
                <div><b>${esc(x.title)}</b><small>${esc(statusText(x.body || ''))}</small></div>
                <span class="mono-num muted-text" style="font-size:10px">${new Date(x.created_at || Date.now()).toLocaleDateString('ru-RU')}</span>
              </div>`
            )
            .join('')
        : '<div class="glass-card">Лента событий пуста.</div>';
    } catch (err) {
      $('#activityContent').innerHTML = `<div class="glass-card">${esc(err.message)}</div>`;
    }
  }

  async function loadAdmin() {
    try {
      const data = await api('/api/miniapp/platform/admin/overview');
      $('#adminContent').innerHTML = `
        <div class="dash-grid">
          <div class="dash-stat"><b class="mono-num">${data.stats?.users || 0}</b><small>Юзеров</small></div>
          <div class="dash-stat"><b class="mono-num">${data.stats?.releases || 0}</b><small>Релизов</small></div>
          <div class="dash-stat"><b class="mono-num">${data.stats?.moderation || 0}</b><small>На проверке</small></div>
          <div class="dash-stat"><b class="mono-num">${data.stats?.links || 0}</b><small>Ссылок</small></div>
        </div>
        <form id="adminNewsForm" class="glass-form">
          <h2 style="font-size:15px">Опубликовать новость</h2>
          <input name="title" required placeholder="Заголовок новости">
          <textarea name="body" rows="2" required placeholder="Текст объявления"></textarea>
          <button class="btn btn-primary full-w" type="submit">Опубликовать</button>
        </form>
        <form id="adminBroadcastForm" class="glass-form">
          <h2 style="font-size:15px">Уведомление в Mini App</h2>
          <input name="title" required placeholder="Заголовок">
          <textarea name="body" rows="2" required placeholder="Сообщение пользователям"></textarea>
          <select name="audience"><option value="all">Все</option><option value="artists">Артисты</option></select>
          <button class="btn btn-cyan full-w" type="submit">Разослать</button>
        </form>
      `;
    } catch (err) {
      $('#adminContent').innerHTML = `<div class="glass-card">${esc(err.message)}</div>`;
    }
  }

  /* ==================== BOOTSTRAP & EVENTS ==================== */
  async function loadAll() {
    try {
      const [catRes, homeRes, dropRes] = await Promise.allSettled([
        api('/api/miniapp/platform/catalog'),
        api('/api/miniapp/platform/home'),
        api('/api/miniapp/drop/state')
      ]);
      if (catRes.status === 'fulfilled') state.releases = catRes.value.releases || [];
      if (homeRes.status === 'fulfilled') state.homeData = homeRes.value;
      if (dropRes.status === 'fulfilled') state.dropState = dropRes.value;

      if (state.homeData?.is_admin) {
        $('#adminEntry').innerHTML = `<button class="more-card accent-cyan" style="margin-top:10px;width:100%" data-screen="admin" type="button">
          <span class="more-ico" data-icon="shield"></span>
          <div><b>Control Center (Admin)</b><small>Статистика лейбла, новости и рассылка</small></div>
          <span class="svg-icon" data-icon="arrow"></span>
        </button>`;
      }

      updateTopbar();
      renderHome();
      renderIcons();

      try {
        const draft = JSON.parse(localStorage.getItem('cxrner_release_draft_v3') || 'null');
        if (draft) {
          Object.entries(draft).forEach(([k, v]) => {
            const el = $(`#releaseForm [name="${k}"]`);
            if (el && !el.value) el.value = v;
          });
          if (draft.cover_url) updateWizardCoverPreview(draft.cover_url);
        }
        const tgUser = state.homeData?.user || tg?.initDataUnsafe?.user;
        const tgInput = $('#releaseForm [name="tg"]');
        if (tgInput && !tgInput.value && tgUser?.username) tgInput.value = `@${tgUser.username}`;
      } catch {}

      // Deep link support
      const params = new URLSearchParams(location.search);
      const startParam = String(params.get('startapp') || params.get('screen') || tg?.initDataUnsafe?.start_param || '').trim();
      if (startParam && $('#' + startParam)) switchScreen(startParam, false);
    } catch (err) {
      toast('Ошибка загрузки: ' + err.message);
    } finally {
      $('#loader').classList.add('is-hidden');
      $('#appShell').classList.remove('is-hidden');
      syncBackButton();
    }
  }

  document.addEventListener('click', async (e) => {
    const btn = e.target.closest('button');
    if (btn) haptic('light');

    const screenBtn = e.target.closest('[data-screen]');
    if (screenBtn) { closeModal(); switchScreen(screenBtn.dataset.screen); return; }

    if (e.target.closest('[data-close-modal]')) { closeModal(); return; }

    const relCard = e.target.closest('[data-release-id]:not([data-engage])');
    if (relCard) { openRelease(relCard.dataset.releaseId); return; }

    const artistBtn = e.target.closest('[data-artist-id]');
    if (artistBtn) { openArtistModal(artistBtn.dataset.artistId); return; }

    if (e.target.closest('[data-action="quick-daily-scan"], [data-drop-daily]')) {
      try {
        const r = await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'daily_scan' } });
        toast(`+${r.reward_cxp} CXP получено!`);
        await Promise.allSettled([loadAll(), state.screen === 'drop' ? loadDropScreen() : Promise.resolve()]);
      } catch (err) { toast(err.message); }
      return;
    }

    const genreChip = e.target.closest('[data-genre-chip]');
    if (genreChip) { state.genre = genreChip.dataset.genreChip; renderCatalog(); return; }

    if (e.target.id === 'catalogFavToggle') { state.onlyFavorites = !state.onlyFavorites; renderCatalog(); return; }
    if (e.target.id === 'clearSearch' || e.target.closest('#clearSearch')) { $('#catalogSearch').value = ''; state.search = ''; renderCatalog(); return; }

    const preset = e.target.closest('[data-preset-genre]');
    if (preset) {
      const inp = $('#releaseForm [name="genre"]');
      if (inp) inp.value = preset.dataset.presetGenre;
      $$('#genrePresetPills button').forEach((b) => b.classList.remove('is-selected'));
      preset.classList.add('is-selected');
      return;
    }

    if (e.target.closest('.next-step')) { if (validateStep()) setStep(state.step + 1); return; }
    if (e.target.closest('.prev-step')) { setStep(state.step - 1); return; }
    if (e.target.id === 'wizardResetBtn') {
      $('#wizardSuccessCard').classList.add('is-hidden');
      $('#releaseForm').classList.remove('is-hidden');
      $('#wizardStepper').classList.remove('is-hidden');
      $('#releaseForm').reset();
      updateWizardCoverPreview('');
      setStep(1);
      return;
    }

    const pTab = e.target.closest('[data-profile-tab]');
    if (pTab) { state.profileTab = pTab.dataset.profileTab; renderProfile(); return; }

    const dTab = e.target.closest('[data-drop-tab]');
    if (dTab) { state.dropTab = dTab.dataset.dropTab; renderDropScreen(); return; }

    if (e.target.id === 'openProfileEditorBtn') { openProfileEditorModal(); return; }

    const openCase = e.target.closest('[data-open-case]');
    if (openCase) { handleOpenCase(openCase.dataset.openCase); return; }

    const battleCase = e.target.closest('[data-battle-case]');
    if (battleCase) {
      try {
        const res = await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'battle', case_id: battleCase.dataset.battleCase } });
        toast(res.won ? `Победа! +2 артефакта и +${res.reward_cxp} CXP!` : 'Поражение в дуэли — повезёт в следующий раз.');
        await loadDropScreen();
      } catch (err) { toast(err.message); }
      return;
    }

    const toggleFuse = e.target.closest('[data-toggle-fuse]');
    if (toggleFuse) {
      const id = String(toggleFuse.dataset.toggleFuse);
      const idx = state.fusionSelection.indexOf(id);
      if (idx >= 0) state.fusionSelection.splice(idx, 1);
      else if (state.fusionSelection.length < 5) state.fusionSelection.push(id);
      renderDropScreen();
      return;
    }

    if (e.target.id === 'runFusionBtn') {
      try {
        const res = await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'fuse', item_ids: state.fusionSelection } });
        state.fusionSelection = [];
        toast(`Синтезирован ${res.forged?.item_name}!`);
        await loadDropScreen();
        if (res.forged) inspectDropItem(res.forged, true);
      } catch (err) { toast(err.message); }
      return;
    }

    const inspectBtn = e.target.closest('[data-inspect-item]');
    if (inspectBtn) {
      const it = state.dropState?.inventory?.find((x) => String(x.id) === String(inspectBtn.dataset.inspectItem));
      if (it) inspectDropItem(it, false);
      return;
    }

    const salvageBtn = e.target.closest('[data-item-salvage]');
    if (salvageBtn) {
      try {
        const r = await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'salvage', item_id: salvageBtn.dataset.itemSalvage } });
        closeModal();
        toast(`Распылено: +${r.gained_cxp} CXP!`);
        await loadDropScreen();
      } catch (err) { toast(err.message); }
      return;
    }

    const pinItemBtn = e.target.closest('[data-item-pin]');
    if (pinItemBtn) {
      try {
        await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'pin_item', item_id: pinItemBtn.dataset.itemPin } });
        closeModal();
        await loadDropScreen();
        toast('Витрина обновлена');
      } catch (err) { toast(err.message); }
      return;
    }

    const giftBtn = e.target.closest('[data-item-gift]');
    if (giftBtn) {
      try {
        const res = await api('/api/miniapp/drop/action', { method: 'POST', body: { action: 'create_gift', item_id: giftBtn.dataset.itemGift } });
        await navigator.clipboard?.writeText(res.gift_code).catch(() => {});
        toast(`Подарочный код скопирован: ${res.gift_code.slice(0, 18)}…`);
        closeModal();
        await loadDropScreen();
      } catch (err) { toast(err.message); }
      return;
    }

    const engBtn = e.target.closest('[data-engage]');
    if (engBtn) {
      const rid = String(engBtn.dataset.releaseId), kind = engBtn.dataset.engage;
      if (kind === 'favorite') {
        const fav = getFavoritesSet();
        const active = !fav.has(rid);
        if (active) fav.add(rid); else fav.delete(rid);
        localStorage.setItem('cxrner_fav_ids', JSON.stringify([...fav]));
        await api('/api/miniapp/platform/engagement', { method: 'POST', body: { release_id: rid, kind, active } }).catch(() => {});
        openRelease(rid);
      } else {
        await api('/api/miniapp/platform/engagement', { method: 'POST', body: { release_id: rid, kind, active: true } }).catch(() => {});
        toast('Отмечено ♥');
      }
      return;
    }

    const followBtn = e.target.closest('[data-follow-artist]');
    if (followBtn) {
      try {
        await api('/api/miniapp/platform/follow', { method: 'POST', body: { following_id: followBtn.dataset.followArtist, active: true } });
        toast('Вы подписались на артиста');
      } catch (err) { toast(err.message); }
      return;
    }

    const copyBtn = e.target.closest('[data-copy-text]');
    if (copyBtn) {
      await navigator.clipboard?.writeText(copyBtn.dataset.copyText).catch(() => {});
      toast('Скопировано в буфер обмена');
      return;
    }

    const editLink = e.target.closest('[data-edit-link]');
    if (editLink) {
      state.editingLink = state.links.find((l) => String(l.id) === String(editLink.dataset.editLink)) || null;
      loadLinkEditor();
      window.scrollTo({ top: 0, behavior: 'smooth' });
      return;
    }
    if (e.target.id === 'resetLinkFormBtn') { state.editingLink = null; loadLinkEditor(); return; }
    const delLink = e.target.closest('[data-delete-link]');
    if (delLink) {
      try {
        await api('/api/miniapp/link', { method: 'POST', body: { action: 'delete', link_id: delLink.dataset.deleteLink } });
        toast('Ссылка удалена');
        loadLinkEditor();
      } catch (err) { toast(err.message); }
      return;
    }
    if (e.target.id === 'upcLookupBtn') {
      const upc = $('#upcLookupInput')?.value?.trim();
      if (!upc) { toast('Введи UPC'); return; }
      try {
        const res = await api(`/api/miniapp/link/lookup?upc=${encodeURIComponent(upc)}`);
        const r = res.release;
        const form = $('#linkEditorForm');
        if (form && r) {
          form.title.value = r.title || '';
          form.artist.value = r.artist || '';
          form.cover_url.value = r.cover || '';
          Object.entries(r.platforms || {}).forEach(([k, u]) => { if (form[`pl_${k}`]) form[`pl_${k}`].value = u; });
          toast('Данные релиза заполнены по UPC!');
        }
      } catch (err) { toast(err.message); }
      return;
    }

    const delOwnRel = e.target.closest('[data-delete-own-release]');
    if (delOwnRel) {
      if (!confirm('Удалить эту заявку?')) return;
      try {
        await api('/api/miniapp/platform/release', { method: 'POST', body: { action: 'delete', release_id: delOwnRel.dataset.deleteOwnRelease } });
        toast('Релиз удалён');
        loadProfile();
      } catch (err) { toast(err.message); }
      return;
    }

    const quickLinkRel = e.target.closest('[data-quick-link-release]');
    if (quickLinkRel) {
      const rel = state.profile?.releases?.find((r) => String(r.id) === String(quickLinkRel.dataset.quickLinkRelease));
      if (rel) {
        const pl = rel.form_payload || {};
        state.editingLink = { title: rel.track_name || pl.name, artist: rel.artist_name || pl.nick, cover_url: pl.cover_url || '', platforms: { yandex: pl.yandex || '' } };
        switchScreen('link');
      }
      return;
    }

    const editOwnRel = e.target.closest('[data-edit-own-release]');
    if (editOwnRel) {
      const rel = state.profile?.releases?.find((r) => String(r.id) === String(editOwnRel.dataset.editOwnRelease));
      if (!rel) return;
      const pl = rel.form_payload || {};
      openModal(`
        <form id="editReleaseMediaForm" class="stack-gap" style="padding-top:6px">
          <input type="hidden" name="release_id" value="${esc(rel.id)}">
          <h2 style="font-family:'Unbounded',sans-serif;font-size:16px;margin:0">Медиа: ${esc(rel.track_name || pl.name || '')}</h2>
          <label style="display:grid;gap:5px;font-size:12px">Ссылка на обложку<input class="input-box" name="cover_url" type="url" value="${esc(pl.cover_url || '')}"></label>
          <label style="display:grid;gap:5px;font-size:12px">Мультиссылка / BandLink<input class="input-box" name="smart_link" type="url" value="${esc(pl.smart_link || '')}"></label>
          <button class="btn btn-primary full-w" type="submit">Сохранить</button>
        </form>
      `);
      return;
    }

    if (e.target.id === 'markAllReadBtn') {
      await api('/api/miniapp/platform/notifications/read', { method: 'POST', body: {} }).catch(() => {});
      if (state.homeData?.notifications) state.homeData.notifications.forEach((n) => { n.read_at = new Date().toISOString(); });
      updateTopbar();
      renderNotifications();
      toast('Все уведомления отмечены прочитанными');
      return;
    }

    if (e.target.id === 'editAvatarUrlBtn') {
      const url = window.prompt('Вставь прямую HTTPS-ссылку на аватар:', $('#editAvatarVal')?.value || '');
      if (url !== null) { $('#editAvatarVal').value = url.trim(); if (url.trim()) $('#editAvatarThumb').src = url.trim(); }
      return;
    }
    if (e.target.id === 'editBannerUrlBtn') {
      const url = window.prompt('Вставь прямую HTTPS-ссылку на баннер:', $('#editBannerVal')?.value || '');
      if (url !== null) { $('#editBannerVal').value = url.trim(); if (url.trim()) $('#editBannerThumb').src = url.trim(); }
      return;
    }

    const delCom = e.target.closest('[data-del-comment]');
    if (delCom) {
      await api('/api/miniapp/platform/comments', { method: 'DELETE', body: { id: delCom.dataset.delComment } }).catch(() => {});
      loadComments(state.currentReleaseId);
      return;
    }
    const pinCom = e.target.closest('[data-pin-comment]');
    if (pinCom) {
      await api('/api/miniapp/platform/comments', { method: 'PATCH', body: { action: 'pin', id: pinCom.dataset.pinComment, pinned: pinCom.dataset.pinned === 'true' } }).catch(() => {});
      loadComments(state.currentReleaseId);
      return;
    }
    const listenAnchor = e.target.closest('[data-track-listen]');
    if (listenAnchor) {
      api('/api/miniapp/platform/listen', { method: 'POST', body: { release_id: listenAnchor.dataset.trackListen, seconds_played: 15 } }).catch(() => {});
    }
  });

  document.addEventListener('change', async (e) => {
    if (e.target.id === 'sortFilter') { state.sort = e.target.value; renderCatalog(); return; }
    if (e.target.id === 'rankingSort') { loadRanking(); return; }
    if (e.target.matches('#releaseForm [name="type"]')) {
      $$('#releaseForm .album-only').forEach((el) => el.classList.toggle('is-hidden', e.target.value !== 'album'));
    }
    if (e.target.id === 'editAvatarFile' && e.target.files?.[0]) {
      try {
        const dataUrl = await compressImageFile(e.target.files[0], 420, 420, true);
        $('#editAvatarVal').value = dataUrl;
        $('#editAvatarThumb').src = dataUrl;
        toast('Фото аватара сжато и готово');
      } catch (err) { toast(err.message); }
    }
    if (e.target.id === 'editBannerFile' && e.target.files?.[0]) {
      try {
        const dataUrl = await compressImageFile(e.target.files[0], 1100, 420, false);
        $('#editBannerVal').value = dataUrl;
        $('#editBannerThumb').src = dataUrl;
        toast('Баннер сжат и готов');
      } catch (err) { toast(err.message); }
    }
  });

  document.addEventListener('input', (e) => {
    if (e.target.id === 'catalogSearch') { state.search = e.target.value; renderCatalog(); return; }
    if (e.target.id === 'wizardCoverInput') { updateWizardCoverPreview(e.target.value); }
    if (e.target.closest('#releaseForm')) {
      try {
        const vals = Object.fromEntries(new FormData($('#releaseForm')).entries());
        localStorage.setItem('cxrner_release_draft_v3', JSON.stringify(vals));
      } catch {}
    }
  });

  document.addEventListener('submit', async (e) => {
    if (e.target.id === 'releaseForm') { submitReleaseForm(e); return; }

    if (e.target.id === 'profileEditorForm') {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        await api('/api/miniapp/platform/artist/profile', {
          method: 'POST',
          body: {
            profile: {
              display_name: fd.get('display_name'),
              bio: fd.get('bio'),
              avatar_url: fd.get('avatar_url'),
              banner_url: fd.get('banner_url'),
              city: fd.get('city'),
              country: fd.get('country'),
              music_roles: String(fd.get('music_roles') || '').split(',').map((s) => s.trim()).filter(Boolean),
              genres: String(fd.get('genres') || '').split(',').map((s) => s.trim()).filter(Boolean),
              equipped_frame: fd.get('equipped_frame') || 'default'
            }
          }
        });
        closeModal();
        toast('Профиль CXRNER ID сохранён!');
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
        toast(res.type === 'promo' ? `Промокод активирован: +${res.cxp} CXP!` : `Подарок получен: ${res.item?.item_name}!`);
        await loadDropScreen();
      } catch (err) { toast(err.message); }
      return;
    }

    if (e.target.id === 'linkEditorForm') {
      e.preventDefault();
      const fd = new FormData(e.target);
      const platforms = {};
      ['yandex', 'vk', 'spotify', 'apple', 'youtube', 'deezer', 'zvuk', 'soundcloud'].forEach((k) => {
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
        state.editingLink = null;
        toast('CXRNER LINK сохранён!');
        loadLinkEditor();
      } catch (err) { toast(err.message); }
      return;
    }

    if (e.target.id === 'verificationForm') {
      e.preventDefault();
      const fd = new FormData(e.target);
      try {
        await api('/api/miniapp/platform/verification', {
          method: 'POST',
          body: Object.fromEntries(fd.entries())
        });
        toast('Заявка на верификацию отправлена!');
        if (state.homeData) state.homeData.verification_status = 'pending_review';
        renderVerification();
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
        toast(`Отправлено уведомлений: ${res.delivered}`);
      } catch (err) { toast(err.message); }
    }
  });

  window.addEventListener('load', () => {
    tg = window.Telegram?.WebApp || tg;
    try {
      tg?.ready();
      tg?.expand();
      tg?.onEvent?.('safeAreaChanged', applyTelegramSafeArea);
      tg?.onEvent?.('contentSafeAreaChanged', applyTelegramSafeArea);
      tg?.onEvent?.('viewportChanged', applyTelegramSafeArea);
      tg?.BackButton?.onClick(() => {
        if (!$('#releaseModal').classList.contains('is-hidden')) {
          closeModal();
        } else if (state.screen !== 'home') {
          state.history.pop();
          const prev = state.history[state.history.length - 1] || 'home';
          switchScreen(prev === state.screen ? 'home' : prev, false);
        }
      });
    } catch {}
    applyTelegramSafeArea();
    loadAll();
  });
})();
