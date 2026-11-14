(() => {
  'use strict';

  const slug = decodeURIComponent(location.pathname.split('/').filter(Boolean).pop() || '');
  const app = document.getElementById('linkApp');
  const API_PREFIX = location.pathname.startsWith('/miniapp') ? '/miniapp' : '';
  const apiUrl = (path) => (API_PREFIX && path.startsWith('/api/') ? API_PREFIX + path : path);

  const escapeHtml = (value) =>
    String(value ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  const PLATFORM_META = {
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

  function generateFallbackCover(title, artist) {
    const seed = String(title || 'CX') + ':' + String(artist || 'MUSIC');
    let h = 2166136261;
    for (let i = 0; i < seed.length; i += 1) {
      h ^= seed.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    const hues = [268, 190, 295, 220, 330, 250];
    const h1 = hues[Math.abs(h) % hues.length];
    const h2 = hues[Math.abs(h >> 3) % hues.length];
    const mono = escapeHtml((String(title || 'CX').slice(0, 2)).toUpperCase());
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 400 400"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="hsl(${h1},65%,18%)"/><stop offset="100%" stop-color="hsl(${h2},75%,8%)"/></linearGradient></defs><rect width="400" height="400" fill="url(#g)"/><circle cx="200" cy="200" r="145" fill="none" stroke="rgba(255,255,255,0.12)" stroke-width="2"/><circle cx="200" cy="200" r="95" fill="none" stroke="rgba(0,240,255,0.28)" stroke-width="1.5"/><text x="200" y="218" text-anchor="middle" fill="#FFFFFF" font-family="sans-serif" font-weight="800" font-size="88">${mono}</text></svg>`;
    return 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svg);
  }

  function event(type, platform = '') {
    fetch(apiUrl('/api/miniapp/link/event'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        slug,
        event_type: type,
        platform,
        source: document.referrer || '',
        device: /Mobi|Android/i.test(navigator.userAgent) ? 'mobile' : 'desktop'
      })
    }).catch(() => {});
  }

  async function load() {
    const response = await fetch(apiUrl('/api/miniapp/link?slug=' + encodeURIComponent(slug)));
    const data = await response.json();
    if (!response.ok || !data.link) throw new Error(data.error || 'Ссылка не найдена');
    const link = data.link;
    const fallbackImg = generateFallbackCover(link.title, link.artist);
    document.title = `${link.title} — ${link.artist} · CXRNER LINK`;
    app.className = 'link-app theme-' + escapeHtml(link.theme || 'aurora');

    const platforms = Object.entries(link.platforms || {})
      .filter(([, url]) => url)
      .map(([name, url]) => {
        const meta = PLATFORM_META[name.toLowerCase()] || {
          name,
          bg: 'linear-gradient(135deg,#8737FF,#5B21B6)',
          fg: '#FFFFFF',
          svg: '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 3v10.55A4 4 0 1 0 14 17V7h4V3h-6Z"/></svg>'
        };
        return `<a class="platform-link" href="${escapeHtml(url)}" data-platform="${escapeHtml(name)}" target="_blank" rel="noopener">
          <span class="platform-icon" style="background:${meta.bg};color:${meta.fg}">${meta.svg}</span>
          <span class="platform-info"><strong>${escapeHtml(meta.name)}</strong><small>OFFICIAL DSP</small></span>
          <span class="platform-cta">Слушать ↗</span>
        </a>`;
      })
      .join('');

    const extras = (link.extra_links || [])
      .map(
        (item) => `<a class="platform-link" href="${escapeHtml(item.url)}" data-platform="extra" target="_blank" rel="noopener">
          <span class="platform-icon" style="background:rgba(255,255,255,0.08);color:#fff">↗</span>
          <span class="platform-info"><strong>${escapeHtml(item.label)}</strong><small>LINK</small></span>
          <span class="platform-cta">Открыть ↗</span>
        </a>`
      )
      .join('');

    const first = Object.entries(link.platforms || {}).find(([, url]) => url)?.[1] || '#';
    app.innerHTML = `
      <div class="link-top"><img src="/assets/logo.png" alt=""> CXRNER LINK</div>
      <div class="link-cover"><img src="${escapeHtml(link.cover_url || fallbackImg)}" alt="${escapeHtml(link.title)}"></div>
      <h1>${escapeHtml(link.title)}</h1>
      <p class="link-artist">${escapeHtml(link.artist)}</p>
      <p class="link-meta">${escapeHtml([link.genre, link.release_date].filter(Boolean).join(' · '))}</p>
      <a class="link-listen" href="${escapeHtml(first)}" data-platform="listen" target="_blank" rel="noopener">▶ Слушать релиз</a>
      ${platforms ? `<span class="section-label">МУЗЫКАЛЬНЫЕ ПЛОЩАДКИ</span><div class="platform-grid">${platforms}</div>` : '<p class="link-meta">Площадки пока не добавлены.</p>'}
      ${extras ? `<span class="section-label">ЕЩЁ ОТ АРТИСТА</span><div class="platform-grid">${extras}</div>` : ''}
      <div class="share-row">
        <button id="shareLink" type="button">Поделиться</button>
        <button id="copyLink" type="button">Скопировать</button>
      </div>
      <div class="link-footer">CXRNER MUSIC · OFFICIAL SMART LINK</div>
    `;

    app.querySelector('.link-cover img')?.addEventListener('error', (e) => {
      e.currentTarget.src = fallbackImg;
    });
    app.querySelectorAll('[data-platform]').forEach((node) => {
      node.addEventListener('click', () => event('click', node.dataset.platform));
    });
    document.getElementById('shareLink').onclick = async () => {
      if (navigator.share) await navigator.share({ title: link.title, text: link.artist, url: location.href }).catch(() => {});
      else { await navigator.clipboard?.writeText(location.href); alert('Ссылка скопирована'); }
    };
    document.getElementById('copyLink').onclick = async () => {
      await navigator.clipboard?.writeText(location.href);
      alert('Ссылка скопирована');
    };
    event('view');
    setTimeout(() => document.getElementById('linkSplash')?.remove(), 450);
  }

  load().catch((error) => {
    document.getElementById('linkSplash')?.remove();
    app.className = 'link-app';
    app.innerHTML = `<div class="link-error"><h1>Ссылка не найдена</h1><p>${escapeHtml(error.message)}</p></div>`;
  });
})();
