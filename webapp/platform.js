/* CXRNER Platform UI. Keeps the legacy release form intact and adds the account platform. */
(function () {
  'use strict';

  const state = { tab: 'home', releases: [], profile: null, selected: null, comments: [], admin: null, search: '', sort: 'newest', filter: 'all' };
  const tg = window.Telegram?.WebApp;
  const ICONS = {
    bell: '<path d="M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4"/>',
    search: '<circle cx="11" cy="11" r="7"/><path d="m20 20-4-4"/>',
    home: '<path d="m3 10 9-7 9 7v10a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1z"/><path d="M9 21v-7h6v7"/>',
    music: '<path d="M9 18V5l11-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="17" cy="16" r="3"/>',
    user: '<circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0 1 16 0"/>',
    upload: '<path d="M12 16V4m0 0L7 9m5-5 5 5"/><path d="M5 14v5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-5"/>',
    heart: '<path d="M20.8 8.6c0 5.4-8.8 10.4-8.8 10.4S3.2 14 3.2 8.6A4.6 4.6 0 0 1 12 6.4a4.6 4.6 0 0 1 8.8 2.2Z"/>',
    star: '<path d="m12 3 2.8 5.7 6.2.9-4.5 4.4 1.1 6.2-5.6-3-5.6 3 1.1-6.2L3 9.6l6.2-.9z"/>',
    arrow: '<path d="M5 12h14m-6-6 6 6-6 6"/>',
    close: '<path d="m6 6 12 12M18 6 6 18"/>'
  };
  const icon = (name, size = 18) => `<svg class="ui-icon" width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${ICONS[name] || ICONS.music}</svg>`;
  const esc = (value) => String(value ?? '').replace(/[&<>'"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[c]));
  const money = (value) => new Intl.NumberFormat('ru-RU').format(Number(value || 0));
  const date = (value) => value ? new Date(value).toLocaleDateString('ru-RU') : '—';
  const initData = () => tg?.initData || '';
  async function api(path, options = {}) {
    const headers = { ...(options.headers || {}), 'x-telegram-init-data': initData() };
    if (options.body && typeof options.body !== 'string') { headers['content-type'] = 'application/json'; options.body = JSON.stringify(options.body); }
    const response = await fetch(path, { ...options, headers });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) throw new Error(data.error || `HTTP ${response.status}`);
    return data;
  }
  function toast(message) { const el = document.getElementById('toast'); if (el) { el.textContent = message; el.classList.remove('hidden'); setTimeout(() => el.classList.add('hidden'), 2600); } }
  function makeNav() {
    const nav = document.querySelector('.bottom-nav');
    if (!nav || nav.querySelector('[data-tab="platform"]')) return;
    const button = document.createElement('button');
    button.className = 'nav-item platform-nav'; button.dataset.tab = 'platform'; button.type = 'button';
    button.innerHTML = `<span class="nav-icon">${icon('music', 17)}</span><span>Платформа</span>`;
    nav.prepend(button);
    nav.style.gridTemplateColumns = 'repeat(6, minmax(0, 1fr))';
  }
  function mount() {
    const main = document.getElementById('app');
    if (!main || document.getElementById('platform')) return;
    const section = document.createElement('section'); section.id = 'platform'; section.className = 'view platform-view';
    section.innerHTML = `
      <div class="platform-shell">
        <div class="platform-heading"><div><p class="platform-kicker">CXRNER MUSIC PLATFORM</p><h2 id="platformTitle">Твоя музыкальная система</h2></div><button class="platform-icon-btn" data-platform-action="notifications" aria-label="Уведомления">${icon('bell', 19)}<span id="notificationDot"></span></button></div>
        <div class="platform-search"><span>${icon('search', 18)}</span><input id="platformSearch" placeholder="Искать релизы, артистов, UPC..."><button data-platform-action="search">Найти</button></div>
        <div id="platformContent"></div>
      </div>`;
    const nav = main.querySelector('.bottom-nav'); main.insertBefore(section, nav); makeNav();
    document.addEventListener('click', onClick); document.addEventListener('submit', onSubmit);
    document.getElementById('platformSearch').addEventListener('input', (event) => { state.search = event.target.value; if (state.tab === 'catalog') render(); });
  }
  function switchPlatformTab(tab) { state.tab = tab; render(); document.getElementById('platform')?.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
  function image(url, fallback = 'assets/general.jpg') { return esc(url || fallback); }
  function releaseCard(item) { return `<article class="platform-release-card" data-release="${esc(item.id)}"><div class="platform-cover"><img src="${image(item.cover)}" loading="lazy" onerror="this.src='assets/general.jpg'"><span class="release-status status-${esc(item.status)}">${esc(statusText(item.status))}</span></div><div class="platform-card-body"><h4>${esc(item.title)}</h4><p>${esc(item.artist)} · ${esc(item.genre || 'Музыка')}</p><small>${esc(item.type)} · ${esc(item.date || 'дата не указана')}</small><div class="platform-card-actions"><button data-release-open="${esc(item.id)}">Открыть</button><button data-engage="favorite" data-release-id="${esc(item.id)}">♡</button></div></div></article>`; }
  function statusText(status) { return ({ approved: 'Одобрено', published: 'Опубликовано', rejected: 'Отклонено', moderation: 'На проверке', on_moderation: 'На проверке', pending: 'Черновик' })[status] || status || 'Статус'; }
  function renderHome() {
    const latest = state.releases.slice(0, 6);
    const profile = state.profile?.user || {};
    return `<div class="platform-welcome"><div><p class="platform-kicker">${profile.first_name ? `С возвращением, ${esc(profile.first_name)}` : 'Добро пожаловать в CXRNER'}</p><h3>Выпускай музыку<br><span>своим темпом.</span></h3><p>Каталог релизов, кабинет артиста, аналитика и связь с модерацией — в одном Mini App.</p><div class="platform-actions"><button class="platform-primary" data-platform-tab="catalog">${icon('music', 17)} Открыть каталог</button><button class="platform-secondary" data-goto="submit">${icon('upload', 17)} Отправить релиз</button></div></div><div class="platform-orb">CXRNER<br><b>MUSIC</b></div></div><div class="platform-section-head"><h3>Последние релизы</h3><button data-platform-tab="catalog">Все релизы ${icon('arrow', 15)}</button></div><div class="platform-release-grid">${latest.length ? latest.map(releaseCard).join('') : empty('Релизы пока загружаются...')}</div><div class="platform-quick-grid"><button data-platform-tab="profile"><b>${icon('user', 19)}</b><span>Мой профиль</span><small>Статистика и достижения</small></button><button data-goto="submit"><b>${icon('upload', 19)}</b><span>Новая анкета</span><small>Пошаговая отправка</small></button><button data-platform-tab="favorites"><b>${icon('heart', 19)}</b><span>Избранное</span><small>Сохранённые релизы</small></button></div>`;
  }
  function renderCatalog() {
    let list = state.releases.filter((x) => `${x.title} ${x.artist} ${x.genre} ${x.upc}`.toLowerCase().includes(state.search.toLowerCase()));
    if (state.filter !== 'all') list = list.filter((x) => x.genre === state.filter);
    if (state.sort === 'title') list.sort((a, b) => a.title.localeCompare(b.title));
    const genres = [...new Set(state.releases.map((x) => x.genre).filter(Boolean))].slice(0, 12);
    return `<div class="platform-section-head"><div><h3>Каталог релизов</h3><p class="platform-muted">${list.length} релизов в витрине</p></div><div class="platform-controls"><select id="platformSort"><option value="newest">Новые</option><option value="title">По названию</option></select><select id="platformFilter"><option value="all">Все жанры</option>${genres.map((x) => `<option>${esc(x)}</option>`).join('')}</select></div></div><div class="platform-release-grid">${list.length ? list.map(releaseCard).join('') : empty('Ничего не найдено')}</div>`;
  }
  function renderProfile() {
    const p = state.profile || {}, u = p.user || {}, s = p.stats || {};
    return `<div class="profile-hero"><img src="${image(u.photo_url, 'assets/logo.png')}" onerror="this.src='assets/logo.png'"><div><p class="platform-kicker">${u.role === 'admin' ? 'ADMIN ACCOUNT' : 'ARTIST ACCOUNT'}</p><h3>${esc(u.first_name || 'Пользователь')} ${esc(u.last_name || '')}</h3><p>${u.username ? '@' + esc(u.username) : 'Username не указан'} · ID ${esc(u.telegram_id)}</p></div><span class="profile-role">${u.role === 'admin' ? 'Администратор' : 'Артист'}</span></div><div class="profile-stat-grid"><div><b>${money(s.releases)}</b><small>Релизов</small></div><div><b>${money(s.listens)}</b><small>Прослушиваний</small></div><div><b>${money(s.likes)}</b><small>Лайков</small></div><div><b>${money(s.favorites)}</b><small>Избранное</small></div></div><div class="platform-panel"><div class="platform-section-head"><h3>О аккаунте</h3><span class="release-status status-approved">${esc(u.status === 'active' ? 'Активен' : u.status || '—')}</span></div><p class="platform-muted">Регистрация: ${date(u.registered_at)}<br>Данные привязаны к Telegram и синхронизируются с ботом.</p></div><div class="platform-panel"><h3>Мои релизы</h3>${(p.releases || []).slice(0, 8).map((r) => `<div class="profile-release-row"><span>${esc(r.track_name || r.form_payload?.name || 'Без названия')}</span><span class="release-status status-${esc(r.status)}">${esc(statusText(r.status))}</span></div>`).join('') || empty('Анкет ещё нет')}</div><div class="platform-panel"><h3>Достижения и бейджи</h3><div class="badge-row">${(p.badges || []).map((b) => `<span class="badge-pill">✦ ${esc(b.badge_id)}</span>`).join('') || '<span class="platform-muted">Новые достижения появятся автоматически.</span>'}</div></div>${u.role === 'admin' ? '<button class="platform-primary full-width" data-platform-tab="admin">Открыть админ-панель</button>' : ''}`;
  }
  function renderFavorites() { const ids = new Set((state.profile?.engagements || []).filter((x) => x.kind === 'favorite').map((x) => x.release_id)); return `<div class="platform-section-head"><h3>Избранное</h3></div><div class="platform-release-grid">${state.releases.filter((x) => ids.has(x.id)).map(releaseCard).join('') || empty('Добавляй релизы в избранное')}</div>`; }
  function renderAdmin() { const a = state.admin; if (!a) return '<div class="platform-panel">Загрузка админ-панели...</div>'; const cards = [['users','Пользователи'],['releases','Релизы'],['forms','Анкеты'],['likes','Лайки'],['comments','Комментарии'],['payouts','Выплаты']]; return `<div class="platform-section-head"><div><p class="platform-kicker">CONTROL CENTER</p><h3>Админ-панель</h3></div><span class="release-status status-approved">Защищено Telegram ID</span></div><div class="admin-stat-grid">${cards.map(([key,label]) => `<div><b>${money(a.stats?.[key])}</b><small>${label}</small></div>`).join('')}</div><div class="platform-panel"><h3>Последние заявки</h3>${(a.forms || []).slice(0, 12).map((x) => `<div class="admin-row"><div><b>${esc(x.track_name || 'Без названия')}</b><small>${esc(x.artist_name || '')} · ${date(x.updated_at)}</small></div><span class="release-status status-${esc(x.status)}">${esc(statusText(x.status))}</span></div>`).join('') || empty('Заявок нет')}</div><div class="platform-panel"><h3>Разделы управления</h3><div class="admin-menu">${['Пользователи','Артисты','Релизы','Модерация','Новости','Комментарии','Лайки','Избранное','Бейджи','Достижения','Выплаты','Аналитика','Рефералы','Логи'].map((x) => `<button>${esc(x)} <span>→</span></button>`).join('')}</div></div>`; }
  function empty(text) { return `<div class="platform-empty">${esc(text)}</div>`; }
  function render() { const el = document.getElementById('platformContent'); if (!el) return; el.innerHTML = state.tab === 'home' ? renderHome() : state.tab === 'catalog' ? renderCatalog() : state.tab === 'profile' ? renderProfile() : state.tab === 'favorites' ? renderFavorites() : renderAdmin(); document.getElementById('platformTitle').textContent = ({ home: 'Твоя музыкальная система', catalog: 'Каталог CXRNER', profile: 'Личный профиль', favorites: 'Избранные релизы', admin: 'Админ-панель' })[state.tab]; const sort=document.getElementById('platformSort'); if(sort){sort.value=state.sort;sort.onchange=()=>{state.sort=sort.value;render();}} const filter=document.getElementById('platformFilter'); if(filter){filter.value=state.filter;filter.onchange=()=>{state.filter=filter.value;render();}} }
  async function openRelease(id) { const item = state.releases.find((x) => x.id === id); if (!item) return; state.selected = item; const modal = document.getElementById('releaseModal'), body = document.getElementById('modalBody'); body.innerHTML = `<img class="cover-img loaded" src="${image(item.cover)}" onerror="this.src='assets/general.jpg'"><p class="platform-kicker">${esc(item.genre || 'RELEASE')}</p><h3>${esc(item.title)}</h3><p class="release-artist">${esc(item.artist)}</p><p class="platform-muted">${esc(item.description || 'Описание релиза появится после публикации.')}<br>UPC: ${esc(item.upc || '—')} · Дата: ${esc(item.date || '—')}</p><div class="platform-modal-actions"><button class="btn btn-neon" data-engage="like" data-release-id="${esc(item.id)}">${icon('heart', 16)} Лайк</button><button class="btn btn-ghost" data-engage="favorite" data-release-id="${esc(item.id)}">${icon('star', 16)} Сохранить</button></div><div class="stream-grid">${Object.entries(item.dsp_links || {}).filter(([,v])=>v).map(([k,v])=>`<button class="btn btn-neon" data-stream-url="${esc(v)}">${icon('music', 15)} ${esc(k)}</button>`).join('')}</div><div class="platform-comments"><h4>Комментарии</h4><div id="commentsList">Загрузка...</div><form class="comment-form"><input name="body" maxlength="2000" placeholder="Написать комментарий..." required><button>Отправить</button></form></div>`; modal.classList.remove('hidden'); const result=await fetch(`/api/miniapp/platform/comments?release_id=${encodeURIComponent(item.id)}`,{headers:{'x-telegram-init-data':initData()}}).then((x)=>x.json()).catch(()=>({})); state.comments=result.comments||[]; const list=document.getElementById('commentsList'); if(list) list.innerHTML=state.comments.map((x)=>`<p class="comment"><b>${esc(x.telegram_id)}</b> ${esc(x.body)}<small>${date(x.created_at)}</small></p>`).join('')||'<span class="platform-muted">Будь первым.</span>'; }
  async function load() { try { const [catalog, profile] = await Promise.all([api('/api/miniapp/platform/catalog'), api('/api/miniapp/platform/profile')]); state.releases=catalog.releases||[]; state.profile=profile; render(); } catch (error) { console.error('[platform]', error); state.releases=[]; render(); toast('Открой Mini App из Telegram для авторизации'); } }
  async function onClick(event) { const tab=event.target.closest('[data-platform-tab]'); if(tab){switchPlatformTab(tab.dataset.platformTab);return;} const nav=event.target.closest('[data-tab="platform"]'); if(nav){switchPlatformTab('home');return;} const open=event.target.closest('[data-release-open]'); if(open){openRelease(open.dataset.releaseOpen);return;} const engage=event.target.closest('[data-engage]'); if(engage){const id=engage.dataset.releaseId;const kind=engage.dataset.engage;try{await api('/api/miniapp/platform/engagement',{method:'POST',body:{init_data:initData(),release_id:id,kind,active:true}});toast(kind==='like'?'Лайк поставлен':'Добавлено в избранное');const p=await api('/api/miniapp/platform/profile');state.profile=p;if(state.tab==='favorites')render();}catch(e){toast(e.message);}return;} const action=event.target.closest('[data-platform-action="notifications"]');if(action){switchPlatformTab('profile');toast('Центр уведомлений находится в профиле');} }
  async function onSubmit(event) { if(!event.target.matches('.comment-form'))return; event.preventDefault();const body=new FormData(event.target).get('body');try{await api('/api/miniapp/platform/comments',{method:'POST',body:{init_data:initData(),release_id:state.selected.id,body}});event.target.reset();toast('Комментарий добавлен');openRelease(state.selected.id);}catch(e){toast(e.message);} }
  window.addEventListener('load', () => { mount(); setTimeout(() => { if (tg) { tg.ready(); tg.expand(); } load(); if (window.switchTab) window.switchTab('platform'); }, 650); });
})();
