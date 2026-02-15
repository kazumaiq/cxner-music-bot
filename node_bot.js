'use strict';

const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');

const ROOT = __dirname;
const CFG = loadJson('deploy_config.json', {});

function clean(v) { return String(v ?? '').trim(); }
function esc(v) {
  return String(v ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}
function loadJson(file, fallback) {
  try {
    const p = path.resolve(ROOT, file);
    if (!fs.existsSync(p)) return fallback;
    return JSON.parse(fs.readFileSync(p, 'utf8'));
  } catch { return fallback; }
}
function saveJson(file, data) {
  const p = path.resolve(ROOT, file);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const tmp = `${p}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
  fs.renameSync(tmp, p);
}
function envStr(name, def = '') {
  const ev = clean(process.env[name]);
  if (ev) return ev;
  const cv = clean(CFG[name]);
  if (cv) return cv;
  return def;
}
function envInt(name, def) {
  const raw = clean(process.env[name] ?? CFG[name]);
  if (!raw) return def;
  const n = Number.parseInt(raw, 10);
  return Number.isFinite(n) ? n : def;
}
function envBool(name, def = false) {
  const raw = clean(process.env[name] ?? CFG[name]).toLowerCase();
  if (!raw) return def;
  return ['1', 'true', 'yes', 'on'].includes(raw);
}

const TOKEN = envStr('BOT_TOKEN') || envStr('TOKEN') || envStr('bot_token');
if (!TOKEN) {
  console.error('BOT_TOKEN is missing.');
  process.exit(1);
}
const MOD_CHAT = envInt('MODERATION_CHAT_ID', -1002117586464);
const BASE = envStr('PUBLIC_BASE_URL', '');
const WEBAPP_URL = envStr('WEBAPP_URL', BASE ? `${BASE.replace(/\/+$/, '')}/index.html` : '');
const WEB_HOST = envStr('WEB_SERVER_HOST', '0.0.0.0');
const WEB_PORT = envInt('PORT', envInt('WEB_SERVER_PORT', 8080));
const WEB_DIR = envStr('WEB_SERVER_DIR', 'webapp');
const WEB_ENABLED = envBool('ENABLE_WEB_SERVER', true);

const DB_FILE = 'releases.json';
const MOD_DB_FILE = 'moderation_releases.json';
const CAB_FILE = 'cabinet_users.json';
const EXP_REL = 'webapp/data/releases-public.json';
const EXP_CAB = 'webapp/data/cabinet-users.json';

const STATUS = {
  ON_UPLOAD: 'on_upload',
  MODERATION: 'moderation',
  APPROVED: 'approved',
  REJECTED: 'rejected',
  NEEDS_FIX: 'needs_fix',
  DELETED: 'deleted'
};
const STATUS_TEXT = {
  [STATUS.ON_UPLOAD]: 'На отгрузке',
  [STATUS.MODERATION]: 'На модерации',
  [STATUS.APPROVED]: 'Одобрено',
  [STATUS.REJECTED]: 'Отклонено',
  [STATUS.NEEDS_FIX]: 'На исправлении',
  [STATUS.DELETED]: 'Удалено'
};
const STATUS_EMOJI = {
  [STATUS.ON_UPLOAD]: '🕓',
  [STATUS.MODERATION]: '🧠',
  [STATUS.APPROVED]: '✅',
  [STATUS.REJECTED]: '❌',
  [STATUS.NEEDS_FIX]: '✏️',
  [STATUS.DELETED]: '🗑'
};

let db = loadJson(DB_FILE, {});
let modDb = loadJson(MOD_DB_FILE, { moderation_messages: [] });
let cabUsers = loadJson(CAB_FILE, {});

const API = `https://api.telegram.org/bot${TOKEN}`;
async function tg(method, payload = {}) {
  const r = await fetch(`${API}/${method}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j.ok) throw new Error(`[${method}] ${j.description || r.statusText}`);
  return j.result;
}
function saveDb() {
  saveJson(DB_FILE, db);
  exportReleases();
}
function saveModDb() { saveJson(MOD_DB_FILE, modDb); }
function saveCab() {
  saveJson(CAB_FILE, cabUsers);
  exportCabinet();
}
function exportReleases() {
  const out = { updated_at: new Date().toISOString(), users: {} };
  for (const [uid, items] of Object.entries(db || {})) {
    const list = Array.isArray(items) ? items : [];
    out.users[uid] = list.map((r, i) => ({
      id: i,
      type: r.type || '',
      name: r.name || '',
      subname: r.subname || '',
      nick: r.nick || '',
      date: r.date || '',
      genre: r.genre || '',
      status: r.status || STATUS.ON_UPLOAD,
      submission_time: r.submission_time || '',
      moderation_time: r.moderation_time || '',
      reject_reason: r.reject_reason || '',
      moderator_comment: r.moderator_comment || '',
      upc: r.upc || '',
      link_published: r.link_published || '',
      source: r.source || 'node_bot',
      user_deleted: !!r.user_deleted
    }));
  }
  saveJson(EXP_REL, out);
}
function exportCabinet() {
  const out = { updated_at: new Date().toISOString(), users: {} };
  for (const [uid, info] of Object.entries(cabUsers || {})) {
    out.users[uid] = {
      approved: !!info.approved,
      activated_at: info.activated_at || '',
      username: info.username || '',
      first_name: info.first_name || ''
    };
  }
  saveJson(EXP_CAB, out);
}

function keyboardMain() {
  return { inline_keyboard: [
    [{ text: '📀 Дистрибуция', callback_data: 'menu_distribution' }],
    [{ text: '💼 Сервисы', callback_data: 'menu_services' }],
    [{ text: '🧑‍💻 Кабинет', callback_data: 'menu_cabinet' }],
    [{ text: '🌐 Комьюнити', callback_data: 'menu_community' }],
    [{ text: 'Открыть приложение', callback_data: 'open_app' }]
  ]};
}
function keyboardDist() {
  return { inline_keyboard: [
    [{ text: 'Загрузить релиз', callback_data: 'report' }],
    [{ text: 'Мои релизы', callback_data: 'my_releases' }],
    [{ text: '⬅️ Главное меню', callback_data: 'main' }]
  ]};
}
function keyboardServices() {
  return { inline_keyboard: [
    [{ text: 'Заказать обложку (500р)', callback_data: 'service_cover' }],
    [{ text: 'Промо-текст под релиз', callback_data: 'service_promo' }],
    [{ text: '⬅️ Главное меню', callback_data: 'main' }]
  ]};
}
function keyboardCabinet() {
  return { inline_keyboard: [
    [{ text: 'Мои релизы', callback_data: 'my_releases' }],
    [{ text: 'Открыть приложение', callback_data: 'open_app' }],
    [{ text: '⬅️ Главное меню', callback_data: 'main' }]
  ]};
}
function keyboardCommunity() {
  return { inline_keyboard: [
    [{ text: 'Канал CXRNER MUSIC', url: 'https://t.me/cxrnermusic' }],
    [{ text: 'Чат артистов', url: 'https://t.me/+oVmX3_dkyWJhNjJi' }],
    [{ text: 'Официальный сайт', url: 'https://cxrnermusic.vercel.app/' }],
    [{ text: '⬅️ Главное меню', callback_data: 'main' }]
  ]};
}
function webappReplyKeyboard() {
  return {
    keyboard: [[{ text: 'Открыть приложение', web_app: { url: WEBAPP_URL } }]],
    resize_keyboard: true,
    one_time_keyboard: true
  };
}
function moderationKeyboard(uid, idx) {
  return { inline_keyboard: [
    [
      { text: '🕓 На отгрузке', callback_data: `m_upload_${uid}_${idx}` },
      { text: '🧠 Модерация', callback_data: `m_moderate_${uid}_${idx}` },
      { text: '✅ Принято', callback_data: `m_approve_${uid}_${idx}` }
    ],
    [
      { text: '❌ Отклонить', callback_data: `m_reject_${uid}_${idx}` },
      { text: '✏️ На исправлении', callback_data: `m_needfix_${uid}_${idx}` },
      { text: '🗑 Удалено', callback_data: `m_delete_${uid}_${idx}` }
    ]
  ]};
}

function sendText(chatId, text, extra = {}) {
  return tg('sendMessage', { chat_id: chatId, text, parse_mode: 'HTML', disable_web_page_preview: true, ...extra });
}
function welcomeText() {
  return 'Добро пожаловать в систему дистрибуции CXRNER MUSIC.\nУправляй релизами. Загружай треки. Масштабируй звук.';
}

function normalizeType(v) {
  const t = clean(v).toLowerCase();
  if (['сингл', 'single', 'singl'].includes(t)) return 'сингл';
  if (['альбом', 'album'].includes(t)) return 'альбом';
  return null;
}
function parseRuDate(v) {
  const m = /^(\d{2})\.(\d{2})\.(\d{4})$/.exec(clean(v));
  if (!m) return null;
  const d = Number(m[1]); const mm = Number(m[2]); const y = Number(m[3]);
  const dt = new Date(y, mm - 1, d, 12, 0, 0, 0);
  if (dt.getFullYear() !== y || dt.getMonth() !== mm - 1 || dt.getDate() !== d) return null;
  return dt;
}
function isHttpUrl(v) {
  try { const u = new URL(clean(v)); return u.protocol === 'http:' || u.protocol === 'https:'; }
  catch { return false; }
}
function fmtForm(user, uid, r) {
  const u = user?.username ? `@${user.username}` : 'нет';
  const lines = [
    '🎵 <b>НОВАЯ АНКЕТА!</b>',
    `От: ${esc(u)}`,
    `ID: <code>${esc(uid)}</code>`,
    `Тип: ${esc(r.type || '—')}`,
    '',
    `• <b>Название:</b> ${esc(r.name || '—')}`,
    `• <b>Саб-название:</b> ${esc(r.subname || '.')}`,
    `• <b>Ник:</b> ${esc(r.nick || '—')}`,
    `• <b>ФИО:</b> ${esc(r.fio || '—')}`,
    `• <b>Дата:</b> ${esc(r.date || '—')}`,
    `• <b>Версия:</b> ${esc(r.version || 'Оригинал')}`,
    `• <b>Жанр:</b> ${esc(r.genre || '—')}`,
    `• <b>Ссылка:</b> ${esc(r.link || '—')}`,
    `• <b>Яндекс Музыка:</b> ${esc(r.yandex || '.')}`,
    `• <b>Мат:</b> ${esc(r.mat || '—')}`,
    `• <b>Промо:</b> ${esc(r.promo || '.')}`,
    `• <b>Комментарий:</b> ${esc(r.comment || '.')}`
  ];
  if (r.type === 'альбом') lines.push(`• <b>Tracklist:</b> ${esc(r.tracklist || '.')}`);
  lines.push(`• <b>Tg:</b> ${esc(r.tg || '—')}`);
  return lines.join('\n');
}
function withStatus(status, original) {
  return `${STATUS_EMOJI[status] || '⏳'} <b>СТАТУС: ${esc(STATUS_TEXT[status] || status)}</b>\n\n${original || ''}`;
}
function parseWebappPayload(payload) {
  let action = clean(payload?.action);
  const hasForm = payload?.form && typeof payload.form === 'object';
  if (!action && hasForm) action = 'submit_release';
  let form = hasForm ? payload.form : null;
  if (!form && payload && typeof payload === 'object' && (payload.type || payload.name || payload.track_title)) {
    action = action || 'submit_release';
    form = payload;
  }
  if (!form) return { action, form: null };
  if (!form.type && (form.artist_name || form.track_title || form.release_date || form.telegram_contact)) {
    let date = clean(form.release_date || form.date);
    if (date.includes('-')) {
      const p = date.split('-');
      if (p.length === 3) date = `${p[2]}.${p[1]}.${p[0]}`;
    }
    const tr = clean(form.release_type || form.type || 'single').toLowerCase();
    const type = ['альбом', 'album'].includes(tr) ? 'альбом' : 'сингл';
    form = {
      type,
      name: form.track_title || form.name || '',
      subname: form.subname || '.',
      has_lyrics: form.has_lyrics || form.lyrics || 'Нет, это инструментал',
      nick: form.artist_name || form.nick || '',
      fio: form.artist_name || form.fio || '',
      date,
      version: form.version || 'Оригинал',
      genre: form.genre || '',
      link: form.link || form.files_link || form.audio_link || '.',
      yandex: form.yandex || form.yandex_link || '.',
      mat: form.mat || 'Нет',
      promo: form.promo || '.',
      comment: form.comment || '.',
      tracklist: form.tracklist || '.',
      tg: form.telegram_contact || form.contact || form.tg || ''
    };
  }
  return { action, form };
}
function validateForm(form) {
  const errors = [];
  const type = normalizeType(form?.type);
  if (!type) errors.push('Укажите тип релиза: сингл или альбом.');
  const name = clean(form?.name); if (!name) errors.push('Поле «Название релиза» обязательно.');
  const subname = clean(form?.subname) || '.';
  const hasLyrics = clean(form?.has_lyrics); if (!hasLyrics) errors.push('Укажите, есть ли слова в релизе.');
  const nick = clean(form?.nick); if (!nick) errors.push('Поле «Ник исполнителя» обязательно.');
  const fio = clean(form?.fio); if (!fio) errors.push('Поле «ФИО исполнителя» обязательно.');
  const date = clean(form?.date);
  if (!date) errors.push('Укажите дату релиза в формате ДД.ММ.ГГГГ.');
  else {
    const dt = parseRuDate(date);
    if (!dt) errors.push('Неверный формат даты. Используйте ДД.ММ.ГГГГ.');
    else {
      const minDays = type === 'альбом' ? 7 : 3;
      const minDate = new Date(); minDate.setHours(0,0,0,0); minDate.setDate(minDate.getDate() + minDays);
      if (dt < minDate) errors.push(`Дата релиза должна быть минимум через ${minDays} дней.`);
    }
  }
  const version = clean(form?.version) || 'Оригинал';
  const genre = clean(form?.genre); if (!genre) errors.push('Поле «Жанр» обязательно.');
  const link = clean(form?.link);
  if (!link) errors.push('Добавьте ссылку на файлы.');
  else if (!isHttpUrl(link)) errors.push('Ссылка на файлы должна начинаться с http:// или https://.');
  let yandex = clean(form?.yandex);
  if (!yandex || ['-', 'нет', 'none'].includes(yandex.toLowerCase())) yandex = '.';
  else if (!isHttpUrl(yandex)) errors.push('Поле «Яндекс Музыка» должно быть URL или точкой.');
  const mat = clean(form?.mat); if (!mat) errors.push('Укажите, есть ли ненормативная лексика.');
  const promo = clean(form?.promo) || '.';
  const comment = clean(form?.comment) || '.';
  let tracklist = clean(form?.tracklist) || '.';
  if (type !== 'альбом') tracklist = '.';
  if (type === 'альбом' && tracklist === '.') errors.push('Для альбома заполните Tracklist.');
  const tgContact = clean(form?.tg); if (!tgContact) errors.push('Укажите контакт Telegram.');
  return { errors, data: { type, name, subname, has_lyrics: hasLyrics, nick, fio, date, version, genre, link, yandex, mat, promo, comment, tracklist, tg: tgContact } };
}

async function sendWebappButton(chatId) {
  if (!WEBAPP_URL || WEBAPP_URL.includes('example.com')) {
    await sendText(chatId, '❌ WEBAPP_URL не настроен.');
    return;
  }
  await sendText(chatId, '🎵 Открытие Mini App\n\nНажмите кнопку ниже.', { reply_markup: webappReplyKeyboard() });
}
async function sendMy(chatId, uid) {
  const rel = Array.isArray(db?.[uid]) ? db[uid].filter((x) => !x?.user_deleted) : [];
  if (!rel.length) {
    await sendText(chatId, '🎵 <b>Мой кабинет</b>\n\nРелизов пока нет.');
    return;
  }
  const total = rel.length;
  const c = {
    [STATUS.ON_UPLOAD]: rel.filter((x) => x.status === STATUS.ON_UPLOAD).length,
    [STATUS.MODERATION]: rel.filter((x) => x.status === STATUS.MODERATION).length,
    [STATUS.APPROVED]: rel.filter((x) => x.status === STATUS.APPROVED).length,
    [STATUS.REJECTED]: rel.filter((x) => x.status === STATUS.REJECTED).length,
    [STATUS.NEEDS_FIX]: rel.filter((x) => x.status === STATUS.NEEDS_FIX).length
  };
  let text = `🎵 <b>Мой кабинет</b>\nВсего релизов: <b>${total}</b>\n✅ Одобрено: <b>${c[STATUS.APPROVED]}</b>\n🕓 На отгрузке: <b>${c[STATUS.ON_UPLOAD]}</b>\n🧠 На модерации: <b>${c[STATUS.MODERATION]}</b>\n✏️ На исправлении: <b>${c[STATUS.NEEDS_FIX]}</b>\n❌ Отклонено: <b>${c[STATUS.REJECTED]}</b>\n\n`;
  for (const r of rel.slice(-10).reverse()) {
    text += `${STATUS_EMOJI[r.status] || '⏳'} <b>${esc(r.name || 'Без названия')}</b>\n`;
    text += `👤 ${esc(r.nick || '—')} • 📅 ${esc(r.date || '—')}\nСтатус: ${esc(STATUS_TEXT[r.status] || r.status)}\n`;
    if (r.reject_reason) text += `Причина: ${esc(r.reject_reason)}\n`;
    text += '\n';
  }
  await sendText(chatId, text);
}

async function processWebAppData(msg) {
  const raw = msg.web_app_data?.data || '';
  let payload;
  try { payload = JSON.parse(raw); }
  catch {
    await sendText(msg.chat.id, '❌ Не удалось распознать данные Mini App.');
    return;
  }
  const uid = String(msg.from?.id || '');
  const user = msg.from;
  const parsed = parseWebappPayload(payload);
  const action = parsed.action;
  console.info(`[WEBAPP] action=${action || '-'} user_id=${uid || '-'} bytes=${Buffer.byteLength(raw, 'utf8')}`);

  if (action === 'cabinet_activate') {
    cabUsers[uid] = {
      approved: true,
      activated_at: new Date().toISOString(),
      username: user?.username || '',
      first_name: user?.first_name || ''
    };
    saveCab();
    await sendText(msg.chat.id, '✅ <b>Личный кабинет активирован</b>');
    return;
  }

  if (!['webapp_release_submit', 'submit_release', ''].includes(action)) {
    await sendText(msg.chat.id, '✅ Данные Mini App получены.');
    return;
  }

  if (!parsed.form || typeof parsed.form !== 'object') {
    await sendText(msg.chat.id, '❌ Ошибка данных формы. Отправьте анкету еще раз.');
    return;
  }

  const vr = validateForm(parsed.form);
  if (vr.errors.length) {
    const list = vr.errors.slice(0, 8).map((e) => `• ${esc(e)}`).join('\n');
    await sendText(msg.chat.id, `❌ <b>Анкета Mini App не отправлена</b>\n\n${list}`);
    return;
  }

  db[uid] = Array.isArray(db[uid]) ? db[uid] : [];
  const idx = db[uid].length;
  const rel = {
    ...vr.data,
    status: STATUS.ON_UPLOAD,
    source: 'mini_app',
    submission_time: new Date().toISOString(),
    username: user?.username || ''
  };
  const orig = fmtForm(user, uid, rel);
  try {
    const sent = await tg('sendMessage', {
      chat_id: MOD_CHAT,
      text: withStatus(rel.status, orig),
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      reply_markup: moderationKeyboard(uid, idx)
    });
    rel.moderation_message_id = sent.message_id;
    rel.moderation_original_text = orig;
    db[uid].push(rel);
    saveDb();
    modDb.moderation_messages = Array.isArray(modDb.moderation_messages) ? modDb.moderation_messages : [];
    modDb.moderation_messages.push({ ...rel, user_id: uid, idx, message_id: sent.message_id });
    saveModDb();
    try { await tg('pinChatMessage', { chat_id: MOD_CHAT, message_id: sent.message_id }); } catch {}
    await sendText(msg.chat.id, '✅ <b>Анкета отправлена в модерацию</b>');
  } catch (e) {
    console.error('[WEBAPP] submit failed:', e.message || e);
    await sendText(msg.chat.id, '❌ Не удалось отправить анкету в модерацию.');
  }
}

async function canModerate(userId) {
  try {
    const m = await tg('getChatMember', { chat_id: MOD_CHAT, user_id: Number(userId) });
    const s = clean(m?.status).toLowerCase();
    return !['left', 'kicked'].includes(s);
  } catch { return false; }
}
async function applyModeration(query, action, uid, idx) {
  if (Number(query?.message?.chat?.id || 0) !== Number(MOD_CHAT)) {
    await tg('answerCallbackQuery', { callback_query_id: query.id, text: 'Только в группе модерации.', show_alert: true });
    return;
  }
  if (!(await canModerate(query.from.id))) {
    await tg('answerCallbackQuery', { callback_query_id: query.id, text: 'Доступ только участникам группы модерации.', show_alert: true });
    return;
  }
  const list = Array.isArray(db?.[uid]) ? db[uid] : null;
  if (!list || !list[idx]) {
    await tg('answerCallbackQuery', { callback_query_id: query.id, text: 'Релиз не найден.', show_alert: true });
    return;
  }
  const map = {
    upload: STATUS.ON_UPLOAD,
    moderate: STATUS.MODERATION,
    approve: STATUS.APPROVED,
    reject: STATUS.REJECTED,
    needfix: STATUS.NEEDS_FIX,
    delete: STATUS.DELETED
  };
  const st = map[action];
  const rel = list[idx];
  rel.status = st;
  rel.moderation_time = new Date().toISOString();
  rel.moderator = String(query.from.id);
  rel.moderator_username = query.from.username || query.from.first_name || 'moderator';
  if (st === STATUS.REJECTED && !rel.reject_reason) rel.reject_reason = 'Отклонено модератором';
  if (st === STATUS.NEEDS_FIX && !rel.moderator_comment) rel.moderator_comment = 'Нужны правки перед публикацией';
  if (st === STATUS.DELETED) rel.user_deleted = true;
  saveDb();
  modDb.moderation_messages = Array.isArray(modDb.moderation_messages) ? modDb.moderation_messages : [];
  for (const it of modDb.moderation_messages) {
    if (String(it.user_id) === String(uid) && (Number(it.idx) === Number(idx) || it.submission_time === rel.submission_time)) {
      it.status = rel.status;
      it.moderation_time = rel.moderation_time;
      it.reject_reason = rel.reject_reason || '';
      it.moderator_comment = rel.moderator_comment || '';
      it.user_deleted = !!rel.user_deleted;
    }
  }
  saveModDb();
  const orig = rel.moderation_original_text || fmtForm({ username: rel.username || '' }, uid, rel);
  try {
    await tg('editMessageText', {
      chat_id: MOD_CHAT,
      message_id: Number(rel.moderation_message_id || query.message.message_id),
      text: withStatus(st, orig),
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      reply_markup: moderationKeyboard(uid, idx)
    });
  } catch (e) {
    console.error('[MODERATION] edit failed:', e.message || e);
  }
  try {
    let note = `${STATUS_EMOJI[st] || '⏳'} <b>${esc(STATUS_TEXT[st] || st)}</b>\n\n🎵 <b>${esc(rel.name || 'Релиз')}</b>\n👤 ${esc(rel.nick || '—')}\n📅 ${esc(rel.date || '—')}`;
    if (st === STATUS.REJECTED && rel.reject_reason) note += `\nПричина: ${esc(rel.reject_reason)}`;
    await sendText(Number(uid), note);
  } catch (e) {
    console.error('[MODERATION] notify failed:', e.message || e);
  }
  await tg('answerCallbackQuery', { callback_query_id: query.id, text: `Статус: ${STATUS_TEXT[st] || st}` });
}

async function onMessage(msg) {
  if (msg.web_app_data?.data) { await processWebAppData(msg); return; }
  const text = clean(msg.text);
  const chatId = msg.chat?.id;
  const uid = String(msg.from?.id || '');
  if (!chatId || !text) return;
  if (text === '/start' || text.startsWith('/start ')) {
    await sendText(chatId, welcomeText(), { reply_markup: keyboardMain() });
    return;
  }
  if (text === '/app' || text === 'Открыть приложение') {
    await sendWebappButton(chatId);
    return;
  }
  if (text === '/my' || text === '/my_releases') {
    await sendMy(chatId, uid);
  }
}
async function onCallback(query) {
  const data = clean(query.data);
  const chatId = query?.message?.chat?.id;
  if (!data || !chatId) return;
  try { await tg('answerCallbackQuery', { callback_query_id: query.id }); } catch {}
  const edit = (text, markup) => tg('editMessageText', {
    chat_id: chatId,
    message_id: query.message.message_id,
    text,
    parse_mode: 'HTML',
    disable_web_page_preview: true,
    reply_markup: markup
  });
  if (data === 'main') { await edit(welcomeText(), keyboardMain()); return; }
  if (data === 'menu_distribution') { await edit('<b>Дистрибуция</b>\n\nВыберите действие:', keyboardDist()); return; }
  if (data === 'menu_services') { await edit('<b>Сервисы</b>\n\nВыберите действие:', keyboardServices()); return; }
  if (data === 'menu_cabinet') { await edit('<b>Кабинет</b>\n\nВыберите действие:', keyboardCabinet()); return; }
  if (data === 'menu_community') { await edit('<b>Комьюнити</b>\n\nОфициальные площадки CXRNER MUSIC:', keyboardCommunity()); return; }
  if (data === 'open_app' || data === 'report') { await sendWebappButton(chatId); return; }
  if (data === 'my_releases') { await sendMy(chatId, String(query.from.id)); return; }
  if (data === 'service_cover') { await sendText(chatId, 'Заказ обложки: напишите менеджеру @cxrnermusic.'); return; }
  if (data === 'service_promo') { await sendText(chatId, 'Промо-текст: напишите менеджеру @cxrnermusic.'); return; }
  const m = /^m_(upload|moderate|approve|reject|needfix|delete)_(\d+)_(\d+)$/.exec(data);
  if (m) {
    await applyModeration(query, m[1], m[2], Number.parseInt(m[3], 10));
  }
}

function startStaticServer() {
  if (!WEB_ENABLED) return;
  const root = path.resolve(ROOT, WEB_DIR);
  if (!fs.existsSync(root) || !fs.statSync(root).isDirectory()) return;
  const ct = {
    '.html': 'text/html; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.svg': 'image/svg+xml',
    '.webp': 'image/webp'
  };
  const srv = http.createServer((req, res) => {
    try {
      const u = new URL(req.url || '/', 'http://localhost');
      let p = decodeURIComponent(u.pathname || '/');
      if (p === '/') p = '/index.html';
      const f = path.normalize(path.join(root, p));
      if (!f.startsWith(root) || !fs.existsSync(f) || !fs.statSync(f).isFile()) {
        res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
        res.end('Not found');
        return;
      }
      res.writeHead(200, { 'content-type': ct[path.extname(f).toLowerCase()] || 'application/octet-stream' });
      fs.createReadStream(f).pipe(res);
    } catch {
      res.writeHead(500, { 'content-type': 'text/plain; charset=utf-8' });
      res.end('Server error');
    }
  });
  srv.listen(WEB_PORT, WEB_HOST, () => {
    console.info(`[web] static server started: http://${WEB_HOST}:${WEB_PORT} (dir: ${root})`);
  });
}

let offset = 0;
let stopping = false;
async function loop() {
  while (!stopping) {
    try {
      const updates = await tg('getUpdates', { timeout: 50, offset, allowed_updates: ['message', 'callback_query'] });
      for (const u of updates) {
        offset = Number(u.update_id) + 1;
        try {
          if (u.message) await onMessage(u.message);
          else if (u.callback_query) await onCallback(u.callback_query);
        } catch (e) {
          console.error('[bot] update error:', e.message || e);
        }
      }
    } catch (e) {
      console.error('[bot] polling error:', e.message || e);
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
}

(async () => {
  fs.mkdirSync(path.resolve(ROOT, 'webapp/data'), { recursive: true });
  exportReleases();
  exportCabinet();
  startStaticServer();
  console.info('[bot] CXRNER Node fallback bot started');
  console.info(`[bot] moderation chat: ${MOD_CHAT}`);
  if (WEBAPP_URL) console.info(`[bot] webapp url: ${WEBAPP_URL}`);
  try { await tg('deleteWebhook', { drop_pending_updates: false }); } catch {}
  process.on('SIGINT', () => { stopping = true; process.exit(0); });
  process.on('SIGTERM', () => { stopping = true; process.exit(0); });
  await loop();
})();
