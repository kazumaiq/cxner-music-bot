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
let WEBAPP_URL = envStr('WEBAPP_URL', BASE ? `${BASE.replace(/\/+$/, '')}/index.html` : '');
if (/\.vercel\.app\/index\.html$/i.test(WEBAPP_URL)) {
  WEBAPP_URL = WEBAPP_URL.replace(/\/index\.html$/i, '/');
}
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
const userForms = {};
const coverSessions = {};
const promoSessions = {};

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
    [{ text: 'Загрузить релиз (анкета в боте)', callback_data: 'report_text' }],
    [{ text: 'Открыть Mini App', callback_data: 'report_app' }],
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

async function submitReleaseToModeration(user, uid, releaseData, source = 'mini_app') {
  db[uid] = Array.isArray(db[uid]) ? db[uid] : [];
  const idx = db[uid].length;
  const rel = {
    ...releaseData,
    status: STATUS.ON_UPLOAD,
    source,
    submission_time: new Date().toISOString(),
    username: user?.username || ''
  };
  const orig = fmtForm(user, uid, rel);
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
  return { idx, rel };
}

function getFormSession(uid) {
  return userForms[String(uid)] || null;
}

function resetFormSession(uid) {
  delete userForms[String(uid)];
}

function getCoverSession(uid) {
  return coverSessions[String(uid)] || null;
}

function resetCoverSession(uid) {
  delete coverSessions[String(uid)];
}

function getPromoSession(uid) {
  return promoSessions[String(uid)] || null;
}

function resetPromoSession(uid) {
  delete promoSessions[String(uid)];
}

function resetAllSessions(uid, except = '') {
  const key = String(uid);
  if (except !== 'release') delete userForms[key];
  if (except !== 'cover') delete coverSessions[key];
  if (except !== 'promo') delete promoSessions[key];
}

function createFormSession(uid, user) {
  resetAllSessions(uid, 'release');
  userForms[String(uid)] = {
    uid: String(uid),
    user,
    step: 'type',
    form: {
      type: '',
      name: '',
      subname: '.',
      has_lyrics: '',
      nick: '',
      fio: '',
      date: '',
      version: 'Оригинал',
      genre: '',
      link: '',
      yandex: '.',
      mat: '',
      promo: '.',
      comment: '.',
      tracklist: '.',
      tg: ''
    }
  };
  return userForms[String(uid)];
}

async function sendFormStep(chatId, uid) {
  const s = getFormSession(uid);
  if (!s) return;

  if (s.step === 'type') {
    await sendText(chatId, 'Выберите тип релиза:', {
      reply_markup: {
        inline_keyboard: [
          [
            { text: 'Сингл', callback_data: 'form_type_single' },
            { text: 'Альбом', callback_data: 'form_type_album' }
          ],
          [{ text: 'Отмена', callback_data: 'form_cancel' }]
        ]
      }
    });
    return;
  }
  if (s.step === 'name') { await sendText(chatId, 'Введите название релиза:'); return; }
  if (s.step === 'subname') { await sendText(chatId, 'Саб-название (если нет, отправьте точку "."):'); return; }
  if (s.step === 'has_lyrics') {
    await sendText(chatId, 'Есть слова в релизе?', {
      reply_markup: {
        inline_keyboard: [
          [
            { text: 'Да', callback_data: 'form_lyrics_yes' },
            { text: 'Нет, инструментал', callback_data: 'form_lyrics_no' }
          ],
          [{ text: 'Отмена', callback_data: 'form_cancel' }]
        ]
      }
    });
    return;
  }
  if (s.step === 'nick') { await sendText(chatId, 'Ник исполнителя:'); return; }
  if (s.step === 'fio') { await sendText(chatId, 'ФИО исполнителя:'); return; }
  if (s.step === 'date') { await sendText(chatId, 'Дата релиза в формате ДД.ММ.ГГГГ:'); return; }
  if (s.step === 'version') { await sendText(chatId, 'Версия релиза (или "Оригинал"):'); return; }
  if (s.step === 'genre') { await sendText(chatId, 'Жанр:'); return; }
  if (s.step === 'link') { await sendText(chatId, 'Ссылка на файлы (http/https):'); return; }
  if (s.step === 'yandex') { await sendText(chatId, 'Ссылка Яндекс Музыки или точка ".":'); return; }
  if (s.step === 'mat') {
    await sendText(chatId, 'Есть ненормативная лексика?', {
      reply_markup: {
        inline_keyboard: [
          [
            { text: 'Да', callback_data: 'form_mat_yes' },
            { text: 'Нет', callback_data: 'form_mat_no' }
          ],
          [{ text: 'Отмена', callback_data: 'form_cancel' }]
        ]
      }
    });
    return;
  }
  if (s.step === 'promo') { await sendText(chatId, 'Промо-текст (или точка "."):'); return; }
  if (s.step === 'comment') { await sendText(chatId, 'Комментарий (или точка "."):'); return; }
  if (s.step === 'tracklist') { await sendText(chatId, 'Tracklist для альбома (обязательно):'); return; }
  if (s.step === 'tg') { await sendText(chatId, 'Контакт Telegram для связи (@username):'); return; }
  if (s.step === 'confirm') {
    const preview = fmtForm(s.user, s.uid, s.form);
    await sendText(chatId, `Проверьте анкету:\n\n${preview}`, {
      reply_markup: {
        inline_keyboard: [
          [{ text: 'Отправить в модерацию', callback_data: 'form_send' }],
          [{ text: 'Отмена', callback_data: 'form_cancel' }]
        ]
      }
    });
  }
}

function parseYesNoText(value) {
  const v = clean(value).toLowerCase();
  if (['да', 'yes', 'y', '1'].includes(v)) return 'yes';
  if (['нет', 'no', 'n', '0'].includes(v)) return 'no';
  return '';
}

async function startTextForm(chatId, uid, user) {
  createFormSession(uid, user);
  await sendText(chatId, 'Запуск анкеты в боте.\n\nЧтобы отменить в любой момент: /cancel');
  await sendFormStep(chatId, uid);
}

async function handleFormTextMessage(msg) {
  const uid = String(msg.from?.id || '');
  const s = getFormSession(uid);
  if (!s) return false;

  const text = clean(msg.text);
  const chatId = msg.chat.id;

  if (!text) return true;
  if (text.startsWith('/') && text !== '/cancel') return false;
  if (text === '/cancel') {
    resetFormSession(uid);
    await sendText(chatId, 'Анкета отменена.');
    return true;
  }

  if (s.step === 'type') {
    const t = normalizeType(text);
    if (!t) {
      await sendText(chatId, 'Выберите тип кнопками или введите: сингл / альбом.');
      return true;
    }
    s.form.type = t;
    s.step = 'name';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'name') {
    if (!text) { await sendText(chatId, 'Название не может быть пустым.'); return true; }
    s.form.name = text;
    s.step = 'subname';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'subname') {
    s.form.subname = text || '.';
    s.step = 'has_lyrics';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'has_lyrics') {
    const yn = parseYesNoText(text);
    if (!yn) {
      await sendText(chatId, 'Ответьте "Да" или "Нет".');
      return true;
    }
    s.form.has_lyrics = yn === 'yes' ? 'Да' : 'Нет, это инструментал';
    s.step = 'nick';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'nick') {
    if (!text) { await sendText(chatId, 'Ник обязателен.'); return true; }
    s.form.nick = text;
    s.step = 'fio';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'fio') {
    if (!text) { await sendText(chatId, 'ФИО обязательно.'); return true; }
    s.form.fio = text;
    s.step = 'date';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'date') {
    if (!parseRuDate(text)) { await sendText(chatId, 'Неверный формат. Используйте ДД.ММ.ГГГГ.'); return true; }
    s.form.date = text;
    s.step = 'version';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'version') {
    s.form.version = text || 'Оригинал';
    s.step = 'genre';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'genre') {
    if (!text) { await sendText(chatId, 'Жанр обязателен.'); return true; }
    s.form.genre = text;
    s.step = 'link';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'link') {
    if (!isHttpUrl(text)) { await sendText(chatId, 'Нужен валидный URL (http/https).'); return true; }
    s.form.link = text;
    s.step = 'yandex';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'yandex') {
    if (text === '.' || text === '-') {
      s.form.yandex = '.';
    } else {
      if (!isHttpUrl(text)) { await sendText(chatId, 'Введите URL или точку ".".'); return true; }
      s.form.yandex = text;
    }
    s.step = 'mat';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'mat') {
    const yn = parseYesNoText(text);
    if (!yn) { await sendText(chatId, 'Ответьте "Да" или "Нет".'); return true; }
    s.form.mat = yn === 'yes' ? 'Да' : 'Нет';
    s.step = 'promo';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'promo') {
    s.form.promo = text || '.';
    s.step = 'comment';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'comment') {
    s.form.comment = text || '.';
    if (s.form.type === 'альбом') {
      s.step = 'tracklist';
    } else {
      s.form.tracklist = '.';
      s.step = 'tg';
    }
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'tracklist') {
    if (!text || text === '.') { await sendText(chatId, 'Для альбома tracklist обязателен.'); return true; }
    s.form.tracklist = text;
    s.step = 'tg';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'tg') {
    if (!text) { await sendText(chatId, 'Контакт Telegram обязателен.'); return true; }
    s.form.tg = text;
    s.step = 'confirm';
    await sendFormStep(chatId, uid);
    return true;
  }
  return true;
}

async function handleFormCallback(query, data) {
  if (!data.startsWith('form_')) return false;
  const uid = String(query.from.id);
  const s = getFormSession(uid);
  if (!s && data !== 'form_cancel') {
    await sendText(query.message.chat.id, 'Анкета не активна. Запустите её заново через «Загрузить релиз».');
    return true;
  }

  if (data === 'form_cancel') {
    resetFormSession(uid);
    await sendText(query.message.chat.id, 'Анкета отменена.');
    return true;
  }

  if (data === 'form_type_single' || data === 'form_type_album') {
    s.form.type = data === 'form_type_album' ? 'альбом' : 'сингл';
    s.step = 'name';
    await sendFormStep(query.message.chat.id, uid);
    return true;
  }

  if (data === 'form_lyrics_yes' || data === 'form_lyrics_no') {
    s.form.has_lyrics = data === 'form_lyrics_yes' ? 'Да' : 'Нет, это инструментал';
    s.step = 'nick';
    await sendFormStep(query.message.chat.id, uid);
    return true;
  }

  if (data === 'form_mat_yes' || data === 'form_mat_no') {
    s.form.mat = data === 'form_mat_yes' ? 'Да' : 'Нет';
    s.step = 'promo';
    await sendFormStep(query.message.chat.id, uid);
    return true;
  }

  if (data === 'form_send') {
    const vr = validateForm(s.form);
    if (vr.errors.length) {
      const list = vr.errors.slice(0, 5).map((e) => `• ${esc(e)}`).join('\n');
      await sendText(query.message.chat.id, `❌ Анкета не прошла проверку:\n${list}`);
      return true;
    }
    try {
      await submitReleaseToModeration(query.from, uid, vr.data, 'bot_text');
      resetFormSession(uid);
      await sendText(query.message.chat.id, '✅ Анкета отправлена в модерацию.');
    } catch (e) {
      await sendText(query.message.chat.id, '❌ Не удалось отправить анкету в модерацию.');
      console.error('[FORM] submit failed:', e.message || e);
    }
    return true;
  }

  return false;
}

function generatePromoPacket(data) {
  const artist = data.artist || 'Артист';
  const projectType = data.project_type || 'проект';
  const releaseName = data.release_name || 'Релиз';
  const releaseKind = data.release_kind || 'трек';
  const genreMain = data.genre_main || 'электроника';
  const genreExtra = (data.genre_extra && data.genre_extra !== '-') ? `, ${data.genre_extra}` : '';
  const mood = data.mood || 'динамичный';
  const vibe = data.vibe || 'энергичный';
  const sound = data.sound || 'современный';
  const vocal = data.vocal || 'instrumental';
  const emotion = data.emotion || 'эмоциональный';
  const country = data.country || '—';
  const usecase = data.usecase || 'в плейлистах и коротких видео';

  return [
    '<b>📝 ОПИСАНИЕ АРТИСТА (RU)</b>',
    '',
    `${esc(artist)} — ${esc(projectType)}, работающий в жанре ${esc(genreMain)}${esc(genreExtra)}.`,
    `Фокус на ${esc(vibe)} энергии, ${esc(sound)} звучании и ${esc(emotion)} атмосфере.`,
    '',
    '<b>📝 ОПИСАНИЕ РЕЛИЗА (RU)</b>',
    '',
    `${esc(releaseName)} — ${esc(releaseKind)} с ${esc(mood)} настроением.`,
    `Трек работает лучше всего: ${esc(usecase)}.`,
    '',
    '<b>🎵 SPOTIFY (EN, short)</b>',
    '',
    `${esc(artist)} is a ${esc(projectType)} in ${esc(genreMain)}${esc(genreExtra)},`,
    `focused on ${esc(sound)} sound and ${esc(vibe)} energy.`,
    `${esc(releaseName)} delivers ${esc(mood)} mood with ${esc(vocal)} style vocals.`,
    '',
    '<b>🎧 DEEZER (EN, extended)</b>',
    '',
    `${esc(artist)} explores ${esc(genreMain)}${esc(genreExtra)} through`,
    `${esc(sound)} production, ${esc(vibe)} drive and ${esc(emotion)} atmosphere.`,
    `${esc(releaseName)} fits perfectly ${esc(usecase)}.`,
    '',
    `🌍 Страна артиста: <b>${esc(country)}</b>`
  ].join('\n');
}

async function startCoverFlow(chatId, uid, user) {
  resetAllSessions(uid, 'cover');
  coverSessions[String(uid)] = {
    uid: String(uid),
    user,
    step: 'reference',
    data: {
      reference_text: '',
      reference_photo: '',
      colors: '',
      title: '',
      prefs: '',
      tg: ''
    }
  };
  await sendText(chatId, '📦 <b>Заказ обложки — шаг 1/6</b>\n\nОтправьте референс: текстом/ссылкой или фото.', {
    parse_mode: 'HTML'
  });
}

async function handleCoverMessage(msg) {
  const uid = String(msg.from?.id || '');
  const s = getCoverSession(uid);
  if (!s) return false;

  const chatId = msg.chat.id;
  const text = clean(msg.text);
  const hasPhoto = Array.isArray(msg.photo) && msg.photo.length > 0;

  if (text === '/cancel') {
    resetCoverSession(uid);
    await sendText(chatId, 'Заказ обложки отменён.');
    return true;
  }
  if (text.startsWith('/') && text !== '/cancel') return false;

  if (s.step === 'reference') {
    if (hasPhoto) {
      s.data.reference_photo = msg.photo[msg.photo.length - 1].file_id;
    } else if (text) {
      s.data.reference_text = text;
    } else {
      await sendText(chatId, 'Пришлите текст/ссылку или фото-референс.');
      return true;
    }
    s.step = 'colors';
    await sendText(chatId, '🎨 <b>Шаг 2/6</b>\n\nКакие основные цвета должны быть в обложке?', { parse_mode: 'HTML' });
    return true;
  }

  if (s.step === 'colors') {
    if (!text) { await sendText(chatId, 'Опишите цвета текстом.'); return true; }
    s.data.colors = text;
    s.step = 'title';
    await sendText(chatId, '✍️ <b>Шаг 3/6</b>\n\nНазвание релиза (как написать на обложке):', { parse_mode: 'HTML' });
    return true;
  }

  if (s.step === 'title') {
    if (!text) { await sendText(chatId, 'Введите название релиза.'); return true; }
    s.data.title = text;
    s.step = 'prefs';
    await sendText(chatId, '📝 <b>Шаг 4/6</b>\n\nВаши пожелания/комментарии по дизайну:', { parse_mode: 'HTML' });
    return true;
  }

  if (s.step === 'prefs') {
    s.data.prefs = text || '.';
    s.step = 'tg';
    await sendText(chatId, '📱 <b>Шаг 5/6</b>\n\nУкажите ваш Telegram для связи:', { parse_mode: 'HTML' });
    return true;
  }

  if (s.step === 'tg') {
    if (!text) { await sendText(chatId, 'Укажите Telegram для связи.'); return true; }
    s.data.tg = text;
    s.step = 'wait_screenshot';
    await sendText(chatId,
      '💳 <b>Шаг 6/6 — Оплата 500₽</b>\n\n' +
      'Карта MIR\n<code>2200 7004 9056 2443</code>\n\n' +
      'Карта VISA\n<code>4177 4901 8116 9097</code>\n\n' +
      'USDT TRC20\n<code>TW5awCiuhfpAoLGvu1WXXWzKHbgEEDbv1x</code>\n\n' +
      'После оплаты отправьте скриншот сюда.',
      { parse_mode: 'HTML' }
    );
    return true;
  }

  if (s.step === 'wait_screenshot') {
    if (!hasPhoto) {
      await sendText(chatId, 'Ожидаю фото-скриншот оплаты.');
      return true;
    }

    const caption =
      '📌 <b>ЗАКАЗ ОБЛОЖКИ</b>\n' +
      `От: @${esc(msg.from?.username || '')} (ID: <code>${esc(uid)}</code>)\n` +
      `Название: ${esc(s.data.title || '—')}\n` +
      `Цвета: ${esc(s.data.colors || '—')}\n` +
      `Пожелания: ${esc(s.data.prefs || '.')}\n` +
      `TG: ${esc(s.data.tg || '—')}\n` +
      `Референс: ${esc(s.data.reference_text || 'фото')}`;

    try {
      const sent = await tg('sendPhoto', {
        chat_id: MOD_CHAT,
        photo: msg.photo[msg.photo.length - 1].file_id,
        caption,
        parse_mode: 'HTML'
      });
      try { await tg('pinChatMessage', { chat_id: MOD_CHAT, message_id: sent.message_id }); } catch {}
      await sendText(chatId, '✅ Заказ обложки отправлен в модерацию.');
      resetCoverSession(uid);
    } catch (e) {
      console.error('[COVER] submit failed:', e.message || e);
      await sendText(chatId, '❌ Не удалось отправить заказ. Попробуйте ещё раз.');
    }
    return true;
  }

  return true;
}

async function startPromoFlow(chatId, uid, user) {
  resetAllSessions(uid, 'promo');
  promoSessions[String(uid)] = {
    uid: String(uid),
    user,
    step: 'artist',
    data: {}
  };
  await sendText(chatId, '📝 <b>Промо-текст — шаг 1/13</b>\n\nУкажите имя артиста:', { parse_mode: 'HTML' });
}

async function sendPromoStep(chatId, uid) {
  const s = getPromoSession(uid);
  if (!s) return;
  if (s.step === 'project_type') {
    await sendText(chatId, 'Укажите тип проекта:', {
      reply_markup: {
        inline_keyboard: [[
          { text: '🎤 Solo', callback_data: 'promo_project_solo' },
          { text: '🎵 Feat', callback_data: 'promo_project_feat' }
        ]]
      }
    });
    return;
  }
  if (s.step === 'release_name') { await sendText(chatId, 'Шаг 3/13: Название релиза:'); return; }
  if (s.step === 'release_kind') {
    await sendText(chatId, 'Шаг 4/13: Тип релиза:', {
      reply_markup: {
        inline_keyboard: [[
          { text: '🎵 Сингл', callback_data: 'promo_kind_single' },
          { text: '💿 EP', callback_data: 'promo_kind_ep' },
          { text: '📀 Альбом', callback_data: 'promo_kind_album' }
        ]]
      }
    });
    return;
  }
  if (s.step === 'genre_main') { await sendText(chatId, 'Шаг 5/13: Жанр (основной):'); return; }
  if (s.step === 'genre_extra') { await sendText(chatId, 'Шаг 6/13: +1 доп.жанр (или "-"):'); return; }
  if (s.step === 'mood') { await sendText(chatId, 'Шаг 7/13: Настроение (2-4 слова):'); return; }
  if (s.step === 'vibe') { await sendText(chatId, 'Шаг 8/13: Вайб / образ:'); return; }
  if (s.step === 'sound') { await sendText(chatId, 'Шаг 9/13: Звучание:'); return; }
  if (s.step === 'vocal') {
    await sendText(chatId, 'Шаг 10/13: Вокал:', {
      reply_markup: {
        inline_keyboard: [
          [{ text: '❌ Без вокала', callback_data: 'promo_vocal_no' }],
          [
            { text: '🎤 Мужской', callback_data: 'promo_vocal_male' },
            { text: '👩 Женский', callback_data: 'promo_vocal_female' }
          ]
        ]
      }
    });
    return;
  }
  if (s.step === 'emotion') { await sendText(chatId, 'Шаг 11/13: Эмоция трека:'); return; }
  if (s.step === 'country') { await sendText(chatId, 'Шаг 12/13: Страна артиста:'); return; }
  if (s.step === 'usecase') { await sendText(chatId, 'Шаг 13/13: Где трек работает лучше всего?'); return; }
  if (s.step === 'done') {
    const packet = generatePromoPacket(s.data);
    await sendText(chatId, packet);
    await sendText(chatId, '✅ Промо-пакет готов.');
    resetPromoSession(uid);
  }
}

async function handlePromoMessage(msg) {
  const uid = String(msg.from?.id || '');
  const s = getPromoSession(uid);
  if (!s) return false;

  const text = clean(msg.text);
  const chatId = msg.chat.id;

  if (!text) return true;
  if (text === '/cancel') {
    resetPromoSession(uid);
    await sendText(chatId, 'Промо-анкета отменена.');
    return true;
  }
  if (text.startsWith('/')) return false;

  if (s.step === 'artist') {
    s.data.artist = text;
    s.step = 'project_type';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'project_type') {
    s.data.project_type = text;
    s.step = 'release_name';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'release_name') {
    s.data.release_name = text;
    s.step = 'release_kind';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'release_kind') {
    s.data.release_kind = text;
    s.step = 'genre_main';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'genre_main') {
    s.data.genre_main = text;
    s.step = 'genre_extra';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'genre_extra') {
    s.data.genre_extra = text;
    s.step = 'mood';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'mood') {
    s.data.mood = text;
    s.step = 'vibe';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'vibe') {
    s.data.vibe = text;
    s.step = 'sound';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'sound') {
    s.data.sound = text;
    s.step = 'vocal';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'vocal') {
    s.data.vocal = text;
    s.step = 'emotion';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'emotion') {
    s.data.emotion = text;
    s.step = 'country';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'country') {
    s.data.country = text;
    s.step = 'usecase';
    await sendPromoStep(chatId, uid);
    return true;
  }
  if (s.step === 'usecase') {
    s.data.usecase = text;
    s.step = 'done';
    await sendPromoStep(chatId, uid);
    return true;
  }
  return true;
}

async function handlePromoCallback(query, data) {
  if (!data.startsWith('promo_')) return false;
  const uid = String(query.from.id);
  const s = getPromoSession(uid);
  if (!s) {
    await sendText(query.message.chat.id, 'Промо-анкета не активна. Запустите «Промо-текст под релиз» заново.');
    return true;
  }

  if (data === 'promo_project_solo' || data === 'promo_project_feat') {
    s.data.project_type = data === 'promo_project_solo' ? 'Solo' : 'Feat';
    s.step = 'release_name';
    await sendPromoStep(query.message.chat.id, uid);
    return true;
  }
  if (data === 'promo_kind_single' || data === 'promo_kind_ep' || data === 'promo_kind_album') {
    const map = { promo_kind_single: 'Сингл', promo_kind_ep: 'EP', promo_kind_album: 'Альбом' };
    s.data.release_kind = map[data];
    s.step = 'genre_main';
    await sendPromoStep(query.message.chat.id, uid);
    return true;
  }
  if (data === 'promo_vocal_no' || data === 'promo_vocal_male' || data === 'promo_vocal_female') {
    const map = { promo_vocal_no: 'instrumental', promo_vocal_male: 'male', promo_vocal_female: 'female' };
    s.data.vocal = map[data];
    s.step = 'emotion';
    await sendPromoStep(query.message.chat.id, uid);
    return true;
  }
  return false;
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

  try {
    await submitReleaseToModeration(user, uid, vr.data, 'mini_app');
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
    await sendText(query.message.chat.id, 'Эти кнопки работают только в группе модерации.');
    return;
  }
  if (!(await canModerate(query.from.id))) {
    await sendText(query.message.chat.id, 'Доступ только участникам группы модерации.');
    return;
  }
  const list = Array.isArray(db?.[uid]) ? db[uid] : null;
  if (!list || !list[idx]) {
    await sendText(query.message.chat.id, 'Релиз не найден.');
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
  await sendText(query.message.chat.id, `Статус обновлен: ${STATUS_TEXT[st] || st}`);
}

async function onMessage(msg) {
  if (msg.web_app_data?.data) { await processWebAppData(msg); return; }
  const chatId = msg.chat?.id;
  const uid = String(msg.from?.id || '');
  const text = clean(msg.text);
  if (!chatId) return;

  if (await handleCoverMessage(msg)) return;
  if (await handlePromoMessage(msg)) return;
  if (await handleFormTextMessage(msg)) return;

  if (!text) return;

  if (text === '/start' || text.startsWith('/start ')) {
    resetAllSessions(uid);
    await sendText(chatId, welcomeText(), { reply_markup: keyboardMain() });
    return;
  }
  if (text === '/release') {
    await startTextForm(chatId, uid, msg.from);
    return;
  }
  if (text === '/cover') {
    await startCoverFlow(chatId, uid, msg.from);
    return;
  }
  if (text === '/promo') {
    await startPromoFlow(chatId, uid, msg.from);
    return;
  }
  if (text === '/cancel') {
    const hadAny = !!(getFormSession(uid) || getCoverSession(uid) || getPromoSession(uid));
    resetAllSessions(uid);
    await sendText(chatId, hadAny ? 'Текущая анкета отменена.' : 'Нет активной анкеты.');
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

  if (await handleFormCallback(query, data)) return;
  if (await handlePromoCallback(query, data)) return;

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
  if (data === 'open_app' || data === 'report_app') { await sendWebappButton(chatId); return; }
  if (data === 'report' || data === 'report_text') {
    await startTextForm(chatId, String(query.from.id), query.from);
    return;
  }
  if (data === 'my_releases') { await sendMy(chatId, String(query.from.id)); return; }
  if (data === 'service_cover') { await startCoverFlow(chatId, String(query.from.id), query.from); return; }
  if (data === 'service_promo') { await startPromoFlow(chatId, String(query.from.id), query.from); return; }
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
