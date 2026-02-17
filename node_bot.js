'use strict';

const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const dns = require('node:dns');
const crypto = require('node:crypto');

const ROOT = __dirname;
const CFG = loadJson('deploy_config.json', {});

try {
  // In some hosting environments IPv6 routing is unstable for api.telegram.org.
  // Prefer IPv4 to reduce intermittent "fetch failed" during long polling.
  if (typeof dns.setDefaultResultOrder === 'function') {
    dns.setDefaultResultOrder('ipv4first');
  }
} catch {
  // ignore dns tuning errors
}

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
function envIntList(name, def = []) {
  const source = process.env[name] ?? CFG[name];
  if (Array.isArray(source)) {
    const out = source
      .map((v) => Number.parseInt(String(v), 10))
      .filter((v) => Number.isFinite(v));
    return out.length ? [...new Set(out)] : def;
  }
  const raw = clean(source);
  if (!raw) return def;
  const out = raw
    .split(/[,\s;]+/)
    .map((v) => Number.parseInt(v, 10))
    .filter((v) => Number.isFinite(v));
  return out.length ? [...new Set(out)] : def;
}

function stripHtml(input) {
  return String(input ?? '')
    .replace(/<[^>]*>/g, ' ')
    .replace(/[\u0000-\u0008\u000B-\u001F\u007F]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function sanitizeText(input, maxLen = 320) {
  const txt = stripHtml(input);
  if (!txt) return '';
  if (txt.length <= maxLen) return txt;
  return txt.slice(0, Math.max(0, maxLen)).trim();
}

function safeJson(value) {
  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return null;
  }
}

function sha1Hex(input) {
  return crypto.createHash('sha1').update(String(input ?? ''), 'utf8').digest('hex');
}

function mapReleaseStatusToFormStatus(status) {
  const canon = canonicalStatus(status);
  if (canon === STATUS.APPROVED || canon === STATUS.PUBLISHED) return FORM_STATUS.APPROVED;
  if (canon === STATUS.REJECTED || canon === STATUS.DELETED) return FORM_STATUS.REJECTED;
  if (canon === STATUS.ON_UPLOAD) return FORM_STATUS.PENDING;
  return FORM_STATUS.ON_MODERATION;
}

function mapFormStatusToReleaseStatus(formStatus) {
  const st = clean(formStatus).toLowerCase();
  if (st === FORM_STATUS.APPROVED) return STATUS.APPROVED;
  if (st === FORM_STATUS.REJECTED) return STATUS.REJECTED;
  if (st === FORM_STATUS.PENDING) return STATUS.ON_UPLOAD;
  return STATUS.MODERATION;
}

function verifyTelegramInitData(initDataRaw, expectedUserId = '') {
  const initData = clean(initDataRaw);
  if (!initData) {
    return { ok: false, reason: 'initData отсутствует' };
  }
  try {
    const params = new URLSearchParams(initData);
    const hash = clean(params.get('hash')).toLowerCase();
    if (!hash) {
      return { ok: false, reason: 'hash отсутствует в initData' };
    }
    params.delete('hash');
    const pairs = [];
    for (const [k, v] of params.entries()) {
      pairs.push(`${k}=${v}`);
    }
    pairs.sort((a, b) => a.localeCompare(b));
    const dataCheckString = pairs.join('\n');
    const secretKey = crypto
      .createHmac('sha256', 'WebAppData')
      .update(TOKEN, 'utf8')
      .digest();
    const calc = crypto
      .createHmac('sha256', secretKey)
      .update(dataCheckString, 'utf8')
      .digest('hex')
      .toLowerCase();
    if (calc !== hash) {
      return { ok: false, reason: 'hash initData не прошёл проверку' };
    }
    const authDateRaw = clean(params.get('auth_date'));
    const authDate = Number.parseInt(authDateRaw, 10);
    if (Number.isFinite(authDate) && TELEGRAM_INITDATA_MAX_AGE_SEC > 0) {
      const now = Math.floor(Date.now() / 1000);
      if (Math.abs(now - authDate) > TELEGRAM_INITDATA_MAX_AGE_SEC) {
        return { ok: false, reason: 'initData устарел' };
      }
    }
    let user = null;
    const rawUser = params.get('user');
    if (rawUser) {
      try { user = JSON.parse(rawUser); } catch { user = null; }
    }
    const initUserId = clean(user?.id ?? '');
    if (expectedUserId && initUserId && initUserId !== String(expectedUserId)) {
      return { ok: false, reason: 'telegram_id не совпадает с initData user.id' };
    }
    return { ok: true, user };
  } catch (e) {
    return { ok: false, reason: `ошибка проверки initData: ${clean(e?.message || e)}` };
  }
}

function verifyWebappAntiSpam(userId, payloadJson) {
  const uid = clean(userId);
  if (!uid) return { ok: false, reason: 'user_id отсутствует' };
  const payloadHash = sha1Hex(payloadJson);
  const now = Date.now();
  const prev = webappSubmitAntiSpam.get(uid);
  if (prev && prev.hash === payloadHash && (now - prev.at) < WEBAPP_SUBMIT_THROTTLE_MS) {
    return { ok: false, reason: 'дубликат анкеты отправлен слишком быстро' };
  }
  webappSubmitAntiSpam.set(uid, { hash: payloadHash, at: now });
  if (webappSubmitAntiSpam.size > 3000) {
    for (const [key, row] of webappSubmitAntiSpam.entries()) {
      if ((now - Number(row?.at || 0)) > Math.max(60000, WEBAPP_SUBMIT_THROTTLE_MS * 10)) {
        webappSubmitAntiSpam.delete(key);
      }
    }
  }
  return { ok: true };
}

const TOKEN = envStr('BOT_TOKEN') || envStr('TOKEN') || envStr('bot_token');
if (!TOKEN) {
  console.error('BOT_TOKEN is missing.');
  process.exit(1);
}
const MOD_CHAT = envInt('MODERATION_CHAT_ID', -1002117586464);
const MODERATION_THREAD_ID = envInt('MODERATION_THREAD_ID', 0);
const TELEGRAM_API_BASE = envStr('TELEGRAM_API_BASE', 'https://api.telegram.org').replace(/\/+$/, '');
const TG_FETCH_TIMEOUT_MS = envInt('TG_FETCH_TIMEOUT_MS', 70000);
const TG_FETCH_RETRIES = envInt('TG_FETCH_RETRIES', 2);
const TG_FETCH_RETRY_DELAY_MS = envInt('TG_FETCH_RETRY_DELAY_MS', 800);
const BASE = envStr('PUBLIC_BASE_URL', '');
let WEBAPP_URL = envStr('WEBAPP_URL', BASE ? `${BASE.replace(/\/+$/, '')}/index.html` : '');
if (/\.vercel\.app\/index\.html$/i.test(WEBAPP_URL)) {
  WEBAPP_URL = WEBAPP_URL.replace(/\/index\.html$/i, '/');
}
const WEB_HOST = envStr('WEB_SERVER_HOST', '0.0.0.0');
const WEB_PORT = envInt('PORT', envInt('WEB_SERVER_PORT', 8080));
const WEB_DIR = envStr('WEB_SERVER_DIR', 'webapp');
const WEB_ENABLED = envBool('ENABLE_WEB_SERVER', true);
const ADMIN_IDS = envIntList('ADMIN_IDS', [881379104]);
const MODERATION_HEALTH_TTL_MS = envInt('MODERATION_HEALTH_TTL_MS', 180000);
const TELEGRAM_INITDATA_MAX_AGE_SEC = envInt('TELEGRAM_INITDATA_MAX_AGE_SEC', 86400);
const WEBAPP_SUBMIT_THROTTLE_MS = envInt('WEBAPP_SUBMIT_THROTTLE_MS', 7000);
const WEBAPP_MAX_PAYLOAD_BYTES = envInt('WEBAPP_MAX_PAYLOAD_BYTES', 3900);
const WEBAPP_REQUIRE_INITDATA = envBool('WEBAPP_REQUIRE_INITDATA', false);
const SUPABASE_URL = envStr('SUPABASE_URL', '');
const SUPABASE_SERVICE_ROLE_KEY = envStr('SUPABASE_SERVICE_ROLE_KEY', envStr('SUPABASE_KEY', ''));
const SUPABASE_SCHEMA = envStr('SUPABASE_SCHEMA', 'public') || 'public';
const SUPABASE_RELEASES_TABLE_RAW = envStr('SUPABASE_RELEASES_TABLE', 'cxrner_releases') || 'cxrner_releases';
const SUPABASE_CABINET_TABLE_RAW = envStr('SUPABASE_CABINET_TABLE', 'cxrner_cabinet_users') || 'cxrner_cabinet_users';
const SUPABASE_FORMS_TABLE_RAW = envStr('SUPABASE_FORMS_TABLE', 'cxrner_forms') || 'cxrner_forms';
const SUPABASE_USERS_TABLE_RAW = envStr('SUPABASE_USERS_TABLE', 'cxrner_users') || 'cxrner_users';
const SUPABASE_PUBLIC_RELEASES_TABLE_RAW = envStr('SUPABASE_PUBLIC_RELEASES_TABLE', 'cxrner_public_releases') || 'cxrner_public_releases';
const SUPABASE_SYNC_ENABLED = !!SUPABASE_URL && !!SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_FETCH_TIMEOUT_MS = envInt('SUPABASE_FETCH_TIMEOUT_MS', 25000);
const SUPABASE_FETCH_RETRIES = envInt('SUPABASE_FETCH_RETRIES', 2);
const SUPABASE_FETCH_RETRY_DELAY_MS = envInt('SUPABASE_FETCH_RETRY_DELAY_MS', 900);
const SUPABASE_SYNC_DEBOUNCE_MS = envInt('SUPABASE_SYNC_DEBOUNCE_MS', 1200);
const IMPORT_RELEASES_BACKUP_FILE = envStr('IMPORT_RELEASES_BACKUP_FILE', '');

const DB_FILE = 'releases.json';
const MOD_DB_FILE = 'moderation_releases.json';
const CAB_FILE = 'cabinet_users.json';
const EXP_REL = 'webapp/data/releases-public.json';
const EXP_CAB = 'webapp/data/cabinet-users.json';
const CLEANUP_KEEP_DAYS = envInt('CLEANUP_KEEP_DAYS', 180);

const STATUS = {
  ON_UPLOAD: 'on_upload',
  MODERATION: 'moderation',
  APPROVED: 'approved',
  REJECTED: 'rejected',
  NEEDS_FIX: 'needs_fix',
  DELETED: 'deleted',
  PUBLISHED: 'published'
};
const STATUS_TEXT = {
  [STATUS.ON_UPLOAD]: 'На отгрузке',
  [STATUS.MODERATION]: 'На модерации',
  [STATUS.APPROVED]: 'Одобрено',
  [STATUS.REJECTED]: 'Отклонено',
  [STATUS.NEEDS_FIX]: 'На исправлении',
  [STATUS.DELETED]: 'Удалено',
  [STATUS.PUBLISHED]: 'Опубликовано'
};
const STATUS_EMOJI = {
  [STATUS.ON_UPLOAD]: '🕓',
  [STATUS.MODERATION]: '🧠',
  [STATUS.APPROVED]: '✅',
  [STATUS.REJECTED]: '❌',
  [STATUS.NEEDS_FIX]: '✏️',
  [STATUS.DELETED]: '🗑',
  [STATUS.PUBLISHED]: '📢'
};
const MODERATION_TEXT_MAX = 3900;
const MODERATION_HISTORY_LIMIT = 5;
const MODERATION_ACTION_LOG_LIMIT = 24;
const LEGACY_STATUS_MAP = {
  pending: STATUS.ON_UPLOAD,
  on_upload: STATUS.ON_UPLOAD,
  moderation: STATUS.MODERATION,
  approved: STATUS.APPROVED,
  rejected: STATUS.REJECTED,
  needs_fix: STATUS.NEEDS_FIX,
  deleted: STATUS.DELETED,
  published: STATUS.PUBLISHED
};

let db = loadJson(DB_FILE, {});
let modDb = loadJson(MOD_DB_FILE, { moderation_messages: [], pending_actions: [] });
let cabUsers = loadJson(CAB_FILE, {});
const userForms = {};
const coverSessions = {};
const promoSessions = {};
const broadcastSessions = {};
const PENDING_ACTION_TTL_MS = 1000 * 60 * 60 * 6; // 6 hours
const SUPABASE_RELEASES_TABLE = /^[A-Za-z_][A-Za-z0-9_]*$/.test(SUPABASE_RELEASES_TABLE_RAW)
  ? SUPABASE_RELEASES_TABLE_RAW
  : 'cxrner_releases';
const SUPABASE_CABINET_TABLE = /^[A-Za-z_][A-Za-z0-9_]*$/.test(SUPABASE_CABINET_TABLE_RAW)
  ? SUPABASE_CABINET_TABLE_RAW
  : 'cxrner_cabinet_users';
const SUPABASE_FORMS_TABLE = /^[A-Za-z_][A-Za-z0-9_]*$/.test(SUPABASE_FORMS_TABLE_RAW)
  ? SUPABASE_FORMS_TABLE_RAW
  : 'cxrner_forms';
const SUPABASE_USERS_TABLE = /^[A-Za-z_][A-Za-z0-9_]*$/.test(SUPABASE_USERS_TABLE_RAW)
  ? SUPABASE_USERS_TABLE_RAW
  : 'cxrner_users';
const SUPABASE_PUBLIC_RELEASES_TABLE = /^[A-Za-z_][A-Za-z0-9_]*$/.test(SUPABASE_PUBLIC_RELEASES_TABLE_RAW)
  ? SUPABASE_PUBLIC_RELEASES_TABLE_RAW
  : 'cxrner_public_releases';
const FORM_STATUS = {
  PENDING: 'pending',
  ON_MODERATION: 'on_moderation',
  APPROVED: 'approved',
  REJECTED: 'rejected'
};
const moderationHealth = {
  ok: null,
  checked_at: 0,
  reason: '',
  chat_title: '',
  bot_id: '',
  bot_username: '',
  bot_status: '',
  can_send_messages: null
};
let supabaseSyncInProgress = false;
let supabaseSyncQueued = false;
let supabaseSyncTimer = null;
const webappSubmitAntiSpam = new Map();
const supabaseFeatureState = {
  forms: true,
  users: true,
  public_releases: true
};
ensureModDbShape();

const API = `${TELEGRAM_API_BASE}/bot${TOKEN}`;

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function classifyFetchError(err) {
  const raw = clean(err?.message || err);
  const lower = raw.toLowerCase();
  const cause = err?.cause && typeof err.cause === 'object' ? err.cause : null;
  const code = clean(cause?.code || err?.code || '').toUpperCase();
  const errno = clean(cause?.errno || err?.errno || '');
  const syscall = clean(cause?.syscall || err?.syscall || '');
  const hostname = clean(cause?.hostname || cause?.host || err?.hostname || '');
  const address = clean(cause?.address || err?.address || '');
  const port = clean(cause?.port || err?.port || '');

  const isAbort = clean(err?.name || '') === 'AbortError' || lower.includes('aborted');
  const isFetchFailed = lower.includes('fetch failed');
  const codeLower = code.toLowerCase();
  const networkCodes = new Set([
    'eai_again', 'enotfound', 'ecannothost', 'econnreset', 'etimedout',
    'ehostunreach', 'enetunreach', 'econnrefused', 'epipe', 'ecanceled',
    'ecancelled', 'ecouldntconnect'
  ]);
  const isNetwork = isFetchFailed || networkCodes.has(codeLower);

  const parts = [];
  if (raw) parts.push(raw);
  if (code) parts.push(`code=${code}`);
  if (errno) parts.push(`errno=${errno}`);
  if (syscall) parts.push(`syscall=${syscall}`);
  if (hostname) parts.push(`host=${hostname}`);
  if (address) parts.push(`addr=${address}`);
  if (port) parts.push(`port=${port}`);

  return {
    raw,
    isAbort,
    isFetchFailed,
    isNetwork,
    details: parts.join('; ') || 'unknown network error'
  };
}

async function tg(method, payload = {}) {
  const url = `${API}/${method}`;
  const retries = Math.max(0, Number(TG_FETCH_RETRIES) || 0);
  let lastErr = null;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = typeof AbortController === 'function' ? new AbortController() : null;
    let timeoutRef = null;
    if (controller) {
      timeoutRef = setTimeout(() => controller.abort(), Math.max(5000, TG_FETCH_TIMEOUT_MS));
    }

    try {
      const r = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload),
        signal: controller ? controller.signal : undefined
      });
      if (timeoutRef) clearTimeout(timeoutRef);
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j.ok) throw new Error(`[${method}] ${j.description || r.statusText}`);
      return j.result;
    } catch (err) {
      if (timeoutRef) clearTimeout(timeoutRef);
      lastErr = err;
      const meta = classifyFetchError(err);
      const canRetry = attempt < retries && (meta.isNetwork || meta.isAbort || meta.isFetchFailed);
      if (canRetry) {
        await delay(Math.max(200, TG_FETCH_RETRY_DELAY_MS) * (attempt + 1));
        continue;
      }
      if (meta.isNetwork || meta.isAbort || meta.isFetchFailed) {
        throw new Error(`[${method}] network error: ${meta.details}`);
      }
      throw err;
    }
  }
  throw lastErr || new Error(`[${method}] unknown error`);
}
function saveDb() {
  saveJson(DB_FILE, db);
  exportReleases();
  scheduleSupabaseSync('db');
}
function saveModDb() {
  ensureModDbShape();
  saveJson(MOD_DB_FILE, modDb);
}
function saveCab() {
  saveJson(CAB_FILE, cabUsers);
  exportCabinet();
  scheduleSupabaseSync('cabinet');
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
      status: canonicalStatus(r.status || STATUS.ON_UPLOAD),
      status_raw: r.status || STATUS.ON_UPLOAD,
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

function releaseKey(rel) {
  const time = clean(rel?.submission_time || rel?.moderation_time || '');
  return [
    clean(rel?.name).toLowerCase(),
    clean(rel?.nick).toLowerCase(),
    clean(rel?.date),
    clean(rel?.link),
    time
  ].join('|');
}

function releaseFreshnessScore(rel) {
  const a = Date.parse(clean(rel?.moderation_time || ''));
  const b = Date.parse(clean(rel?.submission_time || ''));
  if (Number.isFinite(a)) return a;
  if (Number.isFinite(b)) return b;
  return 0;
}

function normalizeRelease(relRaw) {
  const rel = relRaw && typeof relRaw === 'object' ? { ...relRaw } : {};
  rel.type = clean(rel.type) || 'сингл';
  rel.status = canonicalStatus(rel.status || STATUS.ON_UPLOAD);
  rel.name = clean(rel.name);
  rel.subname = clean(rel.subname || '.');
  rel.nick = clean(rel.nick);
  rel.fio = clean(rel.fio);
  rel.date = clean(rel.date);
  rel.version = clean(rel.version || 'Оригинал');
  rel.genre = clean(rel.genre);
  rel.link = clean(rel.link);
  rel.yandex = clean(rel.yandex || '.');
  rel.mat = clean(rel.mat || 'Нет');
  rel.promo = clean(rel.promo || '.');
  rel.comment = clean(rel.comment || '.');
  rel.tracklist = clean(rel.tracklist || '.');
  rel.tg = clean(rel.tg || rel.telegram_contact || '');
  rel.source = clean(rel.source || 'node_bot');
  rel.submission_time = clean(rel.submission_time || new Date().toISOString());
  if (rel.moderation_time) rel.moderation_time = clean(rel.moderation_time);
  if (rel.reject_reason) rel.reject_reason = clean(rel.reject_reason);
  if (rel.upc) rel.upc = clean(rel.upc).toUpperCase();
  if (rel.moderation_message_id) rel.moderation_message_id = Number(rel.moderation_message_id) || 0;
  rel.available_for_upload = !!rel.available_for_upload;
  if (rel.available_marked_at) rel.available_marked_at = clean(rel.available_marked_at);
  if (rel.available_marked_by) rel.available_marked_by = clean(rel.available_marked_by);
  if (rel.available_marked_by_username) rel.available_marked_by_username = clean(rel.available_marked_by_username);
  if (rel.available_marked_by_name) rel.available_marked_by_name = clean(rel.available_marked_by_name);
  rel.interactions = normalizeInteractionLog(rel.interactions);
  if (rel.interactions.length > MODERATION_ACTION_LOG_LIMIT) {
    rel.interactions = rel.interactions.slice(-MODERATION_ACTION_LOG_LIMIT);
  }
  if (!clean(rel.last_action_at) && rel.interactions.length) {
    const last = rel.interactions[rel.interactions.length - 1];
    rel.last_action_at = clean(last?.at || '');
    rel.last_actor_id = clean(last?.actor_id || '');
    rel.last_actor_username = clean(last?.actor_username || '');
    rel.last_actor_name = clean(last?.actor_name || '');
    rel.last_action_type = clean(last?.type || '');
    rel.last_action_note = clean(last?.note || last?.reason || last?.upc || '');
  }
  rel.user_deleted = !!rel.user_deleted;
  return rel;
}

function mergeReleasesIntoDb(sourceDb, sourceLabel = 'source') {
  if (!sourceDb || typeof sourceDb !== 'object') return { added: 0, merged: 0 };
  let added = 0;
  let merged = 0;

  for (const [uidRaw, listRaw] of Object.entries(sourceDb)) {
    const uid = String(uidRaw);
    const incoming = Array.isArray(listRaw) ? listRaw : [];
    if (!incoming.length) continue;

    const current = Array.isArray(db[uid]) ? db[uid] : [];
    if (!Array.isArray(db[uid])) db[uid] = current;

    const byMsg = new Map();
    const bySubmission = new Map();
    const byKey = new Map();
    for (let idx = 0; idx < current.length; idx += 1) {
      const rel = current[idx];
      if (!rel || typeof rel !== 'object') continue;
      const msgId = Number(rel.moderation_message_id || 0);
      if (msgId) byMsg.set(msgId, idx);
      const submission = clean(rel.submission_time);
      if (submission) bySubmission.set(submission, idx);
      byKey.set(releaseKey(rel), idx);
    }

    for (let srcIdx = 0; srcIdx < incoming.length; srcIdx += 1) {
      const rawIncoming = incoming[srcIdx];
      if (!rawIncoming || typeof rawIncoming !== 'object') continue;
      const srcRel = normalizeRelease(rawIncoming);
      let idx = -1;
      const msgId = Number(srcRel.moderation_message_id || 0);
      if (msgId && byMsg.has(msgId)) idx = byMsg.get(msgId);
      if (idx < 0) {
        const submission = clean(srcRel.submission_time);
        if (submission && bySubmission.has(submission)) idx = bySubmission.get(submission);
      }
      if (idx < 0 && byKey.has(releaseKey(srcRel))) idx = byKey.get(releaseKey(srcRel));

      if (idx >= 0 && current[idx]) {
        const localRel = normalizeRelease(current[idx]);
        const localScore = releaseFreshnessScore(localRel);
        const remoteScore = releaseFreshnessScore(srcRel);
        current[idx] = (remoteScore > localScore)
          ? { ...localRel, ...srcRel }
          : { ...srcRel, ...localRel };
        merged += 1;
        continue;
      }

      current.push(srcRel);
      const newIdx = current.length - 1;
      if (msgId) byMsg.set(msgId, newIdx);
      if (srcRel.submission_time) bySubmission.set(srcRel.submission_time, newIdx);
      byKey.set(releaseKey(srcRel), newIdx);
      added += 1;
    }
  }

  if (added || merged) {
    console.info(`[db] merge from ${sourceLabel}: added=${added}, merged=${merged}`);
  }
  return { added, merged };
}

function mergeCabinetIntoState(sourceCabinet, sourceLabel = 'source') {
  if (!sourceCabinet || typeof sourceCabinet !== 'object') return { added: 0, merged: 0 };
  let added = 0;
  let merged = 0;
  for (const [uidRaw, valueRaw] of Object.entries(sourceCabinet)) {
    const uid = String(uidRaw);
    const value = valueRaw && typeof valueRaw === 'object' ? { ...valueRaw } : {};
    const next = {
      approved: !!value.approved,
      activated_at: clean(value.activated_at || ''),
      username: clean(value.username || ''),
      first_name: clean(value.first_name || '')
    };
    if (!cabUsers[uid]) {
      cabUsers[uid] = next;
      added += 1;
      continue;
    }
    cabUsers[uid] = {
      ...cabUsers[uid],
      ...next,
      approved: !!(cabUsers[uid].approved || next.approved)
    };
    merged += 1;
  }
  if (added || merged) {
    console.info(`[cabinet] merge from ${sourceLabel}: added=${added}, merged=${merged}`);
  }
  return { added, merged };
}

function importBackupsIntoDb() {
  const backups = [];
  if (IMPORT_RELEASES_BACKUP_FILE) {
    backups.push(IMPORT_RELEASES_BACKUP_FILE);
  }
  try {
    const auto = fs.readdirSync(ROOT)
      .filter((name) => /^releases_backup_\d{8}_\d{6}\.json$/i.test(name))
      .sort();
    for (const file of auto) backups.push(file);
  } catch {
    // ignore backup scan errors
  }

  let totalAdded = 0;
  let totalMerged = 0;
  const seen = new Set();
  for (const backupFileRaw of backups) {
    const backupFile = path.isAbsolute(backupFileRaw)
      ? backupFileRaw
      : path.resolve(ROOT, backupFileRaw);
    if (seen.has(backupFile)) continue;
    seen.add(backupFile);
    if (!fs.existsSync(backupFile)) continue;
    try {
      const payload = JSON.parse(fs.readFileSync(backupFile, 'utf8'));
      const merged = mergeReleasesIntoDb(payload, path.basename(backupFile));
      totalAdded += merged.added;
      totalMerged += merged.merged;
    } catch (e) {
      console.error(`[db] backup import failed (${backupFile}):`, clean(e?.message || e));
    }
  }
  if (totalAdded || totalMerged) {
    saveJson(DB_FILE, db);
    exportReleases();
  }
  return { added: totalAdded, merged: totalMerged };
}

function supabaseHeaders(contentProfile = false) {
  const headers = {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`
  };
  if (SUPABASE_SCHEMA && SUPABASE_SCHEMA !== 'public') {
    headers['Accept-Profile'] = SUPABASE_SCHEMA;
    if (contentProfile) headers['Content-Profile'] = SUPABASE_SCHEMA;
  }
  return headers;
}

function supabaseUrl(pathAndQuery) {
  const base = SUPABASE_URL.replace(/\/+$/, '');
  return `${base}/rest/v1/${pathAndQuery}`;
}

function supabaseHostFromUrl() {
  try {
    return new URL(SUPABASE_URL).hostname;
  } catch {
    return '';
  }
}

async function logSupabaseDns(label = '') {
  const host = supabaseHostFromUrl();
  if (!host) return;
  try {
    const rows = await dns.promises.lookup(host, { all: true });
    const list = Array.isArray(rows)
      ? rows.map((it) => `${it.address}/${it.family}`).join(', ')
      : '';
    if (list) console.error(`[supabase] dns ${host}${label ? ` (${label})` : ''}: ${list}`);
  } catch (e) {
    console.error(`[supabase] dns lookup failed (${host}): ${clean(e?.message || e)}`);
  }
}

async function supabaseRequest(pathAndQuery, opts = {}) {
  const method = opts.method || 'GET';
  const headers = {
    ...supabaseHeaders(method !== 'GET'),
    ...(opts.headers || {})
  };
  const request = {
    method,
    headers
  };
  if (opts.body !== undefined) {
    headers['content-type'] = 'application/json';
    request.body = JSON.stringify(opts.body);
  }
  const retries = Math.max(0, Number(SUPABASE_FETCH_RETRIES) || 0);
  let lastErr = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = typeof AbortController === 'function' ? new AbortController() : null;
    let timeoutRef = null;
    if (controller) {
      timeoutRef = setTimeout(() => controller.abort(), Math.max(5000, SUPABASE_FETCH_TIMEOUT_MS));
    }
    try {
      const res = await fetch(supabaseUrl(pathAndQuery), {
        ...request,
        signal: controller ? controller.signal : undefined
      });
      if (timeoutRef) clearTimeout(timeoutRef);
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`supabase ${method} ${pathAndQuery} -> ${res.status}: ${clean(text).slice(0, 300)}`);
      }
      if (res.status === 204) return null;
      const ct = clean(res.headers.get('content-type') || '').toLowerCase();
      if (ct.includes('application/json')) return res.json();
      return res.text();
    } catch (err) {
      if (timeoutRef) clearTimeout(timeoutRef);
      lastErr = err;
      const meta = classifyFetchError(err);
      const canRetry = attempt < retries && (meta.isNetwork || meta.isAbort || meta.isFetchFailed);
      if (canRetry) {
        await delay(Math.max(200, SUPABASE_FETCH_RETRY_DELAY_MS) * (attempt + 1));
        continue;
      }
      if (meta.isNetwork || meta.isAbort || meta.isFetchFailed) {
        await logSupabaseDns(`attempt=${attempt + 1}`);
        throw new Error(`supabase ${method} ${pathAndQuery} network error: ${meta.details}`);
      }
      throw err;
    }
  }
  throw lastErr || new Error(`supabase ${method} ${pathAndQuery} unknown error`);
}

async function supabaseSelectAll(tableName, selectCols, orderExpr = '') {
  const out = [];
  const pageSize = 1000;
  let from = 0;
  while (true) {
    const to = from + pageSize - 1;
    const query = [];
    query.push(`select=${encodeURIComponent(selectCols)}`);
    if (orderExpr) query.push(`order=${encodeURIComponent(orderExpr)}`);
    const rows = await supabaseRequest(`${tableName}?${query.join('&')}`, {
      headers: { Range: `${from}-${to}` }
    });
    const list = Array.isArray(rows) ? rows : [];
    out.push(...list);
    if (list.length < pageSize) break;
    from += pageSize;
  }
  return out;
}

async function supabaseSelectWhere(tableName, selectCols, filters = [], orderExpr = '', limit = 0) {
  const query = [];
  query.push(`select=${encodeURIComponent(selectCols)}`);
  for (const row of filters) {
    const key = clean(row?.key);
    const op = clean(row?.op || 'eq');
    const value = clean(row?.value);
    if (!key || !value) continue;
    query.push(`${encodeURIComponent(key)}=${encodeURIComponent(`${op}.${value}`)}`);
  }
  if (orderExpr) query.push(`order=${encodeURIComponent(orderExpr)}`);
  if (Number(limit) > 0) query.push(`limit=${Number(limit)}`);
  const rows = await supabaseRequest(`${tableName}?${query.join('&')}`);
  return Array.isArray(rows) ? rows : [];
}

function mapFormRowToCabinetRelease(row) {
  const payload = row?.form_payload && typeof row.form_payload === 'object' ? row.form_payload : {};
  return normalizeRelease({
    type: normalizeType(payload?.type || row?.release_type) || 'сингл',
    name: clean(payload?.name || row?.track_name || ''),
    subname: clean(payload?.subname || '.'),
    nick: clean(payload?.nick || row?.artist_name || ''),
    fio: clean(payload?.fio || payload?.artist_name || row?.artist_name || ''),
    date: clean(payload?.date || ''),
    version: clean(payload?.version || 'Оригинал'),
    genre: clean(payload?.genre || row?.genre || ''),
    link: clean(payload?.link || '.'),
    yandex: clean(payload?.yandex || '.'),
    mat: clean(payload?.mat || 'Нет'),
    promo: clean(payload?.promo || '.'),
    comment: clean(payload?.comment || '.'),
    tracklist: clean(payload?.tracklist || '.'),
    tg: clean(payload?.tg || payload?.telegram_contact || ''),
    status: mapFormStatusToReleaseStatus(row?.status || ''),
    reject_reason: clean(row?.reject_reason || payload?.reject_reason || ''),
    upc: clean(row?.upc || payload?.upc || ''),
    submission_time: clean(row?.submission_key || row?.created_at || payload?.submission_time || ''),
    moderation_time: clean(row?.updated_at || payload?.moderation_time || ''),
    source: clean(payload?.source || row?.source || 'supabase'),
    username: clean(payload?.username || row?.username || ''),
    supabase_form_id: clean(row?.id || row?.form_id || '')
  });
}

async function getCabinetSnapshot(userId) {
  const uid = clean(userId);
  const fallback = () => {
    const local = Array.isArray(db[uid]) ? db[uid] : [];
    const releases = local
      .filter((rel) => rel && typeof rel === 'object' && !rel.user_deleted)
      .map((rel) => normalizeRelease(rel));
    const localProfile = cabUsers[uid] && typeof cabUsers[uid] === 'object' ? cabUsers[uid] : {};
    return {
      source: 'local',
      cabinet_active: !!localProfile.approved,
      profile: {
        telegram_id: uid,
        username: clean(localProfile.username || ''),
        first_name: clean(localProfile.first_name || '')
      },
      releases
    };
  };

  if (!SUPABASE_SYNC_ENABLED || !uid) {
    return fallback();
  }

  try {
    const tasks = [];
    tasks.push(
      supabaseFeatureState.users
        ? supabaseSelectWhere(
          SUPABASE_USERS_TABLE,
          'telegram_id,username,first_name,cabinet_active,created_at,updated_at',
          [{ key: 'telegram_id', value: uid }],
          'updated_at.desc',
          1
        ).catch(() => [])
        : Promise.resolve([])
    );
    tasks.push(
      supabaseFeatureState.forms
        ? supabaseSelectWhere(
          SUPABASE_FORMS_TABLE,
          'id,telegram_id,username,artist_name,track_name,genre,release_type,status,reject_reason,upc,submission_key,created_at,updated_at,source,form_payload',
          [{ key: 'telegram_id', value: uid }],
          'created_at.desc',
          300
        ).catch(() => [])
        : Promise.resolve([])
    );
    const [userRows, formRows] = await Promise.all(tasks);
    const userRow = Array.isArray(userRows) && userRows.length ? userRows[0] : null;
    const releases = Array.isArray(formRows) ? formRows.map((row) => mapFormRowToCabinetRelease(row)) : [];
    if (userRow || releases.length) {
      return {
        source: 'supabase',
        cabinet_active: !!(userRow?.cabinet_active),
        profile: {
          telegram_id: uid,
          username: clean(userRow?.username || ''),
          first_name: clean(userRow?.first_name || '')
        },
        releases
      };
    }
  } catch (e) {
    console.error('[miniapp] cabinet supabase fetch failed:', clean(e?.message || e));
  }
  return fallback();
}

async function supabaseUpsertCabinetUser(userId, user, cabinetActive = true) {
  if (!SUPABASE_SYNC_ENABLED) return false;
  const uid = clean(userId);
  if (!uid) return false;
  const now = new Date().toISOString();
  const username = clean(user?.username || '');
  const firstName = clean(user?.first_name || '');
  let ok = false;

  if (supabaseFeatureState.users) {
    try {
      await supabaseRequest(`${SUPABASE_USERS_TABLE}?on_conflict=telegram_id`, {
        method: 'POST',
        headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
        body: [{
          telegram_id: uid,
          username,
          first_name: firstName,
          cabinet_active: !!cabinetActive,
          created_at: now,
          updated_at: now
        }]
      });
      ok = true;
    } catch (e) {
      const errText = clean(e?.message || e);
      console.error('[supabase] users upsert failed:', errText);
      if (isSupabaseSchemaError(e)) disableSupabaseFeature('users', errText);
    }
  }

  try {
    await supabaseRequest(`${SUPABASE_CABINET_TABLE}?on_conflict=user_id`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: [{
        user_id: uid,
        profile: {
          approved: !!cabinetActive,
          activated_at: now,
          username,
          first_name: firstName
        },
        updated_at: now
      }]
    });
    ok = true;
  } catch (e) {
    console.error('[supabase] cabinet upsert failed:', clean(e?.message || e));
  }

  return ok;
}

function buildSupabaseFormRow(userId, user, rel, status = FORM_STATUS.PENDING) {
  const uid = clean(userId);
  const rowStatus = clean(status) || FORM_STATUS.PENDING;
  const now = new Date().toISOString();
  const relSafe = rel && typeof rel === 'object' ? rel : {};
  const type = normalizeType(relSafe.type) === 'альбом' ? 'album' : 'single';
  const payload = safeJson({
    ...relSafe,
    telegram_id: uid,
    artist_name: sanitizeText(relSafe.nick || relSafe.artist_name || '', 130),
    track_name: sanitizeText(relSafe.name || relSafe.track_name || '', 160),
    genre: sanitizeText(relSafe.genre || '', 90),
    release_type: type
  }) || {};
  const submissionKey = clean(relSafe.submission_time || now);
  return {
    telegram_id: uid,
    username: clean(user?.username || relSafe.username || ''),
    artist_name: sanitizeText(relSafe.nick || relSafe.artist_name || '', 130),
    track_name: sanitizeText(relSafe.name || relSafe.track_name || '', 160),
    genre: sanitizeText(relSafe.genre || '', 90),
    release_type: type,
    status: rowStatus,
    reject_reason: clean(relSafe.reject_reason || ''),
    upc: clean(relSafe.upc || ''),
    moderation_message_id: Number(relSafe.moderation_message_id || 0) || null,
    source: clean(relSafe.source || 'mini_app'),
    submission_key: submissionKey,
    form_payload: payload,
    created_at: submissionKey,
    updated_at: now
  };
}

async function supabaseInsertForm(userId, user, rel, status = FORM_STATUS.PENDING) {
  if (!SUPABASE_SYNC_ENABLED) return '';
  if (!supabaseFeatureState.forms) return '';
  const row = buildSupabaseFormRow(userId, user, rel, status);
  try {
    const inserted = await supabaseRequest(SUPABASE_FORMS_TABLE, {
      method: 'POST',
      headers: { Prefer: 'return=representation' },
      body: [row]
    });
    const first = Array.isArray(inserted) ? inserted[0] : null;
    const formId = clean(first?.id || first?.form_id || '');
    console.info(
      `[supabase] form inserted: telegram_id=${row.telegram_id || '-'} id=${formId || '-'} status=${row.status}`
    );
    return formId;
  } catch (e) {
    const errText = clean(e?.message || e);
    console.error('[supabase] form insert failed:', errText);
    if (isSupabaseSchemaError(e)) disableSupabaseFeature('forms', errText);
    return '';
  }
}

function isSupabaseSchemaError(error) {
  const txt = clean(error?.message || error).toLowerCase();
  return txt.includes(' does not exist')
    || txt.includes('relation ')
    || txt.includes('column ')
    || txt.includes('schema cache')
    || txt.includes('42p01')
    || txt.includes('42703');
}

function disableSupabaseFeature(featureKey, reason) {
  if (!Object.prototype.hasOwnProperty.call(supabaseFeatureState, featureKey)) return;
  if (!supabaseFeatureState[featureKey]) return;
  supabaseFeatureState[featureKey] = false;
  console.error(`[supabase] feature disabled (${featureKey}): ${clean(reason || 'schema error')}`);
}

async function supabasePatchFormByRelease(userId, rel, patch = {}) {
  if (!SUPABASE_SYNC_ENABLED || !rel || typeof rel !== 'object') return false;
  if (!supabaseFeatureState.forms) return false;
  const uid = clean(userId);
  const now = new Date().toISOString();
  const formId = clean(rel.supabase_form_id || '');
  const body = {
    ...patch,
    updated_at: now
  };
  if (Object.prototype.hasOwnProperty.call(body, 'status')) {
    body.status = clean(body.status || FORM_STATUS.ON_MODERATION);
  }
  if (Object.prototype.hasOwnProperty.call(body, 'reject_reason')) {
    body.reject_reason = clean(body.reject_reason || '');
  }
  if (Object.prototype.hasOwnProperty.call(body, 'upc')) {
    body.upc = clean(body.upc || '');
  }

  try {
    if (formId) {
      await supabaseRequest(`${SUPABASE_FORMS_TABLE}?id=eq.${encodeURIComponent(formId)}`, {
        method: 'PATCH',
        headers: { Prefer: 'return=minimal' },
        body
      });
      return true;
    }
    const key = clean(rel.submission_time || '');
    if (!uid || !key) return false;
    await supabaseRequest(
      `${SUPABASE_FORMS_TABLE}?telegram_id=eq.${encodeURIComponent(uid)}&submission_key=eq.${encodeURIComponent(key)}`,
      {
        method: 'PATCH',
        headers: { Prefer: 'return=minimal' },
        body
      }
    );
    return true;
  } catch (e) {
    const errText = clean(e?.message || e);
    console.error('[supabase] form patch failed:', errText);
    if (isSupabaseSchemaError(e)) disableSupabaseFeature('forms', errText);
    return false;
  }
}

async function supabaseUpsertApprovedRelease(userId, rel) {
  if (!SUPABASE_SYNC_ENABLED || !rel || typeof rel !== 'object') return false;
  if (!supabaseFeatureState.public_releases) return false;
  const uid = clean(userId);
  if (!uid) return false;
  const now = new Date().toISOString();
  const formId = clean(rel.supabase_form_id || `${uid}:${clean(rel.submission_time || now)}`);
  const row = {
    form_id: formId,
    telegram_id: uid,
    username: clean(rel.username || ''),
    artist_name: sanitizeText(rel.nick || '', 130),
    track_name: sanitizeText(rel.name || '', 160),
    genre: sanitizeText(rel.genre || '', 90),
    release_type: normalizeType(rel.type) === 'альбом' ? 'album' : 'single',
    status: 'approved',
    approved_at: clean(rel.moderation_time || now),
    updated_at: now,
    release_data: safeJson(rel) || {}
  };
  try {
    await supabaseRequest(`${SUPABASE_PUBLIC_RELEASES_TABLE}?on_conflict=form_id`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: [row]
    });
    return true;
  } catch (e) {
    const errText = clean(e?.message || e);
    console.error('[supabase] approved release upsert failed:', errText);
    if (isSupabaseSchemaError(e)) disableSupabaseFeature('public_releases', errText);
    return false;
  }
}

async function supabaseDeleteApprovedRelease(userId, rel) {
  if (!SUPABASE_SYNC_ENABLED || !rel || typeof rel !== 'object') return false;
  if (!supabaseFeatureState.public_releases) return false;
  const uid = clean(userId);
  const formId = clean(rel.supabase_form_id || `${uid}:${clean(rel.submission_time || '')}`);
  if (!formId) return false;
  try {
    await supabaseRequest(`${SUPABASE_PUBLIC_RELEASES_TABLE}?form_id=eq.${encodeURIComponent(formId)}`, {
      method: 'DELETE',
      headers: { Prefer: 'return=minimal' }
    });
    return true;
  } catch (e) {
    const errText = clean(e?.message || e);
    console.error('[supabase] approved release delete failed:', errText);
    if (isSupabaseSchemaError(e)) disableSupabaseFeature('public_releases', errText);
    return false;
  }
}

async function hydrateFromSupabase() {
  if (!SUPABASE_SYNC_ENABLED) return;
  try {
    const releasePromise = supabaseSelectAll(
      SUPABASE_RELEASES_TABLE,
      'user_id,release_idx,release_data,updated_at',
      'user_id.asc,release_idx.asc'
    );
    const cabinetPromise = supabaseSelectAll(
      SUPABASE_CABINET_TABLE,
      'user_id,profile,updated_at',
      'user_id.asc'
    );
    const formsPromise = supabaseFeatureState.forms
      ? supabaseSelectAll(
        SUPABASE_FORMS_TABLE,
        'id,telegram_id,username,artist_name,track_name,genre,release_type,status,reject_reason,upc,moderation_message_id,submission_key,source,created_at,updated_at,form_payload',
        'created_at.asc'
      ).catch((e) => {
        const errText = clean(e?.message || e);
        console.error('[supabase] forms hydrate failed:', errText);
        if (isSupabaseSchemaError(e)) disableSupabaseFeature('forms', errText);
        return [];
      })
      : Promise.resolve([]);
    const usersPromise = supabaseFeatureState.users
      ? supabaseSelectAll(
        SUPABASE_USERS_TABLE,
        'telegram_id,username,first_name,cabinet_active,created_at,updated_at',
        'updated_at.asc'
      ).catch((e) => {
        const errText = clean(e?.message || e);
        console.error('[supabase] users hydrate failed:', errText);
        if (isSupabaseSchemaError(e)) disableSupabaseFeature('users', errText);
        return [];
      })
      : Promise.resolve([]);

    const [releaseRows, cabinetRows, formsRows, userRows] = await Promise.all([
      releasePromise,
      cabinetPromise,
      formsPromise,
      usersPromise
    ]);

    const remoteDb = {};
    for (const row of releaseRows) {
      const uid = String(row?.user_id || '');
      const idx = Number(row?.release_idx);
      if (!uid || !Number.isFinite(idx) || idx < 0) continue;
      if (!Array.isArray(remoteDb[uid])) remoteDb[uid] = [];
      const rel = normalizeRelease(row?.release_data || {});
      remoteDb[uid][idx] = rel;
    }

    for (const row of formsRows || []) {
      const uid = clean(row?.telegram_id || '');
      if (!uid) continue;
      if (!Array.isArray(remoteDb[uid])) remoteDb[uid] = [];
      const payload = row?.form_payload && typeof row.form_payload === 'object' ? row.form_payload : {};
      const rel = normalizeRelease({
        ...payload,
        type: normalizeType(payload?.type || row?.release_type) || 'сингл',
        name: clean(payload?.name || row?.track_name || ''),
        nick: clean(payload?.nick || row?.artist_name || ''),
        genre: clean(payload?.genre || row?.genre || ''),
        tg: clean(payload?.tg || payload?.telegram_contact || ''),
        status: mapFormStatusToReleaseStatus(row?.status || ''),
        reject_reason: clean(row?.reject_reason || payload?.reject_reason || ''),
        upc: clean(row?.upc || payload?.upc || ''),
        moderation_message_id: Number(row?.moderation_message_id || payload?.moderation_message_id || 0) || 0,
        submission_time: clean(row?.submission_key || row?.created_at || payload?.submission_time || ''),
        moderation_time: clean(row?.updated_at || payload?.moderation_time || ''),
        source: clean(payload?.source || row?.source || 'mini_app'),
        username: clean(payload?.username || row?.username || ''),
        supabase_form_id: clean(row?.id || row?.form_id || '')
      });
      remoteDb[uid].push(rel);
    }

    const remoteCab = {};
    for (const row of cabinetRows) {
      const uid = String(row?.user_id || '');
      if (!uid) continue;
      remoteCab[uid] = row?.profile && typeof row.profile === 'object' ? row.profile : {};
    }
    for (const row of userRows || []) {
      const uid = clean(row?.telegram_id || '');
      if (!uid) continue;
      remoteCab[uid] = {
        approved: !!row?.cabinet_active,
        activated_at: clean(row?.created_at || ''),
        username: clean(row?.username || ''),
        first_name: clean(row?.first_name || '')
      };
    }

    mergeReleasesIntoDb(remoteDb, 'supabase');
    mergeCabinetIntoState(remoteCab, 'supabase');
    saveJson(DB_FILE, db);
    saveJson(CAB_FILE, cabUsers);
    exportReleases();
    exportCabinet();
    console.info(
      `[supabase] hydrated: releases=${releaseRows.length}, cabinet=${cabinetRows.length}, forms=${formsRows.length}, users=${userRows.length}`
    );
  } catch (e) {
    console.error('[supabase] hydrate failed:', clean(e?.message || e));
  }
}

function buildSupabaseReleaseRows() {
  const rows = [];
  const now = new Date().toISOString();
  for (const [uid, listRaw] of Object.entries(db || {})) {
    const list = Array.isArray(listRaw) ? listRaw : [];
    for (let idx = 0; idx < list.length; idx += 1) {
      const rawRel = list[idx];
      if (!rawRel || typeof rawRel !== 'object') continue;
      const rel = normalizeRelease(rawRel);
      rows.push({
        user_id: String(uid),
        release_idx: idx,
        release_data: rel,
        updated_at: now
      });
    }
  }
  return rows;
}

function buildSupabaseCabinetRows() {
  const rows = [];
  const now = new Date().toISOString();
  for (const [uid, profileRaw] of Object.entries(cabUsers || {})) {
    const profile = profileRaw && typeof profileRaw === 'object' ? profileRaw : {};
    rows.push({
      user_id: String(uid),
      profile: {
        approved: !!profile.approved,
        activated_at: clean(profile.activated_at || ''),
        username: clean(profile.username || ''),
        first_name: clean(profile.first_name || '')
      },
      updated_at: now
    });
  }
  return rows;
}

function buildSupabaseFormRows() {
  const rows = [];
  for (const [uid, listRaw] of Object.entries(db || {})) {
    const list = Array.isArray(listRaw) ? listRaw : [];
    for (let idx = 0; idx < list.length; idx += 1) {
      const rel = normalizeRelease(list[idx] || {});
      const row = buildSupabaseFormRow(uid, { username: rel.username || '' }, rel, mapReleaseStatusToFormStatus(rel.status));
      row.form_payload = safeJson(rel) || {};
      row.moderation_message_id = Number(rel.moderation_message_id || 0) || null;
      rows.push(row);
    }
  }
  return rows;
}

function buildSupabaseUserRows() {
  const rows = [];
  const now = new Date().toISOString();
  for (const [uid, profileRaw] of Object.entries(cabUsers || {})) {
    const profile = profileRaw && typeof profileRaw === 'object' ? profileRaw : {};
    rows.push({
      telegram_id: String(uid),
      username: clean(profile.username || ''),
      first_name: clean(profile.first_name || ''),
      cabinet_active: !!profile.approved,
      created_at: clean(profile.activated_at || now),
      updated_at: now
    });
  }
  return rows;
}

function buildSupabaseApprovedReleaseRows() {
  const rows = [];
  const now = new Date().toISOString();
  for (const [uid, listRaw] of Object.entries(db || {})) {
    const list = Array.isArray(listRaw) ? listRaw : [];
    for (let idx = 0; idx < list.length; idx += 1) {
      const rel = normalizeRelease(list[idx] || {});
      const st = canonicalStatus(rel.status);
      if (![STATUS.APPROVED, STATUS.PUBLISHED].includes(st)) continue;
      const formId = clean(rel.supabase_form_id || `${uid}:${clean(rel.submission_time || idx)}`);
      rows.push({
        form_id: formId,
        telegram_id: String(uid),
        username: clean(rel.username || ''),
        artist_name: sanitizeText(rel.nick || '', 130),
        track_name: sanitizeText(rel.name || '', 160),
        genre: sanitizeText(rel.genre || '', 90),
        release_type: normalizeType(rel.type) === 'альбом' ? 'album' : 'single',
        status: 'approved',
        approved_at: clean(rel.moderation_time || now),
        updated_at: now,
        release_data: safeJson(rel) || {}
      });
    }
  }
  return rows;
}

async function supabaseUpsertRows(tableName, rows, chunkSize = 250) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabaseRequest(`${tableName}?on_conflict=user_id,release_idx`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: chunk
    });
  }
}

async function supabaseUpsertCabinetRows(tableName, rows, chunkSize = 250) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabaseRequest(`${tableName}?on_conflict=user_id`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: chunk
    });
  }
}

async function supabaseUpsertFormRows(tableName, rows, chunkSize = 250) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabaseRequest(`${tableName}?on_conflict=telegram_id,submission_key`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: chunk
    });
  }
}

async function supabaseUpsertUserRows(tableName, rows, chunkSize = 250) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabaseRequest(`${tableName}?on_conflict=telegram_id`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: chunk
    });
  }
}

async function supabaseUpsertApprovedRows(tableName, rows, chunkSize = 250) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabaseRequest(`${tableName}?on_conflict=form_id`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: chunk
    });
  }
}

async function syncSupabaseNow(reason = '') {
  if (!SUPABASE_SYNC_ENABLED) return;
  if (supabaseSyncInProgress) {
    supabaseSyncQueued = true;
    return;
  }
  supabaseSyncInProgress = true;
  try {
    const relRows = buildSupabaseReleaseRows();
    const cabRows = buildSupabaseCabinetRows();
    const formRows = supabaseFeatureState.forms ? buildSupabaseFormRows() : [];
    const userRows = supabaseFeatureState.users ? buildSupabaseUserRows() : [];
    const approvedRows = supabaseFeatureState.public_releases ? buildSupabaseApprovedReleaseRows() : [];

    await supabaseUpsertRows(SUPABASE_RELEASES_TABLE, relRows);
    await supabaseUpsertCabinetRows(SUPABASE_CABINET_TABLE, cabRows);

    if (supabaseFeatureState.forms) {
      try {
        await supabaseUpsertFormRows(SUPABASE_FORMS_TABLE, formRows);
      } catch (e) {
        const errText = clean(e?.message || e);
        console.error('[supabase] forms sync failed:', errText);
        if (isSupabaseSchemaError(e)) disableSupabaseFeature('forms', errText);
      }
    }
    if (supabaseFeatureState.users) {
      try {
        await supabaseUpsertUserRows(SUPABASE_USERS_TABLE, userRows);
      } catch (e) {
        const errText = clean(e?.message || e);
        console.error('[supabase] users sync failed:', errText);
        if (isSupabaseSchemaError(e)) disableSupabaseFeature('users', errText);
      }
    }
    if (supabaseFeatureState.public_releases) {
      try {
        await supabaseUpsertApprovedRows(SUPABASE_PUBLIC_RELEASES_TABLE, approvedRows);
      } catch (e) {
        const errText = clean(e?.message || e);
        console.error('[supabase] public releases sync failed:', errText);
        if (isSupabaseSchemaError(e)) disableSupabaseFeature('public_releases', errText);
      }
    }

    console.info(
      `[supabase] synced (${reason || 'manual'}): releases=${relRows.length}, cabinet=${cabRows.length}` +
      ` forms=${formRows.length} users=${userRows.length} approved=${approvedRows.length}`
    );
  } catch (e) {
    console.error('[supabase] sync failed:', clean(e?.message || e));
  } finally {
    supabaseSyncInProgress = false;
    if (supabaseSyncQueued) {
      supabaseSyncQueued = false;
      setTimeout(() => { syncSupabaseNow('queued').catch(() => {}); }, 25);
    }
  }
}

function scheduleSupabaseSync(reason = '') {
  if (!SUPABASE_SYNC_ENABLED) return;
  if (supabaseSyncTimer) clearTimeout(supabaseSyncTimer);
  supabaseSyncTimer = setTimeout(() => {
    supabaseSyncTimer = null;
    syncSupabaseNow(reason).catch(() => {});
  }, Math.max(300, SUPABASE_SYNC_DEBOUNCE_MS));
}

async function supabaseDeleteAllReleases() {
  if (!SUPABASE_SYNC_ENABLED) return;
  await supabaseRequest(`${SUPABASE_RELEASES_TABLE}?user_id=neq.__none__`, {
    method: 'DELETE',
    headers: { Prefer: 'return=minimal' }
  });
}

function hasValidWebAppUrl() {
  return !!WEBAPP_URL && !WEBAPP_URL.includes('example.com');
}

function shortTgError(err) {
  const raw = clean(err?.message || err);
  const m = /\[[^\]]+\]\s*(.+)$/.exec(raw);
  const text = m ? m[1] : raw;
  return text || 'неизвестная ошибка';
}

function moderationPayload(payload = {}) {
  const out = { ...payload };
  if (out.chat_id === undefined || out.chat_id === null) out.chat_id = MOD_CHAT;
  if (Number(MODERATION_THREAD_ID) > 0 && (out.message_thread_id === undefined || out.message_thread_id === null)) {
    out.message_thread_id = Number(MODERATION_THREAD_ID);
  }
  return out;
}

function moderationErrorHint(errText) {
  const src = clean(errText).toLowerCase();
  if (!src) return '';
  if (src.includes('chat not found')) return 'Проверьте MODERATION_CHAT_ID: бот не видит этот чат.';
  if (src.includes('bot is not a member')) return 'Добавьте бота в группу модерации.';
  if (src.includes('not enough rights') || src.includes('have no rights') || src.includes('forbidden')) {
    return 'Выдайте боту права на отправку сообщений в группе модерации.';
  }
  if (src.includes('message thread not found') || src.includes('topic')) {
    return 'Для группы с топиками укажите MODERATION_THREAD_ID (ID нужного топика).';
  }
  return '';
}

function describeBotMemberStatus(member) {
  const status = clean(member?.status).toLowerCase();
  if (!status) return { ok: false, reason: 'не удалось определить статус бота в чате', can_send_messages: null, status: '' };
  if (status === 'left' || status === 'kicked') {
    return { ok: false, reason: `бот имеет статус "${status}" в группе модерации`, can_send_messages: false, status };
  }
  if (status === 'restricted') {
    const canSend = member?.can_send_messages !== false;
    if (!canSend) {
      return { ok: false, reason: 'бот ограничен и не может отправлять сообщения (can_send_messages=false)', can_send_messages: false, status };
    }
    return { ok: true, reason: '', can_send_messages: true, status };
  }
  if (status === 'member' || status === 'administrator' || status === 'creator') {
    return { ok: true, reason: '', can_send_messages: true, status };
  }
  return { ok: true, reason: '', can_send_messages: null, status };
}

async function ensureModerationHealth(force = false) {
  const now = Date.now();
  if (!force && moderationHealth.checked_at && (now - moderationHealth.checked_at) < MODERATION_HEALTH_TTL_MS) {
    return moderationHealth;
  }
  await verifyModerationChatAccess(force);
  return moderationHealth;
}

function sendModerationText(text, extra = {}) {
  return tg('sendMessage', moderationPayload({
    text,
    parse_mode: 'HTML',
    disable_web_page_preview: true,
    ...extra
  }));
}

function keyboardMain() {
  return { inline_keyboard: [
    [{ text: '📀 Дистрибуция', callback_data: 'menu_distribution' }],
    [{ text: '💼 Сервисы', callback_data: 'menu_services' }],
    [{ text: '🧑‍💻 Кабинет', callback_data: 'menu_cabinet' }],
    [{ text: '🌐 Комьюнити', callback_data: 'menu_community' }]
  ]};
}
function keyboardDist() {
  return { inline_keyboard: [
    [{ text: 'Загрузить релиз (анкета в боте)', callback_data: 'report_text' }],
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
    ],
    [
      { text: '📦 Присвоить UPC', callback_data: `m_upc_${uid}_${idx}` }
    ]
  ]};
}

function sendText(chatId, text, extra = {}) {
  return tg('sendMessage', { chat_id: chatId, text, parse_mode: 'HTML', disable_web_page_preview: true, ...extra });
}
function sendTextPlain(chatId, text, extra = {}) {
  return tg('sendMessage', { chat_id: chatId, text, disable_web_page_preview: true, ...extra });
}

async function sendDocument(chatId, filePath, caption = '') {
  const abs = path.isAbsolute(filePath) ? filePath : path.resolve(ROOT, filePath);
  const payload = fs.readFileSync(abs);
  const form = new FormData();
  form.append('chat_id', String(chatId));
  if (caption) form.append('caption', caption);
  form.append('document', new Blob([payload]), path.basename(abs));

  const response = await fetch(`${API}/sendDocument`, {
    method: 'POST',
    body: form
  }).catch((e) => {
    throw new Error(`[sendDocument] ${clean(e?.message || e)}`);
  });

  const result = await response.json().catch(() => ({}));
  if (!response.ok || !result.ok) {
    throw new Error(`[sendDocument] ${result.description || response.statusText}`);
  }
  return result.result;
}
function welcomeText() {
  return 'Добро пожаловать в систему дистрибуции CXRNER MUSIC.\nУправляй релизами. Загружай треки. Масштабируй звук.';
}

function normalizeType(v) {
  const t = clean(v).toLowerCase();
  if (['сингл', 'single', 'singl', 'сњсѓрЅрір»', 'сингал', 'сингел'].includes(t)) return 'сингл';
  if (['альбом', 'album', 'р°р»сњр±рѕрј'].includes(t)) return 'альбом';
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
  const yandexText = r.yandex === 'create_new_card' ? 'Создать новую карточку' : (r.yandex || '.');
  const lines = [
    '🎵 <b>НОВАЯ АНКЕТА!</b>',
    `От: ${esc(u)}`,
    `ID: <code>${esc(uid)}</code>`,
    `Тип: ${esc(r.type || '—')}`,
    '',
    `🎵 <b>Название:</b> ${esc(r.name || '—')}`,
    `✨ <b>Саб-название:</b> ${esc(r.subname || '.')}`,
    `🎤 <b>Ник:</b> ${esc(r.nick || '—')}`,
    `🪪 <b>ФИО:</b> ${esc(r.fio || '—')}`,
    `📅 <b>Дата:</b> ${esc(r.date || '—')}`,
    `🧩 <b>Версия:</b> ${esc(r.version || 'Оригинал')}`,
    `🏷 <b>Жанр:</b> ${esc(r.genre || '—')}`,
    `🔗 <b>Ссылка:</b> ${esc(r.link || '—')}`,
    `🟡 <b>Яндекс Музыка:</b> ${esc(yandexText)}`,
    `⚠️ <b>Мат:</b> ${esc(r.mat || '—')}`,
    `📢 <b>Промо:</b> ${esc(r.promo || '.')}`,
    `💬 <b>Комментарий:</b> ${esc(r.comment || '.')}`
  ];
  if (r.type === 'альбом') lines.push(`📋 <b>Tracklist:</b> ${esc(r.tracklist || '.')}`);
  lines.push(`📱 <b>Tg:</b> ${esc(r.tg || '—')}`);
  return lines.join('\n');
}
function withStatus(status, original) {
  const canon = canonicalStatus(status);
  return `${STATUS_EMOJI[canon] || '⏳'} <b>СТАТУС: ${esc(STATUS_TEXT[canon] || canon)}</b>\n\n${original || ''}`;
}
function canonicalStatus(status) {
  const raw = clean(status).toLowerCase();
  return LEGACY_STATUS_MAP[raw] || STATUS.ON_UPLOAD;
}
function statusText(status) {
  const canon = canonicalStatus(status);
  return STATUS_TEXT[canon] || canon;
}
function statusEmoji(status) {
  const canon = canonicalStatus(status);
  return STATUS_EMOJI[canon] || '⏳';
}
function normalizeInteractionLog(listRaw) {
  const src = Array.isArray(listRaw) ? listRaw : [];
  const out = [];
  for (const row of src) {
    if (!row || typeof row !== 'object') continue;
    const type = clean(row.type || 'action');
    const at = clean(row.at || row.timestamp || row.time || '');
    const actorId = clean(row.actor_id || row.user_id || row.moderator_id || '');
    const actorUsername = clean(row.actor_username || row.username || '');
    const actorName = clean(row.actor_name || row.name || '');
    const statusFromRaw = clean(row.status_from || row.from_status || '');
    const statusToRaw = clean(row.status_to || row.to_status || '');
    const ev = {
      type: type || 'action',
      at,
      actor_id: actorId,
      actor_username: actorUsername,
      actor_name: actorName,
      status_from: statusFromRaw ? canonicalStatus(statusFromRaw) : '',
      status_to: statusToRaw ? canonicalStatus(statusToRaw) : '',
      upc: clean(row.upc || ''),
      reason: clean(row.reason || row.reject_reason || ''),
      note: clean(row.note || row.details || '')
    };
    out.push(ev);
  }
  return out;
}
function formatInteractionTime(iso) {
  const src = clean(iso);
  if (!src) return '';
  const dt = new Date(src);
  if (!Number.isFinite(dt.getTime())) return src.slice(0, 19).replace('T', ' ');
  const dd = String(dt.getDate()).padStart(2, '0');
  const mm = String(dt.getMonth() + 1).padStart(2, '0');
  const hh = String(dt.getHours()).padStart(2, '0');
  const mi = String(dt.getMinutes()).padStart(2, '0');
  return `${dd}.${mm} ${hh}:${mi}`;
}
function interactionActorLabel(ev) {
  const username = clean(ev?.actor_username || '');
  if (username) return `@${username}`;
  const name = clean(ev?.actor_name || '');
  if (name) return name;
  const actorId = clean(ev?.actor_id || '');
  if (actorId) return `ID ${actorId}`;
  return 'неизвестный';
}
function shortenForInteraction(text, maxLen = 120) {
  const src = clean(text);
  if (!src) return '';
  if (src.length <= maxLen) return src;
  return `${src.slice(0, maxLen - 3)}...`;
}
function interactionActionLabel(ev) {
  const type = clean(ev?.type || '');
  if (type === 'status_change') {
    const toStatus = clean(ev?.status_to || '');
    const fromStatus = clean(ev?.status_from || '');
    const toText = toStatus ? statusText(toStatus) : '';
    const fromText = fromStatus ? statusText(fromStatus) : '';
    if (fromText && toText && fromText !== toText) return `статус: ${fromText} -> ${toText}`;
    if (toText) return `статус: ${toText}`;
    return 'изменил статус';
  }
  if (type === 'upc_assigned') {
    const upc = clean(ev?.upc || '');
    return upc ? `присвоил UPC: ${upc}` : 'присвоил UPC';
  }
  if (type === 'marked_free') return 'пометил как свободный к отгрузке';
  if (type === 'reject_reason') {
    const reason = shortenForInteraction(ev?.reason || ev?.note, 100);
    return reason ? `указал причину: ${reason}` : 'указал причину отклонения';
  }
  return shortenForInteraction(ev?.note || type || 'действие', 120) || 'действие';
}
function pushReleaseInteraction(rel, type, actor, payload = {}) {
  if (!rel || typeof rel !== 'object') return;
  const firstName = clean(actor?.first_name || '');
  const lastName = clean(actor?.last_name || '');
  const actorName = clean([firstName, lastName].filter(Boolean).join(' '));
  const statusFromRaw = clean(payload.status_from || '');
  const statusToRaw = clean(payload.status_to || '');
  const event = {
    type: clean(type || 'action') || 'action',
    at: new Date().toISOString(),
    actor_id: clean(actor?.id || payload.actor_id || ''),
    actor_username: clean(actor?.username || payload.actor_username || ''),
    actor_name: actorName || clean(payload.actor_name || ''),
    status_from: statusFromRaw ? canonicalStatus(statusFromRaw) : '',
    status_to: statusToRaw ? canonicalStatus(statusToRaw) : '',
    upc: clean(payload.upc || ''),
    reason: clean(payload.reason || ''),
    note: clean(payload.note || '')
  };
  rel.interactions = normalizeInteractionLog(rel.interactions);
  rel.interactions.push(event);
  if (rel.interactions.length > MODERATION_ACTION_LOG_LIMIT) {
    rel.interactions = rel.interactions.slice(-MODERATION_ACTION_LOG_LIMIT);
  }
  rel.last_action_at = event.at;
  rel.last_actor_id = event.actor_id;
  rel.last_actor_username = event.actor_username;
  rel.last_actor_name = event.actor_name;
  rel.last_action_type = event.type;
  rel.last_action_note = event.note || event.reason || event.upc || '';
}
function buildModerationActionsBlock(rel) {
  const list = normalizeInteractionLog(rel?.interactions);
  if (!list.length) {
    const fallbackUsername = clean(rel?.moderator_username || '');
    const fallbackId = clean(rel?.moderator || '');
    if (!fallbackUsername && !fallbackId) return '';
    const actor = fallbackUsername
      ? (fallbackUsername.includes(' ') ? fallbackUsername : `@${fallbackUsername}`)
      : `ID ${fallbackId}`;
    const when = formatInteractionTime(rel?.moderation_time || '');
    const statusLabel = statusText(rel?.status || STATUS.ON_UPLOAD);
    const line = when
      ? `• <i>${esc(when)}</i> — ${esc(actor)}: ${esc(`статус: ${statusLabel}`)}`
      : `• ${esc(actor)}: ${esc(`статус: ${statusLabel}`)}`;
    return ['👤 <b>Действия:</b>', line].join('\n');
  }
  const tail = list.slice(-MODERATION_HISTORY_LIMIT).reverse();
  const lines = ['👤 <b>Действия:</b>'];
  for (const ev of tail) {
    const when = formatInteractionTime(ev.at);
    const actor = interactionActorLabel(ev);
    const action = interactionActionLabel(ev);
    if (when) lines.push(`• <i>${esc(when)}</i> — ${esc(actor)}: ${esc(action)}`);
    else lines.push(`• ${esc(actor)}: ${esc(action)}`);
  }
  return lines.join('\n');
}
function ensureModDbShape() {
  modDb = modDb && typeof modDb === 'object' ? modDb : {};
  if (!Array.isArray(modDb.moderation_messages)) modDb.moderation_messages = [];
  if (!Array.isArray(modDb.pending_actions)) modDb.pending_actions = [];
}
function cleanupPendingActions() {
  ensureModDbShape();
  const now = Date.now();
  const before = modDb.pending_actions.length;
  modDb.pending_actions = modDb.pending_actions.filter((it) => {
    const ts = new Date(it?.created_at || 0).getTime();
    return Number.isFinite(ts) && (now - ts) <= PENDING_ACTION_TTL_MS;
  });
  if (modDb.pending_actions.length !== before) saveModDb();
}
function findRelease(uid, idx) {
  const list = Array.isArray(db?.[uid]) ? db[uid] : null;
  if (!list || !list[idx]) return null;
  return list[idx];
}
function findReleaseByModerationMessageId(messageId) {
  const mid = Number(messageId || 0);
  if (!mid) return null;
  for (const [uid, listRaw] of Object.entries(db || {})) {
    const list = Array.isArray(listRaw) ? listRaw : [];
    for (let idx = 0; idx < list.length; idx += 1) {
      const rel = list[idx];
      if (Number(rel?.moderation_message_id || 0) === mid) {
        return { uid: String(uid), idx, rel };
      }
    }
  }
  return null;
}
function resolveReleaseRef(uid, idx, fallbackMessageId = 0) {
  const direct = findRelease(uid, idx);
  if (direct) return { uid: String(uid), idx: Number(idx), rel: direct };
  const byMsg = findReleaseByModerationMessageId(fallbackMessageId);
  if (byMsg) return byMsg;
  return null;
}
function syncModerationMirror(uid, idx, rel) {
  ensureModDbShape();
  let found = false;
  const interactions = normalizeInteractionLog(rel.interactions);
  for (const it of modDb.moderation_messages) {
    if (
      String(it.user_id) === String(uid) &&
      (Number(it.idx) === Number(idx) || it.submission_time === rel.submission_time)
    ) {
      Object.assign(it, {
        status: rel.status,
        moderation_time: rel.moderation_time || '',
        reject_reason: rel.reject_reason || '',
        moderator_comment: rel.moderator_comment || '',
        user_deleted: !!rel.user_deleted,
        upc: rel.upc || '',
        interactions,
        last_action_at: rel.last_action_at || '',
        last_actor_id: rel.last_actor_id || '',
        last_actor_username: rel.last_actor_username || '',
        last_actor_name: rel.last_actor_name || '',
        last_action_type: rel.last_action_type || '',
        last_action_note: rel.last_action_note || '',
        available_for_upload: !!rel.available_for_upload,
        available_marked_at: rel.available_marked_at || '',
        available_marked_by: rel.available_marked_by || '',
        available_marked_by_username: rel.available_marked_by_username || '',
        available_marked_by_name: rel.available_marked_by_name || '',
        moderation_message_id: rel.moderation_message_id || it.moderation_message_id || 0,
        message_id: rel.moderation_message_id || it.message_id || 0
      });
      found = true;
    }
  }
  if (!found) {
    modDb.moderation_messages.push({
      ...rel,
      user_id: String(uid),
      idx: Number(idx),
      message_id: Number(rel.moderation_message_id || 0)
    });
  }
}
function buildModerationText(uid, rel) {
  const orig = rel.moderation_original_text || fmtForm({ username: rel.username || '' }, uid, rel);
  const details = [];
  if (rel.upc) details.push(`📦 <b>UPC:</b> <code>${esc(rel.upc)}</code>`);
  if (canonicalStatus(rel.status) === STATUS.REJECTED && rel.reject_reason) details.push(`❌ <b>Причина:</b> ${esc(rel.reject_reason)}`);
  if (canonicalStatus(rel.status) === STATUS.ON_UPLOAD && rel.available_for_upload) {
    const whoUsername = clean(rel.available_marked_by_username || rel.last_actor_username || '');
    const whoName = clean(rel.available_marked_by_name || rel.last_actor_name || '');
    const markedAt = formatInteractionTime(rel.available_marked_at || rel.last_action_at || '');
    let freeLine = '🆕 <b>Свободно для отгрузки</b>';
    if (whoUsername) freeLine += ` • @${esc(whoUsername)}`;
    else if (whoName) freeLine += ` • ${esc(whoName)}`;
    if (markedAt) freeLine += ` • <i>${esc(markedAt)}</i>`;
    details.push(freeLine);
  }
  const actionsBlock = buildModerationActionsBlock(rel);
  if (actionsBlock) details.push(actionsBlock);
  const body = details.length ? `${orig}\n\n${details.join('\n')}` : orig;
  return withStatus(rel.status || STATUS.ON_UPLOAD, body);
}

function truncateMiddle(text, maxLen = 280) {
  const src = clean(text);
  if (!src || src.length <= maxLen) return src || '.';
  const head = Math.max(40, Math.floor(maxLen * 0.6));
  const tail = Math.max(20, maxLen - head - 3);
  return `${src.slice(0, head)}...${src.slice(src.length - tail)}`;
}

function buildCompactModerationOriginal(user, uid, rel) {
  const compact = {
    ...rel,
    name: truncateMiddle(rel.name, 140),
    subname: truncateMiddle(rel.subname, 120),
    nick: truncateMiddle(rel.nick, 180),
    fio: truncateMiddle(rel.fio, 180),
    genre: truncateMiddle(rel.genre, 160),
    link: truncateMiddle(rel.link, 220),
    yandex: truncateMiddle(rel.yandex, 220),
    promo: truncateMiddle(rel.promo, 320),
    comment: truncateMiddle(rel.comment, 240),
    tracklist: truncateMiddle(rel.tracklist, 350),
    tg: truncateMiddle(rel.tg, 120)
  };
  return `${fmtForm(user, uid, compact)}\n\n⚠️ <i>Длинные поля автоматически сокращены для Telegram.</i>`;
}
async function refreshModerationMessage(uid, idx, rel, fallbackMessageId = 0) {
  const messageId = Number(rel.moderation_message_id || fallbackMessageId || 0);
  if (!messageId) return false;
  try {
    await tg('editMessageText', {
      chat_id: MOD_CHAT,
      message_id: messageId,
      text: buildModerationText(uid, rel),
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      reply_markup: moderationKeyboard(uid, idx)
    });
    return true;
  } catch (e) {
    const msg = clean(e?.message || e);
    if (msg.includes('message is not modified')) return true;
    console.error('[MODERATION] edit failed:', msg || e);
    return false;
  }
}
function removePendingActionByIndex(idx) {
  ensureModDbShape();
  if (idx < 0 || idx >= modDb.pending_actions.length) return;
  modDb.pending_actions.splice(idx, 1);
  saveModDb();
}
function findPendingActionForReply(moderatorId, replyMessageId) {
  cleanupPendingActions();
  ensureModDbShape();
  const modId = String(moderatorId);
  const rid = Number(replyMessageId || 0);
  for (let i = modDb.pending_actions.length - 1; i >= 0; i -= 1) {
    const it = modDb.pending_actions[i];
    if (String(it.moderator_id) !== modId) continue;
    if (Number(it.prompt_message_id) === rid || Number(it.release_message_id) === rid) {
      return { index: i, action: it };
    }
  }
  return null;
}
function rememberPendingAction(data) {
  cleanupPendingActions();
  ensureModDbShape();
  modDb.pending_actions = modDb.pending_actions.filter((it) => String(it.moderator_id) !== String(data.moderator_id));
  modDb.pending_actions.push(data);
  saveModDb();
}
function normalizeUpcCode(raw) {
  const code = clean(raw).replace(/\s+/g, '').toUpperCase();
  if (!code) return '';
  if (code.length < 4 || code.length > 40) return '';
  if (!/^[A-Z0-9._-]+$/.test(code)) return '';
  return code;
}
function parseModerationReplyShortcut(text) {
  const src = clean(text);
  if (!src) return null;
  const upcMatch = /^(?:upc|апс|юпс|код|upc\/ean)\s*[:\-]\s*(.+)$/i.exec(src);
  if (upcMatch) return { type: 'upc', value: clean(upcMatch[1]) };
  const rejectMatch = /^(?:reject|отклон|причина)\s*[:\-]\s*(.+)$/i.exec(src);
  if (rejectMatch) return { type: 'reject_reason', value: clean(rejectMatch[1]) };
  return null;
}
async function notifyArtistAboutRelease(uid, rel, status) {
  try {
    const canon = canonicalStatus(status);
    let note = `${statusEmoji(canon)} <b>${esc(statusText(canon))}</b>\n\n`;
    note += `🎵 <b>${esc(rel.name || 'Релиз')}</b>\n👤 ${esc(rel.nick || '—')}\n📅 ${esc(rel.date || '—')}`;
    if (rel.upc) note += `\n📦 UPC: <code>${esc(rel.upc)}</code>`;
    if (canon === STATUS.REJECTED && rel.reject_reason) note += `\n❌ Причина: ${esc(rel.reject_reason)}`;
    await sendText(Number(uid), note);
  } catch (e) {
    console.error('[MODERATION] notify failed:', clean(e?.message || e));
  }
}
async function applyReleaseStatus(uid, idx, status, moderator, opts = {}) {
  const ref = resolveReleaseRef(uid, idx, opts.fallbackMessageId || 0);
  if (!ref || !ref.rel) return { ok: false, error: 'Релиз не найден.' };

  uid = ref.uid;
  idx = ref.idx;
  const rel = ref.rel;
  const prevStatus = canonicalStatus(rel.status || STATUS.ON_UPLOAD);
  const canonStatus = canonicalStatus(status);

  rel.status = canonStatus;
  rel.moderation_time = new Date().toISOString();
  rel.moderator = String(moderator?.id || '');
  rel.moderator_username = moderator?.username || moderator?.first_name || 'moderator';
  if (canonStatus === STATUS.REJECTED) {
    const reason = clean(opts.rejectReason || rel.reject_reason || 'Отклонено модератором');
    rel.reject_reason = reason;
  }
  if (canonStatus !== STATUS.REJECTED && opts.clearRejectReason) {
    rel.reject_reason = '';
  }
  if (canonStatus === STATUS.NEEDS_FIX && !rel.moderator_comment) rel.moderator_comment = 'Нужны правки перед публикацией';
  if (canonStatus === STATUS.DELETED) rel.user_deleted = true;
  if (canonStatus !== STATUS.DELETED && opts.restoreFromDelete) rel.user_deleted = false;
  if (canonStatus === STATUS.ON_UPLOAD) {
    rel.available_for_upload = true;
    rel.available_marked_at = rel.moderation_time;
    rel.available_marked_by = String(moderator?.id || '');
    rel.available_marked_by_username = clean(moderator?.username || '');
    rel.available_marked_by_name = clean([moderator?.first_name || '', moderator?.last_name || ''].filter(Boolean).join(' '));
  } else {
    rel.available_for_upload = false;
  }

  const statusChanged = prevStatus !== canonStatus;
  const reasonChanged = canonStatus === STATUS.REJECTED && clean(opts.rejectReason || '') !== '';
  if (statusChanged || reasonChanged) {
    pushReleaseInteraction(rel, 'status_change', moderator, {
      status_from: prevStatus,
      status_to: canonStatus,
      reason: canonStatus === STATUS.REJECTED ? rel.reject_reason : '',
      note: clean(opts.note || '')
    });
  }

  saveDb();
  syncModerationMirror(uid, idx, rel);
  saveModDb();

  const formStatus = mapReleaseStatusToFormStatus(canonStatus);
  await supabasePatchFormByRelease(uid, rel, {
    status: formStatus,
    reject_reason: canonStatus === STATUS.REJECTED ? clean(rel.reject_reason || '') : '',
    upc: clean(rel.upc || ''),
    moderation_message_id: Number(rel.moderation_message_id || 0) || null,
    form_payload: safeJson(rel) || {}
  });
  if (canonStatus === STATUS.APPROVED || canonStatus === STATUS.PUBLISHED) {
    await supabaseUpsertApprovedRelease(uid, rel);
  } else if ([STATUS.REJECTED, STATUS.DELETED].includes(canonStatus)) {
    await supabaseDeleteApprovedRelease(uid, rel);
  }

  console.info(
    `[MODERATION] status change: user_id=${uid} idx=${idx} ${prevStatus} -> ${canonStatus}` +
    ` by=${clean(moderator?.username || moderator?.id || '-')}`
  );

  await refreshModerationMessage(uid, idx, rel, opts.fallbackMessageId);
  await notifyArtistAboutRelease(uid, rel, canonStatus);
  return { ok: true, rel, uid, idx, status: canonStatus };
}
async function startModerationReplyFlow(query, type, uid, idx) {
  if (Number(query?.message?.chat?.id || 0) !== Number(MOD_CHAT)) {
    await sendText(query.message.chat.id, 'Эти кнопки работают только в группе модерации.');
    return;
  }
  if (!(await canModerate(query.from.id))) {
    await sendText(query.message.chat.id, 'Доступ только участникам группы модерации.');
    return;
  }
  const ref = resolveReleaseRef(uid, idx, Number(query?.message?.message_id || 0));
  if (!ref || !ref.rel) {
    await sendText(query.message.chat.id, 'Релиз не найден.');
    return;
  }
  uid = ref.uid;
  idx = ref.idx;
  const rel = ref.rel;
  const replyTo = Number(rel.moderation_message_id || query.message.message_id || 0);
  const text = type === 'upc'
    ? '📦 Отправьте UPC код <b>ответом</b> на это сообщение (или на анкету релиза).'
    : '❌ Отправьте причину отклонения <b>ответом</b> на это сообщение (или на анкету релиза).';
  const prompt = await sendText(query.message.chat.id, text, { reply_to_message_id: replyTo });
  rememberPendingAction({
    type,
    uid: String(uid),
    idx: Number(idx),
    moderator_id: String(query.from.id),
    release_message_id: replyTo,
    prompt_message_id: Number(prompt?.message_id || 0),
    created_at: new Date().toISOString()
  });
}
async function handleModerationReplyMessage(msg) {
  if (Number(msg?.chat?.id || 0) !== Number(MOD_CHAT)) return false;
  const text = clean(msg.text);
  const replyToId = Number(msg.reply_to_message?.message_id || 0);
  if (!text || !replyToId) return false;
  if (!(await canModerate(msg.from?.id))) return false;

  const pending = findPendingActionForReply(msg.from.id, replyToId);
  let actionType = '';
  let actionValue = text;
  let uid = '';
  let idx = 0;
  let rel = null;
  let pendingIndex = -1;
  let fallbackMessageId = replyToId;

  if (pending) {
    const { index, action } = pending;
    pendingIndex = index;
    actionType = String(action.type || '');
    uid = String(action.uid || '');
    idx = Number(action.idx || 0);
    fallbackMessageId = Number(action.release_message_id || replyToId || 0);
    const ref = resolveReleaseRef(uid, idx, fallbackMessageId);
    rel = ref?.rel || null;
    if (ref) {
      uid = ref.uid;
      idx = ref.idx;
      fallbackMessageId = Number(rel?.moderation_message_id || fallbackMessageId || 0);
    }
    if (!rel) {
      removePendingActionByIndex(index);
      await sendText(msg.chat.id, '❌ Релиз для этого действия не найден.');
      return true;
    }
  } else {
    const shortcut = parseModerationReplyShortcut(text);
    if (!shortcut) return false;
    const ref = findReleaseByModerationMessageId(replyToId);
    if (!ref || !ref.rel) return false;
    actionType = shortcut.type;
    actionValue = shortcut.value;
    uid = ref.uid;
    idx = ref.idx;
    rel = ref.rel;
    fallbackMessageId = Number(rel.moderation_message_id || replyToId || 0);
  }

  if (actionType === 'upc') {
    const upc = normalizeUpcCode(actionValue);
    if (!upc) {
      await sendText(msg.chat.id, '❌ Неверный UPC. Допустимо 4-40 символов: A-Z, 0-9, ".", "-", "_".');
      return true;
    }
    rel.upc = upc;
    rel.upc_assigned_at = new Date().toISOString();
    rel.upc_assigned_by = String(msg.from.id);
    pushReleaseInteraction(rel, 'upc_assigned', msg.from, { upc, note: 'UPC присвоен в модерации' });
    saveDb();
    await supabasePatchFormByRelease(uid, rel, {
      upc,
      form_payload: safeJson(rel) || {}
    });
    if ([STATUS.APPROVED, STATUS.PUBLISHED].includes(canonicalStatus(rel.status))) {
      await supabaseUpsertApprovedRelease(uid, rel);
    }
    syncModerationMirror(uid, idx, rel);
    saveModDb();
    await refreshModerationMessage(uid, idx, rel, fallbackMessageId);
    if (pendingIndex >= 0) removePendingActionByIndex(pendingIndex);
    await sendText(msg.chat.id, `✅ UPC присвоен: <code>${esc(upc)}</code>`, {
      reply_to_message_id: Number(rel.moderation_message_id || fallbackMessageId || 0) || undefined
    });
    try {
      await sendText(Number(uid), `📦 <b>UPC присвоен</b>\n\n🎵 <b>${esc(rel.name || 'Релиз')}</b>\nUPC: <code>${esc(upc)}</code>`);
    } catch (e) {
      console.error('[MODERATION] upc notify failed:', clean(e?.message || e));
    }
    return true;
  }

  if (actionType === 'reject_reason') {
    const reason = clean(actionValue);
    if (reason.length < 3) {
      await sendText(msg.chat.id, '❌ Причина слишком короткая. Укажите подробнее.');
      return true;
    }
    if (pendingIndex >= 0) removePendingActionByIndex(pendingIndex);
    const out = await applyReleaseStatus(uid, idx, STATUS.REJECTED, msg.from, {
      rejectReason: reason,
      fallbackMessageId,
      note: 'Причина отклонения добавлена ответом на анкету'
    });
    if (!out.ok) {
      await sendText(msg.chat.id, `❌ ${out.error}`);
      return true;
    }
    await sendText(msg.chat.id, '✅ Релиз отклонён, причина отправлена артисту.', {
      reply_to_message_id: Number(rel.moderation_message_id || fallbackMessageId || 0) || undefined
    });
    return true;
  }
  return false;
}
function parseWebappPayload(payload) {
  const src = payload && typeof payload === 'object' ? payload : {};
  let action = sanitizeText(src.action || '', 64).toLowerCase();
  const hasForm = src.form && typeof src.form === 'object';
  if (!action && hasForm) action = 'webapp_release_submit';

  let form = hasForm ? src.form : null;
  if (!form && src && typeof src === 'object' && (src.type || src.name || src.track_title || src.track_name)) {
    action = action || 'webapp_release_submit';
    form = src;
  }

  const telegramId = clean(
    src.telegram_id ||
    src.telegramId ||
    src.user_id ||
    src.userId ||
    src.user?.id ||
    form?.telegram_id ||
    form?.telegramId
  );

  const initData = clean(src.init_data || src.initData || src.tg_init_data || '');
  const requestId = sanitizeText(src.request_id || src.requestId || '', 72);
  const source = sanitizeText(src.source || '', 40) || 'mini_app';
  if (!form) {
    return {
      action,
      form: null,
      telegram_id: telegramId,
      init_data: initData,
      request_id: requestId,
      source
    };
  }

  let date = sanitizeText(form.release_date || form.date || '', 32);
  if (date.includes('-')) {
    const p = date.split('-');
    if (p.length === 3) date = `${p[2]}.${p[1]}.${p[0]}`;
  }
  const trRaw = sanitizeText(form.release_type || form.type || 'single', 20).toLowerCase();
  const type = ['альбом', 'album'].includes(trRaw) ? 'альбом' : 'сингл';
  const artistName = sanitizeText(form.artist_name || form.nick || '', 130);
  const trackName = sanitizeText(form.track_name || form.track_title || form.name || '', 160);
  const genre = sanitizeText(form.genre || '', 90);
  const tgContact = sanitizeText(form.telegram_contact || form.contact || form.tg || '', 180);

  const normalizedForm = {
    type,
    name: trackName,
    subname: sanitizeText(form.subname || '.', 120) || '.',
    has_lyrics: sanitizeText(form.has_lyrics || form.lyrics || 'Нет, это инструментал', 64),
    nick: artistName,
    fio: sanitizeText(form.fio || artistName, 180),
    date,
    version: sanitizeText(form.version || 'Оригинал', 120) || 'Оригинал',
    genre,
    link: sanitizeText(form.link || form.files_link || form.audio_link || '.', 500) || '.',
    yandex: sanitizeText(form.yandex || form.yandex_link || '.', 500) || '.',
    mat: sanitizeText(form.mat || 'Нет', 20) || 'Нет',
    promo: sanitizeText(form.promo || '.', 1200) || '.',
    comment: sanitizeText(form.comment || '.', 1200) || '.',
    tracklist: sanitizeText(form.tracklist || '.', 2400) || '.',
    tg: tgContact,
    artist_name: artistName,
    track_name: trackName,
    release_type: type === 'альбом' ? 'album' : 'single',
    telegram_id: clean(form.telegram_id || form.telegramId || telegramId),
    source
  };

  return {
    action,
    form: normalizedForm,
    telegram_id: telegramId,
    init_data: initData,
    request_id: requestId,
    source
  };
}
function validateForm(form, envelope = {}) {
  const errors = [];
  const source = clean(envelope?.source || form?.source || '').toLowerCase();
  const action = clean(envelope?.action || '').toLowerCase();
  const requireTelegramId = source === 'mini_app'
    || source === 'webapp'
    || action === 'webapp_release_submit'
    || action === 'submit_release';
  const artistName = sanitizeText(form?.artist_name || form?.nick || '', 130);
  const trackName = sanitizeText(form?.track_name || form?.name || '', 160);
  const genreRaw = sanitizeText(form?.genre || '', 90);
  const releaseTypeRaw = sanitizeText(form?.release_type || form?.type || '', 20).toLowerCase();
  const releaseType = ['album', 'альбом'].includes(releaseTypeRaw)
    ? 'album'
    : (['single', 'сингл'].includes(releaseTypeRaw) ? 'single' : '');
  const telegramId = clean(form?.telegram_id || envelope?.telegram_id || '');

  if (!artistName) errors.push('Поле «Artist Name» (artist_name) обязательно.');
  if (!trackName) errors.push('Поле «Track Name» (track_name) обязательно.');
  if (!genreRaw) errors.push('Поле «Genre» (genre) обязательно.');
  if (!releaseType) errors.push('Поле «Release Type» (release_type) должно быть single или album.');
  if (requireTelegramId && !telegramId) errors.push('Поле «telegram_id» обязательно.');
  if (requireTelegramId && telegramId && !/^\d{4,20}$/.test(telegramId)) {
    errors.push('Поле «telegram_id» содержит неверный формат.');
  }

  const type = normalizeType(form?.type);
  if (!type) errors.push('Укажите тип релиза: сингл или альбом.');
  const name = sanitizeText(form?.name || trackName, 160); if (!name) errors.push('Поле «Название релиза» обязательно.');
  const subname = clean(form?.subname) || '.';
  const hasLyrics = sanitizeText(form?.has_lyrics, 64); if (!hasLyrics) errors.push('Укажите, есть ли слова в релизе.');
  const nick = sanitizeText(form?.nick || artistName, 130); if (!nick) errors.push('Поле «Ник исполнителя» обязательно.');
  const fio = sanitizeText(form?.fio, 180); if (!fio) errors.push('Поле «ФИО исполнителя» обязательно.');
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
  const genre = genreRaw || clean(form?.genre); if (!genre) errors.push('Поле «Жанр» обязательно.');
  const link = sanitizeText(form?.link, 500);
  if (!link) errors.push('Добавьте ссылку на файлы.');
  else if (!isHttpUrl(link)) errors.push('Ссылка на файлы должна начинаться с http:// или https://.');
  let yandex = sanitizeText(form?.yandex, 500);
  const yandexLower = yandex.toLowerCase();
  if (!yandex || ['-', 'нет', 'none'].includes(yandexLower)) {
    yandex = '.';
  } else if (['create_new_card', 'создать новую карточку', 'новая карточка'].includes(yandexLower)) {
    yandex = 'create_new_card';
  } else if (!isHttpUrl(yandex)) {
    errors.push('Поле «Яндекс Музыка» должно быть URL, точкой или вариантом «Создать новую карточку».');
  }
  const mat = clean(form?.mat); if (!mat) errors.push('Укажите, есть ли ненормативная лексика.');
  const promo = sanitizeText(form?.promo, 1200) || '.';
  const comment = sanitizeText(form?.comment, 1200) || '.';
  let tracklist = sanitizeText(form?.tracklist, 2400) || '.';
  if (type !== 'альбом') tracklist = '.';
  if (type === 'альбом' && tracklist === '.') errors.push('Для альбома заполните Tracklist.');
  const tgContact = sanitizeText(form?.tg, 180); if (!tgContact) errors.push('Укажите контакт Telegram.');
  return {
    errors,
    data: {
      type,
      name,
      subname,
      has_lyrics: hasLyrics,
      nick,
      fio,
      date,
      version,
      genre,
      link,
      yandex,
      mat,
      promo,
      comment,
      tracklist,
      tg: tgContact,
      artist_name: artistName || nick,
      track_name: trackName || name,
      release_type: releaseType || (type === 'альбом' ? 'album' : 'single'),
      telegram_id: telegramId
    }
  };
}

async function submitReleaseToModeration(user, uid, releaseData, source = 'mini_app') {
  await ensureModerationHealth(false);
  db[uid] = Array.isArray(db[uid]) ? db[uid] : [];
  const idx = db[uid].length;
  const submissionTime = new Date().toISOString();
  const rel = {
    ...releaseData,
    status: STATUS.ON_UPLOAD,
    source,
    submission_time: submissionTime,
    username: user?.username || ''
  };

  if (SUPABASE_SYNC_ENABLED) {
    const formId = await supabaseInsertForm(uid, user, rel, FORM_STATUS.PENDING);
    if (formId) rel.supabase_form_id = formId;
  }

  let orig = fmtForm(user, uid, rel);
  let moderationText = buildModerationText(uid, { ...rel, moderation_original_text: orig });
  if (moderationText.length > MODERATION_TEXT_MAX) {
    orig = buildCompactModerationOriginal(user, uid, rel);
    moderationText = buildModerationText(uid, { ...rel, moderation_original_text: orig });
  }

  let sent;
  try {
    sent = await tg('sendMessage', moderationPayload({
      text: moderationText,
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      reply_markup: moderationKeyboard(uid, idx)
    }));
  } catch (e) {
    const msg = clean(e?.message || e);
    const retryable = msg.includes('message is too long') || msg.includes('message text is too long');
    if (retryable) {
      orig = buildCompactModerationOriginal(user, uid, rel);
      moderationText = buildModerationText(uid, { ...rel, moderation_original_text: orig });
      sent = await tg('sendMessage', moderationPayload({
        text: moderationText,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        reply_markup: moderationKeyboard(uid, idx)
      }));
    } else {
      const hint = moderationErrorHint(msg);
      console.error(
        `[MODERATION] submit failed: chat=${MOD_CHAT}` +
        ` thread=${MODERATION_THREAD_ID > 0 ? MODERATION_THREAD_ID : 'default'}` +
        ` error=${msg || 'unknown'}`
      );
      await verifyModerationChatAccess(true);
      await supabasePatchFormByRelease(uid, rel, {
        status: FORM_STATUS.PENDING,
        form_payload: safeJson(rel) || {}
      });
      throw new Error(hint ? `${msg}. ${hint}` : msg);
    }
  }

  rel.moderation_message_id = sent.message_id;
  rel.moderation_original_text = orig;
  db[uid].push(rel);
  saveDb();

  await supabasePatchFormByRelease(uid, rel, {
    status: FORM_STATUS.ON_MODERATION,
    moderation_message_id: Number(sent.message_id || 0) || null,
    form_payload: safeJson(rel) || {}
  });

  console.info(
    `[WEBAPP] release accepted to moderation: user_id=${uid} idx=${idx} msg_id=${Number(sent.message_id || 0)}`
  );

  ensureModDbShape();
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
    await sendText(chatId, '🧾 <b>Анкета релиза</b>\n\nВыберите тип релиза:', {
      parse_mode: 'HTML',
      reply_markup: {
        inline_keyboard: [
          [
            { text: '🎵 Сингл', callback_data: 'form_type_single' },
            { text: '📀 Альбом', callback_data: 'form_type_album' }
          ],
          [{ text: '✖️ Отмена', callback_data: 'form_cancel' }]
        ]
      }
    });
    return;
  }
  if (s.step === 'name') { await sendText(chatId, '🎵 Название релиза:'); return; }
  if (s.step === 'subname') { await sendText(chatId, '✨ Саб-название (если нет, отправьте точку "."):'); return; }
  if (s.step === 'has_lyrics') {
    await sendText(chatId, '🗣 Есть слова в релизе?', {
      reply_markup: {
        inline_keyboard: [
          [
            { text: '✅ Да', callback_data: 'form_lyrics_yes' },
            { text: '🎼 Нет, инструментал', callback_data: 'form_lyrics_no' }
          ],
          [{ text: '✖️ Отмена', callback_data: 'form_cancel' }]
        ]
      }
    });
    return;
  }
  if (s.step === 'nick') { await sendText(chatId, '🎤 Ник исполнителя:'); return; }
  if (s.step === 'fio') { await sendText(chatId, '🪪 ФИО исполнителя:'); return; }
  if (s.step === 'date') { await sendText(chatId, '📅 Дата релиза в формате ДД.ММ.ГГГГ:'); return; }
  if (s.step === 'version') { await sendText(chatId, '🧩 Версия релиза (или "Оригинал"):'); return; }
  if (s.step === 'genre') { await sendText(chatId, '🎚 Жанр:'); return; }
  if (s.step === 'link') { await sendText(chatId, '🔗 Ссылка на файлы (http/https):'); return; }
  if (s.step === 'yandex') {
    await sendText(chatId, '🟡 Ссылка на карточку артиста в Яндекс Музыке.\nОтправьте URL, точку "." или нажмите кнопку ниже.', {
      reply_markup: {
        inline_keyboard: [
          [{ text: '➕ Создать новую карточку', callback_data: 'form_yandex_new_card' }],
          [{ text: '✖️ Отмена', callback_data: 'form_cancel' }]
        ]
      }
    });
    return;
  }
  if (s.step === 'mat') {
    await sendText(chatId, '⚠️ Есть ненормативная лексика?', {
      reply_markup: {
        inline_keyboard: [
          [
            { text: '✅ Да', callback_data: 'form_mat_yes' },
            { text: '👌 Нет', callback_data: 'form_mat_no' }
          ],
          [{ text: '✖️ Отмена', callback_data: 'form_cancel' }]
        ]
      }
    });
    return;
  }
  if (s.step === 'promo') { await sendText(chatId, '📢 Промо-текст (или точка "."):'); return; }
  if (s.step === 'comment') { await sendText(chatId, '💬 Комментарий (или точка "."):'); return; }
  if (s.step === 'tracklist') { await sendText(chatId, '📋 Tracklist для альбома (обязательно):'); return; }
  if (s.step === 'tg') { await sendText(chatId, '📱 Контакт Telegram для связи (@username):'); return; }
  if (s.step === 'confirm') {
    const preview = fmtForm(s.user, s.uid, s.form);
    await sendText(chatId, `🧾 <b>Проверьте анкету</b>\n\n${preview}`, {
      parse_mode: 'HTML',
      reply_markup: {
        inline_keyboard: [
          [{ text: '✅ Отправить в модерацию', callback_data: 'form_send' }],
          [{ text: '✖️ Отмена', callback_data: 'form_cancel' }]
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
  await sendText(chatId, '🧾 Анкета релиза запущена.\n\nЗаполняйте шаги по очереди.\nДля отмены в любой момент: /cancel');
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
    await sendText(chatId, '✖️ Анкета отменена.');
    return true;
  }

  if (s.step === 'type') {
    const t = normalizeType(text);
    if (!t) {
      await sendText(chatId, '⚠️ Выберите тип кнопками или введите: сингл / альбом.');
      return true;
    }
    s.form.type = t;
    s.step = 'name';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'name') {
    if (!text) { await sendText(chatId, '⚠️ Название не может быть пустым.'); return true; }
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
      await sendText(chatId, '⚠️ Ответьте "Да" или "Нет".');
      return true;
    }
    s.form.has_lyrics = yn === 'yes' ? 'Да' : 'Нет, это инструментал';
    s.step = 'nick';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'nick') {
    if (!text) { await sendText(chatId, '⚠️ Ник обязателен.'); return true; }
    s.form.nick = text;
    s.step = 'fio';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'fio') {
    if (!text) { await sendText(chatId, '⚠️ ФИО обязательно.'); return true; }
    s.form.fio = text;
    s.step = 'date';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'date') {
    if (!parseRuDate(text)) { await sendText(chatId, '⚠️ Неверный формат. Используйте ДД.ММ.ГГГГ.'); return true; }
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
    if (!text) { await sendText(chatId, '⚠️ Жанр обязателен.'); return true; }
    s.form.genre = text;
    s.step = 'link';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'link') {
    if (!isHttpUrl(text)) { await sendText(chatId, '⚠️ Нужен валидный URL (http/https).'); return true; }
    s.form.link = text;
    s.step = 'yandex';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'yandex') {
    const low = text.toLowerCase();
    if (text === '.' || text === '-') {
      s.form.yandex = '.';
    } else if (low.includes('создать') && low.includes('карточ')) {
      s.form.yandex = 'create_new_card';
    } else {
      if (!isHttpUrl(text)) {
        await sendText(chatId, '⚠️ Введите URL, точку "." или нажмите кнопку «Создать новую карточку».');
        return true;
      }
      s.form.yandex = text;
    }
    s.step = 'mat';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'mat') {
    const yn = parseYesNoText(text);
    if (!yn) { await sendText(chatId, '⚠️ Ответьте "Да" или "Нет".'); return true; }
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
    if (!text || text === '.') { await sendText(chatId, '⚠️ Для альбома tracklist обязателен.'); return true; }
    s.form.tracklist = text;
    s.step = 'tg';
    await sendFormStep(chatId, uid);
    return true;
  }
  if (s.step === 'tg') {
    if (!text) { await sendText(chatId, '⚠️ Контакт Telegram обязателен.'); return true; }
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
    await sendText(query.message.chat.id, '⚠️ Анкета не активна. Запустите её заново через «Загрузить релиз».');
    return true;
  }

  if (data === 'form_cancel') {
    resetFormSession(uid);
    await sendText(query.message.chat.id, '✖️ Анкета отменена.');
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

  if (data === 'form_yandex_new_card') {
    s.form.yandex = 'create_new_card';
    s.step = 'mat';
    await sendFormStep(query.message.chat.id, uid);
    return true;
  }

  if (data === 'form_send') {
    const vr = validateForm(s.form, { source: 'bot_text', telegram_id: uid });
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
      await sendText(
        query.message.chat.id,
        `❌ Не удалось отправить анкету в модерацию.\nПричина: <code>${esc(shortTgError(e))}</code>`
      );
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
      const sent = await tg('sendPhoto', moderationPayload({
        photo: msg.photo[msg.photo.length - 1].file_id,
        caption,
        parse_mode: 'HTML'
      }));
      try { await tg('pinChatMessage', { chat_id: MOD_CHAT, message_id: sent.message_id }); } catch {}
      await sendText(chatId, '✅ Заказ обложки отправлен в модерацию.');
      resetCoverSession(uid);
    } catch (e) {
      const msgErr = clean(e?.message || e);
      console.error('[COVER] submit failed:', msgErr || e);
      await verifyModerationChatAccess(true);
      const hint = moderationErrorHint(msgErr);
      await sendText(chatId, `❌ Не удалось отправить заказ.${hint ? `\n${esc(hint)}` : '\nПопробуйте ещё раз.'}`);
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

function getUserReleaseEntries(uid, includeDeleted = false) {
  const list = Array.isArray(db?.[uid]) ? db[uid] : [];
  const out = [];
  for (let idx = 0; idx < list.length; idx += 1) {
    const rel = list[idx];
    if (!rel || typeof rel !== 'object') continue;
    if (!includeDeleted && rel?.user_deleted) continue;
    out.push({ idx, rel });
  }
  return out;
}
function shortTitle(name, max = 22) {
  const src = clean(name) || 'Без названия';
  return src.length > max ? `${src.slice(0, max - 1)}…` : src;
}
function getCabinetOrderedEntries(uid) {
  const entries = getUserReleaseEntries(uid, false);
  entries.sort((a, b) => {
    const ta = parseIsoTime(a.rel?.submission_time || a.rel?.moderation_time || '');
    const tb = parseIsoTime(b.rel?.submission_time || b.rel?.moderation_time || '');
    if (tb !== ta) return tb - ta;
    return b.idx - a.idx;
  });
  return entries;
}

function buildMyCabinetView(uid, requestedPage = 0) {
  const entries = getCabinetOrderedEntries(uid);
  if (!entries.length) {
    return {
      text:
        '🎧 <b>МОЙ КАБИНЕТ</b>\n\n' +
        'Пока у вас нет релизов.\n' +
        'Нажмите «➕ Новый», чтобы отправить первую анкету.',
      keyboard: {
        inline_keyboard: [
          [
            { text: '➕ Новый', callback_data: 'report_text' },
            { text: '◀️ Меню', callback_data: 'main' }
          ],
          [{ text: '🔄 Обновить', callback_data: 'my_back' }]
        ]
      },
      page: 0,
      total: 0
    };
  }

  const total = entries.length;
  const page = Math.min(Math.max(Number(requestedPage) || 0, 0), total - 1);
  const current = entries[page];
  const rel = current.rel;
  const status = canonicalStatus(rel?.status);
  const upcText = clean(rel?.upc) ? `<code>${esc(rel.upc)}</code>` : '—';

  const counters = {
    [STATUS.ON_UPLOAD]: 0,
    [STATUS.MODERATION]: 0,
    [STATUS.APPROVED]: 0,
    [STATUS.REJECTED]: 0,
    [STATUS.NEEDS_FIX]: 0,
    [STATUS.PUBLISHED]: 0,
    [STATUS.DELETED]: 0
  };
  for (const { rel: item } of entries) {
    const st = canonicalStatus(item?.status);
    counters[st] = (counters[st] || 0) + 1;
  }
  const approvedPct = total ? ((counters[STATUS.APPROVED] + counters[STATUS.PUBLISHED]) * 100 / total) : 0;

  let text =
    `🎧 <b>МОЙ КАБИНЕТ</b> • <b>${total}</b> релизов\n` +
    `✅ Одобрено: <b>${counters[STATUS.APPROVED]}</b> (<b>${approvedPct.toFixed(0)}%</b>)\n` +
    `⏳ На отгрузке: <b>${counters[STATUS.ON_UPLOAD]}</b>\n` +
    `🧠 На модерации: <b>${counters[STATUS.MODERATION]}</b>\n` +
    `⚠️ На правках: <b>${counters[STATUS.NEEDS_FIX]}</b>\n` +
    `❌ Отклонено: <b>${counters[STATUS.REJECTED]}</b>\n\n` +
    `🎵 <b>${esc(rel?.name || 'Без названия')}</b>\n` +
    `📝 Тип: <i>${esc(clean(rel?.type) || 'сингл')}</i>\n` +
    `📅 Дата: <i>${esc(rel?.date || '—')}</i>\n` +
    `👤 Артист: <i>${esc(rel?.nick || '—')}</i>\n` +
    `🏷 Жанр: <i>${esc(rel?.genre || '—')}</i>\n` +
    `📦 UPC: ${upcText}\n\n` +
    `📊 Статус: ${statusEmoji(status)} <b>${esc(statusText(status))}</b>\n`;

  if (status === STATUS.REJECTED && clean(rel?.reject_reason)) {
    text += `❌ Причина: ${esc(rel.reject_reason)}\n`;
  }

  text += `\n🗂 Карточка <b>${page + 1}</b> из <b>${total}</b>`;

  const prevPage = page > 0 ? page - 1 : total - 1;
  const nextPage = page < total - 1 ? page + 1 : 0;
  const rows = [];

  if (total > 1) {
    rows.push([
      { text: `(${page + 1}/${total})`, callback_data: 'my_page_info' },
      { text: '⬅️ Назад', callback_data: `my_page_${uid}_${prevPage}` },
      { text: 'Далее ➡️', callback_data: `my_page_${uid}_${nextPage}` }
    ]);
  } else {
    rows.push([{ text: '(1/1)', callback_data: 'my_page_info' }]);
  }

  rows.push([
    { text: '📄 Детали', callback_data: `release_details_${uid}_${current.idx}_${page}` },
    { text: '🗑 Удалить', callback_data: `delete_release_${uid}_${current.idx}_${page}` }
  ]);
  rows.push([
    { text: '➕ Новый', callback_data: 'report_text' },
    { text: '◀️ Меню', callback_data: 'main' }
  ]);

  return { text, keyboard: { inline_keyboard: rows }, page, total };
}
function buildReleaseDetailsText(uid, idx, rel) {
  const status = canonicalStatus(rel?.status);
  const yandexRaw = clean(rel?.yandex);
  const yandexText = yandexRaw === 'create_new_card' ? 'Создать новую карточку' : (yandexRaw || '.');
  const lines = [
    '📝 <b>Информация о релизе</b>',
    '',
    `🎵 <b>${esc(rel?.name || 'Без названия')}</b>`,
    `📊 Статус: ${statusEmoji(status)} ${esc(statusText(status))}`,
    '',
    '📋 <b>Основное</b>',
    `Тип: <i>${esc(rel?.type || '—')}</i>`,
    `Жанр: <i>${esc(rel?.genre || '—')}</i>`,
    `Дата релиза: <i>${esc(rel?.date || '—')}</i>`,
    `Версия: <i>${esc(rel?.version || '—')}</i>`,
    '',
    '👤 <b>Артист</b>',
    `Ник: <i>${esc(rel?.nick || '—')}</i>`,
    `ФИО: <i>${esc(rel?.fio || '—')}</i>`,
    '',
    '🔗 <b>Ссылки</b>',
    `Файлы: <i>${esc(rel?.link || '—')}</i>`,
    `Яндекс: <i>${esc(yandexText)}</i>`,
    `Telegram: <i>${esc(rel?.tg || '—')}</i>`
  ];
  if (clean(rel?.upc) && clean(rel?.upc) !== '.') lines.push(`\n📦 UPC: <code>${esc(rel.upc)}</code>`);
  if (clean(rel?.isrc) && clean(rel?.isrc) !== '.') lines.push(`🔢 ISRC: <code>${esc(rel.isrc)}</code>`);
  if (status === STATUS.REJECTED && clean(rel?.reject_reason)) {
    lines.push('', `❌ <b>Причина отказа:</b> ${esc(rel.reject_reason)}`);
  }
  if (clean(rel?.moderator_comment)) {
    lines.push('', `💬 <b>Комментарий модератора:</b> ${esc(rel.moderator_comment)}`);
  }
  lines.push('', `🕒 Отправлено: <i>${esc(clean(rel?.submission_time).slice(0, 19) || '—')}</i>`);
  if (clean(rel?.moderation_time)) lines.push(`🕒 Обновлено: <i>${esc(clean(rel.moderation_time).slice(0, 19))}</i>`);
  lines.push(`ID: <code>${esc(uid)}</code> • #${idx}`);
  return lines.join('\n');
}
async function sendMy(chatId, uid) {
  const view = buildMyCabinetView(uid);
  await sendText(chatId, view.text, { reply_markup: view.keyboard });
}

function isAdmin(userId) {
  return ADMIN_IDS.includes(Number(userId));
}

function parseIsoTime(value) {
  const ts = Date.parse(clean(value));
  return Number.isFinite(ts) ? ts : 0;
}

function buildPeriodMeta(period) {
  const now = Date.now();
  if (period === 'week') {
    return { key: 'week', title: 'Последние 7 дней', cutoff: now - (7 * 24 * 60 * 60 * 1000) };
  }
  if (period === 'month') {
    return { key: 'month', title: 'Последние 30 дней', cutoff: now - (30 * 24 * 60 * 60 * 1000) };
  }
  return { key: 'all', title: 'Всё время', cutoff: 0 };
}

function collectStats(cutoff = 0) {
  const stats = {
    users_total: 0,
    users_active: 0,
    total: 0,
    on_upload: 0,
    moderation: 0,
    approved: 0,
    rejected: 0,
    needs_fix: 0,
    deleted: 0,
    published: 0,
    type_single: 0,
    type_album: 0,
    reject_reasons: new Map(),
    top_artists: new Map()
  };
  const users = Object.entries(db || {});
  stats.users_total = users.length;
  for (const [uid, rels] of users) {
    const list = Array.isArray(rels) ? rels : [];
    if (list.length > 0) stats.users_active += 1;
    for (const rel of list) {
      if (!rel || typeof rel !== 'object') continue;
      const submissionTs = parseIsoTime(rel?.submission_time);
      if (cutoff && submissionTs && submissionTs < cutoff) continue;

      stats.total += 1;
      const st = canonicalStatus(rel?.status);
      if (st === STATUS.ON_UPLOAD) stats.on_upload += 1;
      else if (st === STATUS.MODERATION) stats.moderation += 1;
      else if (st === STATUS.APPROVED) stats.approved += 1;
      else if (st === STATUS.REJECTED) stats.rejected += 1;
      else if (st === STATUS.NEEDS_FIX) stats.needs_fix += 1;
      else if (st === STATUS.DELETED) stats.deleted += 1;
      else if (st === STATUS.PUBLISHED) stats.published += 1;

      const tp = clean(rel?.type).toLowerCase();
      if (tp === 'альбом' || tp === 'album') stats.type_album += 1;
      else stats.type_single += 1;

      const reason = clean(rel?.reject_reason);
      if (reason) stats.reject_reasons.set(reason, (stats.reject_reasons.get(reason) || 0) + 1);

      const artist = clean(rel?.nick || rel?.username || uid) || uid;
      stats.top_artists.set(artist, (stats.top_artists.get(artist) || 0) + 1);
    }
  }
  return stats;
}

function sortedTopFromMap(map, limit = 3) {
  return Array.from(map.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit);
}

function adminPanelKeyboard() {
  return {
    inline_keyboard: [
      [
        { text: '🎁 Бэкап БД', callback_data: 'admin_backup' },
        { text: '🗂 Архив мод.', callback_data: 'admin_moderation_backup' }
      ],
      [
        { text: '📊 Статистика', callback_data: 'admin_stats' },
        { text: '⏳ Ожидают', callback_data: 'admin_pending' }
      ],
      [
        { text: '🔄 Очистка', callback_data: 'admin_cleanup' },
        { text: '📢 Рассылка', callback_data: 'admin_broadcast' }
      ],
      [
        { text: '📋 Все релизы', callback_data: 'admin_all_releases' },
        { text: '⚠️ УДАЛИТЬ ВСЁ', callback_data: 'admin_cleanbase_confirm' }
      ],
      [{ text: '🔁 Обновить', callback_data: 'admin_back' }]
    ]
  };
}

function statsPeriodKeyboard(showBackToAdmin = true) {
  const rows = [
    [{ text: '📅 Неделя', callback_data: 'stats_period_week' }],
    [{ text: '📅 Месяц', callback_data: 'stats_period_month' }],
    [{ text: '📅 Всё время', callback_data: 'stats_period_all' }]
  ];
  if (showBackToAdmin) rows.push([{ text: '🔙 В админ', callback_data: 'admin_back' }]);
  return { inline_keyboard: rows };
}

function buildAdminPanelText() {
  const all = collectStats(0);
  const week = collectStats(buildPeriodMeta('week').cutoff);
  const awaiting = all.on_upload + all.moderation + all.needs_fix;
  const supabaseInfo = SUPABASE_SYNC_ENABLED
    ? `🟢 Supabase: подключен (${esc(SUPABASE_RELEASES_TABLE)})`
    : '🟡 Supabase: не настроен';
  return (
    '❄️ <b>АДМИН-ПАНЕЛЬ</b> ❄️\n\n' +
    '📊 <b>ОБЩАЯ СТАТИСТИКА:</b>\n' +
    `👥 Пользователей: <b>${all.users_total}</b>\n` +
    `🎧 Активных: <b>${all.users_active}</b>\n` +
    `📦 Всего релизов: <b>${all.total}</b>\n` +
    `⏳ Ожидает: <b>${awaiting}</b>\n` +
    `✅ Одобрено: <b>${all.approved}</b>\n` +
    `❌ Отклонено: <b>${all.rejected}</b>\n` +
    `📢 Опубликовано: <b>${all.published}</b>\n` +
    `📅 За 7 дней: <b>${week.total}</b>\n\n` +
    '⚙️ <b>УПРАВЛЕНИЕ:</b>\n' +
    '/backup — 📦 База релизов\n' +
    '/moderation_backup — 🗂 Архив модерации\n' +
    '/new — 🆕 Отметить «На отгрузке»\n' +
    '/modtest — 🧪 Проверка отправки в модерацию\n' +
    '/stats /statss — 📊 Подробная статистика\n' +
    '/broadcast — 📢 Рассылка пользователям\n' +
    '/cleanup — 🧹 Очистка служебных данных\n' +
    '/cleanbase — ⚠️ УДАЛИТЬ ВСЕ РЕЛИЗЫ\n\n' +
    `${supabaseInfo}`
  );
}

function buildPeriodStatsText(period) {
  const meta = buildPeriodMeta(period);
  const stats = collectStats(meta.cutoff);
  const approvedPct = stats.total ? ((stats.approved * 100) / stats.total) : 0;
  const topReasons = sortedTopFromMap(stats.reject_reasons, 3);
  const topArtists = sortedTopFromMap(stats.top_artists, 3);

  let text = (
    `📊 <b>СТАТИСТИКА</b> (${esc(meta.title)})\n\n` +
    `📦 Всего анкет: <b>${stats.total}</b>\n` +
    `✅ Принято: <b>${stats.approved}</b> (${approvedPct.toFixed(1)}%)\n` +
    `❌ Отклонено: <b>${stats.rejected}</b>\n` +
    `🧠 На модерации: <b>${stats.moderation}</b>\n` +
    `🕓 На отгрузке: <b>${stats.on_upload}</b>\n\n` +
    '❌ <b>Топ 3 причины отказа:</b>\n'
  );

  if (topReasons.length) {
    topReasons.forEach((item, i) => {
      text += `${i + 1}. ${esc(item[0])} — ${item[1]}\n`;
    });
  } else {
    text += 'Нет данных\n';
  }

  text += '\n🔥 <b>Топ 3 артиста:</b>\n';
  if (topArtists.length) {
    topArtists.forEach((item, i) => {
      text += `${i + 1}. ${esc(item[0])} — ${item[1]}\n`;
    });
  } else {
    text += 'Нет данных\n';
  }
  return text;
}

function buildReleaseRow(uid, idx, rel) {
  const st = canonicalStatus(rel?.status);
  return `${statusEmoji(st)} ${esc(shortTitle(rel?.name || 'Без названия', 42))} — ${esc(rel?.nick || '—')} (<code>${esc(uid)}</code>#${idx})`;
}

function collectReleasesFlat() {
  const out = [];
  for (const [uid, listRaw] of Object.entries(db || {})) {
    const list = Array.isArray(listRaw) ? listRaw : [];
    for (let idx = 0; idx < list.length; idx += 1) {
      const rel = list[idx];
      if (!rel || typeof rel !== 'object') continue;
      out.push({ uid, idx, rel });
    }
  }
  out.sort((a, b) => parseIsoTime(b.rel?.submission_time) - parseIsoTime(a.rel?.submission_time));
  return out;
}

function buildPendingText(limit = 20) {
  const waitingStatuses = new Set([STATUS.ON_UPLOAD, STATUS.MODERATION, STATUS.NEEDS_FIX]);
  const items = collectReleasesFlat().filter(({ rel }) => waitingStatuses.has(canonicalStatus(rel?.status)));
  let text = `⏳ <b>ОЖИДАЮЩИЕ РЕЛИЗЫ</b>\n\nВсего: <b>${items.length}</b>\n\n`;
  if (!items.length) return `${text}Сейчас ожидающих релизов нет.`;
  const view = items.slice(0, limit);
  for (let i = 0; i < view.length; i += 1) {
    const { uid, idx, rel } = view[i];
    text += `${i + 1}. ${buildReleaseRow(uid, idx, rel)}\n`;
  }
  if (items.length > view.length) {
    text += `\n… и ещё ${items.length - view.length}`;
  }
  return text;
}

function buildAllReleasesText(limit = 30) {
  const items = collectReleasesFlat();
  let text = `📋 <b>ВСЕ РЕЛИЗЫ</b>\n\nВсего: <b>${items.length}</b>\n\n`;
  if (!items.length) return `${text}Список пуст.`;
  const view = items.slice(0, limit);
  for (let i = 0; i < view.length; i += 1) {
    const { uid, idx, rel } = view[i];
    text += `${i + 1}. ${buildReleaseRow(uid, idx, rel)}\n`;
  }
  if (items.length > view.length) {
    text += `\n… и ещё ${items.length - view.length}`;
  }
  return text;
}

function getNowStamp() {
  const d = new Date();
  const y = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const hh = String(d.getHours()).padStart(2, '0');
  const mi = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return `${y}${mm}${dd}_${hh}${mi}${ss}`;
}

async function makeBackupAndSend(chatId, kind) {
  const stamp = getNowStamp();
  if (kind === 'releases') {
    const file = path.resolve(ROOT, `releases_backup_${stamp}.json`);
    saveJson(file, db);
    await sendDocument(chatId, file, '📦 Бэкап базы релизов');
    return file;
  }
  const file = path.resolve(ROOT, `moderation_backup_${stamp}.json`);
  saveJson(file, modDb);
  await sendDocument(chatId, file, '🗂 Бэкап архива модерации');
  return file;
}

function runServiceCleanup() {
  const summary = {
    pendingActionsBefore: Array.isArray(modDb?.pending_actions) ? modDb.pending_actions.length : 0,
    pendingActionsAfter: 0,
    moderationMirrorBefore: Array.isArray(modDb?.moderation_messages) ? modDb.moderation_messages.length : 0,
    moderationMirrorAfter: 0
  };
  cleanupPendingActions();
  ensureModDbShape();
  summary.pendingActionsAfter = modDb.pending_actions.length;

  const keepAfter = Date.now() - (Math.max(30, CLEANUP_KEEP_DAYS) * 24 * 60 * 60 * 1000);
  const out = [];
  const seen = new Set();
  for (const row of modDb.moderation_messages) {
    if (!row || typeof row !== 'object') continue;
    const key = `${clean(row.user_id)}:${Number(row.idx || 0)}:${Number(row.message_id || row.moderation_message_id || 0)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const ts = parseIsoTime(row.submission_time || row.moderation_time || '');
    if (ts && ts < keepAfter && row.user_deleted) {
      continue;
    }
    out.push(row);
  }
  modDb.moderation_messages = out;
  summary.moderationMirrorAfter = out.length;
  saveModDb();
  return summary;
}

function setBroadcastSession(uid, active) {
  const key = String(uid);
  if (active) {
    broadcastSessions[key] = { created_at: new Date().toISOString() };
  } else {
    delete broadcastSessions[key];
  }
}

function hasBroadcastSession(uid) {
  return !!broadcastSessions[String(uid)];
}

function collectBroadcastUsers() {
  const ids = new Set();
  for (const uid of Object.keys(db || {})) ids.add(String(uid));
  for (const uid of Object.keys(cabUsers || {})) ids.add(String(uid));
  return Array.from(ids)
    .map((v) => Number(v))
    .filter((v) => Number.isFinite(v) && v > 0);
}

async function runBroadcast(text) {
  const users = collectBroadcastUsers();
  let sent = 0;
  let failed = 0;
  for (const uid of users) {
    try {
      await sendTextPlain(uid, text);
      sent += 1;
    } catch {
      failed += 1;
    }
    await new Promise((r) => setTimeout(r, 22));
  }
  return { total: users.length, sent, failed };
}

async function runCleanbase() {
  db = {};
  modDb = { moderation_messages: [], pending_actions: [] };
  saveDb();
  saveModDb();
  if (SUPABASE_SYNC_ENABLED) {
    try {
      await supabaseDeleteAllReleases();
      await syncSupabaseNow('cleanbase');
    } catch (e) {
      console.error('[cleanbase] supabase cleanup failed:', clean(e?.message || e));
    }
  }
}

async function sendAdminPanel(chatId) {
  await sendText(chatId, buildAdminPanelText(), { reply_markup: adminPanelKeyboard() });
}

async function sendStatsPicker(chatId) {
  await sendText(chatId, '📊 Выберите период для статистики:', { reply_markup: statsPeriodKeyboard(true) });
}

async function verifyModerationChatAccess(force = false) {
  const now = Date.now();
  if (!force && moderationHealth.checked_at && (now - moderationHealth.checked_at) < MODERATION_HEALTH_TTL_MS) {
    return moderationHealth.ok === true;
  }
  moderationHealth.checked_at = now;
  try {
    const [chat, me] = await Promise.all([
      tg('getChat', { chat_id: MOD_CHAT }),
      tg('getMe', {})
    ]);
    const title = clean(chat?.title || chat?.username || '') || String(MOD_CHAT);
    moderationHealth.chat_title = title;
    moderationHealth.bot_id = String(me?.id || '');
    moderationHealth.bot_username = clean(me?.username || '');

    let member = null;
    try {
      member = await tg('getChatMember', { chat_id: MOD_CHAT, user_id: Number(me?.id || 0) });
    } catch (memberErr) {
      moderationHealth.ok = false;
      moderationHealth.reason = `не удалось получить статус бота в группе: ${shortTgError(memberErr)}`;
      moderationHealth.bot_status = '';
      moderationHealth.can_send_messages = null;
      console.error(`[bot] moderation chat access failed: ${moderationHealth.reason}`);
      return false;
    }

    const desc = describeBotMemberStatus(member);
    moderationHealth.ok = !!desc.ok;
    moderationHealth.reason = desc.reason || '';
    moderationHealth.bot_status = desc.status || '';
    moderationHealth.can_send_messages = desc.can_send_messages;

    if (moderationHealth.ok) {
      console.info(
        `[bot] moderation chat access: ok (${title})` +
        ` status=${desc.status || '-'} thread=${MODERATION_THREAD_ID > 0 ? MODERATION_THREAD_ID : 'default'}`
      );
      return true;
    }

    console.error(
      `[bot] moderation chat access failed: ${moderationHealth.reason || 'unknown reason'}` +
      ` (${title}) status=${desc.status || '-'}`
    );
    return false;
  } catch (e) {
    moderationHealth.ok = false;
    moderationHealth.reason = shortTgError(e);
    moderationHealth.bot_status = '';
    moderationHealth.can_send_messages = null;
    console.error(`[bot] moderation chat access failed: ${moderationHealth.reason}`);
    return false;
  }
}

async function processWebAppData(msg) {
  const raw = msg.web_app_data?.data || '';
  const rawBytes = Buffer.byteLength(raw, 'utf8');
  if (!raw || rawBytes > WEBAPP_MAX_PAYLOAD_BYTES) {
    await sendText(msg.chat.id, '❌ Payload Mini App слишком большой или пустой.');
    return;
  }
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
  console.info(`[WEBAPP] action=${action || '-'} user_id=${uid || '-'} bytes=${rawBytes}`);

  if (!uid) {
    await sendText(msg.chat.id, '❌ Не удалось определить Telegram ID отправителя.');
    return;
  }

  const envelopeTgId = clean(parsed.telegram_id || '');
  if (envelopeTgId && envelopeTgId !== uid) {
    console.error(`[WEBAPP] blocked spoof attempt: payload_tg=${envelopeTgId} sender_tg=${uid}`);
    await sendText(msg.chat.id, '❌ Ошибка безопасности: telegram_id в payload не совпадает с отправителем.');
    return;
  }

  const shouldValidateInitData = ['webapp_release_submit', 'submit_release', 'cabinet_activate', 'cabinet_sync_request'].includes(action);
  const initData = clean(parsed.init_data || '');
  if (shouldValidateInitData && !initData) {
    if (WEBAPP_REQUIRE_INITDATA) {
      await sendText(msg.chat.id, '❌ Ошибка безопасности: Mini App initData не передан.');
      return;
    }
    console.warn(`[WEBAPP] initData missing: user_id=${uid} action=${action || '-'} (fallback mode)`);
  }

  if (initData) {
    const initDataCheck = verifyTelegramInitData(initData, uid);
    if (!initDataCheck.ok) {
      if (WEBAPP_REQUIRE_INITDATA) {
        console.error(`[WEBAPP] initData validation failed: user_id=${uid} reason=${initDataCheck.reason}`);
        await sendText(msg.chat.id, `❌ Ошибка проверки Mini App: ${esc(initDataCheck.reason)}.`);
        return;
      }
      console.warn(
        `[WEBAPP] initData validation warning: user_id=${uid} reason=${initDataCheck.reason} (fallback mode)`
      );
    }
  }

  if (action === 'cabinet_activate') {
    cabUsers[uid] = {
      approved: true,
      activated_at: new Date().toISOString(),
      username: user?.username || '',
      first_name: user?.first_name || ''
    };
    saveCab();
    await supabaseUpsertCabinetUser(uid, user, true);
    console.info(`[WEBAPP] cabinet activated: user_id=${uid}`);
    await sendText(msg.chat.id, '✅ <b>Личный кабинет активирован</b>');
    return;
  }

  if (action === 'cabinet_sync_request') {
    await supabaseUpsertCabinetUser(uid, user, true);
    console.info(`[WEBAPP] cabinet sync requested: user_id=${uid}`);
    return;
  }

  if (!['webapp_release_submit', 'submit_release', ''].includes(action)) {
    await sendText(msg.chat.id, '✅ Данные Mini App получены.');
    return;
  }

  const antiSpam = verifyWebappAntiSpam(uid, raw);
  if (!antiSpam.ok) {
    await sendText(msg.chat.id, '⚠️ Анкета уже была отправлена только что. Подождите несколько секунд и попробуйте снова.');
    return;
  }

  if (!parsed.form || typeof parsed.form !== 'object') {
    await sendText(msg.chat.id, '❌ Ошибка данных формы. Отправьте анкету еще раз.');
    return;
  }

  const vr = validateForm(parsed.form, parsed);
  if (vr.errors.length) {
    const list = vr.errors.slice(0, 8).map((e) => `• ${esc(e)}`).join('\n');
    await sendText(msg.chat.id, `❌ <b>Анкета Mini App не отправлена</b>\n\n${list}`);
    return;
  }

  try {
    const out = await submitReleaseToModeration(user, uid, vr.data, 'mini_app');
    await supabaseUpsertCabinetUser(uid, user, true);
    console.info(
      `[WEBAPP] release stored: user_id=${uid} idx=${out?.idx ?? '-'} name=${sanitizeText(vr.data?.name || '', 80)}`
    );
    await sendText(
      msg.chat.id,
      '✅ <b>Анкета отправлена в модерацию</b>\n\n' +
      `🎵 ${esc(vr.data?.name || 'Релиз')}\n` +
      `👤 ${esc(vr.data?.nick || '—')}`
    );
  } catch (e) {
    console.error('[WEBAPP] submit failed:', e.message || e);
    await sendText(
      msg.chat.id,
      `❌ Не удалось отправить анкету в модерацию.\nПричина: <code>${esc(shortTgError(e))}</code>`
    );
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
  const ref = resolveReleaseRef(uid, idx, Number(query?.message?.message_id || 0));
  if (!ref || !ref.rel) {
    await sendText(query.message.chat.id, 'Релиз не найден.');
    return;
  }
  uid = ref.uid;
  idx = ref.idx;
  const map = {
    upload: STATUS.ON_UPLOAD,
    moderate: STATUS.MODERATION,
    approve: STATUS.APPROVED,
    needfix: STATUS.NEEDS_FIX,
    delete: STATUS.DELETED
  };
  const st = map[action];
  if (!st) {
    await sendText(query.message.chat.id, 'Неизвестное действие модерации.');
    return;
  }
  const out = await applyReleaseStatus(uid, idx, st, query.from, {
    fallbackMessageId: Number(query.message.message_id || 0),
    clearRejectReason: st !== STATUS.REJECTED
  });
  if (!out.ok) {
    await sendText(query.message.chat.id, `❌ ${out.error}`);
    return;
  }
  await sendText(query.message.chat.id, `Статус обновлен: ${statusText(st)}`);
}

function collectOnUploadReleaseRefs() {
  const out = [];
  for (const [uidRaw, listRaw] of Object.entries(db || {})) {
    const uid = String(uidRaw);
    const list = Array.isArray(listRaw) ? listRaw : [];
    for (let idx = 0; idx < list.length; idx += 1) {
      const rel = list[idx];
      if (!rel || typeof rel !== 'object') continue;
      if (canonicalStatus(rel.status) !== STATUS.ON_UPLOAD) continue;
      if (rel.user_deleted) continue;
      out.push({ uid, idx, rel });
    }
  }
  out.sort((a, b) => {
    const ta = parseIsoTime(a.rel?.submission_time || a.rel?.moderation_time || '');
    const tb = parseIsoTime(b.rel?.submission_time || b.rel?.moderation_time || '');
    return tb - ta;
  });
  return out;
}

async function runNewModerationSweep(moderator) {
  const refs = collectOnUploadReleaseRefs();
  if (!refs.length) return { total: 0, marked: 0, refreshed: 0, missingMessage: 0 };

  const nowIso = new Date().toISOString();
  const nowTs = Date.now();
  let marked = 0;
  for (const item of refs) {
    const rel = item.rel;
    rel.status = STATUS.ON_UPLOAD;
    rel.available_for_upload = true;
    rel.available_marked_at = nowIso;
    rel.available_marked_by = String(moderator?.id || '');
    rel.available_marked_by_username = clean(moderator?.username || '');
    rel.available_marked_by_name = clean([moderator?.first_name || '', moderator?.last_name || ''].filter(Boolean).join(' '));
    rel.moderation_time = nowIso;

    const events = normalizeInteractionLog(rel.interactions);
    const last = events.length ? events[events.length - 1] : null;
    const lastTs = parseIsoTime(last?.at || '');
    const sameActor = clean(last?.actor_id || '') === String(moderator?.id || '');
    const sameType = clean(last?.type || '') === 'marked_free';
    const tooSoon = sameType && sameActor && lastTs > 0 && (nowTs - lastTs) < (5 * 60 * 1000);
    if (!tooSoon) {
      pushReleaseInteraction(rel, 'marked_free', moderator, {
        status_to: STATUS.ON_UPLOAD,
        note: 'Анкета отмечена как свободная к отгрузке (/new)'
      });
    }

    syncModerationMirror(item.uid, item.idx, rel);
    await supabasePatchFormByRelease(item.uid, rel, {
      status: FORM_STATUS.PENDING,
      form_payload: safeJson(rel) || {}
    });
    marked += 1;
  }
  saveDb();
  saveModDb();

  let refreshed = 0;
  let missingMessage = 0;
  for (const item of refs) {
    const messageId = Number(item.rel?.moderation_message_id || 0);
    if (!messageId) {
      missingMessage += 1;
      continue;
    }
    const ok = await refreshModerationMessage(item.uid, item.idx, item.rel, messageId);
    if (ok) refreshed += 1;
    await new Promise((r) => setTimeout(r, 35));
  }
  return { total: refs.length, marked, refreshed, missingMessage };
}

async function onMessage(msg) {
  if (msg.web_app_data?.data) { await processWebAppData(msg); return; }
  const chatId = msg.chat?.id;
  const uid = String(msg.from?.id || '');
  const text = clean(msg.text);
  if (!chatId) return;

  if (await handleModerationReplyMessage(msg)) return;
  if (await handleCoverMessage(msg)) return;
  if (await handlePromoMessage(msg)) return;
  if (await handleFormTextMessage(msg)) return;

  if (!text) return;

  if (/^\/new(?:@\w+)?$/i.test(text)) {
    if (Number(chatId) !== Number(MOD_CHAT)) {
      await sendText(chatId, 'Команда /new работает только в группе модерации.');
      return;
    }
    if (!(await canModerate(msg.from?.id))) {
      await sendText(chatId, 'Доступ только участникам группы модерации.');
      return;
    }
    const summary = await runNewModerationSweep(msg.from);
    if (!summary.total) {
      await sendText(chatId, '🆕 Анкет со статусом «На отгрузке» сейчас нет.');
      return;
    }
    await sendText(
      chatId,
      '🆕 <b>/new выполнено</b>\n\n' +
      `⏳ На отгрузке: <b>${summary.total}</b>\n` +
      `✅ Отмечено свободными: <b>${summary.marked}</b>\n` +
      `📝 Обновлено сообщений: <b>${summary.refreshed}</b>\n` +
      `⚠️ Без moderation_message_id: <b>${summary.missingMessage}</b>`
    );
    return;
  }

  if (isAdmin(uid) && hasBroadcastSession(uid)) {
    if (text === '/cancel') {
      setBroadcastSession(uid, false);
      await sendText(chatId, '📢 Рассылка отменена.');
      return;
    }
    setBroadcastSession(uid, false);
    await sendText(chatId, '📢 Запускаю рассылку...');
    const result = await runBroadcast(text);
    await sendText(
      chatId,
      `📢 <b>Рассылка завершена</b>\n\n` +
      `Всего получателей: <b>${result.total}</b>\n` +
      `✅ Доставлено: <b>${result.sent}</b>\n` +
      `❌ Ошибок: <b>${result.failed}</b>`
    );
    return;
  }

  if (text === '/start' || text.startsWith('/start ')) {
    resetAllSessions(uid);
    await sendText(chatId, welcomeText(), { reply_markup: keyboardMain() });
    return;
  }
  if (/^\/admin(?:@\w+)?$/i.test(text)) {
    if (!isAdmin(uid)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    await sendAdminPanel(chatId);
    return;
  }
  if (/^\/modtest(?:@\w+)?$/i.test(text)) {
    if (!isAdmin(uid)) {
      await sendText(chatId, '❌ Команда доступна только администраторам.');
      return;
    }
    await ensureModerationHealth(true);
    try {
      const probe = await sendModerationText(
        `🧪 <b>Проверка модерации</b>\n` +
        `Время: <code>${esc(new Date().toISOString())}</code>\n` +
        `Проверка отправки из node_bot.js`
      );
      await sendText(
        chatId,
        `✅ Отправка в модерацию работает.\n` +
        `message_id: <code>${esc(String(probe?.message_id || '0'))}</code>\n` +
        `status бота: <b>${esc(moderationHealth.bot_status || '-')}</b>`
      );
    } catch (e) {
      const errText = shortTgError(e);
      const hint = moderationErrorHint(errText);
      await sendText(
        chatId,
        `❌ Отправка в модерацию не работает.\n` +
        `Причина: <code>${esc(errText)}</code>` +
        (hint ? `\nПодсказка: ${esc(hint)}` : '')
      );
    }
    return;
  }
  if (/^\/statss(?:@\w+)?$/i.test(text) || /^\/stats(?:@\w+)?$/i.test(text)) {
    if (!isAdmin(uid)) {
      await sendText(chatId, '❌ Команда доступна только администраторам.');
      return;
    }
    await sendStatsPicker(chatId);
    return;
  }
  if (/^\/backup(?:@\w+)?$/i.test(text)) {
    if (!isAdmin(uid)) {
      await sendText(chatId, '❌ Команда доступна только администраторам.');
      return;
    }
    const file = await makeBackupAndSend(chatId, 'releases');
    await sendText(chatId, `✅ Бэкап создан: <code>${esc(path.basename(file))}</code>`);
    return;
  }
  if (/^\/moderation_backup(?:@\w+)?$/i.test(text)) {
    if (!isAdmin(uid)) {
      await sendText(chatId, '❌ Команда доступна только администраторам.');
      return;
    }
    const file = await makeBackupAndSend(chatId, 'moderation');
    await sendText(chatId, `✅ Бэкап модерации создан: <code>${esc(path.basename(file))}</code>`);
    return;
  }
  if (/^\/cleanup(?:@\w+)?$/i.test(text)) {
    if (!isAdmin(uid)) {
      await sendText(chatId, '❌ Команда доступна только администраторам.');
      return;
    }
    const summary = runServiceCleanup();
    await sendText(
      chatId,
      '🧹 <b>Очистка завершена</b>\n\n' +
      `Pending actions: <b>${summary.pendingActionsBefore}</b> → <b>${summary.pendingActionsAfter}</b>\n` +
      `Зеркало модерации: <b>${summary.moderationMirrorBefore}</b> → <b>${summary.moderationMirrorAfter}</b>`
    );
    return;
  }
  if (/^\/cleanbase(?:@\w+)?(?:\s+confirm)?$/i.test(text)) {
    if (!isAdmin(uid)) {
      await sendText(chatId, '❌ Команда доступна только администраторам.');
      return;
    }
    if (!/\s+confirm$/i.test(text)) {
      await sendText(
        chatId,
        '⚠️ Это удалит ВСЕ релизы из базы.\n\nДля подтверждения отправьте:\n<code>/cleanbase confirm</code>'
      );
      return;
    }
    await runCleanbase();
    await sendText(chatId, '✅ База релизов очищена.');
    return;
  }
  const broadcastMatch = /^\/broadcast(?:@\w+)?(?:\s+([\s\S]+))?$/i.exec(text);
  if (broadcastMatch) {
    if (!isAdmin(uid)) {
      await sendText(chatId, '❌ Команда доступна только администраторам.');
      return;
    }
    const payloadText = clean(broadcastMatch[1] || '');
    if (payloadText) {
      await sendText(chatId, '📢 Запускаю рассылку...');
      const result = await runBroadcast(payloadText);
      await sendText(
        chatId,
        `📢 <b>Рассылка завершена</b>\n\n` +
        `Всего получателей: <b>${result.total}</b>\n` +
        `✅ Доставлено: <b>${result.sent}</b>\n` +
        `❌ Ошибок: <b>${result.failed}</b>`
      );
      return;
    }
    setBroadcastSession(uid, true);
    await sendText(chatId, '📢 Отправьте текст рассылки следующим сообщением.\n\nДля отмены: /cancel');
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
    const hadAny = !!(getFormSession(uid) || getCoverSession(uid) || getPromoSession(uid) || hasBroadcastSession(uid));
    resetAllSessions(uid);
    setBroadcastSession(uid, false);
    await sendText(chatId, hadAny ? 'Текущая анкета отменена.' : 'Нет активной анкеты.');
    return;
  }
  if (text === '/my' || text === '/my_releases') {
    await sendMy(chatId, uid);
    return;
  }
}
async function onCallback(query) {
  const data = clean(query.data);
  const chatId = query?.message?.chat?.id;
  if (!data || !chatId) return;
  try { await tg('answerCallbackQuery', { callback_query_id: query.id }); } catch {}

  if (await handleFormCallback(query, data)) return;
  if (await handlePromoCallback(query, data)) return;

  const edit = async (text, markup) => {
    try {
      await tg('editMessageText', {
        chat_id: chatId,
        message_id: query.message.message_id,
        text,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
        reply_markup: markup
      });
    } catch (e) {
      const em = clean(e?.message || e);
      if (em.includes('message is not modified')) return;
      throw e;
    }
  };
  if (data === 'admin_back') {
    if (!isAdmin(query.from.id)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    await edit(buildAdminPanelText(), adminPanelKeyboard());
    return;
  }
  if (data === 'admin_stats') {
    if (!isAdmin(query.from.id)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    await edit('📊 Выберите период для статистики:', statsPeriodKeyboard(true));
    return;
  }
  if (data === 'admin_backup') {
    if (!isAdmin(query.from.id)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    const file = await makeBackupAndSend(chatId, 'releases');
    await sendText(chatId, `✅ Бэкап создан: <code>${esc(path.basename(file))}</code>`);
    return;
  }
  if (data === 'admin_moderation_backup') {
    if (!isAdmin(query.from.id)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    const file = await makeBackupAndSend(chatId, 'moderation');
    await sendText(chatId, `✅ Бэкап модерации создан: <code>${esc(path.basename(file))}</code>`);
    return;
  }
  if (data === 'admin_pending') {
    if (!isAdmin(query.from.id)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    await edit(buildPendingText(20), adminPanelKeyboard());
    return;
  }
  if (data === 'admin_all_releases') {
    if (!isAdmin(query.from.id)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    await edit(buildAllReleasesText(30), adminPanelKeyboard());
    return;
  }
  if (data === 'admin_cleanup') {
    if (!isAdmin(query.from.id)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    const summary = runServiceCleanup();
    await edit(
      '🧹 <b>Очистка завершена</b>\n\n' +
      `Pending actions: <b>${summary.pendingActionsBefore}</b> → <b>${summary.pendingActionsAfter}</b>\n` +
      `Зеркало модерации: <b>${summary.moderationMirrorBefore}</b> → <b>${summary.moderationMirrorAfter}</b>`,
      adminPanelKeyboard()
    );
    return;
  }
  if (data === 'admin_broadcast') {
    if (!isAdmin(query.from.id)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    setBroadcastSession(String(query.from.id), true);
    await sendText(chatId, '📢 Отправьте текст рассылки следующим сообщением.\n\nДля отмены: /cancel');
    return;
  }
  if (data === 'admin_cleanbase_confirm') {
    if (!isAdmin(query.from.id)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    await edit(
      '⚠️ <b>Подтверждение очистки</b>\n\nЭто действие удалит ВСЕ релизы из базы.\nПродолжить?',
      {
        inline_keyboard: [
          [
            { text: '✅ Да, удалить всё', callback_data: 'admin_cleanbase_yes' },
            { text: '❌ Отмена', callback_data: 'admin_back' }
          ]
        ]
      }
    );
    return;
  }
  if (data === 'admin_cleanbase_yes') {
    if (!isAdmin(query.from.id)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    await runCleanbase();
    await edit('✅ База релизов полностью очищена.', adminPanelKeyboard());
    return;
  }
  if (data.startsWith('stats_period_')) {
    if (!isAdmin(query.from.id)) {
      await sendText(chatId, '❌ Доступ запрещён.');
      return;
    }
    const period = data.slice('stats_period_'.length);
    const valid = ['week', 'month', 'all'];
    const key = valid.includes(period) ? period : 'all';
    await edit(buildPeriodStatsText(key), statsPeriodKeyboard(true));
    return;
  }
  if (data === 'my_page_info') {
    return;
  }
  const myPageMatch = /^my_page_(\d+)_(\d+)$/.exec(data);
  if (myPageMatch) {
    const ownerId = String(myPageMatch[1]);
    const page = Number.parseInt(myPageMatch[2], 10);
    const requester = String(query.from.id);
    if (requester !== ownerId && !isAdmin(requester)) {
      await sendText(chatId, '❌ Это не ваш кабинет.');
      return;
    }
    const view = buildMyCabinetView(ownerId, page);
    await edit(view.text, view.keyboard);
    return;
  }
  if (data === 'my_back') {
    const uid = String(query.from.id);
    const view = buildMyCabinetView(uid);
    await edit(view.text, view.keyboard);
    return;
  }
  const detailsMatch = /^release_details_(\d+)_(\d+)(?:_(\d+))?$/.exec(data);
  if (detailsMatch) {
    const ownerId = String(detailsMatch[1]);
    const idx = Number.parseInt(detailsMatch[2], 10);
    const page = Number.parseInt(detailsMatch[3] || '0', 10);
    const requester = String(query.from.id);
    if (requester !== ownerId && !isAdmin(requester)) {
      await sendText(chatId, '❌ Это не ваш релиз.');
      return;
    }
    const rel = findRelease(ownerId, idx);
    if (!rel) {
      await sendText(chatId, '❌ Релиз не найден.');
      return;
    }
    await edit(buildReleaseDetailsText(ownerId, idx, rel), {
      inline_keyboard: [[{ text: '◀ В кабинет', callback_data: `my_page_${ownerId}_${Number.isFinite(page) ? Math.max(0, page) : 0}` }]]
    });
    return;
  }
  const deleteMatch = /^delete_release_(\d+)_(\d+)(?:_(\d+))?$/.exec(data);
  if (deleteMatch) {
    const ownerId = String(deleteMatch[1]);
    const idx = Number.parseInt(deleteMatch[2], 10);
    const page = Number.parseInt(deleteMatch[3] || '0', 10);
    const requester = String(query.from.id);
    if (requester !== ownerId && !isAdmin(requester)) {
      await sendText(chatId, '❌ Это не ваш релиз.');
      return;
    }
    const rel = findRelease(ownerId, idx);
    if (!rel) {
      await sendText(chatId, '❌ Релиз не найден.');
      return;
    }
    if (!rel.user_deleted) {
      rel.user_deleted = true;
      rel.deleted_at = new Date().toISOString();
      saveDb();
      syncModerationMirror(ownerId, idx, rel);
      saveModDb();
      try {
        await sendModerationText(
          `🗑 <b>Релиз удалён артистом из кабинета</b>\n\n🎵 ${esc(rel.name || 'Релиз')}\n👤 ${esc(rel.nick || '—')}\nID: <code>${esc(ownerId)}</code>`
        );
      } catch {
        // ignore moderation notify errors
      }
    }
    const view = buildMyCabinetView(ownerId, page);
    await edit(view.text, view.keyboard);
    return;
  }
  if (data === 'main') { await edit(welcomeText(), keyboardMain()); return; }
  if (data === 'menu_distribution') { await edit('<b>Дистрибуция</b>\n\nВыберите действие:', keyboardDist()); return; }
  if (data === 'menu_services') { await edit('<b>Сервисы</b>\n\nВыберите действие:', keyboardServices()); return; }
  if (data === 'menu_cabinet') { await edit('<b>Кабинет</b>\n\nВыберите действие:', keyboardCabinet()); return; }
  if (data === 'menu_community') { await edit('<b>Комьюнити</b>\n\nОфициальные площадки CXRNER MUSIC:', keyboardCommunity()); return; }
  if (data === 'open_app' || data === 'report_app') return;
  if (data === 'report' || data === 'report_text') {
    await startTextForm(chatId, String(query.from.id), query.from);
    return;
  }
  if (data === 'my_releases') { await sendMy(chatId, String(query.from.id)); return; }
  if (data === 'service_cover') { await startCoverFlow(chatId, String(query.from.id), query.from); return; }
  if (data === 'service_promo') { await startPromoFlow(chatId, String(query.from.id), query.from); return; }
  const restoreMatch = /^m_restore_buttons_(\d+)_(\d+)$/.exec(data);
  if (restoreMatch) {
    const uid = restoreMatch[1];
    const idx = Number.parseInt(restoreMatch[2], 10);
    const rel = findRelease(uid, idx);
    if (!rel) {
      await sendText(chatId, '❌ Релиз не найден.');
      return;
    }
    await refreshModerationMessage(uid, idx, rel, Number(query.message?.message_id || 0));
    return;
  }
  const m = /^m_(upload|moderate|approve|reject|needfix|delete|upc)_(\d+)_(\d+)$/.exec(data);
  if (m) {
    if (m[1] === 'reject') {
      await startModerationReplyFlow(query, 'reject_reason', m[2], Number.parseInt(m[3], 10));
      return;
    }
    if (m[1] === 'upc') {
      await startModerationReplyFlow(query, 'upc', m[2], Number.parseInt(m[3], 10));
      return;
    }
    await applyModeration(query, m[1], m[2], Number.parseInt(m[3], 10));
  }
}

function startStaticServer() {
  if (!WEB_ENABLED) return;
  const root = path.resolve(ROOT, WEB_DIR);
  if (!fs.existsSync(root) || !fs.statSync(root).isDirectory()) return;
  const allowedOrigins = new Set(
    [
      BASE,
      envStr('MINIAPP_ORIGIN', ''),
      'https://cxrnermusic.vercel.app',
      'https://web.telegram.org'
    ]
      .map((v) => clean(v).replace(/\/+$/, ''))
      .filter(Boolean)
  );
  function setCors(req, res) {
    const origin = clean(req.headers?.origin || '').replace(/\/+$/, '');
    if (origin && allowedOrigins.has(origin)) {
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Vary', 'Origin');
    }
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'content-type,authorization');
  }
  function sendJson(res, statusCode, data) {
    res.writeHead(statusCode, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(data));
  }
  function readJsonBody(req, maxBytes = 64 * 1024) {
    return new Promise((resolve, reject) => {
      let size = 0;
      const chunks = [];
      req.on('data', (chunk) => {
        size += chunk.length;
        if (size > maxBytes) {
          reject(new Error('payload too large'));
          req.destroy();
          return;
        }
        chunks.push(chunk);
      });
      req.on('end', () => {
        try {
          const raw = Buffer.concat(chunks).toString('utf8').trim();
          if (!raw) {
            resolve({});
            return;
          }
          resolve(JSON.parse(raw));
        } catch (e) {
          reject(new Error(`invalid json: ${clean(e?.message || e)}`));
        }
      });
      req.on('error', (e) => reject(e));
    });
  }
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
  const srv = http.createServer(async (req, res) => {
    try {
      const u = new URL(req.url || '/', 'http://localhost');
      if (u.pathname.startsWith('/api/')) {
        setCors(req, res);
      }
      if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
      }
      if (u.pathname === '/api/miniapp/ping') {
        sendJson(res, 200, {
          ok: true,
          now: new Date().toISOString(),
          supabase_sync: SUPABASE_SYNC_ENABLED,
          features: { ...supabaseFeatureState }
        });
        return;
      }
      if (u.pathname === '/api/miniapp/cabinet') {
        const telegramId = clean(u.searchParams.get('telegram_id') || '');
        const initData = clean(u.searchParams.get('init_data') || '');
        if (!telegramId || !/^\d{4,20}$/.test(telegramId)) {
          sendJson(res, 400, { ok: false, error: 'telegram_id is required' });
          return;
        }
        if (initData) {
          const check = verifyTelegramInitData(initData, telegramId);
          if (!check.ok) {
            sendJson(res, 403, { ok: false, error: 'initData validation failed', reason: check.reason });
            return;
          }
        }
        getCabinetSnapshot(telegramId)
          .then((snapshot) => {
            sendJson(res, 200, { ok: true, telegram_id: telegramId, ...snapshot });
          })
          .catch((err) => {
            sendJson(res, 500, { ok: false, error: clean(err?.message || err) });
        });
        return;
      }
      if (u.pathname === '/api/miniapp/submit') {
        if (req.method !== 'POST') {
          sendJson(res, 405, { ok: false, error: 'method not allowed' });
          return;
        }
        let payload;
        try {
          payload = await readJsonBody(req, Math.max(8192, WEBAPP_MAX_PAYLOAD_BYTES + 1024));
        } catch (e) {
          sendJson(res, 400, { ok: false, error: clean(e?.message || e) || 'invalid request body' });
          return;
        }
        if (!payload || typeof payload !== 'object') {
          sendJson(res, 400, { ok: false, error: 'json object expected' });
          return;
        }

        const parsed = parseWebappPayload(payload);
        const action = clean(parsed.action || '').toLowerCase();
        if (!['webapp_release_submit', 'submit_release', ''].includes(action)) {
          sendJson(res, 400, { ok: false, error: 'unsupported action' });
          return;
        }
        if (!parsed.form || typeof parsed.form !== 'object') {
          sendJson(res, 400, { ok: false, error: 'form object is required' });
          return;
        }

        let uid = clean(parsed.telegram_id || payload.telegram_id || '');
        let user = payload.user && typeof payload.user === 'object' ? payload.user : {};
        const initData = clean(parsed.init_data || payload.init_data || '');

        if (initData) {
          const check = verifyTelegramInitData(initData, uid || '');
          if (!check.ok) {
            if (WEBAPP_REQUIRE_INITDATA) {
              sendJson(res, 403, { ok: false, error: 'initData validation failed', reason: check.reason });
              return;
            }
            console.warn(
              `[WEBAPP] submit api initData warning: user_id=${uid || '-'} reason=${check.reason} (fallback mode)`
            );
          } else {
            const initUid = clean(check.user?.id || '');
            if (!uid && initUid) uid = initUid;
            if ((!user || !user.id) && check.user && typeof check.user === 'object') {
              user = check.user;
            }
          }
        } else if (WEBAPP_REQUIRE_INITDATA) {
          sendJson(res, 403, { ok: false, error: 'initData is required' });
          return;
        }

        if (!uid || !/^\d{4,20}$/.test(uid)) {
          sendJson(res, 400, { ok: false, error: 'valid telegram_id is required' });
          return;
        }
        const antiSpam = verifyWebappAntiSpam(uid, JSON.stringify(payload));
        if (!antiSpam.ok) {
          sendJson(res, 429, { ok: false, error: antiSpam.reason || 'duplicate submit' });
          return;
        }

        const vr = validateForm(parsed.form, {
          ...parsed,
          action: action || 'webapp_release_submit',
          source: 'mini_app',
          telegram_id: uid
        });
        if (vr.errors.length) {
          sendJson(res, 422, { ok: false, errors: vr.errors });
          return;
        }

        const submitUser = {
          id: Number(uid),
          username: sanitizeText(user?.username || '', 64),
          first_name: sanitizeText(user?.first_name || '', 64)
        };
        try {
          const out = await submitReleaseToModeration(submitUser, uid, vr.data, 'mini_app_api');
          await supabaseUpsertCabinetUser(uid, submitUser, true);
          try {
            await sendText(
              Number(uid),
              '✅ <b>Анкета отправлена в модерацию</b>\n\n' +
              `🎵 ${esc(vr.data?.name || 'Релиз')}\n` +
              `👤 ${esc(vr.data?.nick || '—')}`
            );
          } catch {}
          sendJson(res, 200, {
            ok: true,
            telegram_id: uid,
            idx: Number(out?.idx ?? -1),
            moderation_message_id: Number(out?.rel?.moderation_message_id || 0)
          });
        } catch (e) {
          sendJson(res, 500, { ok: false, error: clean(e?.message || e) || 'submit failed' });
        }
        return;
      }
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
let pollingFailStreak = 0;
async function loop() {
  while (!stopping) {
    try {
      const updates = await tg('getUpdates', { timeout: 50, offset, allowed_updates: ['message', 'callback_query'] });
      if (pollingFailStreak > 0) {
        console.info(`[bot] polling recovered after ${pollingFailStreak} errors`);
        pollingFailStreak = 0;
      }
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
      pollingFailStreak += 1;
      const errText = clean(e?.message || e) || 'unknown error';
      console.error(`[bot] polling error #${pollingFailStreak}: ${errText}`);
      if (pollingFailStreak === 1 || pollingFailStreak % 10 === 0) {
        try {
          const dnsRows = await dns.promises.lookup('api.telegram.org', { all: true });
          const list = Array.isArray(dnsRows)
            ? dnsRows.map((it) => `${it.address}/${it.family}`).join(', ')
            : '';
          if (list) console.error(`[bot] dns api.telegram.org: ${list}`);
        } catch (dnsErr) {
          console.error(`[bot] dns lookup failed: ${clean(dnsErr?.message || dnsErr)}`);
        }
      }
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
}

(async () => {
  fs.mkdirSync(path.resolve(ROOT, 'webapp/data'), { recursive: true });
  const imported = importBackupsIntoDb();
  if (SUPABASE_SYNC_ENABLED) {
    await hydrateFromSupabase();
  }
  exportReleases();
  exportCabinet();
  if (SUPABASE_SYNC_ENABLED) {
    await syncSupabaseNow('startup');
  }
  startStaticServer();
  console.info('[bot] CXRNER Node fallback bot started');
  console.info(`[bot] moderation chat: ${MOD_CHAT}`);
  console.info(`[bot] admin ids: ${ADMIN_IDS.join(', ')}`);
  if (imported.added || imported.merged) {
    console.info(`[bot] backup import: added=${imported.added}, merged=${imported.merged}`);
  }
  if (SUPABASE_SYNC_ENABLED) {
    console.info(`[bot] supabase sync: enabled (${SUPABASE_URL})`);
    console.info(
      `[bot] supabase tables: ${SUPABASE_RELEASES_TABLE}, ${SUPABASE_CABINET_TABLE}, ` +
      `${SUPABASE_FORMS_TABLE}, ${SUPABASE_USERS_TABLE}, ${SUPABASE_PUBLIC_RELEASES_TABLE}`
    );
    console.info(
      `[bot] supabase fetch: timeout=${SUPABASE_FETCH_TIMEOUT_MS}ms` +
      ` retries=${SUPABASE_FETCH_RETRIES} delay=${SUPABASE_FETCH_RETRY_DELAY_MS}ms`
    );
  } else {
    console.info('[bot] supabase sync: disabled');
  }
  console.info(`[bot] telegram api base: ${TELEGRAM_API_BASE}`);
  console.info(`[bot] tg fetch: timeout=${TG_FETCH_TIMEOUT_MS}ms retries=${TG_FETCH_RETRIES}`);
  console.info(`[bot] webapp initData strict: ${WEBAPP_REQUIRE_INITDATA ? 'enabled' : 'disabled'}`);
  console.info(`[bot] moderation thread: ${MODERATION_THREAD_ID > 0 ? MODERATION_THREAD_ID : 'default'}`);
  if (WEBAPP_URL) console.info(`[bot] webapp url: ${WEBAPP_URL}`);
  await verifyModerationChatAccess();
  try { await tg('deleteWebhook', { drop_pending_updates: false }); } catch {}
  process.on('SIGINT', () => { stopping = true; process.exit(0); });
  process.on('SIGTERM', () => { stopping = true; process.exit(0); });
  await loop();
})();
