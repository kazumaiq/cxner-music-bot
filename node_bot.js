'use strict';

const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const dns = require('node:dns');
const crypto = require('node:crypto');
const { execSync } = require('node:child_process');

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
let WEBAPP_URL = envStr('WEBAPP_URL', '');
if (/\.vercel\.app\/index\.html$/i.test(WEBAPP_URL)) {
  WEBAPP_URL = WEBAPP_URL.replace(/\/index\.html$/i, '/');
}
const WEB_HOST = envStr('WEB_SERVER_HOST', '0.0.0.0');
const WEB_PORT = envInt('PORT', envInt('WEB_SERVER_PORT', 8080));
const WEB_DIR = envStr('WEB_SERVER_DIR', 'webapp');
// When Bothost provides PORT and a Web App URL, the Mini App server must be
// available even if the optional Supabase sync is temporarily unavailable.
const WEB_ENABLED = envBool('ENABLE_WEB_SERVER', false)
  || (Boolean(clean(process.env.PORT || '')) && Boolean(WEBAPP_URL));
const WEBHOOK_SECRET = envStr('BOT_BACKEND_SECRET', '');
const ADMIN_IDS = envIntList('ADMIN_IDS', [881379104]);
const MODERATION_HEALTH_TTL_MS = envInt('MODERATION_HEALTH_TTL_MS', 180000);
const TELEGRAM_INITDATA_MAX_AGE_SEC = envInt('TELEGRAM_INITDATA_MAX_AGE_SEC', 86400);
const WEBAPP_SUBMIT_THROTTLE_MS = envInt('WEBAPP_SUBMIT_THROTTLE_MS', 7000);
const WEBAPP_MAX_PAYLOAD_BYTES = envInt('WEBAPP_MAX_PAYLOAD_BYTES', 3900);
const SUPABASE_URL = envStr('SUPABASE_URL', '');
const SUPABASE_SERVICE_ROLE_KEY = envStr('SUPABASE_SERVICE_ROLE_KEY', envStr('SUPABASE_KEY', ''));
const SUPABASE_SCHEMA = envStr('SUPABASE_SCHEMA', 'public') || 'public';
const SUPABASE_RELEASES_TABLE_RAW = envStr('SUPABASE_RELEASES_TABLE', 'cxrner_releases') || 'cxrner_releases';
const SUPABASE_CABINET_TABLE_RAW = envStr('SUPABASE_CABINET_TABLE', 'cxrner_cabinet_users') || 'cxrner_cabinet_users';
const SUPABASE_FORMS_TABLE_RAW = envStr('SUPABASE_FORMS_TABLE', 'cxrner_forms') || 'cxrner_forms';
const SUPABASE_USERS_TABLE_RAW = envStr('SUPABASE_USERS_TABLE', 'cxrner_users') || 'cxrner_users';
const SUPABASE_PUBLIC_RELEASES_TABLE_RAW = envStr('SUPABASE_PUBLIC_RELEASES_TABLE', 'cxrner_public_releases') || 'cxrner_public_releases';
const SUPABASE_INTERACTIONS_TABLE_RAW = envStr('SUPABASE_INTERACTIONS_TABLE', 'cxrner_interactions') || 'cxrner_interactions';
const SUPABASE_SYNC_ENABLED = !!SUPABASE_URL && !!SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_FETCH_TIMEOUT_MS = envInt('SUPABASE_FETCH_TIMEOUT_MS', 25000);
const SUPABASE_FETCH_RETRIES = envInt('SUPABASE_FETCH_RETRIES', 4);
const SUPABASE_FETCH_RETRY_DELAY_MS = envInt('SUPABASE_FETCH_RETRY_DELAY_MS', 1200);
const SUPABASE_SYNC_DEBOUNCE_MS = envInt('SUPABASE_SYNC_DEBOUNCE_MS', 1200);
const SUPABASE_SYNC_CHUNK_SIZE = Math.max(10, envInt('SUPABASE_SYNC_CHUNK_SIZE', 50));
const SUPABASE_SYNC_CHUNK_DELAY_MS = Math.max(0, envInt('SUPABASE_SYNC_CHUNK_DELAY_MS', 400));
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
const SUPABASE_INTERACTIONS_TABLE = /^[A-Za-z_][A-Za-z0-9_]*$/.test(SUPABASE_INTERACTIONS_TABLE_RAW)
  ? SUPABASE_INTERACTIONS_TABLE_RAW
  : 'cxrner_interactions';
const SUPABASE_LINKS_TABLE = 'cxrner_links';
const SUPABASE_LINK_EVENTS_TABLE = 'cxrner_link_events';
const FORM_STATUS = {
  PENDING: 'pending',
  ON_MODERATION: 'on_moderation',
  APPROVED: 'approved',
  REJECTED: 'rejected'
};
const BOT_BUILD_VERSION = '2026-11-16-ui-v8';
const BOT_BUILD_RELEASE_NOTE = 'Mini App 4.0 (Liquid Glass, 60 FPS DROP roulette, Путь чемпиона / друзья, Admin Control Center), автозакрепление и кэш-синхронизация';
const BOT_STARTED_AT_MS = Date.now();
const BOT_STARTED_AT = new Date(BOT_STARTED_AT_MS).toISOString();

function readDotGitHeadMeta(rootDir) {
  try {
    const gitDir = path.join(rootDir, '.git');
    const headRaw = clean(fs.readFileSync(path.join(gitDir, 'HEAD'), 'utf8'));
    if (!headRaw) return null;
    if (!headRaw.startsWith('ref:')) {
      return { commit: headRaw.slice(0, 7), branch: '', source: 'dotgit' };
    }
    const refPath = clean(headRaw.slice(4));
    const branch = refPath.replace(/^refs\/heads\//, '');
    const looseRefFile = path.join(gitDir, ...refPath.split('/'));
    if (fs.existsSync(looseRefFile)) {
      const sha = clean(fs.readFileSync(looseRefFile, 'utf8'));
      if (sha) return { commit: sha.slice(0, 7), branch, source: 'dotgit' };
    }
    const packedRefsFile = path.join(gitDir, 'packed-refs');
    if (fs.existsSync(packedRefsFile)) {
      const lines = fs.readFileSync(packedRefsFile, 'utf8').split(/\r?\n/);
      for (const line of lines) {
        if (!line || line.startsWith('#') || line.startsWith('^')) continue;
        const [sha, ref] = line.trim().split(/\s+/);
        if (ref === refPath && sha) return { commit: sha.slice(0, 7), branch, source: 'dotgit' };
      }
    }
  } catch {}
  return null;
}

function readGitDeployMeta() {
  const envCommit = clean(
    process.env.GIT_COMMIT ||
    process.env.SOURCE_COMMIT ||
    process.env.GITHUB_SHA ||
    process.env.RAILWAY_GIT_COMMIT_SHA ||
    process.env.VERCEL_GIT_COMMIT_SHA ||
    process.env.RENDER_GIT_COMMIT ||
    process.env.COMMIT_SHA ||
    process.env.SOURCE_VERSION ||
    ''
  );
  const envBranch = clean(process.env.GIT_BRANCH || process.env.GITHUB_REF_NAME || '');
  const envMessage = clean(process.env.GIT_COMMIT_MESSAGE || '');
  try {
    const out = execSync('git -c safe.directory=* log -1 --format=%h%n%cI%n%s', {
      cwd: ROOT,
      timeout: 1500,
      stdio: ['ignore', 'pipe', 'ignore']
    }).toString('utf8').split(/\r?\n/);
    const branch = clean(execSync('git -c safe.directory=* rev-parse --abbrev-ref HEAD', {
      cwd: ROOT,
      timeout: 1000,
      stdio: ['ignore', 'pipe', 'ignore']
    }).toString('utf8'));
    if (clean(out[0])) {
      return {
        commit: clean(out[0]),
        commit_date: clean(out[1] || ''),
        commit_subject: clean(out[2] || '').slice(0, 120),
        branch: branch || envBranch || 'main',
        source: 'git'
      };
    }
  } catch {}
  const dotGit = readDotGitHeadMeta(ROOT);
  if (dotGit && dotGit.commit) {
    return {
      commit: dotGit.commit,
      commit_date: '',
      commit_subject: envMessage ? envMessage.slice(0, 120) : BOT_BUILD_RELEASE_NOTE,
      branch: dotGit.branch || envBranch || 'main',
      source: dotGit.source
    };
  }
  return {
    commit: envCommit ? envCommit.slice(0, 12) : '',
    commit_date: '',
    commit_subject: envMessage ? envMessage.slice(0, 120) : BOT_BUILD_RELEASE_NOTE,
    branch: envBranch || 'main',
    source: envCommit ? 'env' : 'build_tag'
  };
}
const BOT_DEPLOY_INFO = readGitDeployMeta();

function formatUptimeMs(ms) {
  const totalSec = Math.max(0, Math.floor(Number(ms || 0) / 1000));
  const d = Math.floor(totalSec / 86400);
  const h = Math.floor((totalSec % 86400) / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  const parts = [];
  if (d) parts.push(`${d}д`);
  if (h || parts.length) parts.push(`${h}ч`);
  if (m || parts.length) parts.push(`${m}м`);
  parts.push(`${s}с`);
  return parts.join(' ');
}
const pollingHealth = {
  ok: null,
  fail_streak: 0,
  last_poll_ok_at: null,
  last_update_at: null,
  last_error: '',
  last_command: '',
  last_command_at: null,
  last_command_result: '',
  last_command_error: ''
};
const moderationHealth = {
  ok: null,
  checked_at: 0,
  reason: '',
  chat_title: '',
  bot_id: '',
  bot_username: '',
  bot_status: '',
  can_send_messages: null,
  can_pin_messages: null
};
let supabaseSyncInProgress = false;
let supabaseSyncQueued = false;
let supabaseSyncTimer = null;
const webappSubmitAntiSpam = new Map();
const supabaseFeatureState = {
  forms: true,
  users: true,
  cabinet: true,
  public_releases: true,
  interactions: true
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

function stripInlineButtonStyles(payload) {
  if (!payload || typeof payload !== 'object' || !payload.reply_markup || !Array.isArray(payload.reply_markup.inline_keyboard)) {
    return payload;
  }
  return {
    ...payload,
    reply_markup: {
      ...payload.reply_markup,
      inline_keyboard: payload.reply_markup.inline_keyboard.map((row) =>
        Array.isArray(row)
          ? row.map((btn) => {
              if (!btn || typeof btn !== 'object') return btn;
              const copy = { ...btn };
              delete copy.style;
              delete copy.icon_custom_emoji_id;
              return copy;
            })
          : row
      )
    }
  };
}

function buildModerationMessageLink(messageId) {
  const mid = Number(messageId || 0);
  const raw = String(MOD_CHAT || '');
  if (!mid || !raw.startsWith('-100')) return '';
  return `https://t.me/c/${raw.slice(4)}/${mid}`;
}

function tryDeleteMessageSafe(chatId, messageId, delayMs = 0) {
  const cid = clean(chatId);
  const mid = Number(messageId || 0);
  if (!cid || !mid) return;
  const run = () => tg('deleteMessage', { chat_id: cid, message_id: mid }, { noRateLimitRetry: true }).catch(() => {});
  if (delayMs > 0) {
    setTimeout(run, delayMs);
  } else {
    run();
  }
}

async function safeAnswerCallback(callbackQueryId, opts = {}) {
  if (!callbackQueryId) return;
  const payload = { callback_query_id: callbackQueryId };
  if (opts.text) payload.text = String(opts.text).slice(0, 190);
  if (opts.show_alert) payload.show_alert = true;
  try {
    await tg('answerCallbackQuery', payload, { noRateLimitRetry: true });
  } catch {}
}

async function tg(method, payload = {}, opts = {}) {
  const url = `${API}/${method}`;
  const retries = Math.max(0, Number(TG_FETCH_RETRIES) || 0);
  let lastErr = null;
  let activePayload = payload;
  let strippedStyles = false;
  let rateLimitRetries = 0;

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
        body: JSON.stringify(activePayload),
        signal: controller ? controller.signal : undefined
      });
      if (timeoutRef) clearTimeout(timeoutRef);
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j.ok) {
        const desc = clean(j.description || r.statusText);
        if (
          !strippedStyles &&
          activePayload?.reply_markup?.inline_keyboard &&
          (r.status === 400 || /\bstyle\b|icon_custom_emoji_id|button_type|unknown field/i.test(desc)) &&
          /\bstyle\b|icon_custom_emoji_id|button/i.test(desc)
        ) {
          strippedStyles = true;
          activePayload = stripInlineButtonStyles(activePayload);
          attempt -= 1;
          continue;
        }
        const retrySec = Number(j?.parameters?.retry_after || 0) || (() => {
          const m = /retry\s+after\s+(\d+)/i.exec(desc);
          return m ? Number.parseInt(m[1], 10) : 0;
        })();
        if (!opts.noRateLimitRetry && method !== 'getUpdates' && retrySec > 0 && retrySec <= 25 && rateLimitRetries < 1) {
          rateLimitRetries += 1;
          await delay((retrySec + 1) * 1000);
          attempt -= 1;
          continue;
        }
        throw new Error(`[${method}] ${desc}`);
      }
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
  if (rel.moderation_pinned !== undefined) rel.moderation_pinned = !!rel.moderation_pinned;
  rel.pin_pending = !!rel.pin_pending;
  rel.manual_unpinned = !!rel.manual_unpinned;
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
    cover_url: clean(payload?.cover_url || payload?.cover || ''),
    smart_link: clean(payload?.smart_link || payload?.multilink || payload?.beatlink || ''),
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

  if (!supabaseFeatureState.cabinet) return ok;
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
    const errText = clean(e?.message || e);
    console.error('[supabase] cabinet upsert failed:', errText);
    if (/22P02|uuid/i.test(errText)) {
      disableSupabaseFeature('cabinet', 'legacy cxrner_cabinet_users.user_id is uuid; Telegram IDs are stored in cxrner_users');
    }
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
  const parsedSubmissionDate = Date.parse(submissionKey);
  const createdAt = Number.isFinite(parsedSubmissionDate)
    ? new Date(parsedSubmissionDate).toISOString()
    : now;
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
    created_at: createdAt,
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
    updated_at: now
  };
  if (Object.prototype.hasOwnProperty.call(patch, 'status')) {
    body.status = clean(patch.status || FORM_STATUS.ON_MODERATION);
  }
  if (Object.prototype.hasOwnProperty.call(patch, 'reject_reason')) {
    body.reject_reason = clean(patch.reject_reason || '');
  }
  if (Object.prototype.hasOwnProperty.call(patch, 'upc')) {
    body.upc = clean(patch.upc || '');
  }
  if (Object.prototype.hasOwnProperty.call(patch, 'moderation_message_id')) {
    body.moderation_message_id = String(patch.moderation_message_id || '');
  }
  if (patch.form_payload || patch.moderator_comment !== undefined || patch.user_deleted !== undefined) {
    const existingPayload = rel.form_payload && typeof rel.form_payload === 'object' ? rel.form_payload : {};
    const newPayload = {
      ...existingPayload,
      ...(patch.form_payload && typeof patch.form_payload === 'object' ? patch.form_payload : {})
    };
    if (patch.moderator_comment !== undefined) newPayload.moderator_comment = clean(patch.moderator_comment);
    if (patch.user_deleted !== undefined) newPayload.user_deleted = Boolean(patch.user_deleted);
    body.form_payload = newPayload;
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
  const byKey = new Map();
  for (const [uid, listRaw] of Object.entries(db || {})) {
    const list = Array.isArray(listRaw) ? listRaw : [];
    for (let idx = 0; idx < list.length; idx += 1) {
      const rel = normalizeRelease(list[idx] || {});
      rel.release_idx = idx;
      const row = buildSupabaseFormRow(uid, { username: rel.username || '' }, rel, mapReleaseStatusToFormStatus(rel.status));
      row.form_payload = safeJson(rel) || {};
      row.moderation_message_id = Number(rel.moderation_message_id || 0) || null;
      const key = clean(row.submission_key || `${uid}:${idx}`);
      byKey.set(key, row);
    }
  }
  return Array.from(byKey.values());
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
  const byFormId = new Map();
  const now = new Date().toISOString();
  for (const [uid, listRaw] of Object.entries(db || {})) {
    const list = Array.isArray(listRaw) ? listRaw : [];
    for (let idx = 0; idx < list.length; idx += 1) {
      const rel = normalizeRelease(list[idx] || {});
      const st = canonicalStatus(rel.status);
      if (![STATUS.APPROVED, STATUS.PUBLISHED].includes(st)) continue;
      const formId = clean(rel.supabase_form_id || `${uid}:${clean(rel.submission_time || idx)}`);
      byFormId.set(formId, {
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
  return Array.from(byFormId.values());
}

async function supabaseUpsertRows(tableName, rows, chunkSize = SUPABASE_SYNC_CHUNK_SIZE) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabaseRequest(`${tableName}?on_conflict=user_id,release_idx`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: chunk
    });
    if (SUPABASE_SYNC_CHUNK_DELAY_MS > 0 && i + chunkSize < rows.length) await delay(SUPABASE_SYNC_CHUNK_DELAY_MS);
  }
}

async function supabaseUpsertCabinetRows(tableName, rows, chunkSize = SUPABASE_SYNC_CHUNK_SIZE) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabaseRequest(`${tableName}?on_conflict=user_id`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: chunk
    });
    if (SUPABASE_SYNC_CHUNK_DELAY_MS > 0 && i + chunkSize < rows.length) await delay(SUPABASE_SYNC_CHUNK_DELAY_MS);
  }
}

async function supabaseUpsertFormRows(tableName, rows, chunkSize = SUPABASE_SYNC_CHUNK_SIZE) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    try {
      await supabaseRequest(`${tableName}?on_conflict=telegram_id,submission_key`, {
        method: 'POST',
        headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
        body: chunk
      });
    } catch (e) {
      const msg = clean(e?.message || e);
      if (/cxrner_forms_submission_key_key|23505/i.test(msg)) {
        await supabaseRequest(`${tableName}?on_conflict=submission_key`, {
          method: 'POST',
          headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
          body: chunk
        });
      } else {
        throw e;
      }
    }
    if (SUPABASE_SYNC_CHUNK_DELAY_MS > 0 && i + chunkSize < rows.length) await delay(SUPABASE_SYNC_CHUNK_DELAY_MS);
  }
}

async function supabaseUpsertUserRows(tableName, rows, chunkSize = SUPABASE_SYNC_CHUNK_SIZE) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabaseRequest(`${tableName}?on_conflict=telegram_id`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: chunk
    });
    if (SUPABASE_SYNC_CHUNK_DELAY_MS > 0 && i + chunkSize < rows.length) await delay(SUPABASE_SYNC_CHUNK_DELAY_MS);
  }
}

async function supabaseUpsertApprovedRows(tableName, rows, chunkSize = SUPABASE_SYNC_CHUNK_SIZE) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabaseRequest(`${tableName}?on_conflict=form_id`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: chunk
    });
    if (SUPABASE_SYNC_CHUNK_DELAY_MS > 0 && i + chunkSize < rows.length) await delay(SUPABASE_SYNC_CHUNK_DELAY_MS);
  }
}

async function supabaseUpsertInteractionRows(tableName, rows, chunkSize = SUPABASE_SYNC_CHUNK_SIZE) {
  if (!rows.length) return;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await supabaseRequest(`${tableName}?on_conflict=user_id,release_idx,interaction_idx`, {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: chunk
    });
    if (SUPABASE_SYNC_CHUNK_DELAY_MS > 0 && i + chunkSize < rows.length) await delay(SUPABASE_SYNC_CHUNK_DELAY_MS);
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
    const interactionRows = supabaseFeatureState.interactions ? buildSupabaseInteractionRows() : [];

    await supabaseUpsertRows(SUPABASE_RELEASES_TABLE, relRows);
    if (supabaseFeatureState.cabinet) {
      try {
        await supabaseUpsertCabinetRows(SUPABASE_CABINET_TABLE, cabRows);
      } catch (e) {
        const errText = clean(e?.message || e);
        console.error('[supabase] cabinet sync failed:', errText);
        if (/22P02|uuid/i.test(errText)) disableSupabaseFeature('cabinet', 'legacy cabinet table uses uuid user_id');
      }
    }
    if (supabaseFeatureState.interactions) {
      try {
        await supabaseUpsertInteractionRows(SUPABASE_INTERACTIONS_TABLE, interactionRows);
      } catch (e) {
        const errText = clean(e?.message || e);
        console.error('[supabase] interactions sync failed:', errText);
        if (isSupabaseSchemaError(e)) disableSupabaseFeature('interactions', errText);
      }
    }

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
      ` forms=${formRows.length} users=${userRows.length} approved=${approvedRows.length}` +
      ` interactions=${interactionRows.length}`
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
  if (src.includes('manage pinned') || src.includes('can_pin_messages') || src.includes('chat_admin_required')) {
    return 'Выдайте боту права администратора в группе модерации (включая «Закрепление сообщений» / can_pin_messages).';
  }
  if (src.includes('not enough rights') || src.includes('have no rights') || src.includes('forbidden')) {
    return 'Выдайте боту права на отправку и закрепление/открепление сообщений в группе модерации.';
  }
  if (src.includes('message thread not found') || src.includes('topic')) {
    return 'Для группы с топиками укажите MODERATION_THREAD_ID (ID нужного топика).';
  }
  return '';
}

function describeBotMemberStatus(member) {
  const status = clean(member?.status).toLowerCase();
  if (!status) return { ok: false, reason: 'не удалось определить статус бота в чате', can_send_messages: null, can_pin_messages: null, status: '' };
  if (status === 'left' || status === 'kicked') {
    return { ok: false, reason: `бот имеет статус "${status}" в группе модерации`, can_send_messages: false, can_pin_messages: false, status };
  }
  if (status === 'restricted') {
    const canSend = member?.can_send_messages !== false;
    const canPin = member?.can_pin_messages === true;
    if (!canSend) {
      return { ok: false, reason: 'бот ограничен и не может отправлять сообщения (can_send_messages=false)', can_send_messages: false, can_pin_messages: canPin, status };
    }
    return { ok: true, reason: '', can_send_messages: true, can_pin_messages: canPin, status };
  }
  if (status === 'creator') {
    return { ok: true, reason: '', can_send_messages: true, can_pin_messages: true, status };
  }
  if (status === 'administrator') {
    return {
      ok: true,
      reason: '',
      can_send_messages: member?.can_send_messages !== false,
      can_pin_messages: member?.can_pin_messages !== false,
      status
    };
  }
  if (status === 'member') {
    return { ok: true, reason: '', can_send_messages: true, can_pin_messages: null, status };
  }
  return { ok: true, reason: '', can_send_messages: null, can_pin_messages: null, status };
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
    ...(hasValidWebAppUrl() ? [[{ text: '🚀 Открыть CXRNER Mini App', web_app: { url: WEBAPP_URL }, style: 'primary' }]] : []),
    [
      { text: '📀 Дистрибуция', callback_data: 'menu_distribution', style: 'primary' },
      { text: '📁 Мои релизы', callback_data: 'my_releases' }
    ],
    [
      { text: '💼 Сервисы', callback_data: 'menu_services' },
      { text: '🧑‍💻 Кабинет', callback_data: 'menu_cabinet' }
    ],
    [{ text: '🌐 Комьюнити CXRNER', callback_data: 'menu_community' }]
  ]};
}
function keyboardDist() {
  return { inline_keyboard: [
    [{ text: '🎵 Отправить релиз (анкета)', callback_data: 'report_text', style: 'success' }],
    ...(hasValidWebAppUrl() ? [[{ text: '📱 Отправить в Mini App', web_app: { url: WEBAPP_URL }, style: 'primary' }]] : []),
    [
      { text: '📁 Мои релизы', callback_data: 'my_releases' },
      { text: '⬅️ Меню', callback_data: 'main' }
    ]
  ]};
}
function keyboardServices() {
  return { inline_keyboard: [
    [{ text: '🎨 Заказать обложку (500₽)', callback_data: 'service_cover', style: 'primary' }],
    [{ text: '✍️ Промо-текст под релиз', callback_data: 'service_promo' }],
    [{ text: '⬅️ Главное меню', callback_data: 'main' }]
  ]};
}
async function linkTelegramToWebCabinet(chatId, uid, from, codeOrToken) {
  let raw = clean(codeOrToken).trim();
  raw = raw.replace(/^\/(?:start|link)[_\s]*/i, '').trim();

  if (!raw) {
    await sendText(
      chatId,
      '⚠️ <b>Укажите код привязки</b>\n\n' +
      'Пример: <code>CX-849201</code> или <code>/link CX-849201</code>\n\n' +
      'Скопируйте 6-значный код на сайте в разделе «Синхронизация с Telegram»:\n' +
      'https://cxrnermusic.vercel.app/dashboard',
      {
        reply_markup: {
          inline_keyboard: [
            [{ text: '🌐 Открыть кабинет на сайте', url: 'https://cxrnermusic.vercel.app/dashboard' }],
            [{ text: '⬅️ Главное меню', callback_data: 'main' }]
          ]
        }
      }
    );
    return;
  }

  // Normalization
  let cleanToken = raw.replace(/^link_/i, '').trim();
  let cleanCode = raw.toUpperCase();
  if (/^[0-9]{6}$/.test(raw)) {
    cleanCode = 'CX-' + raw;
  }

  console.info(`[bot] linking telegram uid=${uid} username=${from?.username || ''} with code=${cleanCode} token=${cleanToken}`);

  const websiteApiUrl = envStr('WEBSITE_API_URL', 'https://cxrnermusic.vercel.app').replace(/\/+$/, '');
  let apiSuccess = false;
  let apiData = null;

  // Step 1: Call website verify API
  try {
    const controller = typeof AbortController === 'function' ? new AbortController() : null;
    const timeoutId = controller ? setTimeout(() => controller.abort(), 9000) : null;
    const res = await fetch(`${websiteApiUrl}/api/cabinet/verify-link`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'user-agent': 'cxner-music-bot/1.0'
      },
      body: JSON.stringify({
        code: cleanCode,
        token: cleanToken,
        telegram_id: String(uid),
        username: clean(from?.username || ''),
        first_name: clean(from?.first_name || '')
      }),
      signal: controller ? controller.signal : undefined
    });
    if (timeoutId) clearTimeout(timeoutId);

    apiData = await res.json().catch(() => null);
    if (res.ok && apiData && apiData.ok) {
      apiSuccess = true;
      console.info(`[bot] verify-link API success for user_id=${apiData.user_id}`);
    } else {
      console.warn(`[bot] verify-link API response: status=${res.status}`, apiData);
    }
  } catch (apiErr) {
    console.warn('[bot] verify-link API call failed, falling back to direct Supabase:', apiErr?.message || apiErr);
  }

  // Step 2: Fallback to direct Supabase if website API did not succeed
  let dbSuccess = false;
  let dbArtistName = '';
  if (!apiSuccess && SUPABASE_SYNC_ENABLED) {
    try {
      const allCabinet = await supabaseSelectAll(SUPABASE_CABINET_TABLE, 'user_id,profile,updated_at', 'updated_at.desc').catch(() => []);
      const matchedUser = (allCabinet || []).find((u) => {
        const p = (u && typeof u.profile === 'object') ? u.profile : {};
        const matchesCode = p.link_code && (
          String(p.link_code).trim().toUpperCase() === cleanCode ||
          String(p.link_code).trim().toUpperCase() === raw.toUpperCase()
        );
        const matchesToken = p.link_token && (
          String(p.link_token).trim().toLowerCase() === cleanToken.toLowerCase() ||
          String(p.link_token).trim().toLowerCase() === raw.toLowerCase()
        );
        return matchesCode || matchesToken;
      });

      if (matchedUser) {
        const currentProfile = (matchedUser.profile && typeof matchedUser.profile === 'object') ? matchedUser.profile : {};
        const now = new Date().toISOString();

        if (currentProfile.link_created_at) {
          const createdAt = new Date(currentProfile.link_created_at).getTime();
          const diffMinutes = (Date.now() - createdAt) / (1000 * 60);
          if (diffMinutes > 15) {
            await sendText(
              chatId,
              '⌛ <b>Срок действия кода истёк (15 минут)</b>\n\n' +
              'Пожалуйста, откройте страницу кабинета на сайте и нажмите «Связать с ботом» заново:\n' +
              'https://cxrnermusic.vercel.app/dashboard',
              {
                reply_markup: {
                  inline_keyboard: [
                    [{ text: '🌐 Открыть кабинет на сайте', url: 'https://cxrnermusic.vercel.app/dashboard' }],
                    [{ text: '⬅️ Главное меню', callback_data: 'main' }]
                  ]
                }
              }
            );
            return;
          }
        }

        const updatedProfile = {
          ...currentProfile,
          telegram_id: String(uid),
          telegram_username: clean(from?.username || ''),
          telegram_first_name: clean(from?.first_name || ''),
          telegram_linked_at: now,
          link_code: null,
          link_token: null,
          link_created_at: null
        };
        dbArtistName = clean(currentProfile.artist_name || '');

        await supabaseRequest(
          `${SUPABASE_CABINET_TABLE}?user_id=eq.${encodeURIComponent(matchedUser.user_id)}`,
          {
            method: 'PATCH',
            headers: { Prefer: 'return=representation' },
            body: {
              profile: updatedProfile,
              updated_at: now
            }
          }
        );

        await supabaseRequest(
          `${SUPABASE_TABLE}?telegram_id=eq.${encodeURIComponent(String(uid))}`,
          {
            method: 'PATCH',
            body: { telegram_id: matchedUser.user_id }
          }
        ).catch(() => {});

        dbSuccess = true;
      }
    } catch (dbErr) {
      console.error('[bot] direct Supabase link failed:', dbErr);
    }
  }

  // Step 3: Handle Result
  if (apiSuccess || dbSuccess) {
    const artistName = apiData?.artist_name || dbArtistName;
    const nameLabel = artistName ? ` (<b>${esc(artistName)}</b>)` : '';
    const syncedCount = apiData?.synced_releases_count ?? '';

    // Clear any pending form session so user is in a fresh synchronized state
    try { resetFormSession(uid); } catch {}

    const confirmationText =
      '🎉 <b>Аккаунты успешно синхронизированы!</b>\n\n' +
      `Ваш профиль Telegram и личный кабинет на сайте CXRNER MUSIC${nameLabel} теперь объединены в единую систему.\n\n` +
      '• Все релизы, отправленные через Telegram или Mini App, синхронизированы с сайтом.\n' +
      '• Все релизы, загруженные на сайте (включая CXRNER LINK), доступны из вашего аккаунта.\n' +
      '• Статусы модерации и официальные коды UPC обновляются синхронно.\n' +
      (syncedCount ? `\n📦 <i>Синхронизировано релизов: <b>${syncedCount}</b></i>\n` : '');

    try {
      await sendText(
        chatId,
        confirmationText,
        {
          reply_markup: {
            inline_keyboard: [
              [{ text: '🌐 Открыть кабинет на сайте', url: 'https://cxrnermusic.vercel.app/dashboard' }],
              [{ text: '📁 Мои релизы в боте', callback_data: 'my_releases' }],
              [{ text: '⬅️ Главное меню', callback_data: 'main' }]
            ]
          }
        }
      );
      console.info(`[bot] sync confirmation delivered to chatId=${chatId}`);
    } catch (sendErr) {
      console.warn(`[bot] rich sync message failed, sending plain fallback:`, sendErr?.message || sendErr);
      await sendTextPlain(
        chatId,
        '🎉 Аккаунты успешно синхронизированы!\n\nВаш профиль Telegram и личный кабинет на сайте CXRNER MUSIC теперь объединены.'
      ).catch(() => {});
    }
    return;
  }

  const errMsg = apiData?.error || 'Код привязки не найден или срок его действия истёк (15 минут).';
  try {
    await sendText(
      chatId,
      `❌ <b>Не удалось привязать аккаунт</b>\n\n` +
      `${esc(errMsg)}\n\n` +
      'Перейдите в личный кабинет на сайте, нажмите «Привязать Telegram» и отправьте свежий 6-значный код:\n' +
      'https://cxrnermusic.vercel.app/dashboard',
      {
        reply_markup: {
          inline_keyboard: [
            [{ text: '🌐 Открыть кабинет на сайте', url: 'https://cxrnermusic.vercel.app/dashboard' }],
            [{ text: '🔄 Попробовать снова', callback_data: 'cabinet_sync_site' }],
            [{ text: '⬅️ Главное меню', callback_data: 'main' }]
          ]
        }
      }
    );
  } catch (err) {
    await sendTextPlain(
      chatId,
      `❌ Не удалось привязать аккаунт: ${errMsg}\n\nПолучите новый код на сайте: https://cxrnermusic.vercel.app/dashboard`
    ).catch(() => {});
  }
}

function keyboardCabinet() {
  return { inline_keyboard: [
    ...(hasValidWebAppUrl() ? [[{ text: '📱 Открыть Mini App', web_app: { url: WEBAPP_URL }, style: 'primary' }]] : []),
    [
      { text: '📁 Мои релизы', callback_data: 'my_releases' },
      { text: '🔗 Привязать сайт', callback_data: 'cabinet_sync_site', style: 'primary' }
    ],
    [{ text: '⬅️ Главное меню', callback_data: 'main' }]
  ]};
}
function keyboardCommunity() {
  return { inline_keyboard: [
    [{ text: '📣 Канал CXRNER MUSIC', url: 'https://t.me/cxrnermusic', style: 'primary' }],
    [
      { text: '💬 Чат артистов', url: 'https://t.me/+oVmX3_dkyWJhNjJi' },
      { text: '🌐 Сайт', url: WEBAPP_URL || 'https://cxrnermusic.vercel.app' }
    ],
    [{ text: '⬅️ Главное меню', callback_data: 'main' }]
  ]};
}
function shouldStatusStayPinned(status) {
  return canonicalStatus(status || STATUS.ON_UPLOAD) === STATUS.ON_UPLOAD;
}

const MOD_REASON_PRESETS = {
  reject: {
    cov: { label: '🖼 Обложка', text: 'Обложка не соответствует требованиям площадок (размер, качество или текст).' },
    snd: { label: '🎧 Звук / микс', text: 'Технические проблемы с аудиофайлом, клиппинг или низкое качество мастера.' },
    rig: { label: '©️ Права / сэмплы', text: 'Не подтверждены авторские права или обнаружены неочищенные сэмплы/биты.' },
    meta: { label: '📝 Метаданные', text: 'Ошибка в метаданных, ФИО, нике артиста или несоответствие требованиям DSP.' }
  },
  needfix: {
    cov: { label: '🖼 Обложка 3000×3000', text: 'Замените обложку: нужен квадрат 3000×3000 JPG/PNG без рамок и лишнего текста.' },
    meta: { label: '📝 Ник / ФИО / текст', text: 'Уточните или поправьте метаданные: название трека, ник или ФИО исполнителя.' },
    wav: { label: '🔗 Файлы / WAV', text: 'Обновите ссылку на аудиофайл (нужен чистый WAV 16/24-bit 44.1 kHz) и обложку.' },
    date: { label: '📅 Дата релиза', text: 'Сдвиньте дату релиза вперёд (минимум за 3 дня для сингла и 7 дней для альбома).' }
  }
};

function moderationReplyPromptKeyboard(kind, uid, idx) {
  const presets = MOD_REASON_PRESETS[kind] || {};
  const tag = kind === 'reject' ? 'r' : 'f';
  const style = kind === 'reject' ? 'danger' : 'primary';
  const entries = Object.entries(presets);
  const rows = [];
  for (let i = 0; i < entries.length; i += 2) {
    const chunk = entries.slice(i, i + 2).map(([code, item]) => ({
      text: item.label,
      callback_data: `m_pr_${tag}_${code}_${uid}_${idx}`,
      style
    }));
    rows.push(chunk);
  }
  rows.push([{ text: '✖️ Отменить ввод', callback_data: `m_pc_${uid}_${idx}` }]);
  return { inline_keyboard: rows };
}

function moderationKeyboard(uid, idx, rel = null) {
  const isPinned = rel && typeof rel === 'object'
    ? (rel.moderation_pinned !== undefined ? !!rel.moderation_pinned : shouldStatusStayPinned(rel.status))
    : true;
  const canon = rel && typeof rel === 'object' ? canonicalStatus(rel.status) : STATUS.ON_UPLOAD;
  const mark = (st, label) => (canon === st ? `${label} •` : label);
  return { inline_keyboard: [
    [
      { text: mark(STATUS.APPROVED, '✅ Принять'), callback_data: `m_approve_${uid}_${idx}`, style: 'success' },
      { text: mark(STATUS.REJECTED, '❌ Отклонить'), callback_data: `m_reject_${uid}_${idx}`, style: 'danger' }
    ],
    [
      { text: mark(STATUS.ON_UPLOAD, '🕓 На отгрузку'), callback_data: `m_upload_${uid}_${idx}`, style: 'primary' },
      { text: mark(STATUS.MODERATION, '🧠 В работу'), callback_data: `m_moderate_${uid}_${idx}` }
    ],
    [
      { text: mark(STATUS.NEEDS_FIX, '✏️ Правки'), callback_data: `m_needfix_${uid}_${idx}` },
      { text: clean(rel?.upc) ? '📦 Сменить UPC' : '📦 Задать UPC', callback_data: `m_upc_${uid}_${idx}`, style: clean(rel?.upc) ? undefined : 'primary' }
    ],
    [
      isPinned
        ? { text: '📌 Открепить', callback_data: `m_unpin_${uid}_${idx}` }
        : { text: '📌 Закрепить', callback_data: `m_pin_${uid}_${idx}` },
      { text: mark(STATUS.DELETED, '🗑 Удалить'), callback_data: `m_delete_${uid}_${idx}`, style: 'danger' }
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
  const basename = path.basename(abs);

  const retries = Math.max(0, Number(TG_FETCH_RETRIES) || 0);
  let lastErr = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const payload = fs.readFileSync(abs);
    const form = new FormData();
    form.append('chat_id', String(chatId));
    if (caption) form.append('caption', caption);
    form.append('document', new Blob([payload]), basename);

    const controller = typeof AbortController === 'function' ? new AbortController() : null;
    let timeoutRef = null;
    if (controller) {
      timeoutRef = setTimeout(() => controller.abort(), Math.max(10000, TG_FETCH_TIMEOUT_MS));
    }
    try {
      const response = await fetch(`${API}/sendDocument`, {
        method: 'POST',
        body: form,
        signal: controller ? controller.signal : undefined
      });
      if (timeoutRef) clearTimeout(timeoutRef);
      const result = await response.json().catch(() => ({}));
      if (!response.ok || !result.ok) {
        throw new Error(`[sendDocument] ${result.description || response.statusText}`);
      }
      return result.result;
    } catch (err) {
      if (timeoutRef) clearTimeout(timeoutRef);
      lastErr = err;
      const meta = classifyFetchError(err);
      const canRetry = attempt < retries && (meta.isNetwork || meta.isAbort || meta.isFetchFailed);
      if (canRetry) {
        await delay(Math.max(200, TG_FETCH_RETRY_DELAY_MS) * (attempt + 1));
        continue;
      }
      throw new Error(`[sendDocument] ${clean(err?.message || err)}`);
    }
  }
  throw lastErr || new Error('[sendDocument] unknown error');
}
function welcomeText() {
  return (
    '⚡ <b>CXRNER MUSIC</b>\n' +
    '<i>Экосистема дистрибуции, модерации и релизов.</i>\n\n' +
    'Отправляй треки через Mini App или прямо в боте, отслеживай статусы модерации и получай UPC.'
  );
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
function normalizeImageUrl(value) {
  const url = clean(value);
  const drive = /drive\.google\.com\/file\/d\/([^/]+)/i.exec(url);
  return drive ? `https://drive.google.com/uc?export=view&id=${encodeURIComponent(drive[1])}` : url;
}
function formatCardUrl(rawUrl, maxLabel = 44) {
  const src = clean(rawUrl);
  if (!src || src === '.' || src === '-') return esc(src || '—');
  if (isHttpUrl(src)) {
    const label = src.replace(/^https?:\/\//i, '');
    return `<a href="${esc(src)}">${esc(truncateMiddle(label, maxLabel))}</a>`;
  }
  return esc(src);
}

function fmtForm(user, uid, r) {
  const u = user?.username ? `@${user.username}` : 'нет';
  const src = clean(r.source || '');
  let sourceLabel = '🤖 TELEGRAM';
  if (src === 'web') sourceLabel = '🌐 САЙТ';
  else if (src === 'mini_app') sourceLabel = '📱 MINI APP';

  const yandexRaw = clean(r.yandex || '');
  const yandexDisplay = yandexRaw === 'create_new_card'
    ? 'Создать новую карточку'
    : (yandexRaw && yandexRaw !== '.' ? formatCardUrl(yandexRaw, 42) : '—');

  const lines = [
    `🎵 <b>${esc(r.name || 'Без названия')}</b> <i>(${esc(r.type || 'сингл')} • ${esc(r.genre || '—')})</i>`,
    `${esc(sourceLabel)} • От: ${esc(u)} (<code>${esc(r.telegram_id || uid || '—')}</code>)`,
    '',
    `🎤 <b>Артист:</b> ${esc(r.nick || '—')} <i>(${esc(r.fio || '—')})</i>`,
    `📅 <b>Релиз:</b> ${esc(r.date || '—')} • <b>Версия:</b> ${esc(r.version || 'Оригинал')} • <b>Мат:</b> ${esc(r.mat || '—')}`,
    `🎧 <b>Файлы:</b> ${formatCardUrl(r.link)}`,
    `🟡 <b>Яндекс:</b> ${yandexDisplay}`,
    `📱 <b>Контакт:</b> ${esc(r.tg || '—')}`
  ];

  const extraFields = [];
  const sub = clean(r.subname || '');
  if (sub && sub !== '.' && sub !== '-') extraFields.push(`✨ <b>Саб-название:</b> ${esc(sub)}`);
  const promo = clean(r.promo || '');
  if (promo && promo !== '.' && promo !== '-') extraFields.push(`📢 <b>Промо:</b> ${esc(promo)}`);
  const comment = clean(r.comment || '');
  if (comment && comment !== '.' && comment !== '-') extraFields.push(`💬 <b>Комментарий:</b> ${esc(comment)}`);
  const tracklist = clean(r.tracklist || '');
  if (r.type === 'альбом' && tracklist && tracklist !== '.' && tracklist !== '-') {
    extraFields.push(`📋 <b>Tracklist:</b>\n${esc(tracklist)}`);
  }

  if (extraFields.length) {
    const joinedExtra = extraFields.join('\n');
    if (extraFields.length > 1 || joinedExtra.length > 90) {
      lines.push('', `<blockquote expandable>${joinedExtra}</blockquote>`);
    } else {
      lines.push('', joinedExtra);
    }
  }
  return lines.join('\n');
}
function moderationActorTag(rel) {
  if (!rel || typeof rel !== 'object') return '';
  const uname = clean(rel.last_actor_username || rel.moderator_username || '');
  if (uname) return uname.startsWith('@') ? uname : (uname.includes(' ') ? uname : `@${uname}`);
  const name = clean(rel.last_actor_name || '');
  if (name) return name;
  return '';
}
function withStatus(status, original, rel = null) {
  const canon = canonicalStatus(status);
  const actor = canon === STATUS.MODERATION ? moderationActorTag(rel) : '';
  const actorNote = actor ? ` • <i>ведёт ${esc(actor)}</i>` : '';
  return `${STATUS_EMOJI[canon] || '⏳'} <b>СТАТУС: ${esc(STATUS_TEXT[canon] || canon)}</b>${actorNote}\n\n${original || ''}`;
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
  if (type === 'needfix_comment') {
    const note = shortenForInteraction(ev?.note || ev?.reason, 100);
    return note ? `направил на правки: ${note}` : 'направил на правки';
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
      ? (fallbackUsername.includes(' ') ? fallbackUsername : (fallbackUsername.startsWith('@') ? fallbackUsername : `@${fallbackUsername}`))
      : `ID ${fallbackId}`;
    const when = formatInteractionTime(rel?.moderation_time || '');
    const statusLabel = statusText(rel?.status || STATUS.ON_UPLOAD);
    const line = when
      ? `<i>${esc(when)}</i> — ${esc(actor)}: ${esc(`статус: ${statusLabel}`)}`
      : `${esc(actor)}: ${esc(`статус: ${statusLabel}`)}`;
    return `👤 <b>Последнее:</b> ${line}`;
  }
  const tail = list.slice(-MODERATION_HISTORY_LIMIT).reverse();
  const formatLine = (ev) => {
    const when = formatInteractionTime(ev.at);
    const actor = interactionActorLabel(ev);
    const action = interactionActionLabel(ev);
    return when ? `<i>${esc(when)}</i> — ${esc(actor)}: ${esc(action)}` : `${esc(actor)}: ${esc(action)}`;
  };
  const latestLine = `👤 <b>Последнее:</b> ${formatLine(tail[0])}`;
  if (tail.length === 1) return latestLine;
  const older = tail.slice(1).map((ev) => `• ${formatLine(ev)}`).join('\n');
  return `${latestLine}\n<blockquote expandable>📋 История (${tail.length}):\n${older}</blockquote>`;
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
  const canon = canonicalStatus(rel.status || STATUS.ON_UPLOAD);
  const details = [];
  if (rel.upc) details.push(`📦 <b>UPC:</b> <code>${esc(rel.upc)}</code>`);
  if (canon === STATUS.REJECTED && rel.reject_reason) details.push(`❌ <b>Причина отказа:</b> ${esc(rel.reject_reason)}`);
  if (canon === STATUS.NEEDS_FIX && rel.moderator_comment) details.push(`✏️ <b>Правки:</b> ${esc(rel.moderator_comment)}`);
  if (canon === STATUS.ON_UPLOAD && rel.available_for_upload) {
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
  return withStatus(canon, body, rel);
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
function parseRetryAfterSec(err) {
  const m = /retry\s+after\s+(\d+)/i.exec(clean(err?.message || err));
  return m ? Number.parseInt(m[1], 10) : 0;
}
function isBenignUnpinError(err) {
  const s = clean(err?.message || err).toLowerCase();
  return (
    s.includes('message to unpin not found') ||
    s.includes('message not found') ||
    s.includes('message_id_invalid') ||
    s.includes('message is not pinned')
  );
}
function isMissingPinRightsError(err) {
  const s = clean(err?.message || err).toLowerCase();
  return (
    s.includes('not enough rights') ||
    s.includes('have no rights') ||
    s.includes('chat_admin_required') ||
    s.includes('right_forbidden') ||
    s.includes('can_pin_messages') ||
    s.includes('manage pinned')
  );
}
async function pinModerationMessage(messageId, opts = {}) {
  const mid = Number(messageId || 0);
  if (!mid) return { ok: false, error: 'missing_message_id', retryAfter: 0 };
  try {
    await tg('pinChatMessage', {
      chat_id: MOD_CHAT,
      message_id: mid,
      disable_notification: opts.disableNotification !== false
    }, { noRateLimitRetry: opts.silent === true });
    return { ok: true };
  } catch (e) {
    const errText = shortTgError(e);
    const retryAfter = parseRetryAfterSec(errText);
    if (isMissingPinRightsError(errText)) {
      moderationHealth.can_pin_messages = false;
    }
    if (!opts.silent) {
      console.warn(`[MODERATION] pin failed msg_id=${mid}: ${errText}`);
    }
    return { ok: false, error: errText, missingRights: isMissingPinRightsError(errText), retryAfter };
  }
}
async function unpinModerationMessage(messageId, opts = {}) {
  const mid = Number(messageId || 0);
  if (!mid) return { ok: false, error: 'missing_message_id', retryAfter: 0 };
  try {
    await tg('unpinChatMessage', {
      chat_id: MOD_CHAT,
      message_id: mid
    }, { noRateLimitRetry: opts.silent === true });
    return { ok: true };
  } catch (e) {
    const errText = shortTgError(e);
    const retryAfter = parseRetryAfterSec(errText);
    if (isBenignUnpinError(errText)) {
      return { ok: true, alreadyUnpinned: true };
    }
    if (isMissingPinRightsError(errText)) {
      moderationHealth.can_pin_messages = false;
    }
    if (!opts.silent) {
      console.warn(`[MODERATION] unpin failed msg_id=${mid}: ${errText}`);
    }
    return { ok: false, error: errText, missingRights: isMissingPinRightsError(errText), retryAfter };
  }
}
const pendingPinRetryTimers = new Map();
let autoPinSweepRunning = false;

function clearScheduledModerationPinRetry(uid, idx, messageId = 0) {
  const prefix = `${String(uid)}:${Number(idx)}:`;
  const exact = `${prefix}${Number(messageId || 0)}`;
  if (pendingPinRetryTimers.has(exact)) {
    clearTimeout(pendingPinRetryTimers.get(exact));
    pendingPinRetryTimers.delete(exact);
  }
  for (const [k, timer] of pendingPinRetryTimers.entries()) {
    if (k.startsWith(prefix)) {
      clearTimeout(timer);
      pendingPinRetryTimers.delete(k);
    }
  }
}

async function pinModerationMessageNow(messageId, opts = {}) {
  let res = await pinModerationMessage(messageId, opts);
  if (!res.ok && !res.missingRights && opts.retryOnRateLimit !== false) {
    if (res.retryAfter > 0 && res.retryAfter <= 12) {
      await delay((res.retryAfter + 1) * 1000);
      res = await pinModerationMessage(messageId, opts);
    } else if (opts.immediateRetry) {
      await delay(1200);
      res = await pinModerationMessage(messageId, opts);
    }
  }
  return res;
}

function scheduleModerationPinRetry(uid, idx, messageId, delaySec = 6, attempt = 1) {
  const mid = Number(messageId || 0);
  if (!mid || attempt > 4) return;
  const key = `${String(uid)}:${Number(idx)}:${mid}`;
  if (pendingPinRetryTimers.has(key)) {
    clearTimeout(pendingPinRetryTimers.get(key));
    pendingPinRetryTimers.delete(key);
  }
  const waitMs = Math.max(2000, Math.min(60000, Number(delaySec || 6) * 1000));
  const timer = setTimeout(async () => {
    pendingPinRetryTimers.delete(key);
    try {
      const ref = resolveReleaseRef(uid, idx, mid);
      const rel = ref?.rel;
      if (!rel || rel.user_deleted || rel.manual_unpinned) return;
      if (!shouldStatusStayPinned(rel.status)) {
        rel.pin_pending = false;
        return;
      }
      if (rel.moderation_pinned) {
        rel.pin_pending = false;
        return;
      }
      const targetMid = Number(rel.moderation_message_id || mid || 0);
      if (!targetMid) return;
      const res = await pinModerationMessage(targetMid, { silent: true });
      if (res.ok) {
        rel.moderation_pinned = true;
        rel.pin_pending = false;
        rel.manual_unpinned = false;
        saveDb();
        syncModerationMirror(ref.uid, ref.idx, rel);
        saveModDb();
        await refreshModerationMessage(ref.uid, ref.idx, rel, targetMid).catch(() => {});
        await supabasePatchFormByRelease(ref.uid, rel, { form_payload: safeJson(rel) || {} }).catch(() => {});
        console.info(`[MODERATION] delayed auto-pin succeeded: user_id=${ref.uid} idx=${ref.idx} msg_id=${targetMid}`);
        return;
      }
      if (res.missingRights) {
        rel.pin_pending = false;
        saveDb();
        return;
      }
      const nextDelay = res.retryAfter > 0 ? Math.min(60, res.retryAfter + 2) : Math.min(45, attempt * 10);
      scheduleModerationPinRetry(ref.uid, ref.idx, targetMid, nextDelay, attempt + 1);
    } catch {}
  }, waitMs);
  if (typeof timer?.unref === 'function') timer.unref();
  pendingPinRetryTimers.set(key, timer);
}

function findExistingReleaseByFormOrSubmission(uid, formId = '', submissionKey = '', moderationMsgId = 0) {
  const cleanFormId = clean(formId);
  const cleanSub = clean(submissionKey);
  const targetMid = Number(moderationMsgId || 0);
  const tryScanList = (ownerUid, list) => {
    for (let idx = 0; idx < list.length; idx += 1) {
      const rel = list[idx];
      if (!rel || typeof rel !== 'object') continue;
      if (cleanFormId && clean(rel.supabase_form_id) === cleanFormId) return { uid: ownerUid, idx, rel };
      if (cleanSub && clean(rel.submission_time) === cleanSub && (!uid || String(ownerUid) === String(uid))) {
        return { uid: ownerUid, idx, rel };
      }
      if (targetMid && Number(rel.moderation_message_id || 0) === targetMid) return { uid: ownerUid, idx, rel };
    }
    return null;
  };
  if (uid && Array.isArray(db?.[uid])) {
    const hit = tryScanList(String(uid), db[uid]);
    if (hit) return hit;
  }
  if (cleanFormId || targetMid) {
    for (const [ownerUid, listRaw] of Object.entries(db || {})) {
      if (!Array.isArray(listRaw)) continue;
      const hit = tryScanList(String(ownerUid), listRaw);
      if (hit) return hit;
    }
  }
  return null;
}

async function reconcilePendingWebFormsAndPins() {
  if (stopping || autoPinSweepRunning) return;
  autoPinSweepRunning = true;
  try {
    // 1. Retry pinning any recent / pending local ON_UPLOAD cards that aren't pinned yet
    let localChanged = false;
    for (const [ownerId, listRaw] of Object.entries(db || {})) {
      const list = Array.isArray(listRaw) ? listRaw : [];
      for (let idx = 0; idx < list.length; idx += 1) {
        const rel = list[idx];
        if (!rel || typeof rel !== 'object' || rel.user_deleted || rel.manual_unpinned) continue;
        if (!shouldStatusStayPinned(rel.status)) continue;
        const mid = Number(rel.moderation_message_id || 0);
        if (!mid || rel.moderation_pinned) continue;
        const isRecent = parseIsoTime(rel.submission_time || '') >= BOT_STARTED_AT_MS;
        if (!rel.pin_pending && !isRecent) continue;

        const pRes = await pinModerationMessage(mid, { silent: true });
        if (pRes.ok) {
          rel.moderation_pinned = true;
          rel.pin_pending = false;
          localChanged = true;
          syncModerationMirror(ownerId, idx, rel);
          await refreshModerationMessage(ownerId, idx, rel, mid).catch(() => {});
          await supabasePatchFormByRelease(ownerId, rel, { form_payload: safeJson(rel) || {} }).catch(() => {});
        } else if (pRes.missingRights || pRes.retryAfter > 0) {
          if (pRes.missingRights) {
            rel.pin_pending = false;
            localChanged = true;
          }
          break;
        }
      }
    }
    if (localChanged) {
      saveDb();
      saveModDb();
    }

    // 2. Catch any brand-new website forms (created after bot boot) that need posting or pinning
    if (SUPABASE_SYNC_ENABLED && supabaseFeatureState.forms) {
      const rows = await supabaseSelectWhere(
        SUPABASE_FORMS_TABLE,
        'id,telegram_id,username,artist_name,track_name,genre,release_type,status,upc,moderation_message_id,submission_key,source,created_at,updated_at,form_payload',
        [
          { key: 'status', value: FORM_STATUS.PENDING },
          { key: 'created_at', op: 'gte', value: BOT_STARTED_AT }
        ],
        'created_at.asc',
        25
      ).catch(() => []);

      for (const row of rows || []) {
        const uid = clean(row?.telegram_id || '');
        if (!uid) continue;
        const formId = clean(row?.id || '');
        const msgId = Number(row?.moderation_message_id || 0);
        const payloadForm = row?.form_payload && typeof row.form_payload === 'object' ? row.form_payload : {};
        const existing = findExistingReleaseByFormOrSubmission(uid, formId, row?.submission_key || '', msgId);

        if (!existing && !msgId) {
          const releaseData = normalizeRelease({
            ...payloadForm,
            type: normalizeType(payloadForm.type || row.release_type) || 'сингл',
            name: clean(payloadForm.name || row.track_name || ''),
            nick: clean(payloadForm.nick || row.artist_name || ''),
            genre: clean(payloadForm.genre || row.genre || ''),
            tg: clean(payloadForm.tg || payloadForm.telegram_contact || ''),
            status: STATUS.ON_UPLOAD,
            submission_time: clean(row.submission_key || row.created_at || payloadForm.submission_time || ''),
            source: clean(row.source || payloadForm.source || 'web'),
            username: clean(row.username || payloadForm.username || '')
          });
          const fakeUser = { id: Number(uid) || 0, username: releaseData.username || releaseData.tg || '' };
          try {
            await submitReleaseToModeration(fakeUser, uid, releaseData, releaseData.source || 'web', {
              existing_form_id: formId
            });
          } catch (e) {
            console.warn(`[MODERATION] web sync submit warning form_id=${formId}:`, clean(e?.message || e));
          }
          continue;
        }

        if (msgId > 0 && !payloadForm.manual_unpinned) {
          const alreadyPinnedInDb = Boolean(existing?.rel?.moderation_pinned);
          const alreadyPinnedInPayload = Boolean(payloadForm.moderation_pinned);
          if (existing?.rel?.manual_unpinned) continue;
          if (!alreadyPinnedInDb && !alreadyPinnedInPayload) {
            const pRes = await pinModerationMessage(msgId, { silent: true });
            if (pRes.ok) {
              if (existing?.rel) {
                existing.rel.moderation_pinned = true;
                existing.rel.pin_pending = false;
                saveDb();
                syncModerationMirror(existing.uid, existing.idx, existing.rel);
                saveModDb();
                await refreshModerationMessage(existing.uid, existing.idx, existing.rel, msgId).catch(() => {});
              }
              await supabasePatchFormByRelease(uid, { ...(existing?.rel || payloadForm), supabase_form_id: formId }, {
                moderation_message_id: msgId,
                form_payload: safeJson({ ...(existing?.rel || payloadForm), moderation_pinned: true, manual_unpinned: false }) || {}
              }).catch(() => {});
              console.info(`[MODERATION] pinned web fallback form_id=${formId} msg_id=${msgId}`);
            } else if (pRes.missingRights || pRes.retryAfter > 0) {
              break;
            }
          }
        }
      }
    }
  } finally {
    autoPinSweepRunning = false;
  }
}

async function syncModerationPinForRelease(rel, fallbackMessageId = 0, opts = {}) {
  const mid = Number(rel?.moderation_message_id || fallbackMessageId || 0);
  if (!mid) return { ok: false, skipped: true };
  const canon = canonicalStatus(rel?.status || STATUS.ON_UPLOAD);
  if (!shouldStatusStayPinned(canon)) {
    const res = await unpinModerationMessage(mid, opts);
    if (res.ok && rel && typeof rel === 'object') {
      rel.moderation_pinned = false;
      rel.pin_pending = false;
    }
    return res;
  }
  if (opts.pinIfOnUpload || (rel?.pin_pending && !rel?.manual_unpinned)) {
    const res = await pinModerationMessageNow(mid, opts);
    if (res.ok && rel && typeof rel === 'object') {
      rel.moderation_pinned = true;
      rel.pin_pending = false;
      rel.manual_unpinned = false;
    } else if (rel && typeof rel === 'object' && !res.missingRights) {
      rel.moderation_pinned = false;
      rel.pin_pending = true;
    }
    return res;
  }
  return { ok: true, skipped: true };
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
      reply_markup: moderationKeyboard(uid, idx, rel)
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
  const fixMatch = /^(?:fix|правк[аи]?|коммент(?:арий)?|comment)\s*[:\-]\s*(.+)$/i.exec(src);
  if (fixMatch) return { type: 'needfix_comment', value: clean(fixMatch[1]) };
  return null;
}
async function notifyArtistAboutRelease(uid, rel, status) {
  // Web submissions use a Supabase Auth UUID, not a numeric Telegram chat ID.
  // Their status is synchronized through Supabase; only Telegram users can be DM'd.
  if (!/^\d{4,20}$/.test(clean(uid))) return;
  try {
    const canon = canonicalStatus(status);
    let note = `${statusEmoji(canon)} <b>${esc(statusText(canon))}</b>\n\n`;
    note += `🎵 <b>${esc(rel.name || 'Релиз')}</b>\n👤 ${esc(rel.nick || '—')}\n📅 ${esc(rel.date || '—')}`;
    if (rel.upc) note += `\n📦 UPC: <code>${esc(rel.upc)}</code>`;
    if (canon === STATUS.REJECTED && rel.reject_reason) note += `\n❌ Причина: ${esc(rel.reject_reason)}`;
    if (canon === STATUS.NEEDS_FIX && rel.moderator_comment) note += `\n✏️ Правки: ${esc(rel.moderator_comment)}`;

    const rows = [];
    if (hasValidWebAppUrl()) {
      rows.push([{ text: '📱 Открыть Mini App', web_app: { url: WEBAPP_URL }, style: 'primary' }]);
    }
    if (canon === STATUS.REJECTED || canon === STATUS.NEEDS_FIX) {
      rows.push([
        { text: '🔁 Отправить заново', callback_data: 'report_text', style: 'primary' },
        { text: '📁 Мои релизы', callback_data: 'my_releases' }
      ]);
    } else {
      rows.push([{ text: '📁 Мои релизы', callback_data: 'my_releases' }]);
    }
    await sendText(Number(uid), note, { reply_markup: { inline_keyboard: rows } });
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
  const prevModeratorId = clean(rel.moderator || '');
  const prevModeratorUsername = clean(rel.moderator_username || '');
  const canonStatus = canonicalStatus(status);
  const hadDifferentModerator = Boolean(
    prevStatus === STATUS.MODERATION &&
    prevModeratorId &&
    String(moderator?.id || '') !== prevModeratorId &&
    canonStatus !== prevStatus
  );

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
  if (canonStatus === STATUS.NEEDS_FIX) {
    const fixComment = clean(opts.moderatorComment || rel.moderator_comment || 'Нужны правки перед публикацией');
    rel.moderator_comment = fixComment;
  }
  if (canonStatus !== STATUS.NEEDS_FIX && opts.clearModeratorComment) {
    rel.moderator_comment = '';
  }
  if (canonStatus === STATUS.DELETED) rel.user_deleted = true;
  if (canonStatus !== STATUS.DELETED && opts.restoreFromDelete) rel.user_deleted = false;
  if (canonStatus === STATUS.ON_UPLOAD) {
    rel.available_for_upload = true;
    rel.manual_unpinned = false;
    rel.available_marked_at = rel.moderation_time;
    rel.available_marked_by = String(moderator?.id || '');
    rel.available_marked_by_username = clean(moderator?.username || '');
    rel.available_marked_by_name = clean([moderator?.first_name || '', moderator?.last_name || ''].filter(Boolean).join(' '));
  } else {
    rel.available_for_upload = false;
    rel.pin_pending = false;
    clearScheduledModerationPinRetry(uid, idx, rel.moderation_message_id);
  }

  const statusChanged = prevStatus !== canonStatus;
  const reasonChanged = canonStatus === STATUS.REJECTED && clean(opts.rejectReason || '') !== '';
  const commentChanged = canonStatus === STATUS.NEEDS_FIX && clean(opts.moderatorComment || '') !== '';
  if (statusChanged || reasonChanged || commentChanged) {
    pushReleaseInteraction(rel, 'status_change', moderator, {
      status_from: prevStatus,
      status_to: canonStatus,
      reason: canonStatus === STATUS.REJECTED
        ? rel.reject_reason
        : (canonStatus === STATUS.NEEDS_FIX ? rel.moderator_comment : ''),
      note: clean(opts.note || '')
    });
  }

  const pinResult = await syncModerationPinForRelease(rel, opts.fallbackMessageId, {
    pinIfOnUpload: canonStatus === STATUS.ON_UPLOAD && (statusChanged || !!opts.restorePin || (!rel.moderation_pinned && !rel.manual_unpinned))
  });
  if (canonStatus === STATUS.ON_UPLOAD && rel.pin_pending && !pinResult?.missingRights) {
    scheduleModerationPinRetry(
      uid,
      idx,
      Number(rel.moderation_message_id || opts.fallbackMessageId || 0),
      Math.max(5, (pinResult?.retryAfter || 4) + 1)
    );
  }

  saveDb();
  syncModerationMirror(uid, idx, rel);
  saveModDb();

  const formStatus = mapReleaseStatusToFormStatus(canonStatus);
  await supabasePatchFormByRelease(uid, rel, {
    status: formStatus,
    reject_reason: canonStatus === STATUS.REJECTED ? clean(rel.reject_reason || '') : '',
    moderator_comment: canonStatus === STATUS.NEEDS_FIX ? clean(rel.moderator_comment || '') : '',
    user_deleted: canonStatus === STATUS.DELETED,
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
    ` by=${clean(moderator?.username || moderator?.id || '-')}` +
    (pinResult && !pinResult.skipped ? ` pin=${pinResult.ok ? (shouldStatusStayPinned(canonStatus) ? 'pinned' : 'unpinned') : `err:${pinResult.error}`}` : '')
  );

  await refreshModerationMessage(uid, idx, rel, opts.fallbackMessageId);
  await notifyArtistAboutRelease(uid, rel, canonStatus);
  return {
    ok: true,
    rel,
    uid,
    idx,
    status: canonStatus,
    pin_result: pinResult,
    prev_moderator: hadDifferentModerator ? (prevModeratorUsername || prevModeratorId) : ''
  };
}
async function startModerationReplyFlow(query, type, uid, idx) {
  if (Number(query?.message?.chat?.id || 0) !== Number(MOD_CHAT)) {
    await safeAnswerCallback(query.id, { text: 'Эти кнопки работают только в группе модерации.', show_alert: true });
    return;
  }
  if (!(await canModerate(query.from?.id, query.message))) {
    await safeAnswerCallback(query.id, { text: 'Доступ только участникам группы модерации.', show_alert: true });
    return;
  }
  const ref = resolveReleaseRef(uid, idx, Number(query?.message?.message_id || 0));
  if (!ref || !ref.rel) {
    await safeAnswerCallback(query.id, { text: 'Релиз не найден.', show_alert: true });
    return;
  }
  uid = ref.uid;
  idx = ref.idx;
  const rel = ref.rel;
  const replyTo = Number(rel.moderation_message_id || query.message.message_id || 0);
  let text = '';
  let replyMarkup = null;
  if (type === 'upc') {
    text = '📦 Отправьте UPC код <b>ответом</b> на это сообщение (или на анкету релиза).';
    replyMarkup = { inline_keyboard: [[{ text: '✖️ Отменить ввод', callback_data: `m_pc_${uid}_${idx}` }]] };
  } else if (type === 'needfix_comment') {
    text = '✏️ Выберите шаблон правок ниже или отправьте комментарий <b>ответом</b> на это сообщение (или на анкету релиза).';
    replyMarkup = moderationReplyPromptKeyboard('needfix', uid, idx);
  } else {
    text = '❌ Выберите причину отклонения ниже или напишите свою <b>ответом</b> на это сообщение (или на анкету релиза).';
    replyMarkup = moderationReplyPromptKeyboard('reject', uid, idx);
  }
  const prompt = await replyInChat(query.message, text, {
    reply_to_message_id: replyTo || undefined,
    reply_markup: replyMarkup
  });
  rememberPendingAction({
    type,
    uid: String(uid),
    idx: Number(idx),
    moderator_id: String(query.from.id),
    release_message_id: replyTo,
    prompt_message_id: Number(prompt?.message_id || 0),
    created_at: new Date().toISOString()
  });
  const toast = type === 'upc'
    ? 'Жду UPC ответом на сообщение'
    : (type === 'needfix_comment' ? 'Выберите шаблон правок или ответьте текстом' : 'Выберите причину или ответьте текстом');
  await safeAnswerCallback(query.id, { text: toast });
}
async function handleModerationReplyMessage(msg) {
  if (Number(msg?.chat?.id || 0) !== Number(MOD_CHAT)) return false;
  const text = clean(msg.text);
  const replyToId = Number(msg.reply_to_message?.message_id || 0);
  if (!text || !replyToId) return false;
  if (!(await canModerate(msg.from?.id, msg))) return false;

  const pending = findPendingActionForReply(msg.from.id, replyToId);
  let actionType = '';
  let actionValue = text;
  let uid = '';
  let idx = 0;
  let rel = null;
  let pendingIndex = -1;
  let promptMessageId = 0;
  let fallbackMessageId = replyToId;

  if (pending) {
    const { index, action } = pending;
    pendingIndex = index;
    actionType = String(action.type || '');
    uid = String(action.uid || '');
    idx = Number(action.idx || 0);
    promptMessageId = Number(action.prompt_message_id || 0);
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
      if (promptMessageId) tryDeleteMessageSafe(msg.chat.id, promptMessageId);
      await replyInChat(msg, '❌ Релиз для этого действия не найден.');
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
      await replyInChat(msg, '❌ Неверный UPC. Допустимо 4-40 символов: A-Z, 0-9, ".", "-", "_".', {
        reply_to_message_id: msg.message_id
      });
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
    if (promptMessageId) tryDeleteMessageSafe(msg.chat.id, promptMessageId);
    await replyInChat(msg, `✅ UPC присвоен: <code>${esc(upc)}</code>`, {
      reply_to_message_id: Number(rel.moderation_message_id || fallbackMessageId || 0) || undefined
    });
    try {
      if (/^\d{4,20}$/.test(clean(uid))) {
        await sendText(Number(uid), `📦 <b>UPC присвоен</b>\n\n🎵 <b>${esc(rel.name || 'Релиз')}</b>\nUPC: <code>${esc(upc)}</code>`);
      }
    } catch (e) {
      console.error('[MODERATION] upc notify failed:', clean(e?.message || e));
    }
    return true;
  }

  if (actionType === 'needfix_comment') {
    const comment = clean(actionValue);
    if (comment.length < 3) {
      await replyInChat(msg, '❌ Комментарий по правкам слишком короткий. Укажите подробнее.', {
        reply_to_message_id: msg.message_id
      });
      return true;
    }
    if (pendingIndex >= 0) removePendingActionByIndex(pendingIndex);
    if (promptMessageId) tryDeleteMessageSafe(msg.chat.id, promptMessageId);
    const out = await applyReleaseStatus(uid, idx, STATUS.NEEDS_FIX, msg.from, {
      moderatorComment: comment,
      clearRejectReason: true,
      fallbackMessageId,
      note: 'Комментарий по правкам добавлен в модерации'
    });
    if (!out.ok) {
      await replyInChat(msg, `❌ ${out.error}`, { reply_to_message_id: msg.message_id });
      return true;
    }
    const pinNote = out.pin_result && !out.pin_result.ok && out.pin_result.missingRights
      ? '\n⚠️ Не удалось автоматически открепить сообщение: выдайте боту право «Закрепление сообщений» в настройках администраторов группы.'
      : '';
    await replyInChat(msg, `✏️ Статус «На исправление» обновлён, комментарий отправлен артисту.${pinNote}`, {
      reply_to_message_id: Number(rel.moderation_message_id || fallbackMessageId || 0) || undefined
    });
    return true;
  }

  if (actionType === 'reject_reason') {
    const reason = clean(actionValue);
    if (reason.length < 3) {
      await replyInChat(msg, '❌ Причина слишком короткая. Укажите подробнее.', {
        reply_to_message_id: msg.message_id
      });
      return true;
    }
    if (pendingIndex >= 0) removePendingActionByIndex(pendingIndex);
    if (promptMessageId) tryDeleteMessageSafe(msg.chat.id, promptMessageId);
    const out = await applyReleaseStatus(uid, idx, STATUS.REJECTED, msg.from, {
      rejectReason: reason,
      clearModeratorComment: true,
      fallbackMessageId,
      note: 'Причина отклонения добавлена ответом на анкету'
    });
    if (!out.ok) {
      await replyInChat(msg, `❌ ${out.error}`, { reply_to_message_id: msg.message_id });
      return true;
    }
    const pinNote = out.pin_result && !out.pin_result.ok && out.pin_result.missingRights
      ? '\n⚠️ Не удалось автоматически открепить сообщение: выдайте боту право «Закрепление сообщений» в настройках администраторов группы.'
      : '';
    await replyInChat(msg, `✅ Релиз отклонён, причина отправлена артисту.${pinNote}`, {
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
    cover_url: sanitizeText(form.cover_url || form.cover || '', 700),
    smart_link: sanitizeText(form.smart_link || form.multilink || form.beatlink || '', 700),
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
  if (!telegramId) errors.push('Поле «telegram_id» обязательно.');
  if (telegramId && !/^\d{4,20}$/.test(telegramId)) {
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
  const coverUrl = normalizeImageUrl(sanitizeText(form?.cover_url || form?.cover || '', 700));
  if (coverUrl && !isHttpUrl(coverUrl)) errors.push('Ссылка на обложку должна начинаться с http:// или https://.');
  const smartLink = sanitizeText(form?.smart_link || form?.multilink || form?.beatlink || '', 700);
  if (smartLink && !isHttpUrl(smartLink)) errors.push('Мультиссылка должна начинаться с http:// или https://.');
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
      cover_url: coverUrl,
      smart_link: smartLink,
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

async function submitReleaseToModeration(user, uid, releaseData, source = 'mini_app', options = {}) {
  await ensureModerationHealth(false);
  db[uid] = Array.isArray(db[uid]) ? db[uid] : [];
  const idx = db[uid].length;
  const submissionTime = clean(releaseData?.submission_time) || new Date().toISOString();
  const rel = {
    ...releaseData,
    release_idx: idx,
    status: STATUS.ON_UPLOAD,
    source,
    submission_time: submissionTime,
    manual_unpinned: false,
    pin_pending: false,
    username: user?.username || clean(releaseData?.username || '')
  };

  if (SUPABASE_SYNC_ENABLED) {
    const existingFormId = clean(options.existing_form_id || '');
    const formId = existingFormId || await supabaseInsertForm(uid, user, rel, FORM_STATUS.PENDING);
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
      reply_markup: moderationKeyboard(uid, idx, { ...rel, moderation_pinned: true })
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
        reply_markup: moderationKeyboard(uid, idx, { ...rel, moderation_pinned: true })
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
  const pinRes = await pinModerationMessageNow(sent.message_id, { immediateRetry: true });
  rel.moderation_pinned = !!pinRes.ok;
  rel.pin_pending = !pinRes.ok && !pinRes.missingRights;
  db[uid].push(rel);
  saveDb();

  if (!pinRes.ok) {
    await refreshModerationMessage(uid, idx, rel, sent.message_id).catch(() => {});
    if (rel.pin_pending) {
      scheduleModerationPinRetry(
        uid,
        idx,
        sent.message_id,
        Math.max(5, (pinRes.retryAfter || 4) + 1)
      );
    }
  }

  await supabasePatchFormByRelease(uid, rel, {
    status: mapReleaseStatusToFormStatus(rel.status),
    moderation_message_id: Number(sent.message_id || 0) || null,
    form_payload: safeJson(rel) || {}
  });

  console.info(
    `[WEBAPP] release accepted to moderation: source=${source} user_id=${uid} idx=${idx} msg_id=${Number(sent.message_id || 0)} pin=${pinRes.ok ? 'ok' : pinRes.error}`
  );

  ensureModDbShape();
  modDb.moderation_messages.push({ ...rel, user_id: uid, idx, message_id: sent.message_id });
  saveModDb();
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
      telegram_id: String(uid),
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
  if (s.step === 'subname') {
    await sendText(
      chatId,
      '✨ Саб-название (если нет, отправьте точку "."):\n<i>ℹ️ Саб-название — это дополнительная подпись к названию релиза. Пример: Remix, Slowed, Instrumental, Extended Mix.</i>'
    );
    return;
  }
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
  if (s.step === 'nick') {
    await sendText(
      chatId,
      '🎤 Ник исполнителя:\n<i>ℹ️ Это сценическое имя артиста, которое будет отображаться на стриминговых сервисах.</i>'
    );
    return;
  }
  if (s.step === 'fio') {
    await sendText(
      chatId,
      '🪪 ФИО исполнителя:\n<i>ℹ️ Укажите настоящее имя исполнителя. Это требуется для документов и авторских прав.</i>'
    );
    return;
  }
  if (s.step === 'date') {
    await sendText(
      chatId,
      '📅 Дата релиза в формате ДД.ММ.ГГГГ:\n<i>ℹ️ Укажите дату выхода релиза на площадках минимум за 4 дня.</i>'
    );
    return;
  }
  if (s.step === 'version') {
    await sendText(
      chatId,
      '🧩 Версия релиза (или "Оригинал"):\n<i>ℹ️ Если это обычная версия трека — напишите "-". Если другая версия: Remix, Slowed, Sped Up, Instrumental.</i>'
    );
    return;
  }
  if (s.step === 'genre') {
    await sendText(
      chatId,
      '🏷 Жанр:\n<i>ℹ️ Укажите основной жанр трека. Примеры: Phonk, Brazilian Funk, Hip-Hop, Trap, EDM.</i>'
    );
    return;
  }
  if (s.step === 'link') {
    await sendText(
      chatId,
      '🔗 Ссылка на файлы (http/https):\n<i>ℹ️ В ссылке на Яндекс/Google Диск должна быть папка со следующими файлами: 1) обложка релиза JPG 3000x3000; 2) трек WAV 44100Hz 16 или 24 bit; 3) скриншот(ы) проекта как доказательство авторства.</i>'
    );
    return;
  }
  if (s.step === 'yandex') {
    await sendText(
      chatId,
      '🟡 Ссылка на карточку артиста в Яндекс Музыке:\n<i>ℹ️ Отправьте ссылку на существующую карточку артиста. Если карточки нет — нажмите кнопку "Создать новую карточку".</i>\nОтправьте URL, точку "." или нажмите кнопку ниже.',
      {
      reply_markup: {
        inline_keyboard: [
          [{ text: '➕ Создать новую карточку', callback_data: 'form_yandex_new_card' }],
          [{ text: '✖️ Отмена', callback_data: 'form_cancel' }]
        ]
      }
    }
    );
    return;
  }
  if (s.step === 'mat') {
    await sendText(
      chatId,
      '⚠️ Есть ненормативная лексика?\n<i>ℹ️ Выберите "Да", если в тексте трека присутствует мат. Если трек чистый — выберите "Нет".</i>',
      {
      reply_markup: {
        inline_keyboard: [
          [
            { text: '✅ Да', callback_data: 'form_mat_yes' },
            { text: '👌 Нет', callback_data: 'form_mat_no' }
          ],
          [{ text: '✖️ Отмена', callback_data: 'form_cancel' }]
        ]
      }
    }
    );
    return;
  }
  if (s.step === 'promo') {
    await sendText(
      chatId,
      '📢 Промо-текст (или точка "."):\n<i>ℹ️ Это описание релиза для редакторов площадок. Кратко опишите стиль трека, атмосферу и для каких плейлистов он подходит.</i>'
    );
    return;
  }
  if (s.step === 'comment') {
    await sendText(
      chatId,
      '💬 Комментарий (или точка "."):\n<i>ℹ️ Дополнительная информация для модераторов. Можно оставить пустым.</i>'
    );
    return;
  }
  if (s.step === 'tracklist') { await sendText(chatId, '📋 Tracklist для альбома (обязательно):'); return; }
  if (s.step === 'tg') {
    await sendText(
      chatId,
      '📱 Контакт Telegram для связи (@username):\n<i>ℹ️ Укажите ваш Telegram username для связи с менеджером.</i>'
    );
    return;
  }
  if (s.step === 'confirm') {
    const preview = fmtForm(s.user, s.uid, s.form);
    await sendText(chatId, `🧾 <b>Проверьте анкету</b>\n\n${preview}`, {
      parse_mode: 'HTML'
    });
    const warningText =
      '⚠️ Перед отправкой релиза внимательно проверьте анкету. Отправляя релиз в модерацию, вы подтверждаете, что:\n' +
      '• все данные указаны корректно\n' +
      '• файлы соответствуют требованиям\n' +
      '• обложка соответствует правилам площадок\n' +
      '• у вас есть права на использование всех материалов релиза\n\n' +
      'Если данные указаны неверно или файлы не соответствуют требованиям:\n' +
      '• релиз может быть отклонён модерацией\n' +
      '• релиз может быть снят с отгрузки\n' +
      '• релиз может не выйти в указанную дату\n' +
      '• релиз может быть удалён до исправления ошибок\n\n' +
      'Команда CXRNER MUSIC не несёт ответственности за задержку выхода релиза, если анкета заполнена неверно или файлы не соответствуют требованиям.\n' +
      'Отправляя релиз, вы подтверждаете согласие с правилами загрузки и пользовательским соглашением.';

    await sendText(chatId, warningText, {
      reply_markup: {
        inline_keyboard: [
          [{ text: '✅ Подтвердить и отправить', callback_data: 'form_confirm_send' }],
          [{ text: '❌ Отмена', callback_data: 'form_cancel' }]
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
  if (Number(msg?.chat?.id || 0) === Number(MOD_CHAT)) return false;
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

  if (data === 'form_send' || data === 'form_confirm_send') {
    s.form.telegram_id = String(uid);
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
  if (Number(msg?.chat?.id || 0) === Number(MOD_CHAT)) return false;
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
      await pinModerationMessage(sent.message_id);
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
  if (Number(msg?.chat?.id || 0) === Number(MOD_CHAT)) return false;
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
  let text = '🎵 Mini App открывается кнопкой ниже или через системную кнопку «Приложение» внизу чата.';
  const extra = {};
  if (hasValidWebAppUrl()) {
    text += `\n\nСсылка: ${esc(WEBAPP_URL)}`;
    extra.reply_markup = {
      inline_keyboard: [
        [{ text: '🚀 Открыть Mini App', web_app: { url: WEBAPP_URL }, style: 'primary' }],
        [{ text: '⬅️ Главное меню', callback_data: 'main' }]
      ]
    };
  }
  await sendText(chatId, text, extra);
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
          ...(hasValidWebAppUrl() ? [[{ text: '📱 Открыть Mini App', web_app: { url: WEBAPP_URL }, style: 'primary' }]] : []),
          [
            { text: '➕ Новый', callback_data: 'report_text', style: 'primary' },
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
  if (status === STATUS.NEEDS_FIX && clean(rel?.moderator_comment)) {
    text += `✏️ Правки: ${esc(rel.moderator_comment)}\n`;
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
    { text: '🗑 Удалить', callback_data: `delete_release_${uid}_${current.idx}_${page}`, style: 'danger' }
  ]);
  rows.push([
    { text: '➕ Новый', callback_data: 'report_text', style: 'primary' },
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
        { text: '📊 Статистика', callback_data: 'admin_stats', style: 'primary' },
        { text: '⏳ Ожидают', callback_data: 'admin_pending', style: 'primary' }
      ],
      [
        { text: '🔄 Очистка', callback_data: 'admin_cleanup' },
        { text: '📢 Рассылка', callback_data: 'admin_broadcast', style: 'primary' }
      ],
      [
        { text: '📋 Все релизы', callback_data: 'admin_all_releases' },
        { text: '⚠️ УДАЛИТЬ ВСЁ', callback_data: 'admin_cleanbase_confirm', style: 'danger' }
      ],
      [{ text: '🔁 Обновить', callback_data: 'admin_back' }]
    ]
  };
}

function statsPeriodKeyboard(showBackToAdmin = true) {
  const rows = [
    [{ text: '📅 Неделя', callback_data: 'stats_period_week', style: 'primary' }],
    [{ text: '📅 Месяц', callback_data: 'stats_period_month', style: 'primary' }],
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

async function sendAdminPanel(target) {
  if (target && typeof target === 'object' && target.chat?.id) {
    await replyInChat(target, buildAdminPanelText(), { reply_markup: adminPanelKeyboard() });
    return;
  }
  await sendText(target, buildAdminPanelText(), { reply_markup: adminPanelKeyboard() });
}

async function sendStatsPicker(target) {
  if (target && typeof target === 'object' && target.chat?.id) {
    await replyInChat(target, '📊 Выберите период для статистики:', { reply_markup: statsPeriodKeyboard(true) });
    return;
  }
  await sendText(target, '📊 Выберите период для статистики:', { reply_markup: statsPeriodKeyboard(true) });
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
      moderationHealth.can_pin_messages = null;
      console.error(`[bot] moderation chat access failed: ${moderationHealth.reason}`);
      return false;
    }

    const desc = describeBotMemberStatus(member);
    moderationHealth.ok = !!desc.ok;
    moderationHealth.reason = desc.reason || '';
    moderationHealth.bot_status = desc.status || '';
    moderationHealth.can_send_messages = desc.can_send_messages;
    moderationHealth.can_pin_messages = desc.can_pin_messages;

    if (moderationHealth.ok) {
      console.info(
        `[bot] moderation chat access: ok (${title})` +
        ` status=${desc.status || '-'} can_pin=${desc.can_pin_messages === null ? 'unknown' : desc.can_pin_messages}` +
        ` thread=${MODERATION_THREAD_ID > 0 ? MODERATION_THREAD_ID : 'default'}`
      );
      if (desc.can_pin_messages === false) {
        console.warn('[bot] moderation chat warning: bot lacks can_pin_messages right — auto-pin/unpin will fail until enabled in group admins.');
      }
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
    moderationHealth.can_pin_messages = null;
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

  const shouldRequireInitData = ['webapp_release_submit', 'submit_release', 'cabinet_activate', 'cabinet_sync_request'].includes(action);
  if (shouldRequireInitData && !clean(parsed.init_data || '')) {
    await sendText(msg.chat.id, '❌ Ошибка безопасности: Mini App initData не передан.');
    return;
  }

  const initDataCheck = verifyTelegramInitData(parsed.init_data || '', uid);
  if (parsed.init_data && !initDataCheck.ok) {
    console.error(`[WEBAPP] initData validation failed: user_id=${uid} reason=${initDataCheck.reason}`);
    await sendText(msg.chat.id, `❌ Ошибка проверки Mini App: ${esc(initDataCheck.reason)}.`);
    return;
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

  if (!parsed.telegram_id) parsed.telegram_id = uid;
  if (!parsed.form.telegram_id) parsed.form.telegram_id = uid;
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

async function canModerate(userId, msg = null) {
  if (msg?.sender_chat?.id && Number(msg.sender_chat.id) === Number(MOD_CHAT)) {
    return true;
  }
  if (isAdmin(userId)) {
    return true;
  }
  try {
    const m = await tg('getChatMember', { chat_id: MOD_CHAT, user_id: Number(userId) });
    const s = clean(m?.status).toLowerCase();
    return !['left', 'kicked'].includes(s);
  } catch { return false; }
}

function parseBotCommand(rawText, botUsername = '') {
  let s = clean(rawText);
  if (!s) return null;
  const known = clean(botUsername).replace(/^@/, '').toLowerCase();
  // Support "@botname /command ..." as well as "/command@botname ..."
  const leadMention = /^@([A-Za-z0-9_]{3,})\s+([\s\S]+)$/.exec(s);
  if (leadMention) {
    const targetBot = leadMention[1].toLowerCase();
    if (known && targetBot !== known) return null;
    s = clean(leadMention[2]);
  }
  const cmdMatch = /^(\/[A-Za-z0-9_]+)(?:@([A-Za-z0-9_]{3,}))?(?:\s+([\s\S]+))?$/.exec(s);
  if (!cmdMatch) return null;
  if (cmdMatch[2] && known && cmdMatch[2].toLowerCase() !== known) return null;
  return {
    command: cmdMatch[1].toLowerCase(),
    arg: clean(cmdMatch[3] || '')
  };
}

async function replyInChat(msg, text, extra = {}) {
  const chatId = msg?.chat?.id;
  const threadId = Number(msg?.message_thread_id || 0);
  const payload = { ...extra };
  if (threadId > 0 && payload.message_thread_id === undefined) {
    payload.message_thread_id = threadId;
  }
  const attemptSend = async (body) => {
    try {
      return await sendText(chatId, text, body);
    } catch (err) {
      const errStr = clean(err?.message || err).toLowerCase();
      if (body.reply_to_message_id && (errStr.includes('replied message not found') || errStr.includes('reply_to_message'))) {
        const nextBody = { ...body };
        delete nextBody.reply_to_message_id;
        return await attemptSend(nextBody);
      }
      if (body.message_thread_id && errStr.includes('message thread not found')) {
        const nextBody = { ...body };
        delete nextBody.message_thread_id;
        return await attemptSend(nextBody);
      }
      const waitSec = parseRetryAfterSec(err);
      if (waitSec > 0 && waitSec <= 45) {
        await delay((waitSec + 1) * 1000);
        return await sendText(chatId, text, body);
      }
      throw err;
    }
  };
  return attemptSend(payload);
}

async function updateStatusReport(statusMsg, msg, text) {
  const chatId = msg?.chat?.id;
  if (statusMsg?.message_id) {
    try {
      await tg('editMessageText', {
        chat_id: chatId,
        message_id: statusMsg.message_id,
        text,
        parse_mode: 'HTML',
        disable_web_page_preview: true
      });
      return;
    } catch (e) {
      const errText = clean(e?.message || e);
      if (errText.includes('message is not modified')) return;
      const waitSec = parseRetryAfterSec(errText);
      if (waitSec > 0 && waitSec <= 45) {
        await delay((waitSec + 1) * 1000);
        try {
          await tg('editMessageText', {
            chat_id: chatId,
            message_id: statusMsg.message_id,
            text,
            parse_mode: 'HTML',
            disable_web_page_preview: true
          });
          return;
        } catch {}
      }
    }
  }
  await replyInChat(msg, text, { reply_to_message_id: msg?.message_id }).catch(() => {});
}
async function applyModeration(query, action, uid, idx) {
  if (Number(query?.message?.chat?.id || 0) !== Number(MOD_CHAT)) {
    await safeAnswerCallback(query.id, { text: 'Эти кнопки работают только в группе модерации.', show_alert: true });
    return;
  }
  if (!(await canModerate(query.from?.id, query.message))) {
    await safeAnswerCallback(query.id, { text: 'Доступ только участникам группы модерации.', show_alert: true });
    return;
  }
  const ref = resolveReleaseRef(uid, idx, Number(query?.message?.message_id || 0));
  if (!ref || !ref.rel) {
    await safeAnswerCallback(query.id, { text: 'Релиз не найден.', show_alert: true });
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
    await safeAnswerCallback(query.id, { text: 'Неизвестное действие модерации.', show_alert: true });
    return;
  }
  const out = await applyReleaseStatus(uid, idx, st, query.from, {
    fallbackMessageId: Number(query.message.message_id || 0),
    clearRejectReason: st !== STATUS.REJECTED,
    clearModeratorComment: st !== STATUS.NEEDS_FIX,
    restorePin: st === STATUS.ON_UPLOAD
  });
  if (!out.ok) {
    await safeAnswerCallback(query.id, { text: `❌ ${out.error}`, show_alert: true });
    return;
  }
  if (out.pin_result && !out.pin_result.ok && out.pin_result.missingRights) {
    await safeAnswerCallback(query.id, {
      text: `${statusEmoji(st)} ${statusText(st)} (нет права на ${shouldStatusStayPinned(st) ? 'закреп' : 'откреп'})`,
      show_alert: true
    });
    return;
  }
  const lockNote = out.prev_moderator ? ` • перехвачено у @${out.prev_moderator}` : '';
  const pinNote = out.pin_result && !out.pin_result.skipped && out.pin_result.ok
    ? (shouldStatusStayPinned(st) ? ' • закреплено' : ' • откреплено')
    : '';
  await safeAnswerCallback(query.id, { text: `${statusEmoji(st)} ${statusText(st)}${pinNote}${lockNote}` });
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

  const parsedCmd = parseBotCommand(text, moderationHealth.bot_username);
  if (parsedCmd && ['/status', '/ping', '/ver', '/deploy', '/queue', '/pending', '/find', '/new', '/unpin', '/unpin_done', '/unpin_all', '/modtest'].includes(parsedCmd.command)) {
    const cmd = parsedCmd.command;
    pollingHealth.last_command = cmd;
    pollingHealth.last_command_at = new Date().toISOString();
    pollingHealth.last_command_result = 'handling';
    pollingHealth.last_command_error = '';

    if (cmd === '/status' || cmd === '/ping' || cmd === '/ver' || cmd === '/deploy') {
      const inModChat = Number(chatId) === Number(MOD_CHAT);
      if (!inModChat && !isAdmin(uid)) {
        pollingHealth.last_command_result = 'ignored_chat';
        return;
      }
      if (inModChat && !(await canModerate(msg.from?.id, msg))) {
        await replyInChat(msg, 'Доступ только участникам группы модерации.');
        pollingHealth.last_command_result = 'denied';
        return;
      }
      try {
        const receivedAtMs = Date.now();
        const msgDateSec = Number(msg?.date || 0);
        const deliveryLagSec = msgDateSec > 0 ? Math.max(0, Math.round((receivedAtMs - msgDateSec * 1000) / 1000)) : null;

        let apiPingMs = null;
        let apiPingOk = false;
        const t0 = Date.now();
        try {
          await tg('getMe', {});
          apiPingMs = Date.now() - t0;
          apiPingOk = true;
        } catch (e) {
          apiPingMs = Date.now() - t0;
        }
        await ensureModerationHealth(true);
        const replyBuildMs = Date.now() - receivedAtMs;

        let totalReleases = 0;
        let onUploadCount = 0;
        let moderationCount = 0;
        let approvedCount = 0;
        let rejectedCount = 0;
        for (const listRaw of Object.values(db || {})) {
          const list = Array.isArray(listRaw) ? listRaw : [];
          for (const r of list) {
            if (!r || typeof r !== 'object') continue;
            totalReleases += 1;
            const st = canonicalStatus(r.status);
            if (st === STATUS.ON_UPLOAD) onUploadCount += 1;
            else if (st === STATUS.MODERATION) moderationCount += 1;
            else if (st === STATUS.APPROVED || st === STATUS.PUBLISHED) approvedCount += 1;
            else if (st === STATUS.REJECTED) rejectedCount += 1;
          }
        }

        const gitLine = BOT_DEPLOY_INFO.commit
          ? `<code>${esc(BOT_DEPLOY_INFO.commit)}</code>` +
            (BOT_DEPLOY_INFO.branch ? ` (${esc(BOT_DEPLOY_INFO.branch)})` : '') +
            (BOT_DEPLOY_INFO.commit_subject ? `\n• Коммит: <i>${esc(BOT_DEPLOY_INFO.commit_subject)}</i>` : '')
          : `<i>build_tag (${esc(BOT_DEPLOY_INFO.branch || 'main')}, без .git)</i>` +
            (BOT_DEPLOY_INFO.commit_subject ? `\n• Релиз: <i>${esc(BOT_DEPLOY_INFO.commit_subject)}</i>` : '');

        const pollLine = pollingHealth.ok === true
          ? `🟢 активен${pollingHealth.last_poll_ok_at ? ` (${esc(pollingHealth.last_poll_ok_at.slice(11, 19))} UTC)` : ''}`
          : `🔴 ошибки: ${pollingHealth.fail_streak}${pollingHealth.last_error ? ` (${esc(pollingHealth.last_error.slice(0, 60))})` : ''}`;

        const supabaseBadges = [
          `forms:${supabaseFeatureState.forms ? '✅' : '❌'}`,
          `users:${supabaseFeatureState.users ? '✅' : '❌'}`,
          `public:${supabaseFeatureState.public_releases ? '✅' : '❌'}`,
          `interactions:${supabaseFeatureState.interactions ? '✅' : '❌'}`
        ].join(' • ');

        const msgLines = [
          '🟢 <b>Бот онлайн и отвечает!</b>',
          '',
          '🚀 <b>Деплой и версия:</b>',
          `• Build: <code>${esc(BOT_BUILD_VERSION)}</code>`,
          `• Git: ${gitLine}`,
          `• Старт: <code>${esc(BOT_STARTED_AT.replace('T', ' ').slice(0, 19))} UTC</code>`,
          `• Uptime: <b>${esc(formatUptimeMs(Date.now() - BOT_STARTED_AT_MS))}</b>`,
          '',
          '⚡ <b>Реакция и отклик:</b>',
          `• Telegram API (getMe): ${apiPingOk ? `<b>${apiPingMs} мс</b>` : `❌ ошибка (${apiPingMs} мс)`}`,
          `• Доставка апдейта → старт ответа: <b>${deliveryLagSec !== null ? `${deliveryLagSec} сек` : '—'}</b> (сбор: ${replyBuildMs} мс)`,
          `• Long polling: ${pollLine}`,
          '',
          '🛡 <b>Группа модерации:</b>',
          `• Чат: <b>${esc(moderationHealth.chat_title || String(MOD_CHAT))}</b>`,
          `• Роль: <code>${esc(moderationHealth.bot_status || '-')}</code> (@${esc(moderationHealth.bot_username || '-')})`,
          `• Топик: <code>${MODERATION_THREAD_ID > 0 ? MODERATION_THREAD_ID : (msg?.message_thread_id ? `текущий (${msg.message_thread_id})` : 'default')}</code>`,
          `• Права: отправка ${moderationHealth.can_send_messages === true ? '✅' : '❌'} • закреп/откреп ${moderationHealth.can_pin_messages === true ? '✅' : '❌'}`,
          '',
          '🗄 <b>База и синк:</b>',
          `• Релизов: <b>${totalReleases}</b> (отгрузка: ${onUploadCount}, модерация: ${moderationCount}, принято: ${approvedCount}, откл: ${rejectedCount})`,
          `• Supabase: ${supabaseFeatureState ? supabaseBadges : 'отключен'}`
        ];

        await replyInChat(msg, msgLines.join('\n'), { reply_to_message_id: msg.message_id });
        pollingHealth.last_command_result = 'ok';
      } catch (err) {
        const errText = shortTgError(err);
        pollingHealth.last_command_result = 'error';
        pollingHealth.last_command_error = errText;
        console.error('[bot] /status error:', errText);
        await replyInChat(msg, `❌ Ошибка ${esc(cmd)}: <code>${esc(errText)}</code>`).catch(() => {});
      }
      return;
    }
    if (cmd === '/new') {
      if (Number(chatId) !== Number(MOD_CHAT)) {
        await replyInChat(msg, 'Команда /new работает только в группе модерации.');
        return;
      }
      if (!(await canModerate(msg.from?.id, msg))) {
        await replyInChat(msg, 'Доступ только участникам группы модерации.');
        return;
      }
      const summary = await runNewModerationSweep(msg.from);
      if (!summary.total) {
        await replyInChat(msg, '🆕 Анкет со статусом «На отгрузке» сейчас нет.');
        return;
      }
      await replyInChat(
        msg,
        '🆕 <b>/new выполнено</b>\n\n' +
        `⏳ На отгрузке: <b>${summary.total}</b>\n` +
        `✅ Отмечено свободными: <b>${summary.marked}</b>\n` +
        `📝 Обновлено сообщений: <b>${summary.refreshed}</b>\n` +
        `⚠️ Без moderation_message_id: <b>${summary.missingMessage}</b>`
      );
      return;
    }

    if (cmd === '/unpin') {
      if (Number(chatId) !== Number(MOD_CHAT)) {
        await replyInChat(msg, 'Команда /unpin работает только в группе модерации.');
        return;
      }
      if (!(await canModerate(msg.from?.id, msg))) {
        await replyInChat(msg, 'Доступ только участникам группы модерации.');
        return;
      }
      const targetMsgId = Number(msg.reply_to_message?.message_id || 0);
      if (!targetMsgId) {
        await replyInChat(
          msg,
          'Ответьте командой <code>/unpin</code> на сообщение, которое нужно открепить, или используйте <code>/unpin_done</code> для массового открепления.',
          { reply_to_message_id: msg.message_id }
        );
        return;
      }
      try {
        const res = await unpinModerationMessage(targetMsgId);
        if (!res.ok) {
          const hint = moderationErrorHint(res.error);
          await replyInChat(
            msg,
            `❌ Не удалось открепить сообщение: <code>${esc(res.error)}</code>${hint ? `\n${esc(hint)}` : ''}`,
            { reply_to_message_id: msg.message_id }
          );
          return;
        }
        const ref = findReleaseByModerationMessageId(targetMsgId);
        if (ref?.rel) {
          ref.rel.moderation_pinned = false;
          ref.rel.manual_unpinned = true;
          ref.rel.pin_pending = false;
          clearScheduledModerationPinRetry(ref.uid, ref.idx, targetMsgId);
          saveDb();
          syncModerationMirror(ref.uid, ref.idx, ref.rel);
          saveModDb();
          await refreshModerationMessage(ref.uid, ref.idx, ref.rel, targetMsgId);
        }
        await replyInChat(msg, res.alreadyUnpinned ? 'ℹ️ Сообщение уже откреплено.' : '✅ Сообщение откреплено.', {
          reply_to_message_id: targetMsgId
        });
      } catch (err) {
        await replyInChat(msg, `❌ Ошибка /unpin: <code>${esc(shortTgError(err))}</code>`, {
          reply_to_message_id: msg.message_id
        }).catch(() => {});
      }
      return;
    }

    if (cmd === '/unpin_done') {
      if (Number(chatId) !== Number(MOD_CHAT)) {
        await replyInChat(msg, 'Команда /unpin_done работает только в группе модерации.');
        return;
      }
      if (!(await canModerate(msg.from?.id, msg))) {
        await replyInChat(msg, 'Доступ только участникам группы модерации.');
        return;
      }
      const argLower = clean(parsedCmd.arg || '').toLowerCase();
      const forceAll = argLower === 'all' || argLower === 'force';
      const parsedLimit = Number.parseInt(argLower, 10);
      const batchLimit = Number.isFinite(parsedLimit) && parsedLimit > 0
        ? Math.min(200, parsedLimit)
        : (forceAll ? 200 : 35);

      let statusMsg = null;
      try {
        statusMsg = await replyInChat(
          msg,
          '⏳ Собираю карточки со статусами «Модерация», «Отклонено», «Принято», «Удалено»...',
          { reply_to_message_id: msg.message_id }
        );
      } catch {}

      try {
        const candidates = new Map();
        const dbMessageStatus = new Map();
        for (const [uKey, listRaw] of Object.entries(db || {})) {
          const list = Array.isArray(listRaw) ? listRaw : [];
          for (let i = 0; i < list.length; i += 1) {
            const r = list[i];
            const mid = Number(r?.moderation_message_id || 0);
            if (!mid) continue;
            dbMessageStatus.set(mid, r);
            if (shouldStatusStayPinned(r?.status)) continue;
            if (!forceAll && r.moderation_pinned === false) continue;
            const sortTs = parseIsoTime(r?.moderation_time || r?.submission_time || '') || 0;
            candidates.set(mid, { mid, uid: uKey, idx: i, rel: r, sortTs });
          }
        }
        if (SUPABASE_SYNC_ENABLED) {
          try {
            const rows = await supabaseSelectAll(SUPABASE_FORMS_TABLE, 'id,status,moderation_message_id,created_at,updated_at', 'created_at.desc');
            for (const row of (rows || [])) {
              const mid = Number(row?.moderation_message_id || 0);
              if (!mid) continue;
              const st = mapFormStatusToReleaseStatus(row?.status || '');
              if (shouldStatusStayPinned(st)) continue;
              const knownRel = dbMessageStatus.get(mid);
              if (!forceAll && knownRel && knownRel.moderation_pinned === false) continue;
              if (!candidates.has(mid)) {
                const sortTs = parseIsoTime(row?.updated_at || row?.created_at || '') || 0;
                candidates.set(mid, { mid, uid: '', idx: 0, rel: knownRel || null, sortTs });
              }
            }
          } catch (e) {
            console.warn('[MODERATION] /unpin_done supabase fetch warning:', shortTgError(e));
          }
        }

        const sorted = Array.from(candidates.values()).sort((a, b) => (b.sortTs - a.sortTs) || (b.mid - a.mid));
        const batch = sorted.slice(0, batchLimit);
        const remainingBefore = Math.max(0, sorted.length - batch.length);

        let checked = 0;
        let unpinned = 0;
        let already = 0;
        let failed = 0;
        let missingRights = false;
        let floodStopped = false;
        let dirtyDb = false;

        for (const item of batch) {
          let res = await unpinModerationMessage(item.mid, { silent: true });
          if (!res.ok && res.retryAfter > 0 && res.retryAfter <= 35) {
            await delay((res.retryAfter + 1) * 1000);
            res = await unpinModerationMessage(item.mid, { silent: true });
          }
          checked += 1;
          if (res.ok) {
            if (res.alreadyUnpinned) already += 1;
            else unpinned += 1;
            if (item.rel && item.rel.moderation_pinned !== false) {
              item.rel.moderation_pinned = false;
              if (item.uid !== '') syncModerationMirror(item.uid, item.idx, item.rel);
              dirtyDb = true;
            }
          } else {
            failed += 1;
            if (res.missingRights) {
              missingRights = true;
              break;
            }
            if (res.retryAfter > 0) {
              floodStopped = true;
              break;
            }
          }
          await delay(450);
        }
        if (dirtyDb) {
          saveDb();
          saveModDb();
        }
        const totalLeft = Math.max(0, sorted.length - (unpinned + already));
        const notes = [];
        if (missingRights) {
          notes.push('⚠️ Остановлено: у бота нет права администратора «Закрепление сообщений» (can_pin_messages).');
        }
        if (floodStopped) {
          notes.push('⏳ Сработал лимит Telegram (429). Запустите <code>/unpin_done</code> через ~30 сек или используйте <code>/unpin_all</code> (сбросит все старые закрепы за 1 запрос).');
        } else if (remainingBefore > 0 || totalLeft > 0) {
          notes.push(`ℹ️ В очереди осталось ещё: <b>${totalLeft}</b>. Запустите <code>/unpin_done</code> снова или <code>/unpin_all</code> для мгновенной очистки всех закрепов.`);
        }
        const reportText =
          `🧹 <b>Очистка закрепов завершена</b>\n\n` +
          `Проверено (новейшие): <b>${checked}</b> из <b>${sorted.length}</b>\n` +
          `📌 Откреплено: <b>${unpinned}</b>\n` +
          `ℹ️ Уже не закреплено: <b>${already}</b>\n` +
          `❌ Ошибок: <b>${failed}</b>` +
          (notes.length ? `\n\n${notes.join('\n')}` : '');

        await updateStatusReport(statusMsg, msg, reportText);
      } catch (err) {
        const errReport = `❌ Ошибка /unpin_done: <code>${esc(shortTgError(err))}</code>`;
        await updateStatusReport(statusMsg, msg, errReport);
      }
      return;
    }

    if (cmd === '/unpin_all') {
      if (Number(chatId) !== Number(MOD_CHAT)) {
        await replyInChat(msg, 'Команда /unpin_all работает только в группе модерации.');
        return;
      }
      if (!(await canModerate(msg.from?.id, msg))) {
        await replyInChat(msg, 'Доступ только участникам группы модерации.');
        return;
      }
      let statusMsg = null;
      try {
        statusMsg = await replyInChat(
          msg,
          '🧹 Сбрасываю все старые закрепы и оставляю только актуальные «На отгрузке»...',
          { reply_to_message_id: msg.message_id }
        );
      } catch {}

      try {
        await tg('unpinAllChatMessages', { chat_id: MOD_CHAT });
        for (const [uKey, listRaw] of Object.entries(db || {})) {
          const list = Array.isArray(listRaw) ? listRaw : [];
          for (let i = 0; i < list.length; i += 1) {
            if (list[i] && typeof list[i] === 'object') {
              list[i].moderation_pinned = false;
              syncModerationMirror(uKey, i, list[i]);
            }
          }
        }
        const onUploadRefs = collectOnUploadReleaseRefs().filter((it) => Number(it.rel?.moderation_message_id || 0) > 0);
        const toRepin = onUploadRefs.slice(0, 15).reverse();
        let repinned = 0;
        for (const item of toRepin) {
          const mid = Number(item.rel.moderation_message_id || 0);
          let pRes = await pinModerationMessage(mid, { silent: true });
          if (!pRes.ok && pRes.retryAfter > 0 && pRes.retryAfter <= 35) {
            await delay((pRes.retryAfter + 1) * 1000);
            pRes = await pinModerationMessage(mid, { silent: true });
          }
          if (pRes.ok) {
            item.rel.moderation_pinned = true;
            syncModerationMirror(item.uid, item.idx, item.rel);
            repinned += 1;
          } else if (pRes.retryAfter > 0 || pRes.missingRights) {
            break;
          }
          await delay(450);
        }
        saveDb();
        saveModDb();
        await updateStatusReport(
          statusMsg,
          msg,
          `✅ <b>Все старые закрепы очищены!</b>\n\n` +
          `📌 Обратно закреплено активных «На отгрузке»: <b>${repinned}</b> (из ${onUploadRefs.length}).`
        );
      } catch (err) {
        const errText = shortTgError(err);
        const hint = moderationErrorHint(errText);
        await updateStatusReport(
          statusMsg,
          msg,
          `❌ Не удалось выполнить /unpin_all: <code>${esc(errText)}</code>${hint ? `\n${esc(hint)}` : ''}`
        );
      }
      return;
    }

    if (cmd === '/queue' || cmd === '/pending') {
      const inModChat = Number(chatId) === Number(MOD_CHAT);
      if (!inModChat && !isAdmin(uid)) {
        pollingHealth.last_command_result = 'ignored_chat';
        return;
      }
      if (inModChat && !(await canModerate(msg.from?.id, msg))) {
        await replyInChat(msg, 'Доступ только участникам группы модерации.');
        pollingHealth.last_command_result = 'denied';
        return;
      }
      const onUpload = [];
      const inWork = [];
      const needFix = [];
      let approvedCount = 0;
      let rejectedCount = 0;
      for (const [ownerId, listRaw] of Object.entries(db || {})) {
        const list = Array.isArray(listRaw) ? listRaw : [];
        for (let idx = 0; idx < list.length; idx += 1) {
          const rel = list[idx];
          if (!rel || typeof rel !== 'object' || rel.user_deleted) continue;
          const st = canonicalStatus(rel.status);
          if (st === STATUS.DELETED) continue;
          const entry = { ownerId: String(ownerId), idx, rel, st };
          if (st === STATUS.ON_UPLOAD) onUpload.push(entry);
          else if (st === STATUS.MODERATION) inWork.push(entry);
          else if (st === STATUS.NEEDS_FIX) needFix.push(entry);
          else if (st === STATUS.APPROVED || st === STATUS.PUBLISHED) approvedCount += 1;
          else if (st === STATUS.REJECTED) rejectedCount += 1;
        }
      }
      const sortNewest = (a, b) => {
        const ta = parseIsoTime(a.rel?.submission_time || a.rel?.moderation_time || '');
        const tb = parseIsoTime(b.rel?.submission_time || b.rel?.moderation_time || '');
        if (tb !== ta) return tb - ta;
        return b.idx - a.idx;
      };
      onUpload.sort(sortNewest);
      inWork.sort(sortNewest);
      needFix.sort(sortNewest);

      const formatEntryLink = (entry) => {
        const link = buildModerationMessageLink(entry.rel?.moderation_message_id);
        const title = esc(shortTitle(entry.rel?.name || 'Без названия', 26));
        return link ? `<a href="${esc(link)}">${title}</a>` : `<b>${title}</b>`;
      };

      const lines = [
        '📋 <b>Очередь модерации</b>',
        '',
        `• 🕓 На отгрузке: <b>${onUpload.length}</b> • 🧠 В работе: <b>${inWork.length}</b> • ✏️ Правки: <b>${needFix.length}</b>`,
        `• ✅ Принято: <b>${approvedCount}</b> • ❌ Отклонено: <b>${rejectedCount}</b>`
      ];
      if (!onUpload.length && !inWork.length && !needFix.length) {
        lines.push('', '✅ Очередь чистая — активных релизов на проверку нет.');
      } else {
        if (onUpload.length) {
          lines.push('', `🕓 <b>На отгрузке (${onUpload.length}):</b>`);
          for (let i = 0; i < Math.min(10, onUpload.length); i += 1) {
            const it = onUpload[i];
            const nick = esc(shortTitle(it.rel?.nick || '—', 18));
            const date = esc(clean(it.rel?.date || '—'));
            const upcTag = clean(it.rel?.upc) ? ` • <code>${esc(it.rel.upc)}</code>` : '';
            lines.push(`${i + 1}. ${formatEntryLink(it)} — ${nick} (📅 ${date})${upcTag}`);
          }
          if (onUpload.length > 10) lines.push(`<i>…и ещё ${onUpload.length - 10} в очереди</i>`);
        }
        if (inWork.length) {
          lines.push('', `🧠 <b>В работе сейчас (${inWork.length}):</b>`);
          for (const it of inWork.slice(0, 6)) {
            const nick = esc(shortTitle(it.rel?.nick || '—', 18));
            const actor = moderationActorTag(it.rel);
            const actorPart = actor ? ` • <i>ведёт ${esc(actor)}</i>` : '';
            lines.push(`• ${formatEntryLink(it)} — ${nick}${actorPart}`);
          }
          if (inWork.length > 6) lines.push(`<i>…и ещё ${inWork.length - 6}</i>`);
        }
        if (needFix.length) {
          lines.push('', `✏️ <b>Ждут правок (${needFix.length}):</b>`);
          for (const it of needFix.slice(0, 5)) {
            const nick = esc(shortTitle(it.rel?.nick || '—', 18));
            lines.push(`• ${formatEntryLink(it)} — ${nick}`);
          }
          if (needFix.length > 5) lines.push(`<i>…и ещё ${needFix.length - 5}</i>`);
        }
      }
      pollingHealth.last_command_result = 'ok';
      await replyInChat(msg, lines.join('\n'), { reply_to_message_id: msg.message_id });
      return;
    }

    if (cmd === '/find') {
      const inModChat = Number(chatId) === Number(MOD_CHAT);
      if (!inModChat && !isAdmin(uid)) {
        pollingHealth.last_command_result = 'ignored_chat';
        return;
      }
      if (inModChat && !(await canModerate(msg.from?.id, msg))) {
        await replyInChat(msg, 'Доступ только участникам группы модерации.');
        pollingHealth.last_command_result = 'denied';
        return;
      }
      const queryRaw = clean(parsedCmd.arg);
      if (queryRaw.length < 2) {
        pollingHealth.last_command_result = 'ok';
        await replyInChat(msg, '🔎 Использование: <code>/find название / ник / UPC / ID</code>', {
          reply_to_message_id: msg.message_id
        });
        return;
      }
      const tokens = queryRaw.toLowerCase().split(/\s+/).filter(Boolean);
      const matches = [];
      for (const [ownerId, listRaw] of Object.entries(db || {})) {
        const list = Array.isArray(listRaw) ? listRaw : [];
        for (let idx = 0; idx < list.length; idx += 1) {
          const rel = list[idx];
          if (!rel || typeof rel !== 'object') continue;
          const haystack = [
            rel.name,
            rel.nick,
            rel.fio,
            rel.upc,
            rel.genre,
            rel.tg,
            rel.moderator_username,
            ownerId
          ].map((v) => clean(v).toLowerCase()).join(' ');
          if (tokens.every((t) => haystack.includes(t))) {
            matches.push({ ownerId: String(ownerId), idx, rel, st: canonicalStatus(rel.status) });
          }
        }
      }
      matches.sort((a, b) => {
        const delA = a.rel?.user_deleted || a.st === STATUS.DELETED ? 1 : 0;
        const delB = b.rel?.user_deleted || b.st === STATUS.DELETED ? 1 : 0;
        if (delA !== delB) return delA - delB;
        const ta = parseIsoTime(a.rel?.submission_time || a.rel?.moderation_time || '');
        const tb = parseIsoTime(b.rel?.submission_time || b.rel?.moderation_time || '');
        if (tb !== ta) return tb - ta;
        return b.idx - a.idx;
      });
      if (!matches.length) {
        pollingHealth.last_command_result = 'ok';
        await replyInChat(msg, `🔎 По запросу <code>${esc(queryRaw)}</code> ничего не найдено.`, {
          reply_to_message_id: msg.message_id
        });
        return;
      }
      const lines = [`🔎 <b>Найдено: ${matches.length}</b> (запрос: <code>${esc(queryRaw)}</code>)`, ''];
      for (const it of matches.slice(0, 12)) {
        const link = buildModerationMessageLink(it.rel?.moderation_message_id);
        const title = esc(shortTitle(it.rel?.name || 'Без названия', 26));
        const label = link ? `<a href="${esc(link)}">${title}</a>` : `<b>${title}</b>`;
        const nick = esc(shortTitle(it.rel?.nick || '—', 18));
        const upcPart = clean(it.rel?.upc) ? ` • <code>${esc(it.rel.upc)}</code>` : '';
        lines.push(`${statusEmoji(it.st)} ${label} — ${nick} • <i>${esc(statusText(it.st))}</i>${upcPart}`);
      }
      if (matches.length > 12) {
        lines.push('', `<i>Показаны первые 12 из ${matches.length}. Уточните запрос.</i>`);
      }
      pollingHealth.last_command_result = 'ok';
      await replyInChat(msg, lines.join('\n'), { reply_to_message_id: msg.message_id });
      return;
    }

    if (cmd === '/modtest') {
      const inModChat = Number(chatId) === Number(MOD_CHAT);
      const allowedMod = isAdmin(uid) || (inModChat && await canModerate(msg.from?.id, msg));
      if (!allowedMod) {
        await replyInChat(msg, '❌ Команда доступна только участникам группы модерации или администраторам.');
        pollingHealth.last_command_result = 'denied';
        return;
      }
      await ensureModerationHealth(true);
      try {
        const probeExtra = MODERATION_THREAD_ID <= 0 && Number(msg?.message_thread_id || 0) > 0
          ? { message_thread_id: Number(msg.message_thread_id) }
          : {};
        const probe = await sendModerationText(
          `🧪 <b>Проверка модерации</b>\n` +
          `Build: <code>${esc(BOT_BUILD_VERSION)}</code>\n` +
          `Время: <code>${esc(new Date().toISOString())}</code>\n` +
          `Проверка отправки из node_bot.js`,
          probeExtra
        );
        await replyInChat(
          msg,
          `✅ Отправка в модерацию работает.\n` +
          `build: <code>${esc(BOT_BUILD_VERSION)}</code>\n` +
          `message_id: <code>${esc(String(probe?.message_id || '0'))}</code>\n` +
          `status бота: <b>${esc(moderationHealth.bot_status || '-')}</b>\n` +
          `права на закреп/откреп: <b>${moderationHealth.can_pin_messages === true ? 'есть' : moderationHealth.can_pin_messages === false ? 'НЕТ (выдайте can_pin_messages)' : 'не определено'}</b>`
        );
        pollingHealth.last_command_result = 'ok';
      } catch (e) {
        const errText = shortTgError(e);
        const hint = moderationErrorHint(errText);
        pollingHealth.last_command_result = 'error';
        pollingHealth.last_command_error = errText;
        await replyInChat(
          msg,
          `❌ Отправка в модерацию не работает.\n` +
          `Причина: <code>${esc(errText)}</code>` +
          (hint ? `\nПодсказка: ${esc(hint)}` : '')
        ).catch(() => {});
      }
      return;
    }
  }

  if (await handleModerationReplyMessage(msg)) return;
  if (await handleCoverMessage(msg)) return;
  if (await handlePromoMessage(msg)) return;

  if (!text) return;

  // PRIORITY 1: Account synchronization commands and codes
  // Must run BEFORE handleFormTextMessage so stale form sessions never eat link codes!
  const trimmedText = text.trim();
  if (text.startsWith('/start link_') || text.startsWith('/start CX-') || text.startsWith('/start cx-')) {
    const token = text.slice('/start '.length).trim();
    await linkTelegramToWebCabinet(chatId, uid, msg.from, token);
    return;
  }
  if (text.startsWith('/start ') && (text.includes('link_') || text.includes('cx-') || text.includes('CX-'))) {
    const token = text.slice('/start '.length).trim();
    await linkTelegramToWebCabinet(chatId, uid, msg.from, token);
    return;
  }
  if (text === '/link' || text.startsWith('/link ') || text.startsWith('/link_')) {
    const code = text.replace(/^\/link[_\s]*/i, '').trim();
    await linkTelegramToWebCabinet(chatId, uid, msg.from, code);
    return;
  }
  // Raw sync codes pasted directly in chat (e.g. "CX-360449", "cx-360449", "360449", "link_...", or 32-hex tokens)
  if (
    /^cx-[0-9]{4,8}$/i.test(trimmedText) ||
    /^[0-9]{6}$/.test(trimmedText) ||
    /^link_[0-9a-f]{16,64}$/i.test(trimmedText) ||
    /^[0-9a-f]{32}$/i.test(trimmedText)
  ) {
    await linkTelegramToWebCabinet(chatId, uid, msg.from, trimmedText);
    return;
  }

  // Active form session for distributing tracks
  if (await handleFormTextMessage(msg)) return;

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
    let refNote = '';
    const refMatch = /^\/start(?:@\w+)?\s+ref[_-]?(\d{4,20})\b/i.exec(trimmedText);
    if (refMatch && refMatch[1] !== uid) {
      const refRes = await processReferralSignup(refMatch[1], uid, msg.from).catch(() => null);
      if (refRes?.recorded) {
        refNote = '\n\n🎁 <b>Бонус приглашения:</b> +50 CXP зачислено на твой баланс CXRNER DROP!';
      }
    }
    await sendText(chatId, `${welcomeText()}${refNote}`, { reply_markup: keyboardMain() });
    return;
  }
  if (/^\/admin(?:@\w+)?$/i.test(text)) {
    if (!isAdmin(uid)) {
      await replyInChat(msg, '❌ Доступ запрещён.');
      return;
    }
    await sendAdminPanel(msg);
    return;
  }
  if (/^\/statss(?:@\w+)?$/i.test(text) || /^\/stats(?:@\w+)?$/i.test(text)) {
    if (!isAdmin(uid)) {
      await replyInChat(msg, '❌ Команда доступна только администраторам.');
      return;
    }
    await sendStatsPicker(msg);
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
  if (text === '/app' || text === 'Открыть приложение') {
    await sendWebappButton(chatId);
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
  const deferAnswer = /^m_/.test(data);
  if (!deferAnswer) {
    try { await tg('answerCallbackQuery', { callback_query_id: query.id }, { noRateLimitRetry: true }); } catch {}
  }

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
            { text: '✅ Да, удалить всё', callback_data: 'admin_cleanbase_yes', style: 'danger' },
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
      const prevStatus = canonicalStatus(rel.status || STATUS.ON_UPLOAD);
      rel.user_deleted = true;
      rel.deleted_at = new Date().toISOString();
      rel.status = STATUS.DELETED;
      rel.available_for_upload = false;
      pushReleaseInteraction(rel, 'status_change', query.from, {
        status_from: prevStatus,
        status_to: STATUS.DELETED,
        note: 'Удалено артистом из кабинета'
      });
      await syncModerationPinForRelease(rel, Number(rel.moderation_message_id || 0), { silent: true }).catch(() => {});
      saveDb();
      syncModerationMirror(ownerId, idx, rel);
      saveModDb();
      await refreshModerationMessage(ownerId, idx, rel).catch(() => {});
      await supabasePatchFormByRelease(ownerId, rel, {
        status: mapReleaseStatusToFormStatus(STATUS.DELETED),
        reject_reason: 'Удалено артистом',
        user_deleted: true,
        form_payload: safeJson(rel) || {}
      }).catch(() => {});
      await supabaseDeleteApprovedRelease(ownerId, rel).catch(() => {});
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
  if (data === 'cabinet_sync_site') {
    const text =
      '🔗 <b>Синхронизация с сайтом CXRNER MUSIC</b>\n\n' +
      'Объедините Telegram и сайт в <b>единый личный кабинет</b>:\n\n' +
      '• Все треки и анкеты, отправленные через бота или Mini App, автоматически отобразятся на сайте со всей информацией.\n' +
      '• Все релизы, отправленные через сайт, будут доступны для отслеживания в Telegram.\n' +
      '• Статусы модерации и официальные коды UPC обновляются синхронно.\n\n' +
      '<b>Инструкция по привязке:</b>\n' +
      '1. Перейдите в кабинет на сайте:\n' +
      '   <code>https://cxrnermusic.vercel.app/dashboard</code>\n' +
      '2. В блоке <b>«Синхронизация с Telegram»</b> нажмите кнопку <b>«Связать с ботом»</b> (бот привяжет аккаунт в 1 клик!).\n' +
      '3. Либо скопируйте 6-значный код (например: <code>CX-849201</code>) и отправьте боту:\n' +
      '   <code>/link ВАШ_КОД</code>\n\n' +
      'После привязки вы сможете в один клик возвращаться в бот или Mini App.';
    const kb = {
      inline_keyboard: [
        [{ text: '🌐 Открыть кабинет на сайте', url: 'https://cxrnermusic.vercel.app/dashboard', style: 'primary' }],
        ...(hasValidWebAppUrl() ? [[{ text: '📱 Открыть Mini App', web_app: { url: WEBAPP_URL }, style: 'primary' }]] : []),
        [{ text: '⬅️ Назад в кабинет', callback_data: 'menu_cabinet' }]
      ]
    };
    await edit(text, kb);
    return;
  }
  if (data === 'menu_community') { await edit('<b>Комьюнити</b>\n\nОфициальные площадки CXRNER MUSIC:', keyboardCommunity()); return; }
  if (data === 'open_app' || data === 'report_app') {
    await sendWebappButton(chatId);
    return;
  }
  if (data === 'report' || data === 'report_text') {
    await startTextForm(chatId, String(query.from.id), query.from);
    return;
  }
  if (data === 'my_releases') { await sendMy(chatId, String(query.from.id)); return; }
  if (data === 'service_cover') { await startCoverFlow(chatId, String(query.from.id), query.from); return; }
  if (data === 'service_promo') { await startPromoFlow(chatId, String(query.from.id), query.from); return; }
  const restoreMatch = /^m_restore_buttons_([A-Za-z0-9_-]+)_(\d+)$/.exec(data);
  if (restoreMatch) {
    if (Number(chatId) !== Number(MOD_CHAT)) {
      await safeAnswerCallback(query.id, { text: 'Эти кнопки работают только в группе модерации.', show_alert: true });
      return;
    }
    if (!(await canModerate(query.from?.id, query.message))) {
      await safeAnswerCallback(query.id, { text: 'Доступ только участникам группы модерации.', show_alert: true });
      return;
    }
    const uid = restoreMatch[1];
    const idx = Number.parseInt(restoreMatch[2], 10);
    const ref = resolveReleaseRef(uid, idx, Number(query.message?.message_id || 0));
    if (!ref || !ref.rel) {
      await safeAnswerCallback(query.id, { text: '❌ Релиз не найден.', show_alert: true });
      return;
    }
    await refreshModerationMessage(ref.uid, ref.idx, ref.rel, Number(query.message?.message_id || 0));
    await safeAnswerCallback(query.id, { text: '🔘 Кнопки обновлены' });
    return;
  }
  const cancelPromptMatch = /^m_pc_([A-Za-z0-9_-]+)_(\d+)$/.exec(data);
  if (cancelPromptMatch) {
    if (Number(chatId) !== Number(MOD_CHAT)) {
      await safeAnswerCallback(query.id, { text: 'Эти кнопки работают только в группе модерации.', show_alert: true });
      return;
    }
    if (!(await canModerate(query.from?.id, query.message))) {
      await safeAnswerCallback(query.id, { text: 'Доступ только участникам группы модерации.', show_alert: true });
      return;
    }
    const targetUid = cancelPromptMatch[1];
    const targetIdx = Number.parseInt(cancelPromptMatch[2], 10);
    const promptMsgId = Number(query.message?.message_id || 0);
    ensureModDbShape();
    modDb.pending_actions = modDb.pending_actions.filter((it) => !(
      (promptMsgId && Number(it.prompt_message_id) === promptMsgId) ||
      (String(it.moderator_id) === String(query.from.id) && String(it.uid) === String(targetUid) && Number(it.idx) === targetIdx)
    ));
    saveModDb();
    if (promptMsgId) tryDeleteMessageSafe(chatId, promptMsgId);
    await safeAnswerCallback(query.id, { text: 'Ввод отменён' });
    return;
  }
  const presetMatch = /^m_pr_(r|f)_([a-z0-9]+)_([A-Za-z0-9_-]+)_(\d+)$/.exec(data);
  if (presetMatch) {
    if (Number(chatId) !== Number(MOD_CHAT)) {
      await safeAnswerCallback(query.id, { text: 'Эти кнопки работают только в группе модерации.', show_alert: true });
      return;
    }
    if (!(await canModerate(query.from?.id, query.message))) {
      await safeAnswerCallback(query.id, { text: 'Доступ только участникам группы модерации.', show_alert: true });
      return;
    }
    const kind = presetMatch[1] === 'r' ? 'reject' : 'needfix';
    const preset = MOD_REASON_PRESETS[kind]?.[presetMatch[2]];
    if (!preset) {
      await safeAnswerCallback(query.id, { text: 'Шаблон не найден.', show_alert: true });
      return;
    }
    const targetUid = presetMatch[3];
    const targetIdx = Number.parseInt(presetMatch[4], 10);
    const promptMsgId = Number(query.message?.message_id || 0);
    const fallbackId = Number(query.message?.reply_to_message?.message_id || 0);
    const ref = resolveReleaseRef(targetUid, targetIdx, fallbackId);
    if (!ref || !ref.rel) {
      if (promptMsgId) tryDeleteMessageSafe(chatId, promptMsgId);
      await safeAnswerCallback(query.id, { text: 'Релиз не найден.', show_alert: true });
      return;
    }
    ensureModDbShape();
    modDb.pending_actions = modDb.pending_actions.filter((it) => !(
      (promptMsgId && Number(it.prompt_message_id) === promptMsgId) ||
      (String(it.moderator_id) === String(query.from.id) && String(it.uid) === String(ref.uid) && Number(it.idx) === ref.idx)
    ));
    saveModDb();
    if (promptMsgId) tryDeleteMessageSafe(chatId, promptMsgId);

    const out = kind === 'reject'
      ? await applyReleaseStatus(ref.uid, ref.idx, STATUS.REJECTED, query.from, {
          rejectReason: preset.text,
          clearModeratorComment: true,
          fallbackMessageId: Number(ref.rel.moderation_message_id || fallbackId || 0),
          note: `Причина (${preset.label}) выбрана шаблоном`
        })
      : await applyReleaseStatus(ref.uid, ref.idx, STATUS.NEEDS_FIX, query.from, {
          moderatorComment: preset.text,
          clearRejectReason: true,
          fallbackMessageId: Number(ref.rel.moderation_message_id || fallbackId || 0),
          note: `Правки (${preset.label}) выбраны шаблоном`
        });

    if (!out.ok) {
      await safeAnswerCallback(query.id, { text: `❌ ${out.error}`, show_alert: true });
      return;
    }
    if (out.pin_result && !out.pin_result.ok && out.pin_result.missingRights) {
      await safeAnswerCallback(query.id, {
        text: `${kind === 'reject' ? '❌ Отклонено' : '✏️ Правки'}: ${preset.label} (нет права на откреп)`,
        show_alert: true
      });
      return;
    }
    const pinNote = out.pin_result && !out.pin_result.skipped && out.pin_result.ok ? ' • откреплено' : '';
    await safeAnswerCallback(query.id, {
      text: `${kind === 'reject' ? '❌ Отклонено' : '✏️ Правки'}: ${preset.label}${pinNote}`
    });
    return;
  }
  const pinMatch = /^m_(un)?pin_([A-Za-z0-9_-]+)_(\d+)$/.exec(data);
  if (pinMatch) {
    const answerPin = (text, showAlert = false) => safeAnswerCallback(query.id, { text, show_alert: showAlert });
    if (Number(chatId) !== Number(MOD_CHAT)) {
      await answerPin('Эти кнопки работают только в группе модерации.', true);
      return;
    }
    if (!(await canModerate(query.from?.id, query.message))) {
      await answerPin('Доступ только участникам группы модерации.', true);
      return;
    }
    const wantPin = !pinMatch[1];
    const uid = pinMatch[2];
    const idx = Number.parseInt(pinMatch[3], 10);
    const msgId = Number(query.message?.message_id || 0);
    const ref = resolveReleaseRef(uid, idx, msgId);
    const targetMsgId = Number(ref?.rel?.moderation_message_id || msgId || 0);
    const pinRes = wantPin
      ? await pinModerationMessage(targetMsgId)
      : await unpinModerationMessage(targetMsgId);
    if (!pinRes.ok) {
      const hint = moderationErrorHint(pinRes.error);
      await answerPin(
        `Не удалось ${wantPin ? 'закрепить' : 'открепить'}: ${pinRes.error}${hint ? `\n\n${hint}` : ''}`,
        true
      );
      return;
    }
    if (ref?.rel) {
      ref.rel.moderation_pinned = wantPin;
      ref.rel.manual_unpinned = !wantPin;
      ref.rel.pin_pending = false;
      if (!wantPin) {
        clearScheduledModerationPinRetry(ref.uid, ref.idx, targetMsgId);
      }
      saveDb();
      syncModerationMirror(ref.uid, ref.idx, ref.rel);
      saveModDb();
      await supabasePatchFormByRelease(ref.uid, ref.rel, {
        moderation_message_id: Number(ref.rel.moderation_message_id || targetMsgId || 0) || null,
        form_payload: safeJson(ref.rel) || {}
      });
      await refreshModerationMessage(ref.uid, ref.idx, ref.rel, msgId);
    } else if (msgId) {
      try {
        await tg('editMessageReplyMarkup', {
          chat_id: MOD_CHAT,
          message_id: msgId,
          reply_markup: moderationKeyboard(uid, idx, { moderation_pinned: wantPin })
        });
      } catch {}
    }
    await answerPin(
      wantPin
        ? '📌 Сообщение закреплено'
        : (pinRes.alreadyUnpinned ? 'ℹ️ Сообщение уже откреплено' : '✅ Сообщение откреплено')
    );
    return;
  }
  const m = /^m_(upload|moderate|approve|reject|needfix|delete|upc)_([A-Za-z0-9_-]+)_(\d+)$/.exec(data);
  if (m) {
    if (m[1] === 'reject') {
      await startModerationReplyFlow(query, 'reject_reason', m[2], Number.parseInt(m[3], 10));
      return;
    }
    if (m[1] === 'needfix') {
      await startModerationReplyFlow(query, 'needfix_comment', m[2], Number.parseInt(m[3], 10));
      return;
    }
    if (m[1] === 'upc') {
      await startModerationReplyFlow(query, 'upc', m[2], Number.parseInt(m[3], 10));
      return;
    }
    await applyModeration(query, m[1], m[2], Number.parseInt(m[3], 10));
    return;
  }
  if (deferAnswer) {
    await safeAnswerCallback(query.id);
  }
}

const DROP_CASES_CONFIG = [
  {
    id: 'case_001_archive',
    name: 'CASE 001 — ARCHIVE',
    subtitle: 'SPECIMEN ARCHIVE BASE',
    description: 'Базовые коллекционные артефакты лейбла, цифровые ключи, визор-маски и шанс на ваучер обложки.',
    price_cxp: 50,
    accent: '#00F0FF',
    drop_rates: { common: 55, rare: 25, epic: 13, legendary: 5.5, mythic: 1.4, special_reward: 0.1 }
  },
  {
    id: 'case_002_black_specimen',
    name: 'CASE 002 — BLACK SPECIMEN',
    subtitle: 'ORGANIC FORMS & MASKS',
    description: 'Редкая серия тёмного металла, органических сердец, глаз наблюдателя и эксклюзивных артефактов.',
    price_cxp: 75,
    accent: '#A855F7',
    drop_rates: { common: 0, rare: 50, epic: 32, legendary: 14, mythic: 3.8, special_reward: 0.2 }
  },
  {
    id: 'case_003_chrome_age',
    name: 'CASE 003 — CHROME AGE',
    subtitle: 'LIQUID CHROME & SPIKES',
    description: 'Ультрапремиальная линейка жидкого хрома: Chrome Spike, Void Crown, Chrome Fang Black/White и реликвии.',
    price_cxp: 100,
    accent: '#F59E0B',
    drop_rates: { common: 0, rare: 35, epic: 40, legendary: 20, mythic: 4.7, special_reward: 0.3 }
  }
];
const DROP_SPECIMENS_CATALOG = [
  { item_id: 'specimen_cyber_vinyl_01', item_name: 'CYBER VINYL GENESIS', specimen_code: 'VN-001', specimen_class: 'MEDIA', collection_name: 'GENESIS ARCHIVE', rarity: 'common', model_color: '#A855F7', shape: 'cylinder', description: 'Первый серийный винил с лазерной гравировкой CXRNER-001.' },
  { item_id: 'specimen_data_key_03', item_name: 'DATA KEY', specimen_code: 'DK-003', specimen_class: 'OBJECT', collection_name: 'SPECIMEN ARCHIVE', rarity: 'common', model_color: '#94A3B8', shape: 'box', description: 'Металлический цифровой ключ доступа к мастер-трекам.' },
  { item_id: 'specimen_basic_specimen_04', item_name: 'BASIC SPECIMEN', specimen_code: 'BS-004', specimen_class: 'OBJECT', collection_name: 'SPECIMEN ARCHIVE', rarity: 'common', model_color: '#64748B', shape: 'cylinder', description: 'Запечатанная полимерная капсула архива.' },
  { item_id: 'specimen_chrome_tag_05', item_name: 'CHROME TAG', specimen_code: 'TG-005', specimen_class: 'OBJECT', collection_name: 'CHROME AGE', rarity: 'common', model_color: '#CBD5E1', shape: 'box', description: 'Металлическая маркировочная бирка серии CXRNER.' },
  { item_id: 'specimen_archive_cassette_02', item_name: 'ARCHIVE CASSETTE', specimen_code: 'CA-037', specimen_class: 'MEDIA', collection_name: 'GENESIS ARCHIVE', rarity: 'rare', model_color: '#00F0FF', shape: 'box', description: 'Коллекционная аудиокассета CXRNER ARCHIVE в вакуумной капсуле.' },
  { item_id: 'specimen_chrome_disc_06', item_name: 'CHROME DISC', specimen_code: 'CD-001', specimen_class: 'MEDIA', collection_name: 'CHROME AGE', rarity: 'rare', model_color: '#38BDF8', shape: 'cylinder', description: 'Хромированный CD-диск с радужной голографической дифракцией.' },
  { item_id: 'specimen_chrome_spike_16', item_name: 'CHROME SPIKE', specimen_code: 'CR-017', specimen_class: 'OBJECT', collection_name: 'CHROME AGE', rarity: 'rare', model_color: '#E2E8F0', shape: 'octahedron', description: 'Многогранный шип из жидкого хрома с титановым напылением.' },
  { item_id: 'specimen_violet_visor_13', item_name: 'VIOLET VISOR', specimen_code: 'VG-005', specimen_class: 'ACCESSORY', collection_name: 'SPECIMEN ARCHIVE', rarity: 'epic', model_color: '#C084FC', shape: 'box', description: 'Футуристическая маска-визор саунд-дизайнера со спектральным HUD.' },
  { item_id: 'specimen_black_heart_12', item_name: 'BLACK HEART', specimen_code: 'HR-009', specimen_class: 'ORGANIC', collection_name: 'BLACK SPECIMEN', rarity: 'epic', model_color: '#A855F7', shape: 'octahedron', description: 'Зеркально-чёрное механическое сердце с суб-басовым импульсом.' },
  { item_id: 'specimen_watcher_eye_11', item_name: 'WATCHER EYE', specimen_code: 'EY-004', specimen_class: 'ORGANIC', collection_name: 'BLACK SPECIMEN', rarity: 'legendary', model_color: '#F59E0B', shape: 'dodecahedron', description: 'Биомеханическое око в венце шипов, сканирующее звуковую сцену.' },
  { item_id: 'specimen_void_crown_17', item_name: 'VOID CROWN', specimen_code: 'CR-003', specimen_class: 'ACCESSORY', collection_name: 'BLACK SPECIMEN', rarity: 'legendary', model_color: '#FCD34D', shape: 'torus', description: 'Готическая корона из застывшего жидкого хрома правителя сцены.' },
  { item_id: 'specimen_chrome_fang_19', item_name: 'CHROME FANG BLACK', specimen_code: 'C1/S-B', specimen_class: 'ARTIFACT', collection_name: 'CHROME AGE', rarity: 'legendary', model_color: '#FBBF24', shape: 'dodecahedron', description: 'Чёрная металлическая челюсть хищника с зеркальными клыками.' },
  { item_id: 'specimen_white_fang_20', item_name: 'WHITE FANG SPECIMEN', specimen_code: 'C1/S-W', specimen_class: 'ARTIFACT', collection_name: 'CHROME AGE', rarity: 'legendary', model_color: '#FFFFFF', shape: 'dodecahedron', description: 'Редкая матово-белая мутация клыков хищника C1/S.' },
  { item_id: 'specimen_cxrner_original_24', item_name: 'CHROMATIC FANG // CXRNER ORIGINAL', specimen_code: 'GLITCH-01', specimen_class: 'ARTIFACT', collection_name: 'FOUNDERS EDITION', rarity: 'mythic', model_color: '#F43F5E', shape: 'icosahedron', description: 'Сверхредкий хроматический глитч-артефакт основания CXRNER MUSIC.' },
  { item_id: 'specimen_cover_voucher_25', item_name: 'CXRNER COVER VOUCHER', specimen_code: 'CV-2026', specimen_class: 'REWARD', collection_name: 'SPECIAL REWARDS', rarity: 'special_reward', model_color: '#F59E0B', shape: 'box', is_special_reward: true, description: 'Бесплатная обложка для релиза от @kazumaiq.' },
  { item_id: 'specimen_fasttrack_pitch_26', item_name: 'FAST-TRACK PITCHING VOUCHER', specimen_code: 'FP-2026', specimen_class: 'REWARD', collection_name: 'SPECIAL REWARDS', rarity: 'special_reward', model_color: '#00F0FF', shape: 'box', is_special_reward: true, description: 'Приоритетная модерация 24ч и питчинг плейлистам CXRNER.' },
  { item_id: 'specimen_royalty_boost_27', item_name: 'ROYALTY BOOST 85/15', specimen_code: 'RB-2026', specimen_class: 'REWARD', collection_name: 'SPECIAL REWARDS', rarity: 'special_reward', model_color: '#A855F7', shape: 'box', is_special_reward: true, description: 'Апгрейд ставки роялти 85% артисту на следующий релиз.' },
  { item_id: 'specimen_label_promo_28', item_name: 'LABEL PROMO SPOT', specimen_code: 'LP-2026', specimen_class: 'REWARD', collection_name: 'SPECIAL REWARDS', rarity: 'special_reward', model_color: '#F43F5E', shape: 'box', is_special_reward: true, description: 'Персональный анонс и закреп в витрине CXRNER MUSIC.' }
];
const DROP_SALVAGE_MAP = { common: 15, rare: 30, epic: 55, legendary: 120, mythic: 250, special_reward: 500 };
const DROP_FUSION_NEXT = { common: 'rare', rare: 'epic', epic: 'legendary', legendary: 'mythic' };
const DROP_PROMO_CODES = {
  CXRNER2026: { cxp: 120, label: 'CXRNER 2026 Bonus' },
  DROP100: { cxp: 100, label: 'Drop Boost +100 CXP' },
  KAZUMACEO: { cxp: 150, label: 'CEO Patron Gift' },
  WELCOME50: { cxp: 50, label: 'Welcome Supply' }
};
const CHAMPION_MILESTONES = [
  { step: 1, required: 1, title: '1 друг', subtitle: '+75 CXP на баланс', reward_type: 'cxp', reward_cxp: 75 },
  { step: 2, required: 2, title: '2 друга', subtitle: '+125 CXP на баланс', reward_type: 'cxp', reward_cxp: 125 },
  { step: 3, required: 3, title: '3 друга', subtitle: 'Набор ARCHIVE + 50 CXP', reward_type: 'pack', reward_cxp: 50, case_id: 'case_001_archive', packs_count: 1 },
  { step: 4, required: 4, title: '4 друга', subtitle: '+200 CXP на баланс', reward_type: 'cxp', reward_cxp: 200 },
  { step: 5, required: 5, title: '5 друзей', subtitle: '2× BLACK SPECIMEN', reward_type: 'pack', reward_cxp: 0, case_id: 'case_002_black_specimen', packs_count: 2 }
];
const GIFT_HMAC_SECRET = envStr('DROP_GIFT_SECRET', envStr('SUPABASE_SERVICE_ROLE_KEY', envStr('ADMIN_PASSWORD', 'cxrner_drop_gift_hmac_2026')));
const localDropVaultCache = new Map();
const lastAuthSyncAt = new Map();
const cabinetRowsCache = { rows: [], fetchedAt: 0, promise: null };
const recentGlobalDropsCache = [];

function deterministicTgUuid(uid) {
  const hash = crypto.createHash('sha256').update(`cxrner:tg:${clean(uid)}`).digest('hex');
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-4${hash.slice(13, 16)}-8${hash.slice(17, 20)}-${hash.slice(20, 32)}`;
}
function signMiniAppGiftPayload(b64Data) {
  return crypto.createHmac('sha256', GIFT_HMAC_SECRET).update(b64Data).digest('base64url').slice(0, 16);
}
function rollSpecimenForCase(caseCfg) {
  const rates = caseCfg.drop_rates || {};
  const entries = Object.entries(rates).filter(([, w]) => Number(w) > 0);
  const total = entries.reduce((sum, [, w]) => sum + Number(w), 0);
  let roll = (crypto.randomBytes(4).readUInt32BE(0) / 0x100000000) * total;
  let rarity = entries[0]?.[0] || 'common';
  for (const [r, w] of entries) {
    roll -= Number(w);
    if (roll <= 0) { rarity = r; break; }
  }
  const pool = DROP_SPECIMENS_CATALOG.filter((s) => s.rarity === rarity);
  const pick = pool[crypto.randomInt(0, pool.length)] || DROP_SPECIMENS_CATALOG[0];
  return pick;
}
async function getCabinetRowsCached(force = false) {
  if (!SUPABASE_SYNC_ENABLED) return [];
  const now = Date.now();
  if (!force && cabinetRowsCache.rows.length && (now - cabinetRowsCache.fetchedAt) < 15000) {
    return cabinetRowsCache.rows;
  }
  if (cabinetRowsCache.promise) return cabinetRowsCache.promise;
  cabinetRowsCache.promise = supabaseSelectAll(SUPABASE_CABINET_TABLE_RAW, 'user_id,profile,updated_at', 'updated_at.desc')
    .then((rows) => {
      cabinetRowsCache.rows = Array.isArray(rows) ? rows : [];
      cabinetRowsCache.fetchedAt = Date.now();
      return cabinetRowsCache.rows;
    })
    .catch(() => cabinetRowsCache.rows || [])
    .finally(() => { cabinetRowsCache.promise = null; });
  return cabinetRowsCache.promise;
}
function recordGlobalDropFeedEntry(entry) {
  if (!entry || !entry.item_name) return;
  recentGlobalDropsCache.unshift(entry);
  if (recentGlobalDropsCache.length > 30) recentGlobalDropsCache.length = 30;
}
function buildGlobalDropFeed(rows = [], limit = 14) {
  const combined = [...recentGlobalDropsCache];
  for (const r of rows || []) {
    const p = r?.profile || {};
    const ep = p.ecosystem_profile || {};
    const displayName = clean(ep.display_name || p.display_name || ep.username || p.username || 'Артист');
    const username = clean(ep.username || p.username || '');
    const hist = Array.isArray(p.ecosystem_drop_history) ? p.ecosystem_drop_history.slice(0, 3) : [];
    for (const h of hist) {
      if (!h?.item_name) continue;
      combined.push({
        id: clean(h.id || `${r.user_id}-${h.created_at}`),
        display_name: displayName,
        username,
        item_name: h.item_name,
        specimen_code: h.specimen_code || '',
        rarity: h.rarity || 'common',
        created_at: h.created_at || r.updated_at || new Date().toISOString()
      });
    }
  }
  const seen = new Set();
  const deduped = [];
  combined.sort((a, b) => new Date(b.created_at || 0) - new Date(a.created_at || 0));
  for (const item of combined) {
    const k = `${item.id}:${item.item_name}`;
    if (seen.has(k)) continue;
    seen.add(k);
    deduped.push(item);
    if (deduped.length >= limit) break;
  }
  return deduped;
}
async function resolveMiniAppVault(uid, tgUser = {}) {
  const cleanUid = clean(uid);
  const uname = clean(tgUser?.username || '').replace(/^@/, '').toLowerCase();
  const isCeo = uname === 'kazumaiq' || isAdmin(cleanUid);
  const fallbackUuid = deterministicTgUuid(cleanUid);
  let targetUserId = fallbackUuid;
  let vault = localDropVaultCache.get(cleanUid) || null;
  if (!vault && SUPABASE_SYNC_ENABLED) {
    try {
      const rows = await getCabinetRowsCached(false);
      const matched = (rows || []).find((row) => {
        const p = row?.profile && typeof row.profile === 'object' ? row.profile : {};
        const ep = p.ecosystem_profile && typeof p.ecosystem_profile === 'object' ? p.ecosystem_profile : {};
        const pTg = clean(p.telegram_id || ep.telegram_id || '');
        const pUser = clean(ep.username || ep.telegram_username || p.username || '').replace(/^@/, '').toLowerCase();
        return row.user_id === fallbackUuid || row.user_id === `tg_${cleanUid}` || (pTg && pTg === cleanUid) || (uname && pUser === uname);
      });
      if (matched) {
        targetUserId = matched.user_id;
        vault = matched.profile && typeof matched.profile === 'object' ? { ...matched.profile } : {};
      } else {
        vault = vault || {};
      }
    } catch (err) {
      console.warn('[DROP_VAULT] resolve failed:', clean(err?.message || err));
    }
  }
  vault = vault && typeof vault === 'object' ? { ...vault } : {};
  const nowIso = new Date().toISOString();
  const defaultBalance = isCeo ? 999999 : 150;
  const baseWallet = vault.ecosystem_wallet && typeof vault.ecosystem_wallet === 'object'
    ? { ...vault.ecosystem_wallet }
    : { user_id: targetUserId, balance: defaultBalance, total_earned: defaultBalance, total_spent: 0, updated_at: nowIso };
  if (isCeo && Number(baseWallet.balance || 0) < 999999) {
    baseWallet.balance = 999999;
    baseWallet.total_earned = Math.max(Number(baseWallet.total_earned || 0), 999999);
  }
  const removedSet = new Set(Array.isArray(vault.ecosystem_removed_item_ids) ? vault.ecosystem_removed_item_ids : []);
  let inventory = Array.isArray(vault.ecosystem_inventory) ? vault.ecosystem_inventory.filter((x) => x && !removedSet.has(x.id)) : [];
  const history = Array.isArray(vault.ecosystem_drop_history) ? vault.ecosystem_drop_history : [];
  if (isCeo && !inventory.length) {
    inventory = [
      { id: 'ceo_glitch_01', user_id: targetUserId, item_id: 'specimen_cxrner_original_24', item_name: 'CHROMATIC FANG // CXRNER ORIGINAL', specimen_code: 'GLITCH-01', specimen_class: 'ARTIFACT', collection_name: 'FOUNDERS EDITION', rarity: 'mythic', serial_number: 1, description: 'Именной Founders-артефакт CEO CXRNER MUSIC.', model_color: '#F43F5E', shape: 'icosahedron', obtained_via: 'ceo_sovereign', is_pinned: true, created_at: nowIso },
      { id: 'ceo_crown_01', user_id: targetUserId, item_id: 'specimen_void_crown_17', item_name: 'VOID CROWN', specimen_code: 'CR-003', specimen_class: 'ACCESSORY', collection_name: 'BLACK SPECIMEN', rarity: 'legendary', serial_number: 1, description: 'Корона основателя CXRNER MUSIC.', model_color: '#FCD34D', shape: 'torus', obtained_via: 'ceo_sovereign', is_pinned: true, created_at: nowIso }
    ];
  }
  vault.telegram_id = cleanUid;
  vault.ecosystem_wallet = baseWallet;
  vault.ecosystem_inventory = inventory;
  vault.ecosystem_drop_history = history;
  localDropVaultCache.set(cleanUid, vault);
  return { targetUserId, isCeo, vault, uid: cleanUid };
}
async function saveMiniAppVault(resolved, updater) {
  const nextVault = updater({ ...resolved.vault });
  const nowIso = new Date().toISOString();
  nextVault.telegram_id = resolved.uid;
  nextVault.updated_at = nowIso;
  localDropVaultCache.set(resolved.uid, nextVault);
  if (cabinetRowsCache.rows.length) {
    const idx = cabinetRowsCache.rows.findIndex((r) => r.user_id === resolved.targetUserId);
    const updatedRow = { user_id: resolved.targetUserId, profile: nextVault, updated_at: nowIso };
    if (idx >= 0) cabinetRowsCache.rows[idx] = updatedRow;
    else cabinetRowsCache.rows.unshift(updatedRow);
  }
  if (SUPABASE_SYNC_ENABLED) {
    try {
      await supabaseRequest(`${SUPABASE_CABINET_TABLE_RAW}?on_conflict=user_id`, {
        method: 'POST',
        headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
        body: [{ user_id: resolved.targetUserId, profile: nextVault, updated_at: nowIso }]
      });
      if (nextVault.ecosystem_wallet) {
        await supabaseRequest('cxrner_cxp_wallet?on_conflict=user_id', {
          method: 'POST',
          headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
          body: [{
            user_id: resolved.targetUserId,
            balance: Math.max(0, Number(nextVault.ecosystem_wallet.balance || 0)),
            total_earned: Math.max(0, Number(nextVault.ecosystem_wallet.total_earned || 0)),
            total_spent: Math.max(0, Number(nextVault.ecosystem_wallet.total_spent || 0)),
            updated_at: nowIso
          }]
        }).catch(() => {});
      }
    } catch (err) {
      console.warn('[DROP_VAULT] save error:', clean(err?.message || err));
    }
  }
  resolved.vault = nextVault;
  return nextVault;
}
async function processReferralSignup(referrerUidRaw, invitedUidRaw, invitedUser = {}) {
  const referrerUid = clean(referrerUidRaw).replace(/^ref[_-]?/i, '');
  const invitedUid = clean(invitedUidRaw);
  if (!/^\d{4,20}$/.test(referrerUid) || !/^\d{4,20}$/.test(invitedUid) || referrerUid === invitedUid) {
    return { ok: false, reason: 'invalid_referral' };
  }
  const [invResolved, refResolved] = await Promise.all([
    resolveMiniAppVault(invitedUid, invitedUser),
    resolveMiniAppVault(referrerUid, {})
  ]);
  if (invResolved.vault.ecosystem_referrals?.invited_by) {
    return { ok: true, already_recorded: true, referrer_id: invResolved.vault.ecosystem_referrals.invited_by };
  }
  const existingFriends = Array.isArray(refResolved.vault.ecosystem_referrals?.friends) ? refResolved.vault.ecosystem_referrals.friends : [];
  if (existingFriends.some((f) => String(f.invited_id) === invitedUid)) {
    return { ok: true, already_recorded: true, referrer_id: referrerUid };
  }
  const nowIso = new Date().toISOString();
  const displayName = clean([invitedUser.first_name, invitedUser.last_name].filter(Boolean).join(' ') || invitedUser.username || `ID ${invitedUid}`);
  const username = clean(invitedUser.username || '').replace(/^@/, '');
  const friendEntry = {
    invited_id: Number(invitedUid),
    display_name: displayName,
    username,
    photo_url: clean(invitedUser.photo_url || ''),
    event_type: 'signup',
    bonus: 75,
    created_at: nowIso
  };
  await saveMiniAppVault(invResolved, (prev) => {
    const w = prev.ecosystem_wallet || { user_id: invResolved.targetUserId, balance: 150, total_earned: 150, total_spent: 0 };
    return {
      ...prev,
      ecosystem_referrals: { ...(prev.ecosystem_referrals || {}), invited_by: referrerUid, joined_at: nowIso, welcome_bonus: 50 },
      ecosystem_wallet: { ...w, balance: Number(w.balance || 0) + 50, total_earned: Number(w.total_earned || 0) + 50, updated_at: nowIso }
    };
  });
  await saveMiniAppVault(refResolved, (prev) => {
    const w = prev.ecosystem_wallet || { user_id: refResolved.targetUserId, balance: 150, total_earned: 150, total_spent: 0 };
    const prevRef = prev.ecosystem_referrals && typeof prev.ecosystem_referrals === 'object' ? prev.ecosystem_referrals : {};
    const prevFriends = Array.isArray(prevRef.friends) ? prevRef.friends.filter((f) => String(f.invited_id) !== invitedUid) : [];
    return {
      ...prev,
      ecosystem_referrals: {
        ...prevRef,
        friends: [friendEntry, ...prevFriends].slice(0, 500),
        total_bonus_earned: Number(prevRef.total_bonus_earned || 0) + 75
      },
      ecosystem_wallet: { ...w, balance: Number(w.balance || 0) + 75, total_earned: Number(w.total_earned || 0) + 75, updated_at: nowIso }
    };
  });
  if (SUPABASE_SYNC_ENABLED) {
    await supabaseRequest('cxrner_referrals?on_conflict=telegram_id', {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: [{ telegram_id: Number(invitedUid), code: `ref_${invitedUid}`, invited_by: Number(referrerUid), bonus_balance: 50 }]
    }).catch(() => {});
    await supabaseRequest('cxrner_referral_events?on_conflict=referrer_id,invited_id', {
      method: 'POST',
      headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
      body: [{ referrer_id: Number(referrerUid), invited_id: Number(invitedUid), event_type: 'signup', bonus: 75 }]
    }).catch(() => {});
    await supabaseRequest('cxrner_notifications', {
      method: 'POST',
      headers: { Prefer: 'return=minimal' },
      body: [{ telegram_id: Number(referrerUid), type: 'referral', title: 'Новый друг в команде!', body: `${displayName} перешёл по твоей ссылке. Начислено +75 CXP!` }]
    }).catch(() => {});
  }
  return { ok: true, recorded: true, referrer_id: referrerUid, friend: friendEntry };
}

async function executeModerationAction(opts = {}) {
  const formId = clean(opts.formId || opts.id || opts.form_id || '');
  const status = clean(opts.status || '');
  const action = clean(opts.action || status);
  const upc = clean(opts.upc || '');
  const rejectReason = clean(opts.rejectReason || opts.reject_reason || '');
  const moderatorComment = clean(opts.moderatorComment || opts.moderator_comment || '');
  const moderatorName = clean(opts.moderatorName || opts.moderator_name || 'cxrner.label@gmail.com');
  const telegramId = clean(opts.telegramId || opts.telegram_id || '');
  const trackName = clean(opts.trackName || opts.track_name || '');

  console.info(`[api/moderation/action] processing: form_id=${formId} status=${status} action=${action} upc=${upc}`);

  const map = {
    upload: STATUS.ON_UPLOAD,
    on_upload: STATUS.ON_UPLOAD,
    pending: STATUS.ON_UPLOAD,
    moderate: STATUS.MODERATION,
    moderation: STATUS.MODERATION,
    on_moderation: STATUS.MODERATION,
    approve: STATUS.APPROVED,
    approved: STATUS.APPROVED,
    needfix: STATUS.NEEDS_FIX,
    fix_required: STATUS.NEEDS_FIX,
    reject: STATUS.REJECTED,
    rejected: STATUS.REJECTED,
    delete: STATUS.DELETED,
    deleted: STATUS.DELETED
  };
  const targetStatus = map[status] || map[action] || STATUS.ON_UPLOAD;

  let matched = false;
  let updatedRel = null;

  // Case 1: Composite ID from Telegram bot (uid:time:idx)
  if (formId.includes(':')) {
    const parts = formId.split(':');
    const uid = parts[0];
    const idx = Number.parseInt(parts[parts.length - 1], 10);
    if (Number.isFinite(idx)) {
      const out = await applyReleaseStatus(
        uid,
        idx,
        targetStatus,
        { id: 0, first_name: moderatorName, username: moderatorName },
        {
          upc,
          rejectReason,
          moderatorComment,
          clearRejectReason: targetStatus !== STATUS.REJECTED,
          restoreFromDelete: targetStatus !== STATUS.DELETED,
          restorePin: targetStatus === STATUS.ON_UPLOAD,
          note: `Изменено через Web CMS (${moderatorName})`
        }
      );
      if (out.ok) {
        matched = true;
        updatedRel = out.rel;
        const msgId = Number(out.rel?.moderation_message_id || 0);
        if (upc) {
          await sendText(MOD_CHAT, `✅ UPC присвоен: <code>${esc(upc)}</code>`, {
            reply_to_message_id: msgId || undefined
          }).catch(() => null);
          if (/^\d{4,20}$/.test(clean(uid))) {
            await sendText(Number(uid), `📦 <b>UPC присвоен</b>\n\n🎵 <b>${esc(out.rel?.name || 'Релиз')}</b>\nUPC: <code>${esc(upc)}</code>`).catch(() => null);
          }
        }
        if (action === 'approve' || status === 'approved') {
          await sendText(MOD_CHAT, `✅ <b>Релиз одобрен модератором</b> (${esc(moderatorName)})`, {
            reply_to_message_id: msgId || undefined
          }).catch(() => null);
        } else if (action === 'reject' || status === 'rejected') {
          await sendText(MOD_CHAT, `❌ <b>Релиз отклонен:</b> ${esc(rejectReason || 'Без указания причины')}`, {
            reply_to_message_id: msgId || undefined
          }).catch(() => null);
        } else if (action === 'needfix' || status === 'needfix') {
          await sendText(MOD_CHAT, `✏️ <b>Отправлено на исправления:</b> ${esc(moderatorComment || 'Требуются правки')}`, {
            reply_to_message_id: msgId || undefined
          }).catch(() => null);
        }
      }
    }
  }

  // Case 2: Match by formId or telegram_id in bot memory db
  if (!matched) {
    for (const [uid, list] of Object.entries(db || {})) {
      if (!Array.isArray(list)) continue;
      for (let idx = 0; idx < list.length; idx += 1) {
        const r = list[idx];
        if (r && (r.supabase_form_id === formId || String(r.supabase_form_id || '').startsWith(formId) || (telegramId && String(uid) === String(telegramId) && r.name === trackName))) {
          const out = await applyReleaseStatus(
            uid,
            idx,
            targetStatus,
            { id: 0, first_name: moderatorName, username: moderatorName },
            {
              upc,
              rejectReason,
              moderatorComment,
              clearRejectReason: targetStatus !== STATUS.REJECTED,
              restoreFromDelete: targetStatus !== STATUS.DELETED,
              note: `Изменено через Web CMS (${moderatorName})`
            }
          );
          if (out.ok) {
            matched = true;
            updatedRel = out.rel;
            const msgId = Number(out.rel?.moderation_message_id || 0);
            if (upc) {
              await sendText(MOD_CHAT, `✅ UPC присвоен: <code>${esc(upc)}</code>`, {
                reply_to_message_id: msgId || undefined
              }).catch(() => null);
              if (/^\d{4,20}$/.test(clean(uid))) {
                await sendText(Number(uid), `📦 <b>UPC присвоен</b>\n\n🎵 <b>${esc(out.rel?.name || 'Релиз')}</b>\nUPC: <code>${esc(upc)}</code>`).catch(() => null);
              }
            }
            if (action === 'approve' || status === 'approved') {
              await sendText(MOD_CHAT, `✅ <b>Релиз одобрен модератором</b> (${esc(moderatorName)})`, {
                reply_to_message_id: msgId || undefined
              }).catch(() => null);
            } else if (action === 'reject' || status === 'rejected') {
              await sendText(MOD_CHAT, `❌ <b>Релиз отклонен:</b> ${esc(rejectReason || 'Без указания причины')}`, {
                reply_to_message_id: msgId || undefined
              }).catch(() => null);
            } else if (action === 'needfix' || status === 'needfix') {
              await sendText(MOD_CHAT, `✏️ <b>Отправлено на исправления:</b> ${esc(moderatorComment || 'Требуются правки')}`, {
                reply_to_message_id: msgId || undefined
              }).catch(() => null);
            }
            break;
          }
        }
      }
      if (matched) break;
    }
  }

  // Case 3: Web-submitted release in Supabase cxrner_forms
  if (!matched && formId) {
    try {
      let rows = await supabaseSelectWhere(SUPABASE_FORMS_TABLE, '*', [{ key: 'id', value: formId }], '', 1);
      let formRow = Array.isArray(rows) && rows[0] ? rows[0] : null;

      if (!formRow) {
        const allRecent = await supabaseSelectAll(SUPABASE_FORMS_TABLE, '*', 'created_at.desc');
        formRow = (allRecent || []).find((r) => String(r.id || '').startsWith(formId) || String(r.submission_key || '') === formId) || null;
      }

      if (formRow) {
        const uid = clean(formRow.telegram_id || telegramId || '');
        const p = formRow.form_payload && typeof formRow.form_payload === 'object' ? formRow.form_payload : {};
        const msgId = Number(formRow.moderation_message_id || p.moderation_message_id || 0);

        const finalUpc = upc || formRow.upc || p.upc || '';
        const relData = {
          ...p,
          name: clean(formRow.track_name || p.name || ''),
          nick: clean(formRow.artist_name || p.nick || ''),
          status: targetStatus,
          upc: finalUpc,
          reject_reason: rejectReason || formRow.reject_reason || '',
          moderator_comment: moderatorComment || formRow.moderator_comment || '',
          moderation_message_id: msgId || 0
        };

        const pinHintId = Number(msgId || 0);
        if (pinHintId) {
          await syncModerationPinForRelease(relData, pinHintId, {
            pinIfOnUpload: targetStatus === STATUS.ON_UPLOAD && (canonicalStatus(formRow.status || p.status) !== STATUS.ON_UPLOAD || !p.moderation_pinned)
          });
        }

        if (msgId) {
          try {
            await tg('editMessageText', {
              chat_id: MOD_CHAT,
              message_id: msgId,
              text: buildModerationText(uid || 'web', relData),
              parse_mode: 'HTML',
              disable_web_page_preview: true,
              reply_markup: moderationKeyboard(uid || 'web', 0, relData)
            });
          } catch (err) {
            console.warn('[api/moderation/action] editMessageText notice:', err?.message || err);
          }
        } else {
          try {
            const sent = await tg('sendMessage', moderationPayload({
              text: buildModerationText(uid || 'web', relData),
              parse_mode: 'HTML',
              disable_web_page_preview: true,
              reply_markup: moderationKeyboard(uid || 'web', 0, relData)
            }));
            if (sent?.message_id) {
              relData.moderation_message_id = sent.message_id;
              if (shouldStatusStayPinned(targetStatus)) {
                const pinRes = await pinModerationMessageNow(sent.message_id, { immediateRetry: true });
                relData.moderation_pinned = !!pinRes.ok;
              } else {
                relData.moderation_pinned = false;
              }
            }
          } catch (err) {
            console.warn('[api/moderation/action] sendMessage notice:', err?.message || err);
          }
        }

        const activeMsgId = Number(msgId || relData.moderation_message_id || 0);
        if (upc) {
          await sendText(MOD_CHAT, `✅ UPC присвоен: <code>${esc(upc)}</code>`, {
            reply_to_message_id: activeMsgId || undefined
          }).catch(() => null);
        }
        if (action === 'approve' || status === 'approved') {
          await sendText(MOD_CHAT, `✅ <b>Релиз одобрен модератором</b> (${esc(moderatorName)})`, {
            reply_to_message_id: activeMsgId || undefined
          }).catch(() => null);
        } else if (action === 'reject' || status === 'rejected') {
          await sendText(MOD_CHAT, `❌ <b>Релиз отклонен:</b> ${esc(rejectReason || 'Без указания причины')}`, {
            reply_to_message_id: activeMsgId || undefined
          }).catch(() => null);
        } else if (action === 'needfix' || status === 'needfix') {
          await sendText(MOD_CHAT, `✏️ <b>Отправлено на исправления:</b> ${esc(moderatorComment || 'Требуются правки')}`, {
            reply_to_message_id: activeMsgId || undefined
          }).catch(() => null);
        }

        if (/^\d{4,20}$/.test(clean(uid))) {
          if (upc) {
            await sendText(Number(uid), `📦 <b>UPC присвоен</b>\n\n🎵 <b>${esc(relData.name || 'Релиз')}</b>\nUPC: <code>${esc(upc)}</code>`).catch(() => null);
          }
          if (action === 'approve' || status === 'approved' || action === 'reject' || status === 'rejected' || action === 'needfix' || status === 'needfix') {
            await notifyArtistAboutRelease(uid, relData, targetStatus).catch(() => null);
          }
        }

        await supabasePatchFormByRelease(uid, { ...relData, supabase_form_id: formRow.id }, {
          status: mapReleaseStatusToFormStatus(targetStatus),
          upc: finalUpc,
          reject_reason: clean(relData.reject_reason || ''),
          moderator_comment: clean(relData.moderator_comment || ''),
          moderation_message_id: activeMsgId || null,
          form_payload: relData
        });

        if (targetStatus === STATUS.APPROVED || targetStatus === STATUS.PUBLISHED) {
          await supabaseUpsertApprovedRelease(uid, { ...relData, supabase_form_id: formRow.id });
        } else if ([STATUS.REJECTED, STATUS.DELETED].includes(targetStatus)) {
          await supabaseDeleteApprovedRelease(uid, { ...relData, supabase_form_id: formRow.id });
        }

        matched = true;
        updatedRel = relData;
      }
    } catch (dbErr) {
      console.error('[api/moderation/action] Supabase lookup error:', dbErr?.message || dbErr);
    }
  }

  return {
    matched,
    target_status: targetStatus,
    upc: upc || null,
    rel: updatedRel
  };
}

function startStaticServer() {
  if (!WEB_ENABLED) {
    console.warn('[web] static server disabled: set ENABLE_WEB_SERVER=1 or provide PORT + WEBAPP_URL');
    return;
  }
  const root = path.resolve(ROOT, WEB_DIR);
  if (!fs.existsSync(root) || !fs.statSync(root).isDirectory()) return;
  const allowedOrigins = new Set(
    [
      BASE,
      envStr('MINIAPP_ORIGIN', ''),
      'https://cxrnermusic.vercel.app',
      'https://www.cxrnermusic.vercel.app',
      'https://bot-1787153410-6782-kazumaiq.bothost.tech',
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
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PATCH,DELETE,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'content-type,authorization,x-telegram-init-data');
  }
  function sendJson(res, statusCode, data) {
    res.writeHead(statusCode, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(data));
  }
  function readJsonBody(req, maxBytes = 120000) {
    return new Promise((resolve, reject) => {
      let body = '';
      req.on('data', (chunk) => {
        body += chunk;
        if (body.length > maxBytes) {
          reject(new Error('request body is too large'));
          req.destroy();
        }
      });
      req.on('end', () => {
        try { resolve(JSON.parse(body || '{}')); } catch { reject(new Error('invalid json')); }
      });
      req.on('error', reject);
    });
  }
  async function authorizeMiniApp(req, body = {}) {
    const initData = clean(body.init_data || body.initData || req.headers?.['x-telegram-init-data'] || '');
    const check = verifyTelegramInitData(initData);
    if (!check.ok || !check.user?.id) return { ok: false, status: 403, error: check.reason || 'Telegram authorization failed' };
    const uid = String(check.user.id);
    const now = Date.now();
    const lastSync = lastAuthSyncAt.get(uid) || 0;
    const syncProfiles = async () => {
      try {
        await supabaseRequest('cxrner_telegram_profiles?on_conflict=telegram_id', {
          method: 'POST',
          headers: { Prefer: 'resolution=merge-duplicates,return=minimal' },
          body: [{ telegram_id: Number(uid), username: clean(check.user.username || ''), first_name: clean(check.user.first_name || ''), last_name: clean(check.user.last_name || ''), role: isAdmin(uid) ? 'admin' : 'artist', status: 'active', last_seen_at: new Date().toISOString(), metadata: { photo_url: clean(check.user.photo_url || '') } }]
        });
      } catch (error) {
        console.error(`[PLATFORM_AUTH] profile sync failed user_id=${uid}:`, clean(error?.message || error));
      }
      try { await supabaseUpsertCabinetUser(uid, check.user, true); } catch (error) { console.error(`[PLATFORM_AUTH] cabinet sync failed user_id=${uid}:`, clean(error?.message || error)); }
    };
    if (!lastSync) {
      lastAuthSyncAt.set(uid, now);
      await syncProfiles();
    } else if ((now - lastSync) > 60000) {
      lastAuthSyncAt.set(uid, now);
      syncProfiles().catch(() => {});
    }
    return { ok: true, uid, user: check.user, is_admin: isAdmin(uid) };
  }
  function platformRelease(row, counts = {}) {
    const data = row?.release_data && typeof row.release_data === 'object' ? row.release_data : {};
    return {
      id: clean(row?.form_id || `${row?.telegram_id || ''}:${data.release_idx || data.idx || 0}`),
      telegram_id: clean(row?.telegram_id || data.telegram_id || ''),
      title: clean(row?.track_name || data.name || data.track_name || 'Без названия'),
      artist: clean(row?.artist_name || data.nick || data.artist_name || 'Неизвестный артист'),
      genre: clean(row?.genre || data.genre || ''),
      type: clean(row?.release_type || data.type || 'single'),
      status: clean(row?.status || data.status || 'approved'),
      date: clean(data.date || row?.approved_at || row?.created_at || ''),
      description: clean(data.promo || data.comment || data.description || ''),
      cover: clean(data.cover_url || data.cover || ''),
      audio_url: clean(data.audio_url || data.preview_url || data.link || ''),
      dsp_links: data.links || data.dsp_links || { yandex: data.yandex || '' },
      smart_link: clean(data.smart_link || data.multilink || data.beatlink || ''),
      likes: Number(counts.likes || 0),
      favorites: Number(counts.favorites || 0)
    };
  }
  const linkThemes = ['aurora', 'glass', 'minimal', 'cyber', 'neon', 'liquid', 'chrome', 'midnight', 'purple-dream', 'dynamic', 'modern', 'black-edition'];
  function linkBaseUrl() { return clean(envStr('CXRNER_LINK_BASE_URL', '') || `${WEBAPP_URL || BASE}/link`).replace(/\/+$/, ''); }
  function normalizeLinkRow(row) {
    if (!row) return null;
    return { ...row, public_url: `${linkBaseUrl()}/${encodeURIComponent(clean(row.slug || ''))}`, platforms: row.platforms && typeof row.platforms === 'object' ? row.platforms : {}, extra_links: Array.isArray(row.extra_links) ? row.extra_links : [], settings: row.settings && typeof row.settings === 'object' ? row.settings : {} };
  }
  function linkSlug(value, fallback = 'release') { const slug = clean(value).toLowerCase().normalize('NFKD').replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 72); return slug || fallback; }
  function linkSourceFromRelease(formRow) {
    const payload = formRow?.form_payload && typeof formRow.form_payload === 'object' ? formRow.form_payload : {};
    return { title: clean(formRow?.track_name || payload.name || 'Без названия'), artist: clean(formRow?.artist_name || payload.nick || 'Артист'), genre: clean(formRow?.genre || payload.genre || ''), release_date: clean(payload.date || ''), cover_url: clean(payload.cover_url || payload.cover || ''), platforms: { spotify: clean(payload.spotify || payload.spotify_url || ''), apple: clean(payload.apple || payload.apple_music || ''), yandex: clean(payload.yandex || ''), vk: clean(payload.vk || payload.vk_music || ''), youtube: clean(payload.youtube || payload.youtube_music || ''), deezer: clean(payload.deezer || ''), zvuk: clean(payload.zvuk || ''), soundcloud: clean(payload.soundcloud || '') } };
  }
  async function buildRuntimeDiagnostics(req) {
    const webRoot = path.resolve(ROOT, WEB_DIR);
    const supabaseHost = SUPABASE_URL ? (() => {
      try { return new URL(SUPABASE_URL).hostname; } catch { return ''; }
    })() : '';
    const dnsChecks = {};
    for (const host of [supabaseHost, 'api.telegram.org'].filter(Boolean)) {
      try {
        const rows = await dns.promises.lookup(host, { all: true });
        dnsChecks[host] = { ok: true, addresses: rows.map((row) => row.address).slice(0, 4) };
      } catch (error) {
        dnsChecks[host] = { ok: false, code: clean(error?.code || 'DNS_ERROR') };
      }
    }
    return {
      ok: true,
      service: 'cxrner-music-bot',
      build_version: BOT_BUILD_VERSION,
      deploy: {
        ...BOT_DEPLOY_INFO,
        started_at: BOT_STARTED_AT,
        uptime_sec: Math.floor((Date.now() - BOT_STARTED_AT_MS) / 1000)
      },
      now: new Date().toISOString(),
      request: {
        host: clean(req.headers?.host || ''),
        forwarded_proto: clean(req.headers?.['x-forwarded-proto'] || ''),
        user_agent: clean(req.headers?.['user-agent'] || '').slice(0, 160)
      },
      web: {
        enabled: WEB_ENABLED,
        host: WEB_HOST,
        port: WEB_PORT,
        directory: WEB_DIR,
        directory_exists: fs.existsSync(webRoot),
        webapp_url: WEBAPP_URL || null,
        runtime_port: clean(process.env.PORT || ''),
        runtime_webapp_url: clean(process.env.WEBAPP_URL || '') || null
      },
      integrations: {
        telegram_api_configured: Boolean(TOKEN),
        moderation_chat_configured: Boolean(MOD_CHAT),
        supabase_configured: SUPABASE_SYNC_ENABLED,
        supabase_host: supabaseHost || null,
        supabase_features: { ...supabaseFeatureState }
      },
      moderation_health: {
        ok: moderationHealth.ok,
        chat_title: moderationHealth.chat_title || null,
        bot_username: moderationHealth.bot_username || null,
        bot_status: moderationHealth.bot_status || null,
        can_send_messages: moderationHealth.can_send_messages,
        can_pin_messages: moderationHealth.can_pin_messages,
        reason: moderationHealth.reason || null,
        checked_at: moderationHealth.checked_at || 0
      },
      polling: { ...pollingHealth },
      dns: dnsChecks,
      admin_ids_count: ADMIN_IDS.length
    };
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
  const srv = http.createServer((req, res) => {
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
      if (u.pathname === '/api/webapp/submit' && req.method === 'POST') {
        readJsonBody(req, Math.max(32768, WEBAPP_MAX_PAYLOAD_BYTES)).then(async (body) => {
          const initData = clean(body.initData || body.init_data || '');
          const check = verifyTelegramInitData(initData);
          if (!check.ok || !check.user?.id) {
            sendJson(res, 403, { ok: false, error: 'Telegram initData validation failed', reason: check.reason });
            return;
          }
          const payload = body.payload && typeof body.payload === 'object' ? body.payload : body;
          const parsed = parseWebappPayload(payload);
          const uid = String(check.user.id);
          if (clean(parsed.telegram_id || '') && clean(parsed.telegram_id) !== uid) {
            sendJson(res, 403, { ok: false, error: 'Telegram ID does not match initData user' });
            return;
          }
          if (!parsed.form || typeof parsed.form !== 'object') {
            sendJson(res, 400, { ok: false, error: 'form is required' });
            return;
          }
          parsed.telegram_id = uid;
          parsed.form.telegram_id = uid;
          const antiSpam = verifyWebappAntiSpam(uid, JSON.stringify(payload));
          if (!antiSpam.ok) {
            sendJson(res, 429, { ok: false, duplicate: true, error: 'Анкета уже отправлена. Подожди несколько секунд.' });
            return;
          }
          const validation = validateForm(parsed.form, parsed);
          if (validation.errors.length) {
            sendJson(res, 422, { ok: false, error: validation.errors.slice(0, 8).join('; '), errors: validation.errors });
            return;
          }
          const out = await submitReleaseToModeration(check.user, uid, validation.data, 'mini_app');
          await supabaseUpsertCabinetUser(uid, check.user, true);
          console.info(`[WEBAPP_API] release stored: user_id=${uid} idx=${out?.idx ?? '-'} name=${sanitizeText(validation.data.name || '', 80)}`);
          sendJson(res, 200, { ok: true, user_id: uid, idx: out?.idx ?? null, duplicate: Boolean(out?.duplicate), moderation_message_id: out?.rel?.moderation_message_id ?? null });
        }).catch((error) => {
          console.error('[WEBAPP_API] submit failed:', clean(error?.message || error));
          sendJson(res, 500, { ok: false, error: 'internal_error', detail: clean(error?.message || error) });
        });
        return;
      }
      if (u.pathname === '/api/new-release' && req.method === 'POST') {
        const incomingSecret = clean(
          req.headers['x-cxrner-secret'] ||
          req.headers['x-secret-key'] ||
          req.headers['x-api-key'] ||
          (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '') ||
          ''
        );
        // If WEBHOOK_SECRET is set on the bot, require it to match.
        // If not set on the bot, allow requests so initial setup does not fail.
        if (WEBHOOK_SECRET && incomingSecret !== WEBHOOK_SECRET) {
          console.warn('[api/new-release] Unauthorized: incoming secret does not match BOT_BACKEND_SECRET');
          sendJson(res, 401, { ok: false, error: 'unauthorized', detail: 'secret mismatch' });
          return;
        }
        let body = '';
        req.on('data', (chunk) => {
          body += chunk;
          if (body.length > WEBAPP_MAX_PAYLOAD_BYTES) req.destroy();
        });
        req.on('end', async () => {
          try {
            const payload = JSON.parse(body || '{}');
            const formId = clean(payload.form_id || '');
            if (!formId) {
              sendJson(res, 400, { ok: false, error: 'form_id is required' });
              return;
            }

            console.info(`[api/new-release] processing incoming release form_id=${formId} track="${payload.track_name || ''}"`);

            const rows = await supabaseSelectWhere(
              SUPABASE_FORMS_TABLE,
              'id,telegram_id,username,artist_name,track_name,genre,release_type,status,reject_reason,upc,moderation_message_id,submission_key,source,created_at,updated_at,form_payload',
              [{ key: 'id', value: formId }],
              '',
              1
            );
            const formRow = Array.isArray(rows) && rows[0] ? rows[0] : null;
            if (!formRow) {
              sendJson(res, 404, { ok: false, error: 'form not found' });
              return;
            }

            const uid = clean(formRow.telegram_id || '');
            if (!uid) {
              sendJson(res, 400, { ok: false, error: 'telegram_id is empty in form' });
              return;
            }

            const payloadForm = formRow.form_payload && typeof formRow.form_payload === 'object'
              ? formRow.form_payload
              : {};

            const releaseData = normalizeRelease({
              ...payloadForm,
              type: normalizeType(payload.release_type || payloadForm.type || formRow.release_type) || 'сингл',
              name: clean(payload.track_name || payloadForm.name || formRow.track_name || ''),
              nick: clean(payload.artist_name || payloadForm.nick || formRow.artist_name || ''),
              genre: clean(payload.genre || payloadForm.genre || formRow.genre || ''),
              tg: clean(payloadForm.tg || payloadForm.telegram_contact || ''),
              status: STATUS.ON_UPLOAD,
              submission_time: clean(formRow.submission_key || formRow.created_at || payloadForm.submission_time || ''),
              source: clean(formRow.source || payloadForm.source || 'web'),
              username: clean(formRow.username || payloadForm.username || '')
            });

            const existing = findExistingReleaseByFormOrSubmission(
              uid,
              formId,
              formRow.submission_key || '',
              Number(formRow.moderation_message_id || 0)
            );
            if (existing?.rel) {
              const exRel = existing.rel;
              const exMid = Number(exRel.moderation_message_id || formRow.moderation_message_id || 0);
              if (exMid && shouldStatusStayPinned(exRel.status) && !exRel.moderation_pinned && !exRel.manual_unpinned) {
                const pRes = await pinModerationMessageNow(exMid, { immediateRetry: true });
                exRel.moderation_pinned = !!pRes.ok;
                exRel.pin_pending = !pRes.ok && !pRes.missingRights;
                saveDb();
                syncModerationMirror(existing.uid, existing.idx, exRel);
                saveModDb();
                await refreshModerationMessage(existing.uid, existing.idx, exRel, exMid).catch(() => {});
              }
              sendJson(res, 200, {
                ok: true,
                duplicate: true,
                user_id: existing.uid,
                idx: existing.idx,
                moderation_message_id: exMid || null,
                moderation_pinned: !!exRel.moderation_pinned
              });
              return;
            }

            const existingMsgId = Number(formRow.moderation_message_id || payloadForm.moderation_message_id || 0);
            if (existingMsgId > 0) {
              db[uid] = Array.isArray(db[uid]) ? db[uid] : [];
              const idx = db[uid].length;
              const importedRel = {
                ...releaseData,
                release_idx: idx,
                supabase_form_id: formId,
                moderation_message_id: existingMsgId,
                manual_unpinned: !!payloadForm.manual_unpinned
              };
              let pinRes = { ok: !!payloadForm.moderation_pinned };
              if (shouldStatusStayPinned(importedRel.status) && !importedRel.manual_unpinned && !pinRes.ok) {
                pinRes = await pinModerationMessageNow(existingMsgId, { immediateRetry: true });
              }
              importedRel.moderation_pinned = !!pinRes.ok;
              importedRel.pin_pending = !pinRes.ok && !pinRes.missingRights && shouldStatusStayPinned(importedRel.status);
              db[uid].push(importedRel);
              saveDb();
              syncModerationMirror(uid, idx, importedRel);
              saveModDb();
              await refreshModerationMessage(uid, idx, importedRel, existingMsgId).catch(() => {});
              if (importedRel.pin_pending) {
                scheduleModerationPinRetry(uid, idx, existingMsgId, Math.max(5, (pinRes.retryAfter || 4) + 1));
              }
              await supabasePatchFormByRelease(uid, importedRel, {
                status: mapReleaseStatusToFormStatus(importedRel.status),
                moderation_message_id: existingMsgId,
                form_payload: safeJson(importedRel) || {}
              });
              sendJson(res, 200, {
                ok: true,
                user_id: uid,
                idx,
                moderation_message_id: existingMsgId,
                moderation_pinned: !!importedRel.moderation_pinned
              });
              return;
            }

            const fakeUser = { id: Number(uid) || 0, username: releaseData.username || releaseData.tg || '' };
            const out = await submitReleaseToModeration(fakeUser, uid, releaseData, releaseData.source || 'web', {
              existing_form_id: formId
            });

            await supabasePatchFormByRelease(uid, { ...(out?.rel || releaseData), supabase_form_id: formId }, {
              status: mapReleaseStatusToFormStatus(out?.rel?.status || STATUS.ON_UPLOAD),
              moderation_message_id: Number(out?.rel?.moderation_message_id || 0) || null,
              form_payload: safeJson(out?.rel || releaseData) || {}
            });

            sendJson(res, 200, {
              ok: true,
              user_id: uid,
              idx: out?.idx ?? null,
              moderation_message_id: out?.rel?.moderation_message_id ?? null,
              moderation_pinned: !!out?.rel?.moderation_pinned
            });
          } catch (e) {
            console.error('[api/new-release] failed:', clean(e?.message || e));
            sendJson(res, 500, { ok: false, error: 'internal_error' });
          }
        });
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
      if (u.pathname === '/api/miniapp/platform/catalog' && req.method === 'GET') {
        Promise.all([
          supabaseSelectAll(SUPABASE_PUBLIC_RELEASES_TABLE, 'form_id,telegram_id,username,artist_name,track_name,genre,release_type,status,approved_at,release_data,updated_at', 'approved_at.desc'),
          supabaseSelectAll('cxrner_release_engagements', 'release_id,kind', 'created_at.desc').catch(() => [])
        ]).then(([rows, engagements]) => {
            const approved = (rows || []).filter((row) => ['approved', 'published'].includes(clean(row?.status || '').toLowerCase())).slice(0, 100);
            const counts = new Map();
            for (const engagement of engagements || []) {
              const id = clean(engagement.release_id || '');
              if (!id) continue;
              const current = counts.get(id) || { likes: 0, favorites: 0 };
              if (engagement.kind === 'like') current.likes += 1;
              if (engagement.kind === 'favorite') current.favorites += 1;
              counts.set(id, current);
            }
            const mapRelease = (row) => platformRelease(row, counts.get(clean(row?.form_id || '')) || {});
            sendJson(res, 200, { ok: true, releases: approved.map(mapRelease), total: approved.length, showcase: approved.slice(0, 10).map(mapRelease) });
          })
          .catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/link' && req.method === 'GET') {
        const slug = clean(u.searchParams.get('slug') || '');
        if (u.searchParams.get('mine') === '1') {
          authorizeMiniApp(req).then(async (auth) => {
            if (!auth.ok) { sendJson(res, auth.status, auth); return; }
            const links = await supabaseSelectWhere(SUPABASE_LINKS_TABLE, 'id,telegram_id,release_id,slug,title,artist,genre,release_date,cover_url,platforms,extra_links,theme,settings,status,created_at,updated_at', [{ key: 'telegram_id', value: auth.uid }], 'updated_at.desc', 100);
            sendJson(res, 200, { ok: true, links: (links || []).map(normalizeLinkRow) });
          }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
          return;
        }
        if (!slug) { sendJson(res, 400, { ok: false, error: 'slug is required' }); return; }
        supabaseSelectWhere(SUPABASE_LINKS_TABLE, 'id,telegram_id,release_id,slug,title,artist,genre,release_date,cover_url,platforms,extra_links,theme,settings,status,created_at,updated_at', [{ key: 'slug', value: slug }, { key: 'status', value: 'published' }], '', 1)
          .then((rows) => { if (!rows?.[0]) { sendJson(res, 404, { ok: false, error: 'link not found' }); return; } sendJson(res, 200, { ok: true, link: normalizeLinkRow(rows[0]) }); })
          .catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/link/lookup' && req.method === 'GET') {
        const upc = clean(u.searchParams.get('upc') || '').replace(/[^0-9A-Za-z-]/g, '').slice(0, 80);
        if (!upc) { sendJson(res, 400, { ok: false, error: 'UPC is required' }); return; }
        supabaseSelectAll(SUPABASE_PUBLIC_RELEASES_TABLE, 'form_id,telegram_id,username,artist_name,track_name,genre,release_type,status,approved_at,release_data,updated_at', 'approved_at.desc')
          .then((rows) => {
            const found = (rows || []).find((row) => ['approved', 'published'].includes(clean(row.status || '').toLowerCase()) && clean(row.release_data?.upc || row.release_data?.UPC || '') === upc);
            if (!found) { sendJson(res, 404, { ok: false, error: 'Релиз с таким UPC не найден среди опубликованных' }); return; }
            const release = platformRelease(found);
            const payload = found.release_data && typeof found.release_data === 'object' ? found.release_data : {};
            const platforms = Object.fromEntries(Object.entries(release.dsp_links || {}).filter(([, value]) => isHttpUrl(String(value || ''))));
            sendJson(res, 200, { ok: true, release: { ...release, upc, platforms, release_id: found.form_id, owner_id: found.telegram_id } });
          })
          .catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/link' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const action = clean(body.action || 'create').toLowerCase();
          const linkId = clean(body.link_id || body.id || '');
          if (action === 'delete') {
            if (!linkId) { sendJson(res, 400, { ok: false, error: 'link_id is required' }); return; }
            await supabaseRequest(`${SUPABASE_LINKS_TABLE}?id=eq.${encodeURIComponent(linkId)}&telegram_id=eq.${encodeURIComponent(auth.uid)}`, { method: 'DELETE' });
            sendJson(res, 200, { ok: true, deleted: true });
            return;
          }
          let source = { title: clean(body.title || 'Без названия'), artist: clean(body.artist || 'Артист'), genre: clean(body.genre || ''), release_date: clean(body.release_date || ''), cover_url: clean(body.cover_url || ''), platforms: body.platforms && typeof body.platforms === 'object' ? body.platforms : {} };
          let releaseId = clean(body.release_id || '');
          let releaseFromPublic = false;
          if (!releaseId && clean(body.upc || '')) {
            const publicRows = await supabaseSelectAll(SUPABASE_PUBLIC_RELEASES_TABLE, 'form_id,telegram_id,artist_name,track_name,genre,release_data,status', 'approved_at.desc');
            const found = (publicRows || []).find((row) => clean(row?.release_data?.upc || row?.release_data?.UPC || '') === clean(body.upc));
            if (found) {
              releaseId = clean(found.form_id || '');
              releaseFromPublic = true;
              const release = platformRelease(found);
              source = { title: release.title, artist: release.artist, genre: release.genre, release_date: release.date, cover_url: release.cover, platforms: release.dsp_links || {} };
            }
          }
          if (releaseId && !releaseFromPublic) {
            const forms = await supabaseSelectWhere(SUPABASE_FORMS_TABLE, 'id,telegram_id,track_name,artist_name,genre,form_payload', [{ key: 'id', value: releaseId }, { key: 'telegram_id', value: auth.uid }], '', 1);
            if (!forms?.[0]) { sendJson(res, 404, { ok: false, error: 'release not found or access denied' }); return; }
            source = { ...linkSourceFromRelease(forms[0]), ...source };
            if (!body.title) source.title = linkSourceFromRelease(forms[0]).title;
            if (!body.artist) source.artist = linkSourceFromRelease(forms[0]).artist;
            if (!body.cover_url) source.cover_url = linkSourceFromRelease(forms[0]).cover_url;
            if (!body.genre) source.genre = linkSourceFromRelease(forms[0]).genre;
            if (!body.release_date) source.release_date = linkSourceFromRelease(forms[0]).release_date;
            if (!body.platforms || !Object.keys(body.platforms).length) source.platforms = linkSourceFromRelease(forms[0]).platforms;
          }
          if (source.cover_url && (!isHttpUrl(source.cover_url) || source.cover_url.length > 700)) { sendJson(res, 400, { ok: false, error: 'cover_url must be a valid http(s) URL' }); return; }
          const theme = linkThemes.includes(clean(body.theme)) ? clean(body.theme) : 'aurora';
          const extraLinks = Array.isArray(body.extra_links) ? body.extra_links.map((item) => ({ label: clean(item?.label || '').slice(0, 60), url: clean(item?.url || '').slice(0, 700) })).filter((item) => item.label && isHttpUrl(item.url)).slice(0, 50) : [];
          let slug = linkSlug(body.slug || `${source.artist}-${source.title}`, `release-${Date.now().toString(36)}`);
          if (action === 'update' && linkId) {
            const current = await supabaseSelectWhere(SUPABASE_LINKS_TABLE, 'id,slug', [{ key: 'id', value: linkId }, { key: 'telegram_id', value: auth.uid }], '', 1);
            if (!current?.[0]) { sendJson(res, 404, { ok: false, error: 'link not found or access denied' }); return; }
            slug = linkSlug(body.slug || current[0].slug);
          }
          let suffix = 2;
          while (true) {
            const conflict = await supabaseSelectWhere(SUPABASE_LINKS_TABLE, 'id', [{ key: 'slug', value: slug }], '', 1);
            if (!conflict?.[0] || String(conflict[0].id) === linkId) break;
            slug = `${linkSlug(body.slug || `${source.artist}-${source.title}`)}-${suffix++}`;
          }
          const row = { telegram_id: Number(auth.uid), release_id: releaseId || null, slug, title: source.title, artist: source.artist, genre: source.genre, release_date: source.release_date, cover_url: source.cover_url, platforms: source.platforms, extra_links: extraLinks, theme, settings: body.settings && typeof body.settings === 'object' ? body.settings : {}, status: body.status === 'draft' ? 'draft' : 'published', updated_at: new Date().toISOString() };
          const result = action === 'update' && linkId
            ? await supabaseRequest(`${SUPABASE_LINKS_TABLE}?id=eq.${encodeURIComponent(linkId)}&telegram_id=eq.${encodeURIComponent(auth.uid)}`, { method: 'PATCH', headers: { Prefer: 'return=representation' }, body: row })
            : await supabaseRequest(SUPABASE_LINKS_TABLE, { method: 'POST', headers: { Prefer: 'return=representation' }, body: [{ ...row, created_at: new Date().toISOString() }] });
          const saved = Array.isArray(result) ? result[0] : result;
          const publicUrl = `${linkBaseUrl()}/${encodeURIComponent(slug)}`;
          if (releaseId) {
            const currentForm = await supabaseSelectWhere(SUPABASE_FORMS_TABLE, 'id,form_payload', [{ key: 'id', value: releaseId }, { key: 'telegram_id', value: auth.uid }], '', 1).catch(() => []);
            if (currentForm?.[0]) {
              const formPayload = currentForm[0].form_payload && typeof currentForm[0].form_payload === 'object' ? currentForm[0].form_payload : {};
              await supabaseRequest(`${SUPABASE_FORMS_TABLE}?id=eq.${encodeURIComponent(releaseId)}&telegram_id=eq.${encodeURIComponent(auth.uid)}`, { method: 'PATCH', body: { form_payload: { ...formPayload, smart_link: publicUrl }, updated_at: new Date().toISOString() } }).catch(() => {});
            }
            const publicRows = await supabaseSelectWhere(SUPABASE_PUBLIC_RELEASES_TABLE, 'form_id,release_data', [{ key: 'form_id', value: releaseId }], '', 1).catch(() => []);
            if (publicRows?.[0]) await supabaseRequest(`${SUPABASE_PUBLIC_RELEASES_TABLE}?form_id=eq.${encodeURIComponent(releaseId)}`, { method: 'PATCH', body: { release_data: { ...(publicRows[0].release_data || {}), smart_link: publicUrl }, updated_at: new Date().toISOString() } }).catch(() => {});
            const localList = Array.isArray(db?.[auth.uid]) ? db[auth.uid] : [];
            const localIndex = localList.findIndex((release) => String(release?.supabase_form_id || '') === releaseId);
            if (localIndex >= 0) { localList[localIndex].smart_link = publicUrl; saveDb(); }
          }
          sendJson(res, 200, { ok: true, link: normalizeLinkRow(saved || { ...row, id: linkId }), public_url: publicUrl });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/link/event' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const slug = clean(body.slug || '');
          const eventType = ['view', 'click'].includes(clean(body.event_type)) ? clean(body.event_type) : 'view';
          const rows = await supabaseSelectWhere(SUPABASE_LINKS_TABLE, 'id', [{ key: 'slug', value: slug }, { key: 'status', value: 'published' }], '', 1);
          if (!rows?.[0]) { sendJson(res, 404, { ok: false, error: 'link not found' }); return; }
          await supabaseRequest(SUPABASE_LINK_EVENTS_TABLE, { method: 'POST', headers: { Prefer: 'return=minimal' }, body: [{ link_id: rows[0].id, event_type: eventType, platform: clean(body.platform || '').slice(0, 80), source: clean(body.source || '').slice(0, 120), country: clean(body.country || '').slice(0, 80), device: clean(body.device || '').slice(0, 80), user_agent: clean(req.headers?.['user-agent'] || '').slice(0, 300) }] });
          sendJson(res, 200, { ok: true });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/link/analytics' && req.method === 'GET') {
        authorizeMiniApp(req).then(async (auth) => {
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const linkId = clean(u.searchParams.get('link_id') || '');
          const links = await supabaseSelectWhere(SUPABASE_LINKS_TABLE, 'id,slug,title', [{ key: 'id', value: linkId }, { key: 'telegram_id', value: auth.uid }], '', 1);
          if (!links?.[0]) { sendJson(res, 404, { ok: false, error: 'link not found or access denied' }); return; }
          const events = await supabaseSelectWhere(SUPABASE_LINK_EVENTS_TABLE, 'event_type,platform,source,country,device,created_at', [{ key: 'link_id', value: linkId }], 'created_at.desc', 5000);
          const views = (events || []).filter((event) => event.event_type === 'view').length;
          const clicks = (events || []).filter((event) => event.event_type === 'click').length;
          const platforms = {}; for (const event of events || []) if (event.event_type === 'click') platforms[event.platform || 'unknown'] = (platforms[event.platform || 'unknown'] || 0) + 1;
          sendJson(res, 200, { ok: true, link: links[0], analytics: { views, clicks, ctr: views ? Math.round(clicks / views * 1000) / 10 : 0, platforms, recent: (events || []).slice(0, 30) } });
        }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      // Aggregated dashboard feed. It intentionally composes existing tables so the
      // current profile/catalog endpoints and Telegram moderation flow stay intact.
      if (u.pathname === '/api/miniapp/platform/home' && req.method === 'GET') {
        authorizeMiniApp(req).then(async (auth) => {
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const [profileRows, forms, publicRows, engagements, listens, follows, notifications, news] = await Promise.all([
            supabaseSelectWhere('cxrner_telegram_profiles', 'telegram_id,username,first_name,last_name,photo_url,role,status,registered_at,last_seen_at,metadata', [{ key: 'telegram_id', value: auth.uid }], '', 1).catch(() => []),
            supabaseSelectWhere(SUPABASE_FORMS_TABLE, 'id,telegram_id,artist_name,track_name,genre,release_type,status,reject_reason,upc,created_at,updated_at,form_payload', [{ key: 'telegram_id', value: auth.uid }], 'created_at.desc', 200).catch(() => []),
            supabaseSelectAll(SUPABASE_PUBLIC_RELEASES_TABLE, 'form_id,telegram_id,username,artist_name,track_name,genre,release_type,status,approved_at,release_data,updated_at', 'approved_at.desc').catch(() => []),
            supabaseSelectAll('cxrner_release_engagements', 'release_id,telegram_id,kind,created_at', 'created_at.desc').catch(() => []),
            supabaseSelectAll('cxrner_listen_events', 'release_id,telegram_id,seconds_played,created_at', 'created_at.desc').catch(() => []),
            supabaseSelectAll('cxrner_artist_follows', 'follower_id,following_id,created_at', 'created_at.desc').catch(() => []),
            supabaseSelectWhere('cxrner_notifications', 'id,type,title,body,read_at,created_at', [{ key: 'telegram_id', value: auth.uid }], 'created_at.desc', 12).catch(() => []),
            supabaseSelectAll('cxrner_news', 'id,title,body,cover_url,published,published_at,created_at', 'published_at.desc').catch(() => [])
          ]);
          const countMap = new Map();
          for (const row of engagements || []) {
            const id = clean(row.release_id || ''); if (!id) continue;
            const value = countMap.get(id) || { likes: 0, favorites: 0 };
            if (row.kind === 'like') value.likes += 1;
            if (row.kind === 'favorite') value.favorites += 1;
            countMap.set(id, value);
          }
          const listenMap = new Map();
          for (const row of listens || []) listenMap.set(clean(row.release_id || ''), (listenMap.get(clean(row.release_id || '')) || 0) + 1);
          const releases = (publicRows || []).filter((row) => ['approved', 'published'].includes(clean(row.status || '').toLowerCase())).map((row) => ({ ...platformRelease(row, countMap.get(clean(row.form_id || '')) || {}), listens: listenMap.get(clean(row.form_id || '')) || 0 }));
          const latest = releases.slice(0, 10);
          const popular = [...releases].sort((a, b) => ((b.likes || 0) + (b.favorites || 0) * 0.5 + (b.listens || 0)) - ((a.likes || 0) + (a.favorites || 0) * 0.5 + (a.listens || 0))).slice(0, 10);
          const profile = profileRows?.[0] || {};
          const approvedOwn = (forms || []).filter((row) => ['approved', 'published'].includes(clean(row.status || '').toLowerCase())).length;
          const moderationOwn = (forms || []).filter((row) => ['moderation', 'on_moderation', 'pending'].includes(clean(row.status || '').toLowerCase())).length;
          const followers = (follows || []).filter((row) => Number(row.following_id) === Number(auth.uid)).length;
          const following = (follows || []).filter((row) => Number(row.follower_id) === Number(auth.uid)).length;
          const ownReleaseIds = new Set((forms || []).map((row) => String(row.id)));
          const ownLikes = (engagements || []).filter((row) => ownReleaseIds.has(String(row.release_id)) && row.kind === 'like').length;
          const ownListens = (listens || []).filter((row) => ownReleaseIds.has(String(row.release_id))).length;
          const [resolvedVault, userLinks, cachedCabRows] = await Promise.all([
            resolveMiniAppVault(auth.uid, auth.user).catch(() => null),
            supabaseSelectWhere(SUPABASE_LINKS_TABLE, 'id', [{ key: 'telegram_id', value: auth.uid }], '', 50).catch(() => []),
            getCabinetRowsCached(false).catch(() => [])
          ]);
          const lastScanMs = Number(new Date(resolvedVault?.vault?.ecosystem_daily_scan_at || 0).getTime()) || 0;
          const canDailyScan = !lastScanMs || (Date.now() - lastScanMs) >= 20 * 3600 * 1000;
          const botUsername = clean(moderationHealth.bot_username || envStr('BOT_USERNAME', '')).replace(/^@/, '');
          sendJson(res, 200, {
            ok: true,
            build_version: BOT_BUILD_VERSION,
            bot_username: botUsername,
            user: { ...profile, telegram_id: Number(auth.uid), photo_url: profile.photo_url || auth.user.photo_url || '' },
            stats: { releases: forms?.length || 0, approved: approvedOwn, moderation: moderationOwn, views: 0, listens: ownListens, likes: ownLikes, comments: 0, followers, following, links: userLinks?.length || 0 },
            latest,
            popular,
            recommended: latest.slice(0, 6),
            news: (news || []).filter((row) => row.published !== false).slice(0, 8),
            notifications: notifications || [],
            forms: forms || [],
            is_admin: auth.is_admin,
            wallet: resolvedVault?.vault?.ecosystem_wallet || { balance: 150, total_earned: 150, total_spent: 0 },
            can_daily_scan: canDailyScan,
            inventory_count: resolvedVault?.vault?.ecosystem_inventory?.length || 0,
            verification_status: resolvedVault?.vault?.ecosystem_verification?.status || (resolvedVault?.isCeo ? 'verified' : 'not_verified'),
            site_profile: resolvedVault?.vault?.ecosystem_profile || profile.metadata?.artist_profile || null,
            referrals_count: (resolvedVault?.vault?.ecosystem_referrals?.friends || []).length,
            recent_drops: buildGlobalDropFeed(cachedCabRows, 10)
          });
        }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/activity' && req.method === 'GET') {
        authorizeMiniApp(req).then(async (auth) => {
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const [notifications, forms] = await Promise.all([
            supabaseSelectWhere('cxrner_notifications', 'id,type,title,body,read_at,created_at', [{ key: 'telegram_id', value: auth.uid }], 'created_at.desc', 100).catch(() => []),
            supabaseSelectWhere(SUPABASE_FORMS_TABLE, 'id,track_name,artist_name,status,created_at,updated_at,reject_reason', [{ key: 'telegram_id', value: auth.uid }], 'updated_at.desc', 50).catch(() => [])
          ]);
          sendJson(res, 200, { ok: true, items: [...(notifications || []).map((x) => ({ ...x, kind: 'notification' })), ...(forms || []).map((x) => ({ id: 'form-' + x.id, kind: 'release', title: x.track_name || 'Релиз', body: x.reject_reason || x.status, created_at: x.updated_at || x.created_at }))].sort((a, b) => new Date(b.created_at || 0) - new Date(a.created_at || 0)).slice(0, 100) });
        }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/calendar' && req.method === 'GET') {
        supabaseSelectAll(SUPABASE_PUBLIC_RELEASES_TABLE, 'form_id,telegram_id,artist_name,track_name,genre,status,approved_at,release_data,updated_at', 'approved_at.desc').then((rows) => {
          const items = (rows || []).filter((row) => ['approved', 'published'].includes(clean(row.status || '').toLowerCase())).map((row) => platformRelease(row)).filter((row) => row.date || row.release_date);
          sendJson(res, 200, { ok: true, releases: items.sort((a, b) => new Date(a.date || a.release_date || 0) - new Date(b.date || b.release_date || 0)) });
        }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/rankings' && req.method === 'GET') {
        Promise.all([
          supabaseSelectAll(SUPABASE_PUBLIC_RELEASES_TABLE, 'form_id,telegram_id,artist_name,track_name,genre,status,approved_at,release_data,updated_at', 'approved_at.desc'),
          supabaseSelectAll('cxrner_release_engagements', 'release_id,kind,created_at', 'created_at.desc').catch(() => []),
          supabaseSelectAll('cxrner_listen_events', 'release_id,created_at', 'created_at.desc').catch(() => [])
        ]).then(([rows, engagements, listens]) => {
          const artists = new Map();
          for (const row of rows || []) {
            if (!['approved', 'published'].includes(clean(row.status || '').toLowerCase())) continue;
            const id = String(row.telegram_id || 'unknown'); const item = artists.get(id) || { telegram_id: row.telegram_id, artist: row.artist_name || 'Артист', releases: 0, likes: 0, listens: 0 };
            item.releases += 1; artists.set(id, item);
          }
          const releaseArtist = new Map((rows || []).map((row) => [String(row.form_id), String(row.telegram_id || 'unknown')]));
          for (const row of engagements || []) { const item = artists.get(releaseArtist.get(String(row.release_id))); if (item && row.kind === 'like') item.likes += 1; }
          for (const row of listens || []) { const item = artists.get(releaseArtist.get(String(row.release_id))); if (item) item.listens += 1; }
          const sort = clean(u.searchParams.get('sort') || 'likes'); const result = [...artists.values()].sort((a, b) => (b[sort] || 0) - (a[sort] || 0)).slice(0, 50); sendJson(res, 200, { ok: true, rankings: result });
        }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/profile' && (req.method === 'GET' || req.method === 'POST')) {
        const profileRequest = req.method === 'POST' ? readJsonBody(req).catch(() => ({})) : Promise.resolve({});
        profileRequest.then((profileBody) => authorizeMiniApp(req, profileBody))
          .then(async (auth) => {
            if (!auth.ok) { sendJson(res, auth.status, auth); return; }
            const [profiles, releases, engagements, badges, achievements, notifications, cabinet, interactions, artistProfile] = await Promise.all([
              supabaseSelectWhere('cxrner_telegram_profiles', 'telegram_id,username,first_name,last_name,photo_url,role,status,registered_at,last_seen_at,metadata', [{ key: 'telegram_id', value: auth.uid }], '', 1),
              supabaseSelectWhere(SUPABASE_FORMS_TABLE, 'id,telegram_id,username,artist_name,track_name,genre,release_type,status,reject_reason,upc,moderation_message_id,submission_key,source,created_at,updated_at,form_payload', [{ key: 'telegram_id', value: auth.uid }], 'created_at.desc', 200),
              supabaseSelectWhere('cxrner_release_engagements', 'release_id,kind,created_at', [{ key: 'telegram_id', value: auth.uid }], 'created_at.desc', 500),
              supabaseSelectWhere('cxrner_artist_badges', 'badge_id,assigned_at', [{ key: 'telegram_id', value: auth.uid }], 'assigned_at.desc', 100),
              supabaseSelectWhere('cxrner_user_achievements', 'achievement_id,awarded_at', [{ key: 'telegram_id', value: auth.uid }], 'awarded_at.desc', 100),
              supabaseSelectWhere('cxrner_notifications', 'id,type,title,body,read_at,created_at', [{ key: 'telegram_id', value: auth.uid }], 'created_at.desc', 50),
              getCabinetSnapshot(auth.uid),
              supabaseSelectWhere(SUPABASE_INTERACTIONS_TABLE, 'user_id,release_idx,interaction_idx,interaction,occurred_at,updated_at', [{ key: 'user_id', value: auth.uid }], 'occurred_at.asc', 1000).catch((error) => { console.warn('[PLATFORM] interaction history unavailable:', clean(error?.message || error)); return []; }),
              supabaseSelectWhere('cxrner_artist_profiles', 'telegram_id,display_name,bio,avatar_url,socials,theme,updated_at', [{ key: 'telegram_id', value: auth.uid }], '', 1).catch(() => [])
            ]);
            const profile = profiles?.[0] || {};
            sendJson(res, 200, { ok: true, is_admin: auth.is_admin, user: { ...profile, telegram_id: Number(auth.uid), photo_url: profile.photo_url || auth.user.photo_url || '' }, artist_profile: artistProfile?.[0] || profile.metadata?.artist_profile || {}, releases: releases || [], release_interactions: interactions || [], engagements: engagements || [], badges: badges || [], achievements: achievements || [], notifications: notifications || [], cabinet: cabinet || {}, stats: { releases: releases?.length || 0, likes: (engagements || []).filter((x) => x.kind === 'like').length, favorites: (engagements || []).filter((x) => x.kind === 'favorite').length, listens: 0 } });
          })
          .catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/release' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const releaseId = clean(body.release_id || body.form_id || '');
          if (!releaseId) { sendJson(res, 400, { ok: false, error: 'release_id is required' }); return; }
          const rows = await supabaseSelectWhere(SUPABASE_FORMS_TABLE, 'id,telegram_id,status,reject_reason,submission_key,form_payload', [
            { key: 'id', value: releaseId },
            { key: 'telegram_id', value: auth.uid }
          ], '', 1);
          const formRow = rows?.[0];
          if (!formRow) { sendJson(res, 404, { ok: false, error: 'release not found or access denied' }); return; }
          const payload = formRow.form_payload && typeof formRow.form_payload === 'object' ? { ...formRow.form_payload } : {};
          const list = Array.isArray(db?.[auth.uid]) ? db[auth.uid] : [];
          const localIndex = list.findIndex((release) => String(release?.supabase_form_id || '') === releaseId || String(release?.submission_time || '') === String(formRow.submission_key || ''));
          const action = clean(body.action || 'update').toLowerCase();
          if (action === 'delete') {
            const now = new Date().toISOString();
            const deletedPayload = { ...payload, user_deleted: true, deleted_at: now };
            await supabaseRequest(`${SUPABASE_FORMS_TABLE}?id=eq.${encodeURIComponent(releaseId)}&telegram_id=eq.${encodeURIComponent(auth.uid)}`, { method: 'PATCH', body: { status: FORM_STATUS.REJECTED, reject_reason: 'Удалено артистом', form_payload: deletedPayload, updated_at: now } });
            await supabaseRequest(`${SUPABASE_PUBLIC_RELEASES_TABLE}?form_id=eq.${encodeURIComponent(releaseId)}`, { method: 'DELETE' }).catch(() => {});
            if (localIndex >= 0) {
              list[localIndex].user_deleted = true;
              list[localIndex].deleted_at = now;
              list[localIndex].status = STATUS.DELETED;
              await syncModerationPinForRelease(list[localIndex]).catch(() => {});
              saveDb();
              syncModerationMirror(auth.uid, localIndex, list[localIndex]);
              saveModDb();
              await refreshModerationMessage(auth.uid, localIndex, list[localIndex]).catch(() => {});
            }
            console.info(`[PLATFORM] release deleted: user_id=${auth.uid} form_id=${releaseId}`);
            sendJson(res, 200, { ok: true, deleted: true, release_id: releaseId });
            return;
          }
          const coverUrl = normalizeImageUrl(body.cover_url || '');
          const smartLink = clean(body.smart_link || body.multilink || '');
          if (coverUrl && (!isHttpUrl(coverUrl) || coverUrl.length > 700)) { sendJson(res, 400, { ok: false, error: 'cover_url must be a valid http(s) URL' }); return; }
          if (smartLink && (!isHttpUrl(smartLink) || smartLink.length > 700)) { sendJson(res, 400, { ok: false, error: 'smart_link must be a valid http(s) URL' }); return; }
          const updatedPayload = { ...payload, cover_url: coverUrl, smart_link: smartLink };
          const now = new Date().toISOString();
          try { await supabaseRequest(`${SUPABASE_FORMS_TABLE}?id=eq.${encodeURIComponent(releaseId)}&telegram_id=eq.${encodeURIComponent(auth.uid)}`, { method: 'PATCH', body: { form_payload: updatedPayload, updated_at: now } }); } catch (error) { console.warn(`[PLATFORM] form media update failed form_id=${releaseId}:`, clean(error?.message || error)); }
          const publicRows = await supabaseSelectWhere(SUPABASE_PUBLIC_RELEASES_TABLE, 'form_id,release_data', [{ key: 'form_id', value: releaseId }], '', 1).catch(() => []);
          if (publicRows?.[0]) await supabaseRequest(`${SUPABASE_PUBLIC_RELEASES_TABLE}?form_id=eq.${encodeURIComponent(releaseId)}`, { method: 'PATCH', body: { release_data: { ...(publicRows[0].release_data || {}), ...updatedPayload }, updated_at: now } }).catch(() => {});
          if (localIndex >= 0) {
            list[localIndex].cover_url = coverUrl;
            list[localIndex].smart_link = smartLink;
            saveDb();
          }
          console.info(`[PLATFORM] release media updated: user_id=${auth.uid} form_id=${releaseId}`);
          sendJson(res, 200, { ok: true, release_id: releaseId, cover_url: coverUrl, smart_link: smartLink });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/engagement' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const releaseId = clean(body.release_id || '');
          const kind = clean(body.kind || '');
          if (!releaseId || !['like', 'favorite'].includes(kind)) { sendJson(res, 400, { ok: false, error: 'release_id and valid kind are required' }); return; }
          console.info(`[PLATFORM] engagement: user_id=${auth.uid} release_id=${releaseId} kind=${kind} active=${body.active !== false}`);
          const query = `release_id=eq.${encodeURIComponent(releaseId)}&telegram_id=eq.${encodeURIComponent(auth.uid)}&kind=eq.${encodeURIComponent(kind)}`;
          if (body.active === false) await supabaseRequest(`cxrner_release_engagements?${query}`, { method: 'DELETE' });
          else await supabaseRequest('cxrner_release_engagements?on_conflict=release_id,telegram_id,kind', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates,return=minimal' }, body: [{ release_id: releaseId, telegram_id: Number(auth.uid), kind }] });
          sendJson(res, 200, { ok: true, active: body.active !== false, release_id: releaseId, kind });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/comments' && req.method === 'GET') {
        const releaseId = clean(u.searchParams.get('release_id') || '');
        if (!releaseId) { sendJson(res, 400, { ok: false, error: 'release_id is required' }); return; }
        supabaseSelectWhere('cxrner_comments', 'id,release_id,telegram_id,parent_id,body,is_pinned,created_at,updated_at', [{ key: 'release_id', value: releaseId }], 'created_at.asc', 100)
          .then(async (rows) => {
            const ids = [...new Set((rows || []).map((row) => String(row.telegram_id || '')).filter(Boolean))];
            const profiles = ids.length
              ? await supabaseSelectAll('cxrner_telegram_profiles', 'telegram_id,username,first_name,last_name,photo_url', 'last_seen_at.desc')
              : [];
            const byId = new Map((profiles || []).map((profile) => [String(profile.telegram_id), profile]));
            sendJson(res, 200, { ok: true, comments: (rows || []).map((row) => ({ ...row, author: byId.get(String(row.telegram_id)) || null })) });
          })
          .catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/comments' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const releaseId = clean(body.release_id || ''), comment = clean(body.body || '');
          if (!releaseId || !comment || comment.length > 2000) { sendJson(res, 400, { ok: false, error: 'release_id and comment are required' }); return; }
          const rows = await supabaseRequest('cxrner_comments', { method: 'POST', headers: { Prefer: 'return=representation' }, body: [{ release_id: releaseId, telegram_id: Number(auth.uid), parent_id: body.parent_id || null, body: comment }] });
          sendJson(res, 200, { ok: true, comment: rows?.[0] || null });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/comments' && (req.method === 'PATCH' || req.method === 'DELETE')) {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body), commentId = clean(body.id || u.searchParams.get('id') || '');
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!commentId) { sendJson(res, 400, { ok: false, error: 'comment id is required' }); return; }
          if (req.method === 'DELETE') {
            await supabaseRequest(`cxrner_comments?id=eq.${encodeURIComponent(commentId)}&telegram_id=eq.${encodeURIComponent(auth.uid)}`, { method: 'DELETE' });
          } else {
            if (!auth.is_admin || body.action !== 'pin') { sendJson(res, 403, { ok: false, error: 'only admin can pin comments' }); return; }
            await supabaseRequest(`cxrner_comments?id=eq.${encodeURIComponent(commentId)}`, { method: 'PATCH', body: { is_pinned: Boolean(body.pinned), updated_at: new Date().toISOString() } });
          }
          sendJson(res, 200, { ok: true });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/comment-like' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body), commentId = clean(body.comment_id || '');
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!commentId) { sendJson(res, 400, { ok: false, error: 'comment_id is required' }); return; }
          const query = `comment_id=eq.${encodeURIComponent(commentId)}&telegram_id=eq.${encodeURIComponent(auth.uid)}`;
          if (body.active === false) await supabaseRequest(`cxrner_comment_likes?${query}`, { method: 'DELETE' });
          else await supabaseRequest('cxrner_comment_likes?on_conflict=comment_id,telegram_id', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates,return=minimal' }, body: [{ comment_id: commentId, telegram_id: Number(auth.uid) }] });
          sendJson(res, 200, { ok: true });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/referrals' && (req.method === 'GET' || req.method === 'POST')) {
        const referralBody = req.method === 'POST' ? readJsonBody(req).catch(() => ({})) : Promise.resolve({});
        referralBody.then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const resolved = await resolveMiniAppVault(auth.uid, auth.user);
          const action = clean(body.action || '').toLowerCase();
          if (req.method === 'POST' && (action === 'register_ref' || body.referrer_id || body.ref_code)) {
            const refResult = await processReferralSignup(body.referrer_id || body.ref_code || '', auth.uid, auth.user);
            const refreshed = await resolveMiniAppVault(auth.uid, auth.user);
            sendJson(res, 200, { ok: true, ref_result: refResult, wallet: refreshed.vault.ecosystem_wallet });
            return;
          }
          if (req.method === 'POST' && body.invited_id) {
            await processReferralSignup(auth.uid, body.invited_id, {});
          }
          const [rows, events, allProfiles] = await Promise.all([
            supabaseSelectWhere('cxrner_referrals', 'telegram_id,code,invited_by,bonus_balance,created_at', [{ key: 'telegram_id', value: auth.uid }], '', 1).catch(() => []),
            supabaseSelectWhere('cxrner_referral_events', 'invited_id,event_type,bonus,created_at', [{ key: 'referrer_id', value: auth.uid }], 'created_at.desc', 500).catch(() => []),
            supabaseSelectAll('cxrner_telegram_profiles', 'telegram_id,username,first_name,last_name,photo_url,metadata', 'last_seen_at.desc').catch(() => [])
          ]);
          const profileMap = new Map((allProfiles || []).map((p) => [String(p.telegram_id), p]));
          const vaultFriends = Array.isArray(resolved.vault.ecosystem_referrals?.friends) ? resolved.vault.ecosystem_referrals.friends : [];
          const mergedMap = new Map();
          for (const f of vaultFriends) {
            const fid = clean(f?.invited_id);
            if (!fid) continue;
            mergedMap.set(fid, { ...f });
          }
          for (const ev of events || []) {
            const fid = clean(ev?.invited_id);
            if (!fid) continue;
            if (!mergedMap.has(fid)) {
              mergedMap.set(fid, { invited_id: ev.invited_id, event_type: ev.event_type || 'signup', bonus: Number(ev.bonus || 75), created_at: ev.created_at });
            }
          }
          const enrichedInvited = [...mergedMap.values()].map((item) => {
            const prof = profileMap.get(String(item.invited_id)) || {};
            const metaPhoto = prof?.metadata?.photo_url || '';
            const fullName = clean([prof.first_name, prof.last_name].filter(Boolean).join(' ') || item.display_name || prof.username || item.username || `Участник #${String(item.invited_id).slice(-4)}`);
            return {
              invited_id: item.invited_id,
              display_name: fullName,
              username: clean(item.username || prof.username || ''),
              photo_url: clean(item.photo_url || prof.photo_url || metaPhoto || ''),
              event_type: item.event_type || 'signup',
              bonus: Number(item.bonus || 75),
              created_at: item.created_at || new Date().toISOString()
            };
          }).sort((a, b) => new Date(b.created_at || 0) - new Date(a.created_at || 0));

          const claimedSteps = new Set(Array.isArray(resolved.vault.ecosystem_referrals?.claimed_steps) ? resolved.vault.ecosystem_referrals.claimed_steps.map(Number) : []);

          if (req.method === 'POST' && action === 'claim_milestone') {
            const stepNum = Number(body.step || 0);
            const milestone = CHAMPION_MILESTONES.find((m) => m.step === stepNum);
            if (!milestone) { sendJson(res, 400, { ok: false, error: 'Неизвестный шаг Пути чемпиона' }); return; }
            if (enrichedInvited.length < milestone.required) {
              sendJson(res, 400, { ok: false, error: `Нужно пригласить ${milestone.required} друзей (сейчас: ${enrichedInvited.length})` });
              return;
            }
            if (claimedSteps.has(stepNum)) {
              sendJson(res, 400, { ok: false, error: 'Награда за этот этап уже получена' });
              return;
            }
            const nowIso = new Date().toISOString();
            const bonusItems = [];
            if (milestone.reward_type === 'pack' && milestone.case_id) {
              const caseCfg = DROP_CASES_CONFIG.find((c) => c.id === milestone.case_id) || DROP_CASES_CONFIG[0];
              for (let i = 0; i < (milestone.packs_count || 1); i += 1) {
                const def = rollSpecimenForCase(caseCfg);
                bonusItems.push({
                  id: crypto.randomUUID(),
                  user_id: resolved.targetUserId,
                  item_id: def.item_id,
                  item_name: def.item_name,
                  specimen_code: def.specimen_code,
                  specimen_class: def.specimen_class,
                  collection_name: def.collection_name,
                  rarity: def.rarity,
                  serial_number: crypto.randomInt(1, 500),
                  description: def.description,
                  model_color: def.model_color,
                  shape: def.shape,
                  obtained_via: `champion_step_${stepNum}`,
                  is_pinned: false,
                  created_at: nowIso
                });
              }
            }
            claimedSteps.add(stepNum);
            const nextVault = await saveMiniAppVault(resolved, (prev) => {
              const w = prev.ecosystem_wallet || { user_id: resolved.targetUserId, balance: 150, total_earned: 150, total_spent: 0 };
              const cxpAdd = Number(milestone.reward_cxp || 0);
              return {
                ...prev,
                ecosystem_referrals: { ...(prev.ecosystem_referrals || {}), claimed_steps: [...claimedSteps] },
                ecosystem_wallet: { ...w, balance: Number(w.balance || 0) + cxpAdd, total_earned: Number(w.total_earned || 0) + cxpAdd, updated_at: nowIso },
                ecosystem_inventory: [...bonusItems, ...(Array.isArray(prev.ecosystem_inventory) ? prev.ecosystem_inventory : [])]
              };
            });
            sendJson(res, 200, {
              ok: true,
              step: stepNum,
              reward_cxp: milestone.reward_cxp,
              bonus_items: bonusItems,
              wallet: nextVault.ecosystem_wallet,
              inventory: nextVault.ecosystem_inventory,
              milestones: CHAMPION_MILESTONES.map((m) => ({
                ...m,
                unlocked: enrichedInvited.length >= m.required,
                claimed: claimedSteps.has(m.step)
              }))
            });
            return;
          }

          const botUsername = clean(moderationHealth.bot_username || envStr('BOT_USERNAME', '')).replace(/^@/, '');
          const refCode = `ref_${auth.uid}`;
          const miniappInviteUrl = botUsername ? `https://t.me/${botUsername}?startapp=${encodeURIComponent(refCode)}` : '';
          const botInviteUrl = botUsername ? `https://t.me/${botUsername}?start=${encodeURIComponent(refCode)}` : '';
          sendJson(res, 200, {
            ok: true,
            bot_username: botUsername,
            ref_code: refCode,
            miniapp_invite_url: miniappInviteUrl,
            invite_url: botInviteUrl,
            invited_by: resolved.vault.ecosystem_referrals?.invited_by || rows?.[0]?.invited_by || null,
            referral: rows?.[0] || { telegram_id: Number(auth.uid), code: refCode, bonus_balance: Number(resolved.vault.ecosystem_referrals?.total_bonus_earned || 0) },
            invited: enrichedInvited,
            total: enrichedInvited.length,
            total_bonus_earned: enrichedInvited.reduce((s, x) => s + Number(x.bonus || 75), 0),
            milestones: CHAMPION_MILESTONES.map((m) => ({
              ...m,
              unlocked: enrichedInvited.length >= m.required,
              claimed: claimedSteps.has(m.step)
            }))
          });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/listen' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const releaseId = clean(body.release_id || '');
          if (!releaseId) { sendJson(res, 400, { ok: false, error: 'release_id is required' }); return; }
          await supabaseRequest('cxrner_listen_events', { method: 'POST', body: [{ release_id: releaseId, telegram_id: Number(auth.uid), seconds_played: Math.max(0, Number(body.seconds_played) || 0), source: 'mini_app', country: clean(body.country || '') || null }] });
          sendJson(res, 200, { ok: true });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/analytics' && req.method === 'GET') {
        authorizeMiniApp(req).then(async (auth) => {
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const [revenue, payouts, listens] = await Promise.all([
            supabaseSelectWhere('cxrner_revenue_events', 'platform,country,source,amount,currency,event_date,release_id', [{ key: 'telegram_id', value: auth.uid }], 'event_date.desc', 500),
            supabaseSelectWhere('cxrner_payouts', 'id,amount,currency,status,period,created_at,paid_at', [{ key: 'telegram_id', value: auth.uid }], 'created_at.desc', 200),
            supabaseSelectWhere('cxrner_listen_events', 'release_id,seconds_played,country,created_at', [{ key: 'telegram_id', value: auth.uid }], 'created_at.desc', 500)
          ]);
          const total = (revenue || []).reduce((sum, row) => sum + Number(row.amount || 0), 0);
          sendJson(res, 200, { ok: true, revenue: revenue || [], payouts: payouts || [], listens: listens || [], totals: { revenue: total, streams: (listens || []).length, seconds: (listens || []).reduce((sum, row) => sum + Number(row.seconds_played || 0), 0) } });
        }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/artist' && req.method === 'GET') {
        const artistId = clean(u.searchParams.get('telegram_id') || '');
        if (!/^\d{4,20}$/.test(artistId)) { sendJson(res, 400, { ok: false, error: 'telegram_id is required' }); return; }
        Promise.all([
          supabaseSelectWhere('cxrner_artist_profiles', 'telegram_id,display_name,bio,avatar_url,socials,theme,updated_at', [{ key: 'telegram_id', value: artistId }], '', 1).catch(() => []),
          supabaseSelectWhere('cxrner_artist_badges', 'badge_id,assigned_at', [{ key: 'telegram_id', value: artistId }], 'assigned_at.desc', 100),
          supabaseSelectWhere('cxrner_badges', 'id,slug,name,description,image_url', [], 'created_at.desc', 100),
          supabaseSelectWhere('cxrner_artist_follows', 'follower_id,created_at', [{ key: 'following_id', value: artistId }], 'created_at.desc', 1000),
          supabaseSelectWhere('cxrner_telegram_profiles', 'metadata', [{ key: 'telegram_id', value: artistId }], '', 1).catch(() => [])
        ]).then(([profiles, assigned, badges, followers, telegramProfiles]) => {
          const ids = new Set((assigned || []).map((row) => String(row.badge_id)));
          const fallback = telegramProfiles?.[0]?.metadata?.artist_profile || {};
          sendJson(res, 200, { ok: true, artist: profiles?.[0] || { telegram_id: Number(artistId), display_name: fallback.display_name || '', bio: fallback.bio || '', avatar_url: fallback.avatar_url || '', socials: fallback.socials || {} }, badges: (badges || []).filter((row) => ids.has(String(row.id))), followers: followers?.length || 0, share_url: `${WEBAPP_URL || BASE}?artist=${encodeURIComponent(artistId)}` });
        }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/artist/profile' && req.method === 'POST') {
        readJsonBody(req, 1500000).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const profile = body.profile && typeof body.profile === 'object' ? body.profile : {};
          const displayName = clean(profile.display_name || auth.user.first_name || '').slice(0, 80);
          const bio = clean(profile.bio || '').slice(0, 1000);
          const isAllowedImg = (val) => !val || isHttpUrl(val) || /^data:image\/(png|jpeg|jpg|webp|gif);base64,[A-Za-z0-9+/=]+$/i.test(val);
          const rawAvatar = clean(profile.avatar_url || auth.user.photo_url || '');
          const rawBanner = clean(profile.banner_url || '');
          const avatarUrl = rawAvatar.startsWith('data:image/') ? rawAvatar.slice(0, 650000) : normalizeImageUrl(rawAvatar).slice(0, 700);
          const bannerUrl = rawBanner.startsWith('data:image/') ? rawBanner.slice(0, 650000) : normalizeImageUrl(rawBanner).slice(0, 700);
          if (!isAllowedImg(avatarUrl) || !isAllowedImg(bannerUrl)) { sendJson(res, 400, { ok: false, error: 'Недопустимый формат изображения (нужен URL или сжатое фото)' }); return; }
          const city = clean(profile.city || '').slice(0, 60);
          const country = clean(profile.country || '').slice(0, 60);
          const genres = Array.isArray(profile.genres) ? profile.genres.map((x) => clean(x).slice(0, 32)).filter(Boolean).slice(0, 8) : [];
          const musicRoles = Array.isArray(profile.music_roles) ? profile.music_roles.map((x) => clean(x).slice(0, 32)).filter(Boolean).slice(0, 8) : [];
          const equippedFrame = clean(profile.equipped_frame || 'default').slice(0, 48);
          const profileRow = { telegram_id: Number(auth.uid), display_name: displayName, bio, avatar_url: avatarUrl, socials: profile.socials && typeof profile.socials === 'object' ? profile.socials : {}, theme: profile.theme && typeof profile.theme === 'object' ? profile.theme : {}, updated_at: new Date().toISOString() };
          const fullMeta = { display_name: displayName, bio, avatar_url: avatarUrl, banner_url: bannerUrl, city, country, genres, music_roles: musicRoles, equipped_frame: equippedFrame, socials: profileRow.socials, theme: profileRow.theme };
          try { await supabaseRequest('cxrner_artist_profiles?on_conflict=telegram_id', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates,return=representation' }, body: [{ ...profileRow, avatar_url: avatarUrl.startsWith('data:') ? '' : avatarUrl }] }); } catch (error) { console.warn('[PLATFORM] artist profile table unavailable, using Telegram profile metadata:', clean(error?.message || error)); }
          try {
            const rows = await supabaseSelectWhere('cxrner_telegram_profiles', 'metadata', [{ key: 'telegram_id', value: auth.uid }], '', 1);
            const metadata = rows?.[0]?.metadata && typeof rows[0].metadata === 'object' ? rows[0].metadata : {};
            await supabaseRequest(`cxrner_telegram_profiles?telegram_id=eq.${encodeURIComponent(auth.uid)}`, { method: 'PATCH', body: { metadata: { ...metadata, artist_profile: fullMeta }, last_seen_at: new Date().toISOString() } });
          } catch (error) { console.warn('[PLATFORM] artist metadata fallback failed:', clean(error?.message || error)); }
          try {
            const resolved = await resolveMiniAppVault(auth.uid, auth.user);
            await saveMiniAppVault(resolved, (prev) => ({
              ...prev,
              ecosystem_profile: {
                ...(prev.ecosystem_profile && typeof prev.ecosystem_profile === 'object' ? prev.ecosystem_profile : {}),
                display_name: displayName || prev.ecosystem_profile?.display_name || auth.user.first_name || 'Artist',
                bio,
                avatar_url: avatarUrl || prev.ecosystem_profile?.avatar_url || '',
                banner_url: bannerUrl || prev.ecosystem_profile?.banner_url || '',
                city,
                country,
                genres,
                music_roles: musicRoles,
                equipped_frame: equippedFrame,
                telegram_id: auth.uid,
                telegram_username: clean(auth.user.username || ''),
                telegram_linked: true,
                updated_at: new Date().toISOString()
              }
            }));
          } catch (error) { console.warn('[PLATFORM] vault profile sync skipped:', clean(error?.message || error)); }
          sendJson(res, 200, { ok: true, artist_profile: { ...profileRow, ...fullMeta } });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/follow' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body), followingId = clean(body.following_id || '');
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!/^\d{4,20}$/.test(followingId) || followingId === auth.uid) { sendJson(res, 400, { ok: false, error: 'valid following_id is required' }); return; }
          await supabaseRequest('cxrner_telegram_profiles?on_conflict=telegram_id', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates,return=minimal' }, body: [{ telegram_id: Number(followingId), role: 'artist', status: 'active' }] });
          const query = `follower_id=eq.${encodeURIComponent(auth.uid)}&following_id=eq.${encodeURIComponent(followingId)}`;
          if (body.active === false) await supabaseRequest(`cxrner_artist_follows?${query}`, { method: 'DELETE' });
          else await supabaseRequest('cxrner_artist_follows?on_conflict=follower_id,following_id', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates,return=minimal' }, body: [{ follower_id: Number(auth.uid), following_id: Number(followingId) }] });
          sendJson(res, 200, { ok: true, following: body.active !== false });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/admin/badge' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!auth.is_admin) { sendJson(res, 403, { ok: false, error: 'admin access required' }); return; }
          let badgeId = clean(body.badge_id || '');
          if (body.action === 'create') {
            const name = clean(body.name || ''), imageUrl = clean(body.image_url || '');
            if (!name || !imageUrl.toLowerCase().match(/\.png(?:$|[?#])/)) { sendJson(res, 400, { ok: false, error: 'name and PNG image_url are required' }); return; }
            const rows = await supabaseRequest('cxrner_badges', { method: 'POST', headers: { Prefer: 'return=representation' }, body: [{ slug: clean(body.slug || name.toLowerCase().replace(/[^a-z0-9]+/g, '-')).slice(0, 80), name, description: clean(body.description || ''), image_url: imageUrl }] });
            badgeId = String(rows?.[0]?.id || '');
          }
          const artistId = clean(body.telegram_id || '');
          if (!badgeId || !/^\d{4,20}$/.test(artistId)) { sendJson(res, 400, { ok: false, error: 'badge_id and artist telegram_id are required' }); return; }
          await supabaseRequest('cxrner_telegram_profiles?on_conflict=telegram_id', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates,return=minimal' }, body: [{ telegram_id: Number(artistId), role: 'artist', status: 'active' }] });
          await supabaseRequest('cxrner_artist_badges?on_conflict=badge_id,telegram_id', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates,return=minimal' }, body: [{ badge_id: badgeId, telegram_id: Number(artistId) }] });
          sendJson(res, 200, { ok: true, badge_id: badgeId, telegram_id: artistId });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/admin/overview' && req.method === 'GET') {
        authorizeMiniApp(req).then(async (auth) => {
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!auth.is_admin) { sendJson(res, 403, { ok: false, error: 'admin access required' }); return; }
          await ensureModerationHealth(false).catch(() => {});
          const [users, forms, releases, comments, engagements, payouts, links, news, logs, listens, verifications, cabRows] = await Promise.all([
            supabaseSelectAll('cxrner_telegram_profiles', 'telegram_id,username,first_name,last_name,photo_url,role,status,registered_at,last_seen_at,metadata', 'last_seen_at.desc').catch(() => []),
            supabaseSelectAll(SUPABASE_FORMS_TABLE, 'id,telegram_id,username,artist_name,track_name,genre,release_type,status,upc,source,moderation_message_id,created_at,updated_at,reject_reason,form_payload', 'updated_at.desc').catch(() => []),
            supabaseSelectAll(SUPABASE_PUBLIC_RELEASES_TABLE, 'form_id,telegram_id,artist_name,track_name,status,approved_at,updated_at', 'approved_at.desc').catch(() => []),
            supabaseSelectAll('cxrner_comments', 'id,release_id,telegram_id,body,is_pinned,created_at', 'created_at.desc').catch(() => []),
            supabaseSelectAll('cxrner_release_engagements', 'release_id,telegram_id,kind,created_at', 'created_at.desc').catch(() => []),
            supabaseSelectAll('cxrner_payouts', 'id,telegram_id,amount,currency,status,period,created_at,paid_at', 'created_at.desc').catch(() => []),
            supabaseSelectAll(SUPABASE_LINKS_TABLE, 'id,telegram_id,release_id,slug,title,artist,status,created_at,updated_at', 'updated_at.desc').catch(() => []),
            supabaseSelectAll('cxrner_news', 'id,title,body,cover_url,published,published_at,created_at,updated_at', 'created_at.desc').catch(() => []),
            supabaseSelectAll('cxrner_admin_logs', 'id,admin_id,action,entity,entity_id,payload,created_at', 'created_at.desc').catch(() => []),
            supabaseSelectAll('cxrner_listen_events', 'release_id,telegram_id,created_at', 'created_at.desc').catch(() => []),
            supabaseSelectAll('cxrner_artist_verifications', 'id,user_id,stage_name,method,verification_code,status,dsp_links,evidence_notes,contact_telegram,created_at,updated_at', 'created_at.desc').catch(() => []),
            getCabinetRowsCached(false).catch(() => [])
          ]);
          const seenKeys = new Set();
          const normalizedForms = (forms || []).map((f) => {
            const p = f.form_payload && typeof f.form_payload === 'object' ? f.form_payload : {};
            const st = canonicalStatus(p.status || f.status || '');
            const src = clean(f.source || p.source || 'bot_text').toLowerCase();
            if (f.id) seenKeys.add(String(f.id));
            return {
              id: f.id,
              telegram_id: f.telegram_id,
              username: clean(f.username || p.username || p.tg || ''),
              artist_name: clean(f.artist_name || p.nick || 'Без имени'),
              track_name: clean(f.track_name || p.name || 'Без названия'),
              genre: clean(f.genre || p.genre || ''),
              release_type: clean(f.release_type || p.type || 'single'),
              status: st || f.status || 'pending',
              raw_status: f.status,
              upc: clean(f.upc || p.upc || ''),
              source: src.includes('web') ? 'web' : src.includes('mini') ? 'mini_app' : 'bot',
              moderation_message_id: f.moderation_message_id || p.moderation_message_id || null,
              reject_reason: clean(f.reject_reason || p.reject_reason || ''),
              cover_url: clean(p.cover_url || p.cover || ''),
              created_at: f.created_at,
              updated_at: f.updated_at || f.created_at
            };
          });
          // Merge local db releases not yet present in Supabase forms
          for (const [duid, list] of Object.entries(db || {})) {
            if (!Array.isArray(list)) continue;
            list.forEach((rel, idx) => {
              if (!rel || rel.user_deleted || canonicalStatus(rel.status) === STATUS.DELETED) return;
              const fid = clean(rel.supabase_form_id || `${duid}:0:${idx}`);
              if (seenKeys.has(fid)) return;
              const src = clean(rel.source || 'bot_text').toLowerCase();
              normalizedForms.push({
                id: `${duid}:0:${idx}`,
                telegram_id: duid,
                username: clean(rel.username || rel.tg || ''),
                artist_name: clean(rel.nick || 'Без имени'),
                track_name: clean(rel.name || 'Без названия'),
                genre: clean(rel.genre || ''),
                release_type: clean(rel.type || 'single'),
                status: canonicalStatus(rel.status || STATUS.ON_UPLOAD),
                upc: clean(rel.upc || ''),
                source: src.includes('web') ? 'web' : src.includes('mini') ? 'mini_app' : 'bot',
                moderation_message_id: rel.moderation_message_id || null,
                reject_reason: clean(rel.reject_reason || ''),
                cover_url: clean(rel.cover_url || ''),
                created_at: rel.submission_time || new Date().toISOString(),
                updated_at: rel.updated_at || rel.submission_time || new Date().toISOString()
              });
            });
          }
          normalizedForms.sort((a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0));

          const statusCounts = { on_upload: 0, moderation: 0, approved: 0, needfix: 0, rejected: 0 };
          const sourceCounts = { web: 0, mini_app: 0, bot: 0 };
          for (const f of normalizedForms) {
            const st = clean(f.status);
            if (st === STATUS.ON_UPLOAD) statusCounts.on_upload += 1;
            else if (st === STATUS.MODERATION) statusCounts.moderation += 1;
            else if (st === STATUS.APPROVED || st === STATUS.PUBLISHED) statusCounts.approved += 1;
            else if (st === STATUS.NEEDS_FIX) statusCounts.needfix += 1;
            else if (st === STATUS.REJECTED) statusCounts.rejected += 1;
            if (f.source === 'web') sourceCounts.web += 1;
            else if (f.source === 'mini_app') sourceCounts.mini_app += 1;
            else sourceCounts.bot += 1;
          }

          const mergedProfilesByUid = new Map();
          for (const cr of cabRows || []) {
            const cp = cr?.profile || {};
            if (cp.telegram_id) mergedProfilesByUid.set(String(cp.telegram_id), cp);
          }
          for (const [uidKey, vProfile] of localDropVaultCache.entries()) {
            if (vProfile) mergedProfilesByUid.set(String(uidKey), vProfile);
          }
          let dropTotalItems = 0;
          let dropTotalCxp = 0;
          const userMetaByCab = new Map();
          for (const [uidKey, cp] of mergedProfilesByUid.entries()) {
            const invLen = Array.isArray(cp.ecosystem_inventory) ? cp.ecosystem_inventory.length : 0;
            const bal = Number(cp.ecosystem_wallet?.balance || 0);
            dropTotalItems += invLen;
            dropTotalCxp += bal;
            userMetaByCab.set(String(uidKey), {
              wallet_balance: bal,
              inventory_count: invLen,
              friends_count: Array.isArray(cp.ecosystem_referrals?.friends) ? cp.ecosystem_referrals.friends.length : 0,
              verification_status: cp.ecosystem_verification?.status || ''
            });
          }
          const enrichedUsers = (users || []).map((uRow) => ({
            ...uRow,
            ...(userMetaByCab.get(String(uRow.telegram_id)) || {})
          }));

          const verifList = (verifications || []).slice().sort((a, b) => {
            const ap = a.status === 'pending_review' ? 0 : 1;
            const bp = b.status === 'pending_review' ? 0 : 1;
            if (ap !== bp) return ap - bp;
            return new Date(b.created_at || 0) - new Date(a.created_at || 0);
          });

          const today = new Date(); today.setHours(0, 0, 0, 0);
          sendJson(res, 200, {
            ok: true,
            bot_health: {
              build: BOT_BUILD_VERSION,
              commit: BOT_DEPLOY_INFO.commit || '',
              branch: BOT_DEPLOY_INFO.branch || 'main',
              subject: BOT_DEPLOY_INFO.commit_subject || '',
              started_at: BOT_STARTED_AT,
              uptime_sec: Math.floor((Date.now() - BOT_STARTED_AT_MS) / 1000),
              polling: { ...pollingHealth },
              moderation: { ...moderationHealth },
              supabase_enabled: SUPABASE_SYNC_ENABLED,
              supabase_features: { ...supabaseFeatureState }
            },
            stats: {
              artists: (users || []).filter((x) => x.role === 'artist').length,
              users: users?.length || 0,
              releases: releases?.length || 0,
              forms_total: normalizedForms.length,
              on_upload: statusCounts.on_upload,
              moderation: statusCounts.moderation + statusCounts.on_upload,
              approved: statusCounts.approved,
              needfix: statusCounts.needfix,
              rejected: statusCounts.rejected,
              source_breakdown: sourceCounts,
              verifications_pending: verifList.filter((v) => v.status === 'pending_review').length,
              links: links?.length || 0,
              views: listens?.length || 0,
              comments: comments?.length || 0,
              likes: (engagements || []).filter((x) => x.kind === 'like').length,
              payouts: payouts?.length || 0,
              drop_total_items: dropTotalItems,
              drop_total_cxp: dropTotalCxp,
              new_today: (users || []).filter((x) => new Date(x.registered_at || 0) >= today).length
            },
            users: enrichedUsers,
            forms: normalizedForms,
            releases: releases || [],
            verifications: verifList,
            comments: comments || [],
            engagements: engagements || [],
            payouts: payouts || [],
            links: links || [],
            news: news || [],
            logs: logs || []
          });
        }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/admin/moderate' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!auth.is_admin) { sendJson(res, 403, { ok: false, error: 'admin access required' }); return; }
          const modName = clean(auth.user.username ? `@${auth.user.username}` : auth.user.first_name || `Admin ${auth.uid}`);
          const result = await executeModerationAction({
            formId: clean(body.form_id || body.id || ''),
            status: clean(body.status || ''),
            action: clean(body.action || body.status || ''),
            upc: clean(body.upc || ''),
            rejectReason: clean(body.reject_reason || ''),
            moderatorComment: clean(body.moderator_comment || ''),
            moderatorName: modName,
            telegramId: clean(body.telegram_id || ''),
            trackName: clean(body.track_name || '')
          });
          await supabaseRequest('cxrner_admin_logs', {
            method: 'POST',
            body: [{ admin_id: Number(auth.uid), action: clean(body.action || body.status || 'moderate'), entity: 'form', entity_id: clean(body.form_id || body.id || ''), payload: { upc: clean(body.upc || ''), reject_reason: clean(body.reject_reason || ''), moderator_comment: clean(body.moderator_comment || '') } }]
          }).catch(() => {});
          sendJson(res, 200, { ok: true, ...result });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/admin/verification' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!auth.is_admin) { sendJson(res, 403, { ok: false, error: 'admin access required' }); return; }
          const decision = clean(body.decision || '').toLowerCase() === 'approve' ? 'verified' : 'rejected';
          const verifId = clean(body.id || '');
          const userId = clean(body.user_id || '');
          const stageName = clean(body.stage_name || '');
          const nowIso = new Date().toISOString();
          if (verifId && SUPABASE_SYNC_ENABLED) {
            await supabaseRequest(`cxrner_artist_verifications?id=eq.${encodeURIComponent(verifId)}`, {
              method: 'PATCH',
              body: { status: decision, updated_at: nowIso }
            }).catch(() => {});
          }
          // Find user vault by UUID or telegram_id and update ecosystem_verification
          const cabRows = await getCabinetRowsCached(false).catch(() => []);
          const matchedCab = (cabRows || []).find((r) => String(r.user_id) === userId || String(r.profile?.telegram_id || '') === userId);
          const tgId = clean(matchedCab?.profile?.telegram_id || body.telegram_id || (/^\d{4,20}$/.test(userId) ? userId : ''));
          if (/^\d{4,20}$/.test(tgId)) {
            const resolved = await resolveMiniAppVault(tgId, {});
            await saveMiniAppVault(resolved, (prev) => ({
              ...prev,
              ecosystem_verification: { ...(prev.ecosystem_verification || {}), status: decision, stage_name: stageName || prev.ecosystem_verification?.stage_name || '', reviewed_at: nowIso, reviewed_by: auth.uid }
            }));
            const msgText = decision === 'verified'
              ? `✅ <b>Верификация артиста одобрена!</b>\n\nПрофиль <b>${esc(stageName || 'CXRNER ID')}</b> теперь отмечен галочкой Verified.`
              : `❌ <b>Заявка на верификацию отклонена.</b>\n\n${body.note ? `Комментарий: ${esc(body.note)}` : 'Проверьте ссылки на DSP-площадки и отправьте повторно.'}`;
            await sendText(Number(tgId), msgText).catch(() => {});
            await supabaseRequest('cxrner_notifications', {
              method: 'POST',
              headers: { Prefer: 'return=minimal' },
              body: [{ telegram_id: Number(tgId), type: 'verification', title: decision === 'verified' ? 'Верификация одобрена' : 'Верификация отклонена', body: decision === 'verified' ? 'Твой аккаунт получил статус Verified!' : (clean(body.note) || 'Заявка отклонена модерацией.') }]
            }).catch(() => {});
          }
          await supabaseRequest('cxrner_admin_logs', {
            method: 'POST',
            body: [{ admin_id: Number(auth.uid), action: `verification_${decision}`, entity: 'verification', entity_id: verifId || userId, payload: { stage_name: stageName } }]
          }).catch(() => {});
          sendJson(res, 200, { ok: true, status: decision, id: verifId });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/admin/user-bonus' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!auth.is_admin) { sendJson(res, 403, { ok: false, error: 'admin access required' }); return; }
          const targetUid = clean(body.telegram_id || '');
          if (!/^\d{4,20}$/.test(targetUid)) { sendJson(res, 400, { ok: false, error: 'valid telegram_id is required' }); return; }
          const deltaCxp = Number(body.delta_cxp || 0);
          const nextRole = clean(body.role || '');
          const nextStatus = clean(body.status || '');
          const nowIso = new Date().toISOString();
          let updatedWallet = null;
          if (Number.isFinite(deltaCxp) && deltaCxp !== 0) {
            const resolved = await resolveMiniAppVault(targetUid, {});
            const nv = await saveMiniAppVault(resolved, (prev) => {
              const w = prev.ecosystem_wallet || { user_id: resolved.targetUserId, balance: 150, total_earned: 150, total_spent: 0 };
              const nextBal = Math.max(0, Number(w.balance || 0) + deltaCxp);
              return {
                ...prev,
                ecosystem_wallet: { ...w, balance: nextBal, total_earned: deltaCxp > 0 ? Number(w.total_earned || 0) + deltaCxp : Number(w.total_earned || 0), updated_at: nowIso }
              };
            });
            updatedWallet = nv.ecosystem_wallet;
            if (deltaCxp > 0) {
              await supabaseRequest('cxrner_notifications', {
                method: 'POST',
                headers: { Prefer: 'return=minimal' },
                body: [{ telegram_id: Number(targetUid), type: 'bonus', title: `+${deltaCxp} CXP от администрации`, body: clean(body.reason || 'Бонус начислен на баланс CXRNER DROP') }]
              }).catch(() => {});
            }
          }
          const profilePatch = {};
          if (['artist', 'moderator', 'admin'].includes(nextRole)) profilePatch.role = nextRole;
          if (['active', 'blocked'].includes(nextStatus)) profilePatch.status = nextStatus;
          if (Object.keys(profilePatch).length) {
            await supabaseRequest(`cxrner_telegram_profiles?telegram_id=eq.${encodeURIComponent(targetUid)}`, {
              method: 'PATCH',
              body: profilePatch
            }).catch(() => {});
          }
          await supabaseRequest('cxrner_admin_logs', {
            method: 'POST',
            body: [{ admin_id: Number(auth.uid), action: 'user_update', entity: 'user', entity_id: targetUid, payload: { delta_cxp: deltaCxp, ...profilePatch } }]
          }).catch(() => {});
          sendJson(res, 200, { ok: true, telegram_id: targetUid, wallet: updatedWallet, patch: profilePatch });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/admin/search' && req.method === 'GET') {
        authorizeMiniApp(req).then(async (auth) => {
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!auth.is_admin) { sendJson(res, 403, { ok: false, error: 'admin access required' }); return; }
          const q = clean(u.searchParams.get('q') || '').toLowerCase();
          if (q.length < 2) { sendJson(res, 200, { ok: true, results: [] }); return; }
          const [users, forms, releases, comments, links] = await Promise.all([
            supabaseSelectAll('cxrner_telegram_profiles', 'telegram_id,username,first_name,last_name,role,status,registered_at', 'last_seen_at.desc').catch(() => []),
            supabaseSelectAll(SUPABASE_FORMS_TABLE, 'id,telegram_id,artist_name,track_name,status,upc,created_at', 'updated_at.desc').catch(() => []),
            supabaseSelectAll(SUPABASE_PUBLIC_RELEASES_TABLE, 'form_id,telegram_id,artist_name,track_name,status,release_data,approved_at', 'approved_at.desc').catch(() => []),
            supabaseSelectAll('cxrner_comments', 'id,release_id,telegram_id,body,created_at', 'created_at.desc').catch(() => []),
            supabaseSelectAll(SUPABASE_LINKS_TABLE, 'id,telegram_id,slug,title,artist,status,created_at', 'updated_at.desc').catch(() => [])
          ]);
          const match = (row) => JSON.stringify(row).toLowerCase().includes(q);
          sendJson(res, 200, { ok: true, results: [{ type: 'users', items: (users || []).filter(match).slice(0, 30) }, { type: 'forms', items: (forms || []).filter(match).slice(0, 30) }, { type: 'releases', items: (releases || []).filter(match).slice(0, 30) }, { type: 'comments', items: (comments || []).filter(match).slice(0, 30) }, { type: 'links', items: (links || []).filter(match).slice(0, 30) }] });
        }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/admin/broadcast' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!auth.is_admin) { sendJson(res, 403, { ok: false, error: 'admin access required' }); return; }
          const title = clean(body.title || '').slice(0, 160), message = clean(body.body || '').slice(0, 2000), audience = clean(body.audience || 'all');
          if (!title || !message) { sendJson(res, 400, { ok: false, error: 'title and body are required' }); return; }
          const users = await supabaseSelectAll('cxrner_telegram_profiles', 'telegram_id,role,status,last_seen_at,metadata', 'last_seen_at.desc').catch(() => []);
          const selected = (users || []).filter((user) => audience === 'all' || (audience === 'artists' && user.role === 'artist') || (audience === 'active' && user.status === 'active') || (audience === 'new' && (Date.now() - new Date(user.registered_at || 0).getTime()) < 30 * 86400000));
          if (selected.length) await supabaseRequest('cxrner_notifications', { method: 'POST', headers: { Prefer: 'return=minimal' }, body: selected.map((user) => ({ telegram_id: Number(user.telegram_id), type: body.type || 'announcement', title, body: message })) });
          sendJson(res, 200, { ok: true, delivered: selected.length });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/admin' && req.method === 'GET') {
        authorizeMiniApp(req).then(async (auth) => {
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!auth.is_admin) { sendJson(res, 403, { ok: false, error: 'admin access required' }); return; }
          const [users, forms, releases, comments, engagements, payouts] = await Promise.all([
            supabaseSelectAll('cxrner_telegram_profiles', 'telegram_id,username,first_name,role,status,registered_at,last_seen_at', 'last_seen_at.desc'),
            supabaseSelectAll(SUPABASE_FORMS_TABLE, 'id,telegram_id,artist_name,track_name,status,created_at,updated_at,reject_reason', 'updated_at.desc'),
            supabaseSelectAll(SUPABASE_PUBLIC_RELEASES_TABLE, 'form_id,telegram_id,artist_name,track_name,status,approved_at', 'approved_at.desc'),
            supabaseSelectAll('cxrner_comments', 'id,release_id,telegram_id,body,is_pinned,created_at', 'created_at.desc'),
            supabaseSelectAll('cxrner_release_engagements', 'release_id,telegram_id,kind,created_at', 'created_at.desc'),
            supabaseSelectAll('cxrner_payouts', 'id,telegram_id,amount,currency,status,period,created_at,paid_at', 'created_at.desc')
          ]);
          sendJson(res, 200, { ok: true, stats: { users: users?.length || 0, releases: releases?.length || 0, forms: forms?.length || 0, comments: comments?.length || 0, likes: (engagements || []).filter((x) => x.kind === 'like').length, favorites: (engagements || []).filter((x) => x.kind === 'favorite').length, payouts: payouts?.length || 0 }, users: users || [], forms: forms || [], releases: releases || [], comments: comments || [], engagements: engagements || [], payouts: payouts || [] });
        }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/admin/action' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          if (!auth.is_admin) { sendJson(res, 403, { ok: false, error: 'admin access required' }); return; }
          const entity = clean(body.entity || ''), action = clean(body.action || ''), id = clean(body.id || '');
          const allowed = {
            user: 'cxrner_telegram_profiles', form: SUPABASE_FORMS_TABLE, news: 'cxrner_news', payout: 'cxrner_payouts', revenue: 'cxrner_revenue_events', achievement: 'cxrner_achievements', settings: 'cxrner_platform_settings', comment: 'cxrner_comments', link: SUPABASE_LINKS_TABLE, artist: 'cxrner_artist_profiles', notification: 'cxrner_notifications'
          };
          const table = allowed[entity];
          if (!table || !['create', 'update', 'delete'].includes(action)) { sendJson(res, 400, { ok: false, error: 'unsupported admin action' }); return; }
          const values = body.values && typeof body.values === 'object' ? { ...body.values } : {};
          delete values.id;
          let result;
          if (action === 'create') result = await supabaseRequest(table, { method: 'POST', headers: { Prefer: 'return=representation' }, body: [values] });
          else if (action === 'update') result = await supabaseRequest(`${table}?${entity === 'user' ? 'telegram_id' : 'id'}=eq.${encodeURIComponent(id)}`, { method: 'PATCH', headers: { Prefer: 'return=representation' }, body: { ...values, updated_at: new Date().toISOString() } });
          else result = await supabaseRequest(`${table}?${entity === 'user' ? 'telegram_id' : 'id'}=eq.${encodeURIComponent(id)}`, { method: 'DELETE' });
          await supabaseRequest('cxrner_admin_logs', { method: 'POST', body: [{ admin_id: Number(auth.uid), action, entity, entity_id: id || null, payload: values }] }).catch(() => null);
          sendJson(res, 200, { ok: true, result: result || null });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/notifications/read' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const nowIso = new Date().toISOString();
          await supabaseRequest(`cxrner_notifications?telegram_id=eq.${encodeURIComponent(auth.uid)}&read_at=is.null`, {
            method: 'PATCH',
            body: { read_at: nowIso }
          }).catch(() => {});
          sendJson(res, 200, { ok: true, read_at: nowIso });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/platform/verification' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const stageName = clean(body.stage_name || auth.user.first_name || '').slice(0, 80);
          const spotifyUrl = clean(body.spotify_url || '').slice(0, 400);
          const yandexUrl = clean(body.yandex_url || '').slice(0, 400);
          const vkUrl = clean(body.vk_url || '').slice(0, 400);
          const appleUrl = clean(body.apple_url || '').slice(0, 400);
          const notes = clean(body.notes || '').slice(0, 1000);
          if (!stageName || (!spotifyUrl && !yandexUrl && !vkUrl && !appleUrl)) {
            sendJson(res, 400, { ok: false, error: 'Укажи сценический псевдоним и минимум 1 ссылку на площадку' });
            return;
          }
          const resolved = await resolveMiniAppVault(auth.uid, auth.user);
          const nowIso = new Date().toISOString();
          const verification = {
            id: crypto.randomUUID(),
            user_id: resolved.targetUserId,
            stage_name: stageName,
            method: 'dsp_profile_review',
            verification_code: `CXR-${crypto.randomBytes(3).toString('hex').toUpperCase()}`,
            status: 'pending_review',
            dsp_links: { spotify: spotifyUrl, yandex: yandexUrl, vk: vkUrl, apple: appleUrl },
            evidence_notes: notes,
            contact_telegram: `@${clean(auth.user.username || auth.uid)}`,
            created_at: nowIso,
            updated_at: nowIso
          };
          await saveMiniAppVault(resolved, (prev) => ({ ...prev, ecosystem_verification: verification }));
          await supabaseRequest('cxrner_artist_verifications', {
            method: 'POST',
            headers: { Prefer: 'return=minimal' },
            body: [{
              user_id: resolved.targetUserId,
              stage_name: stageName,
              method: 'dsp_profile_review',
              verification_code: verification.verification_code,
              status: 'pending_review',
              dsp_links: verification.dsp_links,
              evidence_notes: notes,
              contact_telegram: verification.contact_telegram,
              created_at: nowIso,
              updated_at: nowIso
            }]
          }).catch(() => {});
          sendJson(res, 200, { ok: true, verification });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/drop/state' && (req.method === 'GET' || req.method === 'POST')) {
        const dropReq = req.method === 'POST' ? readJsonBody(req).catch(() => ({})) : Promise.resolve({});
        dropReq.then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const resolved = await resolveMiniAppVault(auth.uid, auth.user);
          const v = resolved.vault;
          const lastScanMs = Number(new Date(v.ecosystem_daily_scan_at || 0).getTime()) || 0;
          const cooldownMs = 20 * 3600 * 1000;
          const canClaimDaily = !lastScanMs || (Date.now() - lastScanMs) >= cooldownMs;
          const cabRows = await getCabinetRowsCached(false).catch(() => []);
          const rankWeights = { common: 10, rare: 30, epic: 85, legendary: 260, mythic: 800, special_reward: 1500 };
          const leaderboard = (cabRows || []).map((r) => {
            const p = r?.profile || {};
            const ep = p.ecosystem_profile || {};
            const inv = Array.isArray(p.ecosystem_inventory) ? p.ecosystem_inventory : [];
            const score = inv.reduce((s, it) => s + (rankWeights[it?.rarity] || 10), 0);
            return {
              user_id: r.user_id,
              display_name: ep.display_name || p.display_name || p.username || 'Коллекционер',
              username: ep.username || p.username || '',
              items_count: inv.length,
              score,
              is_ceo: String(ep.username || '').toLowerCase() === 'kazumaiq'
            };
          }).filter((x) => x.items_count > 0 || x.is_ceo).sort((a, b) => b.score - a.score).slice(0, 20);
          sendJson(res, 200, {
            ok: true,
            wallet: v.ecosystem_wallet,
            inventory: v.ecosystem_inventory || [],
            history: (v.ecosystem_drop_history || []).slice(0, 40),
            cases: DROP_CASES_CONFIG,
            specimens: DROP_SPECIMENS_CATALOG,
            leaderboard,
            recent_drops: buildGlobalDropFeed(cabRows, 14),
            daily_scan: {
              can_claim: canClaimDaily,
              next_claim_at: lastScanMs ? new Date(lastScanMs + cooldownMs).toISOString() : null,
              reward_cxp: 35,
              streak: Number(v.ecosystem_daily_streak || 0)
            },
            claimed_promos: v.ecosystem_claimed_promos || []
          });
        }).catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/drop/open' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const caseId = clean(body.case_id || 'case_001_archive');
          const caseCfg = DROP_CASES_CONFIG.find((c) => c.id === caseId);
          if (!caseCfg) { sendJson(res, 400, { ok: false, error: 'Неизвестный кейс' }); return; }
          const resolved = await resolveMiniAppVault(auth.uid, auth.user);
          const currentBal = Number(resolved.vault.ecosystem_wallet?.balance || 0);
          if (!resolved.isCeo && currentBal < caseCfg.price_cxp) {
            sendJson(res, 400, { ok: false, error: `Недостаточно CXP. Нужно ${caseCfg.price_cxp} CXP, на балансе ${currentBal} CXP.` });
            return;
          }
          const picked = rollSpecimenForCase(caseCfg);
          const nowIso = new Date().toISOString();
          const serial = crypto.randomInt(1, 999);
          const item = {
            id: crypto.randomUUID(),
            user_id: resolved.targetUserId,
            item_id: picked.item_id,
            item_name: picked.item_name,
            specimen_code: picked.specimen_code,
            specimen_class: picked.specimen_class,
            collection_name: picked.collection_name,
            rarity: picked.rarity,
            serial_number: serial,
            description: picked.description,
            model_color: picked.model_color,
            shape: picked.shape,
            obtained_via: 'case_open',
            is_special_reward: Boolean(picked.is_special_reward),
            claim_code: picked.is_special_reward ? `CXR-${crypto.randomBytes(4).toString('hex').toUpperCase()}` : null,
            is_pinned: false,
            created_at: nowIso
          };
          const cashback = Math.max(5, Math.round(caseCfg.price_cxp * 0.1));
          const historyEntry = {
            id: crypto.randomUUID(),
            user_id: resolved.targetUserId,
            case_id: caseCfg.id,
            case_name: caseCfg.name,
            item_id: item.item_id,
            item_name: item.item_name,
            specimen_code: item.specimen_code,
            rarity: item.rarity,
            serial_number: serial,
            cxp_spent: resolved.isCeo ? 0 : caseCfg.price_cxp,
            created_at: nowIso
          };
          const nextVault = await saveMiniAppVault(resolved, (prev) => {
            const w = prev.ecosystem_wallet || { user_id: resolved.targetUserId, balance: 150, total_earned: 150, total_spent: 0 };
            const spent = resolved.isCeo ? 0 : caseCfg.price_cxp;
            const nextBal = resolved.isCeo ? Math.max(999999, Number(w.balance || 0)) : Math.max(0, Number(w.balance || 0) - spent + cashback);
            return {
              ...prev,
              ecosystem_wallet: {
                ...w,
                balance: nextBal,
                total_earned: Number(w.total_earned || 0) + cashback,
                total_spent: Number(w.total_spent || 0) + spent,
                updated_at: nowIso
              },
              ecosystem_inventory: [item, ...(Array.isArray(prev.ecosystem_inventory) ? prev.ecosystem_inventory : [])],
              ecosystem_drop_history: [historyEntry, ...(Array.isArray(prev.ecosystem_drop_history) ? prev.ecosystem_drop_history : [])].slice(0, 100)
            };
          });
          if (SUPABASE_SYNC_ENABLED) {
            supabaseRequest('cxrner_drop_inventory', {
              method: 'POST',
              headers: { Prefer: 'return=minimal' },
              body: [{ id: item.id, user_id: item.user_id, item_id: item.item_id, item_name: item.item_name, specimen_code: item.specimen_code, specimen_class: item.specimen_class, collection_name: item.collection_name, rarity: item.rarity, serial_number: serial, description: item.description, model_color: item.model_color, obtained_via: 'mini_app', is_special_reward: item.is_special_reward, claim_code: item.claim_code, created_at: nowIso }]
            }).catch(() => {});
            supabaseRequest('cxrner_drop_history', {
              method: 'POST',
              headers: { Prefer: 'return=minimal' },
              body: [{ id: historyEntry.id, user_id: historyEntry.user_id, case_id: historyEntry.case_id, case_name: historyEntry.case_name, item_id: historyEntry.item_id, item_name: historyEntry.item_name, specimen_code: historyEntry.specimen_code, rarity: historyEntry.rarity, serial_number: serial, cxp_spent: historyEntry.cxp_spent, is_special_reward: item.is_special_reward, claim_code: item.claim_code, created_at: nowIso }]
            }).catch(() => {});
          }
          recordGlobalDropFeedEntry({
            id: historyEntry.id,
            display_name: clean(auth.user?.first_name || auth.user?.username || 'Коллекционер'),
            username: clean(auth.user?.username || ''),
            item_name: item.item_name,
            specimen_code: item.specimen_code,
            rarity: item.rarity,
            created_at: nowIso
          });
          sendJson(res, 200, { ok: true, item, cashback, wallet: nextVault.ecosystem_wallet, history_entry: historyEntry });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/drop/action' && req.method === 'POST') {
        readJsonBody(req).then(async (body) => {
          const auth = await authorizeMiniApp(req, body);
          if (!auth.ok) { sendJson(res, auth.status, auth); return; }
          const action = clean(body.action || '').toLowerCase();
          const resolved = await resolveMiniAppVault(auth.uid, auth.user);
          const nowIso = new Date().toISOString();
          if (action === 'daily_scan') {
            const lastMs = Number(new Date(resolved.vault.ecosystem_daily_scan_at || 0).getTime()) || 0;
            const cooldownMs = 20 * 3600 * 1000;
            if (lastMs && (Date.now() - lastMs) < cooldownMs) {
              const mins = Math.ceil((cooldownMs - (Date.now() - lastMs)) / 60000);
              sendJson(res, 400, { ok: false, error: `Следующее сканирование через ${Math.floor(mins / 60)}ч ${mins % 60}м` });
              return;
            }
            const nextStreak = Math.min(7, Number(resolved.vault.ecosystem_daily_streak || 0) + 1);
            const rewardCxp = 35 + Math.min(35, (nextStreak - 1) * 5);
            const nextVault = await saveMiniAppVault(resolved, (prev) => {
              const w = prev.ecosystem_wallet || { user_id: resolved.targetUserId, balance: 150, total_earned: 150, total_spent: 0 };
              return {
                ...prev,
                ecosystem_daily_scan_at: nowIso,
                ecosystem_daily_streak: nextStreak,
                ecosystem_wallet: { ...w, balance: Number(w.balance || 0) + rewardCxp, total_earned: Number(w.total_earned || 0) + rewardCxp, updated_at: nowIso }
              };
            });
            sendJson(res, 200, { ok: true, reward_cxp: rewardCxp, streak: nextStreak, wallet: nextVault.ecosystem_wallet });
            return;
          }
          if (action === 'salvage') {
            const ids = Array.isArray(body.item_ids) ? body.item_ids.map(String) : [clean(body.item_id || '')].filter(Boolean);
            const inv = resolved.vault.ecosystem_inventory || [];
            const matched = inv.filter((it) => ids.includes(String(it.id)));
            if (!matched.length) { sendJson(res, 400, { ok: false, error: 'Предмет не найден на складе' }); return; }
            const gained = matched.reduce((sum, it) => sum + (DROP_SALVAGE_MAP[it.rarity] || 15), 0);
            const matchedIds = new Set(matched.map((x) => String(x.id)));
            const nextVault = await saveMiniAppVault(resolved, (prev) => {
              const w = prev.ecosystem_wallet || { user_id: resolved.targetUserId, balance: 0, total_earned: 0, total_spent: 0 };
              return {
                ...prev,
                ecosystem_inventory: (prev.ecosystem_inventory || []).filter((it) => !matchedIds.has(String(it.id))),
                ecosystem_removed_item_ids: [...new Set([...(prev.ecosystem_removed_item_ids || []), ...matchedIds])].slice(-200),
                ecosystem_wallet: { ...w, balance: Number(w.balance || 0) + gained, total_earned: Number(w.total_earned || 0) + gained, updated_at: nowIso }
              };
            });
            sendJson(res, 200, { ok: true, gained_cxp: gained, salvaged_count: matched.length, wallet: nextVault.ecosystem_wallet, inventory: nextVault.ecosystem_inventory });
            return;
          }
          if (action === 'fuse') {
            const ids = Array.isArray(body.item_ids) ? [...new Set(body.item_ids.map(String))] : [];
            const inv = resolved.vault.ecosystem_inventory || [];
            const matched = inv.filter((it) => ids.includes(String(it.id)));
            if (matched.length !== 5) { sendJson(res, 400, { ok: false, error: 'Для синтеза выбери ровно 5 предметов одной редкости' }); return; }
            const baseRarity = matched[0].rarity;
            const nextRarity = DROP_FUSION_NEXT[baseRarity];
            if (!nextRarity || !matched.every((it) => it.rarity === baseRarity)) {
              sendJson(res, 400, { ok: false, error: 'Все 5 предметов должны быть одинаковой редкости (Common -> Legendary)' });
              return;
            }
            const pool = DROP_SPECIMENS_CATALOG.filter((s) => s.rarity === nextRarity);
            const forgedDef = pool[crypto.randomInt(0, pool.length)] || DROP_SPECIMENS_CATALOG[0];
            const forgedItem = {
              id: crypto.randomUUID(),
              user_id: resolved.targetUserId,
              item_id: forgedDef.item_id,
              item_name: forgedDef.item_name,
              specimen_code: forgedDef.specimen_code,
              specimen_class: forgedDef.specimen_class,
              collection_name: forgedDef.collection_name,
              rarity: forgedDef.rarity,
              serial_number: crypto.randomInt(1, 500),
              description: forgedDef.description,
              model_color: forgedDef.model_color,
              shape: forgedDef.shape,
              obtained_via: 'fusion_5to1',
              is_pinned: false,
              created_at: nowIso
            };
            const matchedSet = new Set(matched.map((x) => String(x.id)));
            const nextVault = await saveMiniAppVault(resolved, (prev) => ({
              ...prev,
              ecosystem_inventory: [forgedItem, ...(prev.ecosystem_inventory || []).filter((it) => !matchedSet.has(String(it.id)))],
              ecosystem_removed_item_ids: [...new Set([...(prev.ecosystem_removed_item_ids || []), ...matchedSet])].slice(-200)
            }));
            sendJson(res, 200, { ok: true, forged: forgedItem, inventory: nextVault.ecosystem_inventory });
            return;
          }
          if (action === 'battle') {
            const caseCfg = DROP_CASES_CONFIG.find((c) => c.id === clean(body.case_id || 'case_001_archive')) || DROP_CASES_CONFIG[0];
            const wager = caseCfg.price_cxp;
            const bal = Number(resolved.vault.ecosystem_wallet?.balance || 0);
            if (!resolved.isCeo && bal < wager) { sendJson(res, 400, { ok: false, error: `Нужно ${wager} CXP для дуэли` }); return; }
            const playerRoll = rollSpecimenForCase(caseCfg);
            const botRoll = rollSpecimenForCase(caseCfg);
            const scoreMap = { common: 1, rare: 2, epic: 3, legendary: 4, mythic: 5, special_reward: 6 };
            const pScore = (scoreMap[playerRoll.rarity] || 1) * 100 + crypto.randomInt(1, 90);
            const bScore = (scoreMap[botRoll.rarity] || 1) * 100 + crypto.randomInt(1, 90);
            const won = pScore >= bScore;
            const rewardCxp = won ? Math.round(wager * 0.8) : 0;
            const wonItems = won ? [playerRoll, botRoll].map((def) => ({
              id: crypto.randomUUID(),
              user_id: resolved.targetUserId,
              item_id: def.item_id,
              item_name: def.item_name,
              specimen_code: def.specimen_code,
              specimen_class: def.specimen_class,
              collection_name: def.collection_name,
              rarity: def.rarity,
              serial_number: crypto.randomInt(1, 999),
              description: def.description,
              model_color: def.model_color,
              shape: def.shape,
              obtained_via: 'battle_1v1',
              created_at: nowIso
            })) : [];
            const nextVault = await saveMiniAppVault(resolved, (prev) => {
              const w = prev.ecosystem_wallet || { user_id: resolved.targetUserId, balance: 0, total_earned: 0, total_spent: 0 };
              const spent = resolved.isCeo ? 0 : wager;
              const nextBal = resolved.isCeo ? Math.max(999999, Number(w.balance || 0)) : Math.max(0, Number(w.balance || 0) - spent + rewardCxp);
              return {
                ...prev,
                ecosystem_wallet: { ...w, balance: nextBal, total_earned: Number(w.total_earned || 0) + rewardCxp, total_spent: Number(w.total_spent || 0) + spent, updated_at: nowIso },
                ecosystem_inventory: [...wonItems, ...(prev.ecosystem_inventory || [])]
              };
            });
            sendJson(res, 200, { ok: true, won, player_item: playerRoll, bot_item: botRoll, reward_cxp: rewardCxp, wallet: nextVault.ecosystem_wallet, inventory: nextVault.ecosystem_inventory });
            return;
          }
          if (action === 'pin_item') {
            const id = clean(body.item_id || '');
            const nextVault = await saveMiniAppVault(resolved, (prev) => ({
              ...prev,
              ecosystem_inventory: (prev.ecosystem_inventory || []).map((it) => String(it.id) === id ? { ...it, is_pinned: !it.is_pinned } : it)
            }));
            sendJson(res, 200, { ok: true, inventory: nextVault.ecosystem_inventory });
            return;
          }
          if (action === 'create_gift') {
            const id = clean(body.item_id || '');
            const target = (resolved.vault.ecosystem_inventory || []).find((it) => String(it.id) === id);
            if (!target) { sendJson(res, 404, { ok: false, error: 'Экземпляр не найден на складе' }); return; }
            const payloadJson = JSON.stringify({
              id: target.id,
              item_id: target.item_id,
              name: target.item_name,
              specimen_code: target.specimen_code,
              rarity: target.rarity,
              serial: target.serial_number || 1,
              from: resolved.targetUserId,
              ts: Date.now()
            });
            const b64 = Buffer.from(payloadJson, 'utf8').toString('base64url');
            const sig = signMiniAppGiftPayload(b64);
            const giftCode = `GIFT-${b64}.${sig}`;
            const nextVault = await saveMiniAppVault(resolved, (prev) => ({
              ...prev,
              ecosystem_inventory: (prev.ecosystem_inventory || []).filter((it) => String(it.id) !== id),
              ecosystem_removed_item_ids: [...new Set([...(prev.ecosystem_removed_item_ids || []), id])].slice(-200)
            }));
            sendJson(res, 200, { ok: true, gift_code: giftCode, gift_url: `https://cxrnermusic.vercel.app/drop?gift=${encodeURIComponent(giftCode)}`, item_name: target.item_name, inventory: nextVault.ecosystem_inventory });
            return;
          }
          if (action === 'claim_code') {
            const rawCode = clean(body.code || '');
            const upperCode = rawCode.toUpperCase();
            if (!rawCode) { sendJson(res, 400, { ok: false, error: 'Введи код' }); return; }
            if (rawCode.startsWith('GIFT-') && rawCode.includes('.')) {
              const parts = rawCode.slice(5).split('.');
              if (parts.length !== 2 || signMiniAppGiftPayload(parts[0]) !== parts[1]) {
                sendJson(res, 400, { ok: false, error: 'Недействительный или поддельный GIFT-код' });
                return;
              }
              const parsed = JSON.parse(Buffer.from(parts[0], 'base64url').toString('utf8'));
              const claimedGifts = new Set(resolved.vault.ecosystem_claimed_gifts || []);
              if (claimedGifts.has(rawCode)) { sendJson(res, 400, { ok: false, error: 'Этот подарок уже был активирован' }); return; }
              const def = DROP_SPECIMENS_CATALOG.find((s) => s.item_id === parsed.item_id) || DROP_SPECIMENS_CATALOG[0];
              const giftedItem = {
                id: crypto.randomUUID(),
                user_id: resolved.targetUserId,
                item_id: def.item_id,
                item_name: parsed.name || def.item_name,
                specimen_code: parsed.specimen_code || def.specimen_code,
                specimen_class: def.specimen_class,
                collection_name: def.collection_name,
                rarity: parsed.rarity || def.rarity,
                serial_number: Number(parsed.serial || 1),
                description: def.description,
                model_color: def.model_color,
                shape: def.shape,
                obtained_via: 'gift_claim',
                created_at: nowIso
              };
              const nextVault = await saveMiniAppVault(resolved, (prev) => ({
                ...prev,
                ecosystem_claimed_gifts: [...(prev.ecosystem_claimed_gifts || []), rawCode].slice(-100),
                ecosystem_inventory: [giftedItem, ...(prev.ecosystem_inventory || [])]
              }));
              sendJson(res, 200, { ok: true, type: 'gift', item: giftedItem, inventory: nextVault.ecosystem_inventory });
              return;
            }
            const promo = DROP_PROMO_CODES[upperCode];
            if (!promo) { sendJson(res, 400, { ok: false, error: 'Промокод не найден' }); return; }
            const used = new Set(resolved.vault.ecosystem_claimed_promos || []);
            if (used.has(upperCode)) { sendJson(res, 400, { ok: false, error: 'Этот промокод уже активирован' }); return; }
            const nextVault = await saveMiniAppVault(resolved, (prev) => {
              const w = prev.ecosystem_wallet || { user_id: resolved.targetUserId, balance: 0, total_earned: 0, total_spent: 0 };
              return {
                ...prev,
                ecosystem_claimed_promos: [...used, upperCode],
                ecosystem_wallet: { ...w, balance: Number(w.balance || 0) + promo.cxp, total_earned: Number(w.total_earned || 0) + promo.cxp, updated_at: nowIso }
              };
            });
            sendJson(res, 200, { ok: true, type: 'promo', cxp: promo.cxp, label: promo.label, wallet: nextVault.ecosystem_wallet });
            return;
          }
          sendJson(res, 400, { ok: false, error: 'Неизвестное действие CXRNER DROP' });
        }).catch((error) => sendJson(res, 400, { ok: false, error: clean(error?.message || error) }));
        return;
      }
            if (u.pathname === '/api/moderation/action' && req.method === 'POST') {
        const incomingSecret = clean(
          req.headers['x-cxrner-secret'] ||
          req.headers['x-secret-key'] ||
          req.headers['x-api-key'] ||
          (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '') ||
          ''
        );
        if (WEBHOOK_SECRET && incomingSecret && incomingSecret !== WEBHOOK_SECRET) {
          sendJson(res, 401, { ok: false, error: 'unauthorized' });
          return;
        }

        readJsonBody(req).then(async (body) => {
          const result = await executeModerationAction(body);
          sendJson(res, 200, { ok: true, ...result });
        }).catch((err) => {
          console.error('[api/moderation/action] Error:', err);
          sendJson(res, 500, { ok: false, error: clean(err?.message || err) });
        });
        return;
      }
      if (u.pathname === '/health' || u.pathname === '/healthz') {
        sendJson(res, 200, {
          ok: true,
          service: 'cxrner-music-bot',
          build_version: BOT_BUILD_VERSION,
          deploy: {
            ...BOT_DEPLOY_INFO,
            started_at: BOT_STARTED_AT,
            uptime_sec: Math.floor((Date.now() - BOT_STARTED_AT_MS) / 1000)
          },
          webapp: true,
          port: WEB_PORT,
          now: new Date().toISOString()
        });
        return;
      }
      if (u.pathname === '/api/miniapp/diagnostics' && req.method === 'GET') {
        buildRuntimeDiagnostics(req)
          .then((data) => sendJson(res, 200, data))
          .catch((error) => sendJson(res, 500, { ok: false, error: clean(error?.message || error) }));
        return;
      }
      if (u.pathname === '/api/miniapp/session' && req.method === 'POST') {
        let body = '';
        req.on('data', (chunk) => {
          body += chunk;
          if (body.length > 20000) req.destroy();
        });
        req.on('end', async () => {
          try {
            const payload = JSON.parse(body || '{}');
            const initData = clean(payload.init_data || payload.initData || '');
            const check = verifyTelegramInitData(initData);
            if (!check.ok || !check.user?.id) {
              sendJson(res, 403, { ok: false, error: 'Telegram initData validation failed', reason: check.reason });
              return;
            }
            const uid = String(check.user.id);
            await supabaseUpsertCabinetUser(uid, check.user, true);
            const snapshot = await getCabinetSnapshot(uid);
            sendJson(res, 200, {
              ok: true,
              user: {
                id: check.user.id,
                username: clean(check.user.username || ''),
                first_name: clean(check.user.first_name || ''),
                last_name: clean(check.user.last_name || ''),
                photo_url: clean(check.user.photo_url || ''),
                role: isAdmin(uid) ? 'admin' : 'artist'
              },
              ...snapshot
            });
          } catch (e) {
            sendJson(res, 400, { ok: false, error: clean(e?.message || e) || 'invalid request' });
          }
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
      if (/^\/link\/[a-z0-9-]+$/i.test(u.pathname || '')) {
        const linkFile = path.join(root, 'link.html');
        if (fs.existsSync(linkFile)) {
          res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
          fs.createReadStream(linkFile).pipe(res);
          return;
        }
      }
      let p = decodeURIComponent(u.pathname || '/');
      if (p === '/') p = '/index.html';
      const f = path.normalize(path.join(root, p));
      if (!f.startsWith(root) || !fs.existsSync(f) || !fs.statSync(f).isFile()) {
        res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
        res.end('Not found');
        return;
      }
      const ext = path.extname(f).toLowerCase();
      const headers = {
        'content-type': ct[ext] || 'application/octet-stream',
        'x-content-type-options': 'nosniff'
      };
      if (['.html', '.js', '.css'].includes(ext)) {
        headers['cache-control'] = 'no-cache, must-revalidate';
      }
      res.writeHead(200, headers);
      fs.createReadStream(f).pipe(res);
    } catch {
      res.writeHead(500, { 'content-type': 'text/plain; charset=utf-8' });
      res.end('Server error');
    }
  });
  srv.listen(WEB_PORT, WEB_HOST, () => {
    console.info(`[web] static server started: http://${WEB_HOST}:${WEB_PORT} (dir: ${root})`);
    console.info(`[web] diagnostics: /health and /api/miniapp/diagnostics`);
    console.info(`[web] runtime env: PORT=${clean(process.env.PORT || '(unset)')} WEBAPP_URL=${clean(process.env.WEBAPP_URL || '(from deploy_config)')}`);
    console.info(`[web] BotFather Main App URL: ${WEBAPP_URL || '(not configured)'}`);
    const checkRoute = (route) => new Promise((resolve) => {
      const request = http.get({ hostname: '127.0.0.1', port: WEB_PORT, path: route, timeout: 5000 }, (response) => {
        response.resume();
        response.on('end', () => resolve(`${route}=${response.statusCode}`));
      });
      request.on('error', (error) => resolve(`${route}=ERROR:${clean(error?.code || error?.message || error)}`));
      request.on('timeout', () => { request.destroy(); resolve(`${route}=TIMEOUT`); });
    });
    Promise.all(['/health', '/index.html', '/api/miniapp/ping'].map(checkRoute))
      .then((checks) => console.info(`[web-check] local routes: ${checks.join(' ')}`))
      .catch((error) => console.error(`[web-check] failed: ${clean(error?.message || error)}`));
  });
}

function buildSupabaseInteractionRows() {
  const rows = [];
  for (const [uid, listRaw] of Object.entries(db || {})) {
    const list = Array.isArray(listRaw) ? listRaw : [];
    for (let releaseIdx = 0; releaseIdx < list.length; releaseIdx += 1) {
      const interactions = Array.isArray(list[releaseIdx]?.interactions) ? list[releaseIdx].interactions : [];
      interactions.forEach((interaction, interactionIdx) => {
        if (!interaction || typeof interaction !== 'object') return;
        const at = Date.parse(String(interaction.at || ''));
        rows.push({
          user_id: String(uid),
          release_idx: releaseIdx,
          interaction_idx: interactionIdx,
          interaction: safeJson(interaction) || {},
          occurred_at: Number.isFinite(at) ? new Date(at).toISOString() : null,
          updated_at: new Date().toISOString()
        });
      });
    }
  }
  return rows;
}

let offset = 0;
let stopping = false;
let pollingFailStreak = 0;
async function loop() {
  while (!stopping) {
    try {
      const updates = await tg('getUpdates', { timeout: 50, offset, allowed_updates: ['message', 'callback_query'] });
      pollingHealth.ok = true;
      pollingHealth.fail_streak = 0;
      pollingHealth.last_poll_ok_at = new Date().toISOString();
      pollingHealth.last_error = '';
      if (pollingFailStreak > 0) {
        console.info(`[bot] polling recovered after ${pollingFailStreak} errors`);
        pollingFailStreak = 0;
      }
      for (const u of updates) {
        offset = Number(u.update_id) + 1;
        pollingHealth.last_update_at = new Date().toISOString();
        try {
          if (u.message) await onMessage(u.message);
          else if (u.callback_query) await onCallback(u.callback_query);
        } catch (e) {
          const errMsg = clean(e?.message || e);
          if (pollingHealth.last_command_result === 'handling') {
            pollingHealth.last_command_result = 'error';
            pollingHealth.last_command_error = errMsg;
          }
          console.error('[bot] update error:', errMsg);
        }
      }
    } catch (e) {
      pollingFailStreak += 1;
      const errText = clean(e?.message || e) || 'unknown error';
      pollingHealth.ok = false;
      pollingHealth.fail_streak = pollingFailStreak;
      pollingHealth.last_error = errText;
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

async function registerModerationCommands() {
  const modCommands = [
    { command: 'queue', description: 'Очередь релизов на отгрузку и модерацию' },
    { command: 'find', description: 'Поиск релиза по названию, нику или UPC' },
    { command: 'status', description: 'Версия деплоя, отклик и состояние бота' },
    { command: 'unpin_all', description: 'Сбросить все старые закрепы и оставить только «На отгрузке»' },
    { command: 'unpin_done', description: 'Открепить последние карточки не в статусе «На отгрузке»' },
    { command: 'unpin', description: 'Открепить сообщение (ответом на карточку)' },
    { command: 'new', description: 'Пометить анкеты «На отгрузке» свободными' },
    { command: 'modtest', description: 'Проверить права бота в группе модерации' }
  ];
  for (const scope of [
    { type: 'chat', chat_id: MOD_CHAT },
    { type: 'chat_administrators', chat_id: MOD_CHAT }
  ]) {
    try {
      await tg('setMyCommands', { commands: modCommands, scope });
    } catch (err) {
      console.warn(`[bot] setMyCommands (${scope.type}) warning: ${shortTgError(err)}`);
    }
  }
}

(async () => {
  fs.mkdirSync(path.resolve(ROOT, 'webapp/data'), { recursive: true });
  // Start the Mini App before optional remote hydration so a Supabase DNS
  // outage cannot prevent the web interface and health checks from opening.
  startStaticServer();
  const imported = importBackupsIntoDb();
  console.info(`[bot] CXRNER Node fallback bot started build=${BOT_BUILD_VERSION} commit=${BOT_DEPLOY_INFO.commit || 'none'}`);
  console.info(`[bot] moderation chat: ${MOD_CHAT}`);
  console.info(`[bot] admin ids: ${ADMIN_IDS.join(', ')}`);
  if (imported.added || imported.merged) {
    console.info(`[bot] backup import: added=${imported.added}, merged=${imported.merged}`);
  }
  if (SUPABASE_SYNC_ENABLED) {
    const keyLen = (SUPABASE_SERVICE_ROLE_KEY || '').length;
    console.info(`[bot] supabase sync: enabled (${SUPABASE_URL})`);
    console.info(`[bot] supabase key: length=${keyLen}${keyLen < 180 ? ' (возможно anon или обрезка — нужен service_role)' : ''}`);
    console.info(
      `[bot] supabase tables: ${SUPABASE_RELEASES_TABLE}, ${SUPABASE_CABINET_TABLE}, ` +
      `${SUPABASE_FORMS_TABLE}, ${SUPABASE_USERS_TABLE}, ${SUPABASE_PUBLIC_RELEASES_TABLE}`
    );
    console.info(
      `[bot] supabase fetch: timeout=${SUPABASE_FETCH_TIMEOUT_MS}ms` +
      ` retries=${SUPABASE_FETCH_RETRIES} delay=${SUPABASE_FETCH_RETRY_DELAY_MS}ms` +
      ` chunk=${SUPABASE_SYNC_CHUNK_SIZE} chunkDelay=${SUPABASE_SYNC_CHUNK_DELAY_MS}ms`
    );
  } else {
    console.info('[bot] supabase sync: disabled');
  }
  console.info(`[bot] telegram api base: ${TELEGRAM_API_BASE}`);
  console.info(`[bot] tg fetch: timeout=${TG_FETCH_TIMEOUT_MS}ms retries=${TG_FETCH_RETRIES}`);
  console.info(`[bot] moderation thread: ${MODERATION_THREAD_ID > 0 ? MODERATION_THREAD_ID : 'default'}`);
  if (WEBAPP_URL) console.info(`[bot] webapp url: ${WEBAPP_URL}`);

  // Register commands & verify chat access BEFORE slower Supabase sync so the
  // Telegram / command menu and health state update within ~1s of boot.
  await verifyModerationChatAccess();
  await registerModerationCommands();
  if (SUPABASE_SYNC_ENABLED) {
    await hydrateFromSupabase();
  }
  exportReleases();
  exportCabinet();
  if (SUPABASE_SYNC_ENABLED) {
    void syncSupabaseNow('startup').catch((err) => {
      console.warn(`[bot] background startup supabase sync warning: ${shortTgError(err)}`);
    });
  }
  try { await tg('deleteWebhook', { drop_pending_updates: false }); } catch (err) {
    console.warn(`[bot] deleteWebhook warning: ${shortTgError(err)}`);
  }
  const bootAutoPinTimer = setTimeout(() => void reconcilePendingWebFormsAndPins().catch(() => {}), 4000);
  if (typeof bootAutoPinTimer?.unref === 'function') bootAutoPinTimer.unref();
  const periodicAutoPinTimer = setInterval(() => void reconcilePendingWebFormsAndPins().catch(() => {}), 45000);
  if (typeof periodicAutoPinTimer?.unref === 'function') periodicAutoPinTimer.unref();
  process.on('SIGINT', () => { stopping = true; process.exit(0); });
  process.on('SIGTERM', () => { stopping = true; process.exit(0); });
  await loop();
})();
