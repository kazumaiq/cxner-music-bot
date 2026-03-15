# -*- coding: utf-8 -*-
# На нодах без UTF-8 (например Амстердам) русский текст может уходить в Telegram кракозябрами.
# Задаём локаль до импортов, чтобы строки в коде и при отправке были в UTF-8.
import os
if os.environ.get("LANG", "").strip() in ("", "C", "POSIX"):
    os.environ["LANG"] = "en_US.UTF-8"
if os.environ.get("LC_ALL", "").strip() in ("", "C", "POSIX"):
    os.environ["LC_ALL"] = "en_US.UTF-8"

import asyncio
import json
import re
import sys
import tempfile
import threading
import warnings
from datetime import datetime, timedelta
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
    WebAppInfo,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

try:
    # python-telegram-bot 21.x РёСЃРїРѕР»СЊР·СѓРµС‚ httpx РІРЅСѓС‚СЂРё, РёРЅРѕРіРґР° РїСЂРѕР±СЂР°СЃС‹РІР°РµС‚ РѕС€РёР±РєРё РїСЂРѕС‚РѕРєРѕР»Р°.
    import httpx  # type: ignore
except Exception:  # pragma: no cover
    httpx = None

try:
    # Windows consoles may default to cp1251 and crash on emoji output.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# PTB с per_message=False выдаёт предупреждение; с per_message=True все хендлеры должны быть CallbackQueryHandler.
# У нас форма с MessageHandler, оставляем per_message=False и скрываем предупреждение.
try:
    from telegram.warnings import PTBUserWarning
    warnings.filterwarnings("ignore", message=".*per_message.*", category=PTBUserWarning)
except ImportError:
    warnings.filterwarnings("ignore", message=".*per_message.*", category=UserWarning)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except Exception:
        return default


def _load_local_config(path: str = "deploy_config.json") -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


LOCAL_CONFIG = _load_local_config()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_int_list(name: str, default: list[int]) -> list[int]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    result: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            result.append(int(item))
        except Exception:
            continue
    return result or default


def _cfg_str(name: str, default: str = "") -> str:
    env_val = os.getenv(name)
    if env_val is not None and env_val.strip() != "":
        return env_val.strip()
    cfg_val = LOCAL_CONFIG.get(name)
    if cfg_val is None:
        # common aliases for hosting panels / legacy configs
        aliases = {
            "BOT_TOKEN": ("TOKEN", "bot_token", "token"),
            "WEBAPP_URL": ("webapp_url",),
            "PUBLIC_BASE_URL": ("public_base_url", "base_url"),
        }.get(name, ())
        for alias in aliases:
            if alias in LOCAL_CONFIG and str(LOCAL_CONFIG.get(alias) or "").strip():
                cfg_val = LOCAL_CONFIG.get(alias)
                break
    if cfg_val is None:
        return default
    return str(cfg_val).strip()


def _cfg_int(name: str, default: int) -> int:
    env_val = os.getenv(name)
    if env_val is not None and env_val.strip() != "":
        try:
            return int(env_val.strip())
        except Exception:
            return default
    cfg_val = LOCAL_CONFIG.get(name)
    if cfg_val is None:
        return default
    try:
        return int(cfg_val)
    except Exception:
        return default


def _cfg_bool(name: str, default: bool = False) -> bool:
    env_val = os.getenv(name)
    if env_val is not None and env_val.strip() != "":
        return env_val.strip().lower() in {"1", "true", "yes", "on"}
    cfg_val = LOCAL_CONFIG.get(name, default)
    if isinstance(cfg_val, bool):
        return cfg_val
    if isinstance(cfg_val, (int, float)):
        return bool(cfg_val)
    if isinstance(cfg_val, str):
        return cfg_val.strip().lower() in {"1", "true", "yes", "on"}
    return default


def _cfg_int_list(name: str, default: list[int]) -> list[int]:
    env_val = os.getenv(name)
    if env_val is not None and env_val.strip() != "":
        return _env_int_list(name, default)

    cfg_val = LOCAL_CONFIG.get(name)
    if cfg_val is None:
        return default
    result: list[int] = []
    if isinstance(cfg_val, list):
        items = cfg_val
    else:
        items = str(cfg_val).split(",")
    for item in items:
        try:
            result.append(int(str(item).strip()))
        except Exception:
            continue
    return result or default


# === РљРћРќР¤РР“ ===
TOKEN = _cfg_str("BOT_TOKEN", "")
MODERATION_CHAT_ID = _cfg_int("MODERATION_CHAT_ID", -1002117586464)
ADMIN_IDS = _cfg_int_list("ADMIN_IDS", [881379104])
WEBAPP_URL = _cfg_str("WEBAPP_URL", "")
PUBLIC_BASE_URL = _cfg_str("PUBLIC_BASE_URL", "")
if not WEBAPP_URL and PUBLIC_BASE_URL:
    WEBAPP_URL = f"{PUBLIC_BASE_URL.rstrip('/')}/index.html"
ARTISTS_CHAT = "https://t.me/+oVmX3_dkyWJhNjJi"
CHANNEL = "https://t.me/cxrnermusic"
DB_FILE = "releases.json"
MODERATION_DB_FILE = "moderation_releases.json"
HISTORY_FILE = "history.json"
CABINET_USERS_FILE = "cabinet_users.json"
WEBAPP_DATA_DIR = os.path.join("webapp", "data")
WEBAPP_RELEASES_EXPORT_FILE = os.path.join(WEBAPP_DATA_DIR, "releases-public.json")
WEBAPP_CABINET_EXPORT_FILE = os.path.join(WEBAPP_DATA_DIR, "cabinet-users.json")
ENABLE_WEB_SERVER = _cfg_bool("ENABLE_WEB_SERVER", True)
WEB_SERVER_HOST = _cfg_str("WEB_SERVER_HOST", "0.0.0.0")
WEB_SERVER_PORT = _cfg_int("PORT", _cfg_int("WEB_SERVER_PORT", 8080))
WEB_SERVER_DIR = _cfg_str("WEB_SERVER_DIR", "webapp")

# === Р­РњРћР”Р—Р РРќРўР•Р Р¤Р•Р™РЎРђ ===
WINTER_EMOJIS = {
    "snowflake": "рџЋµ",
    "snowman": "рџ—‚пёЏ",
    "tree": "рџљЂ",
    "gift": "рџЋЃ",
    "sparkles": "вњЁ",
    "star": "в­ђпёЏ",
    "fire": "рџ”Ґ",
    "notes": "рџЋµ",
    "headphones": "рџЋ§",
    "clock": "вЏ°",
    "check": "вњ…",
    "cross": "вќЊ",
    "music": "рџЋ¶",
    "waiting": "вЏі",
    "published": "рџ“ў",
    "calendar": "рџ“…",
    "warning": "вљ пёЏ",
    "comment": "рџ’¬",
    "telegram": "рџ“±",
    "list": "рџ“‹",
    "users": "рџ‘Ґ",
    "stats": "рџ“Љ",
    "settings": "вљ™пёЏ",
    "refresh": "рџ”„",
    "brain": "рџ§ ",
    "upload": "рџ•“",
    "delete": "рџ—‘"
}

# === РЎРћРЎРўРћРЇРќРРЇ ===
# NOTE: РЎРѕС…СЂР°РЅСЏРµРј ConversationHandler, РЅРѕ РґРµР»Р°РµРј callback-СЂРѕСѓС‚РµСЂ РіР»РѕР±Р°Р»СЊРЅС‹Рј (С‡С‚РѕР±С‹ /admin РєРЅРѕРїРєРё СЂР°Р±РѕС‚Р°Р»Рё РІРЅРµ РґРёР°Р»РѕРіР°).
(
    REPORT,
    TYPE,
    NAME,
    SUBNAME,
    UPC,
    ISRC,
    HAS_LYRICS,
    SNIPPET_MODE,
    NICK,
    FIO,
    DATE,
    VERSION,
    GENRE,
    LINK,
    MAT,
    PROMO,
    COMMENT,
    TRACKLIST,
    TG,
    YANDEX,
    CONFIRM,
) = range(21)

# --- РќРѕРІС‹Рµ СЃРѕСЃС‚РѕСЏРЅРёСЏ РґР»СЏ Р·Р°РєР°Р·Р° РѕР±Р»РѕР¶РєРё Рё РїСЂРѕРјРѕ-С‚РµРєСЃС‚Р°
COVER_REF = 21
COVER_COLORS = 22
COVER_TITLE = 23
COVER_PREFS = 24
COVER_TG = 25
COVER_PAYMENT = 26
COVER_WAIT_SCREENSHOT = 27

PROMO_ARTIST = 28
PROMO_PROJECT = 29
PROMO_RELEASE_NAME = 30
PROMO_RELEASE_KIND = 31
PROMO_GENRE_MAIN = 32
PROMO_GENRE_EXTRA = 33
PROMO_MOOD = 34
PROMO_VIBE = 35
PROMO_SOUND = 36
PROMO_VOCAL = 37
PROMO_LANGUAGE = 38
PROMO_EMOTION = 39
PROMO_USECASE = 40
PROMO_COUNTRY = 41
PROMO_DONE = 42

# РЎС‚Р°С‚СѓСЃС‹ Р°РЅРєРµС‚ (РёСЃРїРѕР»СЊР·СѓР№С‚Рµ СЌС‚Рё Р·РЅР°С‡РµРЅРёСЏ РІ `status` РїРѕР»СЏС…)
STATUS_ON_UPLOAD = "on_upload"      # РќР° РѕС‚РіСЂСѓР·РєРµ (РїРѕСЃС‚Р°РІР»СЏРµС‚СЃСЏ РїСЂРё РѕС‚РїСЂР°РІРєРµ)
STATUS_MODERATION = "moderation"    # РќР° РјРѕРґРµСЂР°С†РёРё (РјРѕРґРµСЂР°С‚РѕСЂ РІР·СЏР» РІ СЂР°Р±РѕС‚Сѓ)
STATUS_APPROVED = "approved"        # РћРґРѕР±СЂРµРЅРѕ
STATUS_REJECTED = "rejected"        # РћС‚РєР»РѕРЅРµРЅРѕ
STATUS_NEEDS_FIX = "needs_fix"      # РќР° РёСЃРїСЂР°РІР»РµРЅРёРё
STATUS_DELETED = "deleted"          # РЈРґР°Р»РµРЅРѕ (СЃР»СѓР¶РµР±РЅРѕ)

# === Р‘Р” / РҐР РђРќРР›РР©Р• ===
# Р“Р»Р°РІРЅР°СЏ РїСЂРёС‡РёРЅР° вЂњРїСЂРѕРїР°РґР°СЋС‚ СЂРµР»РёР·С‹/РєР°Р±РёРЅРµС‚С‹вЂќ: РЅРµР°С‚РѕРјР°СЂРЅР°СЏ Р·Р°РїРёСЃСЊ JSON + РІРѕР·РјРѕР¶РЅС‹Рµ С‡Р°СЃС‚РёС‡РЅС‹Рµ Р·Р°РїРёСЃРё/РєРѕСЂСЂСѓРїС†РёСЏ.
# Р”РµР»Р°РµРј Р°С‚РѕРјР°СЂРЅС‹Р№ СЃРµР№РІ (temp + os.replace), Р° С‚Р°РєР¶Рµ safe-load СЃ СЂРµР·РµСЂРІРЅРѕР№ РєРѕРїРёРµР№.
def _atomic_write_json(path: str, obj: object) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp", dir=os.path.dirname(path) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def _load_json_or_default(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        # Р•СЃР»Рё С„Р°Р№Р» С‡Р°СЃС‚РёС‡РЅРѕ Р·Р°РїРёСЃР°Р»СЃСЏ/СЃР»РѕРјР°Р»СЃСЏ вЂ” РЅРµ РїР°РґР°РµРј Рё РЅРµ Р·Р°С‚РёСЂР°РµРј РґР°РЅРЅС‹РјРё РІ РїР°РјСЏС‚Рё.
        print(f"вќЊ РћС€РёР±РєР° С‡С‚РµРЅРёСЏ {path}: {e}")
        return default


def load_db():
    return _load_json_or_default(DB_FILE, {})


def _export_webapp_releases(db_obj):
    payload = {"updated_at": datetime.now().isoformat(), "users": {}}
    for uid, rels in (db_obj or {}).items():
        uid_s = str(uid)
        safe_releases = []
        for idx, rel in enumerate(rels or []):
            if not isinstance(rel, dict):
                continue
            safe_releases.append({
                "id": idx,
                "type": rel.get("type", ""),
                "name": rel.get("name", ""),
                "subname": rel.get("subname", ""),
                "nick": rel.get("nick", ""),
                "date": rel.get("date", ""),
                "genre": rel.get("genre", ""),
                "status": rel.get("status", STATUS_ON_UPLOAD),
                "submission_time": rel.get("submission_time", ""),
                "moderation_time": rel.get("moderation_time", ""),
                "reject_reason": rel.get("reject_reason", ""),
                "moderator_comment": rel.get("moderator_comment", ""),
                "upc": rel.get("upc", ""),
                "link_published": rel.get("link_published", ""),
                "source": rel.get("source", "bot"),
                "user_deleted": bool(rel.get("user_deleted", False)),
            })
        payload["users"][uid_s] = safe_releases
    _atomic_write_json(WEBAPP_RELEASES_EXPORT_FILE, payload)


def load_cabinet_users():
    return _load_json_or_default(CABINET_USERS_FILE, {})


def _export_webapp_cabinet_users(cabinet_users_obj):
    payload = {"updated_at": datetime.now().isoformat(), "users": {}}
    for uid, info in (cabinet_users_obj or {}).items():
        if not isinstance(info, dict):
            continue
        payload["users"][str(uid)] = {
            "approved": bool(info.get("approved", True)),
            "activated_at": info.get("activated_at", ""),
            "username": info.get("username", ""),
            "first_name": info.get("first_name", ""),
        }
    _atomic_write_json(WEBAPP_CABINET_EXPORT_FILE, payload)


def save_cabinet_users(cabinet_users_obj):
    _atomic_write_json(CABINET_USERS_FILE, cabinet_users_obj)
    try:
        _export_webapp_cabinet_users(cabinet_users_obj)
    except Exception as e:
        print(f"РћС€РёР±РєР° СЌРєСЃРїРѕСЂС‚Р° cabinet users РґР»СЏ Mini App: {e}")


def save_db(db_obj):
    _atomic_write_json(DB_FILE, db_obj)
    try:
        _export_webapp_releases(db_obj)
    except Exception as e:
        print(f"РћС€РёР±РєР° СЌРєСЃРїРѕСЂС‚Р° СЂРµР»РёР·РѕРІ РґР»СЏ Mini App: {e}")


def load_moderation_db():
    return _load_json_or_default(MODERATION_DB_FILE, {"moderation_messages": []})


def save_moderation_db(moderation_db_obj):
    _atomic_write_json(MODERATION_DB_FILE, moderation_db_obj)

def update_moderation_record(user_id, idx, release_data):
    """РћР±РЅРѕРІР»СЏРµС‚ Р·Р°РїРёСЃСЊ РІ moderation_releases.json РїСЂРё РёР·РјРµРЅРµРЅРёРё СЃС‚Р°С‚СѓСЃР°"""
    try:
        moderation_db = load_moderation_db()
        if 'moderation_messages' in moderation_db:
            for msg in moderation_db['moderation_messages']:
                if msg.get('user_id') == user_id:
                    # РЎСЂР°РІРЅРёРІР°РµРј submission_time РєР°Рє ID СЂРµР»РёР·Р°
                    if msg.get('submission_time') == release_data.get('submission_time'):
                        # РћР±РЅРѕРІР»СЏРµРј СЃС‚Р°С‚СѓСЃ
                        msg['status'] = release_data.get('status')
                        msg['moderator'] = release_data.get('moderator')
                        msg['moderation_time'] = release_data.get('moderation_time')
                        msg['reject_reason'] = release_data.get('reject_reason')
                        save_moderation_db(moderation_db)
                        break
    except Exception as e:
        print(f"РћС€РёР±РєР° РїСЂРё РѕР±РЅРѕРІР»РµРЅРёРё Р·Р°РїРёСЃРё РІ РјРѕРґРµСЂР°С†РёРё: {e}")

# === РРЎРўРћР РРЇ РР—РњР•РќР•РќРР™ ===
def load_history():
    return _load_json_or_default(HISTORY_FILE, {})

def save_history(history):
    _atomic_write_json(HISTORY_FILE, history)

def add_history_entry(user_id, idx, old_status, new_status, moderator_id, moderator_name, reason=None):
    """Р”РѕР±Р°РІР»СЏРµС‚ Р·Р°РїРёСЃСЊ РІ РёСЃС‚РѕСЂРёСЋ РёР·РјРµРЅРµРЅРёР№"""
    history = load_history()
    key = f"{user_id}_{idx}"
    if key not in history:
        history[key] = []
    
    entry = {
        'timestamp': datetime.now().isoformat(),
        'old_status': old_status,
        'new_status': new_status,
        'moderator_id': moderator_id,
        'moderator_name': moderator_name,
        'reason': reason
    }
    history[key].append(entry)
    save_history(history)

user_data = {}
db = load_db()
moderation_db = load_moderation_db()
cabinet_users = load_cabinet_users()

try:
    _export_webapp_releases(db)
    _export_webapp_cabinet_users(cabinet_users)
except Exception as e:
    print(f"РћС€РёР±РєР° РїРµСЂРІРёС‡РЅРѕРіРѕ СЌРєСЃРїРѕСЂС‚Р° РґР°РЅРЅС‹С… Mini App: {e}")

# === DRAFTS (Р°РІС‚РѕСЃРѕС…СЂР°РЅРµРЅРёРµ РїСЂРѕРјРµР¶СѓС‚РѕС‡РЅС‹С… РґР°РЅРЅС‹С…) ===
DRAFTS_FILE = "drafts.json"

def load_drafts():
    return _load_json_or_default(DRAFTS_FILE, {})

def save_drafts(obj):
    _atomic_write_json(DRAFTS_FILE, obj)

def save_draft_for_user(user_id: str):
    drafts = load_drafts()
    drafts[user_id] = {k: v for k, v in user_data.get(user_id, {}).items() if not k.startswith('_')}
    drafts[user_id]['saved_at'] = datetime.now().isoformat()
    save_drafts(drafts)

def delete_draft_for_user(user_id: str):
    drafts = load_drafts()
    if user_id in drafts:
        drafts.pop(user_id, None)
        save_drafts(drafts)

def pop_last_history(user_id: str):
    hist = user_data.get(user_id, {}).get('_history', [])
    if not hist:
        return None
    last = hist.pop()
    # update stored history
    user_data[user_id]['_history'] = hist
    return last


# === Р­РљР РђРќРР РћР’РђРќРР• HTML ===
def escape_html(text):
    if not text:
        return ""
    return (str(text)
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;'))

def clean(text):
    return ' '.join([w for w in text.split() if not w.lower().startswith(('1.', '2.', '3.'))]).strip()


def _looks_like_url(text: str) -> bool:
    if not text or not isinstance(text, str):
        return False
    text = text.strip()
    if text == ".":
        return True
    p = urlparse(text)
    if p.scheme not in ("http", "https"):
        return False
    if not p.netloc:
        return False
    # basic sanity: netloc should contain a dot or be localhost
    if "." not in p.netloc and p.netloc != "localhost":
        return False
    return True


def _looks_like_drive_link(text: str) -> bool:
    if not _looks_like_url(text):
        return False
    lower = text.lower()
    # РџСЂРѕРІРµСЂСЏРµРј РЅР°Р»РёС‡РёРµ drive.google.com РёР»Рё docs.google.com РІ Р»СЋР±РѕР№ С‡Р°СЃС‚Рё URL
    return ("drive.google.com" in lower or 
            "docs.google.com" in lower or 
            "drive.google" in lower or
            "/d/" in text)  # Google Drive С„Р°Р№Р»/РїР°РїРєР° РІСЃРµРіРґР° СЃРѕРґРµСЂР¶РёС‚ /d/


def _looks_like_yandex_music_link(text: str) -> bool:
    if not _looks_like_url(text):
        return False
    lower = text.lower()
    return "music.yandex" in lower or "yandex.ru" in lower


def _normalize_optional_text(value, default: str = ".") -> str:
    text = clean(str(value or "")).strip()
    return text if text else default


def _normalize_release_type(value: str) -> str | None:
    v = clean(str(value or "")).strip().lower()
    if v in {"СЃРёРЅРіР»", "single", "singl"}:
        return "СЃРёРЅРіР»"
    if v in {"Р°Р»СЊР±РѕРј", "album"}:
        return "Р°Р»СЊР±РѕРј"
    return None

# === Р‘Р•Р—РћРџРђРЎРќРђРЇ РћРўРџР РђР’РљРђ / Р Р•РўР РђР (РІ С‚.С‡. httpx.RemoteProtocolError) ===
def _strip_html(text: str) -> str:
    return (
        text.replace("<b>", "")
        .replace("</b>", "")
        .replace("<i>", "")
        .replace("</i>", "")
        .replace("<code>", "")
        .replace("</code>", "")
    )


def _is_remote_protocol_error(e: Exception) -> bool:
    # PTB РјРѕР¶РµС‚ РїСЂРѕР±СЂР°СЃС‹РІР°С‚СЊ httpx.RemoteProtocolError РєР°Рє context.error РёР»Рё РІРЅСѓС‚СЂРё РёСЃРєР»СЋС‡РµРЅРёР№.
    if httpx is not None and isinstance(e, getattr(httpx, "RemoteProtocolError", ())):
        return True
    return "RemoteProtocolError" in str(type(e)) or "Server disconnected without sending a response" in str(e)


async def safe_send(target, text, reply_markup=None, parse_mode=ParseMode.HTML):
    message = target if hasattr(target, "reply_text") else target.message
    for attempt in range(5):
        try:
            await message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            return
        except (TimedOut,) as e:
            await asyncio.sleep(1 + attempt)
            last = e
        except BadRequest as e:
            if "can't parse entities" in str(e).lower():
                await message.reply_text(_strip_html(text), reply_markup=reply_markup, disable_web_page_preview=True)
                return
            raise
        except Exception as e:
            # Р“Р»Р°РІРЅРѕРµ: РЅРµ РїРѕРєР°Р·С‹РІР°С‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ httpx.RemoteProtocolError, РїСЂРѕСЃС‚Рѕ СЂРµС‚СЂР°РёРј.
            if _is_remote_protocol_error(e):
                await asyncio.sleep(1 + attempt)
                last = e
                continue
            await message.reply_text(_strip_html(text), reply_markup=reply_markup, disable_web_page_preview=True)
            return
    await message.reply_text("РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ. РџРѕРїСЂРѕР±СѓР№С‚Рµ РµС‰С‘ СЂР°Р·.")
    if "last" in locals():
        print(f"вќЊ safe_send: {last}")


async def safe_edit(query, text, reply_markup=None, parse_mode=ParseMode.HTML):
    for attempt in range(5):
        try:
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            return
        except (TimedOut,) as e:
            await asyncio.sleep(1 + attempt)
            last = e
        except (BadRequest, Forbidden) as e:
            # РРЅРѕРіРґР° РЅРµР»СЊР·СЏ СЂРµРґР°РєС‚РёСЂРѕРІР°С‚СЊ (РЅР°РїСЂРёРјРµСЂ, СЃР»РёС€РєРѕРј СЃС‚Р°СЂРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ) вЂ” С€Р»С‘Рј РЅРѕРІС‹Рј СЃРѕРѕР±С‰РµРЅРёРµРј.
            await query.message.reply_text(_strip_html(text), reply_markup=reply_markup, disable_web_page_preview=True)
            return
        except Exception as e:
            if _is_remote_protocol_error(e):
                await asyncio.sleep(1 + attempt)
                last = e
                continue
            await query.message.reply_text(_strip_html(text), reply_markup=reply_markup, disable_web_page_preview=True)
            return
    if "last" in locals():
        print(f"вќЊ safe_edit: {last}")


async def safe_edit_reply_markup(query, reply_markup=None):
    for attempt in range(5):
        try:
            await query.edit_message_reply_markup(reply_markup=reply_markup)
            return
        except (TimedOut,) as e:
            await asyncio.sleep(1 + attempt)
            last = e
        except Exception as e:
            if _is_remote_protocol_error(e):
                await asyncio.sleep(1 + attempt)
                last = e
                continue
            print(f"вќЊ safe_edit_reply_markup: {e}")
            return
    if "last" in locals():
        print(f"вќЊ safe_edit_reply_markup: {last}")

# === UI РћР¤РћР РњР›Р•РќРР• ===
def winter_text(text, emoji_key=None):
    if emoji_key and emoji_key in WINTER_EMOJIS:
        return f"{WINTER_EMOJIS[emoji_key]} {text}"
    return text

def winter_header(text):
    return f"{WINTER_EMOJIS['music']} {text}"


def build_main_menu_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("рџ“Ђ Р”РёСЃС‚СЂРёР±СѓС†РёСЏ", callback_data='menu_distribution')],
        [InlineKeyboardButton("рџ’ј РЎРµСЂРІРёСЃС‹", callback_data='menu_services')],
        [InlineKeyboardButton("рџ§‘вЂЌрџ’» РљР°Р±РёРЅРµС‚", callback_data='menu_cabinet')],
        [InlineKeyboardButton("рџЊђ РљРѕРјСЊСЋРЅРёС‚Рё", callback_data='menu_community')],
    ]
    rows.append([InlineKeyboardButton("РћС‚РєСЂС‹С‚СЊ РїСЂРёР»РѕР¶РµРЅРёРµ", callback_data='open_app')])
    return InlineKeyboardMarkup(rows)


def build_distribution_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Р—Р°РіСЂСѓР·РёС‚СЊ СЂРµР»РёР·", callback_data='report')],
        [InlineKeyboardButton("РњРѕРё СЂРµР»РёР·С‹", callback_data='my_releases')],
        [InlineKeyboardButton("в¬…пёЏ Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ", callback_data='main')],
    ])


def build_services_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Р—Р°РєР°Р·Р°С‚СЊ РѕР±Р»РѕР¶РєСѓ (500СЂ)", callback_data='order_cover')],
        [InlineKeyboardButton("РџСЂРѕРјРѕ-С‚РµРєСЃС‚ РїРѕРґ СЂРµР»РёР·", callback_data='promo_text')],
        [InlineKeyboardButton("в¬…пёЏ Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ", callback_data='main')],
    ])


def build_cabinet_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("РњРѕРё СЂРµР»РёР·С‹", callback_data='my_releases')],
        [InlineKeyboardButton("РћС‚РєСЂС‹С‚СЊ РїСЂРёР»РѕР¶РµРЅРёРµ", callback_data='open_app')],
    ]
    rows.append([InlineKeyboardButton("в¬…пёЏ Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ", callback_data='main')])
    return InlineKeyboardMarkup(rows)


def build_community_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("РљР°РЅР°Р» CXRNER MUSIC", url=CHANNEL)],
        [InlineKeyboardButton("Р§Р°С‚ Р°СЂС‚РёСЃС‚РѕРІ", url=ARTISTS_CHAT)],
        [InlineKeyboardButton("РћС„РёС†РёР°Р»СЊРЅС‹Р№ СЃР°Р№С‚", url="https://cxrnermusic.vercel.app/")],
        [InlineKeyboardButton("в¬…пёЏ Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ", callback_data='main')],
    ])

# === РџР РћР’Р•Р РљРђ РђР”РњРРќРђ ===
def is_admin(user_id):
    """РџСЂРѕРІРµСЂСЏРµС‚, СЏРІР»СЏРµС‚СЃСЏ Р»Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂРѕРј"""
    # РџСЂРµРѕР±СЂР°Р·СѓРµРј user_id РІ int РґР»СЏ РєРѕСЂСЂРµРєС‚РЅРѕРіРѕ СЃСЂР°РІРЅРµРЅРёСЏ (РјРѕР¶РµС‚ Р±С‹С‚СЊ СЃС‚СЂРѕРєРѕР№ РёР»Рё int)
    try:
        user_id_int = int(user_id) if user_id else None
        if user_id_int is None:
            return False
        result = user_id_int in ADMIN_IDS
        # Р›РѕРіРёСЂСѓРµРј РґР»СЏ РѕС‚Р»Р°РґРєРё (РјРѕР¶РЅРѕ СѓР±СЂР°С‚СЊ РїРѕСЃР»Рµ РїСЂРѕРІРµСЂРєРё)
        if result:
            print(f"вњ… Р”РѕСЃС‚СѓРї СЂР°Р·СЂРµС€РµРЅ РґР»СЏ Р°РґРјРёРЅР°: {user_id_int}")
        return result
    except (ValueError, TypeError) as e:
        print(f"вќЊ РћС€РёР±РєР° РїСЂРѕРІРµСЂРєРё Р°РґРјРёРЅР° РґР»СЏ {user_id}: {e}")
        return False


def is_moderation_chat(chat_id) -> bool:
    """Checks whether the event came from the moderation group chat."""
    try:
        return int(chat_id) == int(MODERATION_CHAT_ID)
    except (TypeError, ValueError):
        return False


def is_webapp_url_ready() -> bool:
    """Returns True only when WEBAPP_URL is configured and not a placeholder."""
    if not WEBAPP_URL:
        return False
    lower = WEBAPP_URL.lower()
    if "example.com" in lower:
        return False
    return lower.startswith("https://") or lower.startswith("http://localhost") or lower.startswith("http://127.0.0.1")


def build_webapp_reply_keyboard() -> ReplyKeyboardMarkup:
    """KeyboardButton WebApp launcher required for reliable Telegram WebApp.sendData()."""
    return ReplyKeyboardMarkup(
        [[KeyboardButton("РћС‚РєСЂС‹С‚СЊ РїСЂРёР»РѕР¶РµРЅРёРµ", web_app=WebAppInfo(url=WEBAPP_URL))]],
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="РќР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ, С‡С‚РѕР±С‹ РѕС‚РєСЂС‹С‚СЊ Mini App",
    )


def start_static_web_server_if_enabled():
    """Optional static server for webapp/ directory (useful in production hosting)."""
    if not ENABLE_WEB_SERVER:
        return None

    web_root = os.path.abspath(WEB_SERVER_DIR)
    if not os.path.isdir(web_root):
        print(f"вљ пёЏ ENABLE_WEB_SERVER=1, РЅРѕ РґРёСЂРµРєС‚РѕСЂРёСЏ РЅРµ РЅР°Р№РґРµРЅР°: {web_root}")
        return None

    host = WEB_SERVER_HOST or "0.0.0.0"
    port = WEB_SERVER_PORT if WEB_SERVER_PORT > 0 else 8080
    handler = partial(SimpleHTTPRequestHandler, directory=web_root)
    try:
        server = ThreadingHTTPServer((host, port), handler)
    except Exception as e:
        print(f"вљ пёЏ РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РїСѓСЃС‚РёС‚СЊ static Mini App server РЅР° {host}:{port}: {e}")
        return None
    server.daemon_threads = True

    th = threading.Thread(target=server.serve_forever, name="webapp-static-server", daemon=True)
    th.start()
    print(f"рџЊђ Static Mini App server started on http://{host}:{port} (dir: {web_root})")
    return server

# === Р“Р›РђР’РќРћР• РњР•РќР® (/start) ===
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = build_main_menu_keyboard()
    welcome_text = (
        "Р”РѕР±СЂРѕ РїРѕР¶Р°Р»РѕРІР°С‚СЊ РІ СЃРёСЃС‚РµРјСѓ РґРёСЃС‚СЂРёР±СѓС†РёРё CXRNER MUSIC.\n"
        "РЈРїСЂР°РІР»СЏР№ СЂРµР»РёР·Р°РјРё. Р—Р°РіСЂСѓР¶Р°Р№ С‚СЂРµРєРё. РњР°СЃС€С‚Р°Р±РёСЂСѓР№ Р·РІСѓРє."
    )
    
    if update.message:
        await update.message.reply_text(
            welcome_text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
    elif update.callback_query:
        await safe_edit(update.callback_query, welcome_text, reply_markup=keyboard)
    return REPORT


async def app_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a keyboard button to open Telegram Mini App."""
    if not is_webapp_url_ready():
        await update.message.reply_text(
            "вќЊ Mini App URL РЅРµ РЅР°СЃС‚СЂРѕРµРЅ.\n"
            "РЈСЃС‚Р°РЅРѕРІРёС‚Рµ РїРµСЂРµРјРµРЅРЅСѓСЋ РѕРєСЂСѓР¶РµРЅРёСЏ:\n"
            "<code>WEBAPP_URL=https://РІР°С€-РґРѕРјРµРЅ/index.html</code>\n"
            "Рё РїРµСЂРµР·Р°РїСѓСЃС‚РёС‚Рµ Р±РѕС‚Р°.",
            parse_mode=ParseMode.HTML
        )
        return
    keyboard = build_webapp_reply_keyboard()
    await update.message.reply_text(
        f"рџЋµ CXRNER MUSIC Mini App\n\n"
        f"<b>Р’Р°Р¶РЅРѕ:</b> Р·Р°РїСѓСЃРєР°Р№С‚Рµ Mini App РєРЅРѕРїРєРѕР№ РЅРёР¶Рµ.\n"
        f"РўР°Рє Telegram РєРѕСЂСЂРµРєС‚РЅРѕ РїРµСЂРµРґР°СЃС‚ РґР°РЅРЅС‹Рµ Р°РЅРєРµС‚С‹ Р±РѕС‚Сѓ.\n\n"
        f"<b>URL:</b> <code>{escape_html(WEBAPP_URL)}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )


async def _collect_webapp_chain_diag(context: ContextTypes.DEFAULT_TYPE) -> dict:
    """Collects runtime diagnostics for Mini App -> bot -> moderation chain."""
    info = {
        "moderation_chat_id": str(MODERATION_CHAT_ID),
        "chat_ok": False,
        "chat_title": "",
        "bot_member_status": "",
        "bot_is_admin": False,
        "errors": [],
    }
    try:
        chat = await context.bot.get_chat(MODERATION_CHAT_ID)
        info["chat_ok"] = True
        info["chat_title"] = getattr(chat, "title", "") or getattr(chat, "username", "") or ""
    except Exception as e:
        info["errors"].append(f"get_chat_failed: {e}")
        return info

    try:
        me = await context.bot.get_me()
        member = await context.bot.get_chat_member(MODERATION_CHAT_ID, me.id)
        status = str(getattr(member, "status", "") or "")
        info["bot_member_status"] = status
        info["bot_is_admin"] = status in {"administrator", "creator"}
    except Exception as e:
        info["errors"].append(f"get_chat_member_failed: {e}")

    return info


async def _send_webapp_diag_to_moderation(
    context: ContextTypes.DEFAULT_TYPE,
    raw_data: str,
    user_id: str,
    username: str,
) -> tuple[bool, str]:
    """Sends explicit test payload to moderation chat and returns result."""
    diag = await _collect_webapp_chain_diag(context)
    diag_lines = [
        "WEBAPP DIAGNOSTICS",
        f"web_app_data_received: yes",
        f"moderation_chat_id: {diag.get('moderation_chat_id')}",
        f"chat_ok: {diag.get('chat_ok')}",
        f"chat_title: {diag.get('chat_title') or '-'}",
        f"bot_member_status: {diag.get('bot_member_status') or '-'}",
        f"bot_is_admin: {diag.get('bot_is_admin')}",
    ]
    if diag.get("errors"):
        diag_lines.append(f"errors: {' | '.join(str(x) for x in diag['errors'])}")

    text = (
        "РўР•РЎРў РђРќРљР•РўРђ РџРћР›РЈР§Р•РќРђ:\n\n"
        f"{raw_data}\n\n"
        f"РћС‚: @{username or '-'}\n"
        f"ID: {user_id or '-'}\n\n"
        + "\n".join(diag_lines)
    )
    try:
        sent = await context.bot.send_message(
            chat_id=MODERATION_CHAT_ID,
            text=text,
            disable_web_page_preview=True,
        )
        return True, f"message_id={getattr(sent, 'message_id', '-')}"
    except Exception as e:
        return False, str(e)


async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles payload sent from Telegram WebApp via WebApp.sendData()."""
    if not update.message or not update.message.web_app_data:
        return

    raw_data = update.message.web_app_data.data or ""
    user = update.effective_user
    user_id = str(user.id) if user else ""
    username = user.username if user else ""

    print(f"WEBAPP DATA RECEIVED: {raw_data}", flush=True)

    raw_lower = clean(str(raw_data)).strip().lower()
    if raw_lower.startswith("test"):
        ok, details = await _send_webapp_diag_to_moderation(
            context=context,
            raw_data=raw_data,
            user_id=user_id,
            username=username or "",
        )
        if ok:
            print(f"[WEBAPP_DIAG] test payload forwarded to moderation: {details}", flush=True)
            await update.message.reply_text(
                "вњ… WEB_APP_DATA РїРѕР»СѓС‡РµРЅ.\n"
                "РўРµСЃС‚РѕРІР°СЏ Р°РЅРєРµС‚Р° РѕС‚РїСЂР°РІР»РµРЅР° РІ РіСЂСѓРїРїСѓ РјРѕРґРµСЂР°С†РёРё.\n"
                f"details: {details}"
            )
        else:
            print(f"[WEBAPP_DIAG] failed to forward test payload: {details}", flush=True)
            await update.message.reply_text(
                "вќЊ WEB_APP_DATA РїРѕР»СѓС‡РµРЅ, РЅРѕ РѕС‚РїСЂР°РІРєР° РІ РјРѕРґРµСЂР°С†РёСЋ РЅРµ СѓРґР°Р»Р°СЃСЊ.\n"
                f"error: {details}\n"
                f"chat_id: {MODERATION_CHAT_ID}"
            )
        return

    try:
        payload = json.loads(raw_data)
    except Exception as e:
        print(f"[WEBAPP_DIAG] invalid json payload user_id={user_id} error={e}", flush=True)
        ok, details = await _send_webapp_diag_to_moderation(
            context=context,
            raw_data=raw_data,
            user_id=user_id,
            username=username or "",
        )
        print(
            f"[WEBAPP_DIAG] non-json payload forward result: ok={ok} details={details} chat_id={MODERATION_CHAT_ID}",
            flush=True,
        )
        await update.message.reply_text(
            "вќЊ РќРµ СѓРґР°Р»РѕСЃСЊ СЂР°СЃРїРѕР·РЅР°С‚СЊ РґР°РЅРЅС‹Рµ Mini App (payload РЅРµ JSON).\n"
            "Р”РёР°РіРЅРѕСЃС‚РёС‡РµСЃРєРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ РѕС‚РїСЂР°РІР»РµРЅРѕ РІ РјРѕРґРµСЂР°С†РёСЋ."
        )
        return

    legacy_root_keys = {
        "artist_name", "track_title", "release_date", "telegram_contact",
        "type", "name", "nick", "fio", "date", "genre", "link", "tg"
    }
    action = clean(str(payload.get("action", ""))).strip()
    looks_like_submit_payload = isinstance(payload.get("form"), dict) or any(k in payload for k in legacy_root_keys)
    if action not in {"cabinet_activate", "webapp_release_submit", "submit_release"} and looks_like_submit_payload:
        action = "submit_release"

    raw_bytes = len(raw_data.encode("utf-8")) if isinstance(raw_data, str) else 0
    print(f"[WEBAPP] action={action or '-'} user_id={user_id or '-'} bytes={raw_bytes}", flush=True)

    if action == "cabinet_activate":
        if not user or not user_id:
            await update.message.reply_text("вќЊ РќРµ СѓРґР°Р»РѕСЃСЊ РѕРїСЂРµРґРµР»РёС‚СЊ Р°РєРєР°СѓРЅС‚ Telegram РґР»СЏ РїСЂРёРІСЏР·РєРё РєР°Р±РёРЅРµС‚Р°.")
            return
        cabinet_users[user_id] = {
            "approved": True,
            "activated_at": datetime.now().isoformat(),
            "username": user.username or "",
            "first_name": user.first_name or "",
        }
        save_cabinet_users(cabinet_users)
        await update.message.reply_text(
            "вњ… <b>Р›РёС‡РЅС‹Р№ РєР°Р±РёРЅРµС‚ Р°РєС‚РёРІРёСЂРѕРІР°РЅ</b>\n\n"
            "РўРµРїРµСЂСЊ РІ Mini App Р±СѓРґРµС‚ РґРѕСЃС‚СѓРїРµРЅ СЂР°Р·РґРµР» СЃ РІР°С€РёРјРё СЂРµР»РёР·Р°РјРё Рё СЃС‚Р°С‚СѓСЃР°РјРё.",
            parse_mode=ParseMode.HTML,
        )
        return

    if action not in {"webapp_release_submit", "submit_release"}:
        await update.message.reply_text("вњ… Р”Р°РЅРЅС‹Рµ Mini App РїРѕР»СѓС‡РµРЅС‹.")
        return

    if not user or not user_id:
        await update.message.reply_text("вќЊ РќРµ СѓРґР°Р»РѕСЃСЊ РѕРїСЂРµРґРµР»РёС‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ Telegram. РџРµСЂРµР·Р°РїСѓСЃС‚РёС‚Рµ Mini App.")
        return

    form = payload.get("form")
    if not isinstance(form, dict):
        # Fallback for cached legacy Mini App builds that send form fields at root level.
        if isinstance(payload, dict) and any(k in payload for k in legacy_root_keys):
            form = payload
        else:
            await update.message.reply_text("вќЊ РћС€РёР±РєР° РґР°РЅРЅС‹С… С„РѕСЂРјС‹. РћС‚РїСЂР°РІСЊС‚Рµ Р°РЅРєРµС‚Сѓ РµС‰С‘ СЂР°Р·.")
            return

    # Support legacy payload shape from old Mini App versions.
    legacy_form_detected = (
        not form.get("type")
        and any(
            form.get(k)
            for k in ("artist_name", "track_title", "release_date", "telegram_contact", "contact")
        )
    )
    if action == "submit_release" or legacy_form_detected:
        legacy_date = clean(str(form.get("release_date") or form.get("date") or "")).strip()
        if legacy_date and "-" in legacy_date:
            try:
                legacy_date = datetime.strptime(legacy_date, "%Y-%m-%d").strftime("%d.%m.%Y")
            except ValueError:
                pass

        legacy_type_raw = clean(str(form.get("release_type") or form.get("type") or "single")).strip().lower()
        legacy_type = "Р°Р»СЊР±РѕРј" if legacy_type_raw in {"Р°Р»СЊР±РѕРј", "album"} else "СЃРёРЅРіР»"
        legacy_has_lyrics = clean(str(form.get("has_lyrics") or form.get("lyrics") or "")).strip()
        legacy_mat = clean(str(form.get("mat") or "")).strip()

        form = {
            "type": legacy_type,
            "name": form.get("track_title") or form.get("name") or "",
            "subname": form.get("subname") or ".",
            "has_lyrics": legacy_has_lyrics or "РќРµС‚, СЌС‚Рѕ РёРЅСЃС‚СЂСѓРјРµРЅС‚Р°Р»",
            "nick": form.get("artist_name") or form.get("nick") or "",
            "fio": form.get("artist_name") or form.get("fio") or "",
            "date": legacy_date,
            "version": form.get("version") or "РћСЂРёРіРёРЅР°Р»",
            "genre": form.get("genre") or "",
            "link": form.get("link") or form.get("files_link") or form.get("audio_link") or ".",
            "yandex": form.get("yandex") or form.get("yandex_link") or ".",
            "mat": legacy_mat or "РќРµС‚",
            "promo": form.get("promo") or ".",
            "comment": form.get("comment") or ".",
            "tracklist": form.get("tracklist") or ".",
            "tg": form.get("telegram_contact") or form.get("contact") or form.get("tg") or "",
        }

    errors: list[str] = []

    release_type = _normalize_release_type(form.get("type", ""))
    if not release_type:
        errors.append("РЈРєР°Р¶РёС‚Рµ С‚РёРї СЂРµР»РёР·Р°: СЃРёРЅРіР» РёР»Рё Р°Р»СЊР±РѕРј.")

    name = clean(str(form.get("name", ""))).strip()
    if not name:
        errors.append("РџРѕР»Рµ В«РќР°Р·РІР°РЅРёРµ СЂРµР»РёР·Р°В» РѕР±СЏР·Р°С‚РµР»СЊРЅРѕ.")

    subname = _normalize_optional_text(form.get("subname"), ".")

    has_lyrics_raw = clean(str(form.get("has_lyrics", ""))).strip().lower()
    if has_lyrics_raw in {"РґР°", "yes", "y"}:
        has_lyrics = "Р”Р°"
    elif has_lyrics_raw in {"РЅРµС‚", "no", "n", "РЅРµС‚, СЌС‚Рѕ РёРЅСЃС‚СЂСѓРјРµРЅС‚Р°Р»", "РёРЅСЃС‚СЂСѓРјРµРЅС‚Р°Р»", "instrumental"}:
        has_lyrics = "РќРµС‚, СЌС‚Рѕ РёРЅСЃС‚СЂСѓРјРµРЅС‚Р°Р»"
    elif has_lyrics_raw:
        has_lyrics = clean(str(form.get("has_lyrics", ""))).strip()
    else:
        has_lyrics = ""
        errors.append("РЈРєР°Р¶РёС‚Рµ, РµСЃС‚СЊ Р»Рё СЃР»РѕРІР° РІ СЂРµР»РёР·Рµ.")

    nick = clean(str(form.get("nick", ""))).strip()
    if not nick:
        errors.append("РџРѕР»Рµ В«РќРёРє РёСЃРїРѕР»РЅРёС‚РµР»СЏВ» РѕР±СЏР·Р°С‚РµР»СЊРЅРѕ.")

    fio = clean(str(form.get("fio", ""))).strip()
    if not fio:
        errors.append("РџРѕР»Рµ В«Р¤РРћ РёСЃРїРѕР»РЅРёС‚РµР»СЏВ» РѕР±СЏР·Р°С‚РµР»СЊРЅРѕ.")

    date_text = clean(str(form.get("date", ""))).strip()
    if not date_text:
        errors.append("РЈРєР°Р¶РёС‚Рµ РґР°С‚Сѓ СЂРµР»РёР·Р° РІ С„РѕСЂРјР°С‚Рµ Р”Р”.РњРњ.Р“Р“Р“Р“.")
    else:
        try:
            date_obj = datetime.strptime(date_text, "%d.%m.%Y")
            min_days = 7 if release_type == "Р°Р»СЊР±РѕРј" else 3
            if date_obj < datetime.now() + timedelta(days=min_days):
                errors.append(f"Р”Р°С‚Р° СЂРµР»РёР·Р° РґРѕР»Р¶РЅР° Р±С‹С‚СЊ РјРёРЅРёРјСѓРј С‡РµСЂРµР· {min_days} РґРЅРµР№.")
        except ValueError:
            errors.append("РќРµРІРµСЂРЅС‹Р№ С„РѕСЂРјР°С‚ РґР°С‚С‹. РСЃРїРѕР»СЊР·СѓР№С‚Рµ Р”Р”.РњРњ.Р“Р“Р“Р“.")

    version = clean(str(form.get("version", ""))).strip()
    if not version or version == "-":
        version = "РћСЂРёРіРёРЅР°Р»"

    genre = clean(str(form.get("genre", ""))).strip()
    if not genre:
        errors.append("РџРѕР»Рµ В«Р–Р°РЅСЂВ» РѕР±СЏР·Р°С‚РµР»СЊРЅРѕ.")

    link = clean(str(form.get("link", ""))).strip()
    if not link:
        errors.append("Р”РѕР±Р°РІСЊС‚Рµ СЃСЃС‹Р»РєСѓ РЅР° С„Р°Р№Р»С‹.")
    elif not _looks_like_url(link):
        errors.append("РЎСЃС‹Р»РєР° РЅР° С„Р°Р№Р»С‹ РґРѕР»Р¶РЅР° РЅР°С‡РёРЅР°С‚СЊСЃСЏ СЃ http:// РёР»Рё https://.")

    yandex = clean(str(form.get("yandex", ""))).strip()
    if not yandex or yandex in {"-", "РЅРµС‚", "none"}:
        yandex = "."
    if yandex != "." and not _looks_like_url(yandex):
        errors.append("РЎСЃС‹Р»РєР° РЇРЅРґРµРєСЃ РњСѓР·С‹РєРё РґРѕР»Р¶РЅР° Р±С‹С‚СЊ РІР°Р»РёРґРЅС‹Рј URL РёР»Рё С‚РѕС‡РєРѕР№ В«.В».")

    mat_raw = clean(str(form.get("mat", ""))).strip().lower()
    if mat_raw in {"РґР°", "yes", "y"}:
        mat = "Р”Р°"
    elif mat_raw in {"РЅРµС‚", "no", "n"}:
        mat = "РќРµС‚"
    else:
        mat = ""
        errors.append("РЈРєР°Р¶РёС‚Рµ, РµСЃС‚СЊ Р»Рё РЅРµРЅРѕСЂРјР°С‚РёРІРЅР°СЏ Р»РµРєСЃРёРєР° (Р”Р°/РќРµС‚).")

    promo = _normalize_optional_text(form.get("promo"), ".")
    comment = _normalize_optional_text(form.get("comment"), ".")
    tracklist = _normalize_optional_text(form.get("tracklist"), ".")

    tg_contact = clean(str(form.get("tg", ""))).strip()
    if not tg_contact:
        errors.append("РЈРєР°Р¶РёС‚Рµ РєРѕРЅС‚Р°РєС‚ Telegram.")

    if release_type == "Р°Р»СЊР±РѕРј" and tracklist == ".":
        errors.append("Р”Р»СЏ Р°Р»СЊР±РѕРјР° Р·Р°РїРѕР»РЅРёС‚Рµ Tracklist.")

    if errors:
        print(f"[WEBAPP] validation_failed user_id={user_id} errors={errors}", flush=True)
        err_lines = "\n".join(f"вЂў {escape_html(item)}" for item in errors[:8])
        await update.message.reply_text(
            f"{WINTER_EMOJIS['cross']} <b>РђРЅРєРµС‚Р° Mini App РЅРµ РѕС‚РїСЂР°РІР»РµРЅР°</b>\n\n"
            f"{err_lines}\n\n"
            "РСЃРїСЂР°РІСЊС‚Рµ РїРѕР»СЏ Рё РѕС‚РїСЂР°РІСЊС‚Рµ С„РѕСЂРјСѓ РїРѕРІС‚РѕСЂРЅРѕ.",
            parse_mode=ParseMode.HTML,
        )
        return

    release_data = {
        "type": release_type,
        "name": name,
        "subname": subname,
        "has_lyrics": has_lyrics,
        "nick": nick,
        "fio": fio,
        "date": date_text,
        "version": version,
        "genre": genre,
        "link": link,
        "yandex": yandex,
        "mat": mat,
        "promo": promo,
        "comment": comment,
        "tracklist": tracklist,
        "tg": tg_contact,
        "source": "mini_app",
        "webapp_submitted_at": payload.get("submitted_at"),
    }

    if release_type != "Р°Р»СЊР±РѕРј":
        release_data.pop("tracklist", None)

    try:
        await _submit_release_to_moderation(context, user, user_id, release_data)
        print(f"[WEBAPP] submitted_to_moderation user_id={user_id} release={name}", flush=True)
    except Exception as e:
        print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё Р°РЅРєРµС‚С‹ РёР· Mini App: {e}")
        await update.message.reply_text(
            f"{WINTER_EMOJIS['cross']} РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ Р°РЅРєРµС‚Сѓ РІ РјРѕРґРµСЂР°С†РёСЋ. РџРѕРїСЂРѕР±СѓР№С‚Рµ РµС‰С‘ СЂР°Р·.",
            parse_mode=ParseMode.HTML,
        )
        return

    await update.message.reply_text(
        f"{WINTER_EMOJIS['check']} <b>РђРЅРєРµС‚Р° РѕС‚РїСЂР°РІР»РµРЅР° РІ РјРѕРґРµСЂР°С†РёСЋ</b>\n\n"
        "РЎС‚Р°С‚СѓСЃ Р±СѓРґРµС‚ РѕР±РЅРѕРІР»СЏС‚СЊСЃСЏ С‚Р°Рє Р¶Рµ, РєР°Рє Сѓ Р°РЅРєРµС‚С‹ РёР· Р±РѕС‚Р°.\n"
        "РџСЂРѕРІРµСЂРёС‚СЊ РјРѕР¶РЅРѕ РІ СЂР°Р·РґРµР»Рµ В«РњРѕРё СЂРµР»РёР·С‹В».",
        parse_mode=ParseMode.HTML,
    )

# === РљРћРњРђРќР”Рђ /help ===
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = f"""
{winter_header("РЎРџР РђР’РљРђ РџРћ РљРћРњРђРќР”РђРњ")}

{WINTER_EMOJIS['music']} <b>РћРЎРќРћР’РќР«Р• РљРћРњРђРќР”Р«:</b>
/start - Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ
/my - РњРѕРё СЂРµР»РёР·С‹ Рё СЃС‚Р°С‚РёСЃС‚РёРєР°
/search &lt;РЅР°Р·РІР°РЅРёРµ&gt; - РџРѕРёСЃРє СЂРµР»РёР·РѕРІ
/app - РћС‚РєСЂС‹С‚СЊ Mini App
/cancel - РћС‚РјРµРЅРёС‚СЊ С‚РµРєСѓС‰РµРµ РґРµР№СЃС‚РІРёРµ
/help - Р­С‚Р° СЃРїСЂР°РІРєР°

{WINTER_EMOJIS['notes']} <b>РљРђРљ РћРўРџР РђР’РРўР¬ Р Р•Р›РР—:</b>
1. РќР°Р¶РјРёС‚Рµ /start
2. РћС‚РєСЂРѕР№С‚Рµ СЂР°Р·РґРµР» "рџ“Ђ Р”РёСЃС‚СЂРёР±СѓС†РёСЏ"
3. РќР°Р¶РјРёС‚Рµ "Р—Р°РіСЂСѓР·РёС‚СЊ СЂРµР»РёР·"
4. Р’С‹Р±РµСЂРёС‚Рµ С‚РёРї (РЎРёРЅРіР» РёР»Рё РђР»СЊР±РѕРј)
5. Р—Р°РїРѕР»РЅРёС‚Рµ РІСЃРµ РїРѕР»СЏ
6. РџРѕРґС‚РІРµСЂРґРёС‚Рµ РѕС‚РїСЂР°РІРєСѓ

{WINTER_EMOJIS['waiting']} <b>РЎРўРђРўРЈРЎР« Р Р•Р›РР—РћР’:</b>
вЏі РћР¶РёРґР°РµС‚ - РЅР° РјРѕРґРµСЂР°С†РёРё
вњ… РћРґРѕР±СЂРµРЅРѕ - РіРѕС‚РѕРІ Рє РїСѓР±Р»РёРєР°С†РёРё
вќЊ РћС‚РєР»РѕРЅРµРЅРѕ - С‚СЂРµР±СѓРµС‚ РёСЃРїСЂР°РІР»РµРЅРёР№
рџ“ў РћРїСѓР±Р»РёРєРѕРІР°РЅРѕ - СѓР¶Рµ РІ РєР°РЅР°Р»Рµ

{WINTER_EMOJIS['sparkles']} <b>РќРЈР–РќРђ РџРћРњРћР©Р¬?</b>
РќР°РїРёС€РёС‚Рµ РІ С‡Р°С‚ Р°СЂС‚РёСЃС‚РѕРІ РёР»Рё РёСЃРїРѕР»СЊР·СѓР№С‚Рµ /start
"""
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ", callback_data='main')],
        [InlineKeyboardButton("рџ“Ђ Р”РёСЃС‚СЂРёР±СѓС†РёСЏ", callback_data='menu_distribution')],
        [InlineKeyboardButton("рџ§‘вЂЌрџ’» РљР°Р±РёРЅРµС‚", callback_data='menu_cabinet')]
    ])
    
    await update.message.reply_text(
        help_text,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

# === РљРћРњРђРќР”Рђ /cancel ===
async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    
    # РћС‡РёС‰Р°РµРј РґР°РЅРЅС‹Рµ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
    if user_id in user_data:
        del user_data[user_id]
    delete_draft_for_user(user_id)
    
    # РЎР±СЂР°СЃС‹РІР°РµРј СЃРѕСЃС‚РѕСЏРЅРёРµ
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ", "tree"), callback_data='main')]
    ])
    
    text = (
        f"{WINTER_EMOJIS['check']} <b>Р”РµР№СЃС‚РІРёРµ РѕС‚РјРµРЅРµРЅРѕ!</b>\n\n"
        f"Р’СЃРµ РЅРµСЃРѕС…СЂР°РЅРµРЅРЅС‹Рµ РґР°РЅРЅС‹Рµ СѓРґР°Р»РµРЅС‹.\n"
        f"РњРѕР¶РµС‚Рµ РЅР°С‡Р°С‚СЊ Р·Р°РЅРѕРІРѕ СЃ /start"
    )
    
    await update.message.reply_text(
        text,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )
    return ConversationHandler.END

# === РљРћРњРђРќР”Рђ /search ===
async def search_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    
    if not context.args:
        await update.message.reply_text(
            f"{WINTER_EMOJIS['warning']} <b>РСЃРїРѕР»СЊР·РѕРІР°РЅРёРµ:</b>\n"
            f"<code>/search РЅР°Р·РІР°РЅРёРµ СЂРµР»РёР·Р°</code>\n"
            f"<code>/search Р°СЂС‚РёСЃС‚</code>\n\n"
            f"РџСЂРёРјРµСЂ: <code>/search Tokyo Rain</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    search_query = ' '.join(context.args).lower()
    user_releases = db.get(user_id, [])
    
    if not user_releases:
        await update.message.reply_text(
            f"{WINTER_EMOJIS['notes']} <b>РЈ РІР°СЃ РїРѕРєР° РЅРµС‚ СЂРµР»РёР·РѕРІ.</b>\n\n"
            f"РСЃРїРѕР»СЊР·СѓР№С‚Рµ /start С‡С‚РѕР±С‹ РѕС‚РїСЂР°РІРёС‚СЊ РїРµСЂРІС‹Р№!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # РџРѕРёСЃРє РїРѕ РЅР°Р·РІР°РЅРёСЋ Рё Р°СЂС‚РёСЃС‚Сѓ
    found_releases = []
    for idx, release in enumerate(user_releases):
        name = release.get('name', '').lower()
        nick = release.get('nick', '').lower()
        
        if search_query in name or search_query in nick:
            found_releases.append((idx, release))
    
    if not found_releases:
        await update.message.reply_text(
            f"{WINTER_EMOJIS['cross']} <b>РќРёС‡РµРіРѕ РЅРµ РЅР°Р№РґРµРЅРѕ!</b>\n\n"
            f"РџРѕ Р·Р°РїСЂРѕСЃСѓ <b>\"{escape_html(search_query)}\"</b> СЂРµР»РёР·РѕРІ РЅРµ РЅР°Р№РґРµРЅРѕ.\n\n"
            f"РџРѕРїСЂРѕР±СѓР№С‚Рµ РґСЂСѓРіРѕР№ РїРѕРёСЃРєРѕРІС‹Р№ Р·Р°РїСЂРѕСЃ.",
            parse_mode=ParseMode.HTML
        )
        return
    
    text = f"{WINTER_EMOJIS['notes']} <b>РќРђР™Р”Р•РќРћ Р Р•Р›РР—РћР’: {len(found_releases)}</b>\n\n"
    
    status_emoji = {
        "pending": WINTER_EMOJIS['waiting'],
        "approved": WINTER_EMOJIS['check'],
        "rejected": WINTER_EMOJIS['cross'],
        "published": WINTER_EMOJIS['published']
    }
    
    for idx, release in found_releases[:10]:  # РћРіСЂР°РЅРёС‡РёРІР°РµРј 10 СЂРµР·СѓР»СЊС‚Р°С‚Р°РјРё
        status = release.get('status', 'pending')
        emoji = status_emoji.get(status, WINTER_EMOJIS['waiting'])
        status_text = {
            "pending": "РћР¶РёРґР°РµС‚",
            "approved": "РћРґРѕР±СЂРµРЅРѕ",
            "rejected": "РћС‚РєР»РѕРЅРµРЅРѕ",
            "published": "РћРїСѓР±Р»РёРєРѕРІР°РЅРѕ"
        }.get(status, "РћР¶РёРґР°РµС‚")
        
        link = f"\n<a href='{release.get('link_published', '')}'>РЎР»СѓС€Р°С‚СЊ</a>" if status == 'published' and release.get('link_published') else ""
        
        text += (
            f"<b>{escape_html(release.get('name', 'Р‘РµР· РЅР°Р·РІР°РЅРёСЏ'))}</b> {emoji}\n"
            f"<i>РђСЂС‚РёСЃС‚:</i> {escape_html(release.get('nick', 'вЂ”'))}\n"
            f"<i>РўРёРї:</i> {escape_html(release.get('type', 'вЂ”'))}\n"
            f"<i>Р”Р°С‚Р°:</i> {escape_html(release.get('date', 'вЂ”'))}\n"
            f"<i>РЎС‚Р°С‚СѓСЃ:</i> {escape_html(status_text)}{link}\n\n"
        )
    
    if len(found_releases) > 10:
        text += f"<i>... Рё РµС‰С‘ {len(found_releases) - 10} СЂРµР»РёР·РѕРІ</i>"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Р’СЃРµ РјРѕРё СЂРµР»РёР·С‹", "notes"), callback_data='my_releases')],
        [InlineKeyboardButton(winter_text("Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ", "tree"), callback_data='main')]
    ])
    
    await update.message.reply_text(
        text,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

# === РњРћР Р Р•Р›РР—Р« (/my) ===
async def my_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, page=0):
    # РџРѕРґРґРµСЂР¶РєР° РєР°Рє message, С‚Р°Рє Рё callback_query
    if update.message:
        message = update.message
        user_id = str(update.message.from_user.id)
        is_callback = False
    elif update.callback_query:
        message = update.callback_query.message
        user_id = str(update.callback_query.from_user.id)
        is_callback = True
    else:
        return
    
    releases = db.get(user_id, [])
    # Р¤РёР»СЊС‚СЂСѓРµРј СЂРµР»РёР·С‹, РїРѕРјРµС‡РµРЅРЅС‹Рµ РєР°Рє СѓРґР°Р»С‘РЅРЅС‹Рµ РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј
    visible_releases = [r for r in releases if not r.get('user_deleted', False)]
    
    total = len(visible_releases)
    
    if not visible_releases:
        on_upload = moderation = approved = rejected = needs_fix = 0
    else:
        on_upload = sum(1 for r in visible_releases if r.get('status') == STATUS_ON_UPLOAD)
        moderation = sum(1 for r in visible_releases if r.get('status') == STATUS_MODERATION)
        approved = sum(1 for r in visible_releases if r.get('status') == STATUS_APPROVED)
        rejected = sum(1 for r in visible_releases if r.get('status') == STATUS_REJECTED)
        needs_fix = sum(1 for r in visible_releases if r.get('status') == STATUS_NEEDS_FIX)
    
    # Р Р°СЃС‡РµС‚ РїСЂРѕС†РµРЅС‚РѕРІ
    approved_pct = (approved * 100 / total) if total > 0 else 0

    # РљСЂР°СЃРёРІС‹Р№ Р·Р°РіРѕР»РѕРІРѕРє СЃРѕ СЃС‚Р°С‚РёСЃС‚РёРєРѕР№ РІ СЃС‚РѕР»Р±РµС†
    header = (
        f"{WINTER_EMOJIS['headphones']} <b>РњРћР™ РљРђР‘РРќР•Рў</b> вЂў {total} СЂРµР»РёР·РѕРІ\n"
        f"вњ… РћРґРѕР±СЂРµРЅРѕ: {approved} ({approved_pct:.0f}%)\n"
        f"вЏі РќР° РѕС‚РіСЂСѓР·РєРµ: {on_upload}\n"
        f"рџ§  РќР° РјРѕРґРµСЂР°С†РёРё: {moderation}\n"
        f"вљ пёЏ РќР° РїСЂР°РІРєР°С…: {needs_fix}\n"
        f"вќЊ РћС‚РєР»РѕРЅРµРЅРѕ: {rejected}"
    )

    if not visible_releases:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("вћ• РћС‚РїСЂР°РІРёС‚СЊ СЂРµР»РёР·", callback_data='report')],
            [InlineKeyboardButton("в—Ђ Р“Р»Р°РІРЅРѕРµ РјРµРЅСЋ", callback_data='main')]
        ])
        await message.reply_text(
            f"{header}\n\n<i>Р РµР»РёР·РѕРІ РїРѕРєР° РЅРµС‚</i>\n\n"
            f"РЎРѕР·РґР°Р№С‚Рµ СЃРІРѕР№ РїРµСЂРІС‹Р№ СЂРµР»РёР·, РЅР°Р¶Р°РІ РєРЅРѕРїРєСѓ РЅРёР¶Рµ!",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        return

    # РџРѕРєР°Р·С‹РІР°РµРј РѕРґРЅСѓ РєР°СЂС‚РѕС‡РєСѓ РЅР° СЃС‚СЂР°РЅРёС†Рµ (РїР°РіРёРЅР°С†РёСЏ)
    page = max(0, min(page, total - 1))  # Р—Р°С‰РёС‚Р° РѕС‚ РІС‹С…РѕРґР° Р·Р° РіСЂР°РЅРёС†С‹
    rel = visible_releases[page]
    
    status = rel.get('status', STATUS_ON_UPLOAD)
    status_emoji = {
        STATUS_ON_UPLOAD: "вЏі",
        STATUS_MODERATION: "рџ§ ",
        STATUS_APPROVED: "вњ…",
        STATUS_REJECTED: "вќЊ",
        STATUS_NEEDS_FIX: "вљ пёЏ",
    }
    
    status_names = {
        STATUS_ON_UPLOAD: 'РќР° РѕС‚РіСЂСѓР·РєРµ',
        STATUS_MODERATION: 'РќР° РјРѕРґРµСЂР°С†РёРё',
        STATUS_APPROVED: 'РћРґРѕР±СЂРµРЅРѕ вњ“',
        STATUS_REJECTED: 'РћС‚РєР»РѕРЅРµРЅРѕ',
        STATUS_NEEDS_FIX: 'РќР° РїСЂР°РІРєР°С…',
    }
    
    emoji = status_emoji.get(status, "вЏі")
    status_text = status_names.get(status, '?')
    
    # РљР°СЂС‚РѕС‡РєР° СЂРµР»РёР·Р°
    rel_name = escape_html(rel.get('name', 'Р РµР»РёР·'))
    rel_type = escape_html(rel.get('type', 'Р РµР»РёР·'))
    
    text = header + "\n\n"
    text += f"<b>рџЋµ {rel_name}</b>\n"
    text += f"рџ“ќ РўРёРї: <i>{rel_type}</i>\n"
    
    if rel.get('subname') and rel.get('subname') != '.':
        text += f"рџЋ™пёЏ Р’РµСЂСЃРёСЏ: <i>{escape_html(rel.get('subname'))}</i>\n"
    
    text += f"рџ“… Р”Р°С‚Р°: <i>{escape_html(rel.get('date', 'вЂ”'))}</i>\n"
    text += f"рџ‘¤ РђСЂС‚РёСЃС‚: <i>{escape_html(rel.get('nick', 'вЂ”'))}</i>\n"
    text += f"рџЏ·пёЏ Р–Р°РЅСЂ: <i>{escape_html(rel.get('genre', 'вЂ”'))}</i>\n"
    
    # UPC РєРѕРґ
    upc = rel.get('upc', '')
    if upc and upc != '.':
        text += f"рџ“¦ UPC: <i>{escape_html(upc)}</i>\n"
    else:
        text += f"рџ“¦ UPC: <i>вЂ”</i>\n"
    
    text += "\n"
    
    text += f"<b>рџ“Љ РЎС‚Р°С‚СѓСЃ:</b> {emoji} {status_text}\n"
    
    # Р•СЃР»Рё РѕС‚РєР»РѕРЅРµРЅРѕ - РїРѕРєР°Р·С‹РІР°РµРј РїСЂРёС‡РёРЅСѓ
    if status == STATUS_REJECTED and rel.get('reject_reason'):
        reason = escape_html(rel.get('reject_reason'))
        text += f"\nвќЊ <b>РџСЂРёС‡РёРЅР°:</b>\n<i>{reason}</i>\n"
    
    # Р•СЃР»Рё РЅР° РїСЂР°РІРєР°С… - РїРѕРєР°Р·С‹РІР°РµРј РєРѕРјРјРµРЅС‚Р°СЂРёР№
    if status == STATUS_NEEDS_FIX and rel.get('moderator_comment'):
        comment = escape_html(rel.get('moderator_comment'))
        text += f"\nрџ’¬ <b>РљРѕРјРјРµРЅС‚Р°СЂРёР№ РјРѕРґРµСЂР°С‚РѕСЂР°:</b>\n<i>{comment}</i>\n"
    
    text += f"\n<b>РљР°СЂС‚РѕС‡РєР° {page + 1} РёР· {total}</b>"
    
    # РљРЅРѕРїРєРё РЅР°РІРёРіР°С†РёРё Рё РґРµР№СЃС‚РІРёСЏ
    keyboard_buttons = []
    
    # РљРЅРѕРїРєРё РЅР°РІРёРіР°С†РёРё
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("в¬…пёЏ РџСЂРµРґ.", callback_data=f"card_{page - 1}"))
    nav_buttons.append(InlineKeyboardButton(f"({page + 1}/{total})", callback_data="noop"))
    if page < total - 1:
        nav_buttons.append(InlineKeyboardButton("Р”Р°Р»РµРµ вћЎпёЏ", callback_data=f"card_{page + 1}"))
    keyboard_buttons.append(nav_buttons)
    
    # РљРЅРѕРїРєРё РґРµР№СЃС‚РІРёР№
    original_idx = releases.index(rel)
    rel_id = f"{user_id}_{original_idx}"
    keyboard_buttons.append([
        InlineKeyboardButton("рџ“„ Р”РµС‚Р°Р»Рё", callback_data=f"release_details_{rel_id}"),
        InlineKeyboardButton("рџ—‘пёЏ РЈРґР°Р»РёС‚СЊ", callback_data=f"delete_release_{rel_id}")
    ])
    
    # РљРЅРѕРїРєРё РјРµРЅСЋ
    keyboard_buttons.append([
        InlineKeyboardButton("вћ• РќРѕРІС‹Р№", callback_data='report'),
        InlineKeyboardButton("в—Ђ РњРµРЅСЋ", callback_data='main')
    ])
    
    keyboard = InlineKeyboardMarkup(keyboard_buttons)
    
    if is_callback:
        await safe_edit(update.callback_query, text, reply_markup=keyboard)
    else:
        await message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# === Р РђРЎРЁРР Р•РќРќРђРЇ РђР”РњРРќ-РџРђРќР•Р›Р¬ (/admin) ===
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # РџРѕРґРґРµСЂР¶РєР° РєР°Рє message, С‚Р°Рє Рё callback_query
    if update.message:
        user_id = update.message.from_user.id
        message_target = update.message
        send_method = update.message.reply_text
    elif update.callback_query:
        user_id = update.callback_query.from_user.id
        message_target = update.callback_query.message
        send_method = lambda text, **kwargs: safe_edit(update.callback_query, text, **kwargs)
    else:
        return
    
    if not is_admin(user_id):
        if update.message:
            await update.message.reply_text("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ.")
        elif update.callback_query:
            await update.callback_query.answer("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ", show_alert=True)
        return

    # РЎС‚Р°С‚РёСЃС‚РёРєР°
    total_users = len(db)
    total_releases = sum(len(v) for v in db.values())
    pending = sum(1 for u in db.values() for r in u if r.get('status', 'pending') == 'pending')
    approved = sum(1 for u in db.values() for r in u if r.get('status') == 'approved')
    rejected = sum(1 for u in db.values() for r in u if r.get('status') == 'rejected')
    published = sum(1 for u in db.values() for r in u if r.get('status') == 'published')
    
    # РЎС‚Р°С‚РёСЃС‚РёРєР° Р·Р° РїРѕСЃР»РµРґРЅРёРµ 7 РґРЅРµР№
    week_ago = datetime.now() - timedelta(days=7)
    recent_releases = 0
    for user_releases in db.values():
        for release in user_releases:
            if 'submission_time' in release:
                try:
                    submit_time = datetime.fromisoformat(release['submission_time'])
                    if submit_time > week_ago:
                        recent_releases += 1
                except:
                    pass

    text = (
        f"{winter_header('РђР”РњРРќ-РџРђРќР•Р›Р¬')}\n\n"
        f"{WINTER_EMOJIS['stats']} <b>РћР‘Р©РђРЇ РЎРўРђРўРРЎРўРРљРђ:</b>\n"
        f"{WINTER_EMOJIS['users']} РџРѕР»СЊР·РѕРІР°С‚РµР»РµР№: <b>{total_users}</b>\n"
        f"{WINTER_EMOJIS['notes']} Р’СЃРµРіРѕ СЂРµР»РёР·РѕРІ: <b>{total_releases}</b>\n"
        f"{WINTER_EMOJIS['waiting']} РћР¶РёРґР°РµС‚: <b>{pending}</b>\n"
        f"{WINTER_EMOJIS['check']} РћРґРѕР±СЂРµРЅРѕ: <b>{approved}</b>\n"
        f"{WINTER_EMOJIS['cross']} РћС‚РєР»РѕРЅРµРЅРѕ: <b>{rejected}</b>\n"
        f"{WINTER_EMOJIS['published']} РћРїСѓР±Р»РёРєРѕРІР°РЅРѕ: <b>{published}</b>\n"
        f"{WINTER_EMOJIS['calendar']} Р—Р° РЅРµРґРµР»СЋ: <b>{recent_releases}</b>\n\n"
        
        f"{WINTER_EMOJIS['settings']} <b>РЈРџР РђР’Р›Р•РќРР•:</b>\n"
        "/backup - рџ“¦ Р‘Р°Р·Р° РґР°РЅРЅС‹С… СЂРµР»РёР·РѕРІ\n"
        "/moderation_backup - рџ—‚пёЏ РђСЂС…РёРІ РјРѕРґРµСЂР°С†РёРё\n"
        "/stats - рџ“Љ РџРѕРґСЂРѕР±РЅР°СЏ СЃС‚Р°С‚РёСЃС‚РёРєР°\n"
        "/broadcast - рџ“ў Р Р°СЃСЃС‹Р»РєР° РїРѕР»СЊР·РѕРІР°С‚РµР»СЏРј\n"
        "/cleanup - рџ§№ РћС‡РёСЃС‚РєР° СЃС‚Р°СЂС‹С… РґР°РЅРЅС‹С…\n"
        "/cleanbase - рџ’Ј РЈР”РђР›РРўР¬ Р’РЎР• Р Р•Р›РР—Р«\n\n"
        
        f"{WINTER_EMOJIS['warning']} <b>Р‘Р«РЎРўР Р«Р• Р”Р•Р™РЎРўР’РРЇ:</b>"
    )
    
    keyboard = InlineKeyboardMarkup(
        [
        [
            InlineKeyboardButton(winter_text("Р‘СЌРєР°Рї Р‘Р”", "gift"), callback_data='get_db'),
            InlineKeyboardButton(winter_text("РђСЂС…РёРІ РјРѕРґ.", "snowflake"), callback_data='get_moderation_db')
        ],
        [
            InlineKeyboardButton(winter_text("РЎС‚Р°С‚РёСЃС‚РёРєР°", "stats"), callback_data='admin_stats'),
            InlineKeyboardButton(winter_text("РћР¶РёРґР°СЋС‚", "waiting"), callback_data='pending_list')
        ],
        [
            InlineKeyboardButton(winter_text("РћС‡РёСЃС‚РєР°", "refresh"), callback_data='cleanup_db'),
            InlineKeyboardButton(winter_text("Р Р°СЃСЃС‹Р»РєР°", "published"), callback_data='broadcast_menu')
        ],
        [
            InlineKeyboardButton(winter_text("Р’СЃРµ СЂРµР»РёР·С‹", "list"), callback_data='all_releases'),
            InlineKeyboardButton(winter_text("РЈР”РђР›РРўР¬ Р’РЎРЃ", "warning"), callback_data='confirm_cleanbase')
        ]
        ]
    )
    
    if update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    elif update.callback_query:
        await safe_edit(update.callback_query, text, reply_markup=keyboard)

# === РЎРўРђРўРРЎРўРРљРђ Р”Р›РЇ РђР”РњРРќРђ ===
def _all_releases_flat():
    all_rel = []
    for uid, rels in db.items():
        for idx, rel in enumerate(rels):
            all_rel.append((uid, idx, rel))
    # РїРѕ РІСЂРµРјРµРЅРё РѕС‚РїСЂР°РІРєРё
    all_rel.sort(key=lambda x: x[2].get("submission_time", ""), reverse=True)
    return all_rel


def _render_admin_stats_page(page: int, per_page: int = 10):
    all_rel = _all_releases_flat()
    total_users = len(db)
    total_releases = len(all_rel)

    status_stats = {"pending": 0, "approved": 0, "rejected": 0, "published": 0}
    type_stats = {"СЃРёРЅРіР»": 0, "Р°Р»СЊР±РѕРј": 0}
    for _, __, r in all_rel:
        status_stats[r.get("status", "pending")] = status_stats.get(r.get("status", "pending"), 0) + 1
        type_stats[r.get("type", "СЃРёРЅРіР»")] = type_stats.get(r.get("type", "СЃРёРЅРіР»"), 0) + 1

    active_users = sum(1 for rels in db.values() if len(rels) > 0)

    pages = max(1, (total_releases + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = min(total_releases, start + per_page)

    text = (
        f"{winter_header('Р”Р•РўРђР›Р¬РќРђРЇ РЎРўРђРўРРЎРўРРљРђ')}\n\n"
        f"{WINTER_EMOJIS['users']} <b>РџРћР›Р¬Р—РћР’РђРўР•Р›Р:</b>\n"
        f"вЂў Р’СЃРµРіРѕ: <b>{total_users}</b>\n"
        f"вЂў РђРєС‚РёРІРЅС‹С…: <b>{active_users}</b>\n\n"
        f"{WINTER_EMOJIS['notes']} <b>Р Р•Р›РР—Р«:</b>\n"
        f"вЂў Р’СЃРµРіРѕ: <b>{total_releases}</b>\n"
        f"вЂў РЎРёРЅРіР»РѕРІ: <b>{type_stats.get('СЃРёРЅРіР»', 0)}</b>\n"
        f"вЂў РђР»СЊР±РѕРјРѕРІ: <b>{type_stats.get('Р°Р»СЊР±РѕРј', 0)}</b>\n\n"
        f"{WINTER_EMOJIS['stats']} <b>РЎРўРђРўРЈРЎР«:</b>\n"
        f"вЂў РћР¶РёРґР°РµС‚: <b>{status_stats.get('pending', 0)}</b>\n"
        f"вЂў РћРґРѕР±СЂРµРЅРѕ: <b>{status_stats.get('approved', 0)}</b>\n"
        f"вЂў РћС‚РєР»РѕРЅРµРЅРѕ: <b>{status_stats.get('rejected', 0)}</b>\n"
        f"вЂў РћРїСѓР±Р»РёРєРѕРІР°РЅРѕ: <b>{status_stats.get('published', 0)}</b>\n\n"
        f"{WINTER_EMOJIS['list']} <b>Р’РЎР• Р Р•Р›РР—Р« (СЃС‚СЂ. {page+1}/{pages}):</b>\n"
    )

    status_emoji = {
        "pending": WINTER_EMOJIS["waiting"],
        "approved": WINTER_EMOJIS["check"],
        "rejected": WINTER_EMOJIS["cross"],
        "published": WINTER_EMOJIS["published"],
    }

    for i, (uid, idx, r) in enumerate(all_rel[start:end], start=start + 1):
        st = r.get("status", "pending")
        text += (
            f"\n<b>{i}. {escape_html(r.get('name', 'Р‘РµР· РЅР°Р·РІР°РЅРёСЏ'))}</b> {escape_html(status_emoji.get(st, WINTER_EMOJIS['waiting']))}\n"
            f"<i>РўРёРї:</i> {escape_html(r.get('type', 'вЂ”'))}\n"
            f"<i>РђСЂС‚РёСЃС‚:</i> {escape_html(r.get('nick', 'вЂ”'))}\n"
            f"<i>Р”Р°С‚Р°:</i> {escape_html(r.get('date', 'вЂ”'))}\n"
            f"<i>ID:</i> <code>{uid}</code>\n"
        )

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("в¬…пёЏ", callback_data=f"admin_stats_page_{page-1}"))
    nav.append(InlineKeyboardButton("рџ”™ Р’ Р°РґРјРёРЅ", callback_data="admin_back"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("вћЎпёЏ", callback_data=f"admin_stats_page_{page+1}"))

    keyboard = InlineKeyboardMarkup([nav])
    return text, keyboard


async def admin_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """РљРѕРјР°РЅРґР° /statss - СЃС‚Р°С‚РёСЃС‚РёРєР° Р·Р° РІС‹Р±СЂР°РЅРЅС‹Р№ РїРµСЂРёРѕРґ (РґР»СЏ Р°РґРјРёРЅРѕРІ)."""
    user_id = update.message.from_user.id if update.message else None
    
    if not user_id:
        return
    
    # РџСЂРѕРІРµСЂСЏРµРј Р°РґРјРёРЅР°
    if not is_admin(user_id):
        await update.message.reply_text("вќЊ Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ. РљРѕРјР°РЅРґР° /statss РґРѕСЃС‚СѓРїРЅР° С‚РѕР»СЊРєРѕ РґР»СЏ Р°РґРјРёРЅРёСЃС‚СЂР°С‚РѕСЂРѕРІ.")
        return

    # РџРѕРєР°Р·С‹РІР°РµРј РІС‹Р±РѕСЂ РїРµСЂРёРѕРґР°
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("рџ“… РќРµРґРµР»СЏ", callback_data='stats_period_week')],
        [InlineKeyboardButton("рџ“… РњРµСЃСЏС†", callback_data='stats_period_month')],
        [InlineKeyboardButton("рџ“… Р’СЃС‘ РІСЂРµРјСЏ", callback_data='stats_period_all')],
    ])
    await update.message.reply_text("рџ“Љ Р’С‹Р±РµСЂРёС‚Рµ РїРµСЂРёРѕРґ РґР»СЏ СЃС‚Р°С‚РёСЃС‚РёРєРё:", reply_markup=keyboard)

# === РЎРџРРЎРћРљ Р’РЎР•РҐ Р Р•Р›РР—РћР’ ===
async def all_releases_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ", show_alert=True)
        return

    all_releases = []
    for user_id, releases in db.items():
        for idx, release in enumerate(releases):
            all_releases.append((user_id, idx, release))
    
    if not all_releases:
        text = f"{WINTER_EMOJIS['check']} <b>РќРµС‚ СЂРµР»РёР·РѕРІ!</b>"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(winter_text("РќР°Р·Р°Рґ", "tree"), callback_data='admin_back')]
        ])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return
    
    # РЎРѕСЂС‚РёСЂСѓРµРј РїРѕ РІСЂРµРјРµРЅРё РѕС‚РїСЂР°РІРєРё
    all_releases.sort(key=lambda x: x[2].get('submission_time', ''), reverse=True)
    
    text = f"{winter_header('Р’РЎР• Р Р•Р›РР—Р«')}\n\n"
    for i, (user_id, idx, release) in enumerate(all_releases[:15], 1):  # РћРіСЂР°РЅРёС‡РёРІР°РµРј 15 Р·Р°РїРёСЃСЏРјРё
        status_emoji = {
            'pending': WINTER_EMOJIS['waiting'],
            'approved': WINTER_EMOJIS['check'],
            'rejected': WINTER_EMOJIS['cross'],
            'published': WINTER_EMOJIS['published']
        }
        status = release.get('status', 'pending')
        emoji = status_emoji.get(status, WINTER_EMOJIS['waiting'])
        
        # РџРѕРјРµС‚РєР° СѓРґР°Р»РµРЅРЅРѕРіРѕ СЂРµР»РёР·Р°
        deleted_mark = " рџ—‘пёЏ <i>(СѓРґР°Р»РµРЅ Р°СЂС‚РёСЃС‚РѕРј)</i>" if release.get('user_deleted') else ""
        
        text += (
            f"<b>{i}. {escape_html(release.get('name', 'Р‘РµР· РЅР°Р·РІР°РЅРёСЏ'))}</b> {emoji}{deleted_mark}\n"
            f"РўРёРї: {escape_html(release.get('type', 'вЂ”'))}\n"
            f"РђСЂС‚РёСЃС‚: {escape_html(release.get('nick', 'вЂ”'))}\n"
            f"РЎС‚Р°С‚СѓСЃ: {escape_html(status)}\n"
            f"ID: <code>{user_id}</code>\n\n"
        )
    
    if len(all_releases) > 15:
        text += f"<b>... Рё РµС‰С‘ {len(all_releases) - 15} СЂРµР»РёР·РѕРІ</b>"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("РќР°Р·Р°Рґ", "tree"), callback_data='admin_back')]
    ])
    
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === РЎРџРРЎРћРљ РћР–РР”РђР®Р©РРҐ Р Р•Р›РР—РћР’ ===
async def pending_releases_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ", show_alert=True)
        return

    pending_list = []
    for user_id, releases in db.items():
        for idx, release in enumerate(releases):
            if release.get('status', 'pending') == 'pending':
                pending_list.append((user_id, idx, release))
    
    if not pending_list:
        text = f"{WINTER_EMOJIS['check']} <b>РќРµС‚ РѕР¶РёРґР°СЋС‰РёС… СЂРµР»РёР·РѕРІ!</b>"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(winter_text("РќР°Р·Р°Рґ", "tree"), callback_data='admin_back')]
        ])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return
    
    text = f"{winter_header('РћР–РР”РђР®Р©РР• Р Р•Р›РР—Р«')}\n\n"
    for i, (user_id, idx, release) in enumerate(pending_list[:10], 1):  # РћРіСЂР°РЅРёС‡РёРІР°РµРј 10 Р·Р°РїРёСЃСЏРјРё
        text += (
            f"<b>{i}. {escape_html(release.get('name', 'Р‘РµР· РЅР°Р·РІР°РЅРёСЏ'))}</b>\n"
            f"РўРёРї: {escape_html(release.get('type', 'вЂ”'))}\n"
            f"РђСЂС‚РёСЃС‚: {escape_html(release.get('nick', 'вЂ”'))}\n"
            f"Р”Р°С‚Р°: {escape_html(release.get('date', 'вЂ”'))}\n"
            f"ID: <code>{user_id}</code>\n\n"
        )
    
    if len(pending_list) > 10:
        text += f"<b>... Рё РµС‰С‘ {len(pending_list) - 10} СЂРµР»РёР·РѕРІ</b>"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("РќР°Р·Р°Рґ", "tree"), callback_data='admin_back')]
    ])
    
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === РћР§РРЎРўРљРђ Р‘РђР—Р« Р”РђРќРќР«РҐ ===
async def cleanup_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # РџРѕРґРґРµСЂР¶РєР° РєР°Рє message, С‚Р°Рє Рё callback_query
    if update.message:
        user_id = update.message.from_user.id
        query = None
    elif update.callback_query:
        user_id = update.callback_query.from_user.id
        query = update.callback_query
    else:
        return
    
    if not is_admin(user_id):
        if update.message:
            await update.message.reply_text("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ.")
        elif query:
            await query.answer("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ", show_alert=True)
        return

    # РЈРґР°Р»СЏРµРј РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№ Р±РµР· СЂРµР»РёР·РѕРІ
    users_before = len(db)
    empty_users = [uid for uid, releases in db.items() if not releases]
    for uid in empty_users:
        del db[uid]
    
    users_after = len(db)
    users_removed = users_before - users_after
    
    # РЎРѕС…СЂР°РЅСЏРµРј РёР·РјРµРЅРµРЅРёСЏ
    save_db(db)
    
    text = (
        f"{WINTER_EMOJIS['refresh']} <b>РћР§РРЎРўРљРђ Р—РђР’Р•Р РЁР•РќРђ!</b>\n\n"
        f"РЈРґР°Р»РµРЅРѕ РїСѓСЃС‚С‹С… РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№: <b>{users_removed}</b>\n"
        f"РўРµРєСѓС‰РµРµ РєРѕР»РёС‡РµСЃС‚РІРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№: <b>{users_after}</b>"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("РќР°Р·Р°Рґ", "tree"), callback_data='admin_back')]
    ])
    
    if update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    elif query:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === РЈР”РђР›Р•РќРР• Р’РЎР•РҐ Р Р•Р›РР—РћР’ ===
async def cleanbase_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # РџРѕРґРґРµСЂР¶РєР° РєР°Рє message, С‚Р°Рє Рё callback_query
    if update.message:
        user_id = update.message.from_user.id
    elif update.callback_query:
        user_id = update.callback_query.from_user.id
        query = update.callback_query
    else:
        return
    
    if not is_admin(user_id):
        if update.message:
            await update.message.reply_text("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ.")
        elif update.callback_query:
            await update.callback_query.answer("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(winter_text("Р”Рђ, РЈР”РђР›РРўР¬ Р’РЎРЃ", "cross"), callback_data='cleanbase_confirm'),
            InlineKeyboardButton(winter_text("РћС‚РјРµРЅР°", "check"), callback_data='admin_back')
        ]
    ])
    
    text = (
        f"{WINTER_EMOJIS['warning']} <b>Р’РќРРњРђРќРР•! РћРџРђРЎРќРђРЇ РљРћРњРђРќР”Рђ!</b>\n\n"
        f"Р’С‹ СЃРѕР±РёСЂР°РµС‚РµСЃСЊ <b>РџРћР›РќРћРЎРўР¬Р® РћР§РРЎРўРРўР¬</b> Р±Р°Р·Сѓ РґР°РЅРЅС‹С… РІСЃРµС… СЂРµР»РёР·РѕРІ!\n\n"
        f"<b>Р­С‚Рѕ РґРµР№СЃС‚РІРёРµ РЅРµР»СЊР·СЏ РѕС‚РјРµРЅРёС‚СЊ!</b>\n"
        f"Р’СЃРµ РґР°РЅРЅС‹Рµ Р±СѓРґСѓС‚ <b>Р‘Р•Р—Р’РћР—Р’Р РђРўРќРћ РЈРўР•Р РЇРќР«!</b>\n\n"
        f"Р’С‹ СѓРІРµСЂРµРЅС‹, С‡С‚Рѕ С…РѕС‚РёС‚Рµ РїСЂРѕРґРѕР»Р¶РёС‚СЊ?"
    )
    
    if update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    elif update.callback_query:
        await safe_edit(update.callback_query, text, reply_markup=keyboard)

async def cleanbase_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ", show_alert=True)
        return

    # РџРѕР»РЅРѕСЃС‚СЊСЋ РѕС‡РёС‰Р°РµРј Р±Р°Р·Сѓ РґР°РЅРЅС‹С…
    global db
    db = {}
    save_db(db)
    
    text = (
        f"{WINTER_EMOJIS['check']} <b>Р‘РђР—Рђ Р”РђРќРќР«РҐ РџРћР›РќРћРЎРўР¬Р® РћР§РР©Р•РќРђ!</b>\n\n"
        f"Р’СЃРµ СЂРµР»РёР·С‹ Р±С‹Р»Рё <b>СѓРґР°Р»РµРЅС‹</b>!\n"
        f"РљРѕР»РёС‡РµСЃС‚РІРѕ РїРѕР»СЊР·РѕРІР°С‚РµР»РµР№: <b>0</b>\n"
        f"РљРѕР»РёС‡РµСЃС‚РІРѕ СЂРµР»РёР·РѕРІ: <b>0</b>"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Р’ Р°РґРјРёРЅ-РїР°РЅРµР»СЊ", "settings"), callback_data='admin_back')]
    ])
    
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === РњР•РќР® Р РђРЎРЎР«Р›РљР ===
async def broadcast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ", show_alert=True)
        return

    text = (
        f"{winter_header('Р РђРЎРЎР«Р›РљРђ')}\n\n"
        f"{WINTER_EMOJIS['warning']} <b>Р’РќРРњРђРќРР•:</b> Р Р°СЃСЃС‹Р»РєР° Р±СѓРґРµС‚ РѕС‚РїСЂР°РІР»РµРЅР° <b>Р’РЎР•Рњ</b> РїРѕР»СЊР·РѕРІР°С‚РµР»СЏРј Р±РѕС‚Р°!\n\n"
        f"РСЃРїРѕР»СЊР·СѓР№С‚Рµ РєРѕРјР°РЅРґСѓ:\n"
        f"<code>/broadcast РІР°С€ С‚РµРєСЃС‚ СЃРѕРѕР±С‰РµРЅРёСЏ</code>\n\n"
        f"РР»Рё РѕС‚РїСЂР°РІСЊС‚Рµ СЃРѕРѕР±С‰РµРЅРёРµ РѕС‚РІРµС‚РѕРј РЅР° СЌС‚Рѕ СЃРѕРѕР±С‰РµРЅРёРµ РґР»СЏ СЂР°СЃСЃС‹Р»РєРё."
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("РќР°Р·Р°Рґ", "tree"), callback_data='admin_back')]
    ])
    
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === Р РђРЎРЎР«Р›РљРђ ===
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ.")
        return

    if not context.args:
        await update.message.reply_text(
            f"{WINTER_EMOJIS['warning']} РСЃРїРѕР»СЊР·РѕРІР°РЅРёРµ: <code>/broadcast РІР°С€ С‚РµРєСЃС‚</code>",
            parse_mode=ParseMode.HTML
        )
        return

    message_text = ' '.join(context.args)
    broadcast_text = (
        f"{WINTER_EMOJIS['published']} <b>Р’РђР–РќРћР• РћР‘РЄРЇР’Р›Р•РќРР•</b> {WINTER_EMOJIS['published']}\n\n"
        f"{escape_html(message_text)}\n\n"
        f"<i>РЎ СѓРІР°Р¶РµРЅРёРµРј, РєРѕРјР°РЅРґР° CXRNER MUSIC</i> {WINTER_EMOJIS['snowflake']}"
    )

    # РћС‚РїСЂР°РІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ
    sent_count = 0
    error_count = 0
    failed_ids = []

    progress_msg = await update.message.reply_text(
        f"{WINTER_EMOJIS['waiting']} <b>РќР°С‡РёРЅР°СЋ СЂР°СЃСЃС‹Р»РєСѓ...</b>"
    )

    recipients = list(db.keys())
    for uid in recipients:
        # РџС‹С‚Р°РµРјСЃСЏ Р±РµР·РѕРїР°СЃРЅРѕ РїСЂРёРІРµСЃС‚Рё uid Рє int
        try:
            target_id = int(uid)
        except Exception as e:
            error_count += 1
            failed_ids.append(str(uid))
            print(f"РћС€РёР±РєР°: РЅРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ user_id РІ Р±Р°Р·Рµ: {uid} ({e})")
            continue

        # РџРѕРїСЂРѕР±СѓРµРј РѕС‚РїСЂР°РІРёС‚СЊ СЃ РЅРµСЃРєРѕР»СЊРєРёРјРё РїРѕРїС‹С‚РєР°РјРё
        sent = False
        for attempt in range(3):
            try:
                await context.bot.send_message(
                    target_id,
                    broadcast_text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                sent_count += 1
                sent = True
                break
            except Forbidden as e:
                # РџРѕР»СЊР·РѕРІР°С‚РµР»СЊ Р·Р°Р±Р»РѕРєРёСЂРѕРІР°Р» Р±РѕС‚Р° РёР»Рё СѓРґР°Р»РёР» Р°РєРєР°СѓРЅС‚ вЂ” С„РёРєСЃРёСЂСѓРµРј Рё РёРґС‘Рј РґР°Р»СЊС€Рµ
                error_count += 1
                failed_ids.append(str(uid))
                print(f"Forbidden РїСЂРё РѕС‚РїСЂР°РІРєРµ {uid}: {e}")
                break
            except BadRequest as e:
                # Р§Р°СЃС‚Р°СЏ РїСЂРёС‡РёРЅР° вЂ” РїСЂРѕР±Р»РµРјС‹ СЃ РїР°СЂСЃРёРЅРіРѕРј СЃСѓС‰РЅРѕСЃС‚РµР№. РћС‚РїСЂР°РІРёРј plain text.
                if "can't parse entities" in str(e).lower():
                    try:
                        await context.bot.send_message(target_id, _strip_html(broadcast_text), disable_web_page_preview=True)
                        sent_count += 1
                        sent = True
                        break
                    except Exception as e2:
                        error_count += 1
                        failed_ids.append(str(uid))
                        print(f"BadRequest(2) РїСЂРё РѕС‚РїСЂР°РІРєРµ {uid}: {e2}")
                        break
                else:
                    error_count += 1
                    failed_ids.append(str(uid))
                    print(f"BadRequest РїСЂРё РѕС‚РїСЂР°РІРєРµ {uid}: {e}")
                    break
            except TimedOut as e:
                # РўР°Р№РјР°СѓС‚ вЂ” РїРѕРґРѕР¶РґС‘Рј Рё СЂРµС‚СЂР°РёРј
                print(f"TimedOut РїСЂРё РѕС‚РїСЂР°РІРєРµ {uid}, РїРѕРїС‹С‚РєР° {attempt}: {e}")
                await asyncio.sleep(1 + attempt)
                continue
            except Exception as e:
                # Р’РѕР·РјРѕР¶РЅС‹Р№ httpx.RemoteProtocolError РёР»Рё РґСЂСѓРіРёРµ СЃР±РѕРё вЂ” СЂРµС‚СЂР°РёРј РЅРµСЃРєРѕР»СЊРєРѕ СЂР°Р·
                if _is_remote_protocol_error(e):
                    print(f"RemoteProtocolError-ish РїСЂРё РѕС‚РїСЂР°РІРєРµ {uid}, РїРѕРїС‹С‚РєР° {attempt}: {e}")
                    await asyncio.sleep(1 + attempt)
                    continue
                error_count += 1
                failed_ids.append(str(uid))
                print(f"РќРµРёР·РІРµСЃС‚РЅР°СЏ РѕС€РёР±РєР° РїСЂРё РѕС‚РїСЂР°РІРєРµ {uid}: {e}")
                break

        # РќРµР±РѕР»СЊС€Р°СЏ РїР°СѓР·Р° РјРµР¶РґСѓ СѓСЃРїРµС€РЅС‹РјРё РѕС‚РїСЂР°РІРєР°РјРё С‡С‚РѕР±С‹ РЅРµ С‚СЂРёРіРіРµСЂРёС‚СЊ Р»РёРјРёС‚С‹
        if sent:
            await asyncio.sleep(0.15)

    # РџРѕРґРіРѕС‚РѕРІРёРј РєСЂР°С‚РєРёР№ РѕС‚С‡С‘С‚ вЂ” РЅРµ РІС‹РІРѕРґРёРј РґР»РёРЅРЅС‹Рµ СЃРїРёСЃРєРё С†РµР»РёРєРѕРј
    failed_preview = ", ".join(failed_ids[:20])
    failed_more = max(0, len(failed_ids) - 20)

    summary = (
        f"{WINTER_EMOJIS['check']} <b>Р РђРЎРЎР«Р›РљРђ Р—РђР’Р•Р РЁР•РќРђ!</b>\n\n"
        f"вЂў РЈСЃРїРµС€РЅРѕ: <b>{sent_count}</b>\n"
        f"вЂў РћС€РёР±РѕРє: <b>{error_count}</b>\n"
        f"вЂў Р’СЃРµРіРѕ: <b>{sent_count + error_count}</b>"
    )
    if failed_ids:
        summary += f"\n\nР§Р°СЃС‚СЊ РЅРµ РґРѕСЃС‚Р°РІР»РµРЅРЅС‹С… ID (РїРµСЂРІС‹Рµ {min(20, len(failed_ids))}): {escape_html(failed_preview)}"
        if failed_more:
            summary += f" Рё РµС‰С‘ {failed_more}..."

    await progress_msg.edit_text(summary, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# === Р‘Р­РљРђРџР« (С„РёРєСЃ: СЂР°РЅСЊС€Рµ С„СѓРЅРєС†РёРё Р±С‹Р»Рё РїРµСЂРµРѕРїСЂРµРґРµР»РµРЅС‹, РёР·-Р·Р° СЌС‚РѕРіРѕ inline РєРЅРѕРїРєРё /admin "РЅРµ СЂР°Р±РѕС‚Р°Р»Рё") ===
async def _send_file_to_admin(context: ContextTypes.DEFAULT_TYPE, chat_id: int, path: str, caption: str, filename_prefix: str):
    with open(path, "rb") as f:
        await context.bot.send_document(
            chat_id=chat_id,
            document=f,
            filename=f"{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            caption=caption,
        )


async def send_database_backup_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    if not is_admin(user_id):
        if update.callback_query:
            await update.callback_query.answer("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ", show_alert=True)
        return
    try:
        await _send_file_to_admin(
            context,
            chat_id=int(user_id),
            path=DB_FILE,
            caption=f"{WINTER_EMOJIS['snowflake']} Р РµР·РµСЂРІРЅР°СЏ РєРѕРїРёСЏ Р±Р°Р·С‹ РґР°РЅРЅС‹С… СЂРµР»РёР·РѕРІ",
            filename_prefix="releases_backup",
        )
        if update.callback_query:
            await update.callback_query.answer("Р‘Р°Р·Р° РґР°РЅРЅС‹С… РѕС‚РїСЂР°РІР»РµРЅР° РІ Р›РЎ!", show_alert=True)
        else:
            await update.message.reply_text(f"{WINTER_EMOJIS['check']} Р‘Р°Р·Р° РґР°РЅРЅС‹С… РѕС‚РїСЂР°РІР»РµРЅР° РІ Р›РЎ!")
    except Exception as e:
        if update.callback_query:
            await update.callback_query.answer(f"РћС€РёР±РєР°: {e}", show_alert=True)
        else:
            await update.message.reply_text(f"{WINTER_EMOJIS['cross']} РћС€РёР±РєР°: {e}")


async def send_moderation_backup_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    if not is_admin(user_id):
        if update.callback_query:
            await update.callback_query.answer("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ", show_alert=True)
        return
    try:
        await _send_file_to_admin(
            context,
            chat_id=int(user_id),
            path=MODERATION_DB_FILE,
            caption=f"{WINTER_EMOJIS['snowman']} РђСЂС…РёРІ РјРѕРґРµСЂР°С†РёРё",
            filename_prefix="moderation_backup",
        )
        if update.callback_query:
            await update.callback_query.answer("РђСЂС…РёРІ РјРѕРґРµСЂР°С†РёРё РѕС‚РїСЂР°РІР»РµРЅ РІ Р›РЎ!", show_alert=True)
        else:
            await update.message.reply_text(f"{WINTER_EMOJIS['check']} РђСЂС…РёРІ РјРѕРґРµСЂР°С†РёРё РѕС‚РїСЂР°РІР»РµРЅ РІ Р›РЎ!")
    except Exception as e:
        if update.callback_query:
            await update.callback_query.answer(f"РћС€РёР±РєР°: {e}", show_alert=True)
        else:
            await update.message.reply_text(f"{WINTER_EMOJIS['cross']} РћС€РёР±РєР°: {e}")


# === РљРћРњРђРќР”Р« РђР”РњРРќРђ Р”Р›РЇ Р‘Р­РљРђРџРђ ===
async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ.")
        return
    await send_database_backup_to_admin(update, context)

async def moderation_backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Р”РѕСЃС‚СѓРї Р·Р°РїСЂРµС‰С‘РЅ.")
        return
    await send_moderation_backup_to_admin(update, context)

def _format_release_form_for_group(user, user_id: str, data: dict) -> str:
    # Р¤РѕСЂРјР°С‚ СЂРѕРІРЅРѕ РєР°Рє РІ РїСЂРёРјРµСЂРµ, СЃ С„РёРєСЃРёСЂРѕРІР°РЅРЅС‹Рј РїРѕСЂСЏРґРєРѕРј РїРѕР»РµР№.
    username = f"@{user.username}" if user and user.username else "РЅРµС‚"
    release_type = data.get("type", "вЂ”")

    lines = [
        f"{WINTER_EMOJIS['snowflake']} <b>РќРћР’РђРЇ РђРќРљР•РўРђ!</b>",
        f"РћС‚: {escape_html(username)}",
        f"ID: <code>{escape_html(user_id)}</code>",
        f"РўРёРї: {escape_html(release_type)}",
        "",
    ]

    # Р”РѕР±Р°РІР»СЏРµРј UPC РµСЃР»Рё РµСЃС‚СЊ
    upc = data.get("upc")
    if upc:
        lines.append(f"рџ“¦ <b>UPC:</b> <code>{escape_html(upc)}</code>")
        lines.append("")

    def add(label: str, key: str, default: str = "вЂ”"):
        val = data.get(key)
        if val is None or str(val).strip() == "":
            val = default
        lines.append(f"вЂў <b>{label}:</b> {escape_html(val)}")
    # Р СѓСЃСЃРєРёРµ РјРµС‚РєРё Рё СѓР±СЂР°РЅС‹ РїРѕР»СЏ UPC/ISRC
    add("РќР°Р·РІР°РЅРёРµ", "name")
    add("РЎР°Р±-РЅР°Р·РІР°РЅРёРµ", "subname", ".")
    add("РќРёРє", "nick")
    add("Р¤РРћ", "fio")
    add("Р”Р°С‚Р°", "date")
    add("Р’РµСЂСЃРёСЏ", "version")
    add("Р–Р°РЅСЂ", "genre")
    add("РЎСЃС‹Р»РєР°", "link")
    add("РЇРЅРґРµРєСЃ РњСѓР·С‹РєР°", "yandex", ".")
    add("РњР°С‚", "mat")
    add("РџСЂРѕРјРѕ", "promo", ".")
    add("РљРѕРјРјРµРЅС‚Р°СЂРёР№", "comment", ".")
    if data.get("type") == "Р°Р»СЊР±РѕРј":
        add("Tracklist", "tracklist")
    add("Tg", "tg")
    return "\n".join(lines)


def _build_moderation_keyboard(user_id: str, idx: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("рџ•“ РќР° РѕС‚РіСЂСѓР·РєРµ", callback_data=f"m_upload_{user_id}_{idx}"),
            InlineKeyboardButton("рџ§  РњРѕРґРµСЂР°С†РёСЏ", callback_data=f"m_moderate_{user_id}_{idx}"),
            InlineKeyboardButton("вњ… РџСЂРёРЅСЏС‚Рѕ", callback_data=f"m_approve_{user_id}_{idx}")
        ],
        [
            InlineKeyboardButton("вќЊ РћС‚РєР»РѕРЅРёС‚СЊ", callback_data=f"m_reject_{user_id}_{idx}"),
            InlineKeyboardButton("вњЏпёЏ РќР° РёСЃРїСЂР°РІР»РµРЅРёРё", callback_data=f"m_needfix_{user_id}_{idx}"),
            InlineKeyboardButton("рџ—‘ РЈРґР°Р»РµРЅ", callback_data=f"m_delete_{user_id}_{idx}")
        ],
    ])


async def _submit_release_to_moderation(
    context: ContextTypes.DEFAULT_TYPE,
    user,
    user_id: str,
    release_data: dict,
) -> int:
    global moderation_db

    release_data["status"] = STATUS_ON_UPLOAD
    release_data["submission_time"] = release_data.get("submission_time") or datetime.now().isoformat()
    release_data.setdefault("reminder_sent", False)

    idx = len(db.get(user_id, []))
    keyboard = _build_moderation_keyboard(user_id, idx)
    msg = _format_release_form_for_group(user, user_id, release_data)

    print(
        f"[WEBAPP_DIAG] moderation_send_attempt chat_id={MODERATION_CHAT_ID} user_id={user_id} "
        f"release={clean(str(release_data.get('name', '')))}",
        flush=True,
    )
    try:
        moderation_msg = await context.bot.send_message(
            MODERATION_CHAT_ID,
            msg,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
    except Exception as e:
        print(f"[WEBAPP_DIAG] moderation_send_failed chat_id={MODERATION_CHAT_ID} error={e}", flush=True)
        raise
    print(
        f"[WEBAPP_DIAG] moderation_send_ok chat_id={MODERATION_CHAT_ID} message_id={moderation_msg.message_id}",
        flush=True,
    )

    try:
        await context.bot.pin_chat_message(chat_id=MODERATION_CHAT_ID, message_id=moderation_msg.message_id)
    except Exception:
        pass

    release_data["moderation_message_id"] = moderation_msg.message_id
    release_data["moderation_original_text"] = msg

    moderation_data = release_data.copy()
    moderation_data["message_id"] = moderation_msg.message_id
    moderation_data["user_id"] = user_id
    moderation_data["username"] = getattr(user, "username", None)

    moderation_db.setdefault("moderation_messages", []).append(moderation_data)
    save_moderation_db(moderation_db)

    db.setdefault(user_id, [])
    release_data["username"] = getattr(user, "username", "") or release_data.get("username", "")
    db[user_id].append(release_data.copy())
    save_db(db)

    try:
        await _append_status_to_moderation_message(
            context,
            moderation_msg.message_id,
            msg,
            release_data.get("status", STATUS_ON_UPLOAD),
            reply_markup=moderation_msg.reply_markup,
        )
    except Exception as e:
        print(f"РћС€РёР±РєР° РїСЂРё РґРѕР±Р°РІР»РµРЅРёРё С€Р°РїРєРё СЃС‚Р°С‚СѓСЃР°: {e}")

    try:
        upc_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("рџ“¦ РџСЂРёСЃРІРѕРёС‚СЊ UPC", callback_data=f"m_add_upc_{user_id}_{idx}")]
        ])
        await context.bot.send_message(
            chat_id=MODERATION_CHAT_ID,
            text="рџ’ѕ <b>Р”РѕР±Р°РІСЊС‚Рµ UPC РєРѕРґ РґР»СЏ СЌС‚РѕРіРѕ СЂРµР»РёР·Р°</b>\n\n"
                 "РќР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ Рё РѕС‚РІРµС‚СЊС‚Рµ UPC РєРѕРґРѕРј РЅР° РёСЃС…РѕРґРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ Р°РЅРєРµС‚С‹.",
            reply_to_message_id=moderation_msg.message_id,
            parse_mode=ParseMode.HTML,
            reply_markup=upc_keyboard,
        )
    except Exception as e:
        print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё РєРЅРѕРїРєРё UPC: {e}")

    return idx


def _format_status_append(status: str, moderator_username: str | None = None, reason: str | None = None, comment: str | None = None) -> str:
    # FIX: РїСЂРёРІРµРґРµРЅРѕ Рє РµРґРёРЅРѕРјСѓ С„РѕСЂРјР°С‚Сѓ СЃР»СѓР¶РµР±РЅРѕРіРѕ Р±Р»РѕРєР° (immutable РєР°СЂС‚РѕС‡РєР° + РґРѕРї.СЃР»СѓР¶РµР±РЅС‹Р№ Р±Р»РѕРє)
    status_emoji = {
        STATUS_ON_UPLOAD: WINTER_EMOJIS['waiting'],
        STATUS_MODERATION: WINTER_EMOJIS['brain'] if 'brain' in WINTER_EMOJIS else WINTER_EMOJIS['waiting'],
        STATUS_APPROVED: WINTER_EMOJIS['check'],
        STATUS_REJECTED: WINTER_EMOJIS['cross'],
        STATUS_NEEDS_FIX: WINTER_EMOJIS['waiting'],
        STATUS_DELETED: WINTER_EMOJIS['cross'],
    }
    status_text = {
        STATUS_ON_UPLOAD: "РќР° РѕС‚РіСЂСѓР·РєРµ",
        STATUS_MODERATION: "РќР° РјРѕРґРµСЂР°С†РёРё",
        STATUS_APPROVED: "РћРґРѕР±СЂРµРЅРѕ",
        STATUS_REJECTED: "РћС‚РєР»РѕРЅРµРЅРѕ",
        STATUS_NEEDS_FIX: "РўСЂРµР±СѓРµС‚ РїСЂР°РІРѕРє",
        STATUS_DELETED: "РЈРґР°Р»РµРЅРѕ",
    }
    t = datetime.now().strftime("%d.%m.%Y %H:%M")
    lines = ["", "в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ", f"{status_emoji.get(status, WINTER_EMOJIS['waiting'])} <b>РЎС‚Р°С‚СѓСЃ: {escape_html(status_text.get(status, status))}</b>",]
    # СЃР»СѓР¶РµР±РЅС‹Р№ Р±Р»РѕРє РІ С‚СЂРµР±СѓРµРјРѕРј С„РѕСЂРјР°С‚Рµ
    lines.append(f"<b>РџСЂРёС‡РёРЅР°:</b> {escape_html(reason) if reason else 'вЂ”'}")
    lines.append(f"<b>РњРѕРґРµСЂР°С‚РѕСЂ:</b> @{escape_html(moderator_username)}" if moderator_username else f"<b>РњРѕРґРµСЂР°С‚РѕСЂ:</b> вЂ”")
    lines.append(f"<b>РџРѕСЃР»РµРґРЅРµРµ РґРµР№СЃС‚РІРёРµ:</b> {escape_html(comment) if comment else 'вЂ”'}")
    lines.append(f"<b>Р’СЂРµРјСЏ:</b> {escape_html(t)}")
    lines.append("в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ")
    return "\n".join(lines)


async def _append_status_to_moderation_message(context: ContextTypes.DEFAULT_TYPE, message_id: int, original_text: str, status: str, moderator_username: str | None = None, reason: str | None = None, comment: str | None = None, reply_markup=None):
    """Р”РѕР±Р°РІР»СЏРµС‚ СЃР»СѓР¶РµР±РЅС‹Р№ Р±Р»РѕРє СЃС‚Р°С‚СѓСЃР° Рё РїС‹С‚Р°РµС‚СЃСЏ РѕС‚СЂРµРґР°РєС‚РёСЂРѕРІР°С‚СЊ РёСЃС…РѕРґРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ,
    РїСЂРё СЌС‚РѕРј СЃРѕС…СЂР°РЅСЏСЏ РєР»Р°РІРёР°С‚СѓСЂСѓ (С‡РµСЂРµР· РїР°СЂР°РјРµС‚СЂ `reply_markup`). Р•СЃР»Рё СЂРµРґР°РєС‚РёСЂРѕРІР°С‚СЊ РЅРµР»СЊР·СЏ вЂ”
    Fall back: РѕС‚РїСЂР°РІР»СЏРµРј РѕС‚РґРµР»СЊРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ-С€С‚Р°РјРї СЃРѕ СЃС‚Р°С‚СѓСЃРѕРј (РєР°Рє СЂР°РЅСЊС€Рµ).
    """
    status_text = _format_status_append(status, moderator_username=moderator_username, reason=reason, comment=comment)

    # РљРѕСЂРѕС‚РєРёР№ СЃС‚Р°С‚СѓСЃ РґР»СЏ С€Р°РїРєРё Р°РЅРєРµС‚С‹
    status_short = {
        STATUS_ON_UPLOAD: "РќР° РѕС‚РіСЂСѓР·РєРµ",
        STATUS_MODERATION: "РќР° РјРѕРґРµСЂР°С†РёРё",
        STATUS_APPROVED: "РћРґРѕР±СЂРµРЅРѕ",
        STATUS_REJECTED: "РћС‚РєР»РѕРЅРµРЅРѕ",
        STATUS_NEEDS_FIX: "РўСЂРµР±СѓРµС‚ РїСЂР°РІРѕРє",
        STATUS_DELETED: "РЈРґР°Р»РµРЅРѕ",
    }.get(status, status)

    emoji = {
        STATUS_ON_UPLOAD: WINTER_EMOJIS.get('upload', ''),
        STATUS_MODERATION: WINTER_EMOJIS.get('brain', WINTER_EMOJIS.get('waiting')),
        STATUS_APPROVED: WINTER_EMOJIS.get('check', ''),
        STATUS_REJECTED: WINTER_EMOJIS.get('cross', ''),
        STATUS_NEEDS_FIX: WINTER_EMOJIS.get('warning', WINTER_EMOJIS.get('waiting')),
        STATUS_DELETED: WINTER_EMOJIS.get('delete', ''),
    }.get(status, '')

    header = f"{emoji} <b>РЎРўРђРўРЈРЎ: {escape_html(status_short)}</b>\n\n"

    # РџРѕРїСЂРѕР±СѓРµРј РѕС‚СЂРµРґР°РєС‚РёСЂРѕРІР°С‚СЊ РёСЃС…РѕРґРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ, РґРѕР±Р°РІРёРІ С€Р°РїРєСѓ СЃС‚Р°С‚СѓСЃР° Рё СЃРѕС…СЂР°РЅРёРІ РєР»Р°РІРёР°С‚СѓСЂСѓ
    try:
        await context.bot.edit_message_text(
            chat_id=MODERATION_CHAT_ID,
            message_id=message_id,
            text=header + (original_text or ""),
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
    except Exception as e:
        # Р•СЃР»Рё СЂРµРґР°РєС‚РёСЂРѕРІР°С‚СЊ РЅРµР»СЊР·СЏ (РЅР°РїСЂРёРјРµСЂ, СЃСЂРѕРє РёСЃС‚С‘Рє) вЂ” С€Р»С‘Рј РѕС‚РґРµР»СЊРЅС‹Рј СЃРѕРѕР±С‰РµРЅРёРµРј-С€С‚Р°РјРїРѕРј
        try:
            await context.bot.send_message(
                chat_id=MODERATION_CHAT_ID,
                text=status_text,
                parse_mode=ParseMode.HTML,
                reply_to_message_id=message_id,
            )
        except Exception as e2:
            if not (_is_remote_protocol_error(e2) or isinstance(e2, TimedOut)):
                print(f"вќЊ _append_status_to_moderation_message: {e2}")


# === CALLBACK-Р РћРЈРўР•Р  (РіР»РѕР±Р°Р»СЊРЅРѕ) ===
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass
    data = query.data
    user_id = str(query.from_user.id)
    try:
        msg = query.message
        print(f"[BUTTON] callback received: data={data} from_user={user_id} chat_id={getattr(msg, 'chat_id', None)} msg_id={getattr(msg, 'message_id', None)}")
    except Exception as e:
        print(f"[BUTTON] callback debug error: {e}")
    # РџРѕРєР°Р·Р°С‚СЊ РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ РїРѕРґС‚РІРµСЂР¶РґРµРЅРёРµ РІС‹Р±РѕСЂР° РґР»СЏ РїСЂРѕРјРѕ-РєРЅРѕРїРѕРє
    try:
        if data and data.startswith('promo_'):
            pretty = {
                'promo_project_solo': 'Solo',
                'promo_project_feat': 'Feat',
                'promo_kind_single': 'Single',
                'promo_kind_ep': 'EP',
                'promo_kind_album': 'Album',
                'promo_vocal_no': 'Instrumental',
                'promo_vocal_male': 'Male vocal',
                'promo_vocal_female': 'Female vocal',
                'promo_text': 'Start promo',
            }.get(data, data)
            await query.answer(text=f"Р’С‹Р±СЂР°РЅРѕ: {pretty}", show_alert=False)
    except Exception:
        pass

    if data == 'menu_distribution':
        await safe_edit(query, "<b>Р”РёСЃС‚СЂРёР±СѓС†РёСЏ</b>\n\nР’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ:", reply_markup=build_distribution_keyboard())
        return REPORT

    if data == 'menu_services':
        await safe_edit(query, "<b>РЎРµСЂРІРёСЃС‹</b>\n\nР’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ:", reply_markup=build_services_keyboard())
        return REPORT

    if data == 'menu_cabinet':
        await safe_edit(query, "<b>РљР°Р±РёРЅРµС‚</b>\n\nР’С‹Р±РµСЂРёС‚Рµ РґРµР№СЃС‚РІРёРµ:", reply_markup=build_cabinet_keyboard())
        return REPORT

    if data == 'menu_community':
        await safe_edit(query, "<b>РљРѕРјСЊСЋРЅРёС‚Рё</b>\n\nРћС„РёС†РёР°Р»СЊРЅС‹Рµ РїР»РѕС‰Р°РґРєРё CXRNER MUSIC:", reply_markup=build_community_keyboard())
        return REPORT

    if data == 'open_app':
        if not is_webapp_url_ready():
            await query.message.reply_text(
                "вќЊ Mini App URL РЅРµ РЅР°СЃС‚СЂРѕРµРЅ.\n"
                "РЈСЃС‚Р°РЅРѕРІРёС‚Рµ РїРµСЂРµРјРµРЅРЅСѓСЋ РѕРєСЂСѓР¶РµРЅРёСЏ:\n"
                "<code>WEBAPP_URL=https://РІР°С€-РґРѕРјРµРЅ/index.html</code>",
                parse_mode=ParseMode.HTML,
            )
            return REPORT
        await query.message.reply_text(
            "рџЋµ <b>Р—Р°РїСѓСЃРє Mini App</b>\n\n"
            "РќР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ РЅРёР¶Рµ. Р’ С‚Р°РєРѕРј СЂРµР¶РёРјРµ РґР°РЅРЅС‹Рµ Р°РЅРєРµС‚С‹ РіР°СЂР°РЅС‚РёСЂРѕРІР°РЅРЅРѕ СѓС…РѕРґСЏС‚ РІ Р±РѕС‚Р°.",
            parse_mode=ParseMode.HTML,
            reply_markup=build_webapp_reply_keyboard(),
        )
        return REPORT

    if data == 'report':
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("РЎРёРЅРіР»", callback_data='single')],
            [InlineKeyboardButton("РђР»СЊР±РѕРј", callback_data='album')]
        ])
        await safe_edit(query, "<b>Р’С‹Р±РµСЂРёС‚Рµ С‚РёРї СЂРµР»РёР·Р°:</b>", keyboard)
        return TYPE

    if data == 'order_cover':
        return await order_cover_start(update, context)

    if data == 'promo_text':
        return await promo_start(update, context)

    if data == 'my_releases':
        await my_cmd(update, context)
        return REPORT

    # РќР°РІРёРіР°С†РёСЏ РїРѕ РєР°СЂС‚РѕС‡РєР°Рј СЂРµР»РёР·РѕРІ
    if data.startswith('card_'):
        try:
            page = int(data.split('_')[1])
            await my_cmd(update, context, page=page)
        except (ValueError, IndexError):
            await query.answer('вќЊ РћС€РёР±РєР° РЅР°РІРёРіР°С†РёРё', show_alert=True)
        return
    
    # РџСѓСЃС‚РѕР№ callback (РєРЅРѕРїРєР° РЅРѕРјРµСЂР° СЃС‚СЂР°РЅРёС†С‹)
    if data == 'noop':
        await query.answer()
        return

    if data == 'single':
        user_data[user_id] = {"type": "СЃРёРЅРіР»", "status": "pending"}
        await safe_edit(query, f"{WINTER_EMOJIS['notes']} <b>РЎРРќР“Р›</b>\n\n<b>1. РќР°Р·РІР°РЅРёРµ СЂРµР»РёР·Р°</b>\nРџСЂРёРјРµСЂ: Lost in the Void")
        return NAME

    if data == 'album':
        user_data[user_id] = {"type": "Р°Р»СЊР±РѕРј", "status": "pending"}
        await safe_edit(query, f"{WINTER_EMOJIS['notes']} <b>РђР›Р¬Р‘РћРњ</b>\n\n<b>1. РќР°Р·РІР°РЅРёРµ СЂРµР»РёР·Р°</b>\nРџСЂРёРјРµСЂ: Lost in the Void")
        return NAME

    if data == 'send':
        await show_release_warning(query, context)
        return CONFIRM

    if data == 'send_confirm':
        await send_moderation(query, context)
        return REPORT

    if data == 'send_cancel':
        user_data.pop(user_id, None)
        delete_draft_for_user(user_id)
        await safe_edit(query, f"{WINTER_EMOJIS['cross']} <b>РђРЅРєРµС‚Р° РѕС‚РјРµРЅРµРЅР°.</b>", parse_mode=ParseMode.HTML)
        return REPORT

    

    if data == 'main':
        return await start_cmd(update, context)
        
    if data == 'get_db':
        await send_database_backup_to_admin(update, context)
        return
        
    if data == 'get_moderation_db':
        await send_moderation_backup_to_admin(update, context)
        return
        
    # РђРґРјРёРЅСЃРєРёРµ РєРЅРѕРїРєРё
    if data == 'admin_stats':
        await admin_stats_cmd(update, context)
        return
    if data.startswith('stats_period_'):
        # РџРѕРєР°Р·Р°С‚СЊ СЃС‚Р°С‚РёСЃС‚РёРєСѓ Р·Р° РІС‹Р±СЂР°РЅРЅС‹Р№ РїРµСЂРёРѕРґ (РґРѕСЃС‚СѓРїРЅРѕ РІ С‡Р°С‚Рµ РјРѕРґРµСЂР°С†РёРё)
        chat_id = update.callback_query.message.chat_id if update.callback_query.message else None
        if not is_moderation_chat(chat_id):
            await update.callback_query.answer('вќЊ РЎС‚Р°С‚РёСЃС‚РёРєР° РґРѕСЃС‚СѓРїРЅР° С‚РѕР»СЊРєРѕ РІ С‡Р°С‚Рµ РјРѕРґРµСЂР°С†РёРё', show_alert=True)
            return
        
        period = data.split('_')[-1]
        now = datetime.now()
        cutoff = None
        period_name = "Р’СЃРµ РІСЂРµРјСЏ"
        if period == 'week':
            cutoff = now - timedelta(days=7)
            period_name = "РџРѕСЃР»РµРґРЅРёРµ 7 РґРЅРµР№"
        elif period == 'month':
            cutoff = now - timedelta(days=30)
            period_name = "РџРѕСЃР»РµРґРЅРёРµ 30 РґРЅРµР№"
        # РЎРѕР±РёСЂР°РµРј СЃС‚Р°С‚РёСЃС‚РёРєСѓ
        total = 0
        approved = 0
        rejected = 0
        reject_reasons = {}
        artist_counts = {}
        for uid, rels in db.items():
            for r in rels:
                try:
                    st = r.get('submission_time')
                    if cutoff and st:
                        if datetime.fromisoformat(st) < cutoff:
                            continue
                except Exception:
                    pass
                total += 1
                status = r.get('status')
                if status == STATUS_APPROVED:
                    approved += 1
                if status == STATUS_REJECTED:
                    rejected += 1
                if r.get('reject_reason'):
                    reject_reasons[r.get('reject_reason')] = reject_reasons.get(r.get('reject_reason'), 0) + 1
                nick = r.get('nick') or r.get('username') or uid
                artist_counts[nick] = artist_counts.get(nick, 0) + 1

        approved_pct = (approved * 100 / total) if total else 0
        top_reasons = sorted(reject_reasons.items(), key=lambda x: x[1], reverse=True)[:3]
        top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        # РљРѕРјРїР°РєС‚РЅС‹Р№ С„РѕСЂРјР°С‚ СЃС‚Р°С‚РёСЃС‚РёРєРё
        text = (
            f"рџ“Љ <b>РЎРўРђРўРРЎРўРРљРђ</b> ({period_name})\n\n"
            f"рџ“¦ <b>Р’СЃРµРіРѕ Р°РЅРєРµС‚:</b> {total}\n"
            f"вњ… <b>РџСЂРёРЅСЏС‚Рѕ:</b> {approved} ({approved_pct:.1f}%)\n"
            f"вќЊ <b>РћС‚РєР»РѕРЅРµРЅРѕ:</b> {rejected}\n\n"
            f"вќЊ <b>РўРѕРї 3 РїСЂРёС‡РёРЅС‹ РѕС‚РєР°Р·Р°:</b>\n"
        )
        if top_reasons:
            for i, (reason, count) in enumerate(top_reasons, 1):
                text += f"  {i}. {escape_html(reason)} вЂ” {count}\n"
        else:
            text += "  РќРµС‚ РґР°РЅРЅС‹С…\n"
        text += f"\nрџ”Ґ <b>РўРѕРї 3 Р°СЂС‚РёСЃС‚С‹:</b>\n"
        if top_artists:
            for i, (artist, count) in enumerate(top_artists, 1):
                text += f"  {i}. {escape_html(artist)} вЂ” {count}\n"
        else:
            text += "  РќРµС‚ РґР°РЅРЅС‹С…\n"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("в—Ђ РќР°Р·Р°Рґ", callback_data='admin_back')]
        ])
        await safe_edit(update.callback_query, text, reply_markup=keyboard)
        return
    
    # NOTE: РћР±СЂР°Р±РѕС‚С‡РёРє РґР»СЏ РєРЅРѕРїРєРё "РџРѕРґСЂРѕР±РЅРµРµ" РІ Р»РёС‡РЅРѕРј РєР°Р±РёРЅРµС‚Рµ
    if data.startswith('release_details_'):
        parts = data.split('_', 3)  # release_details_userid_idx
        if len(parts) >= 4:
            user_id = parts[2]
            rel_idx = int(parts[3])
            if user_id in db and rel_idx < len(db[user_id]):
                rel = db[user_id][rel_idx]
                
                # РљСЂР°СЃРёРІС‹Р№ С„РѕСЂРјР°С‚ СЃ РіСЂСѓРїРїРёСЂРѕРІРєРѕР№ РёРЅС„РѕСЂРјР°С†РёРё
                status = rel.get('status', STATUS_ON_UPLOAD)
                status_text = {
                    STATUS_ON_UPLOAD: 'вЏі РќР° РѕС‚РіСЂСѓР·РєРµ',
                    STATUS_APPROVED: 'вњ… РћРґРѕР±СЂРµРЅРѕ',
                    STATUS_REJECTED: 'вќЊ РћС‚РєР»РѕРЅРµРЅРѕ',
                    STATUS_NEEDS_FIX: 'вљ пёЏ РўСЂРµР±СѓРµС‚ РїСЂР°РІРѕРє',
                    STATUS_MODERATION: 'рџ§  РќР° РјРѕРґРµСЂР°С†РёРё',
                }.get(status, 'вЂ” РќРµРёР·РІРµСЃС‚РЅРѕ')
                
                # РћСЃРЅРѕРІРЅР°СЏ РёРЅС„РѕСЂРјР°С†РёСЏ
                details_text = (
                    f"{WINTER_EMOJIS['notes']} <b>РРќР¤РћР РњРђР¦РРЇ Рћ Р Р•Р›РР—Р•</b>\n"
                    f"{'в”Ђ' * 40}\n\n"
                    f"<b>РќР°Р·РІР°РЅРёРµ</b>\n"
                    f"рџЋµ {escape_html(rel.get('name', 'вЂ”'))}\n\n"
                )
                
                # Р”РѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Рµ РЅР°Р·РІР°РЅРёСЏ
                if rel.get('subname') and rel.get('subname') != '.':
                    details_text += f"<b>РџРѕРґРёРјРµРЅРѕРІР°РЅРёРµ</b>\n"
                    details_text += f"  {escape_html(rel.get('subname'))}\n\n"
                
                # РћСЃРЅРѕРІРЅС‹Рµ РјРµС‚Р°РґР°РЅРЅС‹Рµ
                details_text += f"<b>рџ“‹ РћРЎРќРћР’РќР«Р• Р”РђРќРќР«Р•</b>\n"
                details_text += f"РўРёРї: <i>{escape_html(rel.get('type', 'вЂ”'))}</i>\n"
                details_text += f"Р–Р°РЅСЂ: <i>{escape_html(rel.get('genre', 'вЂ”'))}</i>\n"
                details_text += f"Р”Р°С‚Р° СЂРµР»РёР·Р°: <i>{escape_html(rel.get('date', 'вЂ”'))}</i>\n"
                details_text += f"Р’РµСЂСЃРёСЏ: <i>{escape_html(rel.get('version', 'вЂ”'))}</i>\n\n"
                
                # РРЅС„РѕСЂРјР°С†РёСЏ РѕР± Р°СЂС‚РёСЃС‚Рµ
                details_text += f"<b>рџ‘¤ РђР РўРРЎРў</b>\n"
                details_text += f"РќРёРє: <i>{escape_html(rel.get('nick', 'вЂ”'))}</i>\n"
                details_text += f"Р¤РРћ: <i>{escape_html(rel.get('fio', 'вЂ”'))}</i>\n\n"
                
                # РљРѕРЅС‚Р°РєС‚С‹ Рё СЃСЃС‹Р»РєРё
                details_text += f"<b>рџ”— РЎРЎР«Р›РљР Р РљРћРќРўРђРљРўР«</b>\n"
                details_text += f"Telegram: <i>{escape_html(rel.get('tg', 'вЂ”'))}</i>\n"
                if rel.get('link'):
                    details_text += f"РЎСЃС‹Р»РєР°: <i>{escape_html(rel.get('link')[:50])}...</i>\n"
                if rel.get('yandex'):
                    details_text += f"РЇРЅРґРµРєСЃ: <i>{escape_html(rel.get('yandex')[:50])}...</i>\n"
                details_text += "\n"
                
                # РљРѕРґС‹ Рё РёРґРµРЅС‚РёС„РёРєР°С‚РѕСЂС‹
                if rel.get('upc') and rel.get('upc') != '.':
                    details_text += f"<b>рџ”ў РљРћР”Р«</b>\n"
                    if rel.get('upc') and rel.get('upc') != '.':
                        details_text += f"UPC: <i>{escape_html(rel.get('upc'))}</i>\n"
                    if rel.get('isrc') and rel.get('isrc') != '.':
                        details_text += f"ISRC: <i>{escape_html(rel.get('isrc'))}</i>\n"
                    details_text += "\n"
                
                # РҐР°СЂР°РєС‚РµСЂРёСЃС‚РёРєРё С‚СЂРµРєР°
                details_text += f"<b>рџЋ™пёЏ РҐРђР РђРљРўР•Р РРЎРўРРљР</b>\n"
                has_lyrics = rel.get('has_lyrics', 'вЂ”')
                details_text += f"РЎР»РѕРІР°: <i>{escape_html(has_lyrics)}</i>\n"
                mat = rel.get('mat', 'вЂ”')
                details_text += f"РњР°С‚: <i>{escape_html(mat)}</i>\n"
                details_text += "\n"
                
                # РљРѕРјРјРµРЅС‚Р°СЂРёРё
                if rel.get('promo') or rel.get('comment'):
                    details_text += f"<b>рџ’¬ РљРћРњРњР•РќРўРђР РР</b>\n"
                    if rel.get('promo'):
                        details_text += f"РџСЂРѕРјРѕ: <i>{escape_html(rel.get('promo')[:80])}...</i>\n"
                    if rel.get('comment'):
                        details_text += f"РљРѕРјРјРµРЅС‚Р°СЂРёР№: <i>{escape_html(rel.get('comment')[:80])}...</i>\n"
                    details_text += "\n"
                
                # РЎС‚Р°С‚СѓСЃ Рё РґР°С‚С‹
                details_text += f"{'в”Ђ' * 40}\n"
                details_text += f"<b>рџ“Љ РЎРўРђРўРЈРЎ</b>\n"
                details_text += f"{status_text}\n"
                
                if rel.get('reject_reason'):
                    details_text += f"\nвќЊ <b>РџСЂРёС‡РёРЅР° РѕС‚РєР°Р·Р°</b>\n"
                    details_text += f"<i>{escape_html(rel.get('reject_reason'))}</i>\n"
                
                if rel.get('moderator_comment'):
                    details_text += f"\nрџ’¬ <b>РљРѕРјРјРµРЅС‚Р°СЂРёР№ РјРѕРґРµСЂР°С‚РѕСЂР°</b>\n"
                    details_text += f"<i>{escape_html(rel.get('moderator_comment'))}</i>\n"
                
                # Р’СЂРµРјСЏ РѕС‚РїСЂР°РІРєРё
                details_text += f"\nвЏ° РћС‚РїСЂР°РІР»РµРЅРѕ: <i>{escape_html(rel.get('submission_time', 'вЂ”')[:19])}</i>"
                if rel.get('moderation_time'):
                    details_text += f"\nвЏ° РњРѕРґРµСЂРёСЂРѕРІР°РЅРѕ: <i>{escape_html(rel.get('moderation_time', 'вЂ”')[:19])}</i>"
                
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("в—Ђ Р’ РєР°Р±РёРЅРµС‚", callback_data="my_back")
                ]])
                await safe_edit(update.callback_query, details_text, reply_markup=keyboard)
        return
    
    # NOTE: РћР±СЂР°Р±РѕС‚С‡РёРє РґР»СЏ РІРѕР·РІСЂР°С‚Р° РІ Р»РёС‡РЅС‹Р№ РєР°Р±РёРЅРµС‚
    if data == 'my_back':
        await my_cmd(update, context)
        return
    
    # NOTE: РћР±СЂР°Р±РѕС‚С‡РёРє РґР»СЏ РёР·РјРµРЅРµРЅРёСЏ СЃС‚Р°С‚СѓСЃР° СЂРµР»РёР·Р° Р°СЂС‚РёСЃС‚РѕРј (РѕРєРЅРѕ РІС‹Р±РѕСЂР° СЃС‚Р°С‚СѓСЃРѕРІ РґР»СЏ РјРѕРґРµСЂР°С†РёРё)
    if data.startswith('delete_release_'):
        # РњСЏРіРєРѕРµ СѓРґР°Р»РµРЅРёРµ СЂРµР»РёР·Р° РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј (РїРѕРјРµС‚РєР°, Р±РµР· С„РёР·РёС‡РµСЃРєРѕРіРѕ СѓРґР°Р»РµРЅРёСЏ)
        parts = data.split('_', 3)  # delete_release_userid_idx
        if len(parts) >= 4:
            user_id = parts[2]
            try:
                rel_idx = int(parts[3])
            except ValueError:
                await update.callback_query.answer('вќЊ РћС€РёР±РєР° СѓРґР°Р»РµРЅРёСЏ', show_alert=True)
                return
            
            if user_id in db and rel_idx < len(db[user_id]):
                rel = db[user_id][rel_idx]
                
                # РџСЂРѕРІРµСЂСЏРµРј С‡С‚Рѕ СЂРµР»РёР· РµС‰С‘ РЅРµ СѓРґР°Р»РµРЅ
                if rel.get('user_deleted'):
                    await update.callback_query.answer('вњ“ Р РµР»РёР· СѓР¶Рµ СѓРґР°Р»РµРЅ', show_alert=True)
                    return
                
                # РџРѕРјРµС‡Р°РµРј РєР°Рє СѓРґР°Р»С‘РЅРЅС‹Р№ РїРѕР»СЊР·РѕРІР°С‚РµР»РµРј, РЅРѕ РќР• СѓРґР°Р»СЏРµРј РёР· db
                rel['user_deleted'] = True
                rel['deleted_at'] = datetime.now().isoformat()
                rel_name = rel.get('name', 'Р РµР»РёР·')
                artist_name = rel.get('nick', 'РђСЂС‚РёСЃС‚')
                rel_type = rel.get('type', 'Р РµР»РёР·')
                rel_date = rel.get('date', 'вЂ”')
                rel_status = rel.get('status', STATUS_ON_UPLOAD)
                save_db(db)
                
                # РЈРІРµРґРѕРјР»СЏРµРј РјРѕРґРµСЂР°С†РёСЋ
                try:
                    notification_text = (
                        f"рџ—‘пёЏ <b>Р Р•Р›РР— РЈР”РђР›Р•Рќ РђР РўРРЎРўРћРњ</b>\n\n"
                        f"рџЋµ <b>{escape_html(rel_name)}</b>\n"
                        f"рџ‘¤ РђСЂС‚РёСЃС‚: {escape_html(artist_name)}\n"
                        f"рџ“ќ РўРёРї: {escape_html(rel_type)}\n"
                        f"рџ“… Р”Р°С‚Р°: {escape_html(rel_date)}\n"
                        f"рџ“Љ РЎС‚Р°С‚СѓСЃ Р±С‹Р»: {rel_status}\n\n"
                        f"рџ’Ў Р”Р»СЏ РїРѕР»РЅРѕРіРѕ СѓРґР°Р»РµРЅРёСЏ СЃ РїР»Р°С‚С„РѕСЂРј СЃРІСЏР¶РёС‚РµСЃСЊ СЃ CEO @kazumaiq"
                    )
                    await context.bot.send_message(
                        chat_id=MODERATION_CHAT_ID,
                        text=notification_text,
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё РІ РјРѕРґРµСЂР°С†РёСЋ: {e}")
                
                # РЈРІРµРґРѕРјР»СЏРµРј Р°СЂС‚РёСЃС‚Р°
                try:
                    artist_msg = (
                        f"вњ… <b>Р РµР»РёР· СѓРґР°Р»РµРЅ</b>\n\n"
                        f"рџЋµ {escape_html(rel_name)}\n\n"
                        f"<i>Р РµР»РёР· СѓРґР°Р»РµРЅ РёР· РІР°С€РµРіРѕ РєР°Р±РёРЅРµС‚Р°.</i>\n"
                        f"<i>Р”Р»СЏ РїРѕР»РЅРѕРіРѕ СѓРґР°Р»РµРЅРёСЏ СЃРѕ РІСЃРµС… РїР»РѕС‰Р°РґРѕРє:</i>\n"
                        f"<i>@kazumaiq</i>"
                    )
                    await context.bot.send_message(
                        int(user_id),
                        artist_msg,
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё Р°СЂС‚РёСЃС‚Сѓ: {e}")
                
                await update.callback_query.answer('вњ… Р РµР»РёР· СѓРґР°Р»РµРЅ', show_alert=False)
                # РћР±РЅРѕРІР»СЏРµРј РєР°Р±РёРЅРµС‚
                await my_cmd(update, context)
            else:
                await update.callback_query.answer('вќЊ Р РµР»РёР· РЅРµ РЅР°Р№РґРµРЅ', show_alert=True)
        return

    if data.startswith("admin_stats_page_"):
        m = re.match(r"^admin_stats_page_(\d+)$", data)
        if not m:
            return
        page = int(m.group(1))
        text, keyboard = _render_admin_stats_page(page)
        await safe_edit(query, text, reply_markup=keyboard)
        return
        
    if data == 'pending_list':
        await pending_releases_list(update, context)
        return
        
    if data == 'all_releases':
        await all_releases_list(update, context)
        return
        
    if data == 'cleanup_db':
        await cleanup_database(update, context)
        return
        
    if data == 'admin_back':
        await admin_panel(update, context)
        return
        
    if data == 'broadcast_menu':
        await broadcast_menu(update, context)
        return
        
    if data == 'confirm_cleanbase':
        await cleanbase_cmd(update, context)
        return
        
    if data == 'cleanbase_confirm':
        await cleanbase_confirm(update, context)
        return

    # РџРµСЂРµС…РѕРґС‹ РІ Р°РЅРєРµС‚Рµ
    if data == "subname_skip":
        user_data[user_id]["subname"] = "."
        # РџСЂРѕРїСѓСЃС‚РёС‚СЊ subname -> СЃСЂР°Р·Сѓ СЃРїСЂР°С€РёРІР°РµРј РїСЂРѕ РЅР°Р»РёС‡РёРµ СЃР»РѕРІ (РјРёРЅРёРјРёР·РёСЂРѕРІР°РЅРЅС‹Р№ РїРѕС‚РѕРє Р±РµР· UPC/ISRC)
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Р”Р°", callback_data="lyrics_yes"),
                    InlineKeyboardButton("РќРµС‚, СЌС‚Рѕ РёРЅСЃС‚СЂСѓРјРµРЅС‚Р°Р»", callback_data="lyrics_no"),
                ]
            ]
        )
        await safe_send(query.message, f"{WINTER_EMOJIS['warning']} <b>Р•СЃС‚СЊ Р»Рё СЃР»РѕРІР° РІ СЂРµР»РёР·Рµ?</b>", keyboard)
        return HAS_LYRICS

    if data == "lyrics_yes":
        user_data[user_id]["has_lyrics"] = "Р”Р°"
        await safe_send(query.message, f"{WINTER_EMOJIS['star']} <b>Ник исполнителя</b>\nℹ️ Это сценическое имя артиста, которое будет отображаться на стриминговых сервисах.\nПример: MAKIZM")
        return NICK

    if data == "lyrics_no":
        user_data[user_id]["has_lyrics"] = "РќРµС‚, СЌС‚Рѕ РёРЅСЃС‚СЂСѓРјРµРЅС‚Р°Р»"
        await safe_send(query.message, f"{WINTER_EMOJIS['star']} <b>Ник исполнителя</b>\nℹ️ Это сценическое имя артиста, которое будет отображаться на стриминговых сервисах.\nПример: MAKIZM")
        return NICK

    # РџСЂРѕРјРѕ-С‚РµРєСЃС‚: РІС‹Р±РѕСЂ С‚РёРїР° РїСЂРѕРµРєС‚Р°
    if data == 'promo_project_solo':
        p = user_data.setdefault(user_id, {}).setdefault('promo', {})
        p['project_type'] = 'solo'
        await query.edit_message_text("РќР°Р·РІР°РЅРёРµ СЂРµР»РёР·Р°:", parse_mode=ParseMode.HTML)
        return PROMO_RELEASE_NAME

    if data == 'promo_project_feat':
        p = user_data.setdefault(user_id, {}).setdefault('promo', {})
        p['project_type'] = 'feat'
        await query.edit_message_text("РќР°Р·РІР°РЅРёРµ СЂРµР»РёР·Р°:", parse_mode=ParseMode.HTML)
        return PROMO_RELEASE_NAME

    # РџСЂРѕРјРѕ-С‚РµРєСЃС‚: РІС‹Р±РѕСЂ С‚РёРїР° СЂРµР»РёР·Р°
    if data == 'promo_kind_single':
        p = user_data.setdefault(user_id, {}).setdefault('promo', {})
        p['release_kind'] = 'СЃРёРЅРіР»'
        await query.edit_message_text("Р–Р°РЅСЂ (РѕСЃРЅРѕРІРЅРѕР№):", parse_mode=ParseMode.HTML)
        return PROMO_GENRE_MAIN

    if data == 'promo_kind_ep':
        p = user_data.setdefault(user_id, {}).setdefault('promo', {})
        p['release_kind'] = 'EP'
        await query.edit_message_text("Р–Р°РЅСЂ (РѕСЃРЅРѕРІРЅРѕР№):", parse_mode=ParseMode.HTML)
        return PROMO_GENRE_MAIN

    if data == 'promo_kind_album':
        p = user_data.setdefault(user_id, {}).setdefault('promo', {})
        p['release_kind'] = 'Р°Р»СЊР±РѕРј'
        await query.edit_message_text("Р–Р°РЅСЂ (РѕСЃРЅРѕРІРЅРѕР№):", parse_mode=ParseMode.HTML)
        return PROMO_GENRE_MAIN

    # РџСЂРѕРјРѕ-С‚РµРєСЃС‚: РІС‹Р±РѕСЂ РІРѕРєР°Р»Р°
    if data == 'promo_vocal_no':
        p = user_data.setdefault(user_id, {}).setdefault('promo', {})
        p['vocal'] = 'instrumental'
        await query.edit_message_text("Р­РјРѕС†РёСЏ (С‡С‚Рѕ РґРѕР»Р¶РµРЅ РїРѕС‡СѓРІСЃС‚РІРѕРІР°С‚СЊ СЃР»СѓС€Р°С‚РµР»СЊ):", parse_mode=ParseMode.HTML)
        return PROMO_EMOTION

    if data == 'promo_vocal_male':
        p = user_data.setdefault(user_id, {}).setdefault('promo', {})
        p['vocal'] = 'male'
        await query.edit_message_text("Р­РјРѕС†РёСЏ (С‡С‚Рѕ РґРѕР»Р¶РµРЅ РїРѕС‡СѓРІСЃС‚РІРѕРІР°С‚СЊ СЃР»СѓС€Р°С‚РµР»СЊ):", parse_mode=ParseMode.HTML)
        return PROMO_EMOTION

    if data == 'promo_vocal_female':
        p = user_data.setdefault(user_id, {}).setdefault('promo', {})
        p['vocal'] = 'female'
        await query.edit_message_text("Р­РјРѕС†РёСЏ (С‡С‚Рѕ РґРѕР»Р¶РµРЅ РїРѕС‡СѓРІСЃС‚РІРѕРІР°С‚СЊ СЃР»СѓС€Р°С‚РµР»СЊ):", parse_mode=ParseMode.HTML)
        return PROMO_EMOTION

    # removed snippet_auto/snippet_manual flow: СЃСЂР°Р·Сѓ РїРµСЂРµС…РѕРґРёРј Рє NICK

# === РџРћР›РЇ ===
async def name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    # СЃРѕС…СЂР°РЅСЏРµРј РїСЂРµРґС‹РґСѓС‰РµРµ Р·РЅР°С‡РµРЅРёРµ РІ РёСЃС‚РѕСЂРёСЋ
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('name', user_data.get(user_id, {}).get('name')))
    user_data[user_id]['name'] = clean(update.message.text)
    save_draft_for_user(user_id)
    # РќРѕРІС‹Р№ Р±Р»РѕРє: sub-name
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("РџСЂРѕРїСѓСЃС‚РёС‚СЊ", callback_data="subname_skip")]])
    await safe_send(update.message, f"{WINTER_EMOJIS['star']} <b>Саб-название (если нет, отправьте точку \".\")</b>\nℹ️ Саб-название - это дополнительная подпись к названию релиза. Пример: Remix, Slowed, Instrumental, Extended Mix.\nЕсли не нужно - нажмите «Пропустить» или отправьте точку '.'", keyboard)
    return SUBNAME


async def subname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    txt = clean(update.message.text)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('subname', user_data.get(user_id, {}).get('subname')))
    user_data[user_id]["subname"] = txt if txt else "."
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Р”Р°", callback_data="lyrics_yes"),
                InlineKeyboardButton("РќРµС‚, СЌС‚Рѕ РёРЅСЃС‚СЂСѓРјРµРЅС‚Р°Р»", callback_data="lyrics_no"),
            ]
        ]
    )
    await safe_send(update.message, f"{WINTER_EMOJIS['warning']} <b>Р•СЃС‚СЊ Р»Рё СЃР»РѕРІР° РІ СЂРµР»РёР·Рµ?</b>", keyboard)
    return HAS_LYRICS


async def upc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('upc', user_data.get(user_id, {}).get('upc')))
    user_data[user_id]["upc"] = clean(update.message.text) or "."
    save_draft_for_user(user_id)
    await safe_send(update.message, f"{WINTER_EMOJIS['notes']} <b>ISRC</b>\nР•СЃР»Рё РЅРµС‚ вЂ” РѕС‚РїСЂР°РІСЊС‚Рµ '.'")
    return ISRC


async def isrc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('isrc', user_data.get(user_id, {}).get('isrc')))
    user_data[user_id]["isrc"] = clean(update.message.text) or "."
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Р”Р°", callback_data="lyrics_yes"),
                InlineKeyboardButton("РќРµС‚, СЌС‚Рѕ РёРЅСЃС‚СЂСѓРјРµРЅС‚Р°Р»", callback_data="lyrics_no"),
            ]
        ]
    )
    await safe_send(update.message, f"{WINTER_EMOJIS['warning']} <b>Р•СЃС‚СЊ Р»Рё СЃР»РѕРІР° РІ СЂРµР»РёР·Рµ?</b>", keyboard)
    return HAS_LYRICS


async def has_lyrics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # state handled by callback (lyrics_yes/lyrics_no)
    return HAS_LYRICS


async def snippet_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # state handled by callback (snippet_auto/snippet_manual)
    return SNIPPET_MODE

async def nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('nick', user_data.get(user_id, {}).get('nick')))
    user_data[user_id]["nick"] = clean(update.message.text)
    save_draft_for_user(user_id)
    await safe_send(update.message, f"{WINTER_EMOJIS['star']} <b>ФИО исполнителя</b>\nℹ️ Укажите настоящее имя исполнителя. Это требуется для документов и авторских прав.\nПример: Иванов Иван, Петров Пётр")
    return FIO


async def fio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('fio', user_data.get(user_id, {}).get('fio')))
    user_data[user_id]["fio"] = clean(update.message.text)
    save_draft_for_user(user_id)
    min_days = 3 if user_data[user_id]["type"] == "СЃРёРЅРіР»" else 7
    await safe_send(update.message, f"{WINTER_EMOJIS['calendar']} <b>Дата релиза в формате ДД.ММ.ГГГГ</b>\nℹ️ Укажите дату выхода релиза на площадках минимум за 4 дня.")
    return DATE

async def date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    text = update.message.text.strip()
    if not all(part.isdigit() for part in text.split('.') if part):
        await safe_send(update.message, f"{WINTER_EMOJIS['cross']} РќРµРІРµСЂРЅС‹Р№ С„РѕСЂРјР°С‚ РґР°С‚С‹! РСЃРїРѕР»СЊР·СѓР№С‚Рµ: Р”Р”.РњРњ.Р“Р“Р“Р“")
        return DATE
    try:
        date_obj = datetime.strptime(text, "%d.%m.%Y")
        min_days = 3 if user_data[user_id]['type'] == 'СЃРёРЅРіР»' else 7
        if date_obj < datetime.now() + timedelta(days=min_days):
            await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Р”Р°С‚Р° РґРѕР»Р¶РЅР° Р±С‹С‚СЊ РјРёРЅРёРјСѓРј С‡РµСЂРµР· {min_days} РґРЅРµР№!")
            return DATE
        user_data.setdefault(user_id, {}).setdefault('_history', []).append(('date', user_data.get(user_id, {}).get('date')))
        user_data[user_id]['date'] = text
        save_draft_for_user(user_id)
        await safe_send(update.message, f"{WINTER_EMOJIS['music']} <b>Версия релиза</b>\nℹ️ Если это обычная версия трека - напишите '-'. Если другая версия: Remix, Slowed, Sped Up, Instrumental.")
        return VERSION
    except ValueError:
        await safe_send(update.message, f"{WINTER_EMOJIS['cross']} РќРµРІРµСЂРЅС‹Р№ С„РѕСЂРјР°С‚ РґР°С‚С‹! РџСЂРёРјРµСЂ: 25.12.2025")
        return DATE

async def version(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    ver = clean(update.message.text)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('version', user_data.get(user_id, {}).get('version')))
    user_data[user_id]['version'] = ver if ver != '-' else 'РћСЂРёРіРёРЅР°Р»'
    save_draft_for_user(user_id)
    await safe_send(update.message, f"{WINTER_EMOJIS['notes']} <b>Жанр</b>\nℹ️ Укажите основной жанр трека. Примеры: Phonk, Brazilian Funk, Hip-Hop, Trap, EDM.")
    return GENRE

async def genre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('genre', user_data.get(user_id, {}).get('genre')))
    user_data[user_id]['genre'] = clean(update.message.text)
    save_draft_for_user(user_id)
    await safe_send(update.message,
        f"{WINTER_EMOJIS['gift']} <b>Ссылка на файлы (http/https)</b>\n"
        "ℹ️ В ссылке на Яндекс/Google Диск должна быть папка со следующими файлами:\n"
        "1. Обложка релиза в формате JPG 3000x3000\n"
        "2. Трек в формате WAV (44100Hz, 16 или 24 bit)\n"
        "3. Скриншот(ы) проекта как доказательство авторства"
    )
    return LINK

async def link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('link', user_data.get(user_id, {}).get('link')))
    user_data[user_id]['link'] = update.message.text.strip()
    save_draft_for_user(user_id)
    # РџСЂРѕСЃС‚РµР№С€Р°СЏ РїСЂРѕРІРµСЂРєР°: СѓР±РµРґРёРјСЃСЏ, С‡С‚Рѕ СЌС‚Рѕ РІС‹РіР»СЏРґСЏС‰Р°СЏ РєР°Рє URL СЃС‚СЂРѕРєР°
    url = user_data[user_id]['link']
    if url and url != ".":
        if not _looks_like_url(url):
            await safe_send(update.message, f"{WINTER_EMOJIS['cross']} РџРѕС…РѕР¶Рµ, РІС‹ РїСЂРёСЃР»Р°Р»Рё РЅРµ СЃСЃС‹Р»РєСѓ. РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РѕС‚РїСЂР°РІСЊС‚Рµ РєРѕСЂСЂРµРєС‚РЅС‹Р№ URL (РЅР°С‡РёРЅР°СЋС‰РёР№СЃСЏ СЃ http:// РёР»Рё https://)")
            return LINK
        # Р”РѕРї. РїРѕРґСЃРєР°Р·РєР°: РµСЃР»Рё СЌС‚Рѕ РЅРµ РѕС‡РµРІРёРґРЅС‹Р№ Google Drive URL, РЅРµ Р±Р»РѕРєРёСЂСѓРµРј вЂ” С‚РѕР»СЊРєРѕ РїРѕРґСЃРєР°Р·РєР°
        if not _looks_like_drive_link(url):
            await safe_send(update.message, f"{WINTER_EMOJIS['warning']} РџСЂРёРјРµС‡Р°РЅРёРµ: СЂРµРєРѕРјРµРЅРґСѓРµС‚СЃСЏ РїСЂРµРґРѕСЃС‚Р°РІРёС‚СЊ СЃСЃС‹Р»РєСѓ СЃ Google Drive (drive.google.com), РЅРѕ РїСЂРёРЅРёРјР°РµС‚СЃСЏ Р»СЋР±РѕР№ РєРѕСЂСЂРµРєС‚РЅС‹Р№ URL.")
        # РџРѕРїС‹С‚РєР° Р±С‹СЃС‚СЂРѕРіРѕ HEAD-Р·Р°РїСЂРѕСЃР° РґР»СЏ Р»РѕРіРёСЂРѕРІР°РЅРёСЏ; РЅРµ РїРѕРєР°Р·С‹РІР°РµРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ Р»РѕР¶РЅС‹Рµ РїСЂРµРґСѓРїСЂРµР¶РґРµРЅРёСЏ РїСЂРё РЅРµСѓРґР°С‡Рµ
        try:
            if httpx is not None:
                async def _check():
                    async with httpx.AsyncClient(timeout=5) as client:
                        await client.head(url, follow_redirects=True)
                await _check()
        except Exception:
            # Р»РѕРіРёСЂСѓРµРј РІРЅСѓС‚СЂРµРЅРЅРµ, РЅРѕ РЅРµ РїСЂРµРґСѓРїСЂРµР¶РґР°РµРј РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ РёР·-Р·Р° Р»РѕР¶РЅС‹С… СЃСЂР°Р±Р°С‚С‹РІР°РЅРёР№
            print(f"вљ пёЏ link validation HEAD failed for {url}")
    # РЎРЅР°С‡Р°Р»Р° СЃРїСЂР°С€РёРІР°РµРј СЃСЃС‹Р»РєСѓ РЅР° РєР°СЂС‚РѕС‡РєСѓ РјСѓР·С‹РєР°РЅС‚Р° РІ РЇРЅРґРµРєСЃ РњСѓР·С‹РєРµ
    await safe_send(update.message, f"{WINTER_EMOJIS['notes']} <b>Ссылка на карточку артиста в Яндекс Музыке</b>\nℹ️ Отправьте ссылку на существующую карточку артиста. Если карточки нет - нажмите кнопку \"Создать новую карточку\" или отправьте '.'")
    return YANDEX


async def yandex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('yandex', user_data.get(user_id, {}).get('yandex')))
    user_data[user_id]['yandex'] = update.message.text.strip() or "."
    save_draft_for_user(user_id)
    url = user_data[user_id]['yandex']
    if url and url != ".":
        if not _looks_like_url(url):
            await safe_send(update.message, f"{WINTER_EMOJIS['cross']} РџРѕС…РѕР¶Рµ, РІС‹ РїСЂРёСЃР»Р°Р»Рё РЅРµ СЃСЃС‹Р»РєСѓ. РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РѕС‚РїСЂР°РІСЊС‚Рµ РєРѕСЂСЂРµРєС‚РЅС‹Р№ URL (РЅР°С‡РёРЅР°СЋС‰РёР№СЃСЏ СЃ http:// РёР»Рё https://)")
            return YANDEX
        if not _looks_like_yandex_music_link(url):
            await safe_send(update.message, f"{WINTER_EMOJIS['warning']} РџСЂРёРјРµС‡Р°РЅРёРµ: СЂРµРєРѕРјРµРЅРґСѓРµС‚СЃСЏ РїСЂРёСЃР»Р°С‚СЊ СЃСЃС‹Р»РєСѓ СЃ Yandex Music (music.yandex.ru), РЅРѕ РїСЂРёРЅРёРјР°РµС‚СЃСЏ Р»СЋР±РѕР№ РєРѕСЂСЂРµРєС‚РЅС‹Р№ URL.")
        try:
            if httpx is not None:
                async with httpx.AsyncClient(timeout=5) as client:
                    await client.head(url, follow_redirects=True)
        except Exception:
            print(f"вљ пёЏ yandex validation HEAD failed for {url}")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Р”Р°", "check"), callback_data='mat_yes')],
        [InlineKeyboardButton(winter_text("РќРµС‚", "cross"), callback_data='mat_no')]
    ])
    await safe_send(update.message, f"{WINTER_EMOJIS['warning']} <b>Есть ли ненормативная лексика?</b>\nℹ️ Выберите \"Да\", если в тексте трека присутствует мат. Если трек чистый - выберите \"Нет\".", keyboard)
    return MAT

async def mat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    user_data[user_id]['mat'] = 'Р”Р°' if query.data == 'mat_yes' else 'РќРµС‚'
    await safe_edit(query, f"{WINTER_EMOJIS['sparkles']} <b>Промо-текст (или точка \".\")</b>\nℹ️ Это описание релиза для редакторов стриминговых сервисов. Кратко опишите стиль трека, атмосферу и для каких плейлистов он подходит.")
    return PROMO

async def promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('promo', user_data.get(user_id, {}).get('promo')))
    user_data[user_id]['promo'] = clean(update.message.text)
    save_draft_for_user(user_id)
    await safe_send(update.message, f"{WINTER_EMOJIS['comment']} <b>Комментарий (или точка \".\")</b>\nℹ️ Дополнительная информация для модераторов. Можно оставить пустым.")
    return COMMENT

async def comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('comment', user_data.get(user_id, {}).get('comment')))
    user_data[user_id]['comment'] = clean(update.message.text)
    save_draft_for_user(user_id)
    if user_data[user_id]["type"] == "Р°Р»СЊР±РѕРј":
        await safe_send(update.message, f"{WINTER_EMOJIS['list']} <b>Tracklist</b>\nРџРµСЂРµС‡РёСЃР»РёС‚Рµ С‚СЂРµРєРё РѕРґРЅРѕР№ СЃС‚СЂРѕРєРѕР№ РёР»Рё СЃРїРёСЃРєРѕРј.")
        return TRACKLIST
    await safe_send(update.message, f"{WINTER_EMOJIS['telegram']} <b>Контакт Telegram для связи (@username):</b>\nℹ️ Укажите ваш Telegram username для связи с менеджером.\n@username (можно несколько через пробел)")
    return TG


async def tracklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('tracklist', user_data.get(user_id, {}).get('tracklist')))
    user_data[user_id]["tracklist"] = clean(update.message.text)
    save_draft_for_user(user_id)
    await safe_send(update.message, f"{WINTER_EMOJIS['telegram']} <b>Контакт Telegram для связи (@username):</b>\nℹ️ Укажите ваш Telegram username для связи с менеджером.\n@username (можно несколько через пробел)")
    return TG


async def tg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('tg', user_data.get(user_id, {}).get('tg')))
    user_data[user_id]["tg"] = update.message.text.strip()
    save_draft_for_user(user_id)
    await show_confirm(update.message, context)
    return CONFIRM

async def show_confirm(message, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(message.from_user.id)
    data = user_data[user_id]
    text = f"{WINTER_EMOJIS['snowflake']} <b>РџР РћР’Р•Р Р¬РўР• РђРќРљР•РўРЈ:</b>\n\n"
    order = [
        ("РўРёРї", "type"),
        ("РќР°Р·РІР°РЅРёРµ", "name"),
        ("РЎР°Р±-РЅР°Р·РІР°РЅРёРµ", "subname"),
        ("Р•СЃС‚СЊ СЃР»РѕРІР°", "has_lyrics"),
        ("РќРёРє", "nick"),
        ("Р¤РРћ", "fio"),
        ("Р”Р°С‚Р°", "date"),
        ("Р’РµСЂСЃРёСЏ", "version"),
        ("Р–Р°РЅСЂ", "genre"),
        ("РЎСЃС‹Р»РєР°", "link"),
        ("РЇРЅРґРµРєСЃ РњСѓР·С‹РєР°", "yandex"),
        ("РњР°С‚", "mat"),
        ("РџСЂРѕРјРѕ", "promo"),
        ("РљРѕРјРјРµРЅС‚Р°СЂРёР№", "comment"),
        ("Tracklist", "tracklist"),
        ("Tg", "tg"),
    ]
    for label, key in order:
        if key in data and data.get(key) is not None:
            text += f"вЂў <b>{escape_html(label)}:</b> {escape_html(data.get(key))}\n"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("РћС‚РїСЂР°РІРёС‚СЊ", "check"), callback_data='send')],
        [InlineKeyboardButton(winter_text("РќР°Р·Р°Рґ", "cross"), callback_data='main')]
    ])
    await safe_send(message, text, keyboard)

# === РћРўРџР РђР’РљРђ Р’ РњРћР”Р•Р РђР¦РР® ===
async def show_release_warning(query, context: ContextTypes.DEFAULT_TYPE):
    warning_text = (
        "вљ пёЏ <b>РџРµСЂРµРґ РѕС‚РїСЂР°РІРєРѕР№ СЂРµР»РёР·Р° РІРЅРёРјР°С‚РµР»СЊРЅРѕ РїСЂРѕРІРµСЂСЊС‚Рµ Р°РЅРєРµС‚Сѓ.</b>\n\n"
        "РћС‚РїСЂР°РІР»СЏСЏ СЂРµР»РёР· РІ РјРѕРґРµСЂР°С†РёСЋ, РІС‹ РїРѕРґС‚РІРµСЂР¶РґР°РµС‚Рµ, С‡С‚Рѕ:\n"
        "вЂў РІСЃРµ РґР°РЅРЅС‹Рµ СѓРєР°Р·Р°РЅС‹ РєРѕСЂСЂРµРєС‚РЅРѕ\n"
        "вЂў С„Р°Р№Р»С‹ СЃРѕРѕС‚РІРµС‚СЃС‚РІСѓСЋС‚ С‚СЂРµР±РѕРІР°РЅРёСЏРј\n"
        "вЂў РѕР±Р»РѕР¶РєР° СЃРѕРѕС‚РІРµС‚СЃС‚РІСѓРµС‚ РїСЂР°РІРёР»Р°Рј РїР»РѕС‰Р°РґРѕРє\n"
        "вЂў Сѓ РІР°СЃ РµСЃС‚СЊ РїСЂР°РІР° РЅР° РёСЃРїРѕР»СЊР·РѕРІР°РЅРёРµ РІСЃРµС… РјР°С‚РµСЂРёР°Р»РѕРІ СЂРµР»РёР·Р°\n\n"
        "Р•СЃР»Рё РґР°РЅРЅС‹Рµ СѓРєР°Р·Р°РЅС‹ РЅРµРІРµСЂРЅРѕ РёР»Рё С„Р°Р№Р»С‹ РЅРµ СЃРѕРѕС‚РІРµС‚СЃС‚РІСѓСЋС‚ С‚СЂРµР±РѕРІР°РЅРёСЏРј:\n"
        "вЂў СЂРµР»РёР· РјРѕР¶РµС‚ Р±С‹С‚СЊ РѕС‚РєР»РѕРЅС‘РЅ РјРѕРґРµСЂР°С†РёРµР№\n"
        "вЂў СЂРµР»РёР· РјРѕР¶РµС‚ Р±С‹С‚СЊ СЃРЅСЏС‚ СЃ РѕС‚РіСЂСѓР·РєРё\n"
        "вЂў СЂРµР»РёР· РјРѕР¶РµС‚ РЅРµ РІС‹Р№С‚Рё РІ СѓРєР°Р·Р°РЅРЅСѓСЋ РґР°С‚Сѓ\n"
        "вЂў СЂРµР»РёР· РјРѕР¶РµС‚ Р±С‹С‚СЊ СѓРґР°Р»С‘РЅ РґРѕ РёСЃРїСЂР°РІР»РµРЅРёСЏ РѕС€РёР±РѕРє\n\n"
        "РљРѕРјР°РЅРґР° CXRNER MUSIC РЅРµ РЅРµСЃС‘С‚ РѕС‚РІРµС‚СЃС‚РІРµРЅРЅРѕСЃС‚Рё Р·Р° Р·Р°РґРµСЂР¶РєСѓ РІС‹С…РѕРґР° СЂРµР»РёР·Р°, "
        "РµСЃР»Рё Р°РЅРєРµС‚Р° Р·Р°РїРѕР»РЅРµРЅР° РЅРµРІРµСЂРЅРѕ РёР»Рё С„Р°Р№Р»С‹ РЅРµ СЃРѕРѕС‚РІРµС‚СЃС‚РІСѓСЋС‚ С‚СЂРµР±РѕРІР°РЅРёСЏРј.\n\n"
        "РћС‚РїСЂР°РІР»СЏСЏ СЂРµР»РёР·, РІС‹ РїРѕРґС‚РІРµСЂР¶РґР°РµС‚Рµ СЃРѕРіР»Р°СЃРёРµ СЃ РїСЂР°РІРёР»Р°РјРё Р·Р°РіСЂСѓР·РєРё Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєРёРј СЃРѕРіР»Р°С€РµРЅРёРµРј."
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("вњ… РџРѕРґС‚РІРµСЂРґРёС‚СЊ Рё РѕС‚РїСЂР°РІРёС‚СЊ", callback_data='send_confirm')],
        [InlineKeyboardButton("вќЊ РћС‚РјРµРЅР°", callback_data='send_cancel')]
    ])
    await safe_edit(query, warning_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

async def send_moderation(query, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(query.from_user.id)
    data = user_data[user_id]
    try:
        await _submit_release_to_moderation(context, query.from_user, user_id, data)
    except Exception as e:
        await safe_edit(query, f"{WINTER_EMOJIS['cross']} РћС€РёР±РєР°: {e}")
        return REPORT

    await safe_edit(query, f"{WINTER_EMOJIS['check']} <b>РђРЅРєРµС‚Р° РѕС‚РїСЂР°РІР»РµРЅР°!</b>\nРћР¶РёРґР°Р№С‚Рµ 12вЂ“72 С‡Р°СЃР°.", parse_mode=ParseMode.HTML)

# === РњРћР”Р•Р РђР¦РРЇ (РљРќРћРџРљР РќР• Р”РћР›Р–РќР« Р—РђРўРР РђРўР¬ РђРќРљР•РўРЈ) ===
# РўСЂРµР±РѕРІР°РЅРёРµ: РёСЃС…РѕРґРЅС‹Р№ С‚РµРєСЃС‚ Р°РЅРєРµС‚С‹ РѕСЃС‚Р°С‘С‚СЃСЏ РЅРµРёР·РјРµРЅРЅС‹Рј; РїРѕСЃР»Рµ РЅР°Р¶Р°С‚РёСЏ РєРЅРѕРїРєРё РїРѕСЏРІР»СЏРµС‚СЃСЏ РґРѕРї.С‚РµРєСЃС‚ СЃРЅРёР·Сѓ,
# Р° РЅРµ вЂњР°РЅРєРµС‚Р° РїСЂРѕРїР°РґР°РµС‚вЂќ. РџРѕСЌС‚РѕРјСѓ:
# - РЅРµ СЂРµРґР°РєС‚РёСЂСѓРµРј С‚РµРєСЃС‚ РїСЂРё РІС‹Р±РѕСЂРµ РґРµР№СЃС‚РІРёР№ (С‚РѕР»СЊРєРѕ РѕС‚РІРµС‡Р°РµРј + С€Р»С‘Рј РѕС‚РґРµР»СЊРЅС‹Рµ СЃРѕРѕР±С‰РµРЅРёСЏ/РєРЅРѕРїРєРё)
# - С„РёРЅР°Р»СЊРЅРѕ: СѓР±РёСЂР°РµРј РєР»Р°РІРёР°С‚СѓСЂСѓ Сѓ РёСЃС…РѕРґРЅРѕРіРѕ СЃРѕРѕР±С‰РµРЅРёСЏ Рё СЂРµРґР°РєС‚РёСЂСѓРµРј С‚РµРєСЃС‚ = original + status_append.


# === РњРћР”Р•Р РђР¦РРЇ (РљРќРћРџРљР РќР• Р”РћР›Р–РќР« Р—РђРўРР РђРўР¬ РђРќРљР•РўРЈ) ===
# РўСЂРµР±РѕРІР°РЅРёРµ: РёСЃС…РѕРґРЅС‹Р№ С‚РµРєСЃС‚ Р°РЅРєРµС‚С‹ РѕСЃС‚Р°С‘С‚СЃСЏ РЅРµРёР·РјРµРЅРЅС‹Рј; РїРѕСЃР»Рµ РЅР°Р¶Р°С‚РёСЏ РєРЅРѕРїРєРё РїРѕСЏРІР»СЏРµС‚СЃСЏ РґРѕРї.С‚РµРєСЃС‚ СЃРЅРёР·Сѓ,
# Р° РЅРµ "Р°РЅРєРµС‚Р° РїСЂРѕРїР°РґР°РµС‚". РџРѕСЌС‚РѕРјСѓ:
# - РЅРµ СЂРµРґР°РєС‚РёСЂСѓРµРј С‚РµРєСЃС‚ РїСЂРё РІС‹Р±РѕСЂРµ РґРµР№СЃС‚РІРёР№ (С‚РѕР»СЊРєРѕ РѕС‚РІРµС‡Р°РµРј + С€Р»С‘Рј РѕС‚РґРµР»СЊРЅС‹Рµ СЃРѕРѕР±С‰РµРЅРёСЏ/РєРЅРѕРїРєРё)
# - С„РёРЅР°Р»СЊРЅРѕ: СѓР±РёСЂР°РµРј РєР»Р°РІРёР°С‚СѓСЂСѓ Сѓ РёСЃС…РѕРґРЅРѕРіРѕ СЃРѕРѕР±С‰РµРЅРёСЏ Рё СЂРµРґР°РєС‚РёСЂСѓРµРј С‚РµРєСЃС‚ = original + status_append.


# === Р РЈР§РќРћР• РћРўРљР›РћРќР•РќРР• РђРќРљР•РўР« Р§Р•Р Р•Р— REPLY ===
# MANUAL_REJECT: РњРѕРґРµСЂР°С‚РѕСЂ РјРѕР¶РµС‚ РѕС‚РєР»РѕРЅРёС‚СЊ Р°РЅРєРµС‚Сѓ, РѕС‚РІРµС‚РёРІ РЅР° РµС‘ СЃРѕРѕР±С‰РµРЅРёРµ РІ С‡Р°С‚Рµ РјРѕРґРµСЂР°С†РёРё
async def manual_reject_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """РћР±СЂР°Р±РѕС‚С‡РёРє СЂСѓС‡РЅРѕРіРѕ РѕС‚РєР»РѕРЅРµРЅРёСЏ Р°РЅРєРµС‚С‹ С‡РµСЂРµР· reply РЅР° СЃРѕРѕР±С‰РµРЅРёРµ Р°РЅРєРµС‚С‹."""
    if not update.message or not update.message.reply_to_message:
        return
    if not is_moderation_chat(update.message.chat_id):
        return
    
    text = clean(update.message.text)
    
    # Р•СЃР»Рё СЌС‚Рѕ РїРѕС…РѕР¶Рµ РЅР° UPC (С‚РѕР»СЊРєРѕ С†РёС„СЂС‹, 10-14 СЃРёРјРІРѕР»РѕРІ) вЂ” РїСЂРѕРїСѓСЃРєР°РµРј, СЌС‚Рѕ РѕР±СЂР°Р±РѕС‚Р°РµС‚ add_upc_handler
    if text.isdigit() and 10 <= len(text) <= 14:
        # РџРѕРїСЂРѕР±СѓРµРј РґРѕР±Р°РІРёС‚СЊ UPC
        await add_upc_handler(update, context)
        return
    
    # MANUAL_REJECT: РџРѕР»СѓС‡Р°РµРј СЃРѕРѕР±С‰РµРЅРёРµ РЅР° РєРѕС‚РѕСЂРѕРµ РѕС‚РІРµС‚РёР»Рё
    replied_msg = update.message.reply_to_message
    replied_msg_id = replied_msg.message_id
    
    # MANUAL_REJECT: РС‰РµРј Р°РЅРєРµС‚Сѓ РІ Р‘Р” РїРѕ moderation_message_id РёР»Рё reject_instruction_message_id
    user_id = None
    idx = None
    for uid, releases in db.items():
        for idx_rel, rel in enumerate(releases):
            # РџСЂРѕРІРµСЂСЏРµРј РѕР±Р° С‚РёРїР° СЃРѕРѕР±С‰РµРЅРёР№:
            # 1. РћС‚РІРµС‚ РЅР° РёРЅСЃС‚СЂСѓРєС†РёРѕРЅРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ РѕС‚РєР»РѕРЅРµРЅРёСЏ (РЅРѕРІС‹Р№ СЃРїРѕСЃРѕР±)
            # 2. РћС‚РІРµС‚ РЅР° РёСЃС…РѕРґРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ Р°РЅРєРµС‚С‹ (СЃС‚Р°СЂС‹Р№ СЃРїРѕСЃРѕР± РґР»СЏ СЃРѕРІРјРµСЃС‚РёРјРѕСЃС‚Рё)
            if (rel.get('reject_instruction_message_id') == replied_msg_id or
                rel.get('moderation_message_id') == replied_msg_id):
                user_id = uid
                idx = idx_rel
                break
        if user_id:
            break
    
    if not user_id or idx is None:
        return  # РњРѕР»С‡Р°Р»РёРІРѕ РёРіРЅРѕСЂРёСЂСѓРµРј РѕР±С‹С‡РЅС‹Рµ СЃРѕРѕР±С‰РµРЅРёСЏ
    
    release = db[user_id][idx]
    
    # MANUAL_REJECT: Р‘РµСЂС‘Рј С‚РµРєСЃС‚ СЃРѕРѕР±С‰РµРЅРёСЏ РєР°Рє РїСЂРёС‡РёРЅСѓ
    reject_reason = clean(update.message.text)
    if not reject_reason:
        await update.message.reply_text("вќЊ РўРµРєСЃС‚ РїСЂРёС‡РёРЅС‹ РЅРµ РјРѕР¶РµС‚ Р±С‹С‚СЊ РїСѓСЃС‚С‹Рј.")
        return
    
    moderator_username = update.message.from_user.username or update.message.from_user.first_name
    
    # MANUAL_REJECT: РћР±РЅРѕРІР»СЏРµРј СЃС‚Р°С‚СѓСЃ РІ Р‘Р”
    old_status = release.get("status")
    release["status"] = STATUS_REJECTED
    release["reject_reason"] = reject_reason
    release["moderator"] = moderator_username
    release["moderation_time"] = datetime.now().isoformat()
    add_history_entry(user_id, idx, old_status, STATUS_REJECTED, update.message.from_user.id, moderator_username, reject_reason)
    save_db(db)
    update_moderation_record(user_id, idx, release)
    
    # MANUAL_REJECT: РЈРґР°Р»СЏРµРј РєРЅРѕРїРєРё Сѓ РёСЃС…РѕРґРЅРѕРіРѕ СЃРѕРѕР±С‰РµРЅРёСЏ Р°РЅРєРµС‚С‹ (С‚РѕР»СЊРєРѕ РµСЃР»Рё СЌС‚Рѕ Р±С‹Р» РѕС‚РІРµС‚ РЅР° РёСЃС…РѕРґРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ)
    if release.get('moderation_message_id') == replied_msg_id:
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=MODERATION_CHAT_ID,
                message_id=replied_msg_id,
                reply_markup=None
            )
        except Exception as e:
            print(f"РћС€РёР±РєР° РїСЂРё СѓРґР°Р»РµРЅРёРё РєРЅРѕРїРѕРє: {e}")
        
        reply_markup_to_preserve = None
    else:
        # Р•СЃР»Рё СЌС‚Рѕ Р±С‹Р» РѕС‚РІРµС‚ РЅР° РёРЅСЃС‚СЂСѓРєС†РёРѕРЅРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ, Р±РµСЂС‘Рј РєР»Р°РІРёР°С‚СѓСЂСѓ РёР· РёСЃС…РѕРґРЅРѕРіРѕ СЃРѕРѕР±С‰РµРЅРёСЏ
        moderation_msg_id = release.get('moderation_message_id')
        if moderation_msg_id:
            try:
                msg = await context.bot.get_file(moderation_msg_id)
                # РќР° СЃР°РјРѕРј РґРµР»Рµ get_file РЅРµ РІРµСЂРЅС‘С‚ message вЂ” РЅСѓР¶РЅРѕ edit_message_reply_markup РЅР° РёСЃС…РѕРґРЅРѕРµ
                await context.bot.edit_message_reply_markup(
                    chat_id=MODERATION_CHAT_ID,
                    message_id=moderation_msg_id,
                    reply_markup=None
                )
            except Exception as e:
                print(f"РћС€РёР±РєР° РїСЂРё СѓРґР°Р»РµРЅРёРё РєРЅРѕРїРѕРє РёР· РёСЃС…РѕРґРЅРѕРіРѕ СЃРѕРѕР±С‰РµРЅРёСЏ: {e}")
        reply_markup_to_preserve = None
    
    # MANUAL_REJECT: Р”РѕРїРёСЃС‹РІР°РµРј СЃС‚Р°С‚СѓСЃ Рє Р°РЅРєРµС‚Рµ
    original = release.get("moderation_original_text") or (replied_msg.text or "")
    moderation_msg_id = release.get('moderation_message_id')
    await _append_status_to_moderation_message(
        context,
        moderation_msg_id,
        original,
        STATUS_REJECTED,
        moderator_username=moderator_username,
        reason=reject_reason,
        reply_markup=None
    )
    
    # MANUAL_REJECT: РћС‚РїСЂР°РІР»СЏРµРј СѓРІРµРґРѕРјР»РµРЅРёРµ Р°СЂС‚РёСЃС‚Сѓ
    try:
        moderation_time = datetime.now().strftime("%d.%m.%Y РІ %H:%M")
        await context.bot.send_message(
            int(user_id),
            f"{WINTER_EMOJIS['cross']} <b>Р’РђРЁ Р Р•Р›РР— РћРўРљР›РћРќРЃРќ</b>\n\n"
            f"рџ“ќ <b>{escape_html(release.get('name', 'вЂ”'))}</b>\n"
            f"рџЋµ <i>РўРёРї:</i> {escape_html(release.get('type', 'вЂ”'))}\n"
            f"рџ“… <i>Р”Р°С‚Р° СЂРµР»РёР·Р°:</i> {escape_html(release.get('date', 'вЂ”'))}\n"
            f"рџ‘¤ <i>РђСЂС‚РёСЃС‚:</i> {escape_html(release.get('nick', 'вЂ”'))}\n"
            f"рџ•ђ <i>РћС‚РєР»РѕРЅРµРЅРѕ:</i> {escape_html(moderation_time)}\n"
            f"рџ‘ЁвЂЌрџ’ј <i>РњРѕРґРµСЂР°С‚РѕСЂ:</i> @{escape_html(moderator_username)}\n\n"
            f"вќЊ <b>РџСЂРёС‡РёРЅР°:</b>\n{escape_html(reject_reason)}\n\n"
            f"{WINTER_EMOJIS['sparkles']} РћС‚РїСЂР°РІСЊС‚Рµ СЂРµР»РёР· Р·Р°РЅРѕРІРѕ С‡РµСЂРµР· /start РїРѕСЃР»Рµ РёСЃРїСЂР°РІР»РµРЅРёР№.",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё СѓРІРµРґРѕРјР»РµРЅРёСЏ Р°СЂС‚РёСЃС‚Сѓ: {e}")
    
    await update.message.reply_text(f"{WINTER_EMOJIS['check']} Р РµР»РёР· РѕС‚РєР»РѕРЅС‘РЅ. РђСЂС‚РёСЃС‚ СѓРІРµРґРѕРјР»РµРЅ.")


async def add_upc_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """РћР±СЂР°Р±РѕС‚С‡РёРє РґРѕР±Р°РІР»РµРЅРёСЏ UPC РєРѕРґР° С‡РµСЂРµР· reply РЅР° СЃРѕРѕР±С‰РµРЅРёРµ Р°РЅРєРµС‚С‹ РёР»Рё РЅР° РёРЅСЃС‚СЂСѓРєС†РёРѕРЅРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ."""
    if not update.message or not update.message.reply_to_message:
        return
    if not is_moderation_chat(update.message.chat_id):
        return
    
    # РџРѕР»СѓС‡Р°РµРј UPC РёР· СЃРѕРѕР±С‰РµРЅРёСЏ
    upc_code = clean(update.message.text)
    if not upc_code:
        return  # РџСѓСЃС‚С‹Рµ СЃРѕРѕР±С‰РµРЅРёСЏ РёРіРЅРѕСЂРёСЂСѓРµРј
    
    # РџСЂРѕРІРµСЂСЏРµРј С‡С‚Рѕ СЌС‚Рѕ РІС‹РіР»СЏРґРёС‚ РєР°Рє UPC (РѕР±С‹С‡РЅРѕ 12-14 С†РёС„СЂ, РЅРµ РјРµРЅРµРµ 10)
    if not upc_code.isdigit() or len(upc_code) < 10:
        return  # РќРµ РїРѕС…РѕР¶Рµ РЅР° UPC, РёРіРЅРѕСЂРёСЂСѓРµРј
    
    # РџРѕР»СѓС‡Р°РµРј СЃРѕРѕР±С‰РµРЅРёРµ РЅР° РєРѕС‚РѕСЂРѕРµ РѕС‚РІРµС‚РёР»Рё
    replied_msg = update.message.reply_to_message
    replied_msg_id = replied_msg.message_id
    
    # РС‰РµРј Р°РЅРєРµС‚Сѓ РІ Р‘Р” РїРѕ moderation_message_id РёР»Рё РїРѕ upc_instruction_message_id
    user_id = None
    idx = None
    for uid, releases in db.items():
        for idx_rel, rel in enumerate(releases):
            # РџСЂРѕРІРµСЂСЏРµРј РѕР±Р° С‚РёРїР° РѕС‚РІРµС‚РѕРІ:
            # 1. РћС‚РІРµС‚ РЅР° РёРЅСЃС‚СЂСѓРєС†РёРѕРЅРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ (РЅРѕРІС‹Р№ СЃРїРѕСЃРѕР±)
            # 2. РћС‚РІРµС‚ РЅР° РёСЃС…РѕРґРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ Р°РЅРєРµС‚С‹ (СЃС‚Р°СЂС‹Р№ СЃРїРѕСЃРѕР± РґР»СЏ СЃРѕРІРјРµСЃС‚РёРјРѕСЃС‚Рё)
            if (rel.get('upc_instruction_message_id') == replied_msg_id or 
                rel.get('moderation_message_id') == replied_msg_id):
                user_id = uid
                idx = idx_rel
                break
        if user_id:
            break
    
    if not user_id or idx is None:
        return  # РњРѕР»С‡Р°Р»РёРІРѕ РёРіРЅРѕСЂРёСЂСѓРµРј СЃРѕРѕР±С‰РµРЅРёСЏ, РєРѕС‚РѕСЂС‹Рµ РЅРµ РїСЂРёРЅР°РґР»РµР¶Р°С‚ РёР·РІРµСЃС‚РЅС‹Рј Р°РЅРєРµС‚Р°Рј
    
    release = db[user_id][idx]
    
    # РЎРѕС…СЂР°РЅСЏРµРј UPC РІ СЂРµР»РёР·Рµ
    release["upc"] = upc_code
    save_db(db)
    update_moderation_record(user_id, idx, release)
    
    # РЈРІРµРґРѕРјР»СЏРµРј РјРѕРґРµСЂР°С‚РѕСЂР°
    await update.message.reply_text(f"{WINTER_EMOJIS['check']} UPC РєРѕРґ <code>{upc_code}</code> РґРѕР±Р°РІР»РµРЅ Рё СЃРѕС…СЂР°РЅРµРЅ!")
    
    # РћР±РЅРѕРІР»СЏРµРј РёСЃС…РѕРґРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ Р°РЅРєРµС‚С‹ РІ С‡Р°С‚Рµ РјРѕРґРµСЂР°С†РёРё, С‡С‚РѕР±С‹ UPC РѕС‚РѕР±СЂР°Р·РёР»СЃСЏ
    moderation_msg_id = release.get('moderation_message_id')
    if moderation_msg_id:
        try:
            # РџРµСЂРµС„РѕСЂРјР°С‚РёСЂСѓРµРј Р°РЅРєРµС‚Сѓ СЃ РЅРѕРІС‹Рј UPC
            from telegram import User
            user_obj = User(id=int(user_id), is_bot=False, first_name="", username=release.get('username'))
            updated_form = _format_release_form_for_group(user_obj, user_id, release)
            
            # РћР±РЅРѕРІР»СЏРµРј РёСЃС…РѕРґРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ, СЃРѕС…СЂР°РЅСЏСЏ СЃС‚Р°С‚СѓСЃ-С€Р°РїРєСѓ Рё РєР»Р°РІРёР°С‚СѓСЂСѓ
            status = release.get('status', STATUS_ON_UPLOAD)
            await _append_status_to_moderation_message(
                context,
                moderation_msg_id,
                updated_form,
                status,
                reply_markup=query.message.reply_markup if hasattr(replied_msg, 'reply_markup') else None
            )
        except Exception as e:
            print(f"РћС€РёР±РєР° РѕР±РЅРѕРІР»РµРЅРёСЏ СЃРѕРѕР±С‰РµРЅРёСЏ Р°РЅРєРµС‚С‹ СЃ UPC: {e}")
    
    # РЈРІРµРґРѕРјР»СЏРµРј Р°СЂС‚РёСЃС‚Р°
    try:
        await context.bot.send_message(
            int(user_id),
            f"{WINTER_EMOJIS['check']} <b>UPC РљРћР” Р”РћР‘РђР’Р›Р•Рќ</b>\n\n"
            f"рџ“ќ <b>{escape_html(release.get('name', 'вЂ”'))}</b>\n"
            f"рџ“¦ <b>UPC:</b> <code>{escape_html(upc_code)}</code>\n\n"
            f"Р’Р°С€ СЂРµР»РёР· РіРѕС‚РѕРІ Рє РїСѓР±Р»РёРєР°С†РёРё!",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё СѓРІРµРґРѕРјР»РµРЅРёСЏ Р°СЂС‚РёСЃС‚Сѓ РѕР± UPC: {e}")


async def order_cover_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    user_data.setdefault(user_id, {})
    user_data[user_id]['cover'] = {}
    await safe_edit(query, "рџ“¦ <b>Р—Р°РєР°Р· РѕР±Р»РѕР¶РєРё вЂ” С€Р°Рі 1/6</b>\n\nРћС‚РїСЂР°РІСЊС‚Рµ СЂРµС„РµСЂРµРЅСЃ (СЃСЃС‹Р»РєСѓ РёР»Рё С„РѕС‚Рѕ) РёР»Рё РєСЂР°С‚РєРѕ РѕРїРёС€РёС‚Рµ, РѕС‚ С‡РµРіРѕ РѕС‚С‚Р°Р»РєРёРІР°С‚СЊСЃСЏ.")
    return COVER_COLORS


async def cover_colors_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # РЎРѕС…СЂР°РЅСЏРµРј СЂРµС„РµСЂРµРЅСЃ (С‚РµРєСЃС‚ РёР»Рё С„РѕС‚Рѕ)
    user_id = str((update.callback_query.from_user.id if update.callback_query else update.message.from_user.id))
    cov = user_data.setdefault(user_id, {}).setdefault('cover', {})
    
    if update.message and update.message.photo:
        cov['reference_photo'] = update.message.photo[-1].file_id
    elif update.message:
        cov['reference_text'] = update.message.text
    
    # РћС‚РїСЂР°РІР»СЏРµРј СЃР»РµРґСѓСЋС‰РёР№ РІРѕРїСЂРѕСЃ (Р±РµР· parse_mode РґР»СЏ С‚РµРєСЃС‚Р° СЃ СЌРјРѕРґР·Рё)
    if update.message:
        await update.message.reply_text("рџЋЁ РЁР°Рі 2/6 вЂ” РљР°РєРёРµ РѕСЃРЅРѕРІРЅС‹Рµ С†РІРµС‚Р° РґРѕР»Р¶РЅС‹ Р±С‹С‚СЊ РІ РѕР±Р»РѕР¶РєРµ?")
    else:
        await safe_edit(update.callback_query, "рџЋЁ РЁР°Рі 2/6 вЂ” РљР°РєРёРµ РѕСЃРЅРѕРІРЅС‹Рµ С†РІРµС‚Р° РґРѕР»Р¶РЅС‹ Р±С‹С‚СЊ РІ РѕР±Р»РѕР¶РєРµ?")
    
    return COVER_TITLE


async def cover_title_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    cov = user_data.setdefault(user_id, {}).setdefault('cover', {})
    cov['colors'] = clean(update.message.text)
    await update.message.reply_text("вњЌпёЏ РЁР°Рі 3/6 вЂ” РќР°Р·РІР°РЅРёРµ СЂРµР»РёР·Р° (РєР°Рє РЅР°РїРёСЃР°С‚СЊ РЅР° РѕР±Р»РѕР¶РєРµ):")
    return COVER_PREFS


async def cover_prefs_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    cov = user_data.setdefault(user_id, {}).setdefault('cover', {})
    cov['title'] = clean(update.message.text)
    await update.message.reply_text("вњЏпёЏ РЁР°Рі 4/6 вЂ” Р’Р°С€Рё РїСЂРµРґРїРѕС‡С‚РµРЅРёСЏ РёР»Рё РєРѕРјРјРµРЅС‚Р°СЂРёРё РїРѕ РґРёР·Р°Р№РЅСѓ:")
    return COVER_TG


async def cover_tg_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    cov = user_data.setdefault(user_id, {}).setdefault('cover', {})
    cov['prefs'] = clean(update.message.text)
    await update.message.reply_text("рџ“± РЁР°Рі 5/6 вЂ” РЈРєР°Р¶РёС‚Рµ РІР°С€ Telegram РґР»СЏ СЃРІСЏР·Рё:")
    return COVER_PAYMENT


async def cover_payment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    cov = user_data.setdefault(user_id, {}).setdefault('cover', {})
    cov['tg'] = clean(update.message.text)

    # РћС‚РїСЂР°РІР»СЏРµРј РёРЅСЃС‚СЂСѓРєС†РёСЋ РїРѕ РѕРїР»Р°С‚Рµ Рё СЃРѕС…СЂР°РЅСЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ РѕР¶РёРґР°РЅРёСЏ СЃРєСЂРёРЅР°
    text = (
        "рџ’і РћРїР»Р°С‚РёС‚Рµ 500в‚Ѕ Рё РѕС‚РїСЂР°РІСЊС‚Рµ СЃРєСЂРёРЅС€РѕС‚ РїР»Р°С‚РµР¶Р° РІ РѕС‚РІРµС‚ РЅР° СЌС‚Рѕ СЃРѕРѕР±С‰РµРЅРёРµ.\n\n"
        "рџ’і РљР°СЂС‚Р° MIR\n2200 7004 9056 2443\n\n"
        "рџ’і РљР°СЂС‚Р° VISA\n4177 4901 8116 9097\n\n"
        "рџ“€ РљСЂРёРїС‚Р° (USDT TRC20)\nTW5awCiuhfpAoLGvu1WXXWzKHbgEEDbv1x\n\n"
        "РџРѕСЃР»Рµ РѕРїР»Р°С‚С‹ РѕС‚РІРµС‚СЊС‚Рµ РЅР° СЌС‚Рѕ СЃРѕРѕР±С‰РµРЅРёРµ СЃРєСЂРёРЅС€РѕС‚РѕРј вЂ” Р·Р°РєР°Р· Р±СѓРґРµС‚ РѕС‚РїСЂР°РІР»РµРЅ РІ РјРѕРґРµСЂР°С†РёСЋ."
    )
    instr = await update.message.reply_text(text)
    cov['payment_instruction_message_id'] = instr.message_id
    await update.message.reply_text("РћР¶РёРґР°СЋ СЃРєСЂРёРЅС€РѕС‚ РѕРїР»Р°С‚С‹ (РѕС‚РІРµС‚СЊС‚Рµ РЅР° РёРЅСЃС‚СЂСѓРєС†РёСЋ).")
    return COVER_WAIT_SCREENSHOT


async def cover_screenshot_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # РћР¶РёРґР°РµРј С„РѕС‚Рѕ-СЃРєСЂРёРЅС€РѕС‚ РѕРїР»Р°С‚С‹
    if not update.message or not update.message.photo:
        return COVER_WAIT_SCREENSHOT
    user_id = str(update.message.from_user.id)
    cov = user_data.setdefault(user_id, {}).get('cover', {})
    # РќР°Р№РґС‘Рј СЂРµР»РёР·-РїСЂРµРґР·Р°РєР°Р· РґР°РЅРЅС‹Рµ
    caption = (
        f"рџ“Њ <b>Р—РђРљРђР— РћР‘Р›РћР–РљР</b>\n"
        f"РћС‚: @{escape_html(update.message.from_user.username or '')} (ID: {user_id})\n"
        f"РќР°Р·РІР°РЅРёРµ: {escape_html(cov.get('title','вЂ”'))}\n"
        f"TG: {escape_html(cov.get('tg','вЂ”'))}\n"
        f"РљРѕРјРјРµРЅС‚Р°СЂРёР№: {escape_html(cov.get('prefs','вЂ”'))}\n"
    )
    # РџРµСЂРµСЃС‹Р»Р°РµРј РІ РјРѕРґРµСЂР°С†РёСЋ РєР°Рє РЅРѕРІРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ СЃ С„РѕС‚Рѕ Рё РїРѕРґРїРёСЃСЊСЋ
    try:
        msg = await context.bot.send_photo(
            chat_id=MODERATION_CHAT_ID,
            photo=update.message.photo[-1].file_id,
            caption=caption,
            parse_mode=ParseMode.HTML,
        )
        try:
            await context.bot.pin_chat_message(chat_id=MODERATION_CHAT_ID, message_id=msg.message_id)
        except Exception:
            pass
        # РЎРѕС…СЂР°РЅСЏРµРј РІ moderation_db РєР°Рє Р·Р°РєР°Р· (Р±РµР· СЃС‚Р°С‚СѓСЃРѕРІ)
        moderation_db = load_moderation_db()
        order = {
            'type': 'cover_order',
            'message_id': msg.message_id,
            'user_id': user_id,
            'data': cov,
            'time': datetime.now().isoformat(),
        }
        moderation_db.setdefault('moderation_messages', []).append(order)
        save_moderation_db(moderation_db)
        await update.message.reply_text("вњ… Р—Р°РєР°Р· РѕС‚РїСЂР°РІР»РµРЅ РІ РјРѕРґРµСЂР°С†РёСЋ. РЎРїР°СЃРёР±Рѕ!")
    except Exception as e:
        print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё Р·Р°РєР°Р·Р° РѕР±Р»РѕР¶РєРё: {e}")
        await update.message.reply_text("вќЊ РќРµ СѓРґР°Р»РѕСЃСЊ РѕС‚РїСЂР°РІРёС‚СЊ Р·Р°РєР°Р·. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.")
    return ConversationHandler.END


async def promo_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    user_data.setdefault(user_id, {})
    user_data[user_id]['promo'] = {}
    await safe_edit(query, "рџ“ќ <b>РџСЂРѕРјРѕ-С‚РµРєСЃС‚ вЂ” С€Р°Рі 1/13</b>\n\nРЈРєР°Р¶РёС‚Рµ РёРјСЏ Р°СЂС‚РёСЃС‚Р°:")
    return PROMO_PROJECT


async def promo_project_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {})['promo'] = {'artist': clean(update.message.text)}
    kb = InlineKeyboardMarkup([
           [InlineKeyboardButton("рџЋ¤ Solo", callback_data='promo_project_solo'),
            InlineKeyboardButton("рџЋµ Feat", callback_data='promo_project_feat')],
    ])
    await update.message.reply_text("РЈРєР°Р¶РёС‚Рµ С‚РёРї РїСЂРѕРµРєС‚Р°:", reply_markup=kb)
    return PROMO_PROJECT


async def promo_release_name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    p['release_name'] = clean(update.message.text)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("рџЋµ РЎРёРЅРіР»", callback_data='promo_kind_single'),
         InlineKeyboardButton("рџ’ї EP", callback_data='promo_kind_ep'),
         InlineKeyboardButton("рџ“Ђ РђР»СЊР±РѕРј", callback_data='promo_kind_album')],
    ])
    await update.message.reply_text("РўРёРї СЂРµР»РёР·Р°:", reply_markup=kb)
    return PROMO_RELEASE_KIND


async def promo_release_kind_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    # Р¤РѕР»Р±РµРє: РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ РІРІС‘Р» С‚РёРї СЂРµР»РёР·Р° С‚РµРєСЃС‚РѕРј
    p['release_kind'] = clean(update.message.text)
    await update.message.reply_text("Р–Р°РЅСЂ (РѕСЃРЅРѕРІРЅРѕР№):")
    return PROMO_GENRE_MAIN


async def promo_genre_main_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    p['genre_main'] = clean(update.message.text)
    await update.message.reply_text("+1 РґРѕРїРѕР»РЅРёС‚РµР»СЊРЅС‹Р№ Р¶Р°РЅСЂ (РµСЃР»Рё РµСЃС‚СЊ), Р»РёР±Рѕ '-' :")
    return PROMO_GENRE_EXTRA


async def promo_genre_extra_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    p['genre_extra'] = clean(update.message.text)
    await update.message.reply_text("РќР°СЃС‚СЂРѕРµРЅРёРµ (2-4 СЃР»РѕРІР°, РЅР°РїСЂРёРјРµСЂ: РјСЂР°С‡РЅС‹Р№, С…РѕР»РѕРґРЅС‹Р№):")
    return PROMO_MOOD


async def promo_mood_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    p['mood'] = clean(update.message.text)
    await update.message.reply_text("Р’Р°Р№Р± / РѕР±СЂР°Р· (Р°СЃСЃРѕС†РёР°С†РёРё, РІРёР·СѓР°Р»):")
    return PROMO_VIBE


async def promo_vibe_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    p['vibe'] = clean(update.message.text)
    await update.message.reply_text("Р—РІСѓС‡Р°РЅРёРµ (РїР»РѕС‚РЅС‹Р№/РјРёРЅРёРјР°Р»РёСЃС‚РёС‡РЅС‹Р№/РіСЂСЏР·РЅС‹Р№/РІРѕР·РґСѓС€РЅС‹Р№):")
    return PROMO_SOUND


async def promo_sound_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    p['sound'] = clean(update.message.text)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("вќЊ Р‘РµР· РІРѕРєР°Р»Р°", callback_data='promo_vocal_no')],
        [InlineKeyboardButton("рџЋ¤ РњСѓР¶СЃРєРѕР№ РІРѕРєР°Р»", callback_data='promo_vocal_male'),
         InlineKeyboardButton("рџ‘ё Р–РµРЅСЃРєРёР№ РІРѕРєР°Р»", callback_data='promo_vocal_female')],
    ])
    await update.message.reply_text("Р’РѕРєР°Р»:", reply_markup=kb)
    return PROMO_VOCAL


async def promo_vocal_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Fallback if user types vocal info instead of pressing inline buttons
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    txt = clean(update.message.text).lower()
    if 'Р±РµР·' in txt or 'instrument' in txt:
        p['vocal'] = 'instrumental'
    elif 'РјСѓР¶' in txt or 'male' in txt:
        p['vocal'] = 'male'
    elif 'Р¶РµРЅ' in txt or 'female' in txt:
        p['vocal'] = 'female'
    else:
        # if unclear, save raw text
        p['vocal'] = clean(update.message.text)
    await update.message.reply_text("Р­РјРѕС†РёСЏ (С‡С‚Рѕ РґРѕР»Р¶РµРЅ РїРѕС‡СѓРІСЃС‚РІРѕРІР°С‚СЊ СЃР»СѓС€Р°С‚РµР»СЊ):")
    return PROMO_EMOTION


async def promo_language_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    p['language'] = clean(update.message.text)
    await update.message.reply_text("Р­РјРѕС†РёСЏ (С‡С‚Рѕ РґРѕР»Р¶РµРЅ РїРѕС‡СѓРІСЃС‚РІРѕРІР°С‚СЊ СЃР»СѓС€Р°С‚РµР»СЊ):")
    return PROMO_EMOTION


async def promo_emotion_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    p['emotion'] = clean(update.message.text)
    await update.message.reply_text("рџЊЌ <b>Р“РґРµ РЅР°С…РѕРґРёС‚СЃСЏ Р°СЂС‚РёСЃС‚?</b>\n\nРЈРєР°Р¶РёС‚Рµ СЃС‚СЂР°РЅСѓ (РЅР°РїСЂРёРјРµСЂ: Р РѕСЃСЃРёСЏ, РЎРЁРђ, РЇРїРѕРЅРёСЏ):", parse_mode=ParseMode.HTML)
    return PROMO_COUNTRY


async def promo_usecase_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    p['usecase'] = clean(update.message.text)

    # РћС‚РїСЂР°РІР»СЏРµРј РЅР°С‡Р°Р»СЊРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ Рѕ РіРµРЅРµСЂР°С†РёРё
    status_msg = await update.message.reply_text("вЏі <b>Р“РµРЅРµСЂРёСЂСѓСЋ РІР°С€ РїСЂРѕРјРѕ-С‚РµРєСЃС‚...</b>\n\nвЊ› РћР±СЂР°Р±РѕС‚РєР° РґР°РЅРЅС‹С…...", parse_mode=ParseMode.HTML)
    
    # РџРѕРєР°Р·С‹РІР°РµРј Р°РЅРёРјР°С†РёСЋ Р·Р°РіСЂСѓР·РєРё
    loading_states = [
        "вЏі <b>Р“РµРЅРµСЂРёСЂСѓСЋ РІР°С€ РїСЂРѕРјРѕ-С‚РµРєСЃС‚...</b>\n\nвЊ› РћР±СЂР°Р±РѕС‚РєР° РґР°РЅРЅС‹С…...",
        "вЏі <b>Р“РµРЅРµСЂРёСЂСѓСЋ РІР°С€ РїСЂРѕРјРѕ-С‚РµРєСЃС‚...</b>\n\nв–Њ РђРЅР°Р»РёР· РёРЅС„РѕСЂРјР°С†РёРё РѕР± Р°СЂС‚РёСЃС‚Рµ...",
        "вЏі <b>Р“РµРЅРµСЂРёСЂСѓСЋ РІР°С€ РїСЂРѕРјРѕ-С‚РµРєСЃС‚...</b>\n\nв–Њв–Њ РџРѕРґРіРѕС‚РѕРІРєР° РѕРїРёСЃР°РЅРёСЏ СЂРµР»РёР·Р°...",
        "вЏі <b>Р“РµРЅРµСЂРёСЂСѓСЋ РІР°С€ РїСЂРѕРјРѕ-С‚РµРєСЃС‚...</b>\n\nв–Њв–Њв–Њ РЎРѕР·РґР°РЅРёРµ С‚РµРєСЃС‚РѕРІ РґР»СЏ РїР»Р°С‚С„РѕСЂРј...",
        "вЏі <b>Р“РµРЅРµСЂРёСЂСѓСЋ РІР°С€ РїСЂРѕРјРѕ-С‚РµРєСЃС‚...</b>\n\nв–Њв–Њв–Њв–Њ Р¤РѕСЂРјР°С‚РёСЂРѕРІР°РЅРёРµ РєРѕРЅС‚РµРЅС‚Р°...",
        "вЏі <b>Р“РµРЅРµСЂРёСЂСѓСЋ РІР°С€ РїСЂРѕРјРѕ-С‚РµРєСЃС‚...</b>\n\nв–Њв–Њв–Њв–Њв–Њ Р¤РёРЅР°Р»СЊРЅР°СЏ РїРѕРґРіРѕС‚РѕРІРєР°...",
    ]
    
    for state in loading_states:
        try:
            await status_msg.edit_text(state, parse_mode=ParseMode.HTML)
            await asyncio.sleep(0.3)
        except:
            pass

    # Р’С‹Р·С‹РІР°РµРј С„СѓРЅРєС†РёСЋ РіРµРЅРµСЂР°С†РёРё СЃ РґР°РЅРЅС‹РјРё
    ai_text = await _call_openai_for_promo_new(p)
    if not ai_text:
        await status_msg.edit_text("вќЊ <b>РћС€РёР±РєР° РіРµРЅРµСЂР°С†РёРё</b>\n\nРќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕР·РґР°С‚СЊ РїСЂРѕРјРѕ-С‚РµРєСЃС‚. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕР·Р¶Рµ.", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    
    # РџРѕРєР°Р·С‹РІР°РµРј СЂРµР·СѓР»СЊС‚Р°С‚ СЃ Р°РЅРёРјР°С†РёРµР№
    sections = ai_text.split('\n\n')
    current_text = ""
    
    for i, section in enumerate(sections):
        current_text += section + "\n\n"
        try:
            if len(current_text) > 4000:  # РћРіСЂР°РЅРёС‡РµРЅРёРµ Telegram
                await status_msg.edit_text(current_text[:4000] + "\n\n...", parse_mode=ParseMode.HTML)
            else:
                await status_msg.edit_text(current_text, parse_mode=ParseMode.HTML)
            await asyncio.sleep(0.2)
        except:
            pass

    # Р¤РёРЅР°Р»СЊРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ вЂ” СѓР±РµРґРёРјСЃСЏ С‡С‚Рѕ РѕРЅРѕ РѕС‚Р»РёС‡Р°РµС‚СЃСЏ РѕС‚ РїРѕСЃР»РµРґРЅРµРіРѕ РѕС‚РїСЂР°РІР»РµРЅРЅРѕРіРѕ (РёР·Р±РµРіР°РµРј Message is not modified)
    last_state = loading_states[-1]
    try:
        if ai_text != last_state:
            await status_msg.edit_text(ai_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        if not _is_remote_protocol_error(e):
            pass
    
    await update.message.reply_text("вњ… <b>Р’Р°С€ РїСЂРѕРјРѕ-С‚РµРєСЃС‚ РіРѕС‚РѕРІ!</b>\n\nР’С‹ РјРѕР¶РµС‚Рµ РёСЃРїРѕР»СЊР·РѕРІР°С‚СЊ СЌС‚Рё С‚РµРєСЃС‚С‹ РґР»СЏ РїСЂРѕРјРѕ РЅР° СЂР°Р·Р»РёС‡РЅС‹С… РїР»Р°С‚С„РѕСЂРјР°С….", parse_mode=ParseMode.HTML)
    
    return ConversationHandler.END


async def promo_country_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    p = user_data.setdefault(user_id, {}).setdefault('promo', {})
    p['country'] = clean(update.message.text)
    await update.message.reply_text("рџ“Ќ <b>Р“РґРµ С‚СЂРµРє СЂР°Р±РѕС‚Р°РµС‚ Р»СѓС‡С€Рµ РІСЃРµРіРѕ?</b>\n\nРџСЂРёРјРµСЂ: РЅР°СѓС€РЅРёРєРё, РјР°С€РёРЅР°, РєР»СѓР±, РІРµС‡РµСЂ/РЅРѕС‡СЊ, РґРѕРјР°, РІ РґРѕСЂРѕРіРµ", parse_mode=ParseMode.HTML)
    return PROMO_USECASE


async def _call_openai_for_promo_new(data: dict) -> str:
    """Р“РµРЅРµСЂРёСЂСѓРµС‚ СЂР°СЃС€РёСЂРµРЅРЅС‹Р№ РїСЂРѕРјРѕ-РїР°РєРµС‚ РІ РїСЂР°РІРёР»СЊРЅРѕРј С„РѕСЂРјР°С‚Рµ."""
    
    artist = data.get('artist', 'РђСЂС‚РёСЃС‚')
    project = data.get('project_type', 'РїСЂРѕРµРєС‚')
    release = data.get('release_name', 'Р РµР»РёР·')
    kind = data.get('release_kind', 'С‚СЂРµРє')
    genre_main = data.get('genre_main', 'СЌР»РµРєС‚СЂРѕРЅРёРєР°')
    genre_extra = data.get('genre_extra', '')
    mood = data.get('mood', 'РґРёРЅР°РјРёС‡РЅС‹Р№')
    vibe = data.get('vibe', 'СЌРЅРµСЂРіРёС‡РЅС‹Р№')
    sound = data.get('sound', 'СЃРѕРІСЂРµРјРµРЅРЅС‹Р№')
    vocal = data.get('vocal', 'РёРЅСЃС‚СЂСѓРјРµРЅС‚Р°Р»СЊРЅС‹Р№')
    language = data.get('language', 'СЂСѓСЃСЃРєРёР№')
    emotion = data.get('emotion', 'РІРґРѕС…РЅРѕРІР»СЏСЋС‰Р°СЏ')
    usecase = data.get('usecase', 'РІРµР·РґРµ')
    country = data.get('country', 'РЅРµРёР·РІРµСЃС‚РЅР°СЏ СЃС‚СЂР°РЅР°')
    
    genre = f"{genre_main} {genre_extra}".strip()
    
    result = f"""<b>рџ“ќ РћРџРРЎРђРќРР• РђР РўРРЎРўРђ РќРђ Р РЈРЎРЎРљРћРњ</b>

{artist} вЂ” {project}, СЂР°Р±РѕС‚Р°СЋС‰РёР№ РІ Р¶Р°РЅСЂРµ {genre} СЃ Р°РєС†РµРЅС‚РѕРј РЅР° {vibe} СЌРЅРµСЂРіРёСЋ, {sound} Р·РІСѓС‡Р°РЅРёРµ Рё {emotion} Р°С‚РјРѕСЃС„РµСЂСѓ. РњСѓР·С‹РєР° СЃС‚СЂРѕРёС‚СЃСЏ РЅР° {mood} СЂРёС‚РјРµ, СЃРѕРІСЂРµРјРµРЅРЅРѕР№ СЌСЃС‚РµС‚РёРєРµ Рё С„РёР·РёС‡РµСЃРєРѕРј РІРѕРІР»РµС‡РµРЅРёРё СЃР»СѓС€Р°С‚РµР»СЏ.

<b>рџ“ќ РћРџРРЎРђРќРР• Р Р•Р›РР—Рђ РќРђ Р РЈРЎРЎРљРћРњ</b>

{release} вЂ” {kind}, РїРѕСЃС‚СЂРѕРµРЅРЅС‹Р№ РІРѕРєСЂСѓРі {genre}-СЂРёС‚РјРёРєРё. Р РµР»РёР· Р·РІСѓС‡РёС‚ РїСЂСЏРјРѕР»РёРЅРµР№РЅРѕ Рё РЅР°РїРѕСЂРёСЃС‚Рѕ, РґРµР»Р°СЏ СЃС‚Р°РІРєСѓ РЅР° {mood} Р°С‚РјРѕСЃС„РµСЂСѓ Рё СЂРёС‚РјРёС‡РµСЃРєРѕРµ РґР°РІР»РµРЅРёРµ. РўСЂРµРє СЃРѕР·РґР°С‘С‚ РѕС‰СѓС‰РµРЅРёРµ РґРІРёР¶РµРЅРёСЏ Рё РґРёРЅР°РјРёРєРё.

<b>рџЋµ РРќР¤РћР РњРђР¦РРЇ Р”Р›РЇ SPOTIFY (РјР°РєСЃ. 500 СЃРёРјРІРѕР»РѕРІ)</b>

{artist} is a {project} in {genre}, focused on {sound} sound. {release} ({kind}) delivers {mood} rhythm and {vibe} energy, perfect for dynamic content with {emotion} atmosphere and {vocal} elements.

<b>рџЋ§ РРќР¤РћР РњРђР¦РРЇ Р”Р›РЇ DEEZER (РјР°РєСЃ. 1500 СЃРёРјРІРѕР»РѕРІ)</b>

{artist} is a {project} in {genre}, emphasizing raw groove and contemporary design. Their music focuses on repetition, pressure, and physical rhythm for immersive listening. {release} ({kind}) brings {emotion} energy and {mood} rhythm. The track works best {usecase}.

в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
рџЊЌ <b>РЎС‚СЂР°РЅР°:</b> {country}
в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ"""
    
    return result


async def _call_openai_for_promo(prompt: str) -> str:
    """Р“РµРЅРµСЂРёСЂСѓРµС‚ СЂР°СЃС€РёСЂРµРЅРЅС‹Р№ РїСЂРѕРјРѕ-С‚РµРєСЃС‚ РґР»СЏ СЂР°Р·РЅС‹С… РїР»Р°С‚С„РѕСЂРј."""
    import re
    
    print('рџ”„ Р“РµРЅРµСЂРёСЂСѓСЋ СЂР°СЃС€РёСЂРµРЅРЅС‹Р№ РїСЂРѕРјРѕ-РїР°РєРµС‚...')
    
    # РџР°СЂСЃРёРј РїСЂРѕРјРїС‚ РґР»СЏ РёР·РІР»РµС‡РµРЅРёСЏ РґР°РЅРЅС‹С…
    data = {
        'artist': 'РЅРµРёР·РІРµСЃС‚РЅС‹Р№ Р°СЂС‚РёСЃС‚',
        'project_type': 'РїСЂРѕРµРєС‚',
        'release_name': 'РЅРѕРІС‹Р№ СЂРµР»РёР·',
        'release_kind': 'С‚СЂРµРє',
        'genre_main': 'СЌР»РµРєС‚СЂРѕРЅРёРєР°',
        'genre_extra': '',
        'mood': 'РґРёРЅР°РјРёС‡РЅС‹Р№',
        'vibe': 'СЌРЅРµСЂРіРёС‡РЅС‹Р№',
        'sound': 'СЃРѕРІСЂРµРјРµРЅРЅС‹Р№',
        'vocal': 'РёРЅСЃС‚СЂСѓРјРµРЅС‚Р°Р»СЊРЅС‹Р№',
        'language': 'СЂСѓСЃСЃРєРёР№',
        'emotion': 'РІРґРѕС…РЅРѕРІР»СЏСЋС‰Р°СЏ',
        'usecase': 'РґР»СЏ РєР»СѓР±РѕРІ',
    }
    
    # РџС‹С‚Р°РµРјСЃСЏ РёР·РІР»РµС‡СЊ Р·РЅР°С‡РµРЅРёСЏ РёР· РїСЂРѕРјРїС‚Р°
    patterns = {
        'artist': r'РђСЂС‚РёСЃС‚:\s*([^\n]+)',
        'project_type': r'РџСЂРѕРµРєС‚:\s*([^\n]+)',
        'release_name': r'Р РµР»РёР·:\s*([^\n]+)',
        'release_kind': r'РўРёРї:\s*([^\n]+)',
        'genre_main': r'Р–Р°РЅСЂ:\s*([^\n,]+)',
        'genre_extra': r'Р–Р°РЅСЂ:.*?([^\n]+)$',
        'mood': r'РќР°СЃС‚СЂРѕРµРЅРёРµ:\s*([^\n]+)',
        'vibe': r'Р’Р°Р№Р±:\s*([^\n]+)',
        'sound': r'Р—РІСѓС‡Р°РЅРёРµ:\s*([^\n]+)',
        'vocal': r'Р’РѕРєР°Р»:\s*([^\n]+)',
        'language': r'РЇР·С‹Рє:\s*([^\n]+)',
        'emotion': r'Р­РјРѕС†РёСЏ:\s*([^\n]+)',
        'usecase': r'Р“РґРµ СЃР»СѓС€Р°С‚СЊ:\s*([^\n]+)',
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, prompt, re.IGNORECASE)
        if match:
            data[key] = match.group(1).strip()
    
    # Р“РµРЅРµСЂРёСЂСѓРµРј РјРЅРѕРіРѕСѓСЂРѕРІРЅРµРІС‹Р№ РїСЂРѕРјРѕ-РїР°РєРµС‚
    artist = data['artist']
    release = data['release_name']
    genre = data['genre_main']
    mood = data['mood']
    vibe = data['vibe']
    sound = data['sound']
    project = data['project_type']
    kind = data['release_kind']
    language = data['language']
    emotion = data['emotion']
    usecase = data['usecase']
    
    result = f"""
рџ“ќ **РћРџРРЎРђРќРР• РђР РўРРЎРўРђ**
{artist} вЂ” {project}, СЂР°Р±РѕС‚Р°СЋС‰РёР№ РІ Р¶Р°РЅСЂРµ {genre} СЃ Р°РєС†РµРЅС‚РѕРј РЅР° {vibe} СЌРЅРµСЂРіРёСЋ, {sound} Р·РІСѓС‡Р°РЅРёРµ Рё {emotion} Р°С‚РјРѕСЃС„РµСЂСѓ. РњСѓР·С‹РєР° СЃС‚СЂРѕРёС‚СЃСЏ РЅР° {mood} СЂРёС‚РјРµ, СЃРѕРІСЂРµРјРµРЅРЅРѕР№ СЌСЃС‚РµС‚РёРєРµ Рё С„РёР·РёС‡РµСЃРєРѕРј РІРѕРІР»РµС‡РµРЅРёРё СЃР»СѓС€Р°С‚РµР»СЏ. РђСЂС‚РёСЃС‚ РѕСЂРёРµРЅС‚РёСЂСѓРµС‚СЃСЏ РЅР° Р°РєС‚СѓР°Р»СЊРЅС‹Р№ Р·РІСѓРє Рё РІРёР·СѓР°Р»СЊРЅСѓСЋ РєСѓР»СЊС‚СѓСЂСѓ.

рџ“ќ **РћРџРРЎРђРќРР• Р Р•Р›РР—Рђ**
{release} вЂ” {kind}, РїРѕСЃС‚СЂРѕРµРЅРЅС‹Р№ РІРѕРєСЂСѓРі {genre}-СЂРёС‚РјРёРєРё Рё РёРЅСЃС‚РёРЅРєС‚РёРІРЅРѕР№ СЌРЅРµСЂРіРёРё. Р РµР»РёР· Р·РІСѓС‡РёС‚ РїСЂСЏРјРѕР»РёРЅРµР№РЅРѕ Рё РЅР°РїРѕСЂРёСЃС‚Рѕ, РґРµР»Р°СЏ СЃС‚Р°РІРєСѓ РЅР° {mood} Р°С‚РјРѕСЃС„РµСЂСѓ Рё СЂРёС‚РјРёС‡РµСЃРєРѕРµ РґР°РІР»РµРЅРёРµ. РўСЂРµРє СЃРѕР·РґР°С‘С‚ РѕС‰СѓС‰РµРЅРёРµ РґРІРёР¶РµРЅРёСЏ Рё РґРёРЅР°РјРёРєРё, Р»РµРіРєРѕ Р°РґР°РїС‚РёСЂСѓРµС‚СЃСЏ РїРѕРґ РєРѕСЂРѕС‚РєРёРµ РІРёРґРµРѕС„РѕСЂРјР°С‚С‹ Рё Р°РєС‚РёРІРЅРѕ СЂР°Р±РѕС‚Р°РµС‚ РІ СЃРѕС†РёР°Р»СЊРЅС‹С… СЃРµС‚СЏС….

рџ“ќ **Р”Р›РЇ SPOTIFY** (РјР°РєСЃ. 500 СЃРёРјРІРѕР»РѕРІ)
{artist} is a {project} focused on {genre}, emphasizing {sound} rhythm, {vibe} energy, and modern digital aesthetics.
The track {release} delivers {mood} groove and physical rhythm, perfect for dynamic content and short-form videos.

рџ“ќ **Р”Р›РЇ DEEZER** (РјР°РєСЃ. 1500 СЃРёРјРІРѕР»РѕРІ)
{artist} is a {project} working within {genre}, emphasizing raw groove, contemporary sound design, and street-inspired aesthetics. Their music is built around repetition, pressure, and physical rhythm, aiming for an instinctive and immersive listening experience.

The release {release} is driven by {emotion} energy and {mood} rhythm. The track focuses on groove rather than complexity, creating a hypnotic effect through tempo and repetition. {usecase}.

рџ“ќ **РћРџРРЎРђРќРР• Р”Р›РЇ РЎРћР¦РЎР•РўР•Р™**
рџЋµ {artist} РїСЂРµРґСЃС‚Р°РІР»СЏРµС‚ {release} вЂ” {kind} РІ Р¶Р°РЅСЂРµ {genre}. {mood} Р°С‚РјРѕСЃС„РµСЂР°, {vibe} РІР°Р№Р±, {sound} Р·РІСѓС‡Р°РЅРёРµ. {emotion} С‚СЂРµРє, РєРѕС‚РѕСЂС‹Р№ СЂР°Р±РѕС‚Р°РµС‚ РІРµР·РґРµ! рџ”Ґ

#{''.join([x[0] for x in genre.split()])}{artist.replace(' ', '')}{release.replace(' ', '')}
"""
    
    print(f'вњ… РџСЂРѕРјРѕ-РїР°РєРµС‚ РіРѕС‚РѕРІ: {len(result)} СЃРёРјРІРѕР»РѕРІ')
    return result




def _check_openai_status() -> dict:
    """РџСЂРѕРІРµСЂСЏРµС‚ РЅР°Р»РёС‡РёРµ Hugging Face С‚РѕРєРµРЅР° Рё РґРѕСЃС‚СѓРїРЅРѕСЃС‚СЊ httpx."""
    hf_token = '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'
    return {
        'has_key': bool(hf_token),
        'httpx_available': httpx is not None,
        'key_preview': (hf_token[:10] + '...' if hf_token else None)
    }


async def check_openai_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status = _check_openai_status()
    lines = ["РџСЂРѕРІРµСЂРєР° Hugging Face API:"]
    lines.append(f"- Hugging Face С‚РѕРєРµРЅ СѓСЃС‚Р°РЅРѕРІР»РµРЅ: {'Р”Р°' if status['has_key'] else 'РќРµС‚'}")
    lines.append(f"- httpx РґРѕСЃС‚СѓРїРµРЅ: {'Р”Р°' if status['httpx_available'] else 'РќРµС‚'}")
    if status['has_key']:
        lines.append(f"- РўРѕРєРµРЅ (preview): {status['key_preview']}")
    lines.append("Р•СЃР»Рё С‡С‚Рѕ-С‚Рѕ РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚ вЂ” СѓСЃС‚Р°РЅРѕРІРёС‚Рµ httpx: pip install httpx")
    await update.message.reply_text("\n".join(lines))


async def moderation_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    # РџСЂРѕРІРµСЂСЏРµРј, С‡С‚Рѕ СЌС‚Рѕ СЃРѕРѕР±С‰РµРЅРёРµ РёР· С‡Р°С‚Р° РјРѕРґРµСЂР°С†РёРё
    if query.message.chat_id != MODERATION_CHAT_ID:
        return
    
    await query.answer()
    
    try:
        # Р Р°Р·Р±РѕСЂ callback_data. РџРѕРґРґРµСЂР¶РёРІР°РµРј СЃР»СѓС‡Р°Рё С‚РёРїР°:
        # m_upload_<user>_<idx>
        # m_rejectreason_<user>_<idx>_<reason_idx>
        # m_add_upc_<user>_<idx>
        parts = query.data.split("_")
        if len(parts) < 4 or parts[0] != "m":
            return
        # РЎРїРµС†РёР°Р»СЊРЅС‹Р№ СЃР»СѓС‡Р°Р№: m_add_upc_<user>_<idx>
        if parts[1] == 'add' and len(parts) >= 5 and parts[2] == 'upc':
            action = 'add_upc'
            user_id = parts[3]
            idx = int(parts[4])
        else:
            action = parts[1]
            user_id = parts[2]
            try:
                idx = int(parts[3])
            except Exception:
                await query.answer("Р РµР»РёР· РЅРµ РЅР°Р№РґРµРЅ", show_alert=True)
                return
        
        if user_id not in db:
            await query.answer("Р РµР»РёР· РЅРµ РЅР°Р№РґРµРЅ", show_alert=True)
            return
            
        if idx >= len(db[user_id]):
            await query.answer("Р РµР»РёР· РЅРµ РЅР°Р№РґРµРЅ", show_alert=True)
            return
        
        release = db[user_id][idx]

        moderator_name = query.from_user.username or query.from_user.first_name

        # FIX: РѕР±СЂР°Р±РѕС‚С‡РёРє РґР»СЏ РїРµСЂРµРєР»СЋС‡РµРЅРёСЏ СЃС‚Р°С‚СѓСЃРѕРІ (РїСЂРѕРјРµР¶СѓС‚РѕС‡РЅС‹Рµ СЃС‚Р°С‚СѓСЃС‹)
        if action == "upload":
            # РџРµСЂРµРєР»СЋС‡Р°РµРј СЃС‚Р°С‚СѓСЃ РЅР° "РЅР° РѕС‚РіСЂСѓР·РєРµ"
            old_status = release.get("status")
            release["status"] = STATUS_ON_UPLOAD
            release["moderator"] = moderator_name
            release["moderation_time"] = datetime.now().isoformat()
            add_history_entry(user_id, idx, old_status, STATUS_ON_UPLOAD, query.from_user.id, moderator_name)
            save_db(db)
            update_moderation_record(user_id, idx, release)

            # РћР±РЅРѕРІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ РІ РјРѕРґРµСЂР°С†РёРё
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_ON_UPLOAD, moderator_username=moderator_name, reply_markup=query.message.reply_markup)
            
            # РЈРІРµРґРѕРјР»РµРЅРёРµ Р°СЂС‚РёСЃС‚Сѓ
            try:
                moderation_time = datetime.now().strftime("%d.%m.%Y РІ %H:%M")
                await context.bot.send_message(
                    int(user_id),
                    f"{WINTER_EMOJIS['upload']} <b>Р Р•Р›РР— РќРђ РћРўР“Р РЈР—РљР•</b>\n\n"
                    f"рџ“ќ <b>{escape_html(release.get('name', 'вЂ”'))}</b>\n"
                    f"рџЋµ <i>РўРёРї:</i> {escape_html(release.get('type', 'вЂ”'))}\n"
                    f"рџ“… <i>Р”Р°С‚Р° СЂРµР»РёР·Р°:</i> {escape_html(release.get('date', 'вЂ”'))}\n"
                    f"рџ‘¤ <i>РђСЂС‚РёСЃС‚:</i> {escape_html(release.get('nick', 'вЂ”'))}\n"
                    f"рџ•ђ <i>Р’СЂРµРјСЏ:</i> {escape_html(moderation_time)}\n"
                    f"рџ‘ЁвЂЌрџ’ј <i>РњРѕРґРµСЂР°С‚РѕСЂ:</i> @{escape_html(moderator_name)}\n\n"
                    f"{WINTER_EMOJIS['sparkles']} Р’Р°С€ СЂРµР»РёР· РіРѕС‚РѕРІРёС‚СЃСЏ Рє РІС‹РїСѓСЃРєСѓ!",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё СѓРІРµРґРѕРјР»РµРЅРёСЏ РЅР° РѕС‚РіСЂСѓР·РєСѓ: {e}")
            
            # Р’РѕСЃСЃС‚Р°РЅР°РІР»РёРІР°РµРј РєРЅРѕРїРєРё СЃС‚Р°С‚СѓСЃРѕРІ (РїСЂРѕРјРµР¶СѓС‚РѕС‡РЅС‹Р№ СЃС‚Р°С‚СѓСЃ - РєРЅРѕРїРєРё РѕСЃС‚Р°СЋС‚СЃСЏ Р°РєС‚РёРІРЅС‹)
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("рџ•“ РќР° РѕС‚РіСЂСѓР·РєРµ", callback_data=f"m_upload_{user_id}_{idx}"),
                    InlineKeyboardButton("рџ§  РњРѕРґРµСЂР°С†РёСЏ", callback_data=f"m_moderate_{user_id}_{idx}"),
                    InlineKeyboardButton("вњ… РџСЂРёРЅСЏС‚Рѕ", callback_data=f"m_approve_{user_id}_{idx}")
                ],
                [
                    InlineKeyboardButton("вќЊ РћС‚РєР»РѕРЅРёС‚СЊ", callback_data=f"m_reject_{user_id}_{idx}"),
                    InlineKeyboardButton("вњЏпёЏ РќР° РёСЃРїСЂР°РІР»РµРЅРёРё", callback_data=f"m_needfix_{user_id}_{idx}"),
                    InlineKeyboardButton("рџ—‘ РЈРґР°Р»РµРЅ", callback_data=f"m_delete_{user_id}_{idx}")
                ],
            ])
            await safe_edit_reply_markup(query, reply_markup=keyboard)
            return

        if action == "moderate":
            # РџРµСЂРµРєР»СЋС‡Р°РµРј СЃС‚Р°С‚СѓСЃ РЅР° "РјРѕРґРµСЂР°С†РёСЏ"
            old_status = release.get("status")
            release["status"] = STATUS_MODERATION
            release["moderator"] = moderator_name
            release["moderation_time"] = datetime.now().isoformat()
            add_history_entry(user_id, idx, old_status, STATUS_MODERATION, query.from_user.id, moderator_name)
            save_db(db)
            update_moderation_record(user_id, idx, release)

            # РћР±РЅРѕРІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ РІ РјРѕРґРµСЂР°С†РёРё
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_MODERATION, moderator_username=moderator_name, reply_markup=query.message.reply_markup)
            
            # РЈРІРµРґРѕРјР»РµРЅРёРµ Р°СЂС‚РёСЃС‚Сѓ
            try:
                moderation_time = datetime.now().strftime("%d.%m.%Y РІ %H:%M")
                await context.bot.send_message(
                    int(user_id),
                    f"{WINTER_EMOJIS['brain']} <b>Р Р•Р›РР— РќРђ РњРћР”Р•Р РђР¦РР</b>\n\n"
                    f"рџ“ќ <b>{escape_html(release.get('name', 'вЂ”'))}</b>\n"
                    f"рџЋµ <i>РўРёРї:</i> {escape_html(release.get('type', 'вЂ”'))}\n"
                    f"рџ“… <i>Р”Р°С‚Р° СЂРµР»РёР·Р°:</i> {escape_html(release.get('date', 'вЂ”'))}\n"
                    f"рџ‘¤ <i>РђСЂС‚РёСЃС‚:</i> {escape_html(release.get('nick', 'вЂ”'))}\n"
                    f"рџ•ђ <i>Р’СЂРµРјСЏ:</i> {escape_html(moderation_time)}\n"
                    f"рџ‘ЁвЂЌрџ’ј <i>РњРѕРґРµСЂР°С‚РѕСЂ:</i> @{escape_html(moderator_name)}\n\n"
                    f"{WINTER_EMOJIS['sparkles']} Р’Р°С€ СЂРµР»РёР· РїСЂРѕС…РѕРґРёС‚ РїСЂРѕРІРµСЂРєСѓ РєР°С‡РµСЃС‚РІР°!",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё СѓРІРµРґРѕРјР»РµРЅРёСЏ Рѕ РјРѕРґРµСЂР°С†РёРё: {e}")
            
            # Р’РѕСЃСЃС‚Р°РЅР°РІР»РёРІР°РµРј РєРЅРѕРїРєРё СЃС‚Р°С‚СѓСЃРѕРІ (РїСЂРѕРјРµР¶СѓС‚РѕС‡РЅС‹Р№ СЃС‚Р°С‚СѓСЃ - РєРЅРѕРїРєРё РѕСЃС‚Р°СЋС‚СЃСЏ Р°РєС‚РёРІРЅС‹)
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("рџ•“ РќР° РѕС‚РіСЂСѓР·РєРµ", callback_data=f"m_upload_{user_id}_{idx}"),
                    InlineKeyboardButton("рџ§  РњРѕРґРµСЂР°С†РёСЏ", callback_data=f"m_moderate_{user_id}_{idx}"),
                    InlineKeyboardButton("вњ… РџСЂРёРЅСЏС‚Рѕ", callback_data=f"m_approve_{user_id}_{idx}")
                ],
                [
                    InlineKeyboardButton("вќЊ РћС‚РєР»РѕРЅРёС‚СЊ", callback_data=f"m_reject_{user_id}_{idx}"),
                    InlineKeyboardButton("вњЏпёЏ РќР° РёСЃРїСЂР°РІР»РµРЅРёРё", callback_data=f"m_needfix_{user_id}_{idx}"),
                    InlineKeyboardButton("рџ—‘ РЈРґР°Р»РµРЅ", callback_data=f"m_delete_{user_id}_{idx}")
                ],
            ])
            await safe_edit_reply_markup(query, reply_markup=keyboard)
            return

        if action == "approve":
            # FIX: РЈРїСЂРѕС‰С‘РЅРЅР°СЏ СЃРёСЃС‚РµРјР° - РїСЂРѕСЃС‚Рѕ РѕРґРѕР±СЂСЏРµРј Р±РµР· РґРѕРї.РєРЅРѕРїРѕРє
            old_status = release.get("status")
            release["status"] = STATUS_APPROVED
            release["moderator"] = moderator_name
            release["moderation_time"] = datetime.now().isoformat()
            add_history_entry(user_id, idx, old_status, STATUS_APPROVED, query.from_user.id, moderator_name)
            save_db(db)
            update_moderation_record(user_id, idx, release)

            # РћР±РЅРѕРІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ РІ РјРѕРґРµСЂР°С†РёРё (РѕС‚РїСЂР°РІР»СЏРµРј РѕС‚РґРµР»СЊРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ СЃРѕ СЃС‚Р°С‚СѓСЃРѕРј)
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_APPROVED, moderator_username=moderator_name, reply_markup=query.message.reply_markup)

            # РћС‚РїСЂР°РІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ СЃ РєРЅРѕРїРєРѕР№ РґР»СЏ РґРѕР±Р°РІР»РµРЅРёСЏ UPC
            try:
                upc_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("рџ“¦ РџСЂРёСЃРІРѕРёС‚СЊ UPC", callback_data=f"m_add_upc_{user_id}_{idx}")]
                ])
                await context.bot.send_message(
                    chat_id=MODERATION_CHAT_ID,
                    text="рџ’ѕ <b>Р”РѕР±Р°РІСЊС‚Рµ UPC РєРѕРґ РґР»СЏ СЌС‚РѕРіРѕ СЂРµР»РёР·Р°</b>\n\n"
                         "РќР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ Рё РѕС‚РІРµС‚СЊС‚Рµ UPC РєРѕРґРѕРј РЅР° РёСЃС…РѕРґРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ Р°РЅРєРµС‚С‹.",
                    reply_to_message_id=query.message.message_id,
                    parse_mode=ParseMode.HTML,
                    reply_markup=upc_keyboard
                )
            except Exception as e:
                print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё РєРЅРѕРїРєРё UPC: {e}")

            # РЈРІРµРґРѕРјР»РµРЅРёРµ Р°СЂС‚РёСЃС‚Сѓ
            try:
                moderation_time = datetime.now().strftime("%d.%m.%Y РІ %H:%M")
                await context.bot.send_message(
                    int(user_id),
                    f"{WINTER_EMOJIS['check']} <b>Р’РђРЁ Р Р•Р›РР— РћР”РћР‘Р Р•Рќ!</b>\n\n"
                    f"рџ“ќ <b>{escape_html(release.get('name', 'вЂ”'))}</b>\n"
                    f"рџЋµ <i>РўРёРї:</i> {escape_html(release.get('type', 'вЂ”'))}\n"
                    f"рџ“… <i>Р”Р°С‚Р° СЂРµР»РёР·Р°:</i> {escape_html(release.get('date', 'вЂ”'))}\n"
                    f"рџ‘¤ <i>РђСЂС‚РёСЃС‚:</i> {escape_html(release.get('nick', 'вЂ”'))}\n"
                    f"рџ•ђ <i>РћРґРѕР±СЂРµРЅРѕ:</i> {escape_html(moderation_time)}\n"
                    f"рџ‘ЁвЂЌрџ’ј <i>РњРѕРґРµСЂР°С‚РѕСЂ:</i> @{escape_html(moderator_name)}\n\n"
                    f"{WINTER_EMOJIS['sparkles']} Р“РѕС‚РѕРІ Рє РїСѓР±Р»РёРєР°С†РёРё РЅР° РІСЃРµС… РїР»Р°С‚С„РѕСЂРјР°С…!",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё РїРѕР»СЊР·РѕРІР°С‚РµР»СЋ: {e}")
            return
        if action == "reject":
            # РћС‚РїСЂР°РІР»СЏРµРј РёРЅСЃС‚СЂСѓРєС†РёРѕРЅРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ РґР»СЏ РјРѕРґРµСЂР°С‚РѕСЂР°
            try:
                reject_instruction_msg = await context.bot.send_message(
                    chat_id=MODERATION_CHAT_ID,
                    text=f"{WINTER_EMOJIS.get('cross', 'вќЊ')} <b>Р’РІРµРґРёС‚Рµ РїСЂРёС‡РёРЅСѓ РѕС‚РєР»РѕРЅРµРЅРёСЏ Р°РЅРєРµС‚С‹</b>\n\n"
                         f"РћС‚РІРµС‚СЊС‚Рµ РЅР° СЌС‚Рѕ СЃРѕРѕР±С‰РµРЅРёРµ СЃ СЂР°Р·РІС‘СЂРЅСѓС‚РѕР№ РїСЂРёС‡РёРЅРѕР№ РѕС‚РєР»РѕРЅРµРЅРёСЏ СЂРµР»РёР·Р°.\n\n"
                         f"<i>Р РµР»РёР·:</i> <code>{escape_html(release.get('name', 'вЂ”')[:30])}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_to_message_id=query.message.message_id,
                )
                # РЎРѕС…СЂР°РЅСЏРµРј ID РёРЅСЃС‚СЂСѓРєС†РёРѕРЅРЅРѕРіРѕ СЃРѕРѕР±С‰РµРЅРёСЏ
                release['reject_instruction_message_id'] = reject_instruction_msg.message_id
                save_db(db)
            except Exception as e:
                print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё РёРЅСЃС‚СЂСѓРєС†РёРё РѕС‚РєР»РѕРЅРµРЅРёСЏ: {e}")
            
            await query.answer("вњ… РРЅСЃС‚СЂСѓРєС†РёСЏ РѕС‚РїСЂР°РІР»РµРЅР°. РћС‚РІРµС‚СЊС‚Рµ РЅР° РЅРµС‘ СЃ РїСЂРёС‡РёРЅРѕР№ РѕС‚РєР»РѕРЅРµРЅРёСЏ.", show_alert=False)
            return
        if action == "needfix":
            # Р‘С‹СЃС‚СЂР°СЏ РїРѕРјРµС‚РєР°: РїРѕРїСЂРѕСЃРёС‚СЊ РїСЂР°РІРєРё вЂ” РґРѕР±Р°РІРёРј РєРѕРјРјРµРЅС‚Р°СЂРёР№ Рё СѓРІРµРґРѕРјРёРј Р°РІС‚РѕСЂР°
            old_status = release.get("status")
            release["status"] = STATUS_NEEDS_FIX
            release["moderator"] = moderator_name
            release["moderation_time"] = datetime.now().isoformat()
            add_history_entry(user_id, idx, old_status, STATUS_NEEDS_FIX, query.from_user.id, moderator_name)
            save_db(db)
            update_moderation_record(user_id, idx, release)

            # РћР±РЅРѕРІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ РІ РјРѕРґРµСЂР°С†РёРё (СЃРѕС…СЂР°РЅСЏСЏ СЃСѓС‰РµСЃС‚РІСѓСЋС‰СѓСЋ РєР»Р°РІРёР°С‚СѓСЂСѓ)
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_NEEDS_FIX, moderator_username=moderator_name, reason="РўСЂРµР±СѓСЋС‚СЃСЏ РїСЂР°РІРєРё", reply_markup=query.message.reply_markup)

            # Р—Р°РјРµРЅСЏРµРј РєРЅРѕРїРєРё РЅР° "РР·РјРµРЅРёС‚СЊ СЃС‚Р°С‚СѓСЃ" РїРѕСЃР»Рµ РѕР±РЅРѕРІР»РµРЅРёСЏ С‚РµРєСЃС‚Р°
            edit_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("рџ”„ РР·РјРµРЅРёС‚СЊ СЃС‚Р°С‚СѓСЃ", callback_data=f"m_restore_buttons_{user_id}_{idx}")]
            ])
            await safe_edit_reply_markup(query, reply_markup=edit_keyboard)

            try:
                moderation_time = datetime.now().strftime("%d.%m.%Y РІ %H:%M")
                await context.bot.send_message(
                    int(user_id),
                    f"{WINTER_EMOJIS['warning']} <b>РўР Р•Р‘РЈР®РўРЎРЇ РџР РђР’РљР</b>\n\n"
                    f"рџ“ќ <b>{escape_html(release.get('name', 'вЂ”'))}</b>\n"
                    f"рџЋµ <i>РўРёРї:</i> {escape_html(release.get('type', 'вЂ”'))}\n"
                    f"рџ“… <i>Р”Р°С‚Р° СЂРµР»РёР·Р°:</i> {escape_html(release.get('date', 'вЂ”'))}\n"
                    f"рџ‘¤ <i>РђСЂС‚РёСЃС‚:</i> {escape_html(release.get('nick', 'вЂ”'))}\n"
                    f"рџ•ђ <i>Р’СЂРµРјСЏ:</i> {escape_html(moderation_time)}\n"
                    f"рџ‘ЁвЂЌрџ’ј <i>РњРѕРґРµСЂР°С‚РѕСЂ:</i> @{escape_html(moderator_name)}\n\n"
                    f"вќ— <b>Р’Р°С€ СЂРµР»РёР· С‚СЂРµР±СѓРµС‚ РґРѕСЂР°Р±РѕС‚РєРё. РџРѕР¶Р°Р»СѓР№СЃС‚Р°, РёСЃРїСЂР°РІСЊС‚Рµ Р·Р°РјРµС‡Р°РЅРёСЏ Рё РѕС‚РїСЂР°РІСЊС‚Рµ Р·Р°РЅРѕРІРѕ.</b>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё СѓРІРµРґРѕРјР»РµРЅРёСЏ Рѕ РїСЂР°РІРєР°С…: {e}")
            return

        if data == 'promo_regen':
            # Regenerate promo text for user
            user_id = str(query.from_user.id)
            p = user_data.get(user_id, {}).get('promo')
            if not p:
                await query.answer('РќРµС‚ РґР°РЅРЅС‹С… РґР»СЏ РіРµРЅРµСЂР°С†РёРё', show_alert=True)
                return
            prompt = (
                f"РЎРѕСЃС‚Р°РІСЊ РєРѕСЂРѕС‚РєРёР№ Р¶РёРІРѕР№ РїСЂРѕРјРѕ-С‚РµРєСЃС‚ РґР»СЏ Р»РµР№Р±Р»Р° РЅР° РѕСЃРЅРѕРІРµ РґР°РЅРЅС‹С…:\n"
                f"РђСЂС‚РёСЃС‚: {p.get('artist','')}\n"
                f"РџСЂРѕРµРєС‚: {p.get('project_type','')}\n"
                f"Р РµР»РёР·: {p.get('release_name','')}\n"
                f"РўРёРї: {p.get('release_kind','')}\n"
                f"Р–Р°РЅСЂ: {p.get('genre_main','')} {p.get('genre_extra','')}\n"
                f"РќР°СЃС‚СЂРѕРµРЅРёРµ: {p.get('mood','')}\n"
                f"Р’Р°Р№Р±: {p.get('vibe','')}\n"
                f"Р—РІСѓС‡Р°РЅРёРµ: {p.get('sound','')}\n"
                f"Р’РѕРєР°Р»: {p.get('vocal','')}\n"
                f"РЇР·С‹Рє: {p.get('language','')}\n"
                f"Р­РјРѕС†РёСЏ: {p.get('emotion','')}\n"
                f"Р“РґРµ СЃР»СѓС€Р°С‚СЊ: {p.get('usecase','') or 'вЂ”'}\n\n"
                f"РўСЂРµР±РѕРІР°РЅРёСЏ: Р¶РёРІРѕР№, С‡РµР»РѕРІРµС‡РµСЃРєРёР№ СЏР·С‹Рє, Р±РµР· РєР»РёС€Рµ, РїРѕРґС…РѕРґРёС‚ РґР»СЏ VK Music, РЇРЅРґРµРєСЃ РњСѓР·С‹РєРё Рё Р—РІСѓРєР°."
            )
            ai_text = await _call_openai_for_promo(prompt)
            if not ai_text:
                await query.answer('РћС€РёР±РєР° РіРµРЅРµСЂР°С†РёРё', show_alert=True)
                return
            try:
                await query.edit_message_text(ai_text, parse_mode=ParseMode.HTML, reply_markup=query.message.reply_markup)
            except Exception:
                await query.message.reply_text(ai_text, parse_mode=ParseMode.HTML)
            return

        if data == 'promo_accept':
            await query.answer('РћС‚Р»РёС‡РЅРѕ вЂ” СЃРѕС…СЂР°РЅРµРЅРѕ', show_alert=True)
            return

        if action == "link":
            # Р‘С‹СЃС‚СЂР°СЏ РїРѕРјРµС‚РєР°: РїСЂРѕР±Р»РµРјР° СЃРѕ СЃСЃС‹Р»РєРѕР№
            old_status = release.get("status")
            release["status"] = STATUS_NEEDS_FIX
            release["moderator"] = moderator_name
            release["moderation_time"] = datetime.now().isoformat()
            add_history_entry(user_id, idx, old_status, STATUS_NEEDS_FIX, query.from_user.id, moderator_name)
            save_db(db)

            await safe_edit_reply_markup(query, reply_markup=None)
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_NEEDS_FIX, moderator_username=moderator_name, reason="РџСЂРѕР±Р»РµРјР° СЃРѕ СЃСЃС‹Р»РєРѕР№", reply_markup=query.message.reply_markup)
            try:
                await context.bot.send_message(int(user_id), f"{WINTER_EMOJIS['warning']} <b>РџСЂРѕР±Р»РµРјР° СЃРѕ СЃСЃС‹Р»РєРѕР№</b>\n\nРџСЂРѕРІРµСЂСЊС‚Рµ СЃСЃС‹Р»РєСѓ РЅР° С„Р°Р№Р»С‹ РёР»Рё РєР°СЂС‚РѕС‡РєСѓ РЇРЅРґРµРєСЃ РњСѓР·С‹РєРё Рё РѕС‚РїСЂР°РІСЊС‚Рµ Р·Р°РЅРѕРІРѕ.")
            except Exception:
                pass
            return
        if action == "delete":
            old_status = release.get("status")
            release["status"] = STATUS_DELETED
            release["moderator"] = moderator_name
            release["moderation_time"] = datetime.now().isoformat()
            add_history_entry(user_id, idx, old_status, STATUS_DELETED, query.from_user.id, moderator_name)
            save_db(db)
            update_moderation_record(user_id, idx, release)

            # РћР±РЅРѕРІР»СЏРµРј СЃРѕРѕР±С‰РµРЅРёРµ РІ РјРѕРґРµСЂР°С†РёРё (СЃРѕС…СЂР°РЅСЏСЏ СЃСѓС‰РµСЃС‚РІСѓСЋС‰СѓСЋ РєР»Р°РІРёР°С‚СѓСЂСѓ)
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_DELETED, moderator_username=moderator_name, reason="РЎР»СѓР¶РµР±РЅРѕ СѓРґР°Р»РµРЅРѕ", reply_markup=query.message.reply_markup)
            
            # Р—Р°РјРµРЅСЏРµРј РєР»Р°РІРёР°С‚СѓСЂСѓ РЅР° РєРЅРѕРїРєСѓ "РР·РјРµРЅРёС‚СЊ СЃС‚Р°С‚СѓСЃ" РїРѕСЃР»Рµ РѕР±РЅРѕРІР»РµРЅРёСЏ С‚РµРєСЃС‚Р°
            edit_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("рџ”„ РР·РјРµРЅРёС‚СЊ СЃС‚Р°С‚СѓСЃ", callback_data=f"m_restore_buttons_{user_id}_{idx}")]
            ])
            await safe_edit_reply_markup(query, reply_markup=edit_keyboard)
            
            try:
                moderation_time = datetime.now().strftime("%d.%m.%Y РІ %H:%M")
                await context.bot.send_message(
                    int(user_id),
                    f"{WINTER_EMOJIS['delete']} <b>РђРќРљР•РўРђ РџРћРњР•Р§Р•РќРђ РљРђРљ РЈР”РђР›РЃРќРќРђРЇ</b>\n\n"
                    f"рџ“ќ <b>{escape_html(release.get('name', 'вЂ”'))}</b>\n"
                    f"рџЋµ <i>РўРёРї:</i> {escape_html(release.get('type', 'вЂ”'))}\n"
                    f"рџ‘¤ <i>РђСЂС‚РёСЃС‚:</i> {escape_html(release.get('nick', 'вЂ”'))}\n"
                    f"рџ•ђ <i>РЈРґР°Р»РµРЅРѕ:</i> {escape_html(moderation_time)}\n"
                    f"рџ‘ЁвЂЌрџ’ј <i>РњРѕРґРµСЂР°С‚РѕСЂ:</i> @{escape_html(moderator_name)}\n\n"
                    f"Р•СЃР»Рё СЌС‚Рѕ РѕС€РёР±РєР° вЂ” СЃРІСЏР¶РёС‚РµСЃСЊ СЃ РјРѕРґРµСЂР°С‚РѕСЂР°РјРё.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё СѓРІРµРґРѕРјР»РµРЅРёСЏ РѕР± СѓРґР°Р»РµРЅРёРё: {e}")
            return
        
        if action == "add_upc":
            # РћС‚РїСЂР°РІР»СЏРµРј РёРЅСЃС‚СЂСѓРєС†РёРѕРЅРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ, РЅР° РєРѕС‚РѕСЂРѕРµ РЅСѓР¶РЅРѕ РѕС‚РІРµС‚РёС‚СЊ СЃ UPC РєРѕРґРѕРј
            try:
                upc_instruction_msg = await context.bot.send_message(
                    chat_id=MODERATION_CHAT_ID,
                    text=f"{WINTER_EMOJIS.get('waiting', 'вЏі')} <b>Р’РІРµРґРёС‚Рµ UPC РєРѕРґ РґР»СЏ СЌС‚РѕРіРѕ СЂРµР»РёР·Р°</b>\n\n"
                         f"РћС‚РІРµС‚СЊС‚Рµ РЅР° СЌС‚Рѕ СЃРѕРѕР±С‰РµРЅРёРµ СЃ UPC РєРѕРґРѕРј (С‚РѕР»СЊРєРѕ С†РёС„СЂС‹, РЅР°РїСЂРёРјРµСЂ: <code>5099994682101</code>)\n\n"
                         f"<i>л¦ґл¦¬м¦€:</i> <code>{escape_html(release.get('name', 'вЂ”')[:30])}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_to_message_id=query.message.message_id,
                )
                # РЎРѕС…СЂР°РЅСЏРµРј ID РёРЅСЃС‚СЂСѓРєС†РёРѕРЅРЅРѕРіРѕ СЃРѕРѕР±С‰РµРЅРёСЏ РІ Р‘Р” РґР»СЏ РїРѕСЃР»РµРґСѓСЋС‰РµРіРѕ РїРѕРёСЃРєР°
                release['upc_instruction_message_id'] = upc_instruction_msg.message_id
                save_db(db)
            except Exception as e:
                print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё РёРЅСЃС‚СЂСѓРєС†РёРё UPC: {e}")
            
            await query.answer("вњ… РРЅСЃС‚СЂСѓРєС†РёСЏ РѕС‚РїСЂР°РІР»РµРЅР°. РћС‚РІРµС‚СЊС‚Рµ РЅР° РЅРµС‘ СЃ UPC РєРѕРґРѕРј.", show_alert=False)
            return
        
        if action == "restore_buttons":
            # Р’РѕСЃСЃС‚Р°РЅР°РІР»РёРІР°РµРј РёСЃС…РѕРґРЅС‹Рµ РєРЅРѕРїРєРё СЃС‚Р°С‚СѓСЃРѕРІ РІРјРµСЃС‚Рѕ "РР·РјРµРЅРёС‚СЊ СЃС‚Р°С‚СѓСЃ"
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("рџ•“ РќР° РѕС‚РіСЂСѓР·РєРµ", callback_data=f"m_upload_{user_id}_{idx}"),
                    InlineKeyboardButton("рџ§  РњРѕРґРµСЂР°С†РёСЏ", callback_data=f"m_moderate_{user_id}_{idx}"),
                    InlineKeyboardButton("вњ… РџСЂРёРЅСЏС‚Рѕ", callback_data=f"m_approve_{user_id}_{idx}")
                ],
                [
                    InlineKeyboardButton("вќЊ РћС‚РєР»РѕРЅРёС‚СЊ", callback_data=f"m_reject_{user_id}_{idx}"),
                    InlineKeyboardButton("вњЏпёЏ РќР° РёСЃРїСЂР°РІР»РµРЅРёРё", callback_data=f"m_needfix_{user_id}_{idx}"),
                    InlineKeyboardButton("рџ—‘ РЈРґР°Р»РµРЅ", callback_data=f"m_delete_{user_id}_{idx}")
                ],
            ])
            await safe_edit_reply_markup(query, reply_markup=keyboard)
            await query.answer("вњ… РљРЅРѕРїРєРё РІРѕСЃСЃС‚Р°РЅРѕРІР»РµРЅС‹", show_alert=False)
            return
    except Exception as e:
        import traceback
        print(f"вќЊ РћС€РёР±РєР° РІ moderation_handler: {e}")
        traceback.print_exception(type(e), e, e.__traceback__)
        try:
            await query.answer("РџСЂРѕРёР·РѕС€Р»Р° РѕС€РёР±РєР° РїСЂРё РѕР±СЂР°Р±РѕС‚РєРµ", show_alert=True)
        except:
            pass

# === РћР‘Р РђР‘РћРўРљРђ РћРЁРР‘РћРљ ===
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    import traceback
    # РќРµ СЃРїР°РјРёРј РІ РєРѕРЅСЃРѕР»СЊ РѕС€РёР±РєРѕР№ RemoteProtocolError РєР°Рє "С„Р°С‚Р°Р»СЊРЅРѕР№"
    if context.error and _is_remote_protocol_error(context.error):
        print("вљ пёЏ RemoteProtocolError: СЃРµСЂРІРµСЂ СЂР°Р·РѕСЂРІР°Р» СЃРѕРµРґРёРЅРµРЅРёРµ Р±РµР· РѕС‚РІРµС‚Р° (РїРѕР№РјР°Р»Рё Рё РїРµСЂРµР¶РёР»Рё).")
        return
    print(f"вќЊ РћС€РёР±РєР°: {context.error}")
    if context.error:
        traceback.print_exception(type(context.error), context.error, context.error.__traceback__)


# === РќРђРџРћРњРќРРўР•Р›Р¬ Рћ РќРђ РћРўР“Р РЈР—РљР• ===
async def _check_on_upload_reminders(context: ContextTypes.DEFAULT_TYPE):
    try:
        now = datetime.now()
        for uid, rels in db.items():
            for idx, r in enumerate(rels):
                try:
                    if r.get('status') != STATUS_ON_UPLOAD:
                        continue
                    st = r.get('submission_time')
                    if not st:
                        continue
                    submit_time = datetime.fromisoformat(st)
                    hours_passed = (now - submit_time).total_seconds() / 3600
                    
                    if hours_passed > 48 and not r.get('reminder_sent'):
                        msg_id = r.get('moderation_message_id')
                        release_name = escape_html(r.get('name', 'РђРЅРєРµС‚Р°'))
                        artist_name = escape_html(r.get('nick', 'РђСЂС‚РёСЃС‚'))
                        submission_time_str = submit_time.strftime("%d.%m.%Y РІ %H:%M")
                        
                        try:
                            reminder_text = (
                                f"вЏ° <b>РќРђРџРћРњРРќРђРќРР•</b>\n\n"
                                f"рџЋµ <b>{release_name}</b>\n"
                                f"рџ‘¤ РђСЂС‚РёСЃС‚: {artist_name}\n"
                                f"рџ“… РћС‚РїСЂР°РІР»РµРЅРѕ: {submission_time_str}\n"
                                f"вЏ±пёЏ РџСЂРѕС€Р»Рѕ: {int(hours_passed)} С‡Р°СЃРѕРІ\n\n"
                                f"вќ— РђРЅРєРµС‚Р° РЅР°С…РѕРґРёС‚СЃСЏ РЅР° РѕС‚РіСЂСѓР·РєРµ Р±РѕР»РµРµ 2 РґРЅРµР№!\n"
                                f"РќРµРѕР±С…РѕРґРёРјРѕ РїСЂРѕРІРµСЃС‚Рё Р·Р°РіСЂСѓР·РєСѓ РЅР° РїР»Р°С‚С„РѕСЂРјС‹."
                            )
                            await context.bot.send_message(
                                chat_id=MODERATION_CHAT_ID,
                                text=reminder_text,
                                reply_to_message_id=msg_id,
                                parse_mode=ParseMode.HTML
                            )
                            r['reminder_sent'] = True
                        except Exception as e:
                            print(f"РћС€РёР±РєР° РѕС‚РїСЂР°РІРєРё РЅР°РїРѕРјРёРЅР°РЅРёСЏ: {e}")
                except Exception:
                    continue
        save_db(db)
    except Exception as e:
        print(f"РћС€РёР±РєР° РІ РЅР°РїРѕРјРёРЅР°С‚РµР»Рµ on_upload: {e}")


async def undo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # РћС‚РєР°С‚С‹РІР°РµС‚ РїРѕСЃР»РµРґРЅРµРµ СЃРѕС…СЂР°РЅС‘РЅРЅРѕРµ Р·РЅР°С‡РµРЅРёРµ РґР»СЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ
    user_id = str(update.message.from_user.id)
    last = pop_last_history(user_id)
    if not last:
        await update.message.reply_text("РќРµС‚ РїСЂРµРґС‹РґСѓС‰РёС… Р·РЅР°С‡РµРЅРёР№ РґР»СЏ РѕС‚РјРµРЅС‹.")
        return
    key, old = last
    user_data.setdefault(user_id, {})[key] = old
    save_draft_for_user(user_id)
    await update.message.reply_text(f"РџРѕР»Рµ '{key}' РІРѕСЃСЃС‚Р°РЅРѕРІР»РµРЅРѕ.")

# === Р—РђРџРЈРЎРљ ===
def main():
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN РЅРµ Р·Р°РґР°РЅ. РЈСЃС‚Р°РЅРѕРІРёС‚Рµ РїРµСЂРµРјРµРЅРЅСѓСЋ РѕРєСЂСѓР¶РµРЅРёСЏ BOT_TOKEN.")

    app = Application.builder().token(TOKEN).read_timeout(120).build()
    
    app.add_handler(CommandHandler('help', help_cmd))
    app.add_handler(CommandHandler('cancel', cancel_cmd))
    app.add_handler(CommandHandler('my', my_cmd))
    app.add_handler(CommandHandler('search', search_cmd))
    app.add_handler(CommandHandler('app', app_cmd))
    app.add_handler(CommandHandler('admin', admin_panel))
    app.add_handler(CommandHandler('backup', backup_cmd))
    app.add_handler(CommandHandler('moderation_backup', moderation_backup_cmd))
    # FIX: /stats РїРµСЂРµРёРјРµРЅРѕРІР°РЅР° РЅР° /statss (СЂР°Р±РѕС‚Р°РµС‚ С‚РѕР»СЊРєРѕ РІ С‡Р°С‚Рµ РјРѕРґРµСЂР°С†РёРё РґР»СЏ Р°РґРјРёРЅРѕРІ)
    app.add_handler(CommandHandler('statss', admin_stats_cmd))
    app.add_handler(CommandHandler('broadcast', broadcast_cmd))
    app.add_handler(CommandHandler('cleanbase', cleanbase_cmd))
    app.add_handler(CommandHandler('undo', undo_cmd))
    app.add_handler(CommandHandler('cleanup', cleanup_database))
    app.add_handler(CommandHandler('check_openai', check_openai_cmd))

    # FIX: РњРѕРґРµСЂР°С†РёСЏ Р”РћР›Р–РќРђ Р±С‹С‚СЊ РџР•Р Р’Р«Рњ РѕР±СЂР°Р±РѕС‚С‡РёРєРѕРј РґРѕ ConversationHandler Рё РіР»РѕР±Р°Р»СЊРЅРѕРіРѕ button
    # РњРѕРґРµСЂР°С†РёСЏ: РѕС‚РґРµР»СЊРЅС‹Р№ handler РїРѕ РїР°С‚С‚РµСЂРЅСѓ m_*
    app.add_handler(CallbackQueryHandler(moderation_handler, pattern=r"^m_.*"))
    # Mini App payload from Telegram WebApp.sendData(...)
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_data_handler))
    # FIX: РћР±СЂР°Р±РѕС‚С‡РёРє РґРѕР±Р°РІР»РµРЅРёСЏ UPC РєРѕРґР° С‡РµСЂРµР· reply РІ С‡Р°С‚Рµ РјРѕРґРµСЂР°С†РёРё (РїСЂРѕРІРµСЂСЏРµРј РїРѕ РЈРџРљ-РїРѕРґРѕР±РЅРѕРјСѓ РєРѕРґСѓ)
    app.add_handler(MessageHandler(filters.TEXT & filters.REPLY & filters.Chat(MODERATION_CHAT_ID) & ~filters.COMMAND, add_upc_handler), group=1)
    # FIX: РћР±СЂР°Р±РѕС‚С‡РёРє СЂСѓС‡РЅРѕРіРѕ РѕС‚РєР»РѕРЅРµРЅРёСЏ Р°РЅРєРµС‚С‹ С‡РµСЂРµР· reply РІ С‡Р°С‚Рµ РјРѕРґРµСЂР°С†РёРё
    app.add_handler(MessageHandler(filters.TEXT & filters.REPLY & filters.Chat(MODERATION_CHAT_ID), manual_reject_handler), group=2)

    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start_cmd), CallbackQueryHandler(button, pattern=r'^promo_text$')],
        states={
            REPORT: [CallbackQueryHandler(button)],
            TYPE: [CallbackQueryHandler(button)],
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, name)],
            SUBNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, subname), CallbackQueryHandler(button)],
            HAS_LYRICS: [CallbackQueryHandler(button)],
            NICK: [MessageHandler(filters.TEXT & ~filters.COMMAND, nick)],
            FIO: [MessageHandler(filters.TEXT & ~filters.COMMAND, fio)],
            DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, date)],
            VERSION: [MessageHandler(filters.TEXT & ~filters.COMMAND, version)],
            GENRE: [MessageHandler(filters.TEXT & ~filters.COMMAND, genre)],
            LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, link)],
            YANDEX: [MessageHandler(filters.TEXT & ~filters.COMMAND, yandex)],
            MAT: [CallbackQueryHandler(mat)],
            PROMO: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo)],
            COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, comment)],
            TRACKLIST: [MessageHandler(filters.TEXT & ~filters.COMMAND, tracklist)],
            TG: [MessageHandler(filters.TEXT & ~filters.COMMAND, tg)],
            # Cover order flow
            COVER_COLORS: [MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, cover_colors_handler)],
            COVER_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, cover_title_handler)],
            COVER_PREFS: [MessageHandler(filters.TEXT & ~filters.COMMAND, cover_prefs_handler)],
            COVER_TG: [MessageHandler(filters.TEXT & ~filters.COMMAND, cover_tg_handler)],
            COVER_PAYMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, cover_payment_handler)],
            COVER_WAIT_SCREENSHOT: [MessageHandler(filters.PHOTO & ~filters.COMMAND, cover_screenshot_handler)],
            # Promo flow
            PROMO_PROJECT: [CallbackQueryHandler(button), MessageHandler(filters.TEXT & ~filters.COMMAND, promo_project_handler)],
            PROMO_RELEASE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_release_name_handler)],
            PROMO_RELEASE_KIND: [CallbackQueryHandler(button), MessageHandler(filters.TEXT & ~filters.COMMAND, promo_release_kind_handler)],
            PROMO_GENRE_MAIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_genre_main_handler)],
            PROMO_GENRE_EXTRA: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_genre_extra_handler)],
            PROMO_MOOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_mood_handler)],
            PROMO_VIBE: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_vibe_handler)],
            PROMO_SOUND: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_sound_handler)],
            PROMO_VOCAL: [CallbackQueryHandler(button), MessageHandler(filters.TEXT & ~filters.COMMAND, promo_vocal_handler)],
            PROMO_EMOTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_emotion_handler)],
            PROMO_USECASE: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_usecase_handler)],
            PROMO_COUNTRY: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo_country_handler)],
            CONFIRM: [CallbackQueryHandler(button)],
        },
        fallbacks=[CommandHandler('start', start_cmd), CommandHandler('cancel', cancel_cmd)],
        per_message=False,
        per_chat=True,
    )
    
    app.add_handler(conv)
    # Р“Р›РћР‘РђР›Р¬РќРћ: С‡С‚РѕР±С‹ /admin РєРЅРѕРїРєРё СЂР°Р±РѕС‚Р°Р»Рё РґР°Р¶Рµ РµСЃР»Рё РїРѕР»СЊР·РѕРІР°С‚РµР»СЊ РЅРµ РІ ConversationHandler state.
    app.add_handler(CallbackQueryHandler(button))
    # FIX: error_handler РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ РІ РєРѕРЅС†Рµ
    app.add_error_handler(error_handler)
    # Р РµРіРёСЃС‚СЂР°С†РёСЏ С„РѕРЅРѕРІРѕР№ Р·Р°РґР°С‡Рё: РЅР°РїРѕРјРёРЅР°РЅРёСЏ РїРѕ РєР°СЂС‚РѕС‡РєР°Рј РЅР° РѕС‚РіСЂСѓР·РєРµ (РєР°Р¶РґС‹Рµ 30 РјРёРЅСѓС‚)
    try:
        app.job_queue.run_repeating(_check_on_upload_reminders, interval=30*60, first=60)
    except Exception:
        # Р•СЃР»Рё РѕС‡РµСЂРµРґСЊ РЅРµ РґРѕСЃС‚СѓРїРЅР° вЂ” РЅРµ РєСЂРёС‚РёС‡РЅРѕ
        pass
    
    # Startup checks for OpenAI/httpx
    status = _check_openai_status()
    if not status['has_key']:
        print("вљ пёЏ OPENAI_API_KEY is not set вЂ” promo generation will be disabled until you set it.")
    if not status['httpx_available']:
        print("вљ пёЏ httpx is not available вЂ” OpenAI calls will be skipped. Install httpx to enable AI generation.")
    if not is_webapp_url_ready():
        print("вљ пёЏ WEBAPP_URL is not configured (or points to example.com). Mini App button is hidden.")
    print(f"{WINTER_EMOJIS['snowflake']} Р‘РћРў Р—РђРџРЈР©Р•Рќ! {WINTER_EMOJIS['snowflake']}")
    # Ensure no webhook is active for this bot (prevents "Conflict: terminated by other getUpdates request").
    try:
        def _ensure_no_webhook(token: str):
            url_info = f"https://api.telegram.org/bot{token}/getWebhookInfo"
            try:
                if httpx is not None:
                    r = httpx.get(url_info, timeout=5.0)
                    j = r.json()
                else:
                    # fallback to stdlib
                    from urllib.request import urlopen
                    import json as _json

                    with urlopen(url_info, timeout=5) as fh:
                        j = _json.load(fh)
            except Exception:
                return

            if not j or not j.get('ok'):
                return
            result = j.get('result') or {}
            webhook_url = result.get('url')
            if webhook_url:
                print('вљ пёЏ Active webhook detected for this bot. Deleting...')
                url_del = f"https://api.telegram.org/bot{token}/deleteWebhook"
                try:
                    if httpx is not None:
                        httpx.get(url_del, timeout=5.0)
                    else:
                        from urllib.request import urlopen

                        with urlopen(url_del, timeout=5) as _:
                            pass
                    print('вњ… Webhook deleted.')
                except Exception:
                    print('вќЊ Failed to delete webhook automatically. Please remove webhook manually.')

        _ensure_no_webhook(TOKEN)
    except Exception:
        pass

    static_server = start_static_web_server_if_enabled()
    try:
        app.run_polling(drop_pending_updates=True)
    finally:
        if static_server:
            try:
                static_server.shutdown()
                static_server.server_close()
                print("рџЊђ Static Mini App server stopped.")
            except Exception as e:
                print(f"вљ пёЏ РћС€РёР±РєР° РѕСЃС‚Р°РЅРѕРІРєРё static server: {e}")

if __name__ == '__main__':
    main()
