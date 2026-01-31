import asyncio
import json
import os
import re
import tempfile
from datetime import datetime, timedelta
from urllib.parse import urlparse

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
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
    # python-telegram-bot 21.x использует httpx внутри, иногда пробрасывает ошибки протокола.
    import httpx  # type: ignore
except Exception:  # pragma: no cover
    httpx = None

# === КОНФИГ ===
TOKEN = os.getenv("BOT_TOKEN", "7932680631:AAG3DW6gwg0Ccvuiq45aPVCSSWsOallp_Pk")
MODERATION_CHAT_ID = -1002117586464
ADMIN_IDS = [881379104]
ARTISTS_CHAT = "https://t.me/+oVmX3_dkyWJhNjJi"
CHANNEL = "https://t.me/cxrnermusic"
DB_FILE = "releases.json"
MODERATION_DB_FILE = "moderation_releases.json"
HISTORY_FILE = "history.json"

# === ЗИМНИЕ ЭМОДЗИ ===
WINTER_EMOJIS = {
    "snowflake": "❄️",
    "snowman": "⛄️",
    "tree": "🎄",
    "gift": "🎁",
    "sparkles": "✨",
    "star": "⭐️",
    "fire": "🔥",
    "notes": "🎵",
    "headphones": "🎧",
    "clock": "⏰",
    "check": "✅",
    "cross": "❌",
    "music": "🎶",
    "waiting": "⏳",
    "published": "📢",
    "calendar": "📅",
    "warning": "⚠️",
    "comment": "💬",
    "telegram": "📱",
    "list": "📋",
    "users": "👥",
    "stats": "📊",
    "settings": "⚙️",
    "refresh": "🔄",
    "brain": "🧠",
    "upload": "🕓",
    "delete": "🗑"
}

# === СОСТОЯНИЯ ===
# NOTE: Сохраняем ConversationHandler, но делаем callback-роутер глобальным (чтобы /admin кнопки работали вне диалога).
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

# Статусы анкет (используйте эти значения в `status` полях)
STATUS_ON_UPLOAD = "on_upload"      # На отгрузке (поставляется при отправке)
STATUS_MODERATION = "moderation"    # На модерации (модератор взял в работу)
STATUS_APPROVED = "approved"        # Одобрено
STATUS_REJECTED = "rejected"        # Отклонено
STATUS_NEEDS_FIX = "needs_fix"      # На исправлении
STATUS_DELETED = "deleted"          # Удалено (служебно)

# === БД / ХРАНИЛИЩЕ ===
# Главная причина “пропадают релизы/кабинеты”: неатомарная запись JSON + возможные частичные записи/коррупция.
# Делаем атомарный сейв (temp + os.replace), а также safe-load с резервной копией.
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
        # Если файл частично записался/сломался — не падаем и не затираем данными в памяти.
        print(f"❌ Ошибка чтения {path}: {e}")
        return default


def load_db():
    return _load_json_or_default(DB_FILE, {})


def save_db(db_obj):
    _atomic_write_json(DB_FILE, db_obj)


def load_moderation_db():
    return _load_json_or_default(MODERATION_DB_FILE, {"moderation_messages": []})


def save_moderation_db(moderation_db_obj):
    _atomic_write_json(MODERATION_DB_FILE, moderation_db_obj)

def update_moderation_record(user_id, idx, release_data):
    """Обновляет запись в moderation_releases.json при изменении статуса"""
    try:
        moderation_db = load_moderation_db()
        if 'moderation_messages' in moderation_db:
            for msg in moderation_db['moderation_messages']:
                if msg.get('user_id') == user_id:
                    # Сравниваем submission_time как ID релиза
                    if msg.get('submission_time') == release_data.get('submission_time'):
                        # Обновляем статус
                        msg['status'] = release_data.get('status')
                        msg['moderator'] = release_data.get('moderator')
                        msg['moderation_time'] = release_data.get('moderation_time')
                        msg['reject_reason'] = release_data.get('reject_reason')
                        save_moderation_db(moderation_db)
                        break
    except Exception as e:
        print(f"Ошибка при обновлении записи в модерации: {e}")

# === ИСТОРИЯ ИЗМЕНЕНИЙ ===
def load_history():
    return _load_json_or_default(HISTORY_FILE, {})

def save_history(history):
    _atomic_write_json(HISTORY_FILE, history)

def add_history_entry(user_id, idx, old_status, new_status, moderator_id, moderator_name, reason=None):
    """Добавляет запись в историю изменений"""
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

# === DRAFTS (автосохранение промежуточных данных) ===
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

def pop_last_history(user_id: str):
    hist = user_data.get(user_id, {}).get('_history', [])
    if not hist:
        return None
    last = hist.pop()
    # update stored history
    user_data[user_id]['_history'] = hist
    return last


# === ЭКРАНИРОВАНИЕ HTML ===
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
    # Проверяем наличие drive.google.com или docs.google.com в любой части URL
    return ("drive.google.com" in lower or 
            "docs.google.com" in lower or 
            "drive.google" in lower or
            "/d/" in text)  # Google Drive файл/папка всегда содержит /d/


def _looks_like_yandex_music_link(text: str) -> bool:
    if not _looks_like_url(text):
        return False
    lower = text.lower()
    return "music.yandex" in lower or "yandex.ru" in lower

# === БЕЗОПАСНАЯ ОТПРАВКА / РЕТРАИ (в т.ч. httpx.RemoteProtocolError) ===
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
    # PTB может пробрасывать httpx.RemoteProtocolError как context.error или внутри исключений.
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
            # Главное: не показывать пользователю httpx.RemoteProtocolError, просто ретраим.
            if _is_remote_protocol_error(e):
                await asyncio.sleep(1 + attempt)
                last = e
                continue
            await message.reply_text(_strip_html(text), reply_markup=reply_markup, disable_web_page_preview=True)
            return
    await message.reply_text("Не удалось отправить. Попробуйте ещё раз.")
    if "last" in locals():
        print(f"❌ safe_send: {last}")


async def safe_edit(query, text, reply_markup=None, parse_mode=ParseMode.HTML):
    for attempt in range(5):
        try:
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            return
        except (TimedOut,) as e:
            await asyncio.sleep(1 + attempt)
            last = e
        except (BadRequest, Forbidden) as e:
            # Иногда нельзя редактировать (например, слишком старое сообщение) — шлём новым сообщением.
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
        print(f"❌ safe_edit: {last}")


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
            print(f"❌ safe_edit_reply_markup: {e}")
            return
    if "last" in locals():
        print(f"❌ safe_edit_reply_markup: {last}")

# === ЗИМНЕЕ ОФОРМЛЕНИЕ ===
def winter_text(text, emoji_key=None):
    if emoji_key and emoji_key in WINTER_EMOJIS:
        return f"{WINTER_EMOJIS[emoji_key]} {text}"
    return text

def winter_header(text):
    return f"{WINTER_EMOJIS['snowflake']} {text} {WINTER_EMOJIS['snowflake']}"

# === ПРОВЕРКА АДМИНА ===
def is_admin(user_id):
    """Проверяет, является ли пользователь администратором"""
    # Преобразуем user_id в int для корректного сравнения (может быть строкой или int)
    try:
        user_id_int = int(user_id) if user_id else None
        if user_id_int is None:
            return False
        result = user_id_int in ADMIN_IDS
        # Логируем для отладки (можно убрать после проверки)
        if result:
            print(f"✅ Доступ разрешен для админа: {user_id_int}")
        return result
    except (ValueError, TypeError) as e:
        print(f"❌ Ошибка проверки админа для {user_id}: {e}")
        return False

# === ГЛАВНОЕ МЕНЮ (/start) ===
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Отправить релиз", "music"), callback_data='report')],
        [InlineKeyboardButton(winter_text("Мои релизы", "notes"), callback_data='my_releases')],
        [InlineKeyboardButton(winter_text("Канал", "published"), url=CHANNEL)],
        [InlineKeyboardButton(winter_text("Чат артистов", "headphones"), url=ARTISTS_CHAT)]
    ])
    
    welcome_text = f"""
{winter_header("CXRNER MUSIC")}

{escape_html("Добро пожаловать в зимнюю студию музыки!")} {WINTER_EMOJIS['tree']}

{escape_html("Выберите действие:")}
"""
    
    if update.message:
        await update.message.reply_text(
            welcome_text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
    elif update.callback_query:
        await safe_edit(update.callback_query, welcome_text, reply_markup=keyboard)
    return REPORT

# === КОМАНДА /help ===
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = f"""
{winter_header("СПРАВКА ПО КОМАНДАМ")}

{WINTER_EMOJIS['music']} <b>ОСНОВНЫЕ КОМАНДЫ:</b>
/start - Главное меню
/my - Мои релизы и статистика
/search &lt;название&gt; - Поиск релизов
/cancel - Отменить текущее действие
/help - Эта справка

{WINTER_EMOJIS['notes']} <b>КАК ОТПРАВИТЬ РЕЛИЗ:</b>
1. Нажмите /start
2. Выберите "Отправить релиз"
3. Выберите тип (Сингл или Альбом)
4. Заполните все поля
5. Подтвердите отправку

{WINTER_EMOJIS['waiting']} <b>СТАТУСЫ РЕЛИЗОВ:</b>
⏳ Ожидает - на модерации
✅ Одобрено - готов к публикации
❌ Отклонено - требует исправлений
📢 Опубликовано - уже в канале

{WINTER_EMOJIS['sparkles']} <b>НУЖНА ПОМОЩЬ?</b>
Напишите в чат артистов или используйте /start
"""
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Главное меню", "tree"), callback_data='main')],
        [InlineKeyboardButton(winter_text("Отправить релиз", "music"), callback_data='report')],
        [InlineKeyboardButton(winter_text("Мои релизы", "notes"), callback_data='my_releases')]
    ])
    
    await update.message.reply_text(
        help_text,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

# === КОМАНДА /cancel ===
async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    
    # Очищаем данные пользователя
    if user_id in user_data:
        del user_data[user_id]
    
    # Сбрасываем состояние
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Главное меню", "tree"), callback_data='main')]
    ])
    
    text = (
        f"{WINTER_EMOJIS['check']} <b>Действие отменено!</b>\n\n"
        f"Все несохраненные данные удалены.\n"
        f"Можете начать заново с /start"
    )
    
    await update.message.reply_text(
        text,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )
    return ConversationHandler.END

# === КОМАНДА /search ===
async def search_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    
    if not context.args:
        await update.message.reply_text(
            f"{WINTER_EMOJIS['warning']} <b>Использование:</b>\n"
            f"<code>/search название релиза</code>\n"
            f"<code>/search артист</code>\n\n"
            f"Пример: <code>/search Tokyo Rain</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    search_query = ' '.join(context.args).lower()
    user_releases = db.get(user_id, [])
    
    if not user_releases:
        await update.message.reply_text(
            f"{WINTER_EMOJIS['notes']} <b>У вас пока нет релизов.</b>\n\n"
            f"Используйте /start чтобы отправить первый!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Поиск по названию и артисту
    found_releases = []
    for idx, release in enumerate(user_releases):
        name = release.get('name', '').lower()
        nick = release.get('nick', '').lower()
        
        if search_query in name or search_query in nick:
            found_releases.append((idx, release))
    
    if not found_releases:
        await update.message.reply_text(
            f"{WINTER_EMOJIS['cross']} <b>Ничего не найдено!</b>\n\n"
            f"По запросу <b>\"{escape_html(search_query)}\"</b> релизов не найдено.\n\n"
            f"Попробуйте другой поисковый запрос.",
            parse_mode=ParseMode.HTML
        )
        return
    
    text = f"{WINTER_EMOJIS['notes']} <b>НАЙДЕНО РЕЛИЗОВ: {len(found_releases)}</b>\n\n"
    
    status_emoji = {
        "pending": WINTER_EMOJIS['waiting'],
        "approved": WINTER_EMOJIS['check'],
        "rejected": WINTER_EMOJIS['cross'],
        "published": WINTER_EMOJIS['published']
    }
    
    for idx, release in found_releases[:10]:  # Ограничиваем 10 результатами
        status = release.get('status', 'pending')
        emoji = status_emoji.get(status, WINTER_EMOJIS['waiting'])
        status_text = {
            "pending": "Ожидает",
            "approved": "Одобрено",
            "rejected": "Отклонено",
            "published": "Опубликовано"
        }.get(status, "Ожидает")
        
        link = f"\n<a href='{release.get('link_published', '')}'>Слушать</a>" if status == 'published' and release.get('link_published') else ""
        
        text += (
            f"<b>{escape_html(release.get('name', 'Без названия'))}</b> {emoji}\n"
            f"<i>Артист:</i> {escape_html(release.get('nick', '—'))}\n"
            f"<i>Тип:</i> {escape_html(release.get('type', '—'))}\n"
            f"<i>Дата:</i> {escape_html(release.get('date', '—'))}\n"
            f"<i>Статус:</i> {escape_html(status_text)}{link}\n\n"
        )
    
    if len(found_releases) > 10:
        text += f"<i>... и ещё {len(found_releases) - 10} релизов</i>"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Все мои релизы", "notes"), callback_data='my_releases')],
        [InlineKeyboardButton(winter_text("Главное меню", "tree"), callback_data='main')]
    ])
    
    await update.message.reply_text(
        text,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

# === МОИ РЕЛИЗЫ (/my) ===
async def my_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, page=0):
    # Поддержка как message, так и callback_query
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
    # Фильтруем релизы, помеченные как удалённые пользователем
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
    
    # Расчет процентов
    approved_pct = (approved * 100 / total) if total > 0 else 0

    # Красивый заголовок со статистикой в столбец
    header = (
        f"{WINTER_EMOJIS['headphones']} <b>МОЙ КАБИНЕТ</b> • {total} релизов\n"
        f"✅ Одобрено: {approved} ({approved_pct:.0f}%)\n"
        f"⏳ На отгрузке: {on_upload}\n"
        f"🧠 На модерации: {moderation}\n"
        f"⚠️ На правках: {needs_fix}\n"
        f"❌ Отклонено: {rejected}"
    )

    if not visible_releases:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Отправить релиз", callback_data='report')],
            [InlineKeyboardButton("◀ Главное меню", callback_data='main')]
        ])
        await message.reply_text(
            f"{header}\n\n<i>Релизов пока нет</i>\n\n"
            f"Создайте свой первый релиз, нажав кнопку ниже!",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )
        return

    # Показываем одну карточку на странице (пагинация)
    page = max(0, min(page, total - 1))  # Защита от выхода за границы
    rel = visible_releases[page]
    
    status = rel.get('status', STATUS_ON_UPLOAD)
    status_emoji = {
        STATUS_ON_UPLOAD: "⏳",
        STATUS_MODERATION: "🧠",
        STATUS_APPROVED: "✅",
        STATUS_REJECTED: "❌",
        STATUS_NEEDS_FIX: "⚠️",
    }
    
    status_names = {
        STATUS_ON_UPLOAD: 'На отгрузке',
        STATUS_MODERATION: 'На модерации',
        STATUS_APPROVED: 'Одобрено ✓',
        STATUS_REJECTED: 'Отклонено',
        STATUS_NEEDS_FIX: 'На правках',
    }
    
    emoji = status_emoji.get(status, "⏳")
    status_text = status_names.get(status, '?')
    
    # Карточка релиза
    rel_name = escape_html(rel.get('name', 'Релиз'))
    rel_type = escape_html(rel.get('type', 'Релиз'))
    
    text = header + "\n\n"
    text += f"<b>🎵 {rel_name}</b>\n"
    text += f"📝 Тип: <i>{rel_type}</i>\n"
    
    if rel.get('subname') and rel.get('subname') != '.':
        text += f"🎙️ Версия: <i>{escape_html(rel.get('subname'))}</i>\n"
    
    text += f"📅 Дата: <i>{escape_html(rel.get('date', '—'))}</i>\n"
    text += f"👤 Артист: <i>{escape_html(rel.get('nick', '—'))}</i>\n"
    text += f"🏷️ Жанр: <i>{escape_html(rel.get('genre', '—'))}</i>\n"
    
    # UPC код
    upc = rel.get('upc', '')
    if upc and upc != '.':
        text += f"📦 UPC: <i>{escape_html(upc)}</i>\n"
    else:
        text += f"📦 UPC: <i>—</i>\n"
    
    text += "\n"
    
    text += f"<b>📊 Статус:</b> {emoji} {status_text}\n"
    
    # Если отклонено - показываем причину
    if status == STATUS_REJECTED and rel.get('reject_reason'):
        reason = escape_html(rel.get('reject_reason'))
        text += f"\n❌ <b>Причина:</b>\n<i>{reason}</i>\n"
    
    # Если на правках - показываем комментарий
    if status == STATUS_NEEDS_FIX and rel.get('moderator_comment'):
        comment = escape_html(rel.get('moderator_comment'))
        text += f"\n💬 <b>Комментарий модератора:</b>\n<i>{comment}</i>\n"
    
    text += f"\n<b>Карточка {page + 1} из {total}</b>"
    
    # Кнопки навигации и действия
    keyboard_buttons = []
    
    # Кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Пред.", callback_data=f"card_{page - 1}"))
    nav_buttons.append(InlineKeyboardButton(f"({page + 1}/{total})", callback_data="noop"))
    if page < total - 1:
        nav_buttons.append(InlineKeyboardButton("Далее ➡️", callback_data=f"card_{page + 1}"))
    keyboard_buttons.append(nav_buttons)
    
    # Кнопки действий
    original_idx = releases.index(rel)
    rel_id = f"{user_id}_{original_idx}"
    keyboard_buttons.append([
        InlineKeyboardButton("📄 Детали", callback_data=f"release_details_{rel_id}"),
        InlineKeyboardButton("🗑️ Удалить", callback_data=f"delete_release_{rel_id}")
    ])
    
    # Кнопки меню
    keyboard_buttons.append([
        InlineKeyboardButton("➕ Новый", callback_data='report'),
        InlineKeyboardButton("◀ Меню", callback_data='main')
    ])
    
    keyboard = InlineKeyboardMarkup(keyboard_buttons)
    
    if is_callback:
        await safe_edit(update.callback_query, text, reply_markup=keyboard)
    else:
        await message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# === РАСШИРЕННАЯ АДМИН-ПАНЕЛЬ (/admin) ===
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Поддержка как message, так и callback_query
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
            await update.message.reply_text("Доступ запрещён.")
        elif update.callback_query:
            await update.callback_query.answer("Доступ запрещён", show_alert=True)
        return

    # Статистика
    total_users = len(db)
    total_releases = sum(len(v) for v in db.values())
    pending = sum(1 for u in db.values() for r in u if r.get('status', 'pending') == 'pending')
    approved = sum(1 for u in db.values() for r in u if r.get('status') == 'approved')
    rejected = sum(1 for u in db.values() for r in u if r.get('status') == 'rejected')
    published = sum(1 for u in db.values() for r in u if r.get('status') == 'published')
    
    # Статистика за последние 7 дней
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
        f"{winter_header('АДМИН-ПАНЕЛЬ')}\n\n"
        f"{WINTER_EMOJIS['stats']} <b>ОБЩАЯ СТАТИСТИКА:</b>\n"
        f"{WINTER_EMOJIS['users']} Пользователей: <b>{total_users}</b>\n"
        f"{WINTER_EMOJIS['notes']} Всего релизов: <b>{total_releases}</b>\n"
        f"{WINTER_EMOJIS['waiting']} Ожидает: <b>{pending}</b>\n"
        f"{WINTER_EMOJIS['check']} Одобрено: <b>{approved}</b>\n"
        f"{WINTER_EMOJIS['cross']} Отклонено: <b>{rejected}</b>\n"
        f"{WINTER_EMOJIS['published']} Опубликовано: <b>{published}</b>\n"
        f"{WINTER_EMOJIS['calendar']} За неделю: <b>{recent_releases}</b>\n\n"
        
        f"{WINTER_EMOJIS['settings']} <b>УПРАВЛЕНИЕ:</b>\n"
        "/backup - 📦 База данных релизов\n"
        "/moderation_backup - 🗂️ Архив модерации\n"
        "/stats - 📊 Подробная статистика\n"
        "/broadcast - 📢 Рассылка пользователям\n"
        "/cleanup - 🧹 Очистка старых данных\n"
        "/cleanbase - 💣 УДАЛИТЬ ВСЕ РЕЛИЗЫ\n\n"
        
        f"{WINTER_EMOJIS['warning']} <b>БЫСТРЫЕ ДЕЙСТВИЯ:</b>"
    )
    
    keyboard = InlineKeyboardMarkup(
        [
        [
            InlineKeyboardButton(winter_text("Бэкап БД", "gift"), callback_data='get_db'),
            InlineKeyboardButton(winter_text("Архив мод.", "snowflake"), callback_data='get_moderation_db')
        ],
        [
            InlineKeyboardButton(winter_text("Статистика", "stats"), callback_data='admin_stats'),
            InlineKeyboardButton(winter_text("Ожидают", "waiting"), callback_data='pending_list')
        ],
        [
            InlineKeyboardButton(winter_text("Очистка", "refresh"), callback_data='cleanup_db'),
            InlineKeyboardButton(winter_text("Рассылка", "published"), callback_data='broadcast_menu')
        ],
        [
            InlineKeyboardButton(winter_text("Все релизы", "list"), callback_data='all_releases'),
            InlineKeyboardButton(winter_text("УДАЛИТЬ ВСЁ", "warning"), callback_data='confirm_cleanbase')
        ]
        ]
    )
    
    if update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    elif update.callback_query:
        await safe_edit(update.callback_query, text, reply_markup=keyboard)

# === СТАТИСТИКА ДЛЯ АДМИНА ===
def _all_releases_flat():
    all_rel = []
    for uid, rels in db.items():
        for idx, rel in enumerate(rels):
            all_rel.append((uid, idx, rel))
    # по времени отправки
    all_rel.sort(key=lambda x: x[2].get("submission_time", ""), reverse=True)
    return all_rel


def _render_admin_stats_page(page: int, per_page: int = 10):
    all_rel = _all_releases_flat()
    total_users = len(db)
    total_releases = len(all_rel)

    status_stats = {"pending": 0, "approved": 0, "rejected": 0, "published": 0}
    type_stats = {"сингл": 0, "альбом": 0}
    for _, __, r in all_rel:
        status_stats[r.get("status", "pending")] = status_stats.get(r.get("status", "pending"), 0) + 1
        type_stats[r.get("type", "сингл")] = type_stats.get(r.get("type", "сингл"), 0) + 1

    active_users = sum(1 for rels in db.values() if len(rels) > 0)

    pages = max(1, (total_releases + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    start = page * per_page
    end = min(total_releases, start + per_page)

    text = (
        f"{winter_header('ДЕТАЛЬНАЯ СТАТИСТИКА')}\n\n"
        f"{WINTER_EMOJIS['users']} <b>ПОЛЬЗОВАТЕЛИ:</b>\n"
        f"• Всего: <b>{total_users}</b>\n"
        f"• Активных: <b>{active_users}</b>\n\n"
        f"{WINTER_EMOJIS['notes']} <b>РЕЛИЗЫ:</b>\n"
        f"• Всего: <b>{total_releases}</b>\n"
        f"• Синглов: <b>{type_stats.get('сингл', 0)}</b>\n"
        f"• Альбомов: <b>{type_stats.get('альбом', 0)}</b>\n\n"
        f"{WINTER_EMOJIS['stats']} <b>СТАТУСЫ:</b>\n"
        f"• Ожидает: <b>{status_stats.get('pending', 0)}</b>\n"
        f"• Одобрено: <b>{status_stats.get('approved', 0)}</b>\n"
        f"• Отклонено: <b>{status_stats.get('rejected', 0)}</b>\n"
        f"• Опубликовано: <b>{status_stats.get('published', 0)}</b>\n\n"
        f"{WINTER_EMOJIS['list']} <b>ВСЕ РЕЛИЗЫ (стр. {page+1}/{pages}):</b>\n"
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
            f"\n<b>{i}. {escape_html(r.get('name', 'Без названия'))}</b> {escape_html(status_emoji.get(st, WINTER_EMOJIS['waiting']))}\n"
            f"<i>Тип:</i> {escape_html(r.get('type', '—'))}\n"
            f"<i>Артист:</i> {escape_html(r.get('nick', '—'))}\n"
            f"<i>Дата:</i> {escape_html(r.get('date', '—'))}\n"
            f"<i>ID:</i> <code>{uid}</code>\n"
        )

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"admin_stats_page_{page-1}"))
    nav.append(InlineKeyboardButton("🔙 В админ", callback_data="admin_back"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"admin_stats_page_{page+1}"))

    keyboard = InlineKeyboardMarkup([nav])
    return text, keyboard


async def admin_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /statss - статистика за выбранный период (для админов)."""
    user_id = update.message.from_user.id if update.message else None
    
    if not user_id:
        return
    
    # Проверяем админа
    if not is_admin(user_id):
        await update.message.reply_text("❌ Доступ запрещён. Команда /statss доступна только для администраторов.")
        return

    # Показываем выбор периода
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Неделя", callback_data='stats_period_week')],
        [InlineKeyboardButton("📅 Месяц", callback_data='stats_period_month')],
        [InlineKeyboardButton("📅 Всё время", callback_data='stats_period_all')],
    ])
    await update.message.reply_text("📊 Выберите период для статистики:", reply_markup=keyboard)

# === СПИСОК ВСЕХ РЕЛИЗОВ ===
async def all_releases_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("Доступ запрещён", show_alert=True)
        return

    all_releases = []
    for user_id, releases in db.items():
        for idx, release in enumerate(releases):
            all_releases.append((user_id, idx, release))
    
    if not all_releases:
        text = f"{WINTER_EMOJIS['check']} <b>Нет релизов!</b>"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
        ])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return
    
    # Сортируем по времени отправки
    all_releases.sort(key=lambda x: x[2].get('submission_time', ''), reverse=True)
    
    text = f"{winter_header('ВСЕ РЕЛИЗЫ')}\n\n"
    for i, (user_id, idx, release) in enumerate(all_releases[:15], 1):  # Ограничиваем 15 записями
        status_emoji = {
            'pending': WINTER_EMOJIS['waiting'],
            'approved': WINTER_EMOJIS['check'],
            'rejected': WINTER_EMOJIS['cross'],
            'published': WINTER_EMOJIS['published']
        }
        status = release.get('status', 'pending')
        emoji = status_emoji.get(status, WINTER_EMOJIS['waiting'])
        
        # Пометка удаленного релиза
        deleted_mark = " 🗑️ <i>(удален артистом)</i>" if release.get('user_deleted') else ""
        
        text += (
            f"<b>{i}. {escape_html(release.get('name', 'Без названия'))}</b> {emoji}{deleted_mark}\n"
            f"Тип: {escape_html(release.get('type', '—'))}\n"
            f"Артист: {escape_html(release.get('nick', '—'))}\n"
            f"Статус: {escape_html(status)}\n"
            f"ID: <code>{user_id}</code>\n\n"
        )
    
    if len(all_releases) > 15:
        text += f"<b>... и ещё {len(all_releases) - 15} релизов</b>"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
    ])
    
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === СПИСОК ОЖИДАЮЩИХ РЕЛИЗОВ ===
async def pending_releases_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("Доступ запрещён", show_alert=True)
        return

    pending_list = []
    for user_id, releases in db.items():
        for idx, release in enumerate(releases):
            if release.get('status', 'pending') == 'pending':
                pending_list.append((user_id, idx, release))
    
    if not pending_list:
        text = f"{WINTER_EMOJIS['check']} <b>Нет ожидающих релизов!</b>"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
        ])
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return
    
    text = f"{winter_header('ОЖИДАЮЩИЕ РЕЛИЗЫ')}\n\n"
    for i, (user_id, idx, release) in enumerate(pending_list[:10], 1):  # Ограничиваем 10 записями
        text += (
            f"<b>{i}. {escape_html(release.get('name', 'Без названия'))}</b>\n"
            f"Тип: {escape_html(release.get('type', '—'))}\n"
            f"Артист: {escape_html(release.get('nick', '—'))}\n"
            f"Дата: {escape_html(release.get('date', '—'))}\n"
            f"ID: <code>{user_id}</code>\n\n"
        )
    
    if len(pending_list) > 10:
        text += f"<b>... и ещё {len(pending_list) - 10} релизов</b>"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
    ])
    
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === ОЧИСТКА БАЗЫ ДАННЫХ ===
async def cleanup_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Поддержка как message, так и callback_query
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
            await update.message.reply_text("Доступ запрещён.")
        elif query:
            await query.answer("Доступ запрещён", show_alert=True)
        return

    # Удаляем пользователей без релизов
    users_before = len(db)
    empty_users = [uid for uid, releases in db.items() if not releases]
    for uid in empty_users:
        del db[uid]
    
    users_after = len(db)
    users_removed = users_before - users_after
    
    # Сохраняем изменения
    save_db(db)
    
    text = (
        f"{WINTER_EMOJIS['refresh']} <b>ОЧИСТКА ЗАВЕРШЕНА!</b>\n\n"
        f"Удалено пустых пользователей: <b>{users_removed}</b>\n"
        f"Текущее количество пользователей: <b>{users_after}</b>"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
    ])
    
    if update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    elif query:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === УДАЛЕНИЕ ВСЕХ РЕЛИЗОВ ===
async def cleanbase_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Поддержка как message, так и callback_query
    if update.message:
        user_id = update.message.from_user.id
    elif update.callback_query:
        user_id = update.callback_query.from_user.id
        query = update.callback_query
    else:
        return
    
    if not is_admin(user_id):
        if update.message:
            await update.message.reply_text("Доступ запрещён.")
        elif update.callback_query:
            await update.callback_query.answer("Доступ запрещён", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(winter_text("ДА, УДАЛИТЬ ВСЁ", "cross"), callback_data='cleanbase_confirm'),
            InlineKeyboardButton(winter_text("Отмена", "check"), callback_data='admin_back')
        ]
    ])
    
    text = (
        f"{WINTER_EMOJIS['warning']} <b>ВНИМАНИЕ! ОПАСНАЯ КОМАНДА!</b>\n\n"
        f"Вы собираетесь <b>ПОЛНОСТЬЮ ОЧИСТИТЬ</b> базу данных всех релизов!\n\n"
        f"<b>Это действие нельзя отменить!</b>\n"
        f"Все данные будут <b>БЕЗВОЗВРАТНО УТЕРЯНЫ!</b>\n\n"
        f"Вы уверены, что хотите продолжить?"
    )
    
    if update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    elif update.callback_query:
        await safe_edit(update.callback_query, text, reply_markup=keyboard)

async def cleanbase_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("Доступ запрещён", show_alert=True)
        return

    # Полностью очищаем базу данных
    global db
    db = {}
    save_db(db)
    
    text = (
        f"{WINTER_EMOJIS['check']} <b>БАЗА ДАННЫХ ПОЛНОСТЬЮ ОЧИЩЕНА!</b>\n\n"
        f"Все релизы были <b>удалены</b>!\n"
        f"Количество пользователей: <b>0</b>\n"
        f"Количество релизов: <b>0</b>"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("В админ-панель", "settings"), callback_data='admin_back')]
    ])
    
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === МЕНЮ РАССЫЛКИ ===
async def broadcast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("Доступ запрещён", show_alert=True)
        return

    text = (
        f"{winter_header('РАССЫЛКА')}\n\n"
        f"{WINTER_EMOJIS['warning']} <b>ВНИМАНИЕ:</b> Рассылка будет отправлена <b>ВСЕМ</b> пользователям бота!\n\n"
        f"Используйте команду:\n"
        f"<code>/broadcast ваш текст сообщения</code>\n\n"
        f"Или отправьте сообщение ответом на это сообщение для рассылки."
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
    ])
    
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === РАССЫЛКА ===
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Доступ запрещён.")
        return

    if not context.args:
        await update.message.reply_text(
            f"{WINTER_EMOJIS['warning']} Использование: <code>/broadcast ваш текст</code>",
            parse_mode=ParseMode.HTML
        )
        return

    message_text = ' '.join(context.args)
    broadcast_text = (
        f"{WINTER_EMOJIS['published']} <b>ВАЖНОЕ ОБЪЯВЛЕНИЕ</b> {WINTER_EMOJIS['published']}\n\n"
        f"{escape_html(message_text)}\n\n"
        f"<i>С уважением, команда CXRNER MUSIC</i> {WINTER_EMOJIS['snowflake']}"
    )

    # Отправляем сообщение
    sent_count = 0
    error_count = 0
    failed_ids = []

    progress_msg = await update.message.reply_text(
        f"{WINTER_EMOJIS['waiting']} <b>Начинаю рассылку...</b>"
    )

    recipients = list(db.keys())
    for uid in recipients:
        # Пытаемся безопасно привести uid к int
        try:
            target_id = int(uid)
        except Exception as e:
            error_count += 1
            failed_ids.append(str(uid))
            print(f"Ошибка: некорректный user_id в базе: {uid} ({e})")
            continue

        # Попробуем отправить с несколькими попытками
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
                # Пользователь заблокировал бота или удалил аккаунт — фиксируем и идём дальше
                error_count += 1
                failed_ids.append(str(uid))
                print(f"Forbidden при отправке {uid}: {e}")
                break
            except BadRequest as e:
                # Частая причина — проблемы с парсингом сущностей. Отправим plain text.
                if "can't parse entities" in str(e).lower():
                    try:
                        await context.bot.send_message(target_id, _strip_html(broadcast_text), disable_web_page_preview=True)
                        sent_count += 1
                        sent = True
                        break
                    except Exception as e2:
                        error_count += 1
                        failed_ids.append(str(uid))
                        print(f"BadRequest(2) при отправке {uid}: {e2}")
                        break
                else:
                    error_count += 1
                    failed_ids.append(str(uid))
                    print(f"BadRequest при отправке {uid}: {e}")
                    break
            except TimedOut as e:
                # Таймаут — подождём и ретраим
                print(f"TimedOut при отправке {uid}, попытка {attempt}: {e}")
                await asyncio.sleep(1 + attempt)
                continue
            except Exception as e:
                # Возможный httpx.RemoteProtocolError или другие сбои — ретраим несколько раз
                if _is_remote_protocol_error(e):
                    print(f"RemoteProtocolError-ish при отправке {uid}, попытка {attempt}: {e}")
                    await asyncio.sleep(1 + attempt)
                    continue
                error_count += 1
                failed_ids.append(str(uid))
                print(f"Неизвестная ошибка при отправке {uid}: {e}")
                break

        # Небольшая пауза между успешными отправками чтобы не триггерить лимиты
        if sent:
            await asyncio.sleep(0.15)

    # Подготовим краткий отчёт — не выводим длинные списки целиком
    failed_preview = ", ".join(failed_ids[:20])
    failed_more = max(0, len(failed_ids) - 20)

    summary = (
        f"{WINTER_EMOJIS['check']} <b>РАССЫЛКА ЗАВЕРШЕНА!</b>\n\n"
        f"• Успешно: <b>{sent_count}</b>\n"
        f"• Ошибок: <b>{error_count}</b>\n"
        f"• Всего: <b>{sent_count + error_count}</b>"
    )
    if failed_ids:
        summary += f"\n\nЧасть не доставленных ID (первые {min(20, len(failed_ids))}): {escape_html(failed_preview)}"
        if failed_more:
            summary += f" и ещё {failed_more}..."

    await progress_msg.edit_text(summary, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# === БЭКАПЫ (фикс: раньше функции были переопределены, из-за этого inline кнопки /admin "не работали") ===
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
            await update.callback_query.answer("Доступ запрещён", show_alert=True)
        return
    try:
        await _send_file_to_admin(
            context,
            chat_id=int(user_id),
            path=DB_FILE,
            caption=f"{WINTER_EMOJIS['snowflake']} Резервная копия базы данных релизов",
            filename_prefix="releases_backup",
        )
        if update.callback_query:
            await update.callback_query.answer("База данных отправлена в ЛС!", show_alert=True)
        else:
            await update.message.reply_text(f"{WINTER_EMOJIS['check']} База данных отправлена в ЛС!")
    except Exception as e:
        if update.callback_query:
            await update.callback_query.answer(f"Ошибка: {e}", show_alert=True)
        else:
            await update.message.reply_text(f"{WINTER_EMOJIS['cross']} Ошибка: {e}")


async def send_moderation_backup_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    if not is_admin(user_id):
        if update.callback_query:
            await update.callback_query.answer("Доступ запрещён", show_alert=True)
        return
    try:
        await _send_file_to_admin(
            context,
            chat_id=int(user_id),
            path=MODERATION_DB_FILE,
            caption=f"{WINTER_EMOJIS['snowman']} Архив модерации",
            filename_prefix="moderation_backup",
        )
        if update.callback_query:
            await update.callback_query.answer("Архив модерации отправлен в ЛС!", show_alert=True)
        else:
            await update.message.reply_text(f"{WINTER_EMOJIS['check']} Архив модерации отправлен в ЛС!")
    except Exception as e:
        if update.callback_query:
            await update.callback_query.answer(f"Ошибка: {e}", show_alert=True)
        else:
            await update.message.reply_text(f"{WINTER_EMOJIS['cross']} Ошибка: {e}")


# === КОМАНДЫ АДМИНА ДЛЯ БЭКАПА ===
async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Доступ запрещён.")
        return
    await send_database_backup_to_admin(update, context)

async def moderation_backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Доступ запрещён.")
        return
    await send_moderation_backup_to_admin(update, context)

def _format_release_form_for_group(user, user_id: str, data: dict) -> str:
    # Формат ровно как в примере, с фиксированным порядком полей.
    username = f"@{user.username}" if user and user.username else "нет"
    release_type = data.get("type", "—")

    lines = [
        f"{WINTER_EMOJIS['snowflake']} <b>НОВАЯ АНКЕТА!</b>",
        f"От: {escape_html(username)}",
        f"ID: <code>{escape_html(user_id)}</code>",
        f"Тип: {escape_html(release_type)}",
        "",
    ]

    def add(label: str, key: str, default: str = "—"):
        val = data.get(key)
        if val is None or str(val).strip() == "":
            val = default
        lines.append(f"• <b>{label}:</b> {escape_html(val)}")
    # Русские метки и убраны поля UPC/ISRC
    add("Название", "name")
    add("Саб-название", "subname", ".")
    add("Ник", "nick")
    add("ФИО", "fio")
    add("Дата", "date")
    add("Версия", "version")
    add("Жанр", "genre")
    add("Ссылка", "link")
    add("Яндекс Музыка", "yandex", ".")
    add("Мат", "mat")
    add("Промо", "promo", ".")
    add("Комментарий", "comment", ".")
    if data.get("type") == "альбом":
        add("Tracklist", "tracklist")
    add("Tg", "tg")
    return "\n".join(lines)


def _format_status_append(status: str, moderator_username: str | None = None, reason: str | None = None, comment: str | None = None) -> str:
    # FIX: приведено к единому формату служебного блока (immutable карточка + доп.служебный блок)
    status_emoji = {
        STATUS_ON_UPLOAD: WINTER_EMOJIS['waiting'],
        STATUS_MODERATION: WINTER_EMOJIS['brain'] if 'brain' in WINTER_EMOJIS else WINTER_EMOJIS['waiting'],
        STATUS_APPROVED: WINTER_EMOJIS['check'],
        STATUS_REJECTED: WINTER_EMOJIS['cross'],
        STATUS_NEEDS_FIX: WINTER_EMOJIS['waiting'],
        STATUS_DELETED: WINTER_EMOJIS['cross'],
    }
    status_text = {
        STATUS_ON_UPLOAD: "На отгрузке",
        STATUS_MODERATION: "На модерации",
        STATUS_APPROVED: "Одобрено",
        STATUS_REJECTED: "Отклонено",
        STATUS_NEEDS_FIX: "Требует правок",
        STATUS_DELETED: "Удалено",
    }
    t = datetime.now().strftime("%d.%m.%Y %H:%M")
    lines = ["", "────────────────────", f"{status_emoji.get(status, WINTER_EMOJIS['waiting'])} <b>Статус: {escape_html(status_text.get(status, status))}</b>",]
    # служебный блок в требуемом формате
    lines.append(f"<b>Причина:</b> {escape_html(reason) if reason else '—'}")
    lines.append(f"<b>Модератор:</b> @{escape_html(moderator_username)}" if moderator_username else f"<b>Модератор:</b> —")
    lines.append(f"<b>Последнее действие:</b> {escape_html(comment) if comment else '—'}")
    lines.append(f"<b>Время:</b> {escape_html(t)}")
    lines.append("────────────────────")
    return "\n".join(lines)


async def _append_status_to_moderation_message(context: ContextTypes.DEFAULT_TYPE, message_id: int, original_text: str, status: str, moderator_username: str | None = None, reason: str | None = None, comment: str | None = None, reply_markup=None):
    """Добавляет служебный блок статуса и пытается отредактировать исходное сообщение,
    при этом сохраняя клавиатуру (через параметр `reply_markup`). Если редактировать нельзя —
    Fall back: отправляем отдельное сообщение-штамп со статусом (как раньше).
    """
    status_text = _format_status_append(status, moderator_username=moderator_username, reason=reason, comment=comment)

    # Короткий статус для шапки анкеты
    status_short = {
        STATUS_ON_UPLOAD: "На отгрузке",
        STATUS_MODERATION: "На модерации",
        STATUS_APPROVED: "Одобрено",
        STATUS_REJECTED: "Отклонено",
        STATUS_NEEDS_FIX: "Требует правок",
        STATUS_DELETED: "Удалено",
    }.get(status, status)

    emoji = {
        STATUS_ON_UPLOAD: WINTER_EMOJIS.get('upload', ''),
        STATUS_MODERATION: WINTER_EMOJIS.get('brain', WINTER_EMOJIS.get('waiting')),
        STATUS_APPROVED: WINTER_EMOJIS.get('check', ''),
        STATUS_REJECTED: WINTER_EMOJIS.get('cross', ''),
        STATUS_NEEDS_FIX: WINTER_EMOJIS.get('warning', WINTER_EMOJIS.get('waiting')),
        STATUS_DELETED: WINTER_EMOJIS.get('delete', ''),
    }.get(status, '')

    header = f"{emoji} <b>СТАТУС: {escape_html(status_short)}</b>\n\n"

    # Попробуем отредактировать исходное сообщение, добавив шапку статуса и сохранив клавиатуру
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
        # Если редактировать нельзя (например, срок истёк) — шлём отдельным сообщением-штампом
        try:
            await context.bot.send_message(
                chat_id=MODERATION_CHAT_ID,
                text=status_text,
                parse_mode=ParseMode.HTML,
                reply_to_message_id=message_id,
            )
        except Exception as e2:
            if not (_is_remote_protocol_error(e2) or isinstance(e2, TimedOut)):
                print(f"❌ _append_status_to_moderation_message: {e2}")


# === CALLBACK-РОУТЕР (глобально) ===
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = str(query.from_user.id)

    if data == 'report':
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(winter_text("Сингл", "music"), callback_data='single')],
            [InlineKeyboardButton(winter_text("Альбом", "notes"), callback_data='album')]
        ])
        await safe_edit(query, f"{WINTER_EMOJIS['snowflake']} <b>Выберите тип релиза:</b>", keyboard)
        return TYPE

    if data == 'my_releases':
        await my_cmd(update, context)
        return REPORT

    # Навигация по карточкам релизов
    if data.startswith('card_'):
        try:
            page = int(data.split('_')[1])
            await my_cmd(update, context, page=page)
        except (ValueError, IndexError):
            await query.answer('❌ Ошибка навигации', show_alert=True)
        return
    
    # Пустой callback (кнопка номера страницы)
    if data == 'noop':
        await query.answer()
        return

    if data == 'single':
        user_data[user_id] = {"type": "сингл", "status": "pending"}
        await safe_edit(query, f"{WINTER_EMOJIS['notes']} <b>СИНГЛ</b>\n\n<b>1. Название релиза</b>\nПример: Lost in the Void")
        return NAME

    if data == 'album':
        user_data[user_id] = {"type": "альбом", "status": "pending"}
        await safe_edit(query, f"{WINTER_EMOJIS['notes']} <b>АЛЬБОМ</b>\n\n<b>1. Название релиза</b>\nПример: Lost in the Void")
        return NAME

    if data == 'send':
        await send_moderation(query, context)
        return REPORT

    

    if data == 'main':
        return await start_cmd(update, context)
        
    if data == 'get_db':
        await send_database_backup_to_admin(update, context)
        return
        
    if data == 'get_moderation_db':
        await send_moderation_backup_to_admin(update, context)
        return
        
    # Админские кнопки
    if data == 'admin_stats':
        await admin_stats_cmd(update, context)
        return
    if data.startswith('stats_period_'):
        # Показать статистику за выбранный период (admin only, from moderation chat)
        if not is_admin(update.callback_query.from_user.id):
            await update.callback_query.answer('❌ Доступ запрещён', show_alert=True)
            return
        
        # Проверяем что это чат модерации
        chat_id = update.callback_query.message.chat_id if update.callback_query.message else None
        if chat_id is None or int(chat_id) != int(MODERATION_CHAT_ID):
            await update.callback_query.answer('❌ Статистика доступна только в чате модерации', show_alert=True)
            return
        
        period = data.split('_')[-1]
        now = datetime.now()
        cutoff = None
        period_name = "Все время"
        if period == 'week':
            cutoff = now - timedelta(days=7)
            period_name = "Последние 7 дней"
        elif period == 'month':
            cutoff = now - timedelta(days=30)
            period_name = "Последние 30 дней"
        # Собираем статистику
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
        # Компактный формат статистики
        text = (
            f"📊 <b>СТАТИСТИКА</b> ({period_name})\n\n"
            f"📦 <b>Всего анкет:</b> {total}\n"
            f"✅ <b>Принято:</b> {approved} ({approved_pct:.1f}%)\n"
            f"❌ <b>Отклонено:</b> {rejected}\n\n"
            f"❌ <b>Топ 3 причины отказа:</b>\n"
        )
        if top_reasons:
            for i, (reason, count) in enumerate(top_reasons, 1):
                text += f"  {i}. {escape_html(reason)} — {count}\n"
        else:
            text += "  Нет данных\n"
        text += f"\n🔥 <b>Топ 3 артисты:</b>\n"
        if top_artists:
            for i, (artist, count) in enumerate(top_artists, 1):
                text += f"  {i}. {escape_html(artist)} — {count}\n"
        else:
            text += "  Нет данных\n"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("◀ Назад", callback_data='admin_back')]
        ])
        await safe_edit(update.callback_query, text, reply_markup=keyboard)
        return
    
    # NOTE: Обработчик для кнопки "Подробнее" в личном кабинете
    if data.startswith('release_details_'):
        parts = data.split('_', 3)  # release_details_userid_idx
        if len(parts) >= 4:
            user_id = parts[2]
            rel_idx = int(parts[3])
            if user_id in db and rel_idx < len(db[user_id]):
                rel = db[user_id][rel_idx]
                
                # Красивый формат с группировкой информации
                status = rel.get('status', STATUS_ON_UPLOAD)
                status_text = {
                    STATUS_ON_UPLOAD: '⏳ На отгрузке',
                    STATUS_APPROVED: '✅ Одобрено',
                    STATUS_REJECTED: '❌ Отклонено',
                    STATUS_NEEDS_FIX: '⚠️ Требует правок',
                    STATUS_MODERATION: '🧠 На модерации',
                }.get(status, '— Неизвестно')
                
                # Основная информация
                details_text = (
                    f"{WINTER_EMOJIS['notes']} <b>ИНФОРМАЦИЯ О РЕЛИЗЕ</b>\n"
                    f"{'─' * 40}\n\n"
                    f"<b>Название</b>\n"
                    f"🎵 {escape_html(rel.get('name', '—'))}\n\n"
                )
                
                # Дополнительные названия
                if rel.get('subname') and rel.get('subname') != '.':
                    details_text += f"<b>Подименование</b>\n"
                    details_text += f"  {escape_html(rel.get('subname'))}\n\n"
                
                # Основные метаданные
                details_text += f"<b>📋 ОСНОВНЫЕ ДАННЫЕ</b>\n"
                details_text += f"Тип: <i>{escape_html(rel.get('type', '—'))}</i>\n"
                details_text += f"Жанр: <i>{escape_html(rel.get('genre', '—'))}</i>\n"
                details_text += f"Дата релиза: <i>{escape_html(rel.get('date', '—'))}</i>\n"
                details_text += f"Версия: <i>{escape_html(rel.get('version', '—'))}</i>\n\n"
                
                # Информация об артисте
                details_text += f"<b>👤 АРТИСТ</b>\n"
                details_text += f"Ник: <i>{escape_html(rel.get('nick', '—'))}</i>\n"
                details_text += f"ФИО: <i>{escape_html(rel.get('fio', '—'))}</i>\n\n"
                
                # Контакты и ссылки
                details_text += f"<b>🔗 ССЫЛКИ И КОНТАКТЫ</b>\n"
                details_text += f"Telegram: <i>{escape_html(rel.get('tg', '—'))}</i>\n"
                if rel.get('link'):
                    details_text += f"Ссылка: <i>{escape_html(rel.get('link')[:50])}...</i>\n"
                if rel.get('yandex'):
                    details_text += f"Яндекс: <i>{escape_html(rel.get('yandex')[:50])}...</i>\n"
                details_text += "\n"
                
                # Коды и идентификаторы
                if rel.get('upc') and rel.get('upc') != '.':
                    details_text += f"<b>🔢 КОДЫ</b>\n"
                    if rel.get('upc') and rel.get('upc') != '.':
                        details_text += f"UPC: <i>{escape_html(rel.get('upc'))}</i>\n"
                    if rel.get('isrc') and rel.get('isrc') != '.':
                        details_text += f"ISRC: <i>{escape_html(rel.get('isrc'))}</i>\n"
                    details_text += "\n"
                
                # Характеристики трека
                details_text += f"<b>🎙️ ХАРАКТЕРИСТИКИ</b>\n"
                has_lyrics = rel.get('has_lyrics', '—')
                details_text += f"Слова: <i>{escape_html(has_lyrics)}</i>\n"
                mat = rel.get('mat', '—')
                details_text += f"Мат: <i>{escape_html(mat)}</i>\n"
                details_text += "\n"
                
                # Комментарии
                if rel.get('promo') or rel.get('comment'):
                    details_text += f"<b>💬 КОММЕНТАРИИ</b>\n"
                    if rel.get('promo'):
                        details_text += f"Промо: <i>{escape_html(rel.get('promo')[:80])}...</i>\n"
                    if rel.get('comment'):
                        details_text += f"Комментарий: <i>{escape_html(rel.get('comment')[:80])}...</i>\n"
                    details_text += "\n"
                
                # Статус и даты
                details_text += f"{'─' * 40}\n"
                details_text += f"<b>📊 СТАТУС</b>\n"
                details_text += f"{status_text}\n"
                
                if rel.get('reject_reason'):
                    details_text += f"\n❌ <b>Причина отказа</b>\n"
                    details_text += f"<i>{escape_html(rel.get('reject_reason'))}</i>\n"
                
                if rel.get('moderator_comment'):
                    details_text += f"\n💬 <b>Комментарий модератора</b>\n"
                    details_text += f"<i>{escape_html(rel.get('moderator_comment'))}</i>\n"
                
                # Время отправки
                details_text += f"\n⏰ Отправлено: <i>{escape_html(rel.get('submission_time', '—')[:19])}</i>"
                if rel.get('moderation_time'):
                    details_text += f"\n⏰ Модерировано: <i>{escape_html(rel.get('moderation_time', '—')[:19])}</i>"
                
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀ В кабинет", callback_data="my_back")
                ]])
                await safe_edit(update.callback_query, details_text, reply_markup=keyboard)
        return
    
    # NOTE: Обработчик для возврата в личный кабинет
    if data == 'my_back':
        await my_cmd(update, context)
        return
    
    # NOTE: Обработчик для изменения статуса релиза артистом (окно выбора статусов для модерации)
    if data.startswith('delete_release_'):
        # Мягкое удаление релиза пользователем (пометка, без физического удаления)
        parts = data.split('_', 3)  # delete_release_userid_idx
        if len(parts) >= 4:
            user_id = parts[2]
            try:
                rel_idx = int(parts[3])
            except ValueError:
                await update.callback_query.answer('❌ Ошибка удаления', show_alert=True)
                return
            
            if user_id in db and rel_idx < len(db[user_id]):
                rel = db[user_id][rel_idx]
                
                # Проверяем что релиз ещё не удален
                if rel.get('user_deleted'):
                    await update.callback_query.answer('✓ Релиз уже удален', show_alert=True)
                    return
                
                # Помечаем как удалённый пользователем, но НЕ удаляем из db
                rel['user_deleted'] = True
                rel['deleted_at'] = datetime.now().isoformat()
                rel_name = rel.get('name', 'Релиз')
                artist_name = rel.get('nick', 'Артист')
                rel_type = rel.get('type', 'Релиз')
                rel_date = rel.get('date', '—')
                rel_status = rel.get('status', STATUS_ON_UPLOAD)
                save_db(db)
                
                # Уведомляем модерацию
                try:
                    notification_text = (
                        f"🗑️ <b>РЕЛИЗ УДАЛЕН АРТИСТОМ</b>\n\n"
                        f"🎵 <b>{escape_html(rel_name)}</b>\n"
                        f"👤 Артист: {escape_html(artist_name)}\n"
                        f"📝 Тип: {escape_html(rel_type)}\n"
                        f"📅 Дата: {escape_html(rel_date)}\n"
                        f"📊 Статус был: {rel_status}\n\n"
                        f"💡 Для полного удаления с платформ свяжитесь с CEO @kazumaiq"
                    )
                    await context.bot.send_message(
                        chat_id=MODERATION_CHAT_ID,
                        text=notification_text,
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    print(f"Ошибка отправки в модерацию: {e}")
                
                # Уведомляем артиста
                try:
                    artist_msg = (
                        f"✅ <b>Релиз удален</b>\n\n"
                        f"🎵 {escape_html(rel_name)}\n\n"
                        f"<i>Релиз удален из вашего кабинета.</i>\n"
                        f"<i>Для полного удаления со всех площадок:</i>\n"
                        f"<i>@kazumaiq</i>"
                    )
                    await context.bot.send_message(
                        int(user_id),
                        artist_msg,
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    print(f"Ошибка отправки артисту: {e}")
                
                await update.callback_query.answer('✅ Релиз удален', show_alert=False)
                # Обновляем кабинет
                await my_cmd(update, context)
            else:
                await update.callback_query.answer('❌ Релиз не найден', show_alert=True)
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

    # Переходы в анкете
    if data == "subname_skip":
        user_data[user_id]["subname"] = "."
        # Пропустить subname -> сразу спрашиваем про наличие слов (минимизированный поток без UPC/ISRC)
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Да", callback_data="lyrics_yes"),
                    InlineKeyboardButton("Нет, это инструментал", callback_data="lyrics_no"),
                ]
            ]
        )
        await safe_send(query.message, f"{WINTER_EMOJIS['warning']} <b>Есть ли слова в релизе?</b>", keyboard)
        return HAS_LYRICS

    if data == "lyrics_yes":
        user_data[user_id]["has_lyrics"] = "Да"
        await safe_send(query.message, f"{WINTER_EMOJIS['star']} <b>Ник исполнителя(ей)</b>\nПример: MAKIZM")
        return NICK

    if data == "lyrics_no":
        user_data[user_id]["has_lyrics"] = "Нет, это инструментал"
        await safe_send(query.message, f"{WINTER_EMOJIS['star']} <b>Ник исполнителя(ей)</b>\nПример: MAKIZM")
        return NICK

    # removed snippet_auto/snippet_manual flow: сразу переходим к NICK

# === ПОЛЯ ===
async def name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    # сохраняем предыдущее значение в историю
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('name', user_data.get(user_id, {}).get('name')))
    user_data[user_id]['name'] = clean(update.message.text)
    save_draft_for_user(user_id)
    # Новый блок: sub-name
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить", callback_data="subname_skip")]])
    await safe_send(update.message, f"{WINTER_EMOJIS['star']} <b>Саб-название</b>\nslowed, speed up, prod и т.д.\nЕсли не нужно — нажмите «Пропустить» или отправьте точку '.'", keyboard)
    return SUBNAME


async def subname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    txt = clean(update.message.text)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('subname', user_data.get(user_id, {}).get('subname')))
    user_data[user_id]["subname"] = txt if txt else "."
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Да", callback_data="lyrics_yes"),
                InlineKeyboardButton("Нет, это инструментал", callback_data="lyrics_no"),
            ]
        ]
    )
    await safe_send(update.message, f"{WINTER_EMOJIS['warning']} <b>Есть ли слова в релизе?</b>", keyboard)
    return HAS_LYRICS


async def upc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('upc', user_data.get(user_id, {}).get('upc')))
    user_data[user_id]["upc"] = clean(update.message.text) or "."
    save_draft_for_user(user_id)
    await safe_send(update.message, f"{WINTER_EMOJIS['notes']} <b>ISRC</b>\nЕсли нет — отправьте '.'")
    return ISRC


async def isrc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('isrc', user_data.get(user_id, {}).get('isrc')))
    user_data[user_id]["isrc"] = clean(update.message.text) or "."
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Да", callback_data="lyrics_yes"),
                InlineKeyboardButton("Нет, это инструментал", callback_data="lyrics_no"),
            ]
        ]
    )
    await safe_send(update.message, f"{WINTER_EMOJIS['warning']} <b>Есть ли слова в релизе?</b>", keyboard)
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
    await safe_send(update.message, f"{WINTER_EMOJIS['star']} <b>ФИО исполнителя(ей)</b>\nПример: Иванов Иван, Петров Пётр")
    return FIO


async def fio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('fio', user_data.get(user_id, {}).get('fio')))
    user_data[user_id]["fio"] = clean(update.message.text)
    save_draft_for_user(user_id)
    min_days = 3 if user_data[user_id]["type"] == "сингл" else 7
    await safe_send(update.message, f"{WINTER_EMOJIS['calendar']} <b>Дата релиза</b>\nМинимум через {min_days} дней\nФормат: ДД.ММ.ГГГГ")
    return DATE

async def date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    text = update.message.text.strip()
    if not all(part.isdigit() for part in text.split('.') if part):
        await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Неверный формат даты! Используйте: ДД.ММ.ГГГГ")
        return DATE
    try:
        date_obj = datetime.strptime(text, "%d.%m.%Y")
        min_days = 3 if user_data[user_id]['type'] == 'сингл' else 7
        if date_obj < datetime.now() + timedelta(days=min_days):
            await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Дата должна быть минимум через {min_days} дней!")
            return DATE
        user_data.setdefault(user_id, {}).setdefault('_history', []).append(('date', user_data.get(user_id, {}).get('date')))
        user_data[user_id]['date'] = text
        save_draft_for_user(user_id)
        await safe_send(update.message, f"{WINTER_EMOJIS['music']} <b>Версия релиза</b>\nSlowed, Speed Up.\nЕсли нет — напиши: -")
        return VERSION
    except ValueError:
        await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Неверный формат даты! Пример: 25.12.2025")
        return DATE

async def version(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    ver = clean(update.message.text)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('version', user_data.get(user_id, {}).get('version')))
    user_data[user_id]['version'] = ver if ver != '-' else 'Оригинал'
    save_draft_for_user(user_id)
    await safe_send(update.message, f"{WINTER_EMOJIS['notes']} <b>Жанр релиза</b>\nПример: Phonk, Trap")
    return GENRE

async def genre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('genre', user_data.get(user_id, {}).get('genre')))
    user_data[user_id]['genre'] = clean(update.message.text)
    save_draft_for_user(user_id)
    await safe_send(update.message,
        f"{WINTER_EMOJIS['gift']} <b>Ссылка на файлы (Yandex/Google Диск)</b>\n\n"
        "В архиве:\n"
        "• WAV 16/24 бит, 44100 Гц\n"
        "• Обложка 3000x3000 JPG\n"
        "• Скриншот проекта"
    )
    return LINK

async def link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('link', user_data.get(user_id, {}).get('link')))
    user_data[user_id]['link'] = update.message.text.strip()
    save_draft_for_user(user_id)
    # Простейшая проверка: убедимся, что это выглядящая как URL строка
    url = user_data[user_id]['link']
    if url and url != ".":
        if not _looks_like_url(url):
            await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Похоже, вы прислали не ссылку. Пожалуйста, отправьте корректный URL (начинающийся с http:// или https://)")
            return LINK
        # Доп. подсказка: если это не очевидный Google Drive URL, не блокируем — только подсказка
        if not _looks_like_drive_link(url):
            await safe_send(update.message, f"{WINTER_EMOJIS['warning']} Примечание: рекомендуется предоставить ссылку с Google Drive (drive.google.com), но принимается любой корректный URL.")
        # Попытка быстрого HEAD-запроса для логирования; не показываем пользователю ложные предупреждения при неудаче
        try:
            if httpx is not None:
                async def _check():
                    async with httpx.AsyncClient(timeout=5) as client:
                        await client.head(url, follow_redirects=True)
                await _check()
        except Exception:
            # логируем внутренне, но не предупреждаем пользователя из-за ложных срабатываний
            print(f"⚠️ link validation HEAD failed for {url}")
    # Сначала спрашиваем ссылку на карточку музыканта в Яндекс Музыке
    await safe_send(update.message, f"{WINTER_EMOJIS['notes']} <b>Укажите ссылку на карточку музыканта в Яндекс Музыке</b>\nЕсли нет — отправьте '.'")
    return YANDEX


async def yandex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('yandex', user_data.get(user_id, {}).get('yandex')))
    user_data[user_id]['yandex'] = update.message.text.strip() or "."
    save_draft_for_user(user_id)
    url = user_data[user_id]['yandex']
    if url and url != ".":
        if not _looks_like_url(url):
            await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Похоже, вы прислали не ссылку. Пожалуйста, отправьте корректный URL (начинающийся с http:// или https://)")
            return YANDEX
        if not _looks_like_yandex_music_link(url):
            await safe_send(update.message, f"{WINTER_EMOJIS['warning']} Примечание: рекомендуется прислать ссылку с Yandex Music (music.yandex.ru), но принимается любой корректный URL.")
        try:
            if httpx is not None:
                async with httpx.AsyncClient(timeout=5) as client:
                    await client.head(url, follow_redirects=True)
        except Exception:
            print(f"⚠️ yandex validation HEAD failed for {url}")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Да", "check"), callback_data='mat_yes')],
        [InlineKeyboardButton(winter_text("Нет", "cross"), callback_data='mat_no')]
    ])
    await safe_send(update.message, f"{WINTER_EMOJIS['warning']} <b>Есть ли ненормативная лексика?</b>", keyboard)
    return MAT

async def mat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    user_data[user_id]['mat'] = 'Да' if query.data == 'mat_yes' else 'Нет'
    await safe_edit(query, f"{WINTER_EMOJIS['sparkles']} <b>Промо текст (необязательно)</b>")
    return PROMO

async def promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('promo', user_data.get(user_id, {}).get('promo')))
    user_data[user_id]['promo'] = clean(update.message.text)
    save_draft_for_user(user_id)
    await safe_send(update.message, f"{WINTER_EMOJIS['comment']} <b>Комментарий для модератора (необязательно)</b>")
    return COMMENT

async def comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('comment', user_data.get(user_id, {}).get('comment')))
    user_data[user_id]['comment'] = clean(update.message.text)
    save_draft_for_user(user_id)
    if user_data[user_id]["type"] == "альбом":
        await safe_send(update.message, f"{WINTER_EMOJIS['list']} <b>Tracklist</b>\nПеречислите треки одной строкой или списком.")
        return TRACKLIST
    await safe_send(update.message, f"{WINTER_EMOJIS['telegram']} <b>Tg</b>\n@username (можно несколько через пробел)")
    return TG


async def tracklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data.setdefault(user_id, {}).setdefault('_history', []).append(('tracklist', user_data.get(user_id, {}).get('tracklist')))
    user_data[user_id]["tracklist"] = clean(update.message.text)
    save_draft_for_user(user_id)
    await safe_send(update.message, f"{WINTER_EMOJIS['telegram']} <b>Tg</b>\n@username (можно несколько через пробел)")
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
    text = f"{WINTER_EMOJIS['snowflake']} <b>ПРОВЕРЬТЕ АНКЕТУ:</b>\n\n"
    order = [
        ("Тип", "type"),
        ("Название", "name"),
        ("Саб-название", "subname"),
        ("Есть слова", "has_lyrics"),
        ("Ник", "nick"),
        ("ФИО", "fio"),
        ("Дата", "date"),
        ("Версия", "version"),
        ("Жанр", "genre"),
        ("Ссылка", "link"),
        ("Яндекс Музыка", "yandex"),
        ("Мат", "mat"),
        ("Промо", "promo"),
        ("Комментарий", "comment"),
        ("Tracklist", "tracklist"),
        ("Tg", "tg"),
    ]
    for label, key in order:
        if key in data and data.get(key) is not None:
            text += f"• <b>{escape_html(label)}:</b> {escape_html(data.get(key))}\n"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Отправить", "check"), callback_data='send')],
        [InlineKeyboardButton(winter_text("Назад", "cross"), callback_data='main')]
    ])
    await safe_send(message, text, keyboard)

# === ОТПРАВКА В МОДЕРАЦИЮ ===
async def send_moderation(query, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(query.from_user.id)
    data = user_data[user_id]
    # При отправке ставим статус на отгрузке — карточка immutable до редактирования модератором
    data["status"] = STATUS_ON_UPLOAD
    data["submission_time"] = datetime.now().isoformat()
    user = query.from_user

    idx = len(db.get(user_id, []))
    
    # FIX: Клавиатура для модерации — все статусы доступны для админов
    # ПЕРВЫЙ РЯД: промежуточные статусы (остаются активны)
    # ВТОРОЙ РЯД: финальные статусы (убираются после выбора)
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🕓 На отгрузке", callback_data=f"m_upload_{user_id}_{idx}"),
            InlineKeyboardButton("🧠 Модерация", callback_data=f"m_moderate_{user_id}_{idx}"),
            InlineKeyboardButton("✅ Принято", callback_data=f"m_approve_{user_id}_{idx}")
        ],
        [
            InlineKeyboardButton("❌ Отклонить", callback_data=f"m_reject_{user_id}_{idx}"),
            InlineKeyboardButton("✏️ На исправлении", callback_data=f"m_needfix_{user_id}_{idx}"),
            InlineKeyboardButton("🗑 Удален", callback_data=f"m_delete_{user_id}_{idx}")
        ],
    ])

    msg = _format_release_form_for_group(user, user_id, data)
    
    try:
        moderation_msg = await context.bot.send_message(
            MODERATION_CHAT_ID, msg, parse_mode=ParseMode.HTML, reply_markup=keyboard, disable_web_page_preview=True
        )
        
        # ЗАКРЕПЛЯЕМ сообщение автоматически
        try:
            await context.bot.pin_chat_message(chat_id=MODERATION_CHAT_ID, message_id=moderation_msg.message_id)
        except Exception:
            # NOTE: отсутствие прав на закрепление — не критично
            pass
        
        # Сохраняем ID сообщения и исходный текст в сам релиз (immutable карточка)
        data["moderation_message_id"] = moderation_msg.message_id
        data["moderation_original_text"] = msg
        # фикс: флаг напоминания, чтобы не рассылать спам
        data.setdefault("reminder_sent", False)
        
        # Сохраняем в архив модерации
        moderation_data = data.copy()
        moderation_data['message_id'] = moderation_msg.message_id
        moderation_data['user_id'] = user_id
        moderation_data['username'] = user.username
        
        if 'moderation_messages' not in moderation_db:
            moderation_db['moderation_messages'] = []
        moderation_db['moderation_messages'].append(moderation_data)
        save_moderation_db(moderation_db)
        
    except Exception as e:
        await safe_edit(query, f"{WINTER_EMOJIS['cross']} Ошибка: {e}")
        return REPORT

    if user_id not in db:
        db[user_id] = []
    # фикс: сохраняем username в релиз, чтобы правильно отображалось в модерации/истории
    data["username"] = user.username or ""
    db[user_id].append(data.copy())
    save_db(db)
    # Добавляем шапку статуса в исходную анкету (чтобы вверху была текущая отметка статуса)
    try:
        await _append_status_to_moderation_message(context, moderation_msg.message_id, msg, data.get('status', STATUS_ON_UPLOAD), reply_markup=moderation_msg.reply_markup)
    except Exception as e:
        print(f"Ошибка при добавлении шапки статуса: {e}")
    
    # Отправляем сообщение с кнопкой для добавления UPC на все релизы
    try:
        upc_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📦 Присвоить UPC", callback_data=f"m_add_upc_{user_id}_{idx}")]
        ])
        await context.bot.send_message(
            chat_id=MODERATION_CHAT_ID,
            text="💾 <b>Добавьте UPC код для этого релиза</b>\n\n"
                 "Нажмите кнопку и ответьте UPC кодом на исходное сообщение анкеты.",
            reply_to_message_id=moderation_msg.message_id,
            parse_mode=ParseMode.HTML,
            reply_markup=upc_keyboard
        )
    except Exception as e:
        print(f"Ошибка отправки кнопки UPC: {e}")
    
    await safe_edit(query, f"{WINTER_EMOJIS['check']} <b>Анкета отправлена!</b>\nОжидайте 12–72 часа.", parse_mode=ParseMode.HTML)

# === МОДЕРАЦИЯ (КНОПКИ НЕ ДОЛЖНЫ ЗАТИРАТЬ АНКЕТУ) ===
# Требование: исходный текст анкеты остаётся неизменным; после нажатия кнопки появляется доп.текст снизу,
# а не “анкета пропадает”. Поэтому:
# - не редактируем текст при выборе действий (только отвечаем + шлём отдельные сообщения/кнопки)
# - финально: убираем клавиатуру у исходного сообщения и редактируем текст = original + status_append.


# === МОДЕРАЦИЯ (КНОПКИ НЕ ДОЛЖНЫ ЗАТИРАТЬ АНКЕТУ) ===
# Требование: исходный текст анкеты остаётся неизменным; после нажатия кнопки появляется доп.текст снизу,
# а не "анкета пропадает". Поэтому:
# - не редактируем текст при выборе действий (только отвечаем + шлём отдельные сообщения/кнопки)
# - финально: убираем клавиатуру у исходного сообщения и редактируем текст = original + status_append.


# === РУЧНОЕ ОТКЛОНЕНИЕ АНКЕТЫ ЧЕРЕЗ REPLY ===
# MANUAL_REJECT: Модератор может отклонить анкету, ответив на её сообщение в чате модерации
async def manual_reject_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ручного отклонения анкеты через reply на сообщение анкеты."""
    if not update.message or not update.message.reply_to_message:
        return
    if update.message.chat_id != MODERATION_CHAT_ID:
        return
    
    # Проверяем что это админ
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ Только администраторы могут отклонять анкеты.")
        return
    
    text = clean(update.message.text)
    
    # Если это похоже на UPC (только цифры, 10-14 символов) — пропускаем, это обработает add_upc_handler
    if text.isdigit() and 10 <= len(text) <= 14:
        # Попробуем добавить UPC
        await add_upc_handler(update, context)
        return
    
    # MANUAL_REJECT: Получаем сообщение на которое ответили
    replied_msg = update.message.reply_to_message
    replied_msg_id = replied_msg.message_id
    
    # MANUAL_REJECT: Ищем анкету в БД по moderation_message_id
    user_id = None
    idx = None
    for uid, releases in db.items():
        for idx_rel, rel in enumerate(releases):
            if rel.get('moderation_message_id') == replied_msg_id:
                user_id = uid
                idx = idx_rel
                break
        if user_id:
            break
    
    if not user_id or idx is None:
        return  # Молчаливо игнорируем обычные сообщения
    
    release = db[user_id][idx]
    
    # MANUAL_REJECT: Берём текст сообщения как причину
    reject_reason = clean(update.message.text)
    if not reject_reason:
        await update.message.reply_text("❌ Текст причины не может быть пустым.")
        return
    
    moderator_username = update.message.from_user.username or update.message.from_user.first_name
    
    # MANUAL_REJECT: Обновляем статус в БД
    old_status = release.get("status")
    release["status"] = STATUS_REJECTED
    release["reject_reason"] = reject_reason
    release["moderator"] = moderator_username
    release["moderation_time"] = datetime.now().isoformat()
    add_history_entry(user_id, idx, old_status, STATUS_REJECTED, update.message.from_user.id, moderator_username, reject_reason)
    save_db(db)
    update_moderation_record(user_id, idx, release)
    
    # MANUAL_REJECT: Удаляем кнопки у исходного сообщения анкеты
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=MODERATION_CHAT_ID,
            message_id=replied_msg_id,
            reply_markup=None
        )
    except Exception as e:
        print(f"Ошибка при удалении кнопок: {e}")
    
    # MANUAL_REJECT: Дописываем статус к анкете
    original = release.get("moderation_original_text") or (replied_msg.text or "")
    await _append_status_to_moderation_message(
        context,
        replied_msg_id,
        original,
        STATUS_REJECTED,
        moderator_username=moderator_username,
        reason=reject_reason,
        reply_markup=None
    )
    
    # MANUAL_REJECT: Отправляем уведомление артисту
    try:
        moderation_time = datetime.now().strftime("%d.%m.%Y в %H:%M")
        await context.bot.send_message(
            int(user_id),
            f"{WINTER_EMOJIS['cross']} <b>ВАШ РЕЛИЗ ОТКЛОНЁН</b>\n\n"
            f"📝 <b>{escape_html(release.get('name', '—'))}</b>\n"
            f"🎵 <i>Тип:</i> {escape_html(release.get('type', '—'))}\n"
            f"📅 <i>Дата релиза:</i> {escape_html(release.get('date', '—'))}\n"
            f"👤 <i>Артист:</i> {escape_html(release.get('nick', '—'))}\n"
            f"🕐 <i>Отклонено:</i> {escape_html(moderation_time)}\n"
            f"👨‍💼 <i>Модератор:</i> @{escape_html(moderator_username)}\n\n"
            f"❌ <b>Причина:</b>\n{escape_html(reject_reason)}\n\n"
            f"{WINTER_EMOJIS['sparkles']} Отправьте релиз заново через /start после исправлений.",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        print(f"Ошибка отправки уведомления артисту: {e}")
    
    await update.message.reply_text(f"{WINTER_EMOJIS['check']} Релиз отклонён. Артист уведомлен.")


async def add_upc_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик добавления UPC кода через reply на сообщение анкеты."""
    if not update.message or not update.message.reply_to_message:
        return
    if update.message.chat_id != MODERATION_CHAT_ID:
        return
    
    # Проверяем что это админ
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ Только администраторы могут добавлять UPC.")
        return
    
    # Получаем сообщение на которое ответили
    replied_msg = update.message.reply_to_message
    replied_msg_id = replied_msg.message_id
    
    # Ищем анкету в БД по moderation_message_id
    user_id = None
    idx = None
    for uid, releases in db.items():
        for idx_rel, rel in enumerate(releases):
            if rel.get('moderation_message_id') == replied_msg_id:
                user_id = uid
                idx = idx_rel
                break
        if user_id:
            break
    
    if not user_id or idx is None:
        return  # Молчаливо игнорируем обычные сообщения
    
    release = db[user_id][idx]
    
    # Получаем UPC из сообщения
    upc_code = clean(update.message.text)
    if not upc_code:
        await update.message.reply_text("❌ UPC код не может быть пустым. Отправьте UPC числа.")
        return
    
    # Проверяем что это выглядит как UPC (обычно 12-14 цифр)
    if not upc_code.isdigit() or len(upc_code) < 10:
        await update.message.reply_text("❌ UPC должен быть числовым кодом (обычно 12-14 цифр)")
        return
    
    # Сохраняем UPC в релизе
    release["upc"] = upc_code
    save_db(db)
    update_moderation_record(user_id, idx, release)
    
    # Уведомляем модератора
    await update.message.reply_text(f"{WINTER_EMOJIS['check']} UPC код <code>{upc_code}</code> добавлен и сохранен!")
    
    # Уведомляем артиста
    try:
        await context.bot.send_message(
            int(user_id),
            f"{WINTER_EMOJIS['check']} <b>UPC КОД ДОБАВЛЕН</b>\n\n"
            f"📝 <b>{escape_html(release.get('name', '—'))}</b>\n"
            f"📦 <b>UPC:</b> <code>{escape_html(upc_code)}</code>\n\n"
            f"Ваш релиз готов к публикации!",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        print(f"Ошибка отправки уведомления артисту об UPC: {e}")


async def moderation_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    # Проверяем, что это сообщение из чата модерации
    if query.message.chat_id != MODERATION_CHAT_ID:
        return
    
    await query.answer()
    
    try:
        # Разбор callback_data. Поддерживаем случаи типа:
        # m_upload_<user>_<idx>
        # m_rejectreason_<user>_<idx>_<reason_idx>
        # m_add_upc_<user>_<idx>
        parts = query.data.split("_")
        if len(parts) < 4 or parts[0] != "m":
            return
        # Специальный случай: m_add_upc_<user>_<idx>
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
                await query.answer("Релиз не найден", show_alert=True)
                return
        
        if user_id not in db:
            await query.answer("Релиз не найден", show_alert=True)
            return
            
        if idx >= len(db[user_id]):
            await query.answer("Релиз не найден", show_alert=True)
            return
        
        release = db[user_id][idx]

        moderator_name = query.from_user.username or query.from_user.first_name

        # FIX: обработчик для переключения статусов (промежуточные статусы)
        if action == "upload":
            # Переключаем статус на "на отгрузке"
            old_status = release.get("status")
            release["status"] = STATUS_ON_UPLOAD
            release["moderator"] = moderator_name
            release["moderation_time"] = datetime.now().isoformat()
            add_history_entry(user_id, idx, old_status, STATUS_ON_UPLOAD, query.from_user.id, moderator_name)
            save_db(db)
            update_moderation_record(user_id, idx, release)

            # Обновляем сообщение в модерации
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_ON_UPLOAD, moderator_username=moderator_name, reply_markup=query.message.reply_markup)
            
            # Уведомление артисту
            try:
                moderation_time = datetime.now().strftime("%d.%m.%Y в %H:%M")
                await context.bot.send_message(
                    int(user_id),
                    f"{WINTER_EMOJIS['upload']} <b>РЕЛИЗ НА ОТГРУЗКЕ</b>\n\n"
                    f"📝 <b>{escape_html(release.get('name', '—'))}</b>\n"
                    f"🎵 <i>Тип:</i> {escape_html(release.get('type', '—'))}\n"
                    f"📅 <i>Дата релиза:</i> {escape_html(release.get('date', '—'))}\n"
                    f"👤 <i>Артист:</i> {escape_html(release.get('nick', '—'))}\n"
                    f"🕐 <i>Время:</i> {escape_html(moderation_time)}\n"
                    f"👨‍💼 <i>Модератор:</i> @{escape_html(moderator_name)}\n\n"
                    f"{WINTER_EMOJIS['sparkles']} Ваш релиз готовится к выпуску!",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                print(f"Ошибка отправки уведомления на отгрузку: {e}")
            
            # Восстанавливаем кнопки статусов (промежуточный статус - кнопки остаются активны)
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🕓 На отгрузке", callback_data=f"m_upload_{user_id}_{idx}"),
                    InlineKeyboardButton("🧠 Модерация", callback_data=f"m_moderate_{user_id}_{idx}"),
                    InlineKeyboardButton("✅ Принято", callback_data=f"m_approve_{user_id}_{idx}")
                ],
                [
                    InlineKeyboardButton("❌ Отклонить", callback_data=f"m_reject_{user_id}_{idx}"),
                    InlineKeyboardButton("✏️ На исправлении", callback_data=f"m_needfix_{user_id}_{idx}"),
                    InlineKeyboardButton("🗑 Удален", callback_data=f"m_delete_{user_id}_{idx}")
                ],
            ])
            await safe_edit_reply_markup(query, reply_markup=keyboard)
            return

        if action == "moderate":
            # Переключаем статус на "модерация"
            old_status = release.get("status")
            release["status"] = STATUS_MODERATION
            release["moderator"] = moderator_name
            release["moderation_time"] = datetime.now().isoformat()
            add_history_entry(user_id, idx, old_status, STATUS_MODERATION, query.from_user.id, moderator_name)
            save_db(db)
            update_moderation_record(user_id, idx, release)

            # Обновляем сообщение в модерации
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_MODERATION, moderator_username=moderator_name, reply_markup=query.message.reply_markup)
            
            # Уведомление артисту
            try:
                moderation_time = datetime.now().strftime("%d.%m.%Y в %H:%M")
                await context.bot.send_message(
                    int(user_id),
                    f"{WINTER_EMOJIS['brain']} <b>РЕЛИЗ НА МОДЕРАЦИИ</b>\n\n"
                    f"📝 <b>{escape_html(release.get('name', '—'))}</b>\n"
                    f"🎵 <i>Тип:</i> {escape_html(release.get('type', '—'))}\n"
                    f"📅 <i>Дата релиза:</i> {escape_html(release.get('date', '—'))}\n"
                    f"👤 <i>Артист:</i> {escape_html(release.get('nick', '—'))}\n"
                    f"🕐 <i>Время:</i> {escape_html(moderation_time)}\n"
                    f"👨‍💼 <i>Модератор:</i> @{escape_html(moderator_name)}\n\n"
                    f"{WINTER_EMOJIS['sparkles']} Ваш релиз проходит проверку качества!",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                print(f"Ошибка отправки уведомления о модерации: {e}")
            
            # Восстанавливаем кнопки статусов (промежуточный статус - кнопки остаются активны)
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🕓 На отгрузке", callback_data=f"m_upload_{user_id}_{idx}"),
                    InlineKeyboardButton("🧠 Модерация", callback_data=f"m_moderate_{user_id}_{idx}"),
                    InlineKeyboardButton("✅ Принято", callback_data=f"m_approve_{user_id}_{idx}")
                ],
                [
                    InlineKeyboardButton("❌ Отклонить", callback_data=f"m_reject_{user_id}_{idx}"),
                    InlineKeyboardButton("✏️ На исправлении", callback_data=f"m_needfix_{user_id}_{idx}"),
                    InlineKeyboardButton("🗑 Удален", callback_data=f"m_delete_{user_id}_{idx}")
                ],
            ])
            await safe_edit_reply_markup(query, reply_markup=keyboard)
            return

        if action == "approve":
            # FIX: Упрощённая система - просто одобряем без доп.кнопок
            old_status = release.get("status")
            release["status"] = STATUS_APPROVED
            release["moderator"] = moderator_name
            release["moderation_time"] = datetime.now().isoformat()
            add_history_entry(user_id, idx, old_status, STATUS_APPROVED, query.from_user.id, moderator_name)
            save_db(db)
            update_moderation_record(user_id, idx, release)

            # Обновляем сообщение в модерации (отправляем отдельное сообщение со статусом)
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_APPROVED, moderator_username=moderator_name, reply_markup=query.message.reply_markup)

            # Отправляем сообщение с кнопкой для добавления UPC
            try:
                upc_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📦 Присвоить UPC", callback_data=f"m_add_upc_{user_id}_{idx}")]
                ])
                await context.bot.send_message(
                    chat_id=MODERATION_CHAT_ID,
                    text="💾 <b>Добавьте UPC код для этого релиза</b>\n\n"
                         "Нажмите кнопку и ответьте UPC кодом на исходное сообщение анкеты.",
                    reply_to_message_id=query.message.message_id,
                    parse_mode=ParseMode.HTML,
                    reply_markup=upc_keyboard
                )
            except Exception as e:
                print(f"Ошибка отправки кнопки UPC: {e}")

            # Уведомление артисту
            try:
                moderation_time = datetime.now().strftime("%d.%m.%Y в %H:%M")
                await context.bot.send_message(
                    int(user_id),
                    f"{WINTER_EMOJIS['check']} <b>ВАШ РЕЛИЗ ОДОБРЕН!</b>\n\n"
                    f"📝 <b>{escape_html(release.get('name', '—'))}</b>\n"
                    f"🎵 <i>Тип:</i> {escape_html(release.get('type', '—'))}\n"
                    f"📅 <i>Дата релиза:</i> {escape_html(release.get('date', '—'))}\n"
                    f"👤 <i>Артист:</i> {escape_html(release.get('nick', '—'))}\n"
                    f"🕐 <i>Одобрено:</i> {escape_html(moderation_time)}\n"
                    f"👨‍💼 <i>Модератор:</i> @{escape_html(moderator_name)}\n\n"
                    f"{WINTER_EMOJIS['sparkles']} Готов к публикации на всех платформах!",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                print(f"Ошибка отправки пользователю: {e}")
            return
        if action == "reject":
            # FIX: Показываем клавиатуру с предустановленными причинами отклонения
            reasons = [
                "Слабый материал",
                "Не формат лейбла",
                "Плохое качество",
                "Нет идеи",
                "Дубликат релиза",
            ]
            # callback_data: m_rejectreason_<user_id>_<idx>_<reason_idx>
            rows = [[InlineKeyboardButton(r, callback_data=f"m_rejectreason_{user_id}_{idx}_{i}")] for i, r in enumerate(reasons, start=1)]
            keyboard = InlineKeyboardMarkup(rows)
            await context.bot.send_message(
                chat_id=MODERATION_CHAT_ID,
                text=f"Выберите причину отклонения для анкеты выше:",
                reply_to_message_id=query.message.message_id,
                reply_markup=keyboard,
            )
            return
        if action == "rejectreason":
            # parts: ['m', 'rejectreason', user_id, idx, reason_idx]
            if len(parts) >= 5:
                user_id = parts[2]
                idx = int(parts[3])
                reason_idx = int(parts[4])
                reasons = [
                    "Слабый материал",
                    "Не формат лейбла",
                    "Плохое качество",
                    "Нет идеи",
                    "Дубликат релиза",
                ]
                reason_text = reasons[reason_idx - 1] if 1 <= reason_idx <= len(reasons) else "Причина"
                old_status = release.get("status")
                release["status"] = STATUS_REJECTED
                release["reject_reason"] = reason_text
                release["moderator"] = moderator_name
                release["moderation_time"] = datetime.now().isoformat()
                add_history_entry(user_id, idx, old_status, STATUS_REJECTED, query.from_user.id, moderator_name, reason=reason_text)
                save_db(db)
                update_moderation_record(user_id, idx, release)

                # Обновляем сообщение в модерации (сохраняя существующую клавиатуру)
                original = release.get("moderation_original_text") or (query.message.text or "")
                await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_REJECTED, moderator_username=moderator_name, reason=reason_text, reply_markup=query.message.reply_markup)
                
                # Заменяем клавиатуру на кнопку "Изменить статус" после обновления текста
                edit_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Изменить статус", callback_data=f"m_restore_buttons_{user_id}_{idx}")]
                ])
                await safe_edit_reply_markup(query, reply_markup=edit_keyboard)
                
                # Уведомление артисту
                try:
                    moderation_time = datetime.now().strftime("%d.%m.%Y в %H:%M")
                    await context.bot.send_message(
                        int(user_id),
                        f"{WINTER_EMOJIS['cross']} <b>ВАШ РЕЛИЗ ОТКЛОНЁН</b>\n\n"
                        f"📝 <b>{escape_html(release.get('name', '—'))}</b>\n"
                        f"🎵 <i>Тип:</i> {escape_html(release.get('type', '—'))}\n"
                        f"📅 <i>Дата релиза:</i> {escape_html(release.get('date', '—'))}\n"
                        f"👤 <i>Артист:</i> {escape_html(release.get('nick', '—'))}\n"
                        f"🕐 <i>Отклонено:</i> {escape_html(moderation_time)}\n"
                        f"👨‍💼 <i>Модератор:</i> @{escape_html(moderator_name)}\n\n"
                        f"❌ <b>Причина отказа:</b>\n{escape_html(reason_text)}\n\n"
                        f"{WINTER_EMOJIS['sparkles']} Отправьте релиз заново после исправлений через /start",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception as e:
                    print(f"Ошибка отправки уведомления об отклонении: {e}")
                return
        if action == "needfix":
            # Быстрая пометка: попросить правки — добавим комментарий и уведомим автора
            old_status = release.get("status")
            release["status"] = STATUS_NEEDS_FIX
            release["moderator"] = moderator_name
            release["moderation_time"] = datetime.now().isoformat()
            add_history_entry(user_id, idx, old_status, STATUS_NEEDS_FIX, query.from_user.id, moderator_name)
            save_db(db)
            update_moderation_record(user_id, idx, release)

            # Обновляем сообщение в модерации (сохраняя существующую клавиатуру)
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_NEEDS_FIX, moderator_username=moderator_name, reason="Требуются правки", reply_markup=query.message.reply_markup)

            # Заменяем кнопки на "Изменить статус" после обновления текста
            edit_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Изменить статус", callback_data=f"m_restore_buttons_{user_id}_{idx}")]
            ])
            await safe_edit_reply_markup(query, reply_markup=edit_keyboard)

            try:
                moderation_time = datetime.now().strftime("%d.%m.%Y в %H:%M")
                await context.bot.send_message(
                    int(user_id),
                    f"{WINTER_EMOJIS['warning']} <b>ТРЕБУЮТСЯ ПРАВКИ</b>\n\n"
                    f"📝 <b>{escape_html(release.get('name', '—'))}</b>\n"
                    f"🎵 <i>Тип:</i> {escape_html(release.get('type', '—'))}\n"
                    f"📅 <i>Дата релиза:</i> {escape_html(release.get('date', '—'))}\n"
                    f"👤 <i>Артист:</i> {escape_html(release.get('nick', '—'))}\n"
                    f"🕐 <i>Время:</i> {escape_html(moderation_time)}\n"
                    f"👨‍💼 <i>Модератор:</i> @{escape_html(moderator_name)}\n\n"
                    f"❗ <b>Ваш релиз требует доработки. Пожалуйста, исправьте замечания и отправьте заново.</b>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                print(f"Ошибка отправки уведомления о правках: {e}")
            return

        if action == "link":
            # Быстрая пометка: проблема со ссылкой
            old_status = release.get("status")
            release["status"] = STATUS_NEEDS_FIX
            release["moderator"] = moderator_name
            release["moderation_time"] = datetime.now().isoformat()
            add_history_entry(user_id, idx, old_status, STATUS_NEEDS_FIX, query.from_user.id, moderator_name)
            save_db(db)

            await safe_edit_reply_markup(query, reply_markup=None)
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_NEEDS_FIX, moderator_username=moderator_name, reason="Проблема со ссылкой", reply_markup=query.message.reply_markup)
            try:
                await context.bot.send_message(int(user_id), f"{WINTER_EMOJIS['warning']} <b>Проблема со ссылкой</b>\n\nПроверьте ссылку на файлы или карточку Яндекс Музыки и отправьте заново.")
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

            # Обновляем сообщение в модерации (сохраняя существующую клавиатуру)
            original = release.get("moderation_original_text") or (query.message.text or "")
            await _append_status_to_moderation_message(context, query.message.message_id, original, STATUS_DELETED, moderator_username=moderator_name, reason="Служебно удалено", reply_markup=query.message.reply_markup)
            
            # Заменяем клавиатуру на кнопку "Изменить статус" после обновления текста
            edit_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Изменить статус", callback_data=f"m_restore_buttons_{user_id}_{idx}")]
            ])
            await safe_edit_reply_markup(query, reply_markup=edit_keyboard)
            
            try:
                moderation_time = datetime.now().strftime("%d.%m.%Y в %H:%M")
                await context.bot.send_message(
                    int(user_id),
                    f"{WINTER_EMOJIS['delete']} <b>АНКЕТА ПОМЕЧЕНА КАК УДАЛЁННАЯ</b>\n\n"
                    f"📝 <b>{escape_html(release.get('name', '—'))}</b>\n"
                    f"🎵 <i>Тип:</i> {escape_html(release.get('type', '—'))}\n"
                    f"👤 <i>Артист:</i> {escape_html(release.get('nick', '—'))}\n"
                    f"🕐 <i>Удалено:</i> {escape_html(moderation_time)}\n"
                    f"👨‍💼 <i>Модератор:</i> @{escape_html(moderator_name)}\n\n"
                    f"Если это ошибка — свяжитесь с модераторами.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                print(f"Ошибка отправки уведомления об удалении: {e}")
            return
        
        if action == "add_upc":
            # Показываем сообщение что нужно ответить UPC кодом
            await query.answer("ℹ️ Ответьте на исходное сообщение анкеты только UPC кодом (например: 5099994682101)", show_alert=True)
            return
        
        if action == "restore_buttons":
            # Восстанавливаем исходные кнопки статусов вместо "Изменить статус"
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🕓 На отгрузке", callback_data=f"m_upload_{user_id}_{idx}"),
                    InlineKeyboardButton("🧠 Модерация", callback_data=f"m_moderate_{user_id}_{idx}"),
                    InlineKeyboardButton("✅ Принято", callback_data=f"m_approve_{user_id}_{idx}")
                ],
                [
                    InlineKeyboardButton("❌ Отклонить", callback_data=f"m_reject_{user_id}_{idx}"),
                    InlineKeyboardButton("✏️ На исправлении", callback_data=f"m_needfix_{user_id}_{idx}"),
                    InlineKeyboardButton("🗑 Удален", callback_data=f"m_delete_{user_id}_{idx}")
                ],
            ])
            await safe_edit_reply_markup(query, reply_markup=keyboard)
            await query.answer("✅ Кнопки восстановлены", show_alert=False)
            return
    except Exception as e:
        import traceback
        print(f"❌ Ошибка в moderation_handler: {e}")
        traceback.print_exception(type(e), e, e.__traceback__)
        try:
            await query.answer("Произошла ошибка при обработке", show_alert=True)
        except:
            pass

# === ОБРАБОТКА ОШИБОК ===
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    import traceback
    # Не спамим в консоль ошибкой RemoteProtocolError как "фатальной"
    if context.error and _is_remote_protocol_error(context.error):
        print("⚠️ RemoteProtocolError: сервер разорвал соединение без ответа (поймали и пережили).")
        return
    print(f"❌ Ошибка: {context.error}")
    if context.error:
        traceback.print_exception(type(context.error), context.error, context.error.__traceback__)


# === НАПОМНИТЕЛЬ О НА ОТГРУЗКЕ ===
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
                        release_name = escape_html(r.get('name', 'Анкета'))
                        artist_name = escape_html(r.get('nick', 'Артист'))
                        submission_time_str = submit_time.strftime("%d.%m.%Y в %H:%M")
                        
                        try:
                            reminder_text = (
                                f"⏰ <b>НАПОМИНАНИЕ</b>\n\n"
                                f"🎵 <b>{release_name}</b>\n"
                                f"👤 Артист: {artist_name}\n"
                                f"📅 Отправлено: {submission_time_str}\n"
                                f"⏱️ Прошло: {int(hours_passed)} часов\n\n"
                                f"❗ Анкета находится на отгрузке более 2 дней!\n"
                                f"Необходимо провести загрузку на платформы."
                            )
                            await context.bot.send_message(
                                chat_id=MODERATION_CHAT_ID,
                                text=reminder_text,
                                reply_to_message_id=msg_id,
                                parse_mode=ParseMode.HTML
                            )
                            r['reminder_sent'] = True
                        except Exception as e:
                            print(f"Ошибка отправки напоминания: {e}")
                except Exception:
                    continue
        save_db(db)
    except Exception as e:
        print(f"Ошибка в напоминателе on_upload: {e}")


async def undo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Откатывает последнее сохранённое значение для пользователя
    user_id = str(update.message.from_user.id)
    last = pop_last_history(user_id)
    if not last:
        await update.message.reply_text("Нет предыдущих значений для отмены.")
        return
    key, old = last
    user_data.setdefault(user_id, {})[key] = old
    save_draft_for_user(user_id)
    await update.message.reply_text(f"Поле '{key}' восстановлено.")

# === ЗАПУСК ===
def main():
    app = Application.builder().token(TOKEN).read_timeout(120).build()
    
    app.add_handler(CommandHandler('help', help_cmd))
    app.add_handler(CommandHandler('cancel', cancel_cmd))
    app.add_handler(CommandHandler('my', my_cmd))
    app.add_handler(CommandHandler('search', search_cmd))
    app.add_handler(CommandHandler('admin', admin_panel))
    app.add_handler(CommandHandler('backup', backup_cmd))
    app.add_handler(CommandHandler('moderation_backup', moderation_backup_cmd))
    # FIX: /stats переименована на /statss (работает только в чате модерации для админов)
    app.add_handler(CommandHandler('statss', admin_stats_cmd))
    app.add_handler(CommandHandler('broadcast', broadcast_cmd))
    app.add_handler(CommandHandler('cleanbase', cleanbase_cmd))
    app.add_handler(CommandHandler('undo', undo_cmd))
    app.add_handler(CommandHandler('cleanup', cleanup_database))

    # FIX: Модерация ДОЛЖНА быть ПЕРВЫМ обработчиком до ConversationHandler и глобального button
    # Модерация: отдельный handler по паттерну m_*
    app.add_handler(CallbackQueryHandler(moderation_handler, pattern=r"^m_.*"))
    # FIX: Обработчик добавления UPC кода через reply в чате модерации (проверяем по УПК-подобному коду)
    app.add_handler(MessageHandler(filters.TEXT & filters.REPLY & filters.Chat(MODERATION_CHAT_ID) & ~filters.COMMAND, add_upc_handler), group=1)
    # FIX: Обработчик ручного отклонения анкеты через reply в чате модерации
    app.add_handler(MessageHandler(filters.TEXT & filters.REPLY & filters.Chat(MODERATION_CHAT_ID), manual_reject_handler), group=2)

    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start_cmd)],
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
            CONFIRM: [CallbackQueryHandler(button)],
        },
        fallbacks=[CommandHandler('start', start_cmd), CommandHandler('cancel', cancel_cmd)],
        per_message=False,
        per_chat=True
    )
    
    app.add_handler(conv)
    # ГЛОБАЛЬНО: чтобы /admin кнопки работали даже если пользователь не в ConversationHandler state.
    app.add_handler(CallbackQueryHandler(button))
    # FIX: error_handler должен быть в конце
    app.add_error_handler(error_handler)
    # Регистрация фоновой задачи: напоминания по карточкам на отгрузке (каждые 30 минут)
    try:
        app.job_queue.run_repeating(_check_on_upload_reminders, interval=30*60, first=60)
    except Exception:
        # Если очередь не доступна — не критично
        pass
    
    print(f"{WINTER_EMOJIS['snowflake']} БОТ ЗАПУЩЕН! {WINTER_EMOJIS['snowflake']}")
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
