import json
import os
import threading
import urllib.parse
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler
)
from telegram.error import BadRequest, TimedOut, Forbidden
import asyncio

# === КОНФИГ ===
TOKEN = os.getenv("BOT_TOKEN", "7932680631:AAG3DW6gwg0Ccvuiq45aPVCSSWsOallp_Pk")
MODERATION_CHAT_ID = -1002117586464
ADMIN_IDS = [881379104]
ARTISTS_CHAT = "https://t.me/+oVmX3_dkyWJhNjJi"
CHANNEL = "https://t.me/cxrnermusic"
DB_FILE = "releases.json"
MODERATION_DB_FILE = "moderation_releases.json"
# URL для Mini App (замените на ваш реальный URL bothost.ru)
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://cxrnerlink.ct.ws/panel.html")
# Username бота (для ссылок в Mini App)
BOT_USERNAME = os.getenv("BOT_USERNAME", "moder_cxrner_bot")

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
    "refresh": "🔄"
}

# === СОСТОЯНИЯ ===
(REPORT, TYPE, NAME, NICK, FIO, DATE, VERSION, GENRE, LINK, MAT, PROMO, COMMENT, TG, CONFIRM,
 ALBUM_NICK, ALBUM_FIO, ALBUM_TRACKLIST, ALBUM_TG, SINGLE_NICK, SINGLE_FIO, SINGLE_TG,
 REJECT_REASON, MODERATION_COMMENT) = range(23)

# === БД ===
def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_db(db):
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def load_moderation_db():
    if os.path.exists(MODERATION_DB_FILE):
        with open(MODERATION_DB_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_moderation_db(moderation_db):
    with open(MODERATION_DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(moderation_db, f, ensure_ascii=False, indent=2)

user_data = {}
db = load_db()
moderation_db = load_moderation_db()

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

# === БЕЗОПАСНАЯ ОТПРАВКА ===
async def safe_send(target, text, reply_markup=None, parse_mode=ParseMode.HTML):
    message = target if hasattr(target, 'reply_text') else target.message
    for _ in range(3):
        try:
            await message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            return
        except TimedOut:
            await asyncio.sleep(2)
        except BadRequest as e:
            if "can't parse entities" in str(e).lower():
                await message.reply_text(text.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', ''), reply_markup=reply_markup)
            else:
                raise
        except Exception:
            await message.reply_text(text.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', ''), reply_markup=reply_markup)
            return
    await message.reply_text("Не удалось отправить.")

async def safe_edit(query, text, reply_markup=None, parse_mode=ParseMode.HTML):
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
    except (BadRequest, TimedOut, Forbidden):
        await query.message.reply_text(text.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', ''), reply_markup=reply_markup)

# === ЗИМНЕЕ ОФОРМЛЕНИЕ ===
def winter_text(text, emoji_key=None):
    if emoji_key and emoji_key in WINTER_EMOJIS:
        return f"{WINTER_EMOJIS[emoji_key]} {text}"
    return text

def winter_header(text):
    return f"{WINTER_EMOJIS['snowflake']} {text} {WINTER_EMOJIS['snowflake']}"

# === ПРОВЕРКА АДМИНА ===
def is_admin(user_id):
    return user_id in ADMIN_IDS

# === ГЛАВНОЕ МЕНЮ (/start) ===
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("📱 Панель управления", "settings"), web_app=WebAppInfo(url=WEBAPP_URL))],
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
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )
    return REPORT

# === МОИ РЕЛИЗЫ (/my) ===
async def my_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    releases = db.get(user_id, [])
    
    total = len(releases)
    pending = sum(1 for r in releases if r.get('status', 'pending') == 'pending')
    approved = sum(1 for r in releases if r.get('status') == 'approved')
    rejected = sum(1 for r in releases if r.get('status') == 'rejected')
    published = sum(1 for r in releases if r.get('status') == 'published')

    stats = (
        f"{winter_header('Твоя статистика')}\n"
        f"{WINTER_EMOJIS['notes']} Всего релизов: <b>{total}</b>\n"
        f"{WINTER_EMOJIS['waiting']} Ожидает: <b>{pending}</b>\n"
        f"{WINTER_EMOJIS['check']} Одобрено: <b>{approved}</b>\n"
        f"{WINTER_EMOJIS['cross']} Отклонено: <b>{rejected}</b>\n"
        f"{WINTER_EMOJIS['published']} Опубликовано: <b>{published}</b>"
    )

    if not releases:
        await update.message.reply_text(
            f"{stats}\n\n<i>У вас пока нет релизов.</i>\n\n/start {WINTER_EMOJIS['gift']} отправить первый!",
            parse_mode=ParseMode.HTML
        )
        return

    text = f"{stats}\n\n<b>Твои релизы:</b>\n\n"
    status_emoji = {"pending": WINTER_EMOJIS['waiting'], "approved": WINTER_EMOJIS['check'], 
                   "rejected": WINTER_EMOJIS['cross'], "published": WINTER_EMOJIS['published']}
    
    for i, rel in enumerate(releases, 1):
        status = rel.get('status', 'pending')
        emoji = status_emoji.get(status, WINTER_EMOJIS['waiting'])
        status_text = {"pending": "Ожидает", "approved": "Одобрено", 
                      "rejected": "Отклонено", "published": "Опубликовано"}.get(status, "Ожидает")
        link = f"\n<a href='{rel.get('link_published', '')}'>Слушать</a>" if status == 'published' and rel.get('link_published') else ""
        text += (
            f"<b>{i}. {escape_html(rel.get('name', 'Без названия'))}</b> {escape_html(emoji)}\n"
            f"<i>Тип:</i> {escape_html(rel.get('type', '—'))}\n"
            f"<i>Ник:</i> {escape_html(rel.get('nick', '—'))}\n"
            f"<i>Дата:</i> {escape_html(rel.get('date', '—'))}\n"
            f"<i>Жанр:</i> {escape_html(rel.get('genre', '—'))}\n"
            f"<i>Мат:</i> {escape_html(rel.get('mat', '—'))}\n"
            f"<i>Статус:</i> {escape_html(status_text)}"
        )
        if status == 'rejected' and rel.get('reject_reason'):
            text += f" ({escape_html(rel['reject_reason'])})"
        text += f"{link}\n\n"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Отправить новый", "music"), callback_data='report')],
        [InlineKeyboardButton(winter_text("Меню", "tree"), callback_data='main')]
    ])
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# === РАСШИРЕННАЯ АДМИН-ПАНЕЛЬ (/admin) ===
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Доступ запрещён.")
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
    
    keyboard = InlineKeyboardMarkup([
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
    ])
    
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === СТАТИСТИКА ДЛЯ АДМИНА ===
async def admin_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Доступ запрещён.")
        return

    # Подробная статистика
    total_users = len(db)
    total_releases = sum(len(v) for v in db.values())
    
    # Статистика по статусам
    status_stats = {'pending': 0, 'approved': 0, 'rejected': 0, 'published': 0}
    for user_releases in db.values():
        for release in user_releases:
            status = release.get('status', 'pending')
            status_stats[status] = status_stats.get(status, 0) + 1
    
    # Статистика по типам релизов
    type_stats = {'сингл': 0, 'альбом': 0}
    for user_releases in db.values():
        for release in user_releases:
            rel_type = release.get('type', 'сингл')
            type_stats[rel_type] = type_stats.get(rel_type, 0) + 1
    
    # Активные пользователи (отправили хотя бы 1 релиз)
    active_users = sum(1 for releases in db.values() if len(releases) > 0)
    
    text = (
        f"{winter_header('ДЕТАЛЬНАЯ СТАТИСТИКА')}\n\n"
        f"{WINTER_EMOJIS['users']} <b>ПОЛЬЗОВАТЕЛИ:</b>\n"
        f"• Всего: <b>{total_users}</b>\n"
        f"• Активных: <b>{active_users}</b>\n\n"
        
        f"{WINTER_EMOJIS['notes']} <b>РЕЛИЗЫ:</b>\n"
        f"• Всего: <b>{total_releases}</b>\n"
        f"• Синглов: <b>{type_stats['сингл']}</b>\n"
        f"• Альбомов: <b>{type_stats['альбом']}</b>\n\n"
        
        f"{WINTER_EMOJIS['stats']} <b>СТАТУСЫ:</b>\n"
        f"• Ожидает: <b>{status_stats['pending']}</b>\n"
        f"• Одобрено: <b>{status_stats['approved']}</b>\n"
        f"• Отклонено: <b>{status_stats['rejected']}</b>\n"
        f"• Опубликовано: <b>{status_stats['published']}</b>\n\n"
        
        f"{WINTER_EMOJIS['calendar']} <b>ПОСЛЕДНИЕ ДЕЙСТВИЯ:</b>\n"
    )
    
    # Добавляем последние 5 релизов
    recent_releases = []
    for user_id, releases in db.items():
        for release in releases:
            if 'submission_time' in release:
                recent_releases.append((release['submission_time'], release))
    
    recent_releases.sort(key=lambda x: x[0], reverse=True)
    
    for i, (time, release) in enumerate(recent_releases[:5], 1):
        status_emoji = {
            'pending': WINTER_EMOJIS['waiting'],
            'approved': WINTER_EMOJIS['check'],
            'rejected': WINTER_EMOJIS['cross'],
            'published': WINTER_EMOJIS['published']
        }
        status = release.get('status', 'pending')
        text += f"{i}. {escape_html(release.get('name', 'Без названия'))} {status_emoji[status]}\n"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
    ])
    
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

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
        
        text += (
            f"<b>{i}. {escape_html(release.get('name', 'Без названия'))}</b> {emoji}\n"
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
    query = update.callback_query
    if not is_admin(query.from_user.id):
        await query.answer("Доступ запрещён", show_alert=True)
        return

    # Удаляем пользователей без релизов
    users_before = len(db)
    empty_users = [user_id for user_id, releases in db.items() if not releases]
    for user_id in empty_users:
        del db[user_id]
    
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
    
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

# === УДАЛЕНИЕ ВСЕХ РЕЛИЗОВ ===
async def cleanbase_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Доступ запрещён.")
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
    
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

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
    
    progress_msg = await update.message.reply_text(
        f"{WINTER_EMOJIS['waiting']} <b>Начинаю рассылку...</b>"
    )

    for user_id in db.keys():
        try:
            await context.bot.send_message(
                int(user_id),
                broadcast_text,
                parse_mode=ParseMode.HTML
            )
            sent_count += 1
            await asyncio.sleep(0.1)  # Задержка чтобы не превысить лимиты
        except Exception as e:
            error_count += 1
            print(f"Ошибка отправки пользователю {user_id}: {e}")

    await progress_msg.edit_text(
        f"{WINTER_EMOJIS['check']} <b>РАССЫЛКА ЗАВЕРШЕНА!</b>\n\n"
        f"• Успешно: <b>{sent_count}</b>\n"
        f"• Ошибок: <b>{error_count}</b>\n"
        f"• Всего: <b>{sent_count + error_count}</b>",
        parse_mode=ParseMode.HTML
    )

# === ОТПРАВКА ФАЙЛОВ БАЗЫ ДАННЫХ ===
async def send_database_backup(query, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(query.from_user.id):
        await query.answer("Доступ запрещён", show_alert=True)
        return
        
    try:
        with open(DB_FILE, 'rb') as f:
            await context.bot.send_document(
                chat_id=query.from_user.id,
                document=f,
                filename=f"releases_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                caption=f"{WINTER_EMOJIS['snowflake']} Резервная копия базы данных релизов"
            )
        await query.answer("База данных отправлена!", show_alert=True)
    except Exception as e:
        await query.answer(f"Ошибка: {e}", show_alert=True)

async def send_moderation_backup(query, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(query.from_user.id):
        await query.answer("Доступ запрещён", show_alert=True)
        return
        
    try:
        with open(MODERATION_DB_FILE, 'rb') as f:
            await context.bot.send_document(
                chat_id=query.from_user.id,
                document=f,
                filename=f"moderation_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                caption=f"{WINTER_EMOJIS['snowman']} Архив модерации"
            )
        await query.answer("Архив модерации отправлен!", show_alert=True)
    except Exception as e:
        await query.answer(f"Ошибка: {e}", show_alert=True)

# === КОМАНДЫ АДМИНА ДЛЯ БЭКАПА ===
async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Доступ запрещён.")
        return
    await send_database_backup(update.message, context)

async def moderation_backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Доступ запрещён.")
        return
    await send_moderation_backup(update.message, context)

async def send_database_backup(message, context: ContextTypes.DEFAULT_TYPE):
    try:
        with open(DB_FILE, 'rb') as f:
            await context.bot.send_document(
                chat_id=message.from_user.id,
                document=f,
                filename=f"releases_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                caption=f"{WINTER_EMOJIS['snowflake']} Резервная копия базы данных релизов"
            )
        await message.reply_text(f"{WINTER_EMOJIS['check']} База данных отправлена!")
    except Exception as e:
        await message.reply_text(f"{WINTER_EMOJIS['cross']} Ошибка: {e}")

async def send_moderation_backup(message, context: ContextTypes.DEFAULT_TYPE):
    try:
        with open(MODERATION_DB_FILE, 'rb') as f:
            await context.bot.send_document(
                chat_id=message.from_user.id,
                document=f,
                filename=f"moderation_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                caption=f"{WINTER_EMOJIS['snowman']} Архив модерации"
            )
        await message.reply_text(f"{WINTER_EMOJIS['check']} Архив модерации отправлен!")
    except Exception as e:
        await message.reply_text(f"{WINTER_EMOJIS['cross']} Ошибка: {e}")

# === КНОПКИ АДМИН-ПАНЕЛИ ===
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
        await my_cmd(query, context)
        return REPORT

    if data == 'single':
        user_data[user_id] = {'type': 'сингл', 'status': 'pending'}
        await safe_edit(query, f"{WINTER_EMOJIS['music']} <b>СИНГЛ</b>\n\n1. Название релиза\nПример: Tokyo Rain")
        return NAME

    if data == 'album':
        user_data[user_id] = {'type': 'альбом', 'status': 'pending'}
        await safe_edit(query, f"{WINTER_EMOJIS['notes']} <b>АЛЬБОМ</b>\n\n1. Название релиза\nПример: Lost in the Void")
        return NAME

    if data == 'send':
        await send_moderation(query, context)
        return REPORT

    if data == 'main':
        return await start_cmd(query, context)
        
    if data == 'get_db':
        await send_database_backup(query, context)
        return
        
    if data == 'get_moderation_db':
        await send_moderation_backup(query, context)
        return
        
    # Админские кнопки
    if data == 'admin_stats':
        await admin_stats_cmd(update, context)
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
        await admin_panel(query, context)
        return
        
    if data == 'broadcast_menu':
        await broadcast_menu(update, context)
        return
        
    if data == 'confirm_cleanbase':
        await cleanbase_cmd(query, context)
        return
        
    if data == 'cleanbase_confirm':
        await cleanbase_confirm(update, context)
        return

# === ПОЛЯ ===
async def name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['name'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['star']} <b>2. Ник исполнителя(ей)</b>\nПример: MAKIZM")
    return SINGLE_NICK if user_data[user_id]['type'] == 'сингл' else ALBUM_NICK

async def single_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['nick'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['star']} <b>3. ФИО исполнителя(ей)</b>\nПример: Иванов Иван")
    return SINGLE_FIO

async def single_fio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['fio'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['calendar']} <b>4. Дата релиза</b>\nМинимум через 5 дней\nФормат: ДД.ММ.ГГГГ")
    return DATE

async def album_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['nick'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['star']} <b>2. ФИО исполнителя(ей) (поочерёдно)</b>\nПример: Иванов Иван, Петров Пётр")
    return ALBUM_FIO

async def album_fio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['fio'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['calendar']} <b>3. Дата релиза</b>\nМинимум через 7 дней\nФормат: ДД.ММ.ГГГГ")
    return DATE

async def date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    text = update.message.text.strip()
    if not all(part.isdigit() for part in text.split('.') if part):
        await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Неверный формат даты! Используйте: ДД.ММ.ГГГГ")
        return DATE
    try:
        date_obj = datetime.strptime(text, "%d.%m.%Y")
        min_days = 5 if user_data[user_id]['type'] == 'сингл' else 7
        if date_obj < datetime.now() + timedelta(days=min_days):
            await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Дата должна быть минимум через {min_days} дней!")
            return DATE
        user_data[user_id]['date'] = text
        await safe_send(update.message, f"{WINTER_EMOJIS['music']} <b>Версия релиза</b>\nSlowed, Speed Up.\nЕсли нет — напиши: -")
        return VERSION
    except ValueError:
        await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Неверный формат даты! Пример: 25.12.2025")
        return DATE

async def version(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    ver = clean(update.message.text)
    user_data[user_id]['version'] = ver if ver != '-' else 'Оригинал'
    await safe_send(update.message, f"{WINTER_EMOJIS['notes']} <b>Жанр релиза</b>\nПример: Phonk, Trap")
    return GENRE

async def genre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['genre'] = clean(update.message.text)
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
    user_data[user_id]['link'] = update.message.text.strip()
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
    user_data[user_id]['promo'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['comment']} <b>Комментарий для модератора (необязательно)</b>")
    return COMMENT

async def comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['comment'] = clean(update.message.text)
    if user_data[user_id]['type'] == 'сингл':
        await safe_send(update.message, f"{WINTER_EMOJIS['telegram']} <b>Ваш Telegram для связи</b>\n@username")
        return SINGLE_TG
    else:
        await safe_send(update.message, f"{WINTER_EMOJIS['list']} <b>Трек-лист альбома</b>\n1. Track 1")
        return ALBUM_TRACKLIST

async def album_tracklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['tracklist'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['telegram']} <b>Ваш Telegram для связи</b>\n@username")
    return ALBUM_TG

async def single_tg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['tg'] = update.message.text.strip()
    await show_confirm(update.message, context)
    return CONFIRM

async def album_tg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['tg'] = update.message.text.strip()
    await show_confirm(update.message, context)
    return CONFIRM

async def show_confirm(message, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(message.from_user.id)
    data = user_data[user_id]
    text = f"{WINTER_EMOJIS['snowflake']} <b>ПРОВЕРЬТЕ АНКЕТУ:</b>\n\n"
    for k, v in data.items():
        if k not in ['type', 'status']:
            text += f"• <b>{k.capitalize()}:</b> {escape_html(v)}\n"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Отправить", "check"), callback_data='send')],
        [InlineKeyboardButton(winter_text("Назад", "cross"), callback_data='main')]
    ])
    await safe_send(message, text, keyboard)

# === ОТПРАВКА В МОДЕРАЦИЮ ===
async def send_moderation(query, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(query.from_user.id)
    data = user_data[user_id]
    data['status'] = 'pending'
    data['submission_time'] = datetime.now().isoformat()
    user = query.from_user

    idx = len(db.get(user_id, []))
    
    # Сохраняем данные для модерации в контексте
    context.user_data['moderation_data'] = {
        'user_id': user_id,
        'idx': idx,
        'data': data.copy()
    }

    # Клавиатура для модерации БЕЗ кнопки "Опубликовать"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Одобрить", "check"), callback_data=f'approve_{user_id}_{idx}')],
        [InlineKeyboardButton(winter_text("Отклонить", "cross"), callback_data=f'reject_{user_id}_{idx}')]
    ])

    msg = (
        f"{WINTER_EMOJIS['snowflake']} <b>НОВАЯ АНКЕТА!</b> \n"
        f"От: @{escape_html(user.username) if user.username else 'нет'}\n"
        f"ID: <code>{user_id}</code>\n"
        f"Тип: {escape_html(data['type'])}\n\n"
    )
    for k, v in data.items():
        if k not in ['type', 'status', 'submission_time']:
            msg += f"• <b>{k.capitalize()}:</b> {escape_html(v)}\n"
    
    try:
        moderation_msg = await context.bot.send_message(MODERATION_CHAT_ID, msg, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        
        # ЗАКРЕПЛЯЕМ сообщение автоматически
        await context.bot.pin_chat_message(chat_id=MODERATION_CHAT_ID, message_id=moderation_msg.message_id)
        
        # Сохраняем ID сообщения для дальнейшего редактирования
        context.user_data['moderation_message_id'] = moderation_msg.message_id
        
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
    db[user_id].append(data.copy())
    save_db(db)
    
    await safe_edit(query, f"{WINTER_EMOJIS['check']} <b>Анкета отправлена!</b> \nОжидайте 12–72 часа.", parse_mode=ParseMode.HTML)

# === ОБНОВЛЕНИЕ СООБЩЕНИЯ В МОДЕРАЦИИ ===
async def update_moderation_message(context, user_id, idx, status, reason=None, moderator_username=None, moderator_comment=None):
    release = db[user_id][idx]
    
    # Создаем обновленное сообщение с сохранением исходной информации
    status_emoji = {
        'approved': WINTER_EMOJIS['check'],
        'rejected': WINTER_EMOJIS['cross'], 
        'published': WINTER_EMOJIS['published']
    }
    
    status_text = {
        'approved': 'ОДОБРЕНО',
        'rejected': 'ОТКЛОНЕНО',
        'published': 'ОПУБЛИКОВАНО'
    }
    
    # Форматируем дату с экранированием
    moderation_time = datetime.now().strftime('%d.%m.%Y %H:%M')
    moderation_time_escaped = escape_html(moderation_time)
    
    msg = (
        f"{status_emoji[status]} <b>АНКЕТА {status_text[status]}!</b> \n\n"
        f"<b>Исходная информация:</b>\n"
        f"От: @{escape_html(release.get('username', 'нет'))}\n"
        f"ID: <code>{user_id}</code>\n"
        f"Тип: {escape_html(release['type'])}\n\n"
    )
    
    # Добавляем все поля анкеты
    for k, v in release.items():
        if k not in ['type', 'status', 'submission_time', 'username', 'moderation_time', 'publish_time', 'reject_reason', 'link_published']:
            msg += f"• <b>{k.capitalize()}:</b> {escape_html(v)}\n"
    
    # Добавляем информацию о модераторе
    if moderator_username:
        msg += f"\n<b>Модератор:</b> @{escape_html(moderator_username)}"
    
    # Добавляем комментарий модератора
    if moderator_comment:
        msg += f"\n<b>Комментарий модератора:</b> {escape_html(moderator_comment)}"
    
    # Добавляем информацию о статусе
    if status == 'rejected' and reason:
        msg += f"\n<b>Причина отклонения:</b> {escape_html(reason)}"
    elif status == 'published' and release.get('link_published'):
        msg += f"\n<b>Ссылка на релиз:</b> {escape_html(release['link_published'])}"
    
    msg += f"\n\n<b>Время модерации:</b> {moderation_time_escaped}"
    
    # Обновляем сообщение в группе модерации (убираем кнопки)
    try:
        await context.bot.edit_message_text(
            chat_id=MODERATION_CHAT_ID,
            message_id=context.user_data.get('moderation_message_id'),
            text=msg,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        print(f"Ошибка при обновлении сообщения: {e}")

# === УПРОЩЕННАЯ ОБРАБОТКА ОДОБРЕНИЯ ===
async def handle_approve_with_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        return
    
    if update.message.chat_id != MODERATION_CHAT_ID:
        return
        
    replied_message = update.message.reply_to_message
    comment_text = clean(update.message.text)
    moderator_username = update.message.from_user.username or update.message.from_user.first_name
    
    # Проверяем, является ли это ответом на сообщение с запросом комментария
    if "комментарий для одобрения" in replied_message.text.lower():
        # Ищем соответствующую анкету в базе данных
        for user_id, releases in db.items():
            for idx, release in enumerate(releases):
                if release.get('status') == 'pending':
                    # Проверяем, соответствует ли это анкета текущему процессу модерации
                    if context.user_data.get('moderation_user_id') == user_id and context.user_data.get('moderation_idx') == idx:
                        # Одобряем релиз с комментарием
                        release['status'] = 'approved'
                        release['moderator'] = moderator_username
                        release['moderator_comment'] = comment_text
                        release['moderation_time'] = datetime.now().isoformat()
                        save_db(db)
                        
                        # Обновляем сообщение в группе модерации
                        await update_moderation_message(context, user_id, idx, 'approved', moderator_username=moderator_username, moderator_comment=comment_text)
                        
                        # Уведомляем пользователя
                        try:
                            await context.bot.send_message(
                                int(user_id),
                                f"{WINTER_EMOJIS['check']} <b>ВАШ РЕЛИЗ ОДОБРЕН!</b> \n\n"
                                f"<b>{escape_html(release['name'])}</b>\n"
                                f"<i>Тип:</i> {escape_html(release['type'])}\n"
                                f"<i>Дата:</i> {escape_html(release['date'])}\n\n"
                                f"<b>Комментарий модератора:</b> {escape_html(comment_text)}\n\n"
                                f"Готов к публикации! {WINTER_EMOJIS['sparkles']}",
                                parse_mode=ParseMode.HTML
                            )
                        except Exception as e:
                            print(f"Ошибка отправки пользователю: {e}")
                        
                        await update.message.reply_text(
                            f"{WINTER_EMOJIS['check']} Релиз одобрен с комментарием!",
                            parse_mode=ParseMode.HTML
                        )
                        return

# === ОБРАБОТКА ОТВЕТОВ НА СООБЩЕНИЯ ===
async def handle_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        return
    
    if update.message.chat_id != MODERATION_CHAT_ID:
        return
        
    replied_message = update.message.reply_to_message
    reply_text = clean(update.message.text)
    
    # Проверяем, является ли это ответом на сообщение с запросом причины отклонения
    if "причину отклонения" in replied_message.text.lower():
        # Ищем соответствующую анкету в базе данных
        for user_id, releases in db.items():
            for idx, release in enumerate(releases):
                if release.get('status') == 'pending':
                    # Проверяем, соответствует ли это анкета текущему процессу модерации
                    if context.user_data.get('moderation_user_id') == user_id and context.user_data.get('moderation_idx') == idx:
                        # Отклоняем релиз с указанной причиной
                        release['status'] = 'rejected'
                        release['reject_reason'] = reply_text
                        release['moderation_time'] = datetime.now().isoformat()
                        save_db(db)
                        
                        # Обновляем сообщение в группе модерации
                        await update_moderation_message(context, user_id, idx, 'rejected', reply_text)
                        
                        # Уведомляем пользователя
                        try:
                            await context.bot.send_message(
                                int(user_id),
                                f"{WINTER_EMOJIS['cross']} <b>ВАШ РЕЛИЗ ОТКЛОНЁН!</b> \n\n"
                                f"<b>{escape_html(release['name'])}</b>\n"
                                f"<i>Тип:</i> {escape_html(release['type'])}\n"
                                f"<i>Дата:</i> {escape_html(release['date'])}\n\n"
                                f"<b>Причина:</b> {escape_html(reply_text)}\n\n"
                                f"Можете исправить и отправить заново! {WINTER_EMOJIS['sparkles']}",
                                parse_mode=ParseMode.HTML
                            )
                        except Exception as e:
                            print(f"Ошибка отправки пользователю: {e}")
                        
                        await update.message.reply_text(
                            f"{WINTER_EMOJIS['check']} Релиз отклонён с причиной!",
                            parse_mode=ParseMode.HTML
                        )
                        return

# === МОДЕРАЦИЯ ===
async def moderation_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.message.chat_id != MODERATION_CHAT_ID:
        return

    data = query.data.split('_')
    action, user_id, idx = data[0], data[1], int(data[2])
    release = db[user_id][idx]

    # Сохраняем ID сообщения для обновления
    context.user_data['moderation_message_id'] = query.message.message_id
    context.user_data['moderation_user_id'] = user_id
    context.user_data['moderation_idx'] = idx

    if action == 'approve':
        # Создаем клавиатуру с опцией комментария
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(winter_text("Одобрить без комментария", "check"), callback_data=f'approve_nocomment_{user_id}_{idx}')],
            [InlineKeyboardButton(winter_text("Одобрить с комментарием", "comment"), callback_data=f'approve_withcomment_{user_id}_{idx}')],
            [InlineKeyboardButton(winter_text("Отмена", "cross"), callback_data=f'cancel_{user_id}_{idx}')]
        ])
        
        await safe_edit(query, f"{WINTER_EMOJIS['comment']} <b>Выберите тип одобрения:</b>", keyboard)

    elif action == 'approve_nocomment':
        # Одобряем без комментария
        release['status'] = 'approved'
        release['moderator'] = query.from_user.username or query.from_user.first_name
        release['moderation_time'] = datetime.now().isoformat()
        save_db(db)
        
        # Обновляем сообщение в группе модерации
        await update_moderation_message(context, user_id, idx, 'approved', moderator_username=release['moderator'])
        
        # Уведомляем пользователя
        try:
            await context.bot.send_message(
                int(user_id),
                f"{WINTER_EMOJIS['check']} <b>ВАШ РЕЛИЗ ОДОБРЕН!</b> \n\n"
                f"<b>{escape_html(release['name'])}</b>\n"
                f"<i>Тип:</i> {escape_html(release['type'])}\n"
                f"<i>Дата:</i> {escape_html(release['date'])}\n\n"
                f"Готов к публикации! {WINTER_EMOJIS['sparkles']}",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            print(f"Ошибка отправки пользователю: {e}")

    elif action == 'approve_withcomment':
        # Запрашиваем комментарий
        await safe_edit(query, f"{WINTER_EMOJIS['comment']} <b>Введите комментарий для одобрения ОТВЕТОМ на это сообщение:</b>")

    elif action == 'reject':
        # Сохраняем данные для обработки ответа
        context.user_data['moderation_user_id'] = user_id
        context.user_data['moderation_idx'] = idx
        
        await safe_edit(query, f"{WINTER_EMOJIS['cross']} <b>Введите причину отклонения ОТВЕТОМ на это сообщение:</b>")

    elif action == 'cancel':
        # Возвращаемся к исходной клавиатуре
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(winter_text("Одобрить", "check"), callback_data=f'approve_{user_id}_{idx}')],
            [InlineKeyboardButton(winter_text("Отклонить", "cross"), callback_data=f'reject_{user_id}_{idx}')]
        ])
        await safe_edit(query, f"{WINTER_EMOJIS['snowflake']} <b>АНКЕТА ДЛЯ МОДЕРАЦИИ:</b>\n\nОт: @{escape_html(release.get('username', 'нет'))}\nID: <code>{user_id}</code>", keyboard)

# === ОБРАБОТКА ОШИБОК ===
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    print(f"Ошибка: {context.error}")

# === ЗАПУСК ===
def main():
    app = Application.builder().token(TOKEN).read_timeout(120).build()
    
    app.add_handler(CommandHandler('my', my_cmd))
    app.add_handler(CommandHandler('admin', admin_panel))
    app.add_handler(CommandHandler('backup', backup_cmd))
    app.add_handler(CommandHandler('moderation_backup', moderation_backup_cmd))
    app.add_handler(CommandHandler('stats', admin_stats_cmd))
    app.add_handler(CommandHandler('broadcast', broadcast_cmd))
    app.add_handler(CommandHandler('cleanbase', cleanbase_cmd))
    app.add_handler(CallbackQueryHandler(moderation_handler, pattern='^(approve|reject|approve_nocomment|approve_withcomment|cancel)_'))
    app.add_handler(MessageHandler(filters.TEXT & filters.REPLY & filters.ChatType.GROUPS, handle_reply))
    app.add_handler(MessageHandler(filters.TEXT & filters.REPLY & filters.ChatType.GROUPS, handle_approve_with_comment))
    app.add_error_handler(error_handler)

    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start_cmd)],
        states={
            REPORT: [CallbackQueryHandler(button)],
            TYPE: [CallbackQueryHandler(button)],
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, name)],
            SINGLE_NICK: [MessageHandler(filters.TEXT & ~filters.COMMAND, single_nick)],
            SINGLE_FIO: [MessageHandler(filters.TEXT & ~filters.COMMAND, single_fio)],
            ALBUM_NICK: [MessageHandler(filters.TEXT & ~filters.COMMAND, album_nick)],
            ALBUM_FIO: [MessageHandler(filters.TEXT & ~filters.COMMAND, album_fio)],
            DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, date)],
            VERSION: [MessageHandler(filters.TEXT & ~filters.COMMAND, version)],
            GENRE: [MessageHandler(filters.TEXT & ~filters.COMMAND, genre)],
            LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, link)],
            MAT: [CallbackQueryHandler(mat)],
            PROMO: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo)],
            COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, comment)],
            SINGLE_TG: [MessageHandler(filters.TEXT & ~filters.COMMAND, single_tg)],
            ALBUM_TRACKLIST: [MessageHandler(filters.TEXT & ~filters.COMMAND, album_tracklist)],
            ALBUM_TG: [MessageHandler(filters.TEXT & ~filters.COMMAND, album_tg)],
            CONFIRM: [CallbackQueryHandler(button)],
        },
        fallbacks=[CommandHandler('start', start_cmd)],
        per_message=False,
        per_chat=True
    )
    
    app.add_handler(conv)
    
    print(f"{WINTER_EMOJIS['snowflake']} БОТ ЗАПУЩЕН! {WINTER_EMOJIS['snowflake']}")
    app.run_polling()

if __name__ == '__main__':
    main()

# === HTTP СЕРВЕР ДЛЯ MINI APP ===
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

class WebAppHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse(self.path)
        
        # API endpoint для получения релизов
        if parsed_path.path == '/api/releases':
            self.handle_api_releases(parsed_path.query)
            return
        
        # API endpoint для получения конфига
        if parsed_path.path == '/api/config':
            self.handle_api_config()
            return
        
        # Отдаем HTML страницу
        if parsed_path.path == '/panel.html' or parsed_path.path == '/':
            self.serve_html()
            return
        
        # Health check
        if parsed_path.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
            return
        
        # 404 для остальных запросов
        self.send_response(404)
        self.end_headers()
        self.wfile.write(b'Not Found')
    
    def handle_api_releases(self, query_string):
        try:
            params = parse_qs(query_string)
            user_id = params.get('user_id', [None])[0]
            
            if not user_id:
                self.send_json_response({'error': 'user_id required'}, 400)
                return
            
            # Загружаем БД
            releases = load_db().get(user_id, [])
            
            # Вычисляем статистику
            stats = {
                'total': len(releases),
                'pending': sum(1 for r in releases if r.get('status', 'pending') == 'pending'),
                'approved': sum(1 for r in releases if r.get('status') == 'approved'),
                'rejected': sum(1 for r in releases if r.get('status') == 'rejected'),
                'published': sum(1 for r in releases if r.get('status') == 'published')
            }
            
            # Сортируем релизы по дате отправки (новые первые)
            sorted_releases = sorted(
                releases,
                key=lambda x: x.get('submission_time', ''),
                reverse=True
            )
            
            response = {
                'stats': stats,
                'releases': sorted_releases
            }
            
            self.send_json_response(response, 200)
            
        except Exception as e:
            print(f"Error in API: {e}")
            self.send_json_response({'error': str(e)}, 500)
    
    def handle_api_config(self):
        """Возвращает конфигурацию для Mini App"""
        config = {
            'bot_username': BOT_USERNAME
        }
        self.send_json_response(config, 200)
    
    def serve_html(self):
        try:
            html_path = os.path.join(os.path.dirname(__file__), 'panel.html')
            if not os.path.exists(html_path):
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b'HTML file not found')
                return
            
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
            self.end_headers()
            self.wfile.write(html_content.encode('utf-8'))
            
        except Exception as e:
            print(f"Error serving HTML: {e}")
            self.send_response(500)
            self.end_headers()
            self.wfile.write(f'Error: {str(e)}'.encode('utf-8'))
    
    def send_json_response(self, data, status_code=200):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
    
    def log_message(self, format, *args):
        # Отключаем логирование каждого запроса
        pass

def run_webapp_server():
    # Используем порт из переменной окружения или 10000 по умолчанию
    PORT = int(os.getenv("PORT", "10000"))
    server = HTTPServer(('0.0.0.0', PORT), WebAppHandler)
    print(f"🌐 WebApp сервер запущен на порту {PORT}")
    server.serve_forever()

# Запускаем HTTP сервер в отдельном потоке
threading.Thread(target=run_webapp_server, daemon=True).start()
