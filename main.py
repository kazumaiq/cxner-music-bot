import json
import os
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
ADMIN_ID = 881379104
ARTISTS_CHAT = "https://t.me/+oVmX3_dkyWJhNjJi"
CHANNEL = "https://t.me/cxrnermusic"
DB_FILE = "releases.json"
MODERATION_DB_FILE = "moderation_releases.json"

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
 REJECT_REASON) = range(22)

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

# === ЭКРАНИРОВАНИЕ ===
def escape_md(text):
    if not text:
        return ""
    return (str(text)
            .replace('\\', '\\\\')
            .replace('_', '\\_')
            .replace('*', '\\*')
            .replace('[', '\\[')
            .replace(']', '\\]')
            .replace('(', '\\(')
            .replace(')', '\\)')
            .replace('~', '\\~')
            .replace('`', '\\`')
            .replace('>', '\\>')
            .replace('#', '\\#')
            .replace('+', '\\+')
            .replace('-', '\\-')
            .replace('=', '\\=')
            .replace('|', '\\|')
            .replace('{', '\\{')
            .replace('}', '\\}')
            .replace('.', '\\.')
            .replace('!', '\\!'))

def clean(text):
    return ' '.join([w for w in text.split() if not w.lower().startswith(('1.', '2.', '3.'))]).strip()

# === БЕЗОПАСНАЯ ОТПРАВКА ===
async def safe_send(target, text, reply_markup=None, parse_mode=ParseMode.MARKDOWN_V2):
    message = target if hasattr(target, 'reply_text') else target.message
    for _ in range(3):
        try:
            await message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
            return
        except TimedOut:
            await asyncio.sleep(2)
        except BadRequest as e:
            if "can't parse entities" in str(e).lower():
                await message.reply_text(text.replace('*', '').replace('_', '').replace('`', '').replace('\\', ''), reply_markup=reply_markup)
            else:
                raise
        except Exception:
            await message.reply_text(text.replace('*', '').replace('_', '').replace('`', ''), reply_markup=reply_markup)
            return
    await message.reply_text("Не удалось отправить\\.")

async def safe_edit(query, text, reply_markup=None, parse_mode=ParseMode.MARKDOWN_V2):
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
    except (BadRequest, TimedOut, Forbidden):
        await query.message.reply_text(text.replace('*', '').replace('_', '').replace('`', ''), reply_markup=reply_markup)

# === ЗИМНЕЕ ОФОРМЛЕНИЕ ===
def winter_text(text, emoji_key=None):
    if emoji_key and emoji_key in WINTER_EMOJIS:
        return f"{WINTER_EMOJIS[emoji_key]} {text}"
    return text

def winter_header(text):
    return f"{WINTER_EMOJIS['snowflake']} {text} {WINTER_EMOJIS['snowflake']}"

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

{escape_md("Добро пожаловать в зимнюю студию музыки!")} {WINTER_EMOJIS['tree']}

{escape_md("Выберите действие:")}
"""
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN_V2
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
        f"{winter_header('Твоя статистика')}\n\n"
        f"{WINTER_EMOJIS['notes']} Всего релизов: *{total}*\n"
        f"{WINTER_EMOJIS['waiting']} Ожидает: *{pending}*\n"
        f"{WINTER_EMOJIS['check']} Одобрено: *{approved}*\n"
        f"{WINTER_EMOJIS['cross']} Отклонено: *{rejected}*\n"
        f"{WINTER_EMOJIS['published']} Опубликовано: *{published}*\n\n"
    )

    if not releases:
        await update.message.reply_text(
            f"{escape_md(stats)}_У вас пока нет релизов\\._\n\n/start {WINTER_EMOJIS['gift']} отправить первый\\!",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    text = f"{escape_md(stats)}*Твои релизы*:\n\n"
    status_emoji = {"pending": WINTER_EMOJIS['waiting'], "approved": WINTER_EMOJIS['check'], 
                   "rejected": WINTER_EMOJIS['cross'], "published": WINTER_EMOJIS['published']}
    
    for i, rel in enumerate(releases, 1):
        status = rel.get('status', 'pending')
        emoji = status_emoji.get(status, WINTER_EMOJIS['waiting'])
        status_text = {"pending": "Ожидает", "approved": "Одобрено", 
                      "rejected": "Отклонено", "published": "Опубликовано"}.get(status, "Ожидает")
        link = f"\n[Слушать]({rel.get('link_published', '')})" if status == 'published' and rel.get('link_published') else ""
        text += (
            f"*{i}\\. {escape_md(rel.get('name', 'Без названия'))}* {escape_md(emoji)}\n"
            f"_Тип:_ {escape_md(rel.get('type', '—'))}\n"
            f"_Ник:_ {escape_md(rel.get('nick', '—'))}\n"
            f"_Дата:_ {escape_md(rel.get('date', '—'))}\n"
            f"_Жанр:_ {escape_md(rel.get('genre', '—'))}\n"
            f"_Мат:_ {escape_md(rel.get('mat', '—'))}\n"
            f"_Статус:_ {escape_md(status_text)}"
        )
        if status == 'rejected' and rel.get('reject_reason'):
            text += f" \\({escape_md(rel['reject_reason'])}\\)"
        text += f"{link}\n\n"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Отправить новый", "music"), callback_data='report')],
        [InlineKeyboardButton(winter_text("Меню", "tree"), callback_data='main')]
    ])
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)

# === РАСШИРЕННАЯ АДМИН-ПАНЕЛЬ (/admin) ===
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        await update.message.reply_text("Доступ запрещён\\.")
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
        f"{WINTER_EMOJIS['stats']} *ОБЩАЯ СТАТИСТИКА:*\n"
        f"{WINTER_EMOJIS['users']} Пользователей: *{total_users}*\n"
        f"{WINTER_EMOJIS['notes']} Всего релизов: *{total_releases}*\n"
        f"{WINTER_EMOJIS['waiting']} Ожидает: *{pending}*\n"
        f"{WINTER_EMOJIS['check']} Одобрено: *{approved}*\n"
        f"{WINTER_EMOJIS['cross']} Отклонено: *{rejected}*\n"
        f"{WINTER_EMOJIS['published']} Опубликовано: *{published}*\n"
        f"{WINTER_EMOJIS['calendar']} За неделю: *{recent_releases}*\n\n"
        
        f"{WINTER_EMOJIS['settings']} *УПРАВЛЕНИЕ:*\n"
        f"/backup \\- 📦 База данных релизов\n"
        f"/moderation_backup \\- 🗂️ Архив модерации\n"
        f"/stats \\- 📊 Подробная статистика\n"
        f"/broadcast \\- 📢 Рассылка пользователям\n"
        f"/cleanup \\- 🧹 Очистка старых данных\n\n"
        
        f"{WINTER_EMOJIS['warning']} *БЫСТРЫЕ ДЕЙСТВИЯ:*"
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
        ]
    ])
    
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)

# === СТАТИСТИКА ДЛЯ АДМИНА ===
async def admin_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        await update.message.reply_text("Доступ запрещён\\.")
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
        f"{WINTER_EMOJIS['users']} *ПОЛЬЗОВАТЕЛИ:*\n"
        f"• Всего: *{total_users}*\n"
        f"• Активных: *{active_users}*\n\n"
        
        f"{WINTER_EMOJIS['notes']} *РЕЛИЗЫ:*\n"
        f"• Всего: *{total_releases}*\n"
        f"• Синглов: *{type_stats['сингл']}*\n"
        f"• Альбомов: *{type_stats['альбом']}*\n\n"
        
        f"{WINTER_EMOJIS['stats']} *СТАТУСЫ:*\n"
        f"• Ожидает: *{status_stats['pending']}*\n"
        f"• Одобрено: *{status_stats['approved']}*\n"
        f"• Отклонено: *{status_stats['rejected']}*\n"
        f"• Опубликовано: *{status_stats['published']}*\n\n"
        
        f"{WINTER_EMOJIS['calendar']} *ПОСЛЕДНИЕ ДЕЙСТВИЯ:*\n"
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
        text += f"{i}\\. {escape_md(release.get('name', 'Без названия'))} {status_emoji[status]}\n"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
    ])
    
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)

# === СПИСОК ОЖИДАЮЩИХ РЕЛИЗОВ ===
async def pending_releases_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query.from_user.id != ADMIN_ID:
        await update.callback_query.answer("Доступ запрещён", show_alert=True)
        return

    pending_list = []
    for user_id, releases in db.items():
        for idx, release in enumerate(releases):
            if release.get('status', 'pending') == 'pending':
                pending_list.append((user_id, idx, release))
    
    if not pending_list:
        text = f"{WINTER_EMOJIS['check']} *Нет ожидающих релизов\\!*"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
        ])
        await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)
        return
    
    text = f"{winter_header('ОЖИДАЮЩИЕ РЕЛИЗЫ')}\n\n"
    for i, (user_id, idx, release) in enumerate(pending_list[:10], 1):  # Ограничиваем 10 записями
        text += (
            f"*{i}\\. {escape_md(release.get('name', 'Без названия'))}*\n"
            f"Тип: {escape_md(release.get('type', '—'))}\n"
            f"Артист: {escape_md(release.get('nick', '—'))}\n"
            f"Дата: {escape_md(release.get('date', '—'))}\n"
            f"ID: `{user_id}`\n\n"
        )
    
    if len(pending_list) > 10:
        text += f"*... и ещё {len(pending_list) - 10} релизов*"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
    ])
    
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)

# === ОЧИСТКА БАЗЫ ДАННЫХ ===
async def cleanup_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query.from_user.id != ADMIN_ID:
        await update.callback_query.answer("Доступ запрещён", show_alert=True)
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
        f"{WINTER_EMOJIS['refresh']} *ОЧИСТКА ЗАВЕРШЕНА\\!*\n\n"
        f"Удалено пустых пользователей: *{users_removed}*\n"
        f"Текущее количество пользователей: *{users_after}*"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
    ])
    
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)

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
        await safe_edit(query, f"{WINTER_EMOJIS['snowflake']} *Выберите тип релиза:*", keyboard)
        return TYPE

    if data == 'my_releases':
        await my_cmd(query, context)
        return REPORT

    if data == 'single':
        user_data[user_id] = {'type': 'сингл', 'status': 'pending'}
        await safe_edit(query, f"{WINTER_EMOJIS['music']} *СИНГЛ*\\.\n\n1\\. Название релиза\nПример: Tokyo Rain")
        return NAME

    if data == 'album':
        user_data[user_id] = {'type': 'альбом', 'status': 'pending'}
        await safe_edit(query, f"{WINTER_EMOJIS['notes']} *АЛЬБОМ*\\.\n\n1\\. Название релиза\nПример: Lost in the Void")
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
        
    if data == 'cleanup_db':
        await cleanup_database(update, context)
        return
        
    if data == 'admin_back':
        await admin_panel(query, context)
        return
        
    if data == 'broadcast_menu':
        await broadcast_menu(update, context)
        return

# === МЕНЮ РАССЫЛКИ ===
async def broadcast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query.from_user.id != ADMIN_ID:
        await update.callback_query.answer("Доступ запрещён", show_alert=True)
        return

    text = (
        f"{winter_header('РАССЫЛКА')}\n\n"
        f"{WINTER_EMOJIS['warning']} *ВНИМАНИЕ:* Рассылка будет отправлена *ВСЕМ* пользователям бота\\!\n\n"
        f"Используйте команду:\n"
        f"`/broadcast ваш текст сообщения`\n\n"
        f"Или отправьте сообщение ответом на это сообщение для рассылки\\."
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Назад", "tree"), callback_data='admin_back')]
    ])
    
    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)

# === РАССЫЛКА ===
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        await update.message.reply_text("Доступ запрещён\\.")
        return

    if not context.args:
        await update.message.reply_text(
            f"{WINTER_EMOJIS['warning']} Использование: `/broadcast ваш текст`",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    message_text = ' '.join(context.args)
    broadcast_text = (
        f"{WINTER_EMOJIS['published']} *ВАЖНОЕ ОБЪЯВЛЕНИЕ* {WINTER_EMOJIS['published']}\n\n"
        f"{escape_md(message_text)}\n\n"
        f"_С уважением, команда CXRNER MUSIC_ {WINTER_EMOJIS['snowflake']}"
    )

    # Отправляем сообщение
    sent_count = 0
    error_count = 0
    
    progress_msg = await update.message.reply_text(
        f"{WINTER_EMOJIS['waiting']} *Начинаю рассылку\\.\\.\\.*"
    )

    for user_id in db.keys():
        try:
            await context.bot.send_message(
                int(user_id),
                broadcast_text,
                parse_mode=ParseMode.MARKDOWN_V2
            )
            sent_count += 1
            await asyncio.sleep(0.1)  # Задержка чтобы не превысить лимиты
        except Exception as e:
            error_count += 1
            print(f"Ошибка отправки пользователю {user_id}: {e}")

    await progress_msg.edit_text(
        f"{WINTER_EMOJIS['check']} *РАССЫЛКА ЗАВЕРШЕНА\\!*\n\n"
        f"• Успешно: *{sent_count}*\n"
        f"• Ошибок: *{error_count}*\n"
        f"• Всего: *{sent_count + error_count}*",
        parse_mode=ParseMode.MARKDOWN_V2
    )

# === ОТПРАВКА ФАЙЛОВ БАЗЫ ДАННЫХ ===
async def send_database_backup(query, context: ContextTypes.DEFAULT_TYPE):
    if query.from_user.id != ADMIN_ID:
        await query.answer("Доступ запрещён", show_alert=True)
        return
        
    try:
        with open(DB_FILE, 'rb') as f:
            await context.bot.send_document(
                chat_id=ADMIN_ID,
                document=f,
                filename=f"releases_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                caption=f"{WINTER_EMOJIS['snowflake']} Резервная копия базы данных релизов"
            )
        await query.answer("База данных отправлена!", show_alert=True)
    except Exception as e:
        await query.answer(f"Ошибка: {e}", show_alert=True)

async def send_moderation_backup(query, context: ContextTypes.DEFAULT_TYPE):
    if query.from_user.id != ADMIN_ID:
        await query.answer("Доступ запрещён", show_alert=True)
        return
        
    try:
        with open(MODERATION_DB_FILE, 'rb') as f:
            await context.bot.send_document(
                chat_id=ADMIN_ID,
                document=f,
                filename=f"moderation_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                caption=f"{WINTER_EMOJIS['snowman']} Архив модерации"
            )
        await query.answer("Архив модерации отправлен!", show_alert=True)
    except Exception as e:
        await query.answer(f"Ошибка: {e}", show_alert=True)

# === КОМАНДЫ АДМИНА ДЛЯ БЭКАПА ===
async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        await update.message.reply_text("Доступ запрещён\\.")
        return
    await send_database_backup(update.message, context)

async def moderation_backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        await update.message.reply_text("Доступ запрещён\\.")
        return
    await send_moderation_backup(update.message, context)

async def send_database_backup(message, context: ContextTypes.DEFAULT_TYPE):
    try:
        with open(DB_FILE, 'rb') as f:
            await context.bot.send_document(
                chat_id=ADMIN_ID,
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
                chat_id=ADMIN_ID,
                document=f,
                filename=f"moderation_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                caption=f"{WINTER_EMOJIS['snowman']} Архив модерации"
            )
        await message.reply_text(f"{WINTER_EMOJIS['check']} Архив модерации отправлен!")
    except Exception as e:
        await message.reply_text(f"{WINTER_EMOJIS['cross']} Ошибка: {e}")

# === ПОЛЯ ===
async def name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['name'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['star']} *2\\. Ник исполнителя\\(ей\\)*\nПример: MAKIZM")
    return SINGLE_NICK if user_data[user_id]['type'] == 'сингл' else ALBUM_NICK

async def single_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['nick'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['star']} *3\\. ФИО исполнителя\\(ей\\)*\nПример: Иванов Иван")
    return SINGLE_FIO

async def single_fio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['fio'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['calendar']} *4\\. Дата релиза*\nМинимум через 5 дней\nФормат: ДД\\.ММ\\.ГГГГ")
    return DATE

async def album_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['nick'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['star']} *2\\. ФИО исполнителя\\(ей\\) \\(поочерёдно\\)*\nПример: Иванов Иван, Петров Пётр")
    return ALBUM_FIO

async def album_fio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['fio'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['calendar']} *3\\. Дата релиза*\nМинимум через 7 дней\nФормат: ДД\\.ММ\\.ГГГГ")
    return DATE

async def date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    text = update.message.text.strip()
    if not all(part.isdigit() for part in text.split('.') if part):
        await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Неверный формат даты\\! Используйте: ДД\\.ММ\\.ГГГГ")
        return DATE
    try:
        date_obj = datetime.strptime(text, "%d.%m.%Y")
        min_days = 5 if user_data[user_id]['type'] == 'сингл' else 7
        if date_obj < datetime.now() + timedelta(days=min_days):
            await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Дата должна быть минимум через {min_days} дней\\!")
            return DATE
        user_data[user_id]['date'] = text
        await safe_send(update.message, f"{WINTER_EMOJIS['music']} *Версия релиза*\nSlowed, Speed Up\\.\nЕсли нет — напиши: —")
        return VERSION
    except ValueError:
        await safe_send(update.message, f"{WINTER_EMOJIS['cross']} Неверный формат даты\\! Пример: 25\\.12\\.2025")
        return DATE

async def version(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    ver = clean(update.message.text)
    user_data[user_id]['version'] = ver if ver != '—' else 'Оригинал'
    await safe_send(update.message, f"{WINTER_EMOJIS['notes']} *Жанр релиза*\nПример: Phonk, Trap")
    return GENRE

async def genre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['genre'] = clean(update.message.text)
    await safe_send(update.message,
        f"{WINTER_EMOJIS['gift']} *Ссылка на файлы \\(Yandex/Google Диск\\)*\n\n"
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
    await safe_send(update.message, f"{WINTER_EMOJIS['warning']} *Есть ли ненормативная лексика?*", keyboard)
    return MAT

async def mat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    user_data[user_id]['mat'] = 'Да' if query.data == 'mat_yes' else 'Нет'
    await safe_edit(query, f"{WINTER_EMOJIS['sparkles']} *Промо текст \\(необязательно\\)*")
    return PROMO

async def promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['promo'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['comment']} *Комментарий для модератора \\(необязательно\\)*")
    return COMMENT

async def comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['comment'] = clean(update.message.text)
    if user_data[user_id]['type'] == 'сингл':
        await safe_send(update.message, f"{WINTER_EMOJIS['telegram']} *Ваш Telegram для связи*\n@username")
        return SINGLE_TG
    else:
        await safe_send(update.message, f"{WINTER_EMOJIS['list']} *Трек\\-лист альбома*\n1\\. Track 1")
        return ALBUM_TRACKLIST

async def album_tracklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['tracklist'] = clean(update.message.text)
    await safe_send(update.message, f"{WINTER_EMOJIS['telegram']} *Ваш Telegram для связи*\n@username")
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
    text = f"{WINTER_EMOJIS['snowflake']} *ПРОВЕРЬТЕ АНКЕТУ:*\\.\n\n"
    for k, v in data.items():
        if k not in ['type', 'status']:
            text += f"• *{k.capitalize()}:* {escape_md(v)}\n"
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

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(winter_text("Одобрить", "check"), callback_data=f'approve_{user_id}_{idx}')],
        [InlineKeyboardButton(winter_text("Отклонить", "cross"), callback_data=f'reject_{user_id}_{idx}')],
        [InlineKeyboardButton(winter_text("Опубликовать", "published"), callback_data=f'publish_{user_id}_{idx}')]
    ])

    msg = (
        f"{WINTER_EMOJIS['snowflake']} *НОВАЯ АНКЕТА\\!* \\.\n"
        f"От: @{escape_md(user.username) if user.username else 'нет'}\n"
        f"ID: `{user_id}`\n"
        f"Тип: {escape_md(data['type'])}\n\n"
    )
    for k, v in data.items():
        if k not in ['type', 'status', 'submission_time']:
            msg += f"• *{k.capitalize()}:* {escape_md(v)}\n"
    
    try:
        moderation_msg = await context.bot.send_message(MODERATION_CHAT_ID, msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)
        
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
    
    await safe_edit(query, f"{WINTER_EMOJIS['check']} *Анкета отправлена\\!* \\.\nОжидайте 12–72 часа\\.", parse_mode=ParseMode.MARKDOWN_V2)

# === ОБНОВЛЕНИЕ СООБЩЕНИЯ В МОДЕРАЦИИ ===
async def update_moderation_message(context, user_id, idx, status, reason=None):
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
    moderation_time_escaped = escape_md(moderation_time)
    
    msg = (
        f"{status_emoji[status]} *АНКЕТА {status_text[status]}\\!* \n\n"
        f"*Исходная информация:*\n"
        f"От: @{escape_md(release.get('username', 'нет'))}\n"
        f"ID: `{user_id}`\n"
        f"Тип: {escape_md(release['type'])}\n\n"
    )
    
    # Добавляем все поля анкеты
    for k, v in release.items():
        if k not in ['type', 'status', 'submission_time', 'username', 'moderation_time', 'publish_time', 'reject_reason', 'link_published']:
            msg += f"• *{k.capitalize()}:* {escape_md(v)}\n"
    
    # Добавляем информацию о статусе
    if status == 'rejected' and reason:
        msg += f"\n*Причина отклонения:* {escape_md(reason)}"
    elif status == 'published' and release.get('link_published'):
        msg += f"\n*Ссылка на релиз:* {escape_md(release['link_published'])}"
    
    msg += f"\n\n*Время модерации:* {moderation_time_escaped}"
    
    # Обновляем сообщение в группе модерации (убираем кнопки)
    try:
        await context.bot.edit_message_text(
            chat_id=MODERATION_CHAT_ID,
            message_id=context.user_data.get('moderation_message_id'),
            text=msg,
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=True
        )
    except Exception as e:
        print(f"Ошибка при обновлении сообщения: {e}")

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
                                f"{WINTER_EMOJIS['cross']} *ВАШ РЕЛИЗ ОТКЛОНЁН\\!* \\.\n\n"
                                f"*{escape_md(release['name'])}*\n"
                                f"_Тип:_ {escape_md(release['type'])}\n"
                                f"_Дата:_ {escape_md(release['date'])}\n\n"
                                f"*Причина:* {escape_md(reply_text)}\n\n"
                                f"Можете исправить и отправить заново\\! {WINTER_EMOJIS['sparkles']}",
                                parse_mode=ParseMode.MARKDOWN_V2
                            )
                        except Exception as e:
                            print(f"Ошибка отправки пользователю: {e}")
                        
                        await update.message.reply_text(
                            f"{WINTER_EMOJIS['check']} Релиз отклонён с причиной\\!",
                            parse_mode=ParseMode.MARKDOWN_V2
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
        release['status'] = 'approved'
        release['moderation_time'] = datetime.now().isoformat()
        save_db(db)
        
        # Обновляем сообщение в группе модерации
        await update_moderation_message(context, user_id, idx, 'approved')
        
        try:
            await context.bot.send_message(
                int(user_id),
                f"{WINTER_EMOJIS['check']} *ВАШ РЕЛИЗ ОДОБРЕН\\!* \\.\n\n"
                f"*{escape_md(release['name'])}*\n"
                f"_Тип:_ {escape_md(release['type'])}\n"
                f"_Дата:_ {escape_md(release['date'])}\n\n"
                f"Готов к публикации\\! {WINTER_EMOJIS['sparkles']}",
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except Exception as e:
            print(f"Ошибка отправки пользователю: {e}")

    elif action == 'reject':
        # Сохраняем данные для обработки ответа
        context.user_data['moderation_user_id'] = user_id
        context.user_data['moderation_idx'] = idx
        
        await safe_edit(query, f"{WINTER_EMOJIS['cross']} *Введите причину отклонения ОТВЕТОМ на это сообщение:*")

    elif action == 'publish':
        release['status'] = 'published'
        release['link_published'] = "https://t.me/cxrnermusic/123"
        release['publish_time'] = datetime.now().isoformat()
        save_db(db)
        
        # Обновляем сообщение в группе модерации
        await update_moderation_message(context, user_id, idx, 'published')
        
        post = f"*{escape_md(release['name'])}* \\- {escape_md(release['nick'])}\n[Слушать]({release['link_published']}) {WINTER_EMOJIS['music']}"
        try:
            await context.bot.send_message(CHANNEL, post, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as e:
            print(f"Ошибка публикации в канале: {e}")
        
        try:
            await context.bot.send_message(
                int(user_id),
                f"{WINTER_EMOJIS['published']} *ВАШ РЕЛИЗ ОПУБЛИКОВАН\\!* \\.\n\n"
                f"*{escape_md(release['name'])}*\n"
                f"_Тип:_ {escape_md(release['type'])}\n"
                f"_Дата:_ {escape_md(release['date'])}\n\n"
                f"[Слушать]({release['link_published']}) {WINTER_EMOJIS['headphones']}",
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except Exception as e:
            print(f"Ошибка отправки пользователю: {e}")

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
    app.add_handler(CallbackQueryHandler(moderation_handler, pattern='^(approve|reject|publish)_'))
    app.add_handler(MessageHandler(filters.TEXT & filters.REPLY & filters.ChatType.GROUPS, handle_reply))
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

# В конец main.py
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_server():
    HTTPServer(('0.0.0.0', 10000), Handler).serve_forever()

threading.Thread(target=run_server, daemon=True).start()
