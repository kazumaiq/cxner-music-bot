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
TOKEN = "7932680631:AAG3DW6gwg0Ccvuiq45aPVCSSWsOallp_Pk"
MODERATION_CHAT_ID = -1002117586464
ADMIN_ID = 881379104
ARTISTS_CHAT = "https://t.me/+oVmX3_dkyWJhNjJi"
CHANNEL = "https://t.me/cxrnermusic"
DB_FILE = "releases.json"

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

user_data = {}
db = load_db()

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

# === ГЛАВНОЕ МЕНЮ (/start) ===
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Отправить релиз", callback_data='report')],
        [InlineKeyboardButton("Мои релизы", callback_data='my_releases')],
        [InlineKeyboardButton("Канал", url=CHANNEL)],
        [InlineKeyboardButton("Чат артистов", url=ARTISTS_CHAT)]
    ])
    await update.message.reply_text(
        "*CXRNER MUSIC* \\.\n\n"
        "Выберите действие:",
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
        f"*Твоя статистика* \\.\n\n"
        f"• Всего релизов: *{total}*\n"
        f"• Ожидает: *{pending}*\n"
        f"• Одобрено: *{approved}*\n"
        f"• Отклонено: *{rejected}*\n"
        f"• Опубликовано: *{published}*\n\n"
    )

    if not releases:
        await update.message.reply_text(
            f"{escape_md(stats)}_У вас пока нет релизов\\._\n\n/start — отправить первый\\!",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    text = f"{escape_md(stats)}*Твои релизы*:\n\n"
    status_emoji = {"pending": "⏳", "approved": "✅", "rejected": "❌", "published": "🎵"}
    for i, rel in enumerate(releases, 1):
        status = rel.get('status', 'pending')
        emoji = status_emoji.get(status, "⏳")
        status_text = {"pending": "Ожидает", "approved": "Одобрено", "rejected": "Отклонено", "published": "Опубликовано"}.get(status, "Ожидает")
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
        [InlineKeyboardButton("Отправить новый", callback_data='report')],
        [InlineKeyboardButton("Меню", callback_data='main')]
    ])
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True)

# === АДМИН-ПАНЕЛЬ (/admin) ===
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        await update.message.reply_text("Доступ запрещён\\.")
        return

    total_users = len(db)
    total_releases = sum(len(v) for v in db.values())
    pending = sum(1 for u in db.values() for r in u if r.get('status', 'pending') == 'pending')
    approved = sum(1 for u in db.values() for r in u if r.get('status') == 'approved')
    rejected = sum(1 for u in db.values() for r in u if r.get('status') == 'rejected')
    published = sum(1 for u in db.values() for r in u if r.get('status') == 'published')

    text = (
        "*АДМИН\\-ПАНЕЛЬ* \\.\n\n"
        f"• Пользователей: *{total_users}*\n"
        f"• Всего релизов: *{total_releases}*\n"
        f"• Ожидает: *{pending}*\n"
        f"• Одобрено: *{approved}*\n"
        f"• Отклонено: *{rejected}*\n"
        f"• Опубликовано: *{published}*\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)

# === КНОПКИ ===
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = str(query.from_user.id)

    if data == 'report':
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Сингл", callback_data='single')],
            [InlineKeyboardButton("Альбом", callback_data='album')]
        ])
        await safe_edit(query, "*Выберите тип релиза:*", keyboard)
        return TYPE

    if data == 'my_releases':
        await my_cmd(query, context)
        return REPORT

    if data == 'single':
        user_data[user_id] = {'type': 'сингл', 'status': 'pending'}
        await safe_edit(query, "*СИНГЛ*\\.\n\n1\\. Название релиза\nПример: Tokyo Rain")
        return NAME

    if data == 'album':
        user_data[user_id] = {'type': 'альбом', 'status': 'pending'}
        await safe_edit(query, "*АЛЬБОМ*\\.\n\n1\\. Название релиза\nПример: Lost in the Void")
        return NAME

    if data == 'send':
        await send_moderation(query, context)
        return REPORT

    if data == 'main':
        return await start_cmd(query, context)

# === ПОЛЯ ===
async def name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['name'] = clean(update.message.text)
    await safe_send(update.message, "*2\\. Ник исполнителя\\(ей\\)*\nПример: MAKIZM")
    return SINGLE_NICK if user_data[user_id]['type'] == 'сингл' else ALBUM_NICK

async def single_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['nick'] = clean(update.message.text)
    await safe_send(update.message, "*3\\. ФИО исполнителя\\(ей\\)*\nПример: Иванов Иван")
    return SINGLE_FIO

async def single_fio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['fio'] = clean(update.message.text)
    await safe_send(update.message, "*4\\. Дата релиза*\nМинимум через 5 дни\nФормат: ДД.ММ.ГГГГ")
    return DATE

async def album_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['nick'] = clean(update.message.text)
    await safe_send(update.message, "*2\\. ФИО исполнителя\\(ей\\) \\(поочерёдно\\)*\nПример: Иванов Иван, Петров Пётр")
    return ALBUM_FIO

async def album_fio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['fio'] = clean(update.message.text)
    await safe_send(update.message, "*3\\. Дата релиза*\nМинимум через 7 дни\nФормат: ДД.ММ.ГГГГ")
    return DATE

async def date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    text = update.message.text.strip()
    if not all(part.isdigit() for part in text.split('.') if part):
        await safe_send(update.message, "Неверный формат даты\\! Используйте: ДД.ММ.ГГГГ")
        return DATE
    try:
        date_obj = datetime.strptime(text, "%d.%m.%Y")
        min_days = 5 if user_data[user_id]['type'] == 'сингл' else 7
        if date_obj < datetime.now() + timedelta(days=min_days):
            await safe_send(update.message, f"Дата должна быть минимум через {min_days} дни\\!")
            return DATE
        user_data[user_id]['date'] = text
        await safe_send(update.message, "*Версия релиза*\nSlowed, Speed Up\\.\nЕсли нет — напиши: —")
        return VERSION
    except ValueError:
        await safe_send(update.message, "Неверный формат даты\\! Пример: 25\\.12\\.2025")
        return DATE

async def version(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    ver = clean(update.message.text)
    user_data[user_id]['version'] = ver if ver != '—' else 'Оригинал'
    await safe_send(update.message, "*Жанр релиза*\nПример: Phonk, Trap")
    return GENRE

async def genre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['genre'] = clean(update.message.text)
    await safe_send(update.message,
        "*Ссылка на файлы \\(Yandex/Google Диск\\)*\n\n"
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
        [InlineKeyboardButton("Да", callback_data='mat_yes')],
        [InlineKeyboardButton("Нет", callback_data='mat_no')]
    ])
    await safe_send(update.message, "*Есть ли ненормативная лексика?*", keyboard)
    return MAT

async def mat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    user_data[user_id]['mat'] = 'Да' if query.data == 'mat_yes' else 'Нет'
    await safe_edit(query, "*Промо текст \\(необязательно\\)*")
    return PROMO

async def promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['promo'] = clean(update.message.text)
    await safe_send(update.message, "*Комментарий для модератора \\(необязательно\\)*")
    return COMMENT

async def comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['comment'] = clean(update.message.text)
    if user_data[user_id]['type'] == 'сингл':
        await safe_send(update.message, "*Ваш Telegram для связи*\n@username")
        return SINGLE_TG
    else:
        await safe_send(update.message, "*Трек\\-лист альбома*\n1\\. Track 1")
        return ALBUM_TRACKLIST

async def album_tracklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['tracklist'] = clean(update.message.text)
    await safe_send(update.message, "*Ваш Telegram для связи*\n@username")
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
    text = "*ПРОВЕРЬТЕ АНКЕТУ:*\\.\n\n"
    for k, v in data.items():
        if k not in ['type', 'status']:
            text += f"• *{k.capitalize()}:* {escape_md(v)}\n"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Отправить", callback_data='send')],
        [InlineKeyboardButton("Назад", callback_data='main')]
    ])
    await safe_send(message, text, keyboard)

# === ОТПРАВКА В МОДЕРАЦИЮ ===
async def send_moderation(query, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(query.from_user.id)
    data = user_data[user_id]
    data['status'] = 'pending'
    user = query.from_user

    idx = len(db.get(user_id, []))
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Одобрить", callback_data=f'approve_{user_id}_{idx}')],
        [InlineKeyboardButton("Отклонить", callback_data=f'reject_{user_id}_{idx}')],
        [InlineKeyboardButton("Опубликовать", callback_data=f'publish_{user_id}_{idx}')]
    ])

    msg = (
        f"*НОВАЯ АНКЕТА\\!* \\.\n"
        f"От: @{escape_md(user.username) if user.username else 'нет'}\n"
        f"ID: `{user_id}`\n"
        f"Тип: {escape_md(data['type'])}\n\n"
    )
    for k, v in data.items():
        if k not in ['type', 'status']:
            msg += f"• *{k.capitalize()}:* {escape_md(v)}\n"
    
    try:
        await context.bot.send_message(MODERATION_CHAT_ID, msg, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=keyboard)
    except Exception as e:
        await safe_edit(query, f"Ошибка: {e}")
        return REPORT

    if user_id not in db:
        db[user_id] = []
    db[user_id].append(data.copy())
    save_db(db)
    
    await safe_edit(query, "*Анкета отправлена\\!* \\.\nОжидайте 12–72 часа\\.", parse_mode=ParseMode.MARKDOWN_V2)

# === МОДЕРАЦИЯ ===
async def moderation_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.message.chat_id != MODERATION_CHAT_ID:
        return

    data = query.data.split('_')
    action, user_id, idx = data[0], data[1], int(data[2])
    release = db[user_id][idx]

    if action == 'approve':
        release['status'] = 'approved'
        save_db(db)
        await safe_edit(query, "Релиз *одобрен*\\!", parse_mode=ParseMode.MARKDOWN_V2)
        await context.bot.send_message(
            int(user_id),
            f"*ВАШ РЕЛИЗ ОДОБРЕН\\!* \\.\n\n"
            f"*{escape_md(release['name'])}*\n"
            f"_Тип:_ {escape_md(release['type'])}\n"
            f"_Дата:_ {escape_md(release['date'])}\n\n"
            f"Готов к публикации\\!",
            parse_mode=ParseMode.MARKDOWN_V2
        )

    elif action == 'reject':
        context.bot_data['reject'] = (user_id, idx)
        await safe_edit(query, "Введите причину отклонения:")
        return REJECT_REASON

    elif action == 'publish':
        release['status'] = 'published'
        release['link_published'] = "https://t.me/cxrnermusic/123"
        save_db(db)
        post = f"*{escape_md(release['name'])}* \\- {escape_md(release['nick'])}\n[Слушать]({release['link_published']})"
        await context.bot.send_message(CHANNEL, post, parse_mode=ParseMode.MARKDOWN_V2)
        await safe_edit(query, "Релиз *опубликован*\\!", parse_mode=ParseMode.MARKDOWN_V2)
        await context.bot.send_message(
            int(user_id),
            f"*ВАШ РЕЛИЗ ОПУБЛИКОВАН\\!* \\.\n\n"
            f"*{escape_md(release['name'])}*\n"
            f"_Тип:_ {escape_md(release['type'])}\n"
            f"_Дата:_ {escape_md(release['date'])}\n\n"
            f"[Слушать]({release['link_published']})",
            parse_mode=ParseMode.MARKDOWN_V2
        )

async def reject_release(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'reject' not in context.bot_data:
        await update.message.reply_text("Ошибка: нет данных\\.")
        return ConversationHandler.END
    user_id, idx = context.bot_data['reject']
    reason = clean(update.message.text)
    release = db[user_id][idx]
    release['status'] = 'rejected'
    release['reject_reason'] = reason
    save_db(db)
    await update.message.reply_text(f"Релиз *отклонён*\\! Причина: {escape_md(reason)}", parse_mode=ParseMode.MARKDOWN_V2)
    await context.bot.send_message(
        int(user_id),
        f"*ВАШ РЕЛИЗ ОТКЛОНЁН\\!* \\.\n\n"
        f"*{escape_md(release['name'])}*\n"
        f"_Тип:_ {escape_md(release['type'])}\n"
        f"_Дата:_ {escape_md(release['date'])}\n\n"
        f"*Причина:* {escape_md(reason)}",
        parse_mode=ParseMode.MARKDOWN_V2
    )
    context.bot_data.pop('reject', None)
    return ConversationHandler.END

# === ОБРАБОТКА ОШИБОК ===
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    print(f"Ошибка: {context.error}")

# === ЗАПУСК ===
def main():
    app = Application.builder().token(TOKEN).read_timeout(120).build()
    
    app.add_handler(CommandHandler('my', my_cmd))
    app.add_handler(CommandHandler('admin', admin_panel))
    app.add_handler(CallbackQueryHandler(moderation_handler, pattern='^(approve|reject|publish)_'))
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
            REJECT_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND, reject_release)]
        },
        fallbacks=[CommandHandler('start', start_cmd)],
        per_message=False,
        per_chat=True
    )
    
    app.add_handler(conv)
    
    print("БОТ ЖИВ! ")
    app.run_polling()

if __name__ == '__main__':
    main()
