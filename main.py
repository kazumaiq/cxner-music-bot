import json
import os
import uuid
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, ConversationHandler
from telegram.error import BadRequest

# === КОНФИГ ===
TOKEN = "7657262123:AAHCcGPJcG6dfz4zazWbCnKhzpYdYyuvgt8"
MODERATION_CHAT_ID = -3279159129
ARTISTS_CHAT = "https://t.me/+oVmX3_dkyWJhNjJi"
CHANNEL = "https://t.me/cxrnermusic"
DB_FILE = "releases.json"

# Состояния
(REPORT, TYPE, NAME, NICK, FIO, DATE, VERSION, GENRE, LINK, MAT, PROMO, COMMENT, TG, CONFIRM,
 ALBUM_NICK, ALBUM_FIO, ALBUM_TRACKLIST, ALBUM_TG) = range(26)

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

async def safe_edit(query, text, reply_markup=None):
    unique_id = str(uuid.uuid4())[:8]
    unique_text = f"{text}\\n\\n`{unique_id}`"
    try:
        await query.edit_message_text(unique_text, reply_markup=reply_markup, parse_mode='MarkdownV2')
    except BadRequest as e:
        if "not modified" in str(e):
            pass
        else:
            await query.message.reply_text(unique_text, reply_markup=reply_markup, parse_mode='MarkdownV2')

def back_btn(to='main'):
    return InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data=to)]])

def clean(text):
    return ' '.join([w for w in text.split() if not w.lower().startswith(('1.', '2.', '3.', '4.', '5.'))]).strip()

# === СТАРТ ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🎧 Отправить релиз", callback_data='report')],
        [InlineKeyboardButton("Канал", url=CHANNEL)],
        [InlineKeyboardButton("Чат артистов", url=ARTISTS_CHAT)],
        [InlineKeyboardButton("Кабинет", callback_data='cabinet')]
    ]
    await update.message.reply_text(
        "**🎧 CXRNER MUSIC — Анкета на отправку релиза**\\n\\n"
        "Эта анкета для артистов, желающих отправить релиз на отгрузку\\.\\n"
        "⚠️ Убедитесь, что трек готов и оформлен\\.\\n"
        "После отправки — модерация \\(12–72 часа\\)\\.\\n\\n"
        "**Что вы отправляете?**",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='MarkdownV2'
    )
    return REPORT

# === КНОПКИ ===
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = str(query.from_user.id)

    if data == 'report':
        keyboard = [
            [InlineKeyboardButton("1. Сингл", callback_data='single')],
            [InlineKeyboardButton("2. Альбом", callback_data='album')]
        ]
        await safe_edit(query, "**Выберите тип релиза:**", InlineKeyboardMarkup(keyboard))
        return TYPE

    if data == 'single':
        user_data[user_id] = {'type': 'сингл'}
        await safe_edit(query, "**Сингл**\\n\\n**Название релиза**\\n_Пример: Tokyo Rain_", parse_mode='MarkdownV2')
        return NAME

    if data == 'album':
        user_data[user_id] = {'type': 'альбом'}
        await safe_edit(query, "**Альбом**\\n\\n**Название релиза**\\n_Пример: Lost in the Void_", parse_mode='MarkdownV2')
        return NAME

    if data == 'confirm':
        await send_moderation(query, context)
        return REPORT

    if data == 'main':
        return await start(update, context)

# === ПОЛЯ (ОБЩИЕ) ===
async def name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['name'] = clean(update.message.text)
    await update.message.reply_text("**Ник исполнителя(лей)**\\n_Пример: MAKIZM, SHOSS_", parse_mode='MarkdownV2')
    return NICK if user_data[user_id]['type'] == 'сингл' else ALBUM_NICK

# СИНГЛ
async def nick_s(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['nick'] = clean(update.message.text)
    await update.message.reply_text("**ФИО исполнителя(лей)**\\n_Пример: Иванов Иван_", parse_mode='MarkdownV2')
    return FIO

async def fio_s(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['fio'] = clean(update.message.text)
    await update.message.reply_text("**Дата релиза**\\n_Минимум через 5 дней_\\n_Формат: ДД\\.ММ\\.ГГГГ_", parse_mode='MarkdownV2')
    return DATE

# АЛЬБОМ
async def album_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['nick'] = clean(update.message.text)
    await update.message.reply_text("**ФИО исполнителя(лей) \\(поочерёдно\\)**\\n_Пример: Иванов Иван, Петров Пётр_", parse_mode='MarkdownV2')
    return ALBUM_FIO

async def album_fio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['fio'] = clean(update.message.text)
    await update.message.reply_text("**Дата релиза**\\n_Минимум через 7 дней_\\n_Формат: ДД\\.ММ\\.ГГГГ_", parse_mode='MarkdownV2')
    return DATE

# ОБЩЕЕ
async def date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    text = update.message.text.strip()
    try:
        date_obj = datetime.strptime(text, "%d.%m.%Y")
        min_days = 5 if user_data[user_id]['type'] == 'сингл' else 7
        if date_obj < datetime.now() + timedelta(days=min_days):
            await update.message.reply_text(f"❌ Дата должна быть минимум через {min_days} дней!", parse_mode='MarkdownV2')
            return DATE
        user_data[user_id]['date'] = text
        await update.message.reply_text("**Укажи версию релиза**\\n_Slowed, Speed Up, Prod\\.\\.\\._", parse_mode='MarkdownV2')
        return VERSION
    except:
        await update.message.reply_text("❌ Неверный формат! Пример: 25\\.12\\.2025", parse_mode='MarkdownV2')
        return DATE

async def version(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['version'] = clean(update.message.text)
    await update.message.reply_text("**Укажи жанр релиза**\\n_Пример: Phonk, Trap_", parse_mode='MarkdownV2')
    return GENRE

async def genre(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['genre'] = clean(update.message.text)
    await update.message.reply_text(
        "**Ссылка на файлы \\(Yandex, Google Диск\\)**\\n\\n"
        "В архиве должно быть:\\n"
        "1\\. WAV 16/24 бит, 44100 Гц\\n"
        "2\\. Обложка 3000x3000 JPG\\n"
        "3\\. Скриншот проекта",
        parse_mode='MarkdownV2'
    )
    return LINK

async def link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['link'] = update.message.text.strip()
    keyboard = [
        [InlineKeyboardButton("Да", callback_data='mat_yes')],
        [InlineKeyboardButton("Нет", callback_data='mat_no')]
    ]
    await update.message.reply_text("**Есть ли ненормативная лексика?**", reply_markup=InlineKeyboardMarkup(keyboard))
    return MAT

async def mat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    user_data[user_id]['mat'] = 'Да' if query.data == 'mat_yes' else 'Нет'
    await safe_edit(query, "**Промо текст релиза** \\(необязательно\\)\\n_Подробно, как в инструкции_", parse_mode='MarkdownV2')
    return PROMO

async def promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['promo'] = clean(update.message.text)
    await update.message.reply_text("**Комментарий для модератора** \\(необязательно\\)", parse_mode='MarkdownV2')
    return COMMENT

async def comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['comment'] = clean(update.message.text)
    if user_data[user_id]['type'] == 'сингл':
        await update.message.reply_text("**Укажите ваш ТГ для связи**\\n_@username или ссылка_", parse_mode='MarkdownV2')
        return TG
    else:
        await update.message.reply_text("**Укажите трек-лист альбома**\\n_1\\. Track 1, 2\\. Track 2_", parse_mode='MarkdownV2')
        return ALBUM_TRACKLIST

# АЛЬБОМ
async def album_tracklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['tracklist'] = clean(update.message.text)
    await update.message.reply_text("**Укажите ваш ТГ для связи**\\n_@username или ссылка_", parse_mode='MarkdownV2')
    return ALBUM_TG

async def tg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['tg'] = update.message.text.strip()
    await show_confirm(update, context)
    return CONFIRM

async def show_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    data = user_data[user_id]
    text = f"**Проверьте анкету:**\\n\\n"
    for k, v in data.items():
        if k != 'type':
            text += f"• **{k.capitalize()}**: {v}\\n"
    keyboard = [[InlineKeyboardButton("✅ Отправить", callback_data='confirm')]]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='MarkdownV2')

async def send_moderation(query: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(query.from_user.id)
    data = user_data[user_id]
    user = query.from_user
    msg = f"**НОВАЯ АНКЕТА!**\\nОт: @{user.username or 'нет'}\\nТип: {data['type']}\\n\\n"
    for k, v in data.items():
        if k != 'type':
            msg += f"• **{k.capitalize()}**: {v}\\n"
    await context.bot.send_message(MODERATION_CHAT_ID, msg, parse_mode='MarkdownV2')
    if user_id not in db: db[user_id] = []
    db[user_id].append(data)
    save_db(db)
    await safe_edit(query, "**Отправлено!**\\nМодерация: 12–72 часа\\.", back_btn('main'))

def main():
    app = Application.builder().token(TOKEN).read_timeout(60).write_timeout(60).connect_timeout(60).pool_timeout(120).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            REPORT: [CallbackQueryHandler(button)],
            TYPE: [CallbackQueryHandler(button)],
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, name)],
            NICK: [MessageHandler(filters.TEXT & ~filters.COMMAND, nick_s)],
            FIO: [MessageHandler(filters.TEXT & ~filters.COMMAND, fio_s)],
            DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, date)],
            VERSION: [MessageHandler(filters.TEXT & ~filters.COMMAND, version)],
            GENRE: [MessageHandler(filters.TEXT & ~filters.COMMAND, genre)],
            LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, link)],
            MAT: [CallbackQueryHandler(mat)],
            PROMO: [MessageHandler(filters.TEXT & ~filters.COMMAND, promo)],
            COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, comment)],
            TG: [MessageHandler(filters.TEXT & ~filters.COMMAND, tg)],
            ALBUM_NICK: [MessageHandler(filters.TEXT & ~filters.COMMAND, album_nick)],
            ALBUM_FIO: [MessageHandler(filters.TEXT & ~filters.COMMAND, album_fio)],
            ALBUM_TRACKLIST: [MessageHandler(filters.TEXT & ~filters.COMMAND, album_tracklist)],
            CONFIRM: [CallbackQueryHandler(button)]
        },
        fallbacks=[CommandHandler('start', start)]
    )
    app.add_handler(conv)
    print("БОТ ЖИВ!")
    app.run_polling()

if __name__ == '__main__':
    main()
