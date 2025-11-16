import json
import os
import uuid
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, ConversationHandler
from telegram.error import NetworkError, BadRequest

TOKEN = "7657262123:AAHCcGPJcG6dfz4zazWbCnKhzpYdYyuvgt8"
MODERATION_CHAT_ID = -3279159129
ARTISTS_CHAT = "https://t.me/+oVmX3_dkyWJhNjJi"
CHANNEL = "https://t.me/cxrnermusic"
DB_FILE = "releases.json"

(REPORT, TYPE, NAME_S, NICK_S, FIO_S, DATE_S, PROMO_S, VER_S, LINK_S, MAT_GEN_S,
 NICK_A, FIO_A, NAME_A, DATE_A, MAT_A, GEN_A, FOCUS_A, PROMO_A, LINK_A, CONFIRM) = range(20)

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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("Сообщить о релизе", callback_data='report')],
        [InlineKeyboardButton("Канал", url=CHANNEL)],
        [InlineKeyboardButton("FAQ", callback_data='faq')],
        [InlineKeyboardButton("Чат артистов", url=ARTISTS_CHAT)],
        [InlineKeyboardButton("Кабинет", callback_data='cabinet')]
    ]
    await update.message.reply_text("**CXRNER MUSIC BOT**", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='MarkdownV2')
    return REPORT

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = str(query.from_user.id)

    if data == 'report':
        keyboard = [[InlineKeyboardButton("Сингл", callback_data='single')],
                    [InlineKeyboardButton("Альбом", callback_data='album')]]
        await safe_edit(query, "Тип релиза:", InlineKeyboardMarkup(keyboard))
        return TYPE

    if data == 'single':
        user_data[user_id] = {'type': 'сингл'}
        await safe_edit(query, "**Сингл**\\n1\\. Название\\n_Пример: Tokyo Rain_", parse_mode='MarkdownV2')
        return NAME_S

    if data == 'cabinet':
        releases = db.get(user_id, [])
        if not releases:
            await safe_edit(query, "Кабинет пуст!", back_btn('main'))
            return REPORT
        text = "**Релизы:**\\n"
        for i, r in enumerate(releases):
            text += f"\\n{r.get('name', '—')} — {r['type']}\\n{r.get('date', '-')}\\n"
        await safe_edit(query, text, back_btn('main'))
        return REPORT

    if data == 'confirm':
        await send_moderation(query, context)
        return REPORT

    if data == 'main':
        return await start(update, context)

def back_btn(to='main'):
    return InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data=to)]])

def clean(text):
    return ' '.join([w for w in text.split() if not w.lower().startswith(('1.', '2.', '3.'))]).strip()

async def name_s(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.message.from_user.id)
    user_data[user_id]['name'] = clean(update.message.text)
    await update.message.reply_text("2\\. Ник\\n_Пример: MAKIZM_", parse_mode='MarkdownV2')
    return NICK_S

async def send_moderation(query: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(query.from_user.id)
    data = user_data[user_id]
    user = query.from_user
    msg = f"**Новая анкета!**\\nОт: @{user.username or 'нет'}\\n\\n"
    for k, v in data.items():
        if k != 'type': msg += f"• {k}: {v}\\n"
    await context.bot.send_message(MODERATION_CHAT_ID, msg, parse_mode='MarkdownV2')
    if user_id not in db: db[user_id] = []
    db[user_id].append(data)
    save_db(db)
    await safe_edit(query, "**Отправлено!**", back_btn('main'))

def main():
    app = Application.builder().token(TOKEN). vred_timeout(60).write_timeout(60).connect_timeout(60).pool_timeout(120).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            REPORT: [CallbackQueryHandler(button)],
            TYPE: [CallbackQueryHandler(button)],
            NAME_S: [MessageHandler(filters.TEXT & ~filters.COMMAND, name_s)],
            CONFIRM: [CallbackQueryHandler(button)]
        },
        fallbacks=[CommandHandler('start', start)]
    )
    app.add_handler(conv)
    print("БОТ ЖИВ!")
    app.run_polling()

if __name__ == '__main__':
    main()