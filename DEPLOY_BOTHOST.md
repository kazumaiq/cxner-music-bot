# Deploy on bothost.ru

## 1. Runtime
- Python: `3.11+`
- Start command:
```bash
python -u main.py
```
- Install command:
```bash
pip install -r requirements.txt
```

## 2. Environment variables
Set these in bothost panel:

- `BOT_TOKEN` — token from BotFather (required)
- `MODERATION_CHAT_ID` — moderation group chat id, e.g. `-100...`
- `ADMIN_IDS` — comma-separated Telegram user IDs, e.g. `881379104,123456789`
- `WEBAPP_URL` — public HTTPS URL for Mini App, e.g. `https://your-domain.tld/index.html`
- `ENABLE_WEB_SERVER=1`
- `PORT` — if bothost provides it automatically, keep host default; otherwise set manually (e.g. `8080`)

Optional:
- `OPENAI_API_KEY`

## 2.1 If only BOT_TOKEN env is available (free plan)
Use local file `deploy_config.json` in project root.  
The bot already reads it automatically.

Required in this case:
- set only `BOT_TOKEN` in panel
- fill `deploy_config.json` fields:
  - `MODERATION_CHAT_ID`
  - `ADMIN_IDS`
  - `PUBLIC_BASE_URL` (your bothost public URL, e.g. `https://your-app.bothost.ru`)
  - or explicit `WEBAPP_URL`

If `PUBLIC_BASE_URL` is set, bot builds Mini App URL automatically as:
- `PUBLIC_BASE_URL + /index.html`

## 3. Mini App domain in BotFather
Configure bot domain:
- `/setdomain` -> select your bot -> set `https://your-domain.tld`

## 4. Mini App open flow (important)
This project opens Mini App through a **keyboard button** for reliable `WebApp.sendData()` delivery.
Use:
- `/start` -> `Открыть приложение`
- or `/app`

## 5. First launch checklist
- Bot starts without `BOT_TOKEN` error
- `/start` shows main menu
- `Открыть приложение` sends keyboard launcher button
- Submit in Mini App creates a new form in moderation group
- `webapp/data/releases-public.json` updates after status changes

## 6. Troubleshooting
- If Mini App opens but submit does not reach bot:
  - ensure app launched from bot button (`/app` or `Открыть приложение`)
  - ensure `WEBAPP_URL` is HTTPS and domain is set in BotFather `/setdomain`
  - check logs for `[WEBAPP] action=...` lines
