# CXRNER MUSIC Bot

Telegram bot + Mini App for release distribution workflow:
- artist release form
- moderation statuses
- cabinet with user releases/statuses
- static Mini App (`webapp/`)
- BotHost static Mini App (`public/`)

## Quick start
```bash
pip install -r requirements.txt
set BOT_TOKEN=123456:token
set MODERATION_CHAT_ID=-1000000000000
set ADMIN_IDS=881379104
set WEBAPP_URL=https://your-domain.tld/index.html
python -u main.py
```

## Quick start (Node only, bothost free plan)
```bash
set BOT_TOKEN=123456:token
node app.js
```

For production deployment on bothost, see `DEPLOY_BOTHOST.md`.

## BotHost Mini App mode
Project now contains BotHost-compatible structure:

- `app.js` - static server entrypoint
- `public/index.html` - Mini App page
- `public/styles.css`, `public/app.js` - UI assets
- `public/assets/*`, `public/data/*` - static files

Start command:
```bash
node app.js
```

By default this starts static Mini App server on `PORT` (or `3000`).
Legacy `/miniapp/*` URLs are also supported and mapped to `public/*`.

If you explicitly need to run Node bot from this entrypoint:
```bash
set APP_MODE=bot
node app.js
```
