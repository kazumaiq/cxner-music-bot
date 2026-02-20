# CXRNER MUSIC Bot

Telegram bot + Mini App for release distribution workflow:
- artist release form
- moderation statuses
- cabinet with user releases/statuses
- static Mini App (`webapp/`)

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

Recommended for bothost:
```bash
set PUBLIC_BASE_URL=https://<your-bothost-domain>
set WEBAPP_URL=https://<your-bothost-domain>/index.html
```

For production deployment on bothost, see `DEPLOY_BOTHOST.md`.

## App Modes (`app.js`)
By default, `node app.js` starts the Node bot (`node_bot.js`) to keep current behavior.

- `APP_MODE` empty: bot mode (default)
- `APP_MODE=miniapp` or `APP_MODE=static`: serves static Mini App from `public/`
- `APP_MODE=both`: starts both static server and bot process

BotHost static structure is included in `public/`:
- `public/index.html`
- `public/styles.css`
- `public/app.js`
- `public/assets/*`
