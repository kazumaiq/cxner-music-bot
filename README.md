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

For production deployment on bothost, see `DEPLOY_BOTHOST.md`.
