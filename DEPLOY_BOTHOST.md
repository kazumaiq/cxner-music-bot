# Deploy на bothost.ru

## Важное
На бесплатном Node-тарифе запускать нужно **только** `app.js` в корне проекта.
Не запускайте `webapp/app.js` как главный файл.

## Вариант 1: Free plan (только Node.js)

1. Runtime: `Node.js`
2. Start file / command: `node app.js`
3. Переменные окружения:
   - `BOT_TOKEN` (обязательно)
4. Конфиг берется из `deploy_config.json` автоматически:
   - `MODERATION_CHAT_ID`
   - `ADMIN_IDS`
   - `WEBAPP_URL`
   - `PUBLIC_BASE_URL`
5. Ожидаемые логи при старте:
   - `[entry] starting app.js -> node_bot.js`
   - `[bot] CXRNER Node fallback bot started`

## Вариант 2: Python runtime (если доступен)

1. Runtime: `Python 3.11+`
2. Install command:
   - `pip install -r requirements.txt`
3. Start command:
   - `python -u main.py`
4. Переменные окружения:
   - `BOT_TOKEN` (обязательно)
   - остальные можно оставить в `deploy_config.json`

## Mini App URL

- Для Vercel укажи:
  - `WEBAPP_URL=https://cxrnermusic.vercel.app/miniapp/index.html`
- В BotFather обязательно:
  - `/setdomain` -> домен `https://cxrnermusic.vercel.app`

## Если снова видишь "Python runtime not found..."

Это означает, что на сервере запущен старый файл или старый билд.

Сделай:
1. Проверь, что старт именно `node app.js`.
2. Перезалей/переклонируй репозиторий (не инкрементальное обновление).
3. Перезапусти приложение.
4. Убедись по логам, что появилась строка `[entry] starting app.js -> node_bot.js`.
