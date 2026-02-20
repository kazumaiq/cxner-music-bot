# Deploy на bothost.ru

## Важно
- На free Node-тарифе запускается один JS-файл: `app.js`.
- Не запускайте `webapp/app.js` как entrypoint.

## Рекомендуемый запуск
1. Runtime: `Node.js`
2. Start command: `node app.js`
3. Обязательная переменная:
   - `BOT_TOKEN`
4. Остальные параметры берутся из `deploy_config.json` или env.

## Минимальные переменные для стабильной работы
- `BOT_TOKEN`
- `MODERATION_CHAT_ID`
- `ADMIN_IDS`
- `PUBLIC_BASE_URL=https://<your-bothost-domain>`
- `WEBAPP_URL=https://<your-bothost-domain>/index.html`

## BotFather
- `/setdomain` -> `https://<your-bothost-domain>`
- Menu Button URL -> `https://<your-bothost-domain>/index.html`

## Проверка после старта
В логах должны быть строки:
- `[entry] starting app.js -> node_bot.js`
- `[bot] CXRNER Node fallback bot started`
- `[bot] public base: https://...`

После первого запроса к Mini App появится:
- `[web] public domain detected: https://<your-bothost-domain>`

## Если снова проблемы
1. Проверьте, что старт именно `node app.js`.
2. Перезапустите приложение после изменения переменных.
3. Убедитесь, что бот добавлен админом в группу модерации.
4. Проверьте:
   - `https://<your-bothost-domain>/index.html`
   - `https://<your-bothost-domain>/api/miniapp/ping`
