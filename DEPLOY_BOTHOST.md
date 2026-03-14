# Deploy на bothost.ru

## Важное
На бесплатном Node-тарифе запускать нужно **только** `app.js` в корне проекта.
Не запускайте `webapp/app.js` как главный файл.

## Вариант 1: Free plan (только Node.js)

1. Runtime: `Node.js`
2. Start file / command: `node app.js`
3. Переменные окружения (в панели BOTHOST → проект → Переменные окружения):
   - `BOT_TOKEN` (обязательно)
   - `SUPABASE_SERVICE_ROLE_KEY` — для Supabase: **Service Role Key** из Supabase Dashboard → Project Settings → API (не anon!)
   - При наличии переменных окружения они имеют приоритет над `deploy_config.json`.
4. Конфиг из `deploy_config.json` (если не задано в env):
   - `MODERATION_CHAT_ID`, `ADMIN_IDS`, `WEBAPP_URL`, `PUBLIC_BASE_URL`, `SUPABASE_URL` и др.
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

## PUBLIC_BASE_URL — как заполнить

**Что это:** полный URL, по которому доступен твой бот (если включён веб-сервер). Нужен для CORS: Mini App с этого домена сможет вызывать API бота (`/api/miniapp/...`).

**Как заполнить:**
- Если деплоишь на **BOTHOST** и включаешь `ENABLE_WEB_SERVER: true` — укажи URL приложения, например:
  - `https://твой-проект.bothost.run` (без слэша в конце)
- Если Mini App лежит на **Vercel**, а бот на BOTHOST без веб-сервера — можно оставить пустым `""`.
- В `webapp/data/supabase-config.json` (и в BotFather) поле `botApiBaseUrl` должно совпадать с этим URL, если юзеры отправляют анкеты через API бота.

## WEBAPP_URL — как заполнить

**Что это:** ссылка на Mini App (анкета релиза), которую бот показывает в меню.

**Как заполнить:**
- Для Vercel:
  - `https://cxrnermusic.vercel.app/` или `https://cxrnermusic.vercel.app/miniapp/index.html`
- В BotFather обязательно:
  - `/setdomain` → домен `https://cxrnermusic.vercel.app`

## Ошибка 401 "Invalid API key" от Supabase

Если в логах: `supabase GET ... -> 401: {"message":"Invalid API key"}`:

1. **Откуда берётся ключ:** сначала проверяются **переменные окружения** BOTHOST, потом `deploy_config.json`. Секреты лучше хранить в переменных окружения в панели BOTHOST, а не в репозитории.
2. В логах при старте есть строка `[bot] supabase key: length=N`. Если **length=0** — ключ не подставлен (env не задан или имя переменной с опечаткой: должно быть именно `SUPABASE_SERVICE_ROLE_KEY`). Если **length &lt; 180** — возможно подставлен anon-ключ или ключ обрезан; нужен полный **service_role** ключ.
3. В [Supabase Dashboard](https://supabase.com/dashboard) → твой проект → **Project Settings** → **API** скопируй ключ **service_role** (Reveal), целиком. Вставь в переменную `SUPABASE_SERVICE_ROLE_KEY` в BOTHOST, сохрани и **перезапусти** приложение.

## Если снова видишь "Python runtime not found..."

Это означает, что на сервере запущен старый файл или старый билд.

Сделай:
1. Проверь, что старт именно `node app.js`.
2. Перезалей/переклонируй репозиторий (не инкрементальное обновление).
3. Перезапусти приложение.
4. Убедись по логам, что появилась строка `[entry] starting app.js -> node_bot.js`.
