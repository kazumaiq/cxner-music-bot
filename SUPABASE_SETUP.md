# Supabase Setup (CXRNER Node Bot)

## 1) Создайте таблицы в Supabase SQL Editor

```sql
create table if not exists public.cxrner_releases (
  user_id text not null,
  release_idx integer not null,
  release_data jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  primary key (user_id, release_idx)
);

create index if not exists idx_cxrner_releases_user on public.cxrner_releases (user_id);
create index if not exists idx_cxrner_releases_updated on public.cxrner_releases (updated_at desc);

create table if not exists public.cxrner_cabinet_users (
  user_id text primary key,
  profile jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create index if not exists idx_cxrner_cabinet_updated on public.cxrner_cabinet_users (updated_at desc);
```

## 2) Заполните `deploy_config.json`

```json
"SUPABASE_URL": "https://<project-ref>.supabase.co",
"SUPABASE_SERVICE_ROLE_KEY": "<service-role-key>",
"SUPABASE_SCHEMA": "public",
"SUPABASE_RELEASES_TABLE": "cxrner_releases",
"SUPABASE_CABINET_TABLE": "cxrner_cabinet_users"
```

Важно: нужен именно `SERVICE_ROLE_KEY` (серверный ключ).

## 3) Перезапустите бота

На старте бот:
- подтянет релизы из локального бэкапа/JSON;
- загрузит релизы из Supabase;
- сольет данные без потери;
- начнет авто-синхронизацию при новых анкетах и смене статусов.

## 4) Проверка

В логах должно появиться:
- `supabase sync: enabled (...)`
- `supabase tables: cxrner_releases, cxrner_cabinet_users`
- `supabase synced (startup): releases=..., cabinet=...`

## 5) Mini App submit API (важно для Menu Button)

`sendData()` из режима Menu Button в Telegram часто не доставляет `WEB_APP_DATA` в бот.
Для стабильной отправки анкеты используйте API бота:

- файл: `webapp/data/supabase-config.json`
- поле: `botApiBaseUrl`
- значение: корневой URL сервера, где запущен `node_bot.js`

Пример:

```json
{
  "botApiBaseUrl": "https://<your-bot-domain>"
}
```

Важно:
- указывать нужно домен бэкенда бота, а не URL статики Mini App;
- не добавляйте `/miniapp` в `botApiBaseUrl`;
- Mini App будет отправлять POST на `https://<your-bot-domain>/api/webapp/submit`.
