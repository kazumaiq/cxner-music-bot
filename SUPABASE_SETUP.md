# Supabase Setup (CXRNER Bot + Mini App)

Новая архитектура:
- Mini App отправляет анкету через `Telegram.WebApp.sendData`.
- Бот принимает `web_app_data`, валидирует payload, пишет в Supabase.
- Бот отправляет анкету в группу модерации.
- Статусы анкеты и кабинет читаются из Supabase.

## 1) Обязательные таблицы (новые)

```sql
create table if not exists public.cxrner_forms (
  id uuid primary key default gen_random_uuid(),
  telegram_id text not null,
  username text,
  artist_name text not null,
  track_name text not null,
  genre text not null,
  release_type text not null,
  status text not null default 'pending' check (status in ('pending','on_moderation','approved','rejected')),
  reject_reason text default '',
  upc text default '',
  moderation_message_id bigint,
  submission_key text not null,
  source text default 'mini_app',
  form_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists uq_cxrner_forms_submission
  on public.cxrner_forms (telegram_id, submission_key);

create index if not exists idx_cxrner_forms_tg
  on public.cxrner_forms (telegram_id);

create index if not exists idx_cxrner_forms_status
  on public.cxrner_forms (status);

create table if not exists public.cxrner_users (
  telegram_id text primary key,
  username text,
  first_name text,
  cabinet_active boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.cxrner_public_releases (
  form_id text primary key,
  telegram_id text not null,
  username text,
  artist_name text not null,
  track_name text not null,
  genre text,
  release_type text,
  status text not null default 'approved',
  approved_at timestamptz,
  updated_at timestamptz not null default now(),
  release_data jsonb not null default '{}'::jsonb
);

create index if not exists idx_cxrner_public_releases_tg
  on public.cxrner_public_releases (telegram_id);
```

## 2) Legacy таблицы (оставляем для совместимости)

```sql
create table if not exists public.cxrner_releases (
  user_id text not null,
  release_idx integer not null,
  release_data jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  primary key (user_id, release_idx)
);

create table if not exists public.cxrner_cabinet_users (
  user_id text primary key,
  profile jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);
```

## 3) deploy_config.json (бот)

```json
{
  "SUPABASE_URL": "https://<project-ref>.supabase.co",
  "SUPABASE_SERVICE_ROLE_KEY": "<service-role-key>",
  "SUPABASE_SCHEMA": "public",

  "SUPABASE_RELEASES_TABLE": "cxrner_releases",
  "SUPABASE_CABINET_TABLE": "cxrner_cabinet_users",

  "SUPABASE_FORMS_TABLE": "cxrner_forms",
  "SUPABASE_USERS_TABLE": "cxrner_users",
  "SUPABASE_PUBLIC_RELEASES_TABLE": "cxrner_public_releases"
}
```

## 4) Mini App runtime config

Файл: `public/miniapp/data/supabase-config.json`

```json
{
  "url": "https://<project-ref>.supabase.co",
  "anonKey": "<anon-public-key>",
  "schema": "public",
  "formsTable": "cxrner_forms",
  "usersTable": "cxrner_users",
  "releasesTable": "cxrner_public_releases"
}
```

Важно: в Mini App используется `anonKey`, а в боте — `service_role_key`.

## 5) Что должно работать после настройки

- Отправка анкеты через Mini App -> группа модерации.
- Статусы формы в `cxrner_forms`: `pending -> on_moderation -> approved/rejected`.
- Кабинет читает релизы из Supabase (`cxrner_forms`).
- `cabinet_activate` создает/обновляет запись в `cxrner_users`.
- При approve бот пишет в `cxrner_public_releases`.
