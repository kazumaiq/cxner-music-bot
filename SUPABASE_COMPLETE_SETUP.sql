-- CXRNER MUSIC: complete Supabase setup
-- Run once in Supabase Dashboard -> SQL Editor -> Run.
create extension if not exists pgcrypto;

create table if not exists public.featured_release (id text primary key, title text not null, artist text not null, cover text not null, updated_at timestamptz not null default now());
create table if not exists public.releases_config (id text primary key, items jsonb not null default '[]'::jsonb, updated_at timestamptz not null default now());
create table if not exists public.artists_config (id text primary key, items jsonb not null default '[]'::jsonb, updated_at timestamptz not null default now());
create table if not exists public.cxrner_cabinet_users (user_id uuid primary key references auth.users(id) on delete cascade, profile jsonb not null default '{}'::jsonb, updated_at timestamptz not null default now());

create table if not exists public.cxrner_forms (
  id uuid primary key default gen_random_uuid(), telegram_id text, username text, artist_name text, track_name text,
  genre text, release_type text, submission_key text not null unique, status text not null default 'pending',
  source text not null default 'telegram', form_payload jsonb not null default '{}'::jsonb, upc text,
  reject_reason text, moderation_message_id text, created_at timestamptz not null default now(), updated_at timestamptz not null default now()
);
create index if not exists cxrner_forms_telegram_id_idx on public.cxrner_forms (telegram_id);
create index if not exists cxrner_forms_created_at_idx on public.cxrner_forms (created_at desc);

create table if not exists public.cxrner_telegram_profiles (
  telegram_id bigint primary key, username text, first_name text, last_name text, photo_url text,
  role text not null default 'artist' check (role in ('artist', 'moderator', 'admin')),
  status text not null default 'active' check (status in ('active', 'blocked')),
  registered_at timestamptz not null default now(), last_seen_at timestamptz not null default now(), metadata jsonb not null default '{}'::jsonb
);
create table if not exists public.cxrner_release_engagements (
  release_id text not null, telegram_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  kind text not null check (kind in ('like', 'favorite')), created_at timestamptz not null default now(), primary key (release_id, telegram_id, kind)
);
create table if not exists public.cxrner_comments (
  id uuid primary key default gen_random_uuid(), release_id text not null,
  telegram_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  parent_id uuid references public.cxrner_comments(id) on delete cascade,
  body text not null check (char_length(body) between 1 and 2000), is_pinned boolean not null default false,
  created_at timestamptz not null default now(), updated_at timestamptz not null default now()
);
create table if not exists public.cxrner_badges (
  id uuid primary key default gen_random_uuid(), slug text not null unique, name text not null,
  description text not null default '', image_url text not null, created_at timestamptz not null default now()
);
create table if not exists public.cxrner_artist_badges (
  badge_id uuid not null references public.cxrner_badges(id) on delete cascade,
  telegram_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  assigned_at timestamptz not null default now(), primary key (badge_id, telegram_id)
);
create table if not exists public.cxrner_achievements (
  id uuid primary key default gen_random_uuid(), slug text not null unique, name text not null,
  description text not null default '', icon text not null default 'sparkles', rule jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);
create table if not exists public.cxrner_user_achievements (
  achievement_id uuid not null references public.cxrner_achievements(id) on delete cascade,
  telegram_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  awarded_at timestamptz not null default now(), primary key (achievement_id, telegram_id)
);
create table if not exists public.cxrner_notifications (
  id uuid primary key default gen_random_uuid(), telegram_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  type text not null, title text not null, body text not null default '', read_at timestamptz, created_at timestamptz not null default now()
);
create table if not exists public.cxrner_referrals (
  telegram_id bigint primary key references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  code text not null unique, invited_by bigint references public.cxrner_telegram_profiles(telegram_id),
  bonus_balance numeric(12,2) not null default 0, created_at timestamptz not null default now()
);

create index if not exists cxrner_comments_release_idx on public.cxrner_comments (release_id, created_at desc);
create index if not exists cxrner_notifications_user_idx on public.cxrner_notifications (telegram_id, created_at desc);
create index if not exists cxrner_profiles_username_idx on public.cxrner_telegram_profiles (lower(username));

insert into public.featured_release (id, title, artist, cover)
values ('current', 'Neon Drift', 'KAZUMAI', '/images/album-01.svg') on conflict (id) do nothing;

alter table public.featured_release enable row level security;
alter table public.releases_config enable row level security;
alter table public.artists_config enable row level security;
alter table public.cxrner_cabinet_users enable row level security;
alter table public.cxrner_forms enable row level security;
alter table public.cxrner_telegram_profiles enable row level security;
alter table public.cxrner_release_engagements enable row level security;
alter table public.cxrner_comments enable row level security;
alter table public.cxrner_badges enable row level security;
alter table public.cxrner_artist_badges enable row level security;
alter table public.cxrner_achievements enable row level security;
alter table public.cxrner_user_achievements enable row level security;
alter table public.cxrner_notifications enable row level security;
alter table public.cxrner_referrals enable row level security;

drop policy if exists "public can read featured" on public.featured_release;
create policy "public can read featured" on public.featured_release for select to anon, authenticated using (true);
drop policy if exists "public can read releases config" on public.releases_config;
create policy "public can read releases config" on public.releases_config for select to anon, authenticated using (true);
drop policy if exists "public can read artists config" on public.artists_config;
create policy "public can read artists config" on public.artists_config for select to anon, authenticated using (true);
drop policy if exists "public reads badges" on public.cxrner_badges;
create policy "public reads badges" on public.cxrner_badges for select to anon, authenticated using (true);
drop policy if exists "public reads achievements" on public.cxrner_achievements;
create policy "public reads achievements" on public.cxrner_achievements for select to anon, authenticated using (true);

drop policy if exists "users can read own profile" on public.cxrner_cabinet_users;
create policy "users can read own profile" on public.cxrner_cabinet_users for select to authenticated using (user_id = auth.uid());
drop policy if exists "users can create own profile" on public.cxrner_cabinet_users;
create policy "users can create own profile" on public.cxrner_cabinet_users for insert to authenticated with check (user_id = auth.uid());
drop policy if exists "users can update own profile" on public.cxrner_cabinet_users;
create policy "users can update own profile" on public.cxrner_cabinet_users for update to authenticated using (user_id = auth.uid()) with check (user_id = auth.uid());
drop policy if exists "users can read own forms" on public.cxrner_forms;
create policy "users can read own forms" on public.cxrner_forms for select to authenticated using (telegram_id = auth.uid()::text);
drop policy if exists "users can create own forms" on public.cxrner_forms;
create policy "users can create own forms" on public.cxrner_forms for insert to authenticated with check (telegram_id = auth.uid()::text);

create or replace function public.handle_new_cxrner_user()
returns trigger language plpgsql security definer set search_path = public as $$
begin
  insert into public.cxrner_cabinet_users (user_id, profile)
  values (new.id, jsonb_strip_nulls(jsonb_build_object('artist_name', new.raw_user_meta_data ->> 'artist_name')))
  on conflict (user_id) do nothing;
  return new;
end;
$$;
drop trigger if exists on_auth_user_created_cxrner on auth.users;
create trigger on_auth_user_created_cxrner after insert on auth.users for each row execute procedure public.handle_new_cxrner_user();

do $$
begin
  if exists (select 1 from pg_publication where pubname = 'supabase_realtime')
    and not exists (select 1 from pg_publication_tables where pubname = 'supabase_realtime' and schemaname = 'public' and tablename = 'cxrner_forms') then
    alter publication supabase_realtime add table public.cxrner_forms;
  end if;
end $$;

insert into storage.buckets (id, name, public) values ('covers', 'covers', true)
on conflict (id) do update set public = true;
