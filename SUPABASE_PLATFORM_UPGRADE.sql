-- CXRNER Mini App platform extension.
-- Run after SUPABASE_COMPLETE_SETUP.sql in the current Supabase project.

create table if not exists public.cxrner_listen_events (
  id uuid primary key default gen_random_uuid(),
  release_id text not null,
  telegram_id bigint references public.cxrner_telegram_profiles(telegram_id) on delete set null,
  seconds_played integer not null default 0,
  source text not null default 'mini_app',
  country text,
  created_at timestamptz not null default now()
);

create table if not exists public.cxrner_payouts (
  id uuid primary key default gen_random_uuid(),
  telegram_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  amount numeric(12,2) not null default 0,
  currency text not null default 'RUB',
  status text not null default 'pending' check (status in ('pending','confirmed','paid','cancelled')),
  period text,
  note text not null default '',
  created_at timestamptz not null default now(),
  paid_at timestamptz
);

create table if not exists public.cxrner_revenue_events (
  id uuid primary key default gen_random_uuid(),
  telegram_id bigint references public.cxrner_telegram_profiles(telegram_id) on delete set null,
  release_id text,
  platform text not null default 'unknown',
  country text,
  source text,
  amount numeric(12,2) not null default 0,
  currency text not null default 'RUB',
  event_date date not null default current_date,
  created_at timestamptz not null default now()
);

create table if not exists public.cxrner_news (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  body text not null default '',
  cover_url text not null default '',
  published boolean not null default false,
  published_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.cxrner_artist_profiles (
  telegram_id bigint primary key references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  display_name text not null default '',
  bio text not null default '',
  avatar_url text not null default '',
  socials jsonb not null default '{}'::jsonb,
  theme jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.cxrner_artist_follows (
  follower_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  following_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (follower_id, following_id),
  check (follower_id <> following_id)
);

create table if not exists public.cxrner_comment_likes (
  comment_id uuid not null references public.cxrner_comments(id) on delete cascade,
  telegram_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (comment_id, telegram_id)
);

create table if not exists public.cxrner_referral_events (
  id uuid primary key default gen_random_uuid(),
  referrer_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  invited_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  event_type text not null default 'signup',
  bonus numeric(12,2) not null default 0,
  created_at timestamptz not null default now(),
  unique (referrer_id, invited_id)
);

create table if not exists public.cxrner_platform_settings (
  key text primary key,
  value jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.cxrner_admin_logs (
  id uuid primary key default gen_random_uuid(),
  admin_id bigint not null,
  action text not null,
  entity text not null,
  entity_id text,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists cxrner_listens_release_idx on public.cxrner_listen_events (release_id, created_at desc);
create index if not exists cxrner_payouts_user_idx on public.cxrner_payouts (telegram_id, created_at desc);
create index if not exists cxrner_revenue_user_idx on public.cxrner_revenue_events (telegram_id, event_date desc);
create index if not exists cxrner_news_published_idx on public.cxrner_news (published, published_at desc);
create index if not exists cxrner_artist_follows_following_idx on public.cxrner_artist_follows (following_id, created_at desc);
create index if not exists cxrner_comment_likes_comment_idx on public.cxrner_comment_likes (comment_id, created_at desc);
create index if not exists cxrner_referral_events_referrer_idx on public.cxrner_referral_events (referrer_id, created_at desc);

alter table public.cxrner_listen_events enable row level security;
alter table public.cxrner_payouts enable row level security;
alter table public.cxrner_revenue_events enable row level security;
alter table public.cxrner_news enable row level security;

drop policy if exists cxrner_news_public_read on public.cxrner_news;
create policy cxrner_news_public_read on public.cxrner_news for select using (published = true);

notify pgrst, 'reload schema';

-- CXRNER LINK v2.0
create table if not exists public.cxrner_links (
  id uuid primary key default gen_random_uuid(), telegram_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  release_id text, slug text not null unique, title text not null default '', artist text not null default '', genre text not null default '', release_date text not null default '', cover_url text not null default '',
  platforms jsonb not null default '{}'::jsonb, extra_links jsonb not null default '[]'::jsonb, theme text not null default 'aurora', settings jsonb not null default '{}'::jsonb,
  status text not null default 'published' check (status in ('draft','published','archived')), created_at timestamptz not null default now(), updated_at timestamptz not null default now()
);
create index if not exists cxrner_links_artist_idx on public.cxrner_links (telegram_id, updated_at desc);
create table if not exists public.cxrner_link_events (
  id uuid primary key default gen_random_uuid(), link_id uuid not null references public.cxrner_links(id) on delete cascade,
  event_type text not null check (event_type in ('view','click')), platform text not null default '', source text not null default '', country text not null default '', device text not null default '', user_agent text not null default '', created_at timestamptz not null default now()
);
create index if not exists cxrner_link_events_link_idx on public.cxrner_link_events (link_id, created_at desc);
alter table public.cxrner_links enable row level security;
alter table public.cxrner_link_events enable row level security;
drop policy if exists "service role manages cxrner links" on public.cxrner_links;
create policy "service role manages cxrner links" on public.cxrner_links for all to service_role using (true) with check (true);
drop policy if exists "service role manages cxrner link events" on public.cxrner_link_events;
create policy "service role manages cxrner link events" on public.cxrner_link_events for all to service_role using (true) with check (true);
notify pgrst, 'reload schema';
