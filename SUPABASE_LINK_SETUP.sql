-- CXRNER LINK v2.0
-- Run once in the current Supabase project.
create table if not exists public.cxrner_links (
  id uuid primary key default gen_random_uuid(),
  telegram_id bigint not null references public.cxrner_telegram_profiles(telegram_id) on delete cascade,
  release_id text,
  slug text not null unique,
  title text not null default '',
  artist text not null default '',
  genre text not null default '',
  release_date text not null default '',
  cover_url text not null default '',
  platforms jsonb not null default '{}'::jsonb,
  extra_links jsonb not null default '[]'::jsonb,
  theme text not null default 'aurora',
  settings jsonb not null default '{}'::jsonb,
  status text not null default 'published' check (status in ('draft','published','archived')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists cxrner_links_artist_idx on public.cxrner_links (telegram_id, updated_at desc);
create index if not exists cxrner_links_release_idx on public.cxrner_links (release_id);

create table if not exists public.cxrner_link_events (
  id uuid primary key default gen_random_uuid(),
  link_id uuid not null references public.cxrner_links(id) on delete cascade,
  event_type text not null check (event_type in ('view','click')),
  platform text not null default '',
  source text not null default '',
  country text not null default '',
  device text not null default '',
  user_agent text not null default '',
  created_at timestamptz not null default now()
);

create index if not exists cxrner_link_events_link_idx on public.cxrner_link_events (link_id, created_at desc);
create index if not exists cxrner_link_events_type_idx on public.cxrner_link_events (event_type, created_at desc);

alter table public.cxrner_links enable row level security;
alter table public.cxrner_link_events enable row level security;

drop policy if exists "service role manages cxrner links" on public.cxrner_links;
create policy "service role manages cxrner links" on public.cxrner_links for all to service_role using (true) with check (true);
drop policy if exists "service role manages cxrner link events" on public.cxrner_link_events;
create policy "service role manages cxrner link events" on public.cxrner_link_events for all to service_role using (true) with check (true);
