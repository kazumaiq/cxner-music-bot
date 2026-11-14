-- =============================================================================
-- CXRNER MUSIC ECOSYSTEM & MINI APP 3.0 — MASTER SUPABASE SQL MIGRATION (006)
-- Полная идемпотентная настройка всех таблиц, индексов и RLS-политик
-- для сайта (cxrnermusic.vercel.app) и Telegram Mini App (cxner-music-bot).
-- Скопируйте этот скрипт целиком в Supabase Dashboard -> SQL Editor -> Run.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. Функция проверки прав администратора и CEO (@kazumaiq)
CREATE OR REPLACE FUNCTION public.is_cxrner_admin()
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
STABLE
AS $$
  SELECT COALESCE(
    (auth.jwt() ->> 'email') IN (
      'cxrner.label@gmail.com',
      'kazumaiq.music@gmail.com',
      'kazumaiq@gmail.com'
    )
    OR (auth.jwt() -> 'user_metadata' ->> 'username') IN ('kazumaiq', 'cxrnermusic')
    OR (auth.jwt() -> 'app_metadata' ->> 'role') = 'admin',
    false
  );
$$;

-- =============================================================================
-- 2. ЯДРО ЭКОСИСТЕМЫ И ЕДИНОГО КАБИНЕТА (cxrner_cabinet_users)
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.cxrner_cabinet_users (
  user_id text PRIMARY KEY,
  profile jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cxrner_cabinet_users_updated_at
  ON public.cxrner_cabinet_users (updated_at DESC);

-- =============================================================================
-- 3. CXRNER DROP — КОШЕЛЬКИ CXP, ТРАНЗАКЦИИ, ИНВЕНТАРЬ, ИСТОРИЯ, ВАУЧЕРЫ
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.cxrner_cxp_wallet (
  user_id text PRIMARY KEY,
  balance integer NOT NULL DEFAULT 100 CHECK (balance >= 0),
  total_earned integer NOT NULL DEFAULT 100,
  total_spent integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cxrner_cxp_transactions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id text NOT NULL,
  amount integer NOT NULL,
  event_type text NOT NULL DEFAULT 'bonus',
  source_event_id text,
  description text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cxrner_cxp_tx_user_created
  ON public.cxrner_cxp_transactions (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS public.cxrner_drop_inventory (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id text NOT NULL,
  item_id text NOT NULL,
  item_name text NOT NULL,
  specimen_code text DEFAULT 'SP-001',
  specimen_class text DEFAULT 'ARCHIVE SPECIMEN',
  collection_name text NOT NULL DEFAULT 'CXRNER ARCHIVE',
  rarity text NOT NULL DEFAULT 'common',
  drop_chance text DEFAULT '54%',
  serial_number integer NOT NULL DEFAULT 1,
  description text DEFAULT '',
  image_url text DEFAULT '',
  model_3d_type text DEFAULT 'procedural',
  model_color text DEFAULT '#8737FF',
  obtained_via text DEFAULT 'case_open',
  is_special_reward boolean NOT NULL DEFAULT false,
  claim_code text,
  is_equipped boolean NOT NULL DEFAULT false,
  is_pinned boolean NOT NULL DEFAULT false,
  is_hidden boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cxrner_drop_inv_user
  ON public.cxrner_drop_inventory (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS public.cxrner_drop_history (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id text NOT NULL,
  case_id text NOT NULL,
  case_name text NOT NULL,
  item_id text NOT NULL,
  item_name text NOT NULL,
  specimen_code text DEFAULT 'SP-001',
  rarity text NOT NULL DEFAULT 'common',
  serial_number integer DEFAULT 1,
  cxp_spent integer NOT NULL DEFAULT 0,
  is_special_reward boolean NOT NULL DEFAULT false,
  claim_code text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cxrner_drop_hist_created
  ON public.cxrner_drop_history (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_cxrner_drop_hist_user
  ON public.cxrner_drop_history (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS public.special_reward_claims (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id text NOT NULL,
  inventory_item_id text,
  claim_code text UNIQUE NOT NULL,
  reward_type text NOT NULL DEFAULT 'free_cover_by_kazumaiq',
  status text NOT NULL DEFAULT 'WON',
  won_at timestamptz NOT NULL DEFAULT now(),
  claimed_at timestamptz,
  redeemed_at timestamptz,
  admin_notes text,
  handled_by_admin text
);

-- =============================================================================
-- 4. ПРОФИЛИ CXRNER ID, АРТИСТЫ, ВЕРИФИКАЦИЯ, РЕФЕРАЛЫ, УВЕДОМЛЕНИЯ
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.cxrner_profiles (
  user_id text PRIMARY KEY,
  username text UNIQUE NOT NULL,
  display_name text NOT NULL DEFAULT 'CXRNER USER',
  avatar_url text DEFAULT '',
  banner_url text DEFAULT '',
  bio text DEFAULT '',
  country text DEFAULT '',
  city text DEFAULT '',
  genres text[] DEFAULT '{}',
  roles text[] DEFAULT '{}',
  social_links jsonb DEFAULT '{}'::jsonb,
  telegram_id text,
  telegram_username text,
  telegram_linked boolean DEFAULT false,
  is_artist_verified boolean DEFAULT false,
  is_public boolean DEFAULT true,
  show_inventory boolean DEFAULT true,
  show_achievements boolean DEFAULT true,
  show_releases boolean DEFAULT true,
  show_stats boolean DEFAULT true,
  equipped_frame_id text,
  equipped_badge_id text,
  equipped_Aura_id text,
  completion_percentage integer DEFAULT 20,
  xp integer DEFAULT 0,
  level integer DEFAULT 1,
  is_banned boolean DEFAULT false,
  ban_reason text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cxrner_artist_profiles (
  user_id text PRIMARY KEY,
  stage_name text NOT NULL,
  project_type text DEFAULT 'solo',
  verification_status text DEFAULT 'not_verified',
  spotify_url text DEFAULT '',
  apple_music_url text DEFAULT '',
  yandex_music_url text DEFAULT '',
  vk_music_url text DEFAULT '',
  deezer_url text DEFAULT '',
  youtube_music_url text DEFAULT '',
  soundcloud_url text DEFAULT '',
  bandlink_url text DEFAULT '',
  monthly_listeners integer DEFAULT 0,
  verified_at timestamptz,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

ALTER TABLE public.cxrner_artist_profiles ADD COLUMN IF NOT EXISTS telegram_id text;
ALTER TABLE public.cxrner_artist_profiles ADD COLUMN IF NOT EXISTS display_name text DEFAULT '';
ALTER TABLE public.cxrner_artist_profiles ADD COLUMN IF NOT EXISTS bio text DEFAULT '';
ALTER TABLE public.cxrner_artist_profiles ADD COLUMN IF NOT EXISTS avatar_url text DEFAULT '';
ALTER TABLE public.cxrner_artist_profiles ADD COLUMN IF NOT EXISTS socials jsonb DEFAULT '{}'::jsonb;
ALTER TABLE public.cxrner_artist_profiles ADD COLUMN IF NOT EXISTS theme jsonb DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS public.cxrner_artist_verifications (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id text NOT NULL,
  stage_name text NOT NULL,
  method text DEFAULT 'manual',
  verification_code text,
  status text DEFAULT 'pending_review',
  dsp_links jsonb DEFAULT '{}'::jsonb,
  social_links jsonb DEFAULT '{}'::jsonb,
  evidence_links jsonb DEFAULT '[]'::jsonb,
  evidence_notes text DEFAULT '',
  contact_telegram text DEFAULT '',
  rejection_reason text,
  reviewed_by text,
  reviewed_at timestamptz,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cxrner_achievements (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id text NOT NULL,
  achievement_key text NOT NULL,
  unlocked_at timestamptz DEFAULT now(),
  UNIQUE(user_id, achievement_key)
);

CREATE TABLE IF NOT EXISTS public.cxrner_referrals (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  referrer_user_id text NOT NULL,
  referred_user_id text UNIQUE NOT NULL,
  referral_code text NOT NULL,
  status text DEFAULT 'registered',
  qualified_at timestamptz,
  reward_cxp integer DEFAULT 50,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cxrner_notifications (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id text,
  telegram_id text,
  category text DEFAULT 'system',
  type text DEFAULT 'system',
  title text NOT NULL,
  message text DEFAULT '',
  body text DEFAULT '',
  action_url text,
  is_read boolean DEFAULT false,
  read_at timestamptz,
  created_at timestamptz DEFAULT now()
);

ALTER TABLE public.cxrner_notifications ADD COLUMN IF NOT EXISTS user_id text;
ALTER TABLE public.cxrner_notifications ADD COLUMN IF NOT EXISTS telegram_id text;
ALTER TABLE public.cxrner_notifications ADD COLUMN IF NOT EXISTS category text DEFAULT 'system';
ALTER TABLE public.cxrner_notifications ADD COLUMN IF NOT EXISTS type text DEFAULT 'system';
ALTER TABLE public.cxrner_notifications ADD COLUMN IF NOT EXISTS message text DEFAULT '';
ALTER TABLE public.cxrner_notifications ADD COLUMN IF NOT EXISTS body text DEFAULT '';
ALTER TABLE public.cxrner_notifications ADD COLUMN IF NOT EXISTS read_at timestamptz;
ALTER TABLE public.cxrner_notifications ADD COLUMN IF NOT EXISTS is_read boolean DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_cxrner_notifications_user
  ON public.cxrner_notifications (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS public.cxrner_admin_audit_log (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  admin_id text NOT NULL,
  action_type text NOT NULL,
  target_type text NOT NULL,
  target_id text NOT NULL,
  details jsonb DEFAULT '{}'::jsonb,
  created_at timestamptz DEFAULT now()
);

-- =============================================================================
-- 5. ТАБЛИЦЫ TELEGRAM MINI APP, ДИСТРИБУЦИИ И CXRNER LINK
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.cxrner_forms (
  id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  telegram_id text,
  username text,
  release_type text DEFAULT 'Сингл',
  title text NOT NULL,
  subtitle text DEFAULT '',
  version text DEFAULT '',
  artists text NOT NULL,
  feat_artists text DEFAULT '',
  genre text DEFAULT '',
  release_date text DEFAULT '',
  upc text DEFAULT '',
  cover_url text DEFAULT '',
  wav_url text DEFAULT '',
  smart_link text DEFAULT '',
  yandex_music_url text DEFAULT '',
  vk_music_url text DEFAULT '',
  spotify_url text DEFAULT '',
  apple_music_url text DEFAULT '',
  youtube_music_url text DEFAULT '',
  deezer_url text DEFAULT '',
  zvuk_url text DEFAULT '',
  lyrics_info text DEFAULT '',
  explicit_info text DEFAULT 'Нет',
  full_name text DEFAULT '',
  contact_tg text DEFAULT '',
  promo_text text DEFAULT '',
  moderator_comment text DEFAULT '',
  admin_comment text DEFAULT '',
  tracks_info text DEFAULT '',
  status text DEFAULT 'pending',
  views_count integer DEFAULT 0,
  likes_count integer DEFAULT 0,
  favorites_count integer DEFAULT 0,
  comments_count integer DEFAULT 0,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cxrner_telegram_profiles (
  telegram_id text PRIMARY KEY,
  username text DEFAULT '',
  first_name text DEFAULT '',
  last_name text DEFAULT '',
  photo_url text DEFAULT '',
  artist_name text DEFAULT '',
  bio text DEFAULT '',
  country text DEFAULT '',
  city text DEFAULT '',
  genres text DEFAULT '',
  yandex_url text DEFAULT '',
  vk_url text DEFAULT '',
  spotify_url text DEFAULT '',
  apple_url text DEFAULT '',
  telegram_channel text DEFAULT '',
  instagram_url text DEFAULT '',
  tiktok_url text DEFAULT '',
  youtube_url text DEFAULT '',
  bandlink_url text DEFAULT '',
  is_verified boolean DEFAULT false,
  followers_count integer DEFAULT 0,
  role text DEFAULT 'artist',
  status text DEFAULT 'active',
  metadata jsonb DEFAULT '{}'::jsonb,
  registered_at timestamptz DEFAULT now(),
  last_seen_at timestamptz DEFAULT now(),
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

ALTER TABLE public.cxrner_telegram_profiles ADD COLUMN IF NOT EXISTS role text DEFAULT 'artist';
ALTER TABLE public.cxrner_telegram_profiles ADD COLUMN IF NOT EXISTS status text DEFAULT 'active';
ALTER TABLE public.cxrner_telegram_profiles ADD COLUMN IF NOT EXISTS metadata jsonb DEFAULT '{}'::jsonb;
ALTER TABLE public.cxrner_telegram_profiles ADD COLUMN IF NOT EXISTS registered_at timestamptz DEFAULT now();
ALTER TABLE public.cxrner_telegram_profiles ADD COLUMN IF NOT EXISTS last_seen_at timestamptz DEFAULT now();

CREATE TABLE IF NOT EXISTS public.cxrner_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  slug text UNIQUE NOT NULL,
  telegram_id text,
  owner_telegram_id text,
  owner_user_id text,
  release_id text,
  upc text DEFAULT '',
  title text NOT NULL,
  artist text NOT NULL,
  subtitle text DEFAULT '',
  release_date text DEFAULT '',
  genre text DEFAULT '',
  cover_url text DEFAULT '',
  audio_preview_url text DEFAULT '',
  theme text DEFAULT 'aurora',
  presave_enabled boolean DEFAULT false,
  platforms jsonb DEFAULT '{}'::jsonb,
  extra_links jsonb DEFAULT '[]'::jsonb,
  settings jsonb DEFAULT '{}'::jsonb,
  status text DEFAULT 'published',
  lyrics text DEFAULT '',
  views integer DEFAULT 0,
  clicks integer DEFAULT 0,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

ALTER TABLE public.cxrner_links ADD COLUMN IF NOT EXISTS telegram_id text;
ALTER TABLE public.cxrner_links ADD COLUMN IF NOT EXISTS release_id text;
ALTER TABLE public.cxrner_links ADD COLUMN IF NOT EXISTS extra_links jsonb DEFAULT '[]'::jsonb;
ALTER TABLE public.cxrner_links ADD COLUMN IF NOT EXISTS settings jsonb DEFAULT '{}'::jsonb;
ALTER TABLE public.cxrner_links ADD COLUMN IF NOT EXISTS status text DEFAULT 'published';

CREATE TABLE IF NOT EXISTS public.cxrner_link_events (
  id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  slug text NOT NULL,
  event_type text NOT NULL,
  platform text DEFAULT '',
  referrer text DEFAULT '',
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cxrner_release_engagements (
  id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  release_id text NOT NULL,
  telegram_id text NOT NULL,
  kind text DEFAULT 'like',
  liked boolean DEFAULT false,
  favorited boolean DEFAULT false,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE(release_id, telegram_id, kind)
);

ALTER TABLE public.cxrner_release_engagements ADD COLUMN IF NOT EXISTS kind text DEFAULT 'like';
ALTER TABLE public.cxrner_release_engagements ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now();

CREATE TABLE IF NOT EXISTS public.cxrner_comments (
  id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  release_id bigint NOT NULL,
  telegram_id text NOT NULL,
  author_name text DEFAULT 'Артист',
  author_username text DEFAULT '',
  author_avatar text DEFAULT '',
  text text NOT NULL,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.cxrner_news (
  id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  title text NOT NULL,
  category text DEFAULT 'ОБНОВЛЕНИЕ',
  summary text NOT NULL,
  body text DEFAULT '',
  action_url text DEFAULT '',
  created_at timestamptz DEFAULT now()
);

-- =============================================================================
-- 6. RLS ПОЛИТИКИ (БЕЗОПАСНАЯ РАБОТА И С SERVICE_ROLE, И С ANON KEY НА СЕРВЕРЕ)
-- =============================================================================
DO $$
DECLARE
  tbl text;
  pol text;
BEGIN
  FOREACH tbl IN ARRAY ARRAY[
    'cxrner_cabinet_users',
    'cxrner_cxp_wallet',
    'cxrner_cxp_transactions',
    'cxrner_drop_inventory',
    'cxrner_drop_history',
    'special_reward_claims',
    'cxrner_profiles',
    'cxrner_artist_profiles',
    'cxrner_artist_verifications',
    'cxrner_achievements',
    'cxrner_referrals',
    'cxrner_notifications',
    'cxrner_admin_audit_log',
    'cxrner_forms',
    'cxrner_telegram_profiles',
    'cxrner_links',
    'cxrner_link_events',
    'cxrner_release_engagements',
    'cxrner_comments',
    'cxrner_news'
  ]
  LOOP
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY;', tbl);
    pol := tbl || '_full_server_access';
    EXECUTE format('DROP POLICY IF EXISTS %I ON public.%I;', pol, tbl);
    EXECUTE format(
      'CREATE POLICY %I ON public.%I FOR ALL USING (true) WITH CHECK (true);',
      pol,
      tbl
    );
  END LOOP;
END $$;

-- =============================================================================
-- 7. ГАРАНТИРОВАННАЯ ИНИЦИАЛИЗАЦИЯ CEO-АККАУНТА @kazumaiq
-- =============================================================================
INSERT INTO public.cxrner_telegram_profiles (
  telegram_id,
  username,
  first_name,
  artist_name,
  bio,
  genres,
  is_verified
)
VALUES (
  '881379104',
  'kazumaiq',
  'kazumaiq',
  'kazumaiq',
  'CEO & Founder of CXRNER MUSIC Ecosystem.',
  'Electronic / Phonk / Dark Wave',
  true
)
ON CONFLICT (telegram_id) DO UPDATE SET
  is_verified = true,
  artist_name = COALESCE(NULLIF(public.cxrner_telegram_profiles.artist_name, ''), 'kazumaiq');
