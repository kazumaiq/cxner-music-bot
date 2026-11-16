-- =============================================================================
-- CXRNER MUSIC · COLD RUSH COMPILATION SUBMISSIONS (SUPABASE MIGRATION)
-- Идемпотентная таблица для приёма и модерации заявок в сборник COLD RUSH.
-- Выполните этот скрипт в Supabase Dashboard -> SQL Editor -> Run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.cxrner_cold_rush_submissions (
  id text PRIMARY KEY,
  campaign_code text NOT NULL DEFAULT 'COLD_RUSH_001',
  telegram_id text NOT NULL,
  telegram_username text NOT NULL DEFAULT '',
  first_name text NOT NULL DEFAULT '',
  artist_name text NOT NULL,
  track_name text NOT NULL,
  genre text NOT NULL,
  audio_source_type text NOT NULL DEFAULT 'url',
  audio_file_id text NOT NULL DEFAULT '',
  audio_file_name text NOT NULL DEFAULT '',
  audio_url text NOT NULL DEFAULT '',
  social_url text NOT NULL DEFAULT '',
  rights_confirmed boolean NOT NULL DEFAULT true,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
  moderation_chat_id text NOT NULL DEFAULT '',
  moderation_message_id bigint,
  moderator_id text NOT NULL DEFAULT '',
  moderator_username text NOT NULL DEFAULT '',
  moderator_note text NOT NULL DEFAULT '',
  decided_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cold_rush_telegram_id
  ON public.cxrner_cold_rush_submissions (telegram_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_cold_rush_campaign_status
  ON public.cxrner_cold_rush_submissions (campaign_code, status, created_at DESC);

ALTER TABLE public.cxrner_cold_rush_submissions ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "cold_rush_service_role_all" ON public.cxrner_cold_rush_submissions;
CREATE POLICY "cold_rush_service_role_all"
  ON public.cxrner_cold_rush_submissions
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
