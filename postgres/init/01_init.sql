-- génère gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.events (
  event_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id    integer,
  event_type text NOT NULL,
  price      numeric(10,2),
  ts         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_ts ON public.events(ts);

-- pas de IF NOT EXISTS sur VIEW en Postgres -> utiliser OR REPLACE
CREATE OR REPLACE VIEW public.revenue_per_minute AS
SELECT
  (ts AT TIME ZONE 'UTC')::date               AS day_bucket,
  date_trunc('minute', ts)                    AS window_start,
  date_trunc('minute', ts) + interval '1 min' AS window_end,
  SUM(price) FILTER (WHERE event_type='purchase') AS total_revenue,
  COUNT(*) FILTER (WHERE event_type='purchase')   AS purchase_count
FROM public.events
GROUP BY 1,2,3
ORDER BY window_start DESC;

