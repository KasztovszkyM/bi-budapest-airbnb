ALTER TABLE raw.batch ALTER COLUMN sample_limit DROP NOT NULL;
ALTER TABLE raw.batch ADD COLUMN IF NOT EXISTS calendar_pending boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS raw.external_source (
 batch_key bigint REFERENCES raw.batch, source_key text NOT NULL, url text NOT NULL,
 request_body text, sha256 text NOT NULL, records integer NOT NULL,
 cache_time timestamptz NOT NULL, registered_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (batch_key,source_key)
);

CREATE TABLE IF NOT EXISTS raw.job_run (
 run_id uuid NOT NULL, job text NOT NULL, batch_key bigint NOT NULL REFERENCES raw.batch,
 started_at timestamptz NOT NULL, finished_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 records jsonb NOT NULL, PRIMARY KEY (run_id,job)
);

CREATE OR REPLACE VIEW raw.source_lineage AS
SELECT batch_key,source_snapshot_date,source_name,loaded_at,
 'listings.csv.gz' AS file_name,listings_sha256 AS sha256,sample_limit
FROM raw.batch
UNION ALL
SELECT batch_key,source_snapshot_date,source_name,loaded_at,
 'calendar.csv.gz',calendar_sha256,sample_limit
FROM raw.batch WHERE calendar_sha256 IS NOT NULL AND NOT calendar_pending;

CREATE OR REPLACE VIEW dwh.price_overview AS
SELECT s.source_snapshot_date,l.district,l.room_type,l.accommodates,
 count(*) AS listings,count(f.quoted_nightly_price) AS quotes,
 percentile_cont(0.25) WITHIN GROUP (ORDER BY f.quoted_nightly_price) AS q1_huf,
 percentile_cont(0.5) WITHIN GROUP (ORDER BY f.quoted_nightly_price) AS median_huf,
 percentile_cont(0.75) WITHIN GROUP (ORDER BY f.quoted_nightly_price) AS q3_huf
FROM (SELECT DISTINCT snapshot_key,listing_key FROM dwh.fact_daily_availability) a
JOIN dwh.dim_listing l USING (listing_key)
JOIN dwh.dim_snapshot s USING (snapshot_key)
LEFT JOIN dwh.fact_price_quote f USING (listing_key,snapshot_key)
GROUP BY s.source_snapshot_date,l.district,l.room_type,l.accommodates;

CREATE OR REPLACE VIEW dwh.monthly_availability AS
SELECT source_snapshot_date,date_trunc('month',date_key)::date AS month_start,district,room_type,
 sum(calendar_days) AS calendar_days,sum(available_days) AS available_days,
 sum(available_days)::numeric/sum(calendar_days) AS availability_ratio
FROM dwh.daily_availability
GROUP BY source_snapshot_date,date_trunc('month',date_key),district,room_type;

