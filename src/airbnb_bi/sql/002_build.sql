INSERT INTO dwh.dim_snapshot
SELECT batch_key,source_snapshot_date,source_name,scrape_id,loaded_at,listings_sha256,calendar_sha256
FROM raw.batch WHERE batch_key=:batch ON CONFLICT DO NOTHING;

INSERT INTO dwh.dim_listing (listing_id,district,room_type,accommodates,bedrooms,bathrooms,latitude,longitude)
SELECT listing_id,district,room_type,accommodates,bedrooms,bathrooms,latitude,longitude
FROM stage.listing WHERE batch_key=:batch ON CONFLICT (listing_id) DO NOTHING;

INSERT INTO dwh.dim_date (date_key)
SELECT day FROM stage.calendar WHERE batch_key=:batch
UNION SELECT checkin FROM stage.listing WHERE batch_key=:batch AND checkin IS NOT NULL
UNION SELECT checkout FROM stage.listing WHERE batch_key=:batch AND checkout IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO dwh.fact_daily_availability
SELECT l.listing_key,c.day,c.batch_key,c.available_flag,c.minimum_nights,c.maximum_nights
FROM stage.calendar c JOIN dwh.dim_listing l USING (listing_id)
WHERE c.batch_key=:batch AND NOT EXISTS (
 SELECT 1 FROM dwh.fact_daily_availability f
 WHERE f.snapshot_key=c.batch_key AND f.listing_key=l.listing_key AND f.date_key=c.day
)
ON CONFLICT DO NOTHING;

INSERT INTO dwh.fact_price_quote
SELECT listing_key,batch_key,checkin,checkout,price_huf
FROM stage.listing JOIN dwh.dim_listing USING (listing_id) WHERE batch_key=:batch AND quote_status='ok'
ON CONFLICT DO NOTHING;

