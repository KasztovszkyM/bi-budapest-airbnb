CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS raw.batch (
 batch_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 source_snapshot_date date NOT NULL UNIQUE,
 source_name text NOT NULL DEFAULT 'Inside Airbnb Budapest',
 scrape_id text NOT NULL UNIQUE,
 listings_sha256 text NOT NULL,
 calendar_sha256 text,
 sample_limit integer NOT NULL CHECK (sample_limit BETWEEN 1 AND 500),
 loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.listing (
 batch_key bigint NOT NULL REFERENCES raw.batch,
 listing_id bigint NOT NULL CHECK (listing_id > 0),
 payload jsonb NOT NULL,
 quote_status text NOT NULL,
 PRIMARY KEY (batch_key, listing_id)
);

CREATE TABLE IF NOT EXISTS raw.calendar (
 batch_key bigint NOT NULL,
 listing_id bigint NOT NULL,
 calendar_date text NOT NULL,
 available text NOT NULL CHECK (available IN ('t','f')),
 minimum_nights text NOT NULL,
 maximum_nights text NOT NULL,
 PRIMARY KEY (batch_key, listing_id, calendar_date),
 FOREIGN KEY (batch_key, listing_id) REFERENCES raw.listing
);

CREATE OR REPLACE VIEW stage.listing AS
SELECT batch_key, listing_id, payload->>'neighbourhood_cleansed' AS district,
 payload->>'room_type' AS room_type, nullif(payload->>'accommodates','')::integer AS accommodates,
 nullif(payload->>'bedrooms','')::numeric AS bedrooms, nullif(payload->>'bathrooms','')::numeric AS bathrooms,
 nullif(payload->>'latitude','')::numeric AS latitude, nullif(payload->>'longitude','')::numeric AS longitude,
 nullif(payload->>'price_quote_checkin_date','')::date AS checkin,
 nullif(payload->>'price_quote_checkout_date','')::date AS checkout,
 CASE WHEN quote_status='ok' THEN (payload->>'price_quote_price_per_night')::numeric END AS price_huf,
 quote_status
FROM raw.listing;

CREATE OR REPLACE VIEW stage.calendar AS
SELECT batch_key, listing_id, calendar_date::date AS day,
 (available='t')::integer AS available_flag,
 nullif(minimum_nights,'')::integer AS minimum_nights,
 nullif(maximum_nights,'')::integer AS maximum_nights
FROM raw.calendar;

CREATE TABLE IF NOT EXISTS dwh.dim_snapshot (
 snapshot_key bigint PRIMARY KEY REFERENCES raw.batch,
 source_snapshot_date date NOT NULL, source_name text NOT NULL, scrape_id text NOT NULL,
 loaded_at timestamptz NOT NULL, listings_sha256 text NOT NULL, calendar_sha256 text NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_listing (
 listing_key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 listing_id bigint NOT NULL UNIQUE, district text, room_type text,
 accommodates integer CHECK (accommodates >= 0), bedrooms numeric, bathrooms numeric,
 latitude numeric CHECK (latitude BETWEEN -90 AND 90),
 longitude numeric CHECK (longitude BETWEEN -180 AND 180)
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
 date_key date PRIMARY KEY,
 year integer GENERATED ALWAYS AS (extract(year FROM date_key)) STORED,
 month integer GENERATED ALWAYS AS (extract(month FROM date_key)) STORED,
 day integer GENERATED ALWAYS AS (extract(day FROM date_key)) STORED,
 iso_weekday integer GENERATED ALWAYS AS (extract(isodow FROM date_key)) STORED,
 is_weekend boolean GENERATED ALWAYS AS (extract(isodow FROM date_key)>=6) STORED
);

CREATE TABLE IF NOT EXISTS dwh.fact_daily_availability (
 listing_key bigint REFERENCES dwh.dim_listing, date_key date REFERENCES dwh.dim_date,
 snapshot_key bigint REFERENCES dwh.dim_snapshot, available_flag smallint NOT NULL CHECK (available_flag IN (0,1)),
 minimum_nights integer CHECK (minimum_nights >= 0), maximum_nights integer CHECK (maximum_nights >= minimum_nights AND maximum_nights >= 0),
 PRIMARY KEY (listing_key,date_key,snapshot_key)
);

CREATE TABLE IF NOT EXISTS dwh.fact_price_quote (
 listing_key bigint REFERENCES dwh.dim_listing, snapshot_key bigint REFERENCES dwh.dim_snapshot,
 checkin_date_key date REFERENCES dwh.dim_date, checkout_date_key date REFERENCES dwh.dim_date,
 quoted_nightly_price numeric NOT NULL CHECK (quoted_nightly_price > 0),
 CHECK (checkout_date_key > checkin_date_key),
 PRIMARY KEY (listing_key,snapshot_key)
);

CREATE OR REPLACE VIEW dwh.daily_availability AS
SELECT s.source_snapshot_date, f.date_key, l.district, l.room_type, count(*) AS calendar_days,
 sum(f.available_flag) AS available_days, avg(f.available_flag) AS availability_ratio
FROM dwh.fact_daily_availability f JOIN dwh.dim_snapshot s USING (snapshot_key)
JOIN dwh.dim_listing l USING (listing_key)
GROUP BY s.source_snapshot_date,f.date_key,l.district,l.room_type;

