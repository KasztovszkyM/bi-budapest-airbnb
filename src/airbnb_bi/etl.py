from datetime import date
from itertools import chain, islice
from pathlib import Path

from sqlalchemy import text
from psycopg.types.json import Jsonb

from .source import LISTING, digest, quote_status, reader


def script(conn, filename, **params):
    sql = (Path(__file__).parent / "sql" / filename).read_text(encoding="utf8")
    for statement in sql.split(";"):
        if statement.strip():
            conn.execute(text(statement), params)


def batch_for(conn, snapshot):
    batch = conn.execute(text("SELECT * FROM raw.batch WHERE source_snapshot_date=:day"),
                         {"day": snapshot}).mappings().one_or_none()
    if batch is None:
        raise ValueError("Először töltsd be a listings fájlt.")
    return batch


def ingest_listings(conn, folder, snapshot, limit):
    checksum = digest(folder, "listings")
    previous = conn.execute(text("SELECT * FROM raw.batch")).mappings().first()
    existing = set()
    if previous:
        if (previous["listings_sha256"], previous["source_snapshot_date"]) != (checksum, snapshot):
            raise ValueError("A forráshash vagy a snapshot eltér a tárolt verziótól.")
        batch = previous["batch_key"]
        if previous["sample_limit"] == limit:
            return conn.scalar(text("SELECT count(*) FROM raw.listing WHERE batch_key=:k"), {"k": batch})
        if limit is not None:
            raise ValueError("A meglévő minta csak teljes betöltéssé bővíthető (--full).")
        existing = set(conn.execute(text("SELECT listing_id FROM raw.listing WHERE batch_key=:k"),
                                    {"k": batch}).scalars())
        conn.execute(text("UPDATE raw.batch SET sample_limit=NULL, calendar_pending=true WHERE batch_key=:k"),
                     {"k": batch})
    seen = set()
    with reader(folder, "listings") as rows:
        sample = islice(rows, limit)
        first = next(sample, None)
        if first is None or not first["scrape_id"].startswith(snapshot.strftime("%Y%m%d")):
            raise ValueError("Hiányzó adat vagy hibás snapshot dátum.")
        if not previous:
            batch = conn.scalar(text("""INSERT INTO raw.batch
                (source_snapshot_date,listings_sha256,scrape_id,sample_limit)
                VALUES (:day,:sha,:scrape,:lim) RETURNING batch_key"""),
                dict(day=snapshot, sha=checksum, scrape=first["scrape_id"], lim=limit))
        with conn.connection.driver_connection.cursor().copy("COPY raw.listing FROM STDIN") as copy:
            for row in chain([first], sample):
                listing = int(row["id"])
                if listing in seen or row["scrape_id"] != first["scrape_id"]:
                    raise ValueError("Ismétlődő listing_id vagy eltérő scrape_id.")
                seen.add(listing)
                if listing not in existing:
                    copy.write_row((batch, listing, Jsonb({k: row[k] for k in LISTING}), quote_status(row)))
    return len(seen)


def ingest_calendar(conn, folder, snapshot):
    batch = batch_for(conn, snapshot)
    key = batch["batch_key"]
    sha = digest(folder, "calendar")
    if batch["calendar_sha256"]:
        if sha != batch["calendar_sha256"]:
            raise ValueError("Megváltozott a naptárfájl.")
        if not batch["calendar_pending"]:
            return conn.scalar(text("SELECT count(*) FROM raw.calendar WHERE batch_key=:k"), {"k": key})
    selected = {str(i) for i in conn.execute(text("SELECT listing_id FROM raw.listing WHERE batch_key=:k"),
                                             {"k": key}).scalars()}
    existing = set(conn.execute(text("SELECT DISTINCT listing_id::text FROM raw.calendar WHERE batch_key=:k"),
                                {"k": key}).scalars()) if batch["calendar_pending"] else set()
    seen = set()
    count = 0
    with reader(folder, "calendar") as rows, conn.connection.driver_connection.cursor().copy(
            "COPY raw.calendar FROM STDIN") as copy:
        for row in rows:
            listing = row["listing_id"]
            if listing not in selected and batch["sample_limit"] is None:
                raise ValueError("Ismeretlen listing_id a teljes naptárban.")
            if listing in existing:
                seen.add(listing)
                count += 1
                continue
            if listing in selected:
                day = date.fromisoformat(row["date"])
                if day.isoformat() != row["date"]:
                    raise ValueError("Nem ISO naptári dátum.")
                copy.write_row((key, listing, row["date"], row["available"], row["minimum_nights"], row["maximum_nights"]))
                seen.add(listing)
                count += 1
    if seen != selected:
        raise ValueError("A minta egy vagy több ingatlanához hiányzik a naptár.")
    conn.execute(text("UPDATE raw.batch SET calendar_sha256=:sha, calendar_pending=false WHERE batch_key=:k"),
                 dict(sha=sha,k=key))
    return count


def build_dwh(conn, snapshot):
    batch = batch_for(conn, snapshot)
    if not batch["calendar_sha256"] or batch["calendar_pending"]:
        raise ValueError("Először töltsd be a naptárt.")
    key = batch["batch_key"]
    script(conn, "002_build.sql", batch=key)
    params = {"k": key}
    raw_days = conn.execute(text("""SELECT count(*),count(*) FILTER (WHERE available='t')
        FROM raw.calendar WHERE batch_key=:k"""), params).one()
    fact_days = conn.execute(text("""SELECT count(*),sum(available_flag)
        FROM dwh.fact_daily_availability WHERE snapshot_key=:k"""), params).one()
    accepted, excluded = conn.execute(text("""SELECT count(*) FILTER (WHERE quote_status='ok'),
        count(*) FILTER (WHERE quote_status<>'ok') FROM stage.listing WHERE batch_key=:k"""), params).one()
    quotes = conn.scalar(text("SELECT count(*) FROM dwh.fact_price_quote WHERE snapshot_key=:k"), params)
    if raw_days != fact_days or accepted != quotes:
        raise ValueError("Eltér a RAW és a DWH sorszáma vagy az elérhető napok száma.")
    return dict(availability=fact_days[0], quotes=quotes, excluded_quotes=excluded)

