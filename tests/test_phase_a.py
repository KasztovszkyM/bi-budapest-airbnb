import csv
import gzip
import json
import os
from datetime import date

import pytest
from sqlalchemy import text
from psycopg.errors import UniqueViolation

from airbnb_bi.cli import engine
from airbnb_bi.etl import build_dwh, ingest_calendar, ingest_listings, script
from airbnb_bi.source import CALENDAR, LISTING, inspect, quote_status
from airbnb_bi import external

SNAPSHOT = date(2026, 9, 21)
HUF = json.dumps({"quote": {"currency": None, "raw_price_line_items": [{"price_string": "Ft 40,000"}]}})


def write_file(folder, name, columns, rows):
    with gzip.open(folder / f"{name}.csv.gz", "wt", newline="", encoding="utf8") as stream:
        writer = csv.DictWriter(stream, columns)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def inputs(tmp_path):
    listing = dict(id="1", scrape_id="20260921222910", price="$40,000", price_quote_raw=HUF,
                   price_quote_price_per_night="40000", price_quote_checkin_date="2026-12-01",
                   price_quote_checkout_date="2026-12-04")
    write_file(tmp_path, "listings", LISTING, [listing, dict(listing, id="2", price_quote_price_per_night="")])
    days = [dict(listing_id="1", date="2026-10-01", available="t"),
            dict(listing_id="1", date="2026-10-02", available="f"),
            dict(listing_id="2", date="2026-10-01", available="f")]
    write_file(tmp_path, "calendar", CALENDAR, days)
    return tmp_path, days


@pytest.mark.parametrize("amount,raw,status", [
    ("123", HUF, "ok"), ("123", HUF.replace("Ft", "-Ft"), "ok"),
    ("", HUF, "missing"), ("-1", HUF, "invalid_price"),
    ("nan", HUF, "invalid_price"), ("1000001", HUF, "review_price"),
    ("10", "{}", "unverified_currency"), ("10", HUF.replace("null", '"USD"'), "unverified_currency"),
    ("10", HUF.replace("Ft", "$"), "unverified_currency")])
def test_currency(amount, raw, status):
    assert quote_status(dict(price_quote_price_per_night=amount, price_quote_raw=raw)) == status


def test_inspection(inputs):
    folder, days = inputs
    write_file(folder, "calendar", CALENDAR, days + [days[0], dict(days[0], listing_id="99")])
    result = inspect(folder)
    assert result["listings"]["missing"]["price_quote_price_per_night"] == 1
    assert result["listings"]["dollar_display_verified_huf"] == 1
    calendar = result["calendar"]
    assert (calendar["duplicate_keys"], calendar["unknown_listing"], calendar["rows"]) == (1, 1, 5)
    assert not calendar["daily_price_present"]
    assert calendar["date_range"] == ["2026-10-01", "2026-10-02"]


@pytest.mark.skipif(os.getenv("AIRBNB_TEST_POSTGRES") != "1", reason="külön PostgreSQL-próba")
def test_postgres(inputs):
    folder, days = inputs
    with engine("airbnb_test").connect() as conn:
        # Külön, friss tesztadatbázis szükséges. Minden módosítást visszagörgetünk.
        script(conn, "001_schema.sql")
        script(conn, "003_full.sql")
        assert conn.scalar(text("SELECT count(*) FROM raw.batch")) == 0
        ingest_listings(conn, folder, SNAPSHOT, 2)
        write_file(folder, "calendar", CALENDAR, days + [days[0]])
        with pytest.raises(UniqueViolation), conn.begin_nested():
            ingest_calendar(conn, folder, SNAPSHOT)
        assert conn.scalar(text("SELECT count(*) FROM raw.calendar")) == 0
        write_file(folder, "calendar", CALENDAR, days)
        for _ in range(2):
            assert ingest_listings(conn, folder, SNAPSHOT, 2) == 2
            assert ingest_calendar(conn, folder, SNAPSHOT) == 3
            assert build_dwh(conn, SNAPSHOT) == dict(availability=3, quotes=1, excluded_quotes=1)
            assert conn.scalar(text("SELECT count(*) FROM dwh.dim_date")) == 4
            assert conn.scalar(text("SELECT sum(available_days) FROM dwh.daily_availability")) == 1
        conn.execute(text("UPDATE dwh.fact_daily_availability SET available_flag=0"))
        with pytest.raises(ValueError, match="elérhető napok"):
            build_dwh(conn, SNAPSHOT)


@pytest.mark.skipif(os.getenv("AIRBNB_TEST_POSTGRES") != "1", reason="külön PostgreSQL-próba")
def test_full_promotion_and_external_cache(inputs, monkeypatch):
    folder, days = inputs
    osm = {"elements": [{"type": "node", "id": 1, "lat": 47.5, "lon": 19.1}]}
    holidays = [{"date": "2026-01-01", "countryCode": "HU", "localName": "Újév", "types": ["Public"]}]
    calls = []

    def download(url, data):
        calls.append(url)
        return json.dumps(osm if data else holidays).encode()

    monkeypatch.setattr(external, "download", download)
    with engine("airbnb_test").connect() as conn:
        script(conn, "001_schema.sql")
        script(conn, "003_full.sql")
        ingest_listings(conn, folder, SNAPSHOT, 1)
        assert ingest_calendar(conn, folder, SNAPSHOT) == 2
        build_dwh(conn, SNAPSHOT)
        assert ingest_listings(conn, folder, SNAPSHOT, None) == 2
        with pytest.raises(ValueError):
            build_dwh(conn, SNAPSHOT)
        assert ingest_calendar(conn, folder, SNAPSHOT) == 3
        for offline in (False, True):
            assert external.ingest_osm(conn, SNAPSHOT, folder, offline) == 1
            assert external.ingest_holidays(conn, SNAPSHOT, folder, offline) == {2026: 1}
            assert build_dwh(conn, SNAPSHOT)["availability"] == 3
        assert len(calls) == 2
        assert conn.execute(text("SELECT sum(listings),sum(quotes) FROM dwh.price_overview")).one() == (2, 1)
        assert conn.scalar(text("SELECT sum(calendar_days) FROM dwh.monthly_availability")) == 3
        assert conn.scalar(text("SELECT count(*) FROM raw.external_source")) == 2
        (folder / SNAPSHOT.isoformat() / "osm-budapest-v1.json").write_bytes(b"{}")
        with pytest.raises(ValueError, match="megváltozott"):
            external.ingest_osm(conn, SNAPSHOT, folder, True)


@pytest.mark.skipif(os.getenv("AIRBNB_TEST_POSTGRES") != "1", reason="külön PostgreSQL-próba")
def test_full_rejects_orphan(inputs):
    folder, days = inputs
    with engine("airbnb_test").connect() as conn:
        script(conn, "001_schema.sql")
        script(conn, "003_full.sql")
        ingest_listings(conn, folder, SNAPSHOT, None)
        write_file(folder, "calendar", CALENDAR, days + [dict(days[0], listing_id="3")])
        with pytest.raises(ValueError, match="Ismeretlen"), conn.begin_nested():
            ingest_calendar(conn, folder, SNAPSHOT)
        assert conn.scalar(text("SELECT count(*) FROM raw.calendar")) == 0


def test_retry(monkeypatch):
    from urllib.error import HTTPError
    from io import BytesIO

    attempts = []
    def request(*args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise HTTPError("https://example.invalid", 503, "unavailable", {}, None)
        return BytesIO(b"ok")

    monkeypatch.setattr(external, "urlopen", request)
    monkeypatch.setattr(external.time, "sleep", lambda seconds: None)
    assert external.download("https://example.invalid") == b"ok"
    assert len(attempts) == 2

