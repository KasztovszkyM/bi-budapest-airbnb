"""A külső forrásválaszok beszerzése, ellenőrzése és verziózása."""
import hashlib
import json
import os
import tempfile
import time
from datetime import date, datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from sqlalchemy import text

from .etl import batch_for

OVERPASS = "https://overpass-api.de/api/interpreter"
QUERY = """[out:json][timeout:60];
area["ISO3166-2"="HU-BU"]["boundary"="administrative"]->.city;
(nwr["railway"="station"]["station"="subway"](area.city);
 nwr["tourism"~"^(attraction|museum|gallery|viewpoint)$"](area.city););
out center;"""


def download(url, data=None):
    request = Request(url, data=data, headers={"User-Agent": "Budapest-Airbnb-BI/0.1"})
    for attempt in range(3):
        try:
            with urlopen(request, timeout=90) as response:
                return response.read()
        except (HTTPError, URLError, TimeoutError) as exc:
            if isinstance(exc, HTTPError) and exc.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == 2:
                raise RuntimeError(f"A külső forrás három kísérlettel sem elérhető: {url}") from exc
            time.sleep(2 ** attempt)


def validate_osm(payload):
    if not isinstance(payload, dict) or payload.get("remark") or not payload.get("elements"):
        raise ValueError("Hiányzó vagy részleges Overpass-válasz.")
    for item in payload["elements"]:
        point = item.get("center", item)
        if (item["type"] not in ("node", "way", "relation") or not isinstance(item["id"], int)
                or not -90 <= point["lat"] <= 90 or not -180 <= point["lon"] <= 180):
            raise ValueError("Hibás OSM-objektum.")
    return len(payload["elements"])


def validate_holidays(payload, year):
    if not isinstance(payload, list) or not payload:
        raise ValueError("Hiányos ünnepnaptár.")
    for item in payload:
        if (item["countryCode"] != "HU" or date.fromisoformat(item["date"]).year != year
                or not item["localName"] or not isinstance(item["types"], list)):
            raise ValueError("Hibás magyar ünnepnaptár.")
    return len(payload)


def capture(conn, key, folder, name, url, data, validate, offline):
    path = folder / f"{name}.json"
    previous = conn.execute(text("""SELECT sha256 FROM raw.external_source
        WHERE batch_key=:key AND source_key=:name"""), dict(key=key, name=name)).scalar_one_or_none()
    cached = path.is_file()
    if not cached and (offline or previous):
        raise ValueError(f"Hiányzó cache-fájl: {path}")
    body = path.read_bytes() if cached else download(url, data)
    sha = hashlib.sha256(body).hexdigest()
    if previous and previous != sha:
        raise ValueError(f"A regisztrált cache megváltozott: {name}")
    count = validate(json.loads(body))
    if not cached:
        folder.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=folder, delete=False) as tmp:
            tmp.write(body)
        os.replace(tmp.name, path)
    conn.execute(text("""INSERT INTO raw.external_source
        (batch_key,source_key,url,request_body,sha256,records,cache_time)
        VALUES (:key,:name,:url,:request,:sha,:count,:stamp) ON CONFLICT DO NOTHING"""),
        dict(key=key, name=name, url=url, request=data.decode() if data else None, sha=sha,
             count=count, stamp=datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)))
    return count


def ingest_osm(conn, snapshot, cache, offline):
    key = batch_for(conn, snapshot)["batch_key"]
    return capture(conn, key, cache / snapshot.isoformat(), "osm-budapest-v1", OVERPASS,
                   urlencode({"data": QUERY}).encode(), validate_osm, offline)


def ingest_holidays(conn, snapshot, cache, offline):
    batch = batch_for(conn, snapshot)
    if not batch["calendar_sha256"] or batch["calendar_pending"]:
        raise ValueError("A dátumtartományhoz kész naptárbetöltés szükséges.")
    key = batch["batch_key"]
    years = conn.execute(text("""SELECT DISTINCT extract(year FROM day)::integer FROM (
        SELECT DISTINCT day FROM stage.calendar WHERE batch_key=:k
        UNION SELECT checkin FROM stage.listing WHERE batch_key=:k
        UNION SELECT checkout FROM stage.listing WHERE batch_key=:k) dates WHERE day IS NOT NULL
        ORDER BY 1"""), {"k": key}).scalars().all()
    counts = {}
    for year in years:
        counts[year] = capture(conn, key, cache / snapshot.isoformat(), f"holidays-HU-{year}",
                               f"https://date.nager.at/api/v3/PublicHolidays/{year}/HU", None,
                               lambda payload: validate_holidays(payload, year), offline)
    return counts

