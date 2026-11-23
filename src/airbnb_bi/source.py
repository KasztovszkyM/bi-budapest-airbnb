"""Az Inside Airbnb forrásfájlok olvasása és profilozása."""
import csv
import gzip
import hashlib
import json
import re
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import date
from decimal import Decimal, InvalidOperation

LISTING = ("id scrape_id neighbourhood_cleansed room_type accommodates bedrooms bathrooms "
           "latitude longitude price price_quote_checkin_date price_quote_checkout_date "
           "price_quote_price_per_night price_quote_raw").split()
CALENDAR = "listing_id date available minimum_nights maximum_nights".split()


@contextmanager
def reader(folder, name):
    with gzip.open(folder / f"{name}.csv.gz", "rt", newline="", encoding="utf-8-sig") as file:
        rows = csv.DictReader(file)
        required = LISTING if name == "listings" else CALENDAR
        missing = set(required).difference(rows.fieldnames or ())
        if missing or len(set(rows.fieldnames or ())) != len(rows.fieldnames or ()):
            raise ValueError(f"Hibás {name} fejléc, hiányzó mezők: {sorted(missing)}")
        yield rows


def digest(folder, name):
    with (folder / f"{name}.csv.gz").open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def quote_status(row):
    value = row["price_quote_price_per_night"]
    if not value:
        return "missing"
    try:
        price = Decimal(value)
        if not price.is_finite() or price <= 0:
            return "invalid_price"
    except InvalidOperation:
        return "invalid_price"
    # Konzervatív mintabetöltési küszöb, nem piaci becslés.
    if price > 1_000_000:
        return "review_price"
    try:
        quote = json.loads(row["price_quote_raw"])["quote"]
        currency = quote.get("currency")
        labels = [item["price_string"].strip() for item in quote.get("raw_price_line_items", [])]
        if currency not in (None, "", "HUF"):
            return "unverified_currency"
        if any(not re.fullmatch(r"-?(Ft|HUF)\s*\d[\d,.]*", label) for label in labels):
            return "unverified_currency"
        return "ok" if currency == "HUF" or labels else "unverified_currency"
    except (TypeError, ValueError, AttributeError, KeyError):
        return "unverified_currency"


def inspect(folder):
    result = {}
    ids = set()
    # Dátumonkénti tömör bitpozíciók, ingatlanonként egy bitmaszk.
    days = {}
    masks = defaultdict(int)
    for name in ("listings", "calendar"):
        with reader(folder, name) as rows:
            fields = rows.fieldnames
            missing = dict.fromkeys(fields, 0)
            stats = Counter()
            quotes, availability, scrapes = Counter(), Counter(), Counter()
            for row in rows:
                stats["rows"] += 1
                for key, value in row.items():
                    if not value:
                        missing[key] += 1
                if name == "listings":
                    key = row["id"]
                    if key in ids:
                        stats["duplicate_keys"] += 1
                    ids.add(key)
                    status = quote_status(row)
                    quotes[status] += 1
                    scrapes[row["scrape_id"]] += 1
                    if status == "ok" and row["price"].startswith("$"):
                        stats["dollar_display_verified_huf"] += 1
                else:
                    key, day = row["listing_id"], row["date"]
                    if day not in days:
                        date.fromisoformat(day)
                        days[day] = 1 << len(days)
                    if masks[key] & days[day]:
                        stats["duplicate_keys"] += 1
                    masks[key] |= days[day]
                    if key not in ids:
                        stats["unknown_listing"] += 1
                    availability[row["available"]] += 1
            report = dict(stats, duplicate_keys=stats["duplicate_keys"], columns=len(fields),
                          missing=missing, price_fields=[f for f in fields if "price" in f])
            if name == "listings":
                report.update(distinct_ids=len(ids), quotes=quotes, scrape_ids=scrapes)
            else:
                report.update(distinct_ids=len(masks), unknown_listing=stats["unknown_listing"],
                              available=availability, daily_price_present="price" in fields,
                              date_range=[min(days, default=None), max(days, default=None)])
            result[name] = report
    return result

