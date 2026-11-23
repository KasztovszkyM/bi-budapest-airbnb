import argparse
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text

from .etl import build_dwh, ingest_calendar, ingest_listings, script
from .source import inspect
from .external import ingest_osm, ingest_holidays

LOCAL_JOBS = ("ingest_listings", "ingest_calendar", "build_dwh")
JOBS = (*LOCAL_JOBS[:2], "ingest_osm", "ingest_holidays", "build_dwh")


def engine(database="airbnb"):
    load_dotenv(Path(".env"))
    return create_engine(URL.create("postgresql+psycopg", username="airbnb",
                         password=os.environ["POSTGRES_PASSWORD"], host="127.0.0.1",
                         port=int(os.getenv("POSTGRES_PORT", 5432)), database=database),
                         hide_parameters=True)


def main():
    parser = argparse.ArgumentParser(description="Budapesti Airbnb-minta feldolgozása")
    parser.add_argument("job", choices=("inspect", "migrate", *JOBS, "run-all"))
    parser.add_argument("--input-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--snapshot-date", type=date.fromisoformat)
    size = parser.add_mutually_exclusive_group()
    size.add_argument("--limit-listings", type=int, default=100)
    size.add_argument("--full", action="store_true", help="A teljes snapshot betöltése")
    parser.add_argument("--with-external", action="store_true", help="Mind az öt job futtatása")
    parser.add_argument("--offline", action="store_true", help="Külső adat csak a helyi cache-ből")
    parser.add_argument("--cache-dir", type=Path, default=Path("data/cache"))
    args = parser.parse_args()
    if args.job not in ("inspect", "migrate") and not args.snapshot_date:
        parser.error("A betöltéshez kötelező a --snapshot-date.")
    if not 1 <= args.limit_listings <= 500:
        parser.error("A mintaméret 1 és 500 közé essen.")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run = str(uuid4())

    def log(job, state, **info):
        logging.info(json.dumps(dict(run_id=run, snapshot=str(args.snapshot_date), job=job,
                                     status=state, **info), ensure_ascii=False))
    job = args.job
    try:
        log(job, "start")
        if job == "inspect":
            print(json.dumps(inspect(args.input_dir), ensure_ascii=False, indent=2))
        else:
            with engine().begin() as conn:
                conn.execute(text("SELECT pg_advisory_xact_lock(260921)"))
                script(conn, "001_schema.sql")
                script(conn, "003_full.sql")
                pipeline = JOBS if args.with_external else LOCAL_JOBS
                for job in pipeline if args.job == "run-all" else [args.job]:
                    started_at = datetime.now(timezone.utc)
                    if args.job == "run-all":
                        log(job, "start")
                    if job == "ingest_listings":
                        counts = ingest_listings(conn, args.input_dir, args.snapshot_date,
                                                 None if args.full else args.limit_listings)
                    elif job == "ingest_calendar":
                        counts = ingest_calendar(conn, args.input_dir, args.snapshot_date)
                    elif job == "ingest_osm":
                        counts = ingest_osm(conn, args.snapshot_date, args.cache_dir, args.offline)
                    elif job == "ingest_holidays":
                        counts = ingest_holidays(conn, args.snapshot_date, args.cache_dir, args.offline)
                    elif job == "build_dwh":
                        counts = build_dwh(conn, args.snapshot_date)
                    else:
                        continue
                    batch_id = conn.scalar(text("SELECT batch_key FROM raw.batch WHERE source_snapshot_date=:day"),
                                           {"day": args.snapshot_date})
                    conn.execute(text("""INSERT INTO raw.job_run (run_id,job,batch_key,started_at,records)
                        VALUES (:run,:job,:batch,:started,cast(:records AS jsonb))"""),
                        dict(run=run, job=job, batch=batch_id, started=started_at, records=json.dumps(counts)))
                    log(job, "processed", batch_id=batch_id, records=counts)
        log(args.job, "committed" if args.job != "inspect" else "done")
    except Exception as exc:
        log(job, "failed", error=type(exc).__name__, detail=str(exc).splitlines()[0])
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()

