"""GTFS Ingestion Service — Downloads, parses, and loads TPBI GTFS data into PostgreSQL."""

from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

from config.settings import settings

logger = logging.getLogger(__name__)

# Ordered by foreign-key dependencies (parents before children)
GTFS_LOAD_ORDER: list[tuple[str, str, list[str]]] = [
    # (gtfs_filename, target_table, columns)
    (
        "agency.txt",
        "transit.agency",
        [
            "agency_id", "agency_name", "agency_url", "agency_timezone",
            "agency_lang", "agency_phone", "agency_fare_url", "agency_email",
        ],
    ),
    (
        "calendar.txt",
        "transit.calendar",
        [
            "service_id", "monday", "tuesday", "wednesday", "thursday",
            "friday", "saturday", "sunday", "start_date", "end_date",
        ],
    ),
    (
        "routes.txt",
        "transit.routes",
        [
            "route_id", "agency_id", "route_short_name", "route_type",
            "route_color", "route_text_color", "route_long_name",
        ],
    ),
    (
        "stops.txt",
        "transit.stops",
        [
            "stop_id", "stop_name", "stop_desc", "stop_lat", "stop_lon",
            "location_type", "platform_code", "parent_station", "level_id",
        ],
    ),
    (
        "shapes.txt",
        "transit.shapes",
        ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"],
    ),
    (
        "trips.txt",
        "transit.trips",
        [
            "trip_id", "route_id", "service_id", "trip_headsign", "trip_short_name",
            "direction_id", "block_id", "shape_id", "wheelchair_accessible",
            "bikes_allowed",
        ],
    ),
    (
        "stop_times.txt",
        "transit.stop_times",
        [
            "trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
            "stop_headsign", "pickup_type", "drop_off_type", "shape_dist_traveled",
            "timepoint",
        ],
    ),
]


def download_gtfs(url: str | None = None) -> zipfile.ZipFile:
    """Download the GTFS zip feed into memory and return a ZipFile handle."""
    url = url or settings.gtfs.download_url
    logger.info("Downloading GTFS feed from %s ...", url)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    logger.info("Downloaded %.2f MB", len(resp.content) / 1_048_576)
    return zipfile.ZipFile(io.BytesIO(resp.content))


def _parse_gtfs_time(val: str) -> str | None:
    """Convert GTFS time (HH:MM:SS, can exceed 24h) to PostgreSQL INTERVAL string.

    GTFS times like '25:30:00' mean 1:30 AM the next day.
    PostgreSQL INTERVAL handles this correctly: '25 hours 30 minutes'.
    """
    if not val or pd.isna(val):
        return None
    parts = str(val).strip().split(":")
    if len(parts) != 3:
        return None
    h, m, s = parts
    return f"{h} hours {m} minutes {s} seconds"


def _parse_date(val: str) -> str | None:
    """Convert GTFS date string YYYYMMDD to ISO format YYYY-MM-DD."""
    if not val or pd.isna(val):
        return None
    s = str(val).strip()
    if len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _read_csv_from_zip(zf: zipfile.ZipFile, filename: str, columns: list[str]) -> pd.DataFrame:
    """Read a CSV file from the GTFS zip, selecting only the needed columns."""
    with zf.open(filename) as f:
        df = pd.read_csv(f, dtype=str, keep_default_na=False, low_memory=False)

    # Keep only expected columns (some feeds have extras); fill missing with empty
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    df = df[columns]

    # Replace empty strings with None for SQL NULL
    df = df.where(df != "", other=None)
    return df


def _bulk_insert(
    conn: psycopg2.extensions.connection,
    table: str,
    columns: list[str],
    df: pd.DataFrame,
    batch_size: int = 5000,
) -> int:
    """COPY-like bulk insert using execute_values for speed."""
    if df.empty:
        return 0

    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    # ON CONFLICT DO NOTHING makes re-runs idempotent
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            psycopg2.extras.execute_batch(cur, sql, batch, page_size=batch_size)
            inserted += len(batch)
    conn.commit()
    return inserted


def run_migration(conn: psycopg2.extensions.connection) -> None:
    """Execute the GTFS schema migration SQL file."""
    migration_path = Path(__file__).resolve().parent.parent.parent / "sql" / "migrations" / "001_gtfs_tables.sql"
    logger.info("Running migration: %s", migration_path.name)
    sql = migration_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info("Migration complete.")


def ingest_gtfs(gtfs_url: str | None = None) -> dict[str, int]:
    """Full pipeline: download GTFS → create tables → bulk load all data.

    Returns a dict mapping table names to row counts inserted.
    """
    zf = download_gtfs(gtfs_url)
    stats: dict[str, int] = {}

    conn = psycopg2.connect(settings.postgres.dsn)
    try:
        # Ensure schema + tables exist
        run_migration(conn)

        for filename, table, columns in GTFS_LOAD_ORDER:
            if filename not in zf.namelist():
                logger.warning("Skipping %s — not found in GTFS archive", filename)
                continue

            logger.info("Loading %s → %s ...", filename, table)
            df = _read_csv_from_zip(zf, filename, columns)

            # Type conversions for specific tables
            if table == "transit.calendar":
                df["start_date"] = df["start_date"].apply(_parse_date)
                df["end_date"] = df["end_date"].apply(_parse_date)

            if table == "transit.stop_times":
                df["arrival_time"] = df["arrival_time"].apply(_parse_gtfs_time)
                df["departure_time"] = df["departure_time"].apply(_parse_gtfs_time)

            count = _bulk_insert(conn, table, columns, df)
            stats[table] = count
            logger.info("  → %s: %d rows", table, count)

    finally:
        conn.close()

    logger.info("GTFS ingestion complete. Totals: %s", stats)
    return stats


def main() -> None:
    """CLI entry point for GTFS ingestion."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    ingest_gtfs()


if __name__ == "__main__":
    main()
