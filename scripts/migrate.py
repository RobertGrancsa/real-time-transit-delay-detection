"""Database Migration Runner — applies SQL migrations in order to TimescaleDB.

Tracks applied migrations in a `transit.schema_migrations` ledger table
so each migration runs at most once. Migrations are ordered by filename
(e.g. 001_*.sql, 002_*.sql, ...).

Usage:
    python -m scripts.migrate        # from project root
    python scripts/migrate.py        # direct execution
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import psycopg2

# Allow running both as `python scripts/migrate.py` and `python -m scripts.migrate`
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

from config.settings import settings  # noqa: E402

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = _project_root / "sql" / "migrations"

# ---------------------------------------------------------------------------
# Ledger table — tracks which migrations have been applied
# ---------------------------------------------------------------------------
_LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS transit.schema_migrations (
    filename    TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def _get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(settings.postgres.dsn)


def _ensure_ledger(conn: psycopg2.extensions.connection) -> None:
    """Create the migration ledger table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute(_LEDGER_DDL)
    conn.commit()


def _applied_migrations(conn: psycopg2.extensions.connection) -> set[str]:
    """Return the set of migration filenames already applied."""
    with conn.cursor() as cur:
        cur.execute("SELECT filename FROM transit.schema_migrations ORDER BY filename")
        return {row[0] for row in cur.fetchall()}


def _discover_migrations() -> list[Path]:
    """Return migration SQL files sorted by name (ascending)."""
    if not MIGRATIONS_DIR.is_dir():
        logger.warning("Migrations directory not found: %s", MIGRATIONS_DIR)
        return []
    return sorted(MIGRATIONS_DIR.glob("*.sql"))


def run_migrations(*, dry_run: bool = False) -> int:
    """Apply pending migrations and return the number applied.

    Args:
        dry_run: If True, print pending migrations without executing them.

    Returns:
        Number of migrations applied (or that would be applied in dry-run mode).
    """
    conn = _get_connection()
    try:
        _ensure_ledger(conn)
        applied = _applied_migrations(conn)
        pending = [
            m for m in _discover_migrations()
            if m.name not in applied
        ]

        if not pending:
            logger.info("All migrations are up to date.")
            return 0

        logger.info("Found %d pending migration(s):", len(pending))
        for m in pending:
            logger.info("  - %s", m.name)

        if dry_run:
            logger.info("Dry run — no changes applied.")
            return len(pending)

        applied_count = 0
        for migration_file in pending:
            sql = migration_file.read_text(encoding="utf-8")
            logger.info("Applying %s ...", migration_file.name)
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    cur.execute(
                        "INSERT INTO transit.schema_migrations (filename) VALUES (%s)",
                        (migration_file.name,),
                    )
                conn.commit()
                applied_count += 1
                logger.info("  ✓ %s applied successfully.", migration_file.name)
            except Exception:
                conn.rollback()
                logger.exception("  ✗ Failed to apply %s", migration_file.name)
                raise

        logger.info("Applied %d migration(s) successfully.", applied_count)
        return applied_count
    finally:
        conn.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    import argparse

    parser = argparse.ArgumentParser(description="Apply SQL migrations to TimescaleDB")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List pending migrations without applying them",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show applied/pending migration status and exit",
    )
    args = parser.parse_args()

    if args.status:
        conn = _get_connection()
        try:
            _ensure_ledger(conn)
            applied = _applied_migrations(conn)
            all_migrations = _discover_migrations()
            print(f"\n{'Migration':<45} {'Status':<12}")
            print("-" * 57)
            for m in all_migrations:
                status = "✓ applied" if m.name in applied else "○ pending"
                print(f"  {m.name:<43} {status}")
            print()
        finally:
            conn.close()
        return

    count = run_migrations(dry_run=args.dry_run)
    if count == 0 and not args.dry_run:
        print("Nothing to do — database is up to date.")


if __name__ == "__main__":
    main()
