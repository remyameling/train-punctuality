#!/usr/bin/env python3
"""
import_knmi.py — Laad KNMI uurwaarden (stations 240, 344, 350) in knmi_hourly_registrations.

Gebruik:
  python import_knmi.py                  # alle zip-/txt-bestanden in ./knmi/
  python import_knmi.py --dry-run        # parseer en print, geen DB-schrijf
  python import_knmi.py --year 2024      # alleen dit jaar (standaard: 2024)
  python import_knmi.py -v               # INFO-logging

Aannames:
  - Bestanden: knmi/uurgeg_240_2021-2030.zip  (ook .txt wordt geaccepteerd)
  - Header: alle regels vóór en inclusief de "# STN,YYYYMMDD,..." kolomregel
  - Datarijen: kommagescheiden, velden kunnen lege strings zijn (→ NULL)
  - obs_hour_start_utc = obs_date + (obs_hour - 1) uur  (HH=1 → 00:00 UTC)
  - Ruwe integers worden ongeschaald opgeslagen (geen deling door 10 e.d.)
"""

import argparse
import hashlib
import io
import logging
import os
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Optional

import pymysql
import pymysql.cursors
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuratie
# ---------------------------------------------------------------------------

KNMI_DIR = Path(__file__).parent / "knmi"
TARGET_YEAR = 2024

# Volgorde zoals KNMI-bestand ze levert (na STN, YYYYMMDD, HH).
KNMI_FILE_COLS = [
    "dd", "fh", "ff", "fx", "t", "t10n", "td",
    "sq", "q", "dr", "rh", "p", "vv", "n", "u",
    "ww", "ix", "m", "r", "s", "o", "y",
]

# Alle bestandskolommen worden ook in de tabel opgeslagen (inclusief q, p, ix).
_DB_COL_INDICES = {col: i for i, col in enumerate(KNMI_FILE_COLS)}
DB_COLS = ["dd", "fh", "ff", "fx", "t", "t10n", "td",
           "sq", "q", "dr", "rh", "p", "vv", "n", "u",
           "ww", "ix", "m", "r", "s", "o", "y"]

_INSERT_SQL = """
    INSERT INTO knmi_hourly_registrations
        (knmi_station, obs_date, obs_hour, obs_hour_start_utc,
         dd, fh, ff, fx, t, t10n, td,
         sq, q, dr, rh, p, vv, n, u,
         ww, ix, m, r, s, o, y,
         source_filename, imported_at)
    VALUES
        (%s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s, %s,
         %s, %s)
"""

_INSERT_LOG_SQL = """
    INSERT INTO processing_knmi
        (filename, md5, year_filter,
         start_time, end_time, duration_seconds,
         total_records, num_inserted, num_skipped, num_errors)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

@dataclass
class DbConfig:
    host: str = os.getenv("DB_HOST", "127.0.0.1")
    port: int = int(os.getenv("DB_PORT", "3306"))
    user: str = os.getenv("DB_USER", "")
    password: str = os.getenv("DB_PASSWORD", "")
    database: str = os.getenv("DB_DATABASE", "nvod")


def get_connection(cfg: DbConfig) -> pymysql.Connection:
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


# ---------------------------------------------------------------------------
# Hulpfuncties
# ---------------------------------------------------------------------------

def _md5_of_file(path: Path) -> str:
    """MD5 van de ruwe bestandsbytes (zip of txt)."""
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _log_import(conn: pymysql.Connection, filename: str, md5: str, year: int,
                start: datetime, end: datetime, stats: dict) -> None:
    duration = max(0, round((end - start).total_seconds()))
    total = stats["inserted"] + stats["skipped"] + stats["errors"]
    with conn.cursor() as cur:
        cur.execute(_INSERT_LOG_SQL, (
            filename, md5, year,
            start, end, duration,
            total, stats["inserted"], stats["skipped"], stats["errors"],
        ))
    conn.commit()
    logger.info(
        "Log geschreven: %s  jaar=%d  inserted=%d  skipped=%d  errors=%d  duur=%ds",
        filename, year, stats["inserted"], stats["skipped"], stats["errors"], duration,
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_int(raw: str) -> Optional[int]:
    """Geeft None terug voor een leeg of spatie-only veld, anders int."""
    s = raw.strip()
    if not s:
        return None
    return int(s)


def _obs_hour_start_utc(obs_date: str, obs_hour: int) -> datetime:
    """
    obs_date : 'YYYYMMDD'
    obs_hour : 1..24  (KNMI: uurvak 1 = 00:00–01:00 UTC)
    Geeft het begin van het uurvak terug als naive UTC datetime (DATETIME-kolom).
    """
    base = datetime(int(obs_date[:4]), int(obs_date[4:6]), int(obs_date[6:8]))
    return base + timedelta(hours=obs_hour - 1)


def iter_lines(path: Path) -> Iterator[str]:
    """Levert tekstregels uit een .zip (eerste lid) of een platte .txt/.gz."""
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            members = [m for m in zf.namelist() if not m.endswith("/")]
            if not members:
                raise ValueError(f"Leeg zip-archief: {path}")
            name = members[0]
            logger.debug("ZIP-lid: %s", name)
            with zf.open(name) as fh:
                for line in io.TextIOWrapper(fh, encoding="latin-1"):
                    yield line
    else:
        with open(path, encoding="latin-1") as fh:
            for line in fh:
                yield line


def parse_file(path: Path, year: int) -> Iterator[tuple]:
    """
    Levert tuples (knmi_station, obs_date_str, obs_hour, obs_hour_start_utc, <db-kolommen>)
    voor alle rijen van het opgegeven jaar.
    """
    header_done = False

    for raw in iter_lines(path):
        line = raw.rstrip("\n")

        if not header_done:
            if line.lstrip().startswith("# STN"):
                header_done = True
            continue

        if not line.strip():
            continue

        parts = line.split(",")
        if len(parts) < 3 + len(KNMI_FILE_COLS):
            logger.warning("Onverwacht aantal velden (%d): %r", len(parts), line)
            continue

        knmi_station = int(parts[0].strip())
        obs_date = parts[1].strip()       # 'YYYYMMDD'
        obs_hour = int(parts[2].strip())  # 1..24

        if int(obs_date[:4]) != year:
            continue

        ts_utc = _obs_hour_start_utc(obs_date, obs_hour)
        obs_date_db = f"{obs_date[:4]}-{obs_date[4:6]}-{obs_date[6:]}"

        all_vals = [_parse_int(parts[3 + i]) for i in range(len(KNMI_FILE_COLS))]
        db_vals = [all_vals[_DB_COL_INDICES[c]] for c in DB_COLS]

        yield (knmi_station, obs_date_db, obs_hour, ts_utc, *db_vals)


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

def import_file(path: Path, cfg: DbConfig, year: int, dry_run: bool) -> dict:
    source_filename = path.name
    stats = {"inserted": 0, "skipped": 0, "errors": 0}

    md5 = _md5_of_file(path)
    logger.debug("MD5 %s: %s", source_filename, md5)

    if dry_run:
        imported_at = datetime.utcnow()
        for row in parse_file(path, year):
            logger.info("DRY-RUN row: %s", row)
            stats["inserted"] += 1
        logger.info(
            "DRY-RUN %s  md5=%s  inserted=%d",
            source_filename, md5, stats["inserted"],
        )
        return stats

    start = datetime.utcnow()
    imported_at = start

    conn = get_connection(cfg)
    try:
        with conn.cursor() as cur:
            for row in parse_file(path, year):
                try:
                    cur.execute(_INSERT_SQL, (*row, source_filename, imported_at))
                    stats["inserted"] += 1
                except pymysql.err.IntegrityError:
                    stats["skipped"] += 1
                except Exception as exc:
                    logger.error("Fout bij rij %s: %s", row[:3], exc)
                    stats["errors"] += 1
        conn.commit()

        end = datetime.utcnow()
        _log_import(conn, source_filename, md5, year, start, end, stats)
    finally:
        conn.close()

    return stats


def find_knmi_files(knmi_dir: Path) -> list[Path]:
    files = sorted(
        p for p in knmi_dir.iterdir()
        if p.suffix.lower() in {".zip", ".txt"} and "uurgeg" in p.name.lower()
    )
    return files


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Laad KNMI uurwaarden in de database.")
    parser.add_argument("--year", type=int, default=TARGET_YEAR,
                        help=f"Alleen dit jaar importeren (standaard {TARGET_YEAR})")
    parser.add_argument("--knmi-dir", type=Path, default=KNMI_DIR,
                        help="Map met KNMI-bestanden (standaard ./knmi/)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parseer en log, schrijf niets naar de DB")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="INFO-logging")
    parser.add_argument("-vv", "--debug", action="store_true",
                        help="DEBUG-logging")

    args = parser.parse_args()

    level = logging.WARNING
    if args.debug:
        level = logging.DEBUG
    elif args.verbose:
        level = logging.INFO
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
        level=level,
        stream=sys.stderr,
    )

    cfg = DbConfig()

    files = find_knmi_files(args.knmi_dir)
    if not files:
        logger.error("Geen KNMI-bestanden gevonden in %s", args.knmi_dir)
        sys.exit(1)

    totals = {"inserted": 0, "skipped": 0, "errors": 0}
    for f in files:
        logger.info("Verwerken: %s", f.name)
        stats = import_file(f, cfg, year=args.year, dry_run=args.dry_run)
        for k in totals:
            totals[k] += stats[k]

    print(
        f"Klaar. Bestanden={len(files)}  "
        f"Inserted={totals['inserted']}  "
        f"Skipped={totals['skipped']}  "
        f"Errors={totals['errors']}"
    )


if __name__ == "__main__":
    main()
