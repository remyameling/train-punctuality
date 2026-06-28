#!/usr/bin/env python3
"""
xml_parser.py — Read and parse XML records from a text file (one XML per line).

Usage:
  python xml_parser.py file.txt            # warnings and errors only
  python xml_parser.py file.txt -v         # INFO (progress + stats)
  python xml_parser.py file.txt -vv        # DEBUG (per-line detail)
  python xml_parser.py file.txt --no-xml-log   # suppress per-XML log line
"""

import argparse
import gzip
import hashlib
import logging
import os
import sys
from dotenv import load_dotenv
from lxml import etree
from tqdm import tqdm
import pymysql
from datetime import datetime, timezone
import pymysql.cursors
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

load_dotenv()

DEFAULT_ONDERGRENS = 5*60   # 5 minuten
EXTRA_ONDERGRENS = 7*60     # 7 minuten
ICD_TREINSOORT = "ICD"      # Intercity Direct (HSL)

NS = '{urn:ndov:cdm:trein:reisinformatie:data:4}'

logger = logging.getLogger(__name__)

@dataclass
class DbConfig:
    host: str = os.getenv("DB_HOST", "127.0.0.1")
    port: int = int(os.getenv("DB_PORT", "3306"))
    user: str = os.getenv("DB_USER", "")
    password: str = os.getenv("DB_PASSWORD", "")
    database: str = os.getenv("DB_DATABASE", "nvod")

@dataclass
class DasRecord:
    message_time: Optional[str]
    rit_id: Optional[str]
    trein_nummer: Optional[str]
    rit_datum: Optional[str]
    station_code: Optional[str]
    planned_aankomst: Optional[str]
    actual_aankomst: Optional[str]
    herkomst_station_code: Optional[str]

_INSERT_PROC_NVOD_SQL = """INSERT INTO processing_nvod (filename,md5, start_time, end_time, duration_seconds,total_records,num_errors,num_inserted,num_updated,num_ignored,num_das_station_match,num_das_others,num_non_das)
                 VALUES (%s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

_INSERT_SQL = """INSERT INTO train_arrivals (message_time, rit_id, trein_nummer, arrival_date, station_code,
        planned_arrival, actual_arrival, delay_seconds, norm_vertraging, extra_vertraging,
        herkomst_station_code, source_filename
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_UPDATE_SQL = """UPDATE train_arrivals SET message_time = %s, planned_arrival = %s, actual_arrival = %s,
            delay_seconds = %s, norm_vertraging = %s, extra_vertraging = %s,
            herkomst_station_code = %s, source_filename = %s WHERE id = %s
"""

DEFAULT_STATIONS = {"ASD", "SHL", "RTD", "BD"}
ALLOWED_VERVOERDERS = {"NS", "NS Int"}

# Separate logger for per-XML lines so it can be disabled independently.
xml_logger = logging.getLogger(f"{__name__}.per_xml")

LOG_FORMAT = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"
LOG_DATEFMT = "%H:%M:%S"


def setup_logging(verbosity: int, log_file: Path) -> None:
    """
    Configure logging with two handlers:
      - stderr: level based on verbosity (0=WARNING, 1=INFO, 2+=DEBUG)
      - log file: always DEBUG (captures everything)
    """
    console_level = {0: logging.WARNING, 1: logging.INFO}.get(verbosity, logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # pass everything; handlers filter individually
    root.addHandler(console_handler)
    root.addHandler(file_handler)


def _try_parse(text: str) -> Optional[etree._Element]:
    """Return a parsed lxml Element, or None if text is not valid XML."""
    text = text.strip()
    if not text or not text.startswith("<"):
        return None
    try:
        return etree.fromstring(text.encode("utf-8"))
    except etree.XMLSyntaxError:
        return None


_READ_BUFFER_SIZE = 4 * 1024 * 1024  # 4 MB


def _raw_lines(filepath: Path) -> Iterator[tuple[int, str]]:
    """Yield (lineno, stripped_line) for every non-empty line. Handles .gz transparently."""
    if filepath.suffix == ".gz":
        raw = open(filepath, "rb", buffering=_READ_BUFFER_SIZE)
        fh = gzip.open(raw, "rt", encoding="utf-8", errors="replace")
    else:
        fh = open(filepath, "r", encoding="utf-8", errors="replace", buffering=_READ_BUFFER_SIZE)
    with fh:
        for lineno, raw_line in enumerate(fh, start=1):
            line = raw_line.strip()
            if line:
                yield lineno, line


def xml_records(filepath: Path) -> Iterator[tuple[int, etree._Element]]:
    """Yield (line_number, etree._Element) for each line that contains valid XML."""
    for lineno, line in _raw_lines(filepath):
        element = _try_parse(line)
        if element is not None:
            yield lineno, element
        else:
            logger.error("Line %d: not a single-line XML record — skipped: %.120s", lineno, line)

def classify_record(element: etree._Element) -> str:
    """
    Return 'DAS', 'DVS', or 'UNKNOWN' based on the first child element's tag.
    """
    for child in element:
        local_name = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if "DAS" in local_name:
            return "DAS"
        if "DVS" in local_name:
            return "DVS"
    return "UNKNOWN"


def _find_by_local_name(element: etree._Element, local_name: str) -> Optional[etree._Element]:
    """Find the first descendant whose local name (ignoring namespace) matches."""
    for el in element.iter():
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
        if tag == local_name:
            return el
    return None


def get_station_code(element: etree._Element) -> Optional[str]:
    """Return the StationCode text from within a RitStation element, or None if not found."""
    rit_station = _find_by_local_name(element, "RitStation")
    if rit_station is None:
        return None
    station_code_el = _find_by_local_name(rit_station, "StationCode")
    if station_code_el is None:
        return None
    return station_code_el.text


def extract_das_data(element: etree._Element) -> DasRecord:
    """Extract fields from a DAS element into a DasRecord."""
    das = _find_by_local_name(element, "DynamischeAankomstStaat")
    rip = _find_by_local_name(element, "RIPAdministratie")

    ReisInformatieTijdstipEL = rip.find(f'{NS}ReisInformatieTijdstip')
    RitIdEl                  = das.find(f'{NS}RitId')
    RitDatumEl               = das.find(f'{NS}RitDatum')

    RitStationEL  = das.find(f'{NS}RitStation')
    StationCodeEl = RitStationEL.find(f'{NS}StationCode')

    TreinAankomstEl       = das.find(f'{NS}TreinAankomst')
    TreinNummerEl         = TreinAankomstEl.find(f'{NS}TreinNummer')
    PlannedAankomstTijdEL = TreinAankomstEl.find(f"{NS}AankomstTijd[@InfoStatus='Gepland']")
    ActualAankomstTijdEl  = TreinAankomstEl.find(f"{NS}AankomstTijd[@InfoStatus='Actueel']")

    herkomst_el           = TreinAankomstEl.find(f"{NS}TreinHerkomst[@InfoStatus='Gepland']")
    HerkomstStationCodeEl = herkomst_el.find(f'{NS}StationCode') if herkomst_el is not None else None

    return DasRecord(
        message_time=ReisInformatieTijdstipEL.text if ReisInformatieTijdstipEL is not None else None,
        rit_id=RitIdEl.text if RitIdEl is not None else None,
        trein_nummer=TreinNummerEl.text if TreinNummerEl is not None else None,
        rit_datum=RitDatumEl.text if RitDatumEl is not None else None,
        station_code=StationCodeEl.text if StationCodeEl is not None else None,
        planned_aankomst=PlannedAankomstTijdEL.text if PlannedAankomstTijdEL is not None else None,
        actual_aankomst=ActualAankomstTijdEl.text if ActualAankomstTijdEl is not None else None,
        herkomst_station_code=HerkomstStationCodeEl.text if HerkomstStationCodeEl is not None else None,
    )


def get_trein_soort_code(element: etree._Element) -> Optional[str]:
    """Return the TreinSoort Code attribute from a DAS element, or None."""
    trein_aankomst = _find_by_local_name(element, "TreinAankomst")
    if trein_aankomst is None:
        return None
    trein_soort_el = trein_aankomst.find(f'{NS}TreinSoort')
    return trein_soort_el.get('Code') if trein_soort_el is not None else None


def check_quality_and_log_route(element: etree._Element, record: DasRecord, lineno: int) -> None:
    """Log route info (always) and error-log any data quality issues."""
    das = _find_by_local_name(element, "DynamischeAankomstStaat")
    trein_aankomst = _find_by_local_name(das, "TreinAankomst") if das is not None else None
    if trein_aankomst is None:
        return

    # Vervoerder — must be in whitelist
    vervoerder_el = trein_aankomst.find(f'{NS}Vervoerder')
    vervoerder = vervoerder_el.text if vervoerder_el is not None else None
    if vervoerder not in ALLOWED_VERVOERDERS:
        logger.error(
            "Quality: line %d  unexpected vervoerder=[%s]  rit_id=%s  rit_datum=%s",
            lineno, vervoerder or "MISSING", record.rit_id, record.rit_datum,
        )

    # Completeness checks
    if record.actual_aankomst is None:
        logger.error(
            "Quality: line %d  missing actual_aankomst  rit_id=%s  rit_datum=%s  station=%s",
            lineno, record.rit_id, record.rit_datum, record.station_code,
        )
    if record.herkomst_station_code is None:
        logger.error(
            "Quality: line %d  missing herkomst_station_code  rit_id=%s  rit_datum=%s  station=%s",
            lineno, record.rit_id, record.rit_datum, record.station_code,
        )

    # Route logging (always at INFO)
    herkomst_el = trein_aankomst.find(f"{NS}TreinHerkomst[@InfoStatus='Gepland']")
    herkomst_korte_naam = None
    if herkomst_el is not None:
        korte_naam_el = herkomst_el.find(f'{NS}KorteNaam')
        herkomst_korte_naam = korte_naam_el.text if korte_naam_el is not None else None

    route_el = trein_aankomst.find(f'{NS}PresentatieVerkorteRouteHerkomst')
    route_text = None
    if route_el is not None:
        uiting_el = route_el.find(f'.//{NS}Uiting')
        route_text = uiting_el.text if uiting_el is not None else None

    logger.info(
        "route  line %-6d  vervoerder=%-4s  herkomst=%-15s  route=%s",
        lineno,
        vervoerder or "?",
        herkomst_korte_naam or "?",
        route_text or "?",
    )


def md5(filepath: Path):
    """Return the hex MD5 digest of a file."""
    h = hashlib.md5()
    with filepath.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    """Parse ISO 8601 datetime string to a timezone-naive UTC datetime."""
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def find_arrival(conn: pymysql.connections.Connection, record: DasRecord):
    """
    Look up a record in train_arrivals by its unique key.
    Returns the row as a dict, or None if not found.
    """
    sql = """SELECT * FROM train_arrivals WHERE arrival_date = %s AND rit_id = %s AND trein_nummer = %s AND station_code = %s"""
    with conn.cursor() as cursor:
        cursor.execute(sql, (record.rit_datum, record.rit_id, record.trein_nummer, record.station_code))
        return cursor.fetchone()


def insert_arrival(conn: pymysql.connections.Connection, record: DasRecord, source_filename: Optional[str] = None) -> int:
    """
    Insert one DAS arrival record into train_arrivals. Returns the auto-generated id.
    Raises ValueError if required fields are missing.
    """
    planned = _parse_dt(record.planned_aankomst)
    actueel = _parse_dt(record.actual_aankomst)
    message_time = _parse_dt(record.message_time)

    if None in (record.rit_id, record.trein_nummer, record.rit_datum, planned, actueel, message_time):
        raise ValueError(f"Missing required fields in DasRecord: {record}")

    delay_seconds = int((actueel - planned).total_seconds()) if planned and actueel else None
    norm_vertraging = 1 if delay_seconds is not None and delay_seconds >= DEFAULT_ONDERGRENS else 0
    extra_vertraging = 1 if delay_seconds is not None and delay_seconds >= EXTRA_ONDERGRENS else 0

    with conn.cursor() as cursor:
        cursor.execute(_INSERT_SQL, (
            message_time,
            int(record.rit_id),
            int(record.trein_nummer),
            record.rit_datum,
            record.station_code,
            planned,
            actueel,
            delay_seconds,
            norm_vertraging,
            extra_vertraging,
            record.herkomst_station_code,
            source_filename,
        ))
    return conn.insert_id()


def insert_processing_nvod(conn: pymysql.connections.Connection,
                           filename, md5, start_time_dt, end_time_dt,
                           total_records, num_errors, num_inserted, num_updated, num_ignored,
                           num_das_station_match, num_das_others, num_non_das) -> int:
    """Insert a processing record into processing_nvod. Returns the auto-generated id."""
    if None in (filename, md5, start_time_dt, end_time_dt, total_records, num_errors,
                num_inserted, num_updated, num_ignored, num_das_station_match, num_das_others, num_non_das):
        raise ValueError("Missing required fields in arguments")

    duration_seconds = int((end_time_dt - start_time_dt).total_seconds())

    with conn.cursor() as cursor:
        cursor.execute(_INSERT_PROC_NVOD_SQL, (
            filename, md5, start_time_dt, end_time_dt, duration_seconds,
            total_records, num_errors, num_inserted, num_updated, num_ignored,
            num_das_station_match, num_das_others, num_non_das,
        ))

    conn.commit()
    return conn.insert_id()


def update_arrival(conn, id, record, source_filename):
    """
    Update a DAS arrival record in train_arrivals.
    Raises ValueError if required fields are missing.
    """
    planned = _parse_dt(record.planned_aankomst)
    actueel = _parse_dt(record.actual_aankomst)
    message_time = _parse_dt(record.message_time)

    if None in (record.rit_id, record.trein_nummer, record.rit_datum, planned, actueel, message_time):
        raise ValueError(f"Missing required fields in DasRecord: {record}")

    delay_seconds = int((actueel - planned).total_seconds()) if planned and actueel else None
    norm_vertraging = 1 if delay_seconds is not None and delay_seconds >= DEFAULT_ONDERGRENS else 0
    extra_vertraging = 1 if delay_seconds is not None and delay_seconds >= EXTRA_ONDERGRENS else 0

    with conn.cursor() as cursor:
        cursor.execute(_UPDATE_SQL, (
            message_time,
            planned,
            actueel,
            delay_seconds,
            norm_vertraging,
            extra_vertraging,
            record.herkomst_station_code,
            source_filename,
            id,
        ))


def process_element(conn, element: etree._Element, filename: str, lineno: int, file_md5: str) -> str:
    """Process one DAS record: insert or update train_arrivals.

    A record is uniquely identified by: arrival_date + rit_id + trein_nummer + station_code.
    If a record already exists, it is only updated when the new message_time is newer.
    """
    new_record = extract_das_data(element)
    check_quality_and_log_route(element, new_record, lineno)

    existing_record = find_arrival(conn, new_record)
    if existing_record is not None:
        message_time = existing_record['message_time']
        new_record_message_time = _parse_dt(new_record.message_time)

        if message_time <= new_record_message_time:
            update_arrival(conn, existing_record['id'], new_record, filename)
            prefix = "update"
        else:
            prefix = "ignore"
    else:
        insert_arrival(conn, new_record, filename)
        prefix = "insert"

    xml_logger.info(
        prefix + " line %-6d  rit_id=%-7s  rit_datum=%-12s  trein_id=%-7s geplande_aankomst=%s   werkelijke_aankomst=%s",
        lineno, new_record.rit_id or "NONE", new_record.rit_datum or "NONE", new_record.trein_nummer or "NONE",
        new_record.planned_aankomst or "NONE", new_record.actual_aankomst or "NONE"
    )

    return prefix


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Read and parse XML records from a text file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("path", type=Path, help="Path to a single input file or a directory of .gz files")
    p.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="-v = INFO, -vv = DEBUG (written to stderr)",
    )
    p.add_argument(
        "--no-xml-log",
        action="store_true",
        help="Disable the per-XML INFO log line (more efficient for large files).",
    )
    p.add_argument(
        "--test",
        action="store_true",
        help="Process only the first 100 matching DAS records, then stop.",
    )
    return p


def collect_input_files(path: Path) -> list[Path]:
    """Return a sorted list of input files to process (.txt and .gz)."""
    if path.is_file():
        return [path]
    if path.is_dir():
        files = sorted(f for f in path.iterdir() if f.is_file() and f.suffix in (".txt", ".gz"))
        if not files:
            logger.warning("No .txt or .gz files found in %s", path)
        return files
    logger.error("Path not found: %s", path)
    sys.exit(1)


def process_file(conn, filepath: Path, args) -> None:
    """Process a single input file."""
    stations = DEFAULT_STATIONS
    test_limit = 100 if args.test else None

    if not test_limit:
        logger.info("Hashing input file: %s", filepath)
        file_md5 = md5(filepath)
    else:
        file_md5 = "0"

    logger.info("Processing: %s  md5=%s  (stations: %s%s)", filepath, file_md5,
                ", ".join(sorted(stations)), "  [TEST MODE: max 100]" if test_limit else "")

    # Pre-filter strings — avoids XML parsing for the majority of lines.
    _DAS_MARKER  = "ReisInformatieProductDAS"
    _ICD_MARKER  = f'Code="{ICD_TREINSOORT}"'
    COMMIT_BATCH_SIZE = 500

    start_time_dt = datetime.now()
    total_count = 0
    das_match_count = 0
    das_other_count = 0
    non_das = 0
    non_icd_count = 0
    error_count = 0
    num_operations = {'insert': 0, 'update': 0, 'ignore': 0}

    try:
        lines = _raw_lines(filepath)
        if args.verbose == 0:
            lines = tqdm(lines, desc=filepath.name, unit=" lines", dynamic_ncols=True)

        for lineno, line in lines:
            total_count += 1

            # Fast string check 1: must be a DAS record.
            if _DAS_MARKER not in line:
                non_das += 1
                continue

            # Fast string check 2: station code in a StationCode element.
            if not any(f"StationCode>{s}<" in line for s in stations):
                das_other_count += 1
                continue

            # Fast string check 3: must be an ICD train.
            if _ICD_MARKER not in line:
                non_icd_count += 1
                continue

            # Only parse XML when all string checks pass.
            element = _try_parse(line)
            if element is None:
                error_count += 1
                logger.error("Line %d: parse error — skipped: %.120s", lineno, line)
                continue

            # Accurate check: station code must be the RitStation StationCode specifically.
            station_code = get_station_code(element)
            if station_code not in stations:
                das_other_count += 1
                continue

            # Accurate check: trein_soort must be ICD.
            if get_trein_soort_code(element) != ICD_TREINSOORT:
                non_icd_count += 1
                continue

            das_match_count += 1
            try:
                operation = process_element(conn, element, filepath, lineno, file_md5)
                num_operations[operation] += 1
            except Exception as e:
                error_count += 1
                logger.exception("Error processing DAS element at line %d", lineno)

            if das_match_count % COMMIT_BATCH_SIZE == 0:
                conn.commit()

            if test_limit and das_match_count >= test_limit:
                logger.info("Test limit of %d reached — stopping early", test_limit)
                break

    except (OSError, IOError):
        logger.exception("Failed to read file: %s", filepath)
        return

    conn.commit()  # flush remaining records

    summary = (
        f"Done: {total_count} records total — "
        f"ICD station match: {das_match_count}, "
        f"non-ICD (station match): {non_icd_count}, "
        f"DAS other station: {das_other_count}, "
        f"non-DAS: {non_das}, "
        f"inserted: {num_operations['insert']}, "
        f"updated: {num_operations['update']}, "
        f"ignored: {num_operations['ignore']}, "
        f"errors: {error_count}"
    )
    logger.info(summary)
    print(summary)

    end_time_dt = datetime.now()
    insert_processing_nvod(conn, filepath.name, file_md5, start_time_dt, end_time_dt,
                           total_count, error_count, num_operations['insert'],
                           num_operations['update'], num_operations['ignore'],
                           das_match_count, das_other_count + non_icd_count, non_das)


def main(args):
    log_file = args.path.with_suffix(args.path.suffix + ".log") if args.path.is_file() \
               else args.path / "xml_parser.log"
    setup_logging(args.verbose, log_file)

    if args.no_xml_log:
        xml_logger.disabled = True

    db_config = DbConfig()
    conn = pymysql.connect(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

    input_files = collect_input_files(args.path)
    logger.info("Found %d file(s) to process", len(input_files))

    for i, filepath in enumerate(input_files, start=1):
        logger.info("File %d/%d: %s", i, len(input_files), filepath.name)
        process_file(conn, filepath, args)

    conn.close()


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    main(args)
