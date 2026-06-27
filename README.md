# parse_nvod

Analysis of Intercity Direct (HSL) train punctuality using real-time arrival data from the NDOV feed and weather data from KNMI.

## Project overview

The NDOV (Nationale Data Openbaar Vervoer) feed delivers real-time train information as XML messages. This project parses those messages, filters for Intercity Direct (ICD) arrivals at four HSL stations, and stores the results in a MariaDB database for further analysis. Weather data from KNMI is imported separately to enable correlation analysis between weather conditions and train delays.

## Requirements

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (package manager)
- MariaDB / MySQL
- Docker (optional, for running MariaDB and phpMyAdmin locally)

Install dependencies:

```bash
uv sync
```

## Configuration

Copy `.env_example` to `.env` and fill in your database credentials:

```bash
cp .env_example .env
```

```ini
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_DATABASE=your_db_name
```

## Scripts

### `xml_parser.py` — Import NDOV arrival data

Parses gzip-compressed NDOV XML files (one XML record per line) and loads Intercity Direct arrival records into the `train_arrivals` table.

**Filtering applied:**
- Record type: DAS (Dynamische AankomstStaat) only
- Train type: ICD (Intercity Direct) only
- Stations: ASD (Amsterdam Centraal), SHL (Schiphol Airport), RTD (Rotterdam Centraal), BD (Breda)

**Usage:**

```bash
# Process a single file
python xml_parser.py path/to/file.gz

# Process all .gz files in a directory
python xml_parser.py path/to/directory/

# Options
python xml_parser.py path/ -v            # INFO logging to stderr
python xml_parser.py path/ -vv           # DEBUG logging to stderr
python xml_parser.py path/ --no-xml-log  # suppress per-record log lines
python xml_parser.py path/ --test        # process only the first 100 matching records
```

A log file (`xml_parser.log`) is written to the input directory. A processing summary is stored in the `processing_nvod` table after each file.

---

### `import_knmi.py` — Import KNMI weather data

Imports hourly weather observations from KNMI zip files into the `knmi_hourly_registrations` table.

**Usage:**

```bash
# Import all KNMI files from ./knmi/
python import_knmi.py

# Options
python import_knmi.py --year 2024        # filter to a specific year (default: 2024)
python import_knmi.py --knmi-dir ./data  # specify a custom input directory
python import_knmi.py --dry-run          # parse and log without writing to the database
python import_knmi.py -v                 # INFO logging
python import_knmi.py -vv               # DEBUG logging
```

Place KNMI zip files (e.g. `uurgeg_240_2021-2030.zip`) in the `./knmi/` directory before running.

## Database

The project uses the following tables:

| Table | Description |
|-------|-------------|
| `train_arrivals` | ICD arrival records with planned/actual times and delay flags |
| `processing_nvod` | Import log per processed NDOV file |
| `knmi_hourly_registrations` | Hourly weather observations per KNMI station |
| `processing_knmi` | Import log per processed KNMI file |
