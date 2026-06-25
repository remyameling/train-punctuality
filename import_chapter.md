# IMPORT XML FILES INTO DATABASE

The raw data consists of gzip-compressed text files, each containing one XML record per line in the NDOV (Nationale Data Openbaar Vervoer) format. A custom Python parser was developed to read these files and load the relevant records into a MariaDB database.

## Filtering

Records were filtered in two stages. A fast string pre-filter first scanned each line without parsing the XML, discarding lines that clearly did not meet the criteria. Only lines passing all string checks were parsed as XML, after which a second, accurate check was applied on the parsed structure.

**Record type.** The NDOV feed contains two record types: DAS (Dynamische AankomstStaat, dynamic arrival state) and DVS (Dynamische VertrekStaat, dynamic departure state). Only DAS records were retained, as the analysis focuses on train arrivals.

**Station.** Only records for four HSL stations were imported: Amsterdam Centraal (ASD), Schiphol Airport (SHL), Rotterdam Centraal (RTD), and Breda (BD). The station code was first checked with a fast string scan (`StationCode>XXX<`), and then verified against the `RitStation/StationCode` element in the parsed XML to avoid false positives from station codes appearing elsewhere in the record.

**Train type.** Only Intercity Direct (ICD) trains were retained, as these are the only passenger services operating on the HSL high-speed line. The train type was first checked with a fast string scan (`Code="ICD"`) and confirmed against the `TreinSoort` element after parsing.

## Data Quality

Several checks were implemented to monitor data quality during import.

**Carrier validation.** The carrier (`Vervoerder`) field was checked against a whitelist of expected operators: NS and NS Int (NS International). Any record with an unexpected carrier was logged as an error.

**Completeness.** Records missing an actual arrival time (`AankomstTijd[@InfoStatus='Actueel']`) or a origin station code (`TreinHerkomst[@InfoStatus='Gepland']/StationCode`) were flagged and logged as errors. These fields are required for delay calculations and route analysis respectively.

**Message recency.** The NDOV feed may deliver multiple updates for the same train arrival. Each record is uniquely identified by the combination of `arrival_date`, `rit_id`, `trein_nummer`, and `station_code`. When a record already exists in the database, it is only updated if the new message timestamp (`ReisInformatieTijdstip`) is more recent than the one already stored. Older or duplicate messages are discarded. This ensures the database always reflects the most recent known state of each arrival.

**Delay flags.** Two binary flags were derived from the delay in seconds: `norm_vertraging` (delay ≥ 5 minutes) and `extra_vertraging` (delay ≥ 7 minutes). These thresholds align with the NS punctuality definitions used in public reporting.

## Processing Log

For each processed file, a summary record was written to a `processing_nvod` table, capturing the filename, MD5 hash, start and end time, total record count, and counts of inserted, updated, ignored, and erroneous records. This provides a full audit trail of the import process.
