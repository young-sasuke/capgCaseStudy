# Evaluation Questions & Answers

## Q1: How does the cleaning pipeline work?
The pipeline follows a sequential, stage-based architecture:

1. **Ingestion** – The dataset generator creates 300+ records with intentional data quality problems (mixed timestamp formats, unit inconsistencies, extra spaces, duplicate rows, etc.) and writes `data/raw_events.csv`.
2. **Cleaning** – Each field is processed by a dedicated function in `cleaner.py`:
   - `Line` / `Station` → trimmed & uppercased.
   - `TS` → parsed from 5 different formats, invalid minutes (>59) clamped to 59, normalised to `YYYY-MM-DD HH:MM`.
   - `Part_No` → uppercased, missing hyphens reinserted.
   - `Torque` → unit suffix stripped, converted to float `Torque_Nm`.
   - `Temp` → Fahrenheit converted to Celsius, stored as float `Temp_C`.
   - `Defect` → `na`, `OK`, `None`, `""` mapped to empty; `Reject`/`Repair` standardised.
   - `VIN` → spaces removed, uppercased.
   - `Supplier` → uppercased.
3. **Validation** – `validator.py` checks each cleaned row against: VIN regex (17 alphanumeric, no I/O/Q), timestamp format, required-field presence, and numeric data types. Failures are routed to `rejected_events.csv` with reasons.
4. **Deduplication** – Exact duplicates identified by `(VIN, Station, TS)` are removed.
5. **Rework Detection** – Any `(VIN, Station)` pair occurring more than once is flagged `Rework_Flag = True`.
6. **Analytics** – 10 KPI families are computed and saved to `metrics_report.csv`.

## Q2: How are duplicates detected?
Duplicates are detected using a **composite key**: `(VIN, Station, TS)`. The `deduplicate()` function iterates through all valid rows, maintaining a `set` of seen keys. If a key has already been seen, the row is discarded. This guarantees O(n) time complexity and preserves the first occurrence.

## Q3: How is First Pass Yield (FPY) calculated?
```
FPY = (Total Records - Defect Records) / Total Records × 100
```
A record is counted as a defect if its normalised `Defect` field equals `"Reject"` or `"Repair"`. Records with empty defect fields (originally `na`, `OK`, `None`, or blank) are treated as passed.

## Q4: How would the system scale?

### Current limitations
- All data is held in memory as a list of dicts.
- Single-threaded, single-machine execution.

### Scaling strategies
1. **Batch processing** – Replace in-memory lists with streaming/chunked CSV readers to handle files that exceed available RAM.
2. **Parallel processing** – Cleaning and validation are row-independent; they can be parallelised with `multiprocessing.Pool` or distributed across workers (e.g. Dask, Spark).
3. **Database backend** – Replace CSV I/O with database ingestion (PostgreSQL, BigQuery). The SQL schema in `sql/` already demonstrates the relational model.
4. **Message queues** – For real-time ingestion, each source system can push events to Kafka/RabbitMQ; workers consume, clean, and write to the database.
5. **Orchestration** – Use Airflow or Prefect to schedule and monitor pipeline runs, add retries, alerting, and lineage tracking.
6. **Schema evolution** – Add a schema registry (e.g. Avro/Protobuf) so upstream format changes are caught before ingestion.

## Q5: What data quality issues does the generator introduce?
- Extra leading/trailing spaces in `Line` and `Station`
- Timestamps in 5 different formats (YYYY-MM-DD, DD/MM/YYYY, MM-DD-YYYY, YYYY/MM/DD, DD-MM-YYYY)
- Invalid minutes (60–99)
- Mixed-case part numbers and missing hyphens
- Torque values with "Nm" suffix
- Temperatures in both Celsius and Fahrenheit
- Defect values: `na`, `OK`, `None`, `""`, `REJECT`, `repair`, etc.
- VINs with embedded spaces and lowercase letters
- Supplier codes in mixed case
- ~15 intentional duplicate rows

## Q6: Why use only the standard library?
The project deliberately avoids third-party dependencies (pandas, numpy, etc.) to:
- Ensure zero-friction setup on any machine with Python 3.8+.
- Demonstrate understanding of core data structures and algorithms.
- Make the codebase easy to audit for correctness.

## Q7: How does the SQL case study relate to the pipeline?
The SQL files (`sql/`) present a separate but complementary Order Processing System. They demonstrate relational modelling (6 normalised tables), data population (10+ rows per table), and advanced SQL techniques (division queries, window functions, CTEs, ranking, running totals). Together with the Python pipeline, they showcase both programmatic and declarative data engineering skills.
