# System Architecture

## High-Level Data Flow
```
Raw Sources                 Pipeline Stages                  Outputs
────────────────────      ───────────────────────      ─────────────────────────────
MES events                1. Dataset generator         data/raw_dataset.csv
Quality checks       -->  2. CSV reader               output/cleaned_dataset.csv
Supplier master           3. Cleaning + enrichment    output/rejected_events.csv
Warranty signals          4. Validation               output/metrics_report.csv
Tool / BOM reference      5. Deduplication            output/analytics_summary.json
                           6. Analytics + anomalies   output/anomaly_report.csv
                           7. Traceability            output/traceability_report.csv
                           8. Export
```

## Module Responsibilities

### Ingestion (`src/ingestion/`)
- `dataset_generator.py` produces synthetic manufacturing events with operational, quality, and traceability fields such as `Work_Order`, `Tool_ID`, `Inspection_Source`, `Scrap_Reason`, `Energy_kWh`, `Pressure`, `Warranty_Claim`, and `Plan_Production`.
- `generator.py` is a compatibility wrapper that re-exports `generate_dataset`.
- `reader.py` remains the thin CSV I/O layer used by the pipeline.

### Cleaning (`src/cleaning/`)
- `cleaner.py` performs field normalization, unit conversion, master-data joins, BOM checks, tool validation, scrap-reason mapping, inspection-source normalization, work-order to VIN consistency checks, and cycle-time plausibility checks.
- Cleaning is fail-soft: records are enriched with flags such as `BOM_Valid`, `Tool_Valid`, `Work_Order_VIN_Valid`, and `Cycle_Time_Anomaly` instead of being discarded immediately.

### Validation (`src/validation/`)
- `validator.py` rejects rows that are structurally unusable after cleaning, including invalid VINs, timestamps, missing required fields, and malformed numeric fields such as torque, temperature, pressure, energy, and planned production.

### Processing (`src/processing/`)
- `deduplicator.py` preserves the existing duplicate-removal utility.
- `traceability.py` builds the supplier-to-warranty lineage table: `Supplier -> Part -> VIN -> Warranty Claim`.

### Analytics (`src/analytics/`)
- `transformer.py` now computes manufacturing KPIs including OEE, cycle-time distribution, bottleneck detection, MTBF, MTTR, SPC metrics, COPQ, energy-per-vehicle, shift comparison, line-balance efficiency, and anomaly detection.

### Pipeline (`src/pipeline/`)
- `pipeline_runner.py` coordinates the full flow used by `python main.py`.
- `run_pipeline.py` is a compatibility wrapper for the existing runner entry point.

## Data Cleaning Pipeline

The cleaning layer combines row-level normalization with dataset-level consistency checks:

1. Timestamp normalization supports multiple source formats and standardizes them to `YYYY-MM-DD HH:MM:SS`.
2. Torque, temperature, pressure, and energy values are converted from noisy string inputs into numeric values.
3. Supplier codes are normalized and enriched from `data/supplier_master.csv`.
4. `Part_No` values are validated against an in-code BOM master.
5. `Tool_ID` values are validated against the `T-###` pattern and a tool master dictionary.
6. `Inspection_Source` values are normalized to `manual`, `camera`, or `sensor`.
7. `Scrap_Reason` codes are mapped to business-readable defect causes.
8. Sequential timestamps per VIN are used to calculate `Cycle_Time_Seconds` and flag implausible transitions.
9. `Work_Order` to `VIN` mapping is checked across the dataset for consistency.

## Manufacturing KPIs

The analytics layer produces both a flat KPI export and a structured JSON summary.

- **OEE per line** uses availability, performance, and quality from planned production, runtime, and good-unit counts.
- **Cycle time distribution** provides minimum, maximum, average, and standard deviation per station.
- **Bottleneck detection** identifies the station with the highest average cycle time.
- **MTBF / MTTR** estimate reliability and repair duration from defect and rework behavior.
- **SPC control metrics** summarize mean and range for `Torque_Nm` and `Temp_C`.
- **COPQ** converts rework and scrap counts into financial impact.
- **Energy per vehicle** shows energy intensity of production.
- **Shift performance** compares production volume, defect rate, and average cycle time by shift.
- **Line balance efficiency** compares actual cycle time with takt time.

## Traceability System

Traceability is implemented as a dedicated processing step after analytics-ready records are finalized.

- Supplier enrichment supplies supplier code, name, and country.
- BOM enrichment supplies the part identifier and description.
- Vehicle lineage is carried by `VIN` and `Work_Order`.
- Warranty exposure is linked through the `Warranty_Claim` field.

The resulting output is written to `output/traceability_report.csv`, enabling downstream supplier and warranty analysis.

## Anomaly Detection

The anomaly report is generated from finalized, deduplicated records and focuses on operational outliers:

- Torque outside `30-70 Nm`
- Temperature outside `70-110 C`
- Pressure outside `25-60 psi`
- Cycle time outside `5-600 seconds`

Each anomaly record includes event, VIN, line, station, timestamp, observed value, and expected range so that quality teams can trace the issue back to the originating event.

## Design Decisions
- **No external dependencies**: the project still uses only the Python standard library.
- **Dict-based data model**: all stages operate on plain dictionaries for low coupling and easy CSV export.
- **Backward-compatible orchestration**: `python main.py` still drives the full pipeline.
- **Deterministic synthetic data**: the generator keeps a fixed random seed for reproducible runs.
