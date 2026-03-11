# System Architecture

## High-Level Data Flow
```
Raw Data Sources        Pipeline Stages              Outputs
─────────────────    ─────────────────────    ──────────────────
MES                  ┌─────────────────┐
IoT Sensors      ──> │  1. Generator   │ ──> data/raw_events.csv
Quality System       └────────┬────────┘
Supplier System               │
Warranty System      ┌────────▼────────┐
                     │  2. Reader      │
                     └────────┬────────┘
                              │
                     ┌────────▼────────┐
                     │  3. Cleaner     │
                     └────────┬────────┘
                              │
                     ┌────────▼────────┐     output/rejected_events.csv
                     │  4. Validator   │ ──>
                     └────────┬────────┘
                              │
                     ┌────────▼────────┐
                     │  5. Deduplicator│
                     └────────┬────────┘
                              │
                     ┌────────▼────────┐     output/cleaned_events.csv
                     │  6. Analytics   │ ──> output/metrics_report.csv
                     └─────────────────┘
```

## Module Responsibilities

### 1. Ingestion (`src/ingestion/`)
- **dataset_generator.py** – Simulates messy production data from 5 different source systems. Generates 300 base records plus ~15 deliberate duplicates. Introduces noise: extra spaces, mixed timestamp formats, invalid minutes, mixed units, inconsistent casing, VIN formatting issues.
- **reader.py** – Thin CSV I/O layer using Python's `csv.DictReader`/`DictWriter`.

### 2. Cleaning (`src/cleaning/`)
- **cleaner.py** – Field-level normalisation functions. Handles multi-format timestamp parsing with heuristic disambiguation (DD/MM vs MM/DD). Clamps invalid minutes to 59. Converts Fahrenheit to Celsius. Strips unit suffixes from torque. Normalises defect codes, VIN, part numbers, supplier codes.

### 3. Validation (`src/validation/`)
- **validator.py** – Applies structural and format validation after cleaning. Checks VIN regex (17-char, no I/O/Q), timestamp format, required-field presence, numeric data types. Rejected rows receive a semicolon-delimited `Rejection_Reasons` field.

### 4. Processing (`src/processing/`)
- **deduplicator.py** – Uses a set-based approach on the composite key `(VIN, Station, TS)` to drop exact duplicates. Then performs rework detection: any `(VIN, Station)` pair appearing more than once is flagged with `Rework_Flag = True`.

### 5. Analytics (`src/analytics/`)
- **transformer.py** – Computes 10 KPI families: First Pass Yield, Defects Per Unit, defect frequency, average torque/temperature per station, production count per line, supplier distribution, rework rate, station utilisation, cycle time distribution.

### 6. Pipeline Orchestration (`src/pipeline/`)
- **pipeline_runner.py** – Wires all stages together, ensures directories exist, and prints a terminal summary with record counts and timing.

## Design Decisions
- **No external dependencies** – Uses only Python's standard library (`csv`, `re`, `collections`, `os`, `sys`, `time`, `random`, `string`) to eliminate installation friction.
- **Dict-based data model** – Each row is a plain `dict`. This keeps the codebase simple and avoids coupling to pandas or any ORM.
- **Fail-soft cleaning** – The cleaner always returns a row (possibly with issues noted). The validator is the gate that separates valid from rejected records.
- **Idempotent pipeline** – Running `python main.py` multiple times regenerates everything from scratch with the same random seed, producing deterministic output.
