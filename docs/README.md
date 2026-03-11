# Car Manufacturing Data Cleaning & Transformation

## Overview
This project implements an end-to-end data cleaning and transformation pipeline for a car manufacturing company. Production data arrives from multiple systems (MES, IoT sensors, quality inspection, supplier systems, warranty systems) and contains various inconsistencies that must be resolved before analytics can be performed.

## Quick Start
```bash
python main.py
```
This single command generates the dataset, cleans it, validates records, removes duplicates, computes KPIs, and writes all output files.

## Project Structure
```
car_manufacturing_project/
├── main.py                          # Entry point
├── data/
│   └── raw_events.csv               # Generated raw dataset (300+ records)
├── output/
│   ├── cleaned_events.csv           # Cleaned & deduplicated records
│   ├── rejected_events.csv          # Invalid records with rejection reasons
│   └── metrics_report.csv           # Manufacturing KPIs
├── src/
│   ├── ingestion/
│   │   ├── dataset_generator.py     # Dev 1: Generates messy dataset
│   │   └── reader.py                # Dev 1: CSV read/write utilities
│   ├── cleaning/
│   │   └── cleaner.py               # Dev 2: All cleaning rules
│   ├── validation/
│   │   └── validator.py             # Dev 3: VIN, timestamp, field validation
│   ├── processing/
│   │   └── deduplicator.py          # Dev 4: Deduplication & rework detection
│   ├── analytics/
│   │   └── transformer.py           # Dev 5: KPI & metrics computation
│   └── pipeline/
│       └── pipeline_runner.py       # Pipeline orchestrator
├── sql/
│   ├── schema.sql                   # Order Processing schema
│   ├── inserts.sql                  # Sample data (10+ rows/table)
│   └── solutions.sql                # 10 advanced SQL queries
└── docs/
    ├── README.md                    # This file
    ├── architecture.md              # System architecture
    └── evaluation_questions.md      # Evaluator Q&A
```

## Team Responsibilities
| Developer | Module | Responsibility |
|-----------|--------|----------------|
| Dev 1 | `src/ingestion/` | Dataset generation & CSV I/O |
| Dev 2 | `src/cleaning/` | Data cleaning & normalisation |
| Dev 3 | `src/validation/` | Record validation & rejection |
| Dev 4 | `src/processing/` | Deduplication & rework detection |
| Dev 5 | `src/analytics/` | Manufacturing KPI computation |
| Dev 6 | `sql/` | SQL schema, data, & advanced queries |

## Pipeline Steps
1. **Generate** – Create 300+ records with intentional data quality issues
2. **Read** – Load raw CSV into memory
3. **Clean** – Normalise timestamps, units, casing, formatting
4. **Validate** – Check VIN format, required fields, data types
5. **Deduplicate** – Remove exact duplicates; detect rework loops
6. **Analyse** – Compute FPY, DPU, station metrics, supplier distribution
7. **Export** – Write cleaned data, rejected records, and metrics to CSV

## Dependencies
- Python 3.8+ (standard library only, no third-party packages required)
