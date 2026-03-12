"""
Pipeline Runner
Orchestrates the full manufacturing data pipeline end-to-end.
"""

import json
import os
import sys
import time

# Ensure project root is on sys.path so imports work from main.py
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.ingestion.dataset_generator import generate_dataset
from src.ingestion.reader import read_csv, write_csv
from src.cleaning.cleaner import (
    build_standardized_output,
    clean_dataset,
    detect_rework_loops,
    remove_duplicate_events,
    validate_cycle_times,
)
from src.validation.validator import validate_dataset
from src.analytics.transformer import compute_anomaly_report, compute_metrics, summarize_metrics
from src.processing.traceability import build_traceability_chain


# ---------------------------------------------------------------------------
# Path constants (relative to project root)
# ---------------------------------------------------------------------------
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "raw_dataset.csv")
CLEANED_PATH = os.path.join(PROJECT_ROOT, "output", "cleaned_dataset.csv")
REJECTED_PATH = os.path.join(PROJECT_ROOT, "output", "rejected_events.csv")
METRICS_PATH = os.path.join(PROJECT_ROOT, "output", "metrics_report.csv")
ANOMALY_PATH = os.path.join(PROJECT_ROOT, "output", "anomaly_report.csv")
TRACEABILITY_PATH = os.path.join(PROJECT_ROOT, "output", "traceability_report.csv")
ANALYTICS_SUMMARY_PATH = os.path.join(PROJECT_ROOT, "output", "analytics_summary.json")


def _ensure_dirs():
    """Create data/ and output/ directories if they do not exist."""
    for d in [os.path.join(PROJECT_ROOT, "data"),
              os.path.join(PROJECT_ROOT, "output")]:
        os.makedirs(d, exist_ok=True)


def run_pipeline():
    """Execute the full pipeline and print a summary."""
    start = time.time()
    separator = "=" * 60

    print(separator)
    print("  CAR MANUFACTURING DATA PIPELINE")
    print(separator)

    # Step 0 – Setup
    _ensure_dirs()

    # Step 1 – Generate dataset
    print("\n[STEP 1] Generating raw dataset ...")
    generate_dataset(RAW_DATA_PATH, num_records=2000)

    # Step 2 – Read raw data
    print("\n[STEP 2] Reading raw data ...")
    raw_rows = read_csv(RAW_DATA_PATH)

    # Step 3 – Clean data
    print("\n[STEP 3] Cleaning data ...")
    cleaned_rows, issue_log = clean_dataset(raw_rows)

    # Step 4 – Validate data
    print("\n[STEP 4] Validating data ...")
    valid_rows, rejected_rows = validate_dataset(cleaned_rows)

    # Step 5 – Deduplicate records
    print("\n[STEP 5] Removing duplicate events ...")
    deduped_rows, duplicates_removed = remove_duplicate_events(valid_rows)

    # Re-evaluate downstream processing after deduplication so KPIs use final records.
    final_rows, invalid_cycle_events = validate_cycle_times(deduped_rows)
    final_rows, rework_summary = detect_rework_loops(final_rows)

    # Step 6 – Run analytics
    print("\n[STEP 6] Running analytics ...")
    metrics = compute_metrics(final_rows, rework_summary)
    analytics_summary = summarize_metrics(final_rows, rework_summary)
    anomaly_report = compute_anomaly_report(final_rows)

    # Step 7 – Build traceability chain
    print("\n[STEP 7] Building traceability chain ...")
    traceability_rows = build_traceability_chain(final_rows, TRACEABILITY_PATH)

    # Step 8 – Export outputs
    print("\n[STEP 8] Writing output files ...")
    cleaned_output_rows = build_standardized_output(final_rows)
    write_csv(cleaned_output_rows, CLEANED_PATH)
    rejected_fieldnames = list(rejected_rows[0].keys()) if rejected_rows else [
        "Event_ID",
        "Line",
        "Station",
        "TS",
        "Part_No",
        "Torque_Nm",
        "Temp_C",
        "Pressure",
        "Defect",
        "VIN",
        "Supplier",
        "Work_Order",
        "Tool_ID",
        "Inspection_Source",
        "Energy_kWh",
        "Plan_Production",
        "Shift",
        "Sensor_Anomaly",
        "Cycle_Time_Seconds",
        "Cycle_Time_Minutes",
        "Cycle_Time_Valid",
        "Rework",
        "Rejection_Reasons",
    ]
    write_csv(rejected_rows, REJECTED_PATH, fieldnames=rejected_fieldnames)
    write_csv(metrics, METRICS_PATH, fieldnames=["Metric", "Value"])
    anomaly_fieldnames = list(anomaly_report[0].keys()) if anomaly_report else [
        "Event_ID",
        "VIN",
        "Line",
        "Station",
        "TS",
        "Anomaly_Type",
        "Observed_Value",
        "Expected_Range",
    ]
    write_csv(anomaly_report, ANOMALY_PATH, fieldnames=anomaly_fieldnames)
    with open(ANALYTICS_SUMMARY_PATH, "w", encoding="utf-8") as handle:
        json.dump(analytics_summary, handle, indent=2)

    # ---------- Summary ----------
    elapsed = round(time.time() - start, 2)
    summary = {
        "raw_records": len(raw_rows),
        "cleaned_records": len(cleaned_rows),
        "valid_records": len(valid_rows),
        "rejected_records": len(rejected_rows),
        "duplicates_removed": duplicates_removed,
        "final_records": len(final_rows),
        "rework_events": rework_summary.get("rework_events", 0),
        "rework_units": rework_summary.get("rework_units", 0),
        "invalid_cycle_events": invalid_cycle_events,
        "metrics_computed": len(metrics),
        "traceability_records": len(traceability_rows),
        "anomaly_records": len(anomaly_report),
        "elapsed_seconds": elapsed,
    }
    print(f"\n{separator}")
    print("  PIPELINE SUMMARY")
    print(separator)
    print(f"  Raw records generated  : {summary['raw_records']}")
    print(f"  Records after cleaning : {summary['cleaned_records']}")
    print(f"  Valid records          : {summary['valid_records']}")
    print(f"  Rejected records       : {summary['rejected_records']}")
    print(f"  Duplicates removed     : {summary['duplicates_removed']}")
    print(f"  Final cleaned records  : {summary['final_records']}")
    print(f"  Rework events flagged  : {summary['rework_events']}")
    print(f"  Rework units flagged   : {summary['rework_units']}")
    print(f"  Invalid cycle events   : {summary['invalid_cycle_events']}")
    print(f"  Metrics computed       : {summary['metrics_computed']}")
    print(f"  Traceability rows      : {summary['traceability_records']}")
    print(f"  Anomaly rows           : {summary['anomaly_records']}")
    print(f"  Elapsed time           : {summary['elapsed_seconds']}s")
    print(separator)
    print(f"  Output files:")
    print(f"    -> {CLEANED_PATH}")
    print(f"    -> {RAW_DATA_PATH}")
    print(f"    -> {REJECTED_PATH}")
    print(f"    -> {METRICS_PATH}")
    print(f"    -> {ANOMALY_PATH}")
    print(f"    -> {TRACEABILITY_PATH}")
    print(f"    -> {ANALYTICS_SUMMARY_PATH}")
    print(separator)
    return summary
