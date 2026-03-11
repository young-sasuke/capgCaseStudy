"""
Pipeline Runner
Orchestrates the full manufacturing data pipeline end-to-end.
"""

import os
import sys
import time

# Ensure project root is on sys.path so imports work from main.py
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.ingestion.dataset_generator import generate_dataset
from src.ingestion.reader import read_csv, write_csv
from src.cleaning.cleaner import clean_dataset
from src.validation.validator import validate_dataset
from src.processing.deduplicator import process as dedup_process
from src.analytics.transformer import compute_metrics


# ---------------------------------------------------------------------------
# Path constants (relative to project root)
# ---------------------------------------------------------------------------
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "raw_events.csv")
CLEANED_PATH = os.path.join(PROJECT_ROOT, "output", "cleaned_events.csv")
REJECTED_PATH = os.path.join(PROJECT_ROOT, "output", "rejected_events.csv")
METRICS_PATH = os.path.join(PROJECT_ROOT, "output", "metrics_report.csv")


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
    generate_dataset(RAW_DATA_PATH, num_records=300)

    # Step 2 – Read raw data
    print("\n[STEP 2] Reading raw data ...")
    raw_rows = read_csv(RAW_DATA_PATH)

    # Step 3 – Clean
    print("\n[STEP 3] Cleaning data ...")
    cleaned_rows, issue_log = clean_dataset(raw_rows)

    # Step 4 – Validate
    print("\n[STEP 4] Validating data ...")
    valid_rows, rejected_rows = validate_dataset(cleaned_rows)

    # Step 5 – Deduplicate & detect rework
    print("\n[STEP 5] Deduplicating & detecting rework ...")
    final_rows, rework_summary = dedup_process(valid_rows)

    # Step 6 – Compute analytics
    print("\n[STEP 6] Computing analytics ...")
    metrics = compute_metrics(final_rows, rework_summary)

    # Step 7 – Write outputs
    print("\n[STEP 7] Writing output files ...")
    write_csv(final_rows, CLEANED_PATH)
    write_csv(rejected_rows, REJECTED_PATH)
    write_csv(metrics, METRICS_PATH, fieldnames=["Metric", "Value"])

    # ---------- Summary ----------
    elapsed = round(time.time() - start, 2)
    print(f"\n{separator}")
    print("  PIPELINE SUMMARY")
    print(separator)
    print(f"  Raw records generated  : {len(raw_rows)}")
    print(f"  Records after cleaning : {len(cleaned_rows)}")
    print(f"  Valid records          : {len(valid_rows)}")
    print(f"  Rejected records       : {len(rejected_rows)}")
    print(f"  After deduplication    : {len(final_rows)}")
    print(f"  Rework loops detected  : {rework_summary.get('rework_loops', 0)}")
    print(f"  Rework VINs            : {rework_summary.get('rework_vins', 0)}")
    print(f"  Metrics computed       : {len(metrics)}")
    print(f"  Elapsed time           : {elapsed}s")
    print(separator)
    print(f"  Output files:")
    print(f"    -> {CLEANED_PATH}")
    print(f"    -> {REJECTED_PATH}")
    print(f"    -> {METRICS_PATH}")
    print(separator)
