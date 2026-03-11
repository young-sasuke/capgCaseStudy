"""
Developer 1 - Data Reader
Reads raw CSV data into a list of dictionaries for downstream processing.
"""

import csv
import os


def read_csv(file_path):
    """Read a CSV file and return rows as a list of dicts."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Dataset not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [dict(row) for row in reader]

    print(f"[Reader] Loaded {len(rows)} records from {file_path}")
    return rows


def write_csv(rows, file_path, fieldnames=None):
    """Write a list of dicts to a CSV file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
        if rows:
            writer.writerows(rows)

    print(f"[Writer] Wrote {len(rows)} records -> {file_path}")
