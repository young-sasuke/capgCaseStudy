"""
Developer 4 - Deduplicator & Rework Detection
Removes duplicate production events and tags rework loops.
"""

from collections import Counter


def deduplicate(rows):
    """
    Remove duplicate events based on (VIN, Station, TS) composite key.
    Returns deduplicated rows list.
    """
    seen = set()
    unique_rows = []
    duplicates_removed = 0

    for row in rows:
        key = (row.get("VIN", ""), row.get("Station", ""), row.get("TS", ""))
        if key in seen:
            duplicates_removed += 1
            continue
        seen.add(key)
        unique_rows.append(row)

    print(f"[Deduplicator] Removed {duplicates_removed} duplicates, {len(unique_rows)} remaining")
    return unique_rows


def detect_rework(rows):
    """
    Detect rework loops: a VIN appearing at the same station more than once
    indicates rework.  Tags matching rows with Rework_Flag = True.
    Returns the rows (modified in place) and a rework summary.
    """
    # Count (VIN, Station) occurrences
    vin_station_counts = Counter()
    for row in rows:
        vin_station_counts[(row.get("VIN", ""), row.get("Station", ""))] += 1

    rework_keys = {k for k, v in vin_station_counts.items() if v > 1}
    rework_count = 0

    for row in rows:
        key = (row.get("VIN", ""), row.get("Station", ""))
        if key in rework_keys:
            row["Rework_Flag"] = "True"
            rework_count += 1
        else:
            row["Rework_Flag"] = "False"

    rework_vins = {k[0] for k in rework_keys}
    print(f"[Rework] Detected {len(rework_keys)} rework loops across {len(rework_vins)} VINs "
          f"({rework_count} events tagged)")
    return rows, {
        "rework_loops": len(rework_keys),
        "rework_vins": len(rework_vins),
        "rework_events": rework_count,
    }


def process(rows):
    """Run deduplication then rework detection."""
    deduped = deduplicate(rows)
    processed, rework_summary = detect_rework(deduped)
    return processed, rework_summary
