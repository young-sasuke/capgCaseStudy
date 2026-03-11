"""Cleaning utilities for the manufacturing data pipeline."""

import re
from collections import defaultdict
from datetime import datetime

CALIBRATION_FACTOR = 1.02
TORQUE_MIN = 20
TORQUE_MAX = 120
TEMP_ANOMALY_THRESHOLD = 200
MAX_REASONABLE_CYCLE_MINUTES = 240
NORMALIZED_TS_FORMAT = "%Y-%m-%d %H:%M"
ACCEPTED_TS_FORMATS = [
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M",
    "%m-%d-%Y %H:%M",
    "%Y/%m/%d %H:%M",
    "%d-%m-%Y %H:%M",
]
STANDARDIZED_OUTPUT_COLUMNS = [
    "Event_ID",
    "Line",
    "Station",
    "TS",
    "Part_No",
    "Torque_Nm",
    "Temp_C",
    "Defect",
    "VIN",
    "Supplier",
]


def _string_value(value):
    return "" if value is None else str(value)


def _parse_normalized_timestamp(ts_value):
    try:
        return datetime.strptime(ts_value, NORMALIZED_TS_FORMAT)
    except (TypeError, ValueError):
        return None


def _fix_invalid_minutes(ts_raw):
    cleaned = _string_value(ts_raw).strip()
    match = re.search(r"(\d{1,2}):(\d{2})$", cleaned)
    if not match:
        return cleaned

    hour = int(match.group(1))
    minute = int(match.group(2))
    if minute <= 59:
        return cleaned

    return f"{cleaned[:match.start()]}{hour:02d}:{minute - 60:02d}"


def normalize_timestamp(ts_raw):
    """Normalize a timestamp to YYYY-MM-DD HH:MM and fix 06:70-like values."""
    candidate = _fix_invalid_minutes(ts_raw)
    for ts_format in ACCEPTED_TS_FORMATS:
        try:
            parsed = datetime.strptime(candidate, ts_format)
            return parsed.strftime(NORMALIZED_TS_FORMAT), True
        except ValueError:
            continue
    return candidate, False


def clean_line(value):
    return _string_value(value).strip().upper()


def clean_station(value):
    return _string_value(value).strip().upper()


def clean_part_number(value):
    part_number = _string_value(value).strip().upper()
    part_number = re.sub(r"\s+", "-", part_number)
    part_number = re.sub(r"^(\d+)([A-Z]+)$", r"\1-\2", part_number)
    return part_number


def parse_torque(value):
    match = re.search(r"-?\d+(?:\.\d+)?", _string_value(value))
    if not match:
        return None
    return round(float(match.group(0)), 2)


def parse_temperature(value):
    raw = _string_value(value).strip().upper()
    match = re.search(r"(-?\d+(?:\.\d+)?)\s*([CF]?)", raw)
    if not match:
        return None

    numeric_value = float(match.group(1))
    unit = match.group(2) or "C"
    if unit == "F":
        numeric_value = (numeric_value - 32) * 5 / 9
    return round(numeric_value, 2)


def clean_defect(value):
    normalized = _string_value(value).strip().lower()
    mapping = {
        "na": "None",
        "ok": "None",
        "none": "None",
        "": "None",
        "reject": "Reject",
        "repair": "Repair",
    }
    return mapping.get(normalized, _string_value(value).strip().title())


def clean_vin(value):
    return re.sub(r"\s+", "", _string_value(value)).upper()


def clean_supplier(value):
    return _string_value(value).strip().upper()


def derive_shift(ts_value):
    parsed = _parse_normalized_timestamp(ts_value)
    if parsed is None:
        return "Unknown"
    if 6 <= parsed.hour < 14:
        return "A"
    if 14 <= parsed.hour < 22:
        return "B"
    return "C"


def detect_sensor_anomaly(torque_nm, temp_c):
    anomaly = False
    if torque_nm is not None and (torque_nm < TORQUE_MIN or torque_nm > TORQUE_MAX):
        anomaly = True
    if temp_c is not None and temp_c > TEMP_ANOMALY_THRESHOLD:
        anomaly = True
    return anomaly


def clean_row(row):
    """Apply normalization and enrichment rules to a single event row."""
    cleaned = {"Event_ID": _string_value(row.get("Event_ID", "")).strip()}
    issues = []

    cleaned["Line"] = clean_line(row.get("Line", ""))
    cleaned["Station"] = clean_station(row.get("Station", ""))

    normalized_ts, ts_valid = normalize_timestamp(row.get("TS", ""))
    cleaned["TS"] = normalized_ts
    if not ts_valid:
        issues.append("invalid_timestamp")

    cleaned["Part_No"] = clean_part_number(row.get("Part_No", ""))

    raw_torque = parse_torque(row.get("Torque", ""))
    cleaned["Raw_Torque_Nm"] = raw_torque
    if raw_torque is None:
        cleaned["Torque_Nm"] = ""
        issues.append("invalid_torque")
    else:
        cleaned["Torque_Nm"] = round(raw_torque * CALIBRATION_FACTOR, 2)

    temp_c = parse_temperature(row.get("Temp", ""))
    if temp_c is None:
        cleaned["Temp_C"] = ""
        issues.append("invalid_temperature")
    else:
        cleaned["Temp_C"] = temp_c

    cleaned["Defect"] = clean_defect(row.get("Defect", ""))
    cleaned["VIN"] = clean_vin(row.get("VIN", ""))
    cleaned["Supplier"] = clean_supplier(row.get("Supplier", ""))
    cleaned["Shift"] = derive_shift(cleaned["TS"]) if ts_valid else "Unknown"
    cleaned["Sensor_Anomaly"] = detect_sensor_anomaly(
        cleaned["Torque_Nm"] if cleaned["Torque_Nm"] != "" else None,
        cleaned["Temp_C"] if cleaned["Temp_C"] != "" else None,
    )
    cleaned["Cycle_Time_Minutes"] = ""
    cleaned["Cycle_Time_Valid"] = True
    cleaned["Rework"] = False

    return cleaned, issues


def clean_dataset(rows):
    cleaned_rows = []
    issue_log = []
    for row in rows:
        cleaned_row, issues = clean_row(row)
        cleaned_rows.append(cleaned_row)
        if issues:
            issue_log.append({"Event_ID": cleaned_row.get("Event_ID", ""), "Issues": "; ".join(issues)})

    print(f"[Cleaner] Cleaned {len(cleaned_rows)} records, {len(issue_log)} rows need review")
    return cleaned_rows, issue_log


def remove_duplicate_events(rows):
    """Remove duplicates using VIN + Station + Timestamp."""
    unique_rows = []
    seen = set()
    duplicates_removed = 0

    for row in rows:
        key = (row.get("VIN", ""), row.get("Station", ""), row.get("TS", ""))
        if key in seen:
            duplicates_removed += 1
            continue
        seen.add(key)
        unique_rows.append(row)

    print(f"[Cleaner] Removed {duplicates_removed} duplicate events")
    return unique_rows, duplicates_removed


def validate_cycle_times(rows):
    """Flag implausible cycle times between consecutive station events for each VIN."""
    grouped_rows = defaultdict(list)
    for row in rows:
        grouped_rows[row.get("VIN", "")].append(row)

    invalid_count = 0
    for vin_rows in grouped_rows.values():
        ordered_rows = sorted(
            vin_rows,
            key=lambda item: _parse_normalized_timestamp(item.get("TS", "")) or datetime.max,
        )

        previous_ts = None
        for row in ordered_rows:
            current_ts = _parse_normalized_timestamp(row.get("TS", ""))
            if previous_ts is None or current_ts is None:
                row["Cycle_Time_Minutes"] = ""
                row["Cycle_Time_Valid"] = current_ts is not None
                if current_ts is None:
                    invalid_count += 1
                previous_ts = current_ts if current_ts is not None else previous_ts
                continue

            diff_minutes = round((current_ts - previous_ts).total_seconds() / 60.0, 2)
            row["Cycle_Time_Minutes"] = diff_minutes
            row["Cycle_Time_Valid"] = 0 <= diff_minutes <= MAX_REASONABLE_CYCLE_MINUTES
            if not row["Cycle_Time_Valid"]:
                invalid_count += 1
            previous_ts = current_ts

    print(f"[Cleaner] Flagged {invalid_count} implausible cycle-time events")
    return rows, invalid_count


def detect_rework_loops(rows):
    """Mark events involved in station revisit loops such as STN-05 -> STN-07 -> STN-05."""
    grouped_rows = defaultdict(list)
    for row in rows:
        row["Rework"] = False
        grouped_rows[row.get("VIN", "")].append(row)

    rework_units = set()
    for vin, vin_rows in grouped_rows.items():
        ordered_rows = sorted(
            vin_rows,
            key=lambda item: _parse_normalized_timestamp(item.get("TS", "")) or datetime.max,
        )
        last_station_position = {}
        for current_position, row in enumerate(ordered_rows):
            station = row.get("Station", "")
            if station in last_station_position and current_position - last_station_position[station] >= 2:
                start_position = last_station_position[station]
                for flagged_position in range(start_position, current_position + 1):
                    ordered_rows[flagged_position]["Rework"] = True
                rework_units.add(vin)
            last_station_position[station] = current_position

    rework_events = sum(1 for row in rows if row.get("Rework"))
    summary = {"rework_events": rework_events, "rework_units": len(rework_units)}
    print(f"[Cleaner] Marked {rework_events} rework events across {len(rework_units)} VINs")
    return rows, summary


def build_standardized_output(rows):
    """Project enriched rows to the required cleaned-dataset schema."""
    return [{column: row.get(column, "") for column in STANDARDIZED_OUTPUT_COLUMNS} for row in rows]
