"""Cleaning utilities for the manufacturing data pipeline."""

import csv
import os
import re
from collections import defaultdict
from datetime import datetime

CALIBRATION_FACTOR = 1.02
TORQUE_MIN = 30
TORQUE_MAX = 70
TEMP_MIN = 70
TEMP_MAX = 110
PRESSURE_MIN = 25
PRESSURE_MAX = 60
MIN_CYCLE_TIME_SECONDS = 5
MAX_CYCLE_TIME_SECONDS = 600
NORMALIZED_TS_FORMAT = "%Y-%m-%d %H:%M:%S"
ACCEPTED_TS_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m-%d-%Y %H:%M:%S",
    "%m-%d-%Y %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y %H:%M",
]
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SUPPLIER_MASTER_PATH = os.path.join(PROJECT_ROOT, "data", "supplier_master.csv")
BOM_MASTER = {
    "123-ABC": "Front Axle Assembly",
    "456-DEF": "Rear Suspension Module",
    "789-GHI": "Steering Rack",
    "101-JKL": "Battery Cooling Harness",
    "202-MNO": "Brake Cylinder",
    "303-PQR": "Door Lock Module",
}
SCRAP_REASON_MAP = {
    "S1": "Material Defect",
    "S2": "Torque Failure",
    "S3": "Assembly Error",
    "S4": "Sensor Fault",
}
TOOL_MASTER = {
    "T-101": "Fixture Clamp",
    "T-102": "Torque Gun",
    "T-103": "Vision Alignment Rig",
    "T-105": "Press Tool",
    "T-107": "Seal Applicator",
    "T-109": "End-of-Line Scanner",
}
INSPECTION_SOURCE_MAP = {
    "manual": "manual",
    "operator": "manual",
    "camera": "camera",
    "vision": "camera",
    "sensor": "sensor",
    "probe": "sensor",
}
STANDARDIZED_OUTPUT_COLUMNS = [
    "Event_ID",
    "Line",
    "Station",
    "TS",
    "Part_No",
    "Part_Description",
    "BOM_Valid",
    "Torque_Nm",
    "Temp_C",
    "Pressure",
    "Defect",
    "VIN",
    "Supplier",
    "Supplier_Name",
    "Supplier_Country",
    "Work_Order",
    "Work_Order_VIN_Valid",
    "Tool_ID",
    "Tool_Description",
    "Tool_Valid",
    "Inspection_Source",
    "Scrap_Reason",
    "Scrap_Reason_Description",
    "Energy_kWh",
    "Warranty_Claim",
    "Plan_Production",
    "Shift",
    "Sensor_Anomaly",
    "Cycle_Time_Seconds",
    "Cycle_Time_Minutes",
    "Cycle_Time_Valid",
    "Cycle_Time_Anomaly",
    "Rework",
    "Cleaning_Issues",
]


def _string_value(value):
    return "" if value is None else str(value)


def _parse_normalized_timestamp(ts_value):
    try:
        return datetime.strptime(_string_value(ts_value).strip(), NORMALIZED_TS_FORMAT)
    except (TypeError, ValueError):
        return None


def _append_issue(row, issue):
    issues = row.setdefault("_issues", [])
    if issue not in issues:
        issues.append(issue)


def _finalize_issues(row):
    issues = row.pop("_issues", [])
    row["Cleaning_Issues"] = "; ".join(sorted(issues))


def _fix_invalid_minutes(ts_raw):
    cleaned = _string_value(ts_raw).strip()
    match = re.search(r"(\d{1,2}):(\d{2})(?::(\d{2}))?$", cleaned)
    if not match:
        return cleaned

    hour = int(match.group(1))
    minute = int(match.group(2))
    second = int(match.group(3) or 0)
    if minute <= 59 and second <= 59:
        return cleaned

    safe_minute = minute if minute <= 59 else minute - 60
    safe_second = min(second, 59)
    time_fragment = f"{hour:02d}:{safe_minute:02d}:{safe_second:02d}" if match.group(3) else f"{hour:02d}:{safe_minute:02d}"
    return f"{cleaned[:match.start()]}{time_fragment}"


def normalize_timestamp(ts_raw):
    """Normalize a timestamp to YYYY-MM-DD HH:MM:SS."""
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


def parse_numeric(value):
    match = re.search(r"-?\d+(?:\.\d+)?", _string_value(value))
    if not match:
        return None
    return round(float(match.group(0)), 2)


def parse_torque(value):
    return parse_numeric(value)


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
    normalized = _string_value(value).strip().upper()
    normalized = re.sub(r"\s+", "", normalized)
    return normalized.replace("SUP", "SUP-", 1) if re.fullmatch(r"SUP\d{2}", normalized) else normalized


def clean_work_order(value):
    normalized = _string_value(value).strip().upper()
    normalized = re.sub(r"\s+", "-", normalized)
    return normalized


def clean_warranty_claim(value):
    normalized = _string_value(value).strip().lower()
    if normalized in {"yes", "y", "true", "1"}:
        return "Yes"
    return "No"


def derive_shift(ts_value):
    parsed = _parse_normalized_timestamp(ts_value)
    if parsed is None:
        return "Unknown"
    if 6 <= parsed.hour < 14:
        return "A"
    if 14 <= parsed.hour < 22:
        return "B"
    return "C"


def detect_sensor_anomaly(torque_nm, temp_c, pressure_value):
    if torque_nm is not None and (torque_nm < TORQUE_MIN or torque_nm > TORQUE_MAX):
        return True
    if temp_c is not None and (temp_c < TEMP_MIN or temp_c > TEMP_MAX):
        return True
    if pressure_value is not None and (pressure_value < PRESSURE_MIN or pressure_value > PRESSURE_MAX):
        return True
    return False


def _load_supplier_master(supplier_master_path=SUPPLIER_MASTER_PATH):
    if not os.path.exists(supplier_master_path):
        return {}

    supplier_master = {}
    with open(supplier_master_path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            code = clean_supplier(row.get("Supplier_Code", ""))
            supplier_master[code] = {
                "Supplier_Name": row.get("Supplier_Name", "").strip(),
                "Supplier_Country": row.get("Country", "").strip(),
            }
    return supplier_master


def clean_row(row):
    """Apply normalization and enrichment rules to a single event row."""
    cleaned = {"Event_ID": _string_value(row.get("Event_ID", "")).strip()}

    cleaned["Line"] = clean_line(row.get("Line", ""))
    cleaned["Station"] = clean_station(row.get("Station", ""))

    normalized_ts, ts_valid = normalize_timestamp(row.get("TS", ""))
    cleaned["TS"] = normalized_ts
    if not ts_valid:
        _append_issue(cleaned, "invalid_timestamp")

    cleaned["Part_No"] = clean_part_number(row.get("Part_No", ""))
    cleaned["Part_Description"] = ""
    cleaned["BOM_Valid"] = True

    raw_torque = parse_torque(row.get("Torque", ""))
    cleaned["Raw_Torque_Nm"] = raw_torque
    if raw_torque is None:
        cleaned["Torque_Nm"] = ""
        _append_issue(cleaned, "invalid_torque")
    else:
        cleaned["Torque_Nm"] = round(raw_torque * CALIBRATION_FACTOR, 2)

    temp_c = parse_temperature(row.get("Temp", ""))
    if temp_c is None:
        cleaned["Temp_C"] = ""
        _append_issue(cleaned, "invalid_temperature")
    else:
        cleaned["Temp_C"] = temp_c

    pressure_value = parse_numeric(row.get("Pressure", ""))
    if pressure_value is None:
        cleaned["Pressure"] = ""
        _append_issue(cleaned, "invalid_pressure")
    else:
        cleaned["Pressure"] = pressure_value

    energy_value = parse_numeric(row.get("Energy_kWh", ""))
    if energy_value is None:
        cleaned["Energy_kWh"] = ""
        _append_issue(cleaned, "invalid_energy")
    else:
        cleaned["Energy_kWh"] = energy_value

    plan_value = parse_numeric(row.get("Plan_Production", ""))
    cleaned["Plan_Production"] = int(plan_value) if plan_value is not None else ""
    if plan_value is None:
        _append_issue(cleaned, "invalid_plan_production")

    cleaned["Defect"] = clean_defect(row.get("Defect", ""))
    cleaned["VIN"] = clean_vin(row.get("VIN", ""))
    cleaned["Supplier"] = clean_supplier(row.get("Supplier", ""))
    cleaned["Supplier_Name"] = ""
    cleaned["Supplier_Country"] = ""
    cleaned["Work_Order"] = clean_work_order(row.get("Work_Order", ""))
    cleaned["Work_Order_VIN_Valid"] = True
    cleaned["Tool_ID"] = _string_value(row.get("Tool_ID", "")).strip().upper().replace(" ", "")
    cleaned["Tool_Description"] = ""
    cleaned["Tool_Valid"] = True
    cleaned["Inspection_Source"] = _string_value(row.get("Inspection_Source", "")).strip().lower()
    cleaned["Scrap_Reason"] = _string_value(row.get("Scrap_Reason", "")).strip().upper()
    cleaned["Scrap_Reason_Description"] = ""
    cleaned["Warranty_Claim"] = clean_warranty_claim(row.get("Warranty_Claim", ""))
    cleaned["Shift"] = derive_shift(cleaned["TS"]) if ts_valid else "Unknown"
    cleaned["Sensor_Anomaly"] = detect_sensor_anomaly(
        cleaned["Torque_Nm"] if cleaned["Torque_Nm"] != "" else None,
        cleaned["Temp_C"] if cleaned["Temp_C"] != "" else None,
        cleaned["Pressure"] if cleaned["Pressure"] != "" else None,
    )
    cleaned["Cycle_Time_Seconds"] = ""
    cleaned["Cycle_Time_Minutes"] = ""
    cleaned["Cycle_Time_Valid"] = True
    cleaned["Cycle_Time_Anomaly"] = False
    cleaned["Rework"] = False
    cleaned["Cleaning_Issues"] = ""
    return cleaned


def validate_bom(rows):
    invalid_parts = 0
    for row in rows:
        part_no = row.get("Part_No", "")
        row["BOM_Valid"] = part_no in BOM_MASTER
        row["Part_Description"] = BOM_MASTER.get(part_no, "")
        if not row["BOM_Valid"]:
            invalid_parts += 1
            _append_issue(row, "invalid_bom_part")
    return rows, invalid_parts


def join_supplier_master(rows, supplier_master_path=SUPPLIER_MASTER_PATH):
    supplier_master = _load_supplier_master(supplier_master_path)
    unmatched = 0
    for row in rows:
        supplier = row.get("Supplier", "")
        master = supplier_master.get(supplier)
        if master is None:
            unmatched += 1
            _append_issue(row, "unknown_supplier")
            row["Supplier_Name"] = "Unknown Supplier"
            row["Supplier_Country"] = ""
            continue
        row["Supplier_Name"] = master["Supplier_Name"]
        row["Supplier_Country"] = master["Supplier_Country"]
    return rows, unmatched


def normalize_inspection_sources(rows):
    invalid_sources = 0
    for row in rows:
        raw_source = row.get("Inspection_Source", "")
        normalized = INSPECTION_SOURCE_MAP.get(raw_source.strip().lower())
        if normalized is None:
            invalid_sources += 1
            normalized = "manual"
            _append_issue(row, "invalid_inspection_source")
        row["Inspection_Source"] = normalized
    return rows, invalid_sources


def map_scrap_reasons(rows):
    invalid_codes = 0
    for row in rows:
        code = row.get("Scrap_Reason", "")
        if not code:
            row["Scrap_Reason_Description"] = ""
            continue
        normalized = code.upper()
        description = SCRAP_REASON_MAP.get(normalized)
        if description is None:
            invalid_codes += 1
            _append_issue(row, "invalid_scrap_reason")
            row["Scrap_Reason"] = normalized
            row["Scrap_Reason_Description"] = ""
            continue
        row["Scrap_Reason"] = normalized
        row["Scrap_Reason_Description"] = description
    return rows, invalid_codes


def validate_tool_ids(rows):
    invalid_tools = 0
    for row in rows:
        tool_id = row.get("Tool_ID", "")
        tool_valid = bool(re.fullmatch(r"T-\d{3}", tool_id)) and tool_id in TOOL_MASTER
        row["Tool_Valid"] = tool_valid
        row["Tool_Description"] = TOOL_MASTER.get(tool_id, "")
        if not tool_valid:
            invalid_tools += 1
            _append_issue(row, "invalid_tool_id")
    return rows, invalid_tools


def validate_work_order_vin_mapping(rows):
    work_order_to_vin = {}
    mismatches = 0
    for row in rows:
        work_order = row.get("Work_Order", "")
        vin = row.get("VIN", "")
        if not work_order or not vin:
            row["Work_Order_VIN_Valid"] = False
            mismatches += 1
            _append_issue(row, "invalid_work_order_vin_mapping")
            continue
        existing_vin = work_order_to_vin.get(work_order)
        if existing_vin is None:
            work_order_to_vin[work_order] = vin
            row["Work_Order_VIN_Valid"] = True
            continue
        row["Work_Order_VIN_Valid"] = existing_vin == vin
        if not row["Work_Order_VIN_Valid"]:
            mismatches += 1
            _append_issue(row, "invalid_work_order_vin_mapping")
    return rows, mismatches


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
            if previous_ts is None:
                row["Cycle_Time_Seconds"] = ""
                row["Cycle_Time_Minutes"] = ""
                row["Cycle_Time_Valid"] = current_ts is not None
                row["Cycle_Time_Anomaly"] = current_ts is None
                if current_ts is None:
                    invalid_count += 1
                    _append_issue(row, "invalid_cycle_time")
                previous_ts = current_ts
                continue

            if current_ts is None:
                row["Cycle_Time_Seconds"] = ""
                row["Cycle_Time_Minutes"] = ""
                row["Cycle_Time_Valid"] = False
                row["Cycle_Time_Anomaly"] = True
                invalid_count += 1
                _append_issue(row, "invalid_cycle_time")
                continue

            diff_seconds = round((current_ts - previous_ts).total_seconds(), 2)
            row["Cycle_Time_Seconds"] = diff_seconds
            row["Cycle_Time_Minutes"] = round(diff_seconds / 60.0, 2)
            row["Cycle_Time_Valid"] = MIN_CYCLE_TIME_SECONDS <= diff_seconds <= MAX_CYCLE_TIME_SECONDS
            row["Cycle_Time_Anomaly"] = not row["Cycle_Time_Valid"]
            if row["Cycle_Time_Anomaly"]:
                invalid_count += 1
                _append_issue(row, "invalid_cycle_time")
            previous_ts = current_ts

    return rows, invalid_count


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


def clean_dataset(rows):
    cleaned_rows = [clean_row(row) for row in rows]

    cleaned_rows, invalid_parts = validate_bom(cleaned_rows)
    cleaned_rows, unmatched_suppliers = join_supplier_master(cleaned_rows)
    cleaned_rows, invalid_sources = normalize_inspection_sources(cleaned_rows)
    cleaned_rows, invalid_scrap_codes = map_scrap_reasons(cleaned_rows)
    cleaned_rows, invalid_tools = validate_tool_ids(cleaned_rows)
    cleaned_rows, work_order_mismatches = validate_work_order_vin_mapping(cleaned_rows)
    cleaned_rows, invalid_cycle_events = validate_cycle_times(cleaned_rows)

    issue_log = []
    for row in cleaned_rows:
        _finalize_issues(row)
        if row["Cleaning_Issues"]:
            issue_log.append({"Event_ID": row.get("Event_ID", ""), "Issues": row["Cleaning_Issues"]})

    print(
        "[Cleaner] Cleaned "
        f"{len(cleaned_rows)} records | "
        f"BOM flags={invalid_parts}, supplier misses={unmatched_suppliers}, "
        f"inspection fixes={invalid_sources}, scrap issues={invalid_scrap_codes}, "
        f"tool flags={invalid_tools}, WO/VIN mismatches={work_order_mismatches}, "
        f"cycle anomalies={invalid_cycle_events}"
    )
    return cleaned_rows, issue_log


def build_standardized_output(rows):
    """Project enriched rows to the required cleaned-dataset schema."""
    return [{column: row.get(column, "") for column in STANDARDIZED_OUTPUT_COLUMNS} for row in rows]
