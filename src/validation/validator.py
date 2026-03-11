"""Validation helpers for cleaned manufacturing events."""

import re
from datetime import datetime

_VIN_PATTERN = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")
_REQUIRED_FIELDS = [
    "Event_ID",
    "Line",
    "Station",
    "TS",
    "Part_No",
    "Torque_Nm",
    "Temp_C",
    "VIN",
    "Supplier",
]


def _string_value(value):
    return "" if value is None else str(value).strip()


def validate_vin(vin):
    """Return True if the VIN is a valid 17-character identifier."""
    return bool(_VIN_PATTERN.fullmatch(_string_value(vin)))


def validate_timestamp(ts):
    """Return True if a normalized timestamp is valid."""
    value = _string_value(ts)
    try:
        datetime.strptime(value, "%Y-%m-%d %H:%M")
        return True
    except ValueError:
        return False


def validate_row(row):
    """Validate a cleaned row and return status plus rejection reasons."""
    reasons = []

    for field in _REQUIRED_FIELDS:
        if _string_value(row.get(field, "")) == "":
            reasons.append(f"missing_{field}")

    vin_value = row.get("VIN", "")
    if _string_value(vin_value) and not validate_vin(vin_value):
        reasons.append("invalid_vin_format")

    ts_value = row.get("TS", "")
    if _string_value(ts_value) and not validate_timestamp(ts_value):
        reasons.append("invalid_timestamp")

    try:
        float(row.get("Torque_Nm", ""))
    except (TypeError, ValueError):
        reasons.append("invalid_torque")

    try:
        float(row.get("Temp_C", ""))
    except (TypeError, ValueError):
        reasons.append("invalid_temperature")

    return len(reasons) == 0, sorted(set(reasons))


def validate_dataset(rows):
    """Split cleaned rows into valid and rejected event sets."""
    valid_rows = []
    rejected_rows = []

    for row in rows:
        is_valid, reasons = validate_row(row)
        if is_valid:
            valid_rows.append(row)
            continue

        rejected_row = dict(row)
        rejected_row["Rejection_Reasons"] = "; ".join(reasons)
        rejected_rows.append(rejected_row)

    print(f"[Validator] Valid: {len(valid_rows)} | Rejected: {len(rejected_rows)}")
    return valid_rows, rejected_rows
