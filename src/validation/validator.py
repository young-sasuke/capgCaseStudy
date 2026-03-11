"""
Developer 3 - Data Validator
Validates cleaned records and separates valid from rejected rows.
"""

import re

# A simplified VIN regex: exactly 17 alphanumeric chars (no I, O, Q)
_VIN_PATTERN = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")

# Normalised timestamp format produced by the cleaner
_TS_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$")

# Required fields that must be non-empty after cleaning
_REQUIRED_FIELDS = ["Event_ID", "Line", "Station", "TS", "Part_No", "VIN", "Supplier"]


def validate_vin(vin):
    """Return True if the VIN is a valid 17-character format."""
    return bool(_VIN_PATTERN.match(vin))


def validate_timestamp(ts):
    """Return True if timestamp matches normalised format YYYY-MM-DD HH:MM."""
    return bool(_TS_PATTERN.match(ts))


def validate_row(row):
    """
    Validate a single cleaned row.
    Returns (is_valid, list_of_reasons).
    """
    reasons = []

    # Check required fields are present
    for field in _REQUIRED_FIELDS:
        if not row.get(field, "").strip():
            reasons.append(f"missing_{field}")

    # VIN format
    vin = row.get("VIN", "")
    if vin and not validate_vin(vin):
        reasons.append("invalid_vin_format")

    # Timestamp format
    ts = row.get("TS", "")
    if ts and not validate_timestamp(ts):
        reasons.append("invalid_timestamp_format")

    # Torque must be numeric (if present)
    torque = row.get("Torque_Nm", "")
    if torque != "":
        try:
            float(torque)
        except (ValueError, TypeError):
            reasons.append("invalid_torque")

    # Temperature must be numeric (if present)
    temp = row.get("Temp_C", "")
    if temp != "":
        try:
            float(temp)
        except (ValueError, TypeError):
            reasons.append("invalid_temperature")

    is_valid = len(reasons) == 0
    return is_valid, reasons


def validate_dataset(rows):
    """
    Validate the full cleaned dataset.
    Returns (valid_rows, rejected_rows).
    Each rejected row gets an extra 'Rejection_Reasons' field.
    """
    valid_rows = []
    rejected_rows = []

    for row in rows:
        is_valid, reasons = validate_row(row)
        if is_valid:
            valid_rows.append(row)
        else:
            rejected = dict(row)
            rejected["Rejection_Reasons"] = "; ".join(reasons)
            rejected_rows.append(rejected)

    print(f"[Validator] Valid: {len(valid_rows)} | Rejected: {len(rejected_rows)}")
    return valid_rows, rejected_rows
