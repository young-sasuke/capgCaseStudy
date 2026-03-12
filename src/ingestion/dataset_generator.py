"""
Manufacturing dataset generator.

Creates intentionally messy raw production events with enough operational
signals to exercise the downstream cleaning, traceability, and KPI logic.
"""

import csv
import os
import random
import string
from datetime import datetime, timedelta

random.seed(42)

LINES = ["GA-1", "GA-2", "GA-3"]
STATIONS = ["STN-01", "STN-02", "STN-03", "STN-05", "STN-07", "STN-09"]
PART_NUMBERS = ["123-ABC", "456-DEF", "789-GHI", "101-JKL", "202-MNO", "303-PQR"]
SUPPLIERS = ["SUP-01", "SUP-02", "SUP-05", "SUP-09", "SUP-11"]
INSPECTION_SOURCES = ["manual", "camera", "sensor"]
SCRAP_CODES = ["", "", "", "S1", "S2", "S3", "S4"]
TIMESTAMP_FORMATS = [
    "{y}-{m:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}",
    "{d:02d}/{m:02d}/{y} {h:02d}:{mi:02d}:{s:02d}",
    "{m:02d}-{d:02d}-{y} {h:02d}:{mi:02d}:{s:02d}",
    "{y}/{m:02d}/{d:02d} {h:02d}:{mi:02d}:{s:02d}",
    "{d:02d}-{m:02d}-{y} {h:02d}:{mi:02d}:{s:02d}",
    "{y}-{m:02d}-{d:02d} {h:02d}:{mi:02d}",
]
INVALID_TIMESTAMP_VALUES = [
    "BAD_TS",
    "2025/13/40 25:99:99",
    "2025-02-30 09:15:75",
    "NOT_A_TIMESTAMP",
]
TOOL_BY_STATION = {
    "STN-01": "T-101",
    "STN-02": "T-102",
    "STN-03": "T-103",
    "STN-05": "T-105",
    "STN-07": "T-107",
    "STN-09": "T-109",
}
PLAN_BY_LINE = {"GA-1": 120, "GA-2": 110, "GA-3": 115}
FIELDNAMES = [
    "Event_ID",
    "Line",
    "Station",
    "TS",
    "Part_No",
    "Torque",
    "Temp",
    "Defect",
    "VIN",
    "Supplier",
    "Work_Order",
    "Tool_ID",
    "Inspection_Source",
    "Scrap_Reason",
    "Energy_kWh",
    "Pressure",
    "Warranty_Claim",
    "Plan_Production",
]


def _base_vin():
    chars = string.ascii_uppercase.replace("I", "").replace("O", "").replace("Q", "") + string.digits
    return "".join(random.choices(chars, k=17))


def _invalid_vin():
    vin = _base_vin()
    choice = random.choice(["short", "forbidden", "long"])
    if choice == "short":
        return vin[:14]
    if choice == "forbidden":
        return vin[:8] + random.choice("IOQ") + vin[9:]
    return vin + "7X"


def _space_noise(value):
    style = random.choice(["clean", "left", "right", "both"])
    if style == "left":
        return f"  {value}"
    if style == "right":
        return f"{value}  "
    if style == "both":
        return f" {value} "
    return value


def _mixed_case_part_number(part_number):
    return random.choice(
        [
            part_number.lower(),
            part_number[:4] + part_number[4:].lower(),
            part_number.replace("-", ""),
            part_number.replace("-", " "),
            part_number,
        ]
    )


def _mixed_case_supplier(supplier):
    return random.choice([supplier, supplier.lower(), supplier.upper(), supplier.title()])


def _messy_vin(vin):
    vin_chars = list(vin)
    for _ in range(random.randint(1, 3)):
        vin_chars.insert(random.randint(1, len(vin_chars) - 1), " ")
    messy = "".join(vin_chars)
    if random.random() < 0.35:
        messy = "".join(char.lower() if random.random() < 0.25 else char for char in messy)
    return messy


def _messy_work_order(work_order):
    return random.choice([work_order, work_order.lower(), f" {work_order} ", work_order.replace("-", " ")])


def _messy_tool_id(tool_id):
    return random.choice([tool_id, tool_id.lower(), f" {tool_id} ", tool_id.replace("-", "")])


def _messy_inspection_source(value):
    variants = {
        "manual": ["manual", "Manual", " MANUAL ", "operator"],
        "camera": ["camera", "Camera", " vision ", "CAMERA"],
        "sensor": ["sensor", "Sensor", " SENSOR ", "probe"],
    }
    return random.choice(variants[value])


def _render_timestamp(dt_value, force_invalid_minutes=False):
    minute = dt_value.minute
    second = dt_value.second
    if force_invalid_minutes:
        minute = min(99, minute + random.randint(60, 70))
    template = random.choice(TIMESTAMP_FORMATS)
    return template.format(
        y=dt_value.year,
        m=dt_value.month,
        d=dt_value.day,
        h=dt_value.hour,
        mi=minute,
        s=second,
    )


def _raw_torque():
    if random.random() < 0.08:
        value = random.choice([random.uniform(18, 29.5), random.uniform(70.5, 88)])
    else:
        value = random.uniform(38, 64)
    value = round(value, 1)
    return f"{value} Nm" if random.random() < 0.2 else str(value)


def _raw_temperature():
    if random.random() < 0.06:
        celsius = round(random.choice([random.uniform(48, 69), random.uniform(111, 135)]), 1)
    else:
        celsius = round(random.uniform(78, 102), 1)

    style = random.choice(["c_space", "c_compact", "fahrenheit"])
    if style == "c_compact":
        return f"{celsius}C"
    if style == "fahrenheit":
        fahrenheit = round(celsius * 9 / 5 + 32, 1)
        return f"{fahrenheit}F"
    return f"{celsius} C"


def _raw_pressure():
    if random.random() < 0.06:
        psi = round(random.choice([random.uniform(18, 24), random.uniform(61, 72)]), 1)
    else:
        psi = round(random.uniform(32, 54), 1)
    return random.choice([f"{psi} psi", f"{psi} PSI", str(psi)])


def _raw_energy(station):
    base = {
        "STN-01": (2.0, 3.5),
        "STN-02": (2.2, 3.8),
        "STN-03": (2.8, 4.6),
        "STN-05": (3.6, 5.8),
        "STN-07": (2.4, 4.0),
        "STN-09": (1.8, 3.2),
    }[station]
    value = round(random.uniform(*base), 2)
    return random.choice([str(value), f"{value} kWh"])


def _raw_defect(rework_route=False):
    populations = ["None", "OK", "na", "Repair", "Reject"]
    weights = [52, 22, 10, 10, 6] if rework_route else [63, 20, 10, 4, 3]
    defect = random.choices(populations, weights=weights, k=1)[0]
    if random.random() < 0.08:
        defect = f" {defect}"
    return defect


def _scrap_reason(defect_value):
    normalized = defect_value.strip().lower()
    if normalized == "reject":
        return random.choice(["S1", "S2", "S3", "S4", "s2"])
    if normalized == "repair" and random.random() < 0.35:
        return random.choice(["S2", "S3"])
    return random.choice(SCRAP_CODES)


def _warranty_claim(defect_value):
    normalized = defect_value.strip().lower()
    value = normalized in {"reject", "repair"} and random.random() < 0.18
    return random.choice(["Yes", "YES", "No", "no", "N", "Y"]) if random.random() < 0.08 else ("Yes" if value else "No")


def _make_event(
    line,
    station,
    ts_value,
    part_no,
    supplier,
    vin,
    work_order,
    messy=True,
    rework_route=False,
):
    defect = _raw_defect(rework_route=rework_route)
    return {
        "Event_ID": "",
        "Line": _space_noise(line) if messy else line,
        "Station": _space_noise(station) if messy else station,
        "TS": _render_timestamp(ts_value, force_invalid_minutes=messy and random.random() < 0.08),
        "Part_No": _mixed_case_part_number(part_no) if messy and random.random() < 0.6 else part_no,
        "Torque": _raw_torque(),
        "Temp": _raw_temperature(),
        "Defect": defect,
        "VIN": _messy_vin(vin) if messy and random.random() < 0.7 else vin,
        "Supplier": _mixed_case_supplier(supplier),
        "Work_Order": _messy_work_order(work_order) if messy and random.random() < 0.4 else work_order,
        "Tool_ID": _messy_tool_id(TOOL_BY_STATION[station]) if messy and random.random() < 0.35 else TOOL_BY_STATION[station],
        "Inspection_Source": _messy_inspection_source(random.choice(INSPECTION_SOURCES)),
        "Scrap_Reason": _scrap_reason(defect),
        "Energy_kWh": _raw_energy(station),
        "Pressure": _raw_pressure(),
        "Warranty_Claim": _warranty_claim(defect),
        "Plan_Production": str(PLAN_BY_LINE[line] + random.randint(-5, 5)),
    }


def _generate_vehicle_events(vehicle_count=300, rework_vehicle_count=80):
    rows = []
    rework_vehicles = set(random.sample(range(vehicle_count), rework_vehicle_count))

    for vehicle_index in range(vehicle_count):
        line = random.choice(LINES)
        supplier = random.choice(SUPPLIERS)
        vin = _base_vin()
        work_order = f"WO-{2025000 + vehicle_index:07d}"
        part_no = random.choice(PART_NUMBERS)
        current_ts = datetime(
            year=2025,
            month=random.randint(1, 12),
            day=random.randint(1, 25),
            hour=random.randint(5, 20),
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        )

        route = ["STN-01", "STN-02", "STN-03", "STN-05", "STN-07", "STN-05", "STN-09"] if vehicle_index in rework_vehicles else list(STATIONS)

        for step_index, station in enumerate(route):
            if step_index > 0:
                gap_seconds = random.randint(45, 420)
                if random.random() < 0.04:
                    gap_seconds = random.choice([random.randint(1, 4), random.randint(601, 900)])
                current_ts += timedelta(seconds=gap_seconds)

            rows.append(
                _make_event(
                    line=line,
                    station=station,
                    ts_value=current_ts,
                    part_no=part_no,
                    supplier=supplier,
                    vin=vin,
                    work_order=work_order,
                    messy=random.random() < 0.8,
                    rework_route=vehicle_index in rework_vehicles,
                )
            )

    return rows


def _generate_invalid_rows(count):
    rows = []
    for index in range(count):
        line = random.choice(LINES)
        station = random.choice(STATIONS)
        defect = random.choice(["None", "Reject", "Repair", "OK"])
        row = {
            "Event_ID": "",
            "Line": _space_noise(line),
            "Station": _space_noise(station),
            "TS": _render_timestamp(
                datetime(
                    2025,
                    random.randint(1, 12),
                    random.randint(1, 25),
                    random.randint(0, 23),
                    random.randint(0, 59),
                    random.randint(0, 59),
                )
            ),
            "Part_No": _mixed_case_part_number(random.choice(PART_NUMBERS)),
            "Torque": _raw_torque(),
            "Temp": _raw_temperature(),
            "Defect": defect,
            "VIN": _messy_vin(_base_vin()),
            "Supplier": _mixed_case_supplier(random.choice(SUPPLIERS)),
            "Work_Order": _messy_work_order(f"WO-ERR-{index:04d}"),
            "Tool_ID": _messy_tool_id(TOOL_BY_STATION[station]),
            "Inspection_Source": _messy_inspection_source(random.choice(INSPECTION_SOURCES)),
            "Scrap_Reason": _scrap_reason(defect),
            "Energy_kWh": _raw_energy(station),
            "Pressure": _raw_pressure(),
            "Warranty_Claim": _warranty_claim(defect),
            "Plan_Production": str(PLAN_BY_LINE[line]),
        }

        invalid_type = random.choice(
            ["bad_vin", "bad_timestamp", "missing_field", "bad_tool", "bad_part", "work_order_mismatch"]
        )
        if invalid_type == "bad_vin":
            row["VIN"] = _invalid_vin()
        elif invalid_type == "bad_timestamp":
            row["TS"] = random.choice(INVALID_TIMESTAMP_VALUES)
        elif invalid_type == "missing_field":
            row[random.choice(["VIN", "Supplier", "Part_No", "Work_Order"])] = ""
        elif invalid_type == "bad_tool":
            row["Tool_ID"] = random.choice(["TL-1", "T10A", "BAD-TOOL"])
        elif invalid_type == "bad_part":
            row["Part_No"] = random.choice(["999-ZZZ", "B0GUS-01"])
        else:
            row["Work_Order"] = "WO-2025123"
            row["VIN"] = _base_vin()

        rows.append(row)

    return rows


def generate_dataset(output_path=None, num_records=2000):
    """Generate the raw manufacturing dataset CSV."""
    output_path = output_path or os.path.join("data", "raw_dataset.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    rows = _generate_vehicle_events()
    rows.extend(_generate_invalid_rows(count=30))

    if len(rows) < num_records:
        duplicates_needed = num_records - len(rows)
        duplicate_source = list(rows)
        for _ in range(duplicates_needed):
            rows.append(dict(random.choice(duplicate_source)))
    elif len(rows) > num_records:
        rows = rows[:num_records]

    random.shuffle(rows)

    for index, row in enumerate(rows, start=1):
        row["Event_ID"] = f"EVT-{index:05d}"

    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[Generator] Created {len(rows)} records -> {output_path}")
    return output_path


if __name__ == "__main__":
    generate_dataset()
