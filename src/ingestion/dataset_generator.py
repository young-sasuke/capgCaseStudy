"""
Manufacturing dataset generator.

Creates an intentionally messy raw manufacturing-event dataset that exercises
the downstream cleaning and validation rules.
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
DEFECT_VALUES = ["None", "OK", "Reject", "Repair", "na"]
SUPPLIERS = ["SUP-01", "SUP-02", "SUP-05", "sup-09", "Sup-11"]
TIMESTAMP_FORMATS = [
    "{y}-{m:02d}-{d:02d} {h:02d}:{mi:02d}",
    "{d:02d}/{m:02d}/{y} {h:02d}:{mi:02d}",
    "{m:02d}-{d:02d}-{y} {h:02d}:{mi:02d}",
    "{y}/{m:02d}/{d:02d} {h:02d}:{mi:02d}",
    "{d:02d}-{m:02d}-{y} {h:02d}:{mi:02d}",
]
INVALID_TIMESTAMP_VALUES = [
    "BAD_TS",
    "2025/13/40 25:99",
    "2025-02-30 09:15",
    "NOT_A_TIMESTAMP",
]
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
    styles = [
        part_number.lower(),
        part_number[:4] + part_number[4:].lower(),
        part_number.replace("-", ""),
        part_number.replace("-", " "),
    ]
    return random.choice(styles)


def _mixed_case_supplier(supplier):
    styles = [supplier, supplier.lower(), supplier.upper(), supplier.title()]
    return random.choice(styles)


def _messy_vin(vin):
    vin_chars = list(vin)
    for _ in range(random.randint(1, 3)):
        vin_chars.insert(random.randint(1, len(vin_chars) - 1), " ")
    messy = "".join(vin_chars)
    if random.random() < 0.35:
        messy = "".join(char.lower() if random.random() < 0.25 else char for char in messy)
    return messy


def _render_timestamp(dt_value, force_invalid_minutes=False):
    minute = dt_value.minute
    if force_invalid_minutes:
        minute = min(99, minute + random.randint(60, 70))
    template = random.choice(TIMESTAMP_FORMATS)
    return template.format(
        y=dt_value.year,
        m=dt_value.month,
        d=dt_value.day,
        h=dt_value.hour,
        mi=minute,
    )


def _raw_torque():
    if random.random() < 0.06:
        value = random.choice([random.uniform(10, 19.5), random.uniform(121, 150)])
    else:
        value = random.uniform(35, 95)
    value = round(value, 1)
    if random.random() < 0.2:
        return f"{value} Nm"
    return str(value)


def _raw_temperature():
    if random.random() < 0.04:
        celsius = round(random.uniform(205, 240), 1)
    else:
        celsius = round(random.uniform(28, 96), 1)

    style = random.choice(["c_space", "c_compact", "fahrenheit"])
    if style == "c_compact":
        return f"{celsius}C"
    if style == "fahrenheit":
        fahrenheit = round(celsius * 9 / 5 + 32, 1)
        return f"{fahrenheit}F"
    return f"{celsius} C"


def _raw_defect(rework_route=False):
    if rework_route:
        return random.choices(
            population=["None", "OK", "na", "Repair", " Reject"],
            weights=[42, 24, 16, 12, 6],
            k=1,
        )[0]
    return random.choices(
        population=["None", "OK", "na", "Repair", "Reject"],
        weights=[56, 24, 15, 3, 2],
        k=1,
    )[0]


def _make_event(line, station, ts_value, part_no, supplier, vin, messy=True, rework_route=False):
    return {
        "Event_ID": "",
        "Line": _space_noise(line) if messy else line,
        "Station": _space_noise(station) if messy else station,
        "TS": _render_timestamp(ts_value, force_invalid_minutes=messy and random.random() < 0.12),
        "Part_No": _mixed_case_part_number(part_no) if messy and random.random() < 0.6 else part_no,
        "Torque": _raw_torque(),
        "Temp": _raw_temperature(),
        "Defect": _raw_defect(rework_route=rework_route),
        "VIN": _messy_vin(vin) if messy and random.random() < 0.7 else vin,
        "Supplier": _mixed_case_supplier(supplier),
    }


def _generate_vehicle_events(vehicle_count=300, rework_vehicle_count=80):
    rows = []
    rework_vehicles = set(random.sample(range(vehicle_count), rework_vehicle_count))

    for vehicle_index in range(vehicle_count):
        line = random.choice(LINES)
        supplier = random.choice(SUPPLIERS)
        vin = _base_vin()
        part_no = random.choice(PART_NUMBERS)
        current_ts = datetime(
            year=2025,
            month=random.randint(1, 12),
            day=random.randint(1, 25),
            hour=random.randint(5, 20),
            minute=random.randint(0, 59),
        )

        if vehicle_index in rework_vehicles:
            route = ["STN-01", "STN-02", "STN-03", "STN-05", "STN-07", "STN-05", "STN-09"]
        else:
            route = list(STATIONS)

        for step_index, station in enumerate(route):
            if step_index > 0:
                gap_minutes = random.randint(4, 28)
                if random.random() < 0.05:
                    gap_minutes += random.randint(260, 420)
                current_ts += timedelta(minutes=gap_minutes)

            rows.append(
                _make_event(
                    line=line,
                    station=station,
                    ts_value=current_ts,
                    part_no=part_no,
                    supplier=supplier,
                    vin=vin,
                    messy=random.random() < 0.8,
                    rework_route=vehicle_index in rework_vehicles,
                )
            )

    return rows


def _generate_invalid_rows(count):
    rows = []
    for _ in range(count):
        row = {
            "Event_ID": "",
            "Line": _space_noise(random.choice(LINES)),
            "Station": _space_noise(random.choice(STATIONS)),
            "TS": _render_timestamp(
                datetime(2025, random.randint(1, 12), random.randint(1, 25), random.randint(0, 23), random.randint(0, 59))
            ),
            "Part_No": _mixed_case_part_number(random.choice(PART_NUMBERS)),
            "Torque": _raw_torque(),
            "Temp": _raw_temperature(),
            "Defect": random.choice(DEFECT_VALUES),
            "VIN": _messy_vin(_base_vin()),
            "Supplier": _mixed_case_supplier(random.choice(SUPPLIERS)),
        }

        invalid_type = random.choice(["bad_vin", "bad_timestamp", "missing_field"])
        if invalid_type == "bad_vin":
            row["VIN"] = _invalid_vin()
        elif invalid_type == "bad_timestamp":
            row["TS"] = random.choice(INVALID_TIMESTAMP_VALUES)
        else:
            missing_field = random.choice(["VIN", "Supplier", "Part_No"])
            row[missing_field] = ""

        rows.append(row)

    return rows


def generate_dataset(output_path=None, num_records=2000):
    """Generate the raw manufacturing dataset CSV."""
    output_path = output_path or os.path.join("data", "raw_dataset.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    rows = _generate_vehicle_events()
    rows.extend(_generate_invalid_rows(count=20))

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
