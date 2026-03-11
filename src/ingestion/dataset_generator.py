"""
Developer 1 - Dataset Generator
Generates a realistic but messy manufacturing dataset with 300 records.
Simulates data from MES, IoT sensors, quality inspection, supplier, and warranty systems.
"""

import csv
import os
import random
import string

# Seed for reproducibility
random.seed(42)

# ---------- reference pools ----------

LINES = ["Line-A", "Line-B", "Line-C", "Line-D"]
STATIONS = ["Welding", "Painting", "Assembly", "Inspection", "Packaging"]
PART_NUMBERS = ["PT-1001", "PT-1002", "PT-1003", "PT-2001", "PT-2002", "PT-3001"]
SUPPLIERS = ["SUP-ALPHA", "SUP-BETA", "SUP-GAMMA", "SUP-DELTA"]
DEFECT_CLEAN = [None, "Reject", "Repair"]
DEFECT_MESSY = ["na", "OK", "None", " Reject", "repair", "REJECT", "ok", "NA", ""]

TIMESTAMP_FORMATS = [
    "{y}-{m:02d}-{d:02d} {h:02d}:{mi:02d}",          # 2024-01-15 08:30
    "{d:02d}/{m:02d}/{y} {h:02d}:{mi:02d}",            # 15/01/2024 08:30
    "{m:02d}-{d:02d}-{y} {h:02d}:{mi:02d}",            # 01-15-2024 08:30
    "{y}/{m:02d}/{d:02d} {h:02d}:{mi:02d}",            # 2024/01/15 08:30
    "{d:02d}-{m:02d}-{y} {h:02d}:{mi:02d}",            # 15-01-2024 08:30
]

INVALID_TIMESTAMPS = [
    "not-a-date",
    "2024/13/40 25:99",
    "00-00-0000 00:00",
    "TIMESTAMP_ERROR",
    "2024-02-30 08:30",
]


def _random_vin(messy=False, invalid=False):
    """Generate a 17-character VIN, optionally with spaces / lowercase."""
    chars = string.ascii_uppercase.replace("I", "").replace("O", "").replace("Q", "") + string.digits
    vin = "".join(random.choices(chars, k=17))
    if invalid:
        # Produce VINs that cannot be fixed by cleaning
        r = random.random()
        if r < 0.33:
            # Wrong length (too short)
            vin = vin[:random.randint(10, 15)]
        elif r < 0.66:
            # Contains invalid chars I, O, Q
            pos = random.randint(0, 16)
            vin = vin[:pos] + random.choice("IOQ") + vin[pos + 1:]
        else:
            # Too long
            vin = vin + "".join(random.choices(chars, k=random.randint(1, 4)))
        return vin
    if messy:
        # insert random spaces or lowercase
        vin_list = list(vin)
        for _ in range(random.randint(1, 3)):
            idx = random.randint(0, 16)
            if random.random() < 0.5:
                vin_list.insert(idx, " ")
            else:
                vin_list[idx] = vin_list[idx].lower()
        vin = "".join(vin_list)
    return vin


def _random_timestamp(messy=False):
    """Return a timestamp string in a random format; optionally with invalid minutes."""
    y = random.choice([2024, 2025])
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    h = random.randint(6, 22)
    mi = random.randint(0, 59)

    if messy and random.random() < 0.3:
        mi = random.randint(60, 99)  # invalid minutes

    fmt = random.choice(TIMESTAMP_FORMATS)
    return fmt.format(y=y, m=m, d=d, h=h, mi=mi)


def _random_part_number(messy=False):
    """Return a part number, optionally with wrong casing / missing hyphen."""
    pn = random.choice(PART_NUMBERS)
    if messy:
        r = random.random()
        if r < 0.3:
            pn = pn.lower()
        elif r < 0.5:
            pn = pn.replace("-", "")
        elif r < 0.7:
            pn = pn.replace("-", " ")
    return pn


def _random_torque(messy=False):
    """Return torque value (Nm). Sometimes includes unit suffix."""
    val = round(random.uniform(18.0, 55.0), 1)
    if messy and random.random() < 0.25:
        return f"{val} Nm"
    return str(val)


def _random_temp(messy=False):
    """Return temperature. Mix Celsius and Fahrenheit."""
    celsius = round(random.uniform(18.0, 45.0), 1)
    if messy and random.random() < 0.35:
        fahrenheit = round(celsius * 9 / 5 + 32, 1)
        return f"{fahrenheit}F"
    return f"{celsius}C"


def _random_defect(messy=False):
    """Return a defect value, optionally messy."""
    if messy:
        return random.choice(DEFECT_MESSY)
    return random.choice(DEFECT_CLEAN) or ""


def _random_supplier(messy=False):
    """Return supplier code, optionally with wrong casing."""
    sup = random.choice(SUPPLIERS)
    if messy:
        r = random.random()
        if r < 0.3:
            sup = sup.lower()
        elif r < 0.5:
            sup = sup.title()
    return sup


def _add_space_noise(value):
    """Add leading / trailing / internal spaces randomly."""
    r = random.random()
    if r < 0.2:
        return f"  {value}"
    elif r < 0.4:
        return f"{value}  "
    elif r < 0.5:
        return f" {value} "
    return value


def generate_dataset(output_path, num_records=300):
    """Generate the raw manufacturing dataset CSV."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    rows = []
    vin_pool = [_random_vin(messy=False) for _ in range(80)]

    for i in range(1, num_records + 1):
        is_messy = random.random() < 0.40  # ~40 % of rows are messy

        line = random.choice(LINES)
        station = random.choice(STATIONS)
        if is_messy:
            line = _add_space_noise(line)
            station = _add_space_noise(station)

        ts = _random_timestamp(messy=is_messy)
        part_no = _random_part_number(messy=is_messy)
        torque = _random_torque(messy=is_messy)
        temp = _random_temp(messy=is_messy)
        defect = _random_defect(messy=is_messy)
        vin = random.choice(vin_pool)
        if is_messy:
            vin = _random_vin(messy=True)
        supplier = _random_supplier(messy=is_messy)

        rows.append({
            "Event_ID": f"EVT-{i:04d}",
            "Line": line,
            "Station": station,
            "TS": ts,
            "Part_No": part_no,
            "Torque": torque,
            "Temp": temp,
            "Defect": defect,
            "VIN": vin,
            "Supplier": supplier,
        })

    # ---------- inject ~20 truly invalid records ----------
    for j in range(20):
        idx = num_records + 1 + j
        inv_type = random.choice(["bad_vin", "bad_ts", "missing_fields"])
        row = {
            "Event_ID": f"EVT-{idx:04d}",
            "Line": random.choice(LINES),
            "Station": random.choice(STATIONS),
            "TS": _random_timestamp(messy=False),
            "Part_No": random.choice(PART_NUMBERS),
            "Torque": str(round(random.uniform(18.0, 55.0), 1)),
            "Temp": f"{round(random.uniform(18.0, 45.0), 1)}C",
            "Defect": "",
            "VIN": random.choice(vin_pool),
            "Supplier": random.choice(SUPPLIERS),
        }
        if inv_type == "bad_vin":
            row["VIN"] = _random_vin(invalid=True)
        elif inv_type == "bad_ts":
            row["TS"] = random.choice(INVALID_TIMESTAMPS)
        else:  # missing_fields
            row["VIN"] = ""
            row["Supplier"] = ""
        rows.append(row)

    # ---------- inject ~15 exact duplicate rows ----------
    for _ in range(15):
        src = random.choice(rows)
        dup = dict(src)
        dup["Event_ID"] = f"EVT-{len(rows) + 1:04d}"
        rows.append(dup)

    random.shuffle(rows)

    # Re-index Event_IDs after shuffle
    for idx, row in enumerate(rows, start=1):
        row["Event_ID"] = f"EVT-{idx:04d}"

    fieldnames = ["Event_ID", "Line", "Station", "TS", "Part_No",
                  "Torque", "Temp", "Defect", "VIN", "Supplier"]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[Generator] Created {len(rows)} records -> {output_path}")
    return output_path


if __name__ == "__main__":
    generate_dataset(os.path.join("data", "raw_events.csv"))
