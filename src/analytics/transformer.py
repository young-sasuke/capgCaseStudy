"""Analytics and KPI calculations for manufacturing events."""

from collections import Counter, defaultdict
from statistics import median

IDEAL_CYCLE_TIME_MINUTES = 15.0


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _group_units(rows):
    units = defaultdict(list)
    for row in rows:
        units[row.get("VIN", "")].append(row)
    return units


def compute_metrics(rows, rework_summary=None):
    """Compute the requested manufacturing metrics and flatten them for CSV export."""
    rework_summary = rework_summary or {}
    if not rows:
        print("[Analytics] No records available for metrics")
        return []

    metrics = []
    total_events = len(rows)
    units = _group_units(rows)
    total_units = len(units)

    defective_event_count = sum(1 for row in rows if row.get("Defect") in {"Reject", "Repair"})
    reworked_units = 0
    good_units = 0
    supplier_units = defaultdict(list)

    for vin, unit_rows in units.items():
        has_defect = any(row.get("Defect") in {"Reject", "Repair"} for row in unit_rows)
        has_rework = any(bool(row.get("Rework")) for row in unit_rows)
        supplier = unit_rows[0].get("Supplier", "UNKNOWN")
        supplier_units[supplier].append({"VIN": vin, "Defective": has_defect})

        if has_rework:
            reworked_units += 1
        if not has_defect and not has_rework:
            good_units += 1

    fpy = round((good_units / total_units) * 100, 2) if total_units else 0.0
    dpu = round(defective_event_count / total_units, 4) if total_units else 0.0
    rework_rate = round((reworked_units / total_units) * 100, 2) if total_units else 0.0
    metrics.extend(
        [
            {"Metric": "FPY_%", "Value": fpy},
            {"Metric": "DPU", "Value": dpu},
            {"Metric": "Rework_Rate_%", "Value": rework_rate},
        ]
    )

    station_torque = defaultdict(list)
    station_temperature = defaultdict(list)
    for row in rows:
        torque = _safe_float(row.get("Torque_Nm"))
        temp = _safe_float(row.get("Temp_C"))
        station = row.get("Station", "UNKNOWN")
        if torque is not None:
            station_torque[station].append(torque)
        if temp is not None:
            station_temperature[station].append(temp)

    for station in sorted(station_torque):
        values = station_torque[station]
        metrics.append({"Metric": f"Average_Torque_{station}", "Value": round(sum(values) / len(values), 2)})

    for station in sorted(station_temperature):
        values = station_temperature[station]
        metrics.append({"Metric": f"Average_Temperature_{station}", "Value": round(sum(values) / len(values), 2)})

    line_counts = Counter(row.get("Line", "UNKNOWN") for row in rows)
    for line, count in sorted(line_counts.items()):
        metrics.append({"Metric": f"Production_Count_{line}", "Value": count})

    supplier_event_counts = Counter(row.get("Supplier", "UNKNOWN") for row in rows)
    for supplier, count in sorted(supplier_event_counts.items()):
        metrics.append({"Metric": f"Supplier_Event_Count_{supplier}", "Value": count})

    for supplier, supplier_rows in sorted(supplier_units.items()):
        total_supplier_parts = len(supplier_rows)
        defective_parts = sum(1 for item in supplier_rows if item["Defective"])
        ppm = round((defective_parts / total_supplier_parts) * 1_000_000, 2) if total_supplier_parts else 0.0
        metrics.append({"Metric": f"Supplier_PPM_{supplier}", "Value": ppm})

    cycle_times = [
        _safe_float(row.get("Cycle_Time_Minutes"))
        for row in rows
        if row.get("Cycle_Time_Valid") and row.get("Cycle_Time_Minutes") not in ("", None)
    ]
    cycle_times = [value for value in cycle_times if value is not None]
    invalid_cycle_events = sum(1 for row in rows if not row.get("Cycle_Time_Valid", True))

    if cycle_times:
        metrics.extend(
            [
                {"Metric": "Cycle_Time_Min_Minutes", "Value": round(min(cycle_times), 2)},
                {"Metric": "Cycle_Time_Avg_Minutes", "Value": round(sum(cycle_times) / len(cycle_times), 2)},
                {"Metric": "Cycle_Time_Median_Minutes", "Value": round(median(cycle_times), 2)},
                {"Metric": "Cycle_Time_Max_Minutes", "Value": round(max(cycle_times), 2)},
            ]
        )
    metrics.append({"Metric": "Cycle_Time_Invalid_Transitions", "Value": invalid_cycle_events})

    availability = round(
        (len(cycle_times) / (len(cycle_times) + invalid_cycle_events)) if (len(cycle_times) + invalid_cycle_events) else 1.0,
        4,
    )
    average_cycle_time = (sum(cycle_times) / len(cycle_times)) if cycle_times else IDEAL_CYCLE_TIME_MINUTES
    performance = round(min(1.0, IDEAL_CYCLE_TIME_MINUTES / average_cycle_time), 4) if average_cycle_time else 1.0
    quality = round((good_units / total_units), 4) if total_units else 1.0
    oee = round(availability * performance * quality * 100, 2)

    metrics.extend(
        [
            {"Metric": "OEE_Availability", "Value": availability},
            {"Metric": "OEE_Performance", "Value": performance},
            {"Metric": "OEE_Quality", "Value": quality},
            {"Metric": "OEE_%", "Value": oee},
            {"Metric": "Total_Events", "Value": total_events},
            {"Metric": "Total_Units", "Value": total_units},
            {"Metric": "Rework_Events", "Value": rework_summary.get("rework_events", 0)},
            {"Metric": "Rework_Units", "Value": rework_summary.get("rework_units", reworked_units)},
        ]
    )

    print(f"[Analytics] Computed {len(metrics)} metrics")
    return metrics
