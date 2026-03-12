"""Analytics and KPI calculations for manufacturing events."""

from collections import defaultdict
from statistics import mean, pstdev

IDEAL_CYCLE_TIME_SECONDS = 300.0
REWORK_COST = 50
SCRAP_COST = 200


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


def _valid_cycle_seconds(rows):
    values = []
    for row in rows:
        cycle_seconds = _safe_float(row.get("Cycle_Time_Seconds"))
        if cycle_seconds is not None and row.get("Cycle_Time_Valid", True):
            values.append(cycle_seconds)
    return values


def _per_station_cycle_distribution(rows):
    station_cycles = defaultdict(list)
    for row in rows:
        cycle_seconds = _safe_float(row.get("Cycle_Time_Seconds"))
        if cycle_seconds is not None and row.get("Cycle_Time_Valid", True):
            station_cycles[row.get("Station", "UNKNOWN")].append(cycle_seconds)

    distribution = []
    for station, values in sorted(station_cycles.items()):
        distribution.append(
            {
                "station": station,
                "min_seconds": round(min(values), 2),
                "max_seconds": round(max(values), 2),
                "average_seconds": round(mean(values), 2),
                "std_deviation_seconds": round(pstdev(values), 2) if len(values) > 1 else 0.0,
            }
        )
    return distribution


def _per_line_oee(rows):
    units = _group_units(rows)
    line_rows = defaultdict(list)
    line_units = defaultdict(set)
    line_plan = {}

    for row in rows:
        line = row.get("Line", "UNKNOWN")
        line_rows[line].append(row)
        line_units[line].add(row.get("VIN", ""))
        plan_production = _safe_float(row.get("Plan_Production"))
        if plan_production is not None:
            line_plan[line] = max(line_plan.get(line, 0), int(plan_production))

    per_line = []
    for line, current_rows in sorted(line_rows.items()):
        total_units = len(line_units[line])
        good_units = 0
        for vin in line_units[line]:
            unit_rows = [row for row in units[vin] if row.get("Line") == line]
            if not any(row.get("Defect") in {"Reject", "Repair"} or row.get("Rework") for row in unit_rows):
                good_units += 1

        cycle_values = _valid_cycle_seconds(current_rows)
        actual_cycle_time = mean(cycle_values) if cycle_values else IDEAL_CYCLE_TIME_SECONDS
        planned_units = max(line_plan.get(line, total_units), total_units or 1)
        planned_time = planned_units * IDEAL_CYCLE_TIME_SECONDS
        runtime = min(sum(cycle_values), planned_time) if cycle_values else 0.0
        availability = round(runtime / planned_time, 4) if planned_time else 0.0
        performance = round(min(1.0, IDEAL_CYCLE_TIME_SECONDS / actual_cycle_time), 4) if actual_cycle_time else 0.0
        quality = round(good_units / total_units, 4) if total_units else 0.0
        oee_value = round(availability * performance * quality, 4)
        per_line.append(
            {
                "line": line,
                "availability": availability,
                "performance": performance,
                "quality": quality,
                "oee": oee_value,
                "planned_time_seconds": round(planned_time, 2),
                "runtime_seconds": round(runtime, 2),
                "actual_cycle_time_seconds": round(actual_cycle_time, 2),
                "ideal_cycle_time_seconds": IDEAL_CYCLE_TIME_SECONDS,
                "good_units": good_units,
                "total_units": total_units,
            }
        )
    return per_line


def _bottleneck_station(cycle_distribution):
    if not cycle_distribution:
        return ""
    return max(cycle_distribution, key=lambda item: item["average_seconds"])["station"]


def _calculate_mtbf(rows):
    failures = [row for row in rows if row.get("Defect") in {"Reject", "Repair"}]
    runtime_seconds = sum(_valid_cycle_seconds(rows))
    if not failures:
        return round(runtime_seconds, 2)
    return round(runtime_seconds / len(failures), 2)


def _calculate_mttr(rows):
    units = _group_units(rows)
    repair_durations = []
    for vin_rows in units.values():
        ordered_rows = sorted(vin_rows, key=lambda item: item.get("TS", ""))
        last_seen_station = {}
        for row in ordered_rows:
            station = row.get("Station", "")
            ts = row.get("TS", "")
            cycle_seconds = _safe_float(row.get("Cycle_Time_Seconds"))
            if station in last_seen_station and cycle_seconds is not None and row.get("Cycle_Time_Valid", True):
                repair_durations.append(cycle_seconds)
            last_seen_station[station] = ts
    if not repair_durations:
        return 0.0
    return round(mean(repair_durations), 2)


def _spc_summary(rows):
    grouped = defaultdict(lambda: {"torque": [], "temp": []})
    for row in rows:
        station = row.get("Station", "UNKNOWN")
        torque = _safe_float(row.get("Torque_Nm"))
        temp = _safe_float(row.get("Temp_C"))
        if torque is not None:
            grouped[station]["torque"].append(torque)
        if temp is not None:
            grouped[station]["temp"].append(temp)

    summary = []
    for station, values in sorted(grouped.items()):
        torque_values = values["torque"]
        temp_values = values["temp"]
        summary.append(
            {
                "station": station,
                "torque_mean": round(mean(torque_values), 2) if torque_values else 0.0,
                "torque_range": round(max(torque_values) - min(torque_values), 2) if torque_values else 0.0,
                "temp_mean": round(mean(temp_values), 2) if temp_values else 0.0,
                "temp_range": round(max(temp_values) - min(temp_values), 2) if temp_values else 0.0,
            }
        )
    return summary


def _copq(rows, rework_summary):
    rework_count = rework_summary.get("rework_events", sum(1 for row in rows if row.get("Rework")))
    scrap_count = sum(1 for row in rows if row.get("Defect") == "Reject")
    return {
        "rework_count": rework_count,
        "scrap_count": scrap_count,
        "copq": (rework_count * REWORK_COST) + (scrap_count * SCRAP_COST),
    }


def _energy_per_vehicle(rows):
    total_energy = sum(_safe_float(row.get("Energy_kWh")) or 0.0 for row in rows)
    vehicles_produced = len(_group_units(rows))
    energy_per_vehicle = round(total_energy / vehicles_produced, 2) if vehicles_produced else 0.0
    return {
        "total_energy_kwh": round(total_energy, 2),
        "vehicles_produced": vehicles_produced,
        "energy_per_vehicle_kwh": energy_per_vehicle,
    }


def _shift_performance(rows):
    shift_data = defaultdict(lambda: {"vins": set(), "defects": 0, "cycle_times": []})
    for row in rows:
        shift = row.get("Shift", "Unknown")
        shift_data[shift]["vins"].add(row.get("VIN", ""))
        if row.get("Defect") in {"Reject", "Repair"}:
            shift_data[shift]["defects"] += 1
        cycle_seconds = _safe_float(row.get("Cycle_Time_Seconds"))
        if cycle_seconds is not None and row.get("Cycle_Time_Valid", True):
            shift_data[shift]["cycle_times"].append(cycle_seconds)

    performance = []
    for shift, data in sorted(shift_data.items()):
        production_count = len(data["vins"])
        performance.append(
            {
                "shift": shift,
                "production_count": production_count,
                "defect_rate": round(data["defects"] / production_count, 4) if production_count else 0.0,
                "average_cycle_time_seconds": round(mean(data["cycle_times"]), 2) if data["cycle_times"] else 0.0,
            }
        )
    return performance


def _line_balance(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.get("Line", "UNKNOWN")].append(row)

    balance = []
    for line, line_rows in sorted(grouped.items()):
        cycle_values = _valid_cycle_seconds(line_rows)
        actual_cycle = mean(cycle_values) if cycle_values else 0.0
        efficiency = round(min(1.0, IDEAL_CYCLE_TIME_SECONDS / actual_cycle) * 100, 2) if actual_cycle else 0.0
        balance.append(
            {
                "line": line,
                "actual_cycle_time_seconds": round(actual_cycle, 2),
                "takt_time_seconds": IDEAL_CYCLE_TIME_SECONDS,
                "line_balance_efficiency": efficiency,
            }
        )
    return balance


def compute_anomaly_report(rows):
    """Build a row-level anomaly report for sensor and cycle-time exceptions."""
    anomalies = []
    for row in rows:
        torque = _safe_float(row.get("Torque_Nm"))
        temp = _safe_float(row.get("Temp_C"))
        pressure = _safe_float(row.get("Pressure"))
        cycle_seconds = _safe_float(row.get("Cycle_Time_Seconds"))
        context = {
            "Event_ID": row.get("Event_ID", ""),
            "VIN": row.get("VIN", ""),
            "Line": row.get("Line", ""),
            "Station": row.get("Station", ""),
            "TS": row.get("TS", ""),
        }

        if torque is not None and not 30 <= torque <= 70:
            anomalies.append(
                {
                    **context,
                    "Anomaly_Type": "torque_out_of_range",
                    "Observed_Value": round(torque, 2),
                    "Expected_Range": "30-70 Nm",
                }
            )
        if temp is not None and not 70 <= temp <= 110:
            anomalies.append(
                {
                    **context,
                    "Anomaly_Type": "temperature_out_of_range",
                    "Observed_Value": round(temp, 2),
                    "Expected_Range": "70-110 C",
                }
            )
        if pressure is not None and not 25 <= pressure <= 60:
            anomalies.append(
                {
                    **context,
                    "Anomaly_Type": "pressure_out_of_range",
                    "Observed_Value": round(pressure, 2),
                    "Expected_Range": "25-60 psi",
                }
            )
        if cycle_seconds is not None and not row.get("Cycle_Time_Valid", True):
            anomalies.append(
                {
                    **context,
                    "Anomaly_Type": "cycle_time_anomaly",
                    "Observed_Value": round(cycle_seconds, 2),
                    "Expected_Range": "5-600 seconds",
                }
            )
    return anomalies


def summarize_metrics(rows, rework_summary=None):
    """Return nested analytics used for JSON export and KPI flattening."""
    rework_summary = rework_summary or {}
    if not rows:
        return {
            "overall": {},
            "line_oee": [],
            "cycle_time_distribution": [],
            "bottleneck_station": "",
            "mtbf_seconds": 0.0,
            "mttr_seconds": 0.0,
            "spc_summary": [],
            "copq": {"rework_count": 0, "scrap_count": 0, "copq": 0},
            "energy_per_vehicle": {"total_energy_kwh": 0.0, "vehicles_produced": 0, "energy_per_vehicle_kwh": 0.0},
            "shift_performance": [],
            "line_balance": [],
            "anomaly_count": 0,
        }

    units = _group_units(rows)
    total_units = len(units)
    defective_event_count = sum(1 for row in rows if row.get("Defect") in {"Reject", "Repair"})
    reworked_units = sum(1 for vin_rows in units.values() if any(row.get("Rework") for row in vin_rows))
    good_units = sum(
        1
        for vin_rows in units.values()
        if not any(row.get("Defect") in {"Reject", "Repair"} or row.get("Rework") for row in vin_rows)
    )
    cycle_distribution = _per_station_cycle_distribution(rows)
    copq = _copq(rows, rework_summary)
    summary = {
        "overall": {
            "total_events": len(rows),
            "total_units": total_units,
            "defective_event_count": defective_event_count,
            "good_units": good_units,
            "reworked_units": reworked_units,
            "fpy_percent": round((good_units / total_units) * 100, 2) if total_units else 0.0,
            "dpu": round(defective_event_count / total_units, 4) if total_units else 0.0,
            "rework_rate_percent": round((reworked_units / total_units) * 100, 2) if total_units else 0.0,
        },
        "line_oee": _per_line_oee(rows),
        "cycle_time_distribution": cycle_distribution,
        "bottleneck_station": _bottleneck_station(cycle_distribution),
        "mtbf_seconds": _calculate_mtbf(rows),
        "mttr_seconds": _calculate_mttr(rows),
        "spc_summary": _spc_summary(rows),
        "copq": copq,
        "energy_per_vehicle": _energy_per_vehicle(rows),
        "shift_performance": _shift_performance(rows),
        "line_balance": _line_balance(rows),
        "anomaly_count": len(compute_anomaly_report(rows)),
    }
    return summary


def compute_metrics(rows, rework_summary=None):
    """Compute the requested manufacturing metrics and flatten them for CSV export."""
    if not rows:
        print("[Analytics] No records available for metrics")
        return []

    summary = summarize_metrics(rows, rework_summary=rework_summary)
    metrics = [
        {"Metric": "FPY_%", "Value": summary["overall"]["fpy_percent"]},
        {"Metric": "DPU", "Value": summary["overall"]["dpu"]},
        {"Metric": "Rework_Rate_%", "Value": summary["overall"]["rework_rate_percent"]},
        {"Metric": "Bottleneck_Station", "Value": summary["bottleneck_station"]},
        {"Metric": "MTBF_Seconds", "Value": summary["mtbf_seconds"]},
        {"Metric": "MTTR_Seconds", "Value": summary["mttr_seconds"]},
        {"Metric": "COPQ", "Value": summary["copq"]["copq"]},
        {"Metric": "Energy_Per_Vehicle_kWh", "Value": summary["energy_per_vehicle"]["energy_per_vehicle_kwh"]},
        {"Metric": "Anomaly_Count", "Value": summary["anomaly_count"]},
        {"Metric": "Total_Events", "Value": summary["overall"]["total_events"]},
        {"Metric": "Total_Units", "Value": summary["overall"]["total_units"]},
    ]

    for item in summary["line_oee"]:
        metrics.extend(
            [
                {"Metric": f"OEE_{item['line']}", "Value": round(item["oee"] * 100, 2)},
                {"Metric": f"Availability_{item['line']}", "Value": item["availability"]},
                {"Metric": f"Performance_{item['line']}", "Value": item["performance"]},
                {"Metric": f"Quality_{item['line']}", "Value": item["quality"]},
            ]
        )

    for item in summary["cycle_time_distribution"]:
        station = item["station"]
        metrics.extend(
            [
                {"Metric": f"Cycle_Time_Min_{station}", "Value": item["min_seconds"]},
                {"Metric": f"Cycle_Time_Max_{station}", "Value": item["max_seconds"]},
                {"Metric": f"Cycle_Time_Avg_{station}", "Value": item["average_seconds"]},
                {"Metric": f"Cycle_Time_Std_{station}", "Value": item["std_deviation_seconds"]},
            ]
        )

    for item in summary["shift_performance"]:
        shift = item["shift"]
        metrics.extend(
            [
                {"Metric": f"Shift_Production_{shift}", "Value": item["production_count"]},
                {"Metric": f"Shift_Defect_Rate_{shift}", "Value": item["defect_rate"]},
                {"Metric": f"Shift_Cycle_Time_{shift}", "Value": item["average_cycle_time_seconds"]},
            ]
        )

    for item in summary["line_balance"]:
        metrics.append({"Metric": f"Line_Balance_Efficiency_{item['line']}", "Value": item["line_balance_efficiency"]})

    for item in summary["spc_summary"]:
        station = item["station"]
        metrics.extend(
            [
                {"Metric": f"SPC_Torque_Mean_{station}", "Value": item["torque_mean"]},
                {"Metric": f"SPC_Torque_Range_{station}", "Value": item["torque_range"]},
                {"Metric": f"SPC_Temp_Mean_{station}", "Value": item["temp_mean"]},
                {"Metric": f"SPC_Temp_Range_{station}", "Value": item["temp_range"]},
            ]
        )

    print(f"[Analytics] Computed {len(metrics)} metrics")
    return metrics
