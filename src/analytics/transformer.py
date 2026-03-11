"""
Developer 5 - Analytics & KPI Transformer
Computes manufacturing metrics and KPIs from the cleaned, validated dataset.
"""

from collections import Counter, defaultdict


def _safe_float(val):
    """Convert to float or return None."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def compute_metrics(rows, rework_summary=None):
    """
    Compute all manufacturing KPIs.
    Returns a list of dicts suitable for CSV export (metric_name, metric_value).
    """
    if rework_summary is None:
        rework_summary = {}

    total = len(rows)
    if total == 0:
        print("[Analytics] No records to analyse.")
        return []

    metrics = []

    # ------- 1. First Pass Yield (FPY) -------
    defect_count = sum(1 for r in rows if r.get("Defect", "") in ("Reject", "Repair"))
    passed_first = total - defect_count
    fpy = round(passed_first / total * 100, 2) if total else 0
    metrics.append({"Metric": "First_Pass_Yield_%", "Value": fpy})

    # ------- 2. Defects Per Unit (DPU) -------
    dpu = round(defect_count / total, 4) if total else 0
    metrics.append({"Metric": "Defects_Per_Unit", "Value": dpu})

    # ------- 3. Defect frequency breakdown -------
    defect_types = Counter(r.get("Defect", "") for r in rows if r.get("Defect", ""))
    for dtype, cnt in defect_types.most_common():
        metrics.append({"Metric": f"Defect_Freq_{dtype}", "Value": cnt})

    # ------- 4. Average Torque per Station -------
    station_torques = defaultdict(list)
    for r in rows:
        t = _safe_float(r.get("Torque_Nm"))
        if t is not None:
            station_torques[r.get("Station", "UNKNOWN")].append(t)
    for station, vals in sorted(station_torques.items()):
        avg = round(sum(vals) / len(vals), 2)
        metrics.append({"Metric": f"Avg_Torque_Nm_{station}", "Value": avg})

    # ------- 5. Average Temperature per Station -------
    station_temps = defaultdict(list)
    for r in rows:
        t = _safe_float(r.get("Temp_C"))
        if t is not None:
            station_temps[r.get("Station", "UNKNOWN")].append(t)
    for station, vals in sorted(station_temps.items()):
        avg = round(sum(vals) / len(vals), 2)
        metrics.append({"Metric": f"Avg_Temp_C_{station}", "Value": avg})

    # ------- 6. Production Count per Line -------
    line_counts = Counter(r.get("Line", "UNKNOWN") for r in rows)
    for line, cnt in sorted(line_counts.items()):
        metrics.append({"Metric": f"Production_Count_{line}", "Value": cnt})

    # ------- 7. Supplier Distribution -------
    supplier_counts = Counter(r.get("Supplier", "UNKNOWN") for r in rows)
    for sup, cnt in sorted(supplier_counts.items()):
        metrics.append({"Metric": f"Supplier_Count_{sup}", "Value": cnt})

    # ------- 8. Rework Rate -------
    rework_events = rework_summary.get("rework_events", 0)
    rework_rate = round(rework_events / total * 100, 2) if total else 0
    metrics.append({"Metric": "Rework_Rate_%", "Value": rework_rate})

    # ------- 9. Station Utilisation (events per station) -------
    station_counts = Counter(r.get("Station", "UNKNOWN") for r in rows)
    for station, cnt in sorted(station_counts.items()):
        pct = round(cnt / total * 100, 2)
        metrics.append({"Metric": f"Station_Utilisation_%_{station}", "Value": pct})

    # ------- 10. Cycle Time Distribution (by hour bucket) -------
    hour_buckets = Counter()
    for r in rows:
        ts = r.get("TS", "")
        if len(ts) >= 13:
            try:
                hour = int(ts[11:13])
                hour_buckets[f"{hour:02d}:00"] += 1
            except ValueError:
                pass
    for bucket, cnt in sorted(hour_buckets.items()):
        metrics.append({"Metric": f"Cycle_Dist_{bucket}", "Value": cnt})

    print(f"[Analytics] Computed {len(metrics)} metrics")
    return metrics
