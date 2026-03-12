"""Traceability reporting for supplier-to-warranty lineage."""

import csv
import os

TRACEABILITY_COLUMNS = [
    "Supplier_Code",
    "Supplier_Name",
    "Country",
    "Part_No",
    "Part_Description",
    "VIN",
    "Work_Order",
    "Warranty_Claim",
    "Defect",
    "Scrap_Reason",
]


def build_traceability_chain(rows, output_path=None):
    """
    Build Supplier -> Part -> VIN -> Warranty Claim records and optionally save them.
    """
    traceability_rows = []
    seen = set()

    for row in rows:
        record = {
            "Supplier_Code": row.get("Supplier", ""),
            "Supplier_Name": row.get("Supplier_Name", ""),
            "Country": row.get("Supplier_Country", ""),
            "Part_No": row.get("Part_No", ""),
            "Part_Description": row.get("Part_Description", ""),
            "VIN": row.get("VIN", ""),
            "Work_Order": row.get("Work_Order", ""),
            "Warranty_Claim": row.get("Warranty_Claim", "No"),
            "Defect": row.get("Defect", ""),
            "Scrap_Reason": row.get("Scrap_Reason_Description", ""),
        }
        key = tuple(record.values())
        if key in seen:
            continue
        seen.add(key)
        traceability_rows.append(record)

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=TRACEABILITY_COLUMNS)
            writer.writeheader()
            if traceability_rows:
                writer.writerows(traceability_rows)

    print(f"[Traceability] Built {len(traceability_rows)} traceability records")
    return traceability_rows
