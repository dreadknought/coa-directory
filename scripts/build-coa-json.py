#!/usr/bin/env python3
# File: scripts/build-coa-json.py

from __future__ import annotations

import csv
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
INPUT_CSV_PATH = REPO_ROOT / "data" / "coa.csv"
OUTPUT_JSON_PATH = REPO_ROOT / "public" / "coa-data.json"


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        return list(reader)


def build_nested_tree(rows: list[dict[str, str]]) -> list[dict]:
    categories: dict[str, dict] = {}

    for row in rows:
        category = (row.get("category") or "").strip() or "Uncategorized"
        product = (row.get("product") or "").strip() or "Unnamed Product"
        lot_number = (row.get("lot_number") or "").strip() or "UNSPECIFIED"
        file_name = (row.get("file_name") or "").strip() or "COA.pdf"
        url = (row.get("url") or "").strip() or "#"
        sample_number = (row.get("sample_number") or "").strip()
        lab_name = (row.get("lab_name") or "").strip()
        report_date = (row.get("report_date") or "").strip()
        notes = (row.get("notes") or "").strip()

        category_bucket = categories.setdefault(
            category,
            {
                "category": category,
                "products": {},
            },
        )

        product_bucket = category_bucket["products"].setdefault(
            product,
            {
                "product": product,
                "lots": {},
            },
        )

        lot_bucket = product_bucket["lots"].setdefault(
            lot_number,
            {
                "lotNumber": lot_number,
                "sampleNumber": sample_number,
                "labName": lab_name,
                "reportDate": report_date,
                "notes": notes,
                "files": [],
            },
        )

        if sample_number and not lot_bucket.get("sampleNumber"):
            lot_bucket["sampleNumber"] = sample_number

        if lab_name and not lot_bucket.get("labName"):
            lot_bucket["labName"] = lab_name

        if report_date and not lot_bucket.get("reportDate"):
            lot_bucket["reportDate"] = report_date

        if notes and not lot_bucket.get("notes"):
            lot_bucket["notes"] = notes

        lot_bucket["files"].append(
            {
                "name": file_name,
                "url": url,
            }
        )

    nested_tree: list[dict] = []

    for category_name in sorted(categories.keys(), key=str.casefold):
        category_bucket = categories[category_name]
        products_output: list[dict] = []

        for product_name in sorted(category_bucket["products"].keys(), key=str.casefold):
            product_bucket = category_bucket["products"][product_name]
            lots_output: list[dict] = []

            for lot_key in sorted(product_bucket["lots"].keys(), key=str.casefold):
                lots_output.append(product_bucket["lots"][lot_key])

            products_output.append(
                {
                    "product": product_name,
                    "lots": lots_output,
                }
            )

        nested_tree.append(
            {
                "category": category_name,
                "products": products_output,
            }
        )

    return nested_tree


def write_json(output_path: Path, payload: list[dict]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2, ensure_ascii=False)
        output_file.write("\n")


def main() -> None:
    rows = load_rows(INPUT_CSV_PATH)
    payload = build_nested_tree(rows)
    write_json(OUTPUT_JSON_PATH, payload)
    print(f"Wrote {OUTPUT_JSON_PATH} with {len(payload)} categories.")


if __name__ == "__main__":
    main()
