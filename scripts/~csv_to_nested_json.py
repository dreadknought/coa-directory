#!/usr/bin/env python3
# scripts/csv_to_nested_json.py
"""
Convert a flat COA CSV into nested JSON.

Input:
    <project_root>/data/coa.csv

Output:
    <project_root>/data/coa_nested.json

Output shape:
{
  "Flower": {
    "Oreoz": {
      "lot_number": "...",
      "file_name": "...",
      "url": "...",
      "sample_number": "...",
      "lab_name": null,
      "report_date": "...",
      "notes": null
    },
    "Some Product With Multiple Rows": [
      { ... },
      { ... }
    ]
  }
}

Behavior:
- Top level groups by category
- Second level groups by product name
- For Flower, prefers "strain" as the nested product key
- Removes redundant grouping fields from child records
- Cleans accidental duplicate slashes in URL paths
- Collapses 1-item lists into a plain object
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


# Resolve paths relative to the project root, not the current shell directory.
SCRIPT_PATH = Path(__file__).resolve()
SCRIPTS_DIR = SCRIPT_PATH.parent
PROJECT_ROOT = SCRIPTS_DIR.parent

INPUT_PATH = PROJECT_ROOT / "data" / "coa.csv"
OUTPUT_PATH = PROJECT_ROOT / "public" / "coas.json"

CATEGORY_ALIASES = (
    "category",
    "product_category",
    "product_type",
    "type",
)

PRODUCT_ALIASES = (
    "product",
    "product_name",
    "name",
    "title",
)

STRAIN_ALIASES = (
    "strain",
    "strain_name",
)


def normalize_header(value: str) -> str:
    """
    Normalize a CSV header so alias matching is more reliable.
    """
    return value.strip().lower().replace("-", "_").replace(" ", "_")


def clean_value(value: Any) -> Any:
    """
    Trim strings and convert blank strings to None.
    """
    if value is None:
        return None

    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned if cleaned else None

    return value


def build_normalized_row(raw_row: dict[str, Any]) -> dict[str, Any]:
    """
    Return a row with normalized keys and cleaned values.
    """
    normalized: dict[str, Any] = {}

    for key, value in raw_row.items():
        normalized[normalize_header(key)] = clean_value(value)

    return normalized


def first_present_value(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    """
    Return the first non-empty value for any alias in the row.
    """
    for alias in aliases:
        if alias in row and row[alias] not in (None, ""):
            return row[alias]
    return None


def choose_category(row: dict[str, Any]) -> str:
    """
    Determine the category for a row.
    """
    category = first_present_value(row, CATEGORY_ALIASES)
    if category is None:
        return "Uncategorized"
    return str(category)


def choose_product_name(row: dict[str, Any], category: str) -> str:
    """
    Determine the product name for a row.

    For Flower, prefer strain if available.
    """
    if category.strip().lower() == "flower":
        strain = first_present_value(row, STRAIN_ALIASES)
        if strain is not None:
            return str(strain)

    product = first_present_value(row, PRODUCT_ALIASES)
    if product is not None:
        return str(product)

    strain = first_present_value(row, STRAIN_ALIASES)
    if strain is not None:
        return str(strain)

    return "Unknown Product"


def normalize_url_path(value: Any) -> Any:
    """
    Normalize a URL-like path by removing duplicate slashes in the path.

    Examples:
    - "/coas/flower//file.pdf" -> "/coas/flower/file.pdf"
    - "coas/flower/file.pdf"   -> "/coas/flower/file.pdf"

    Preserves http:// and https:// if present.
    """
    if value is None:
        return None

    if not isinstance(value, str):
        return value

    url = value.strip()
    if not url:
        return None

    if url.startswith("http://") or url.startswith("https://"):
        scheme, rest = url.split("://", 1)
        parts = [part for part in rest.split("/") if part]
        return f"{scheme}://" + "/".join(parts)

    parts = [part for part in url.split("/") if part]
    return "/" + "/".join(parts)


def build_child_record(row: dict[str, Any]) -> dict[str, Any]:
    """
    Build the child record stored under category -> product.
    """
    child = dict(row)

    # Remove grouping fields already represented by the nesting.
    child.pop("category", None)
    child.pop("product", None)
    child.pop("product_category", None)
    child.pop("product_name", None)

    if "url" in child:
        child["url"] = normalize_url_path(child["url"])

    return child


def sort_and_collapse(
    nested: dict[str, dict[str, list[dict[str, Any]]]]
) -> dict[str, dict[str, Any]]:
    """
    Return a sorted copy of the nested structure.

    If a product has only one record, store it as an object.
    Otherwise keep it as a list.
    """
    result: dict[str, dict[str, Any]] = {}

    for category in sorted(nested.keys(), key=lambda value: value.lower()):
        result[category] = {}

        for product_name in sorted(nested[category].keys(), key=lambda value: value.lower()):
            records = nested[category][product_name]
            result[category][product_name] = records[0] if len(records) == 1 else records

    return result


def convert_csv_to_nested_json(input_path: Path, output_path: Path) -> None:
    """
    Read the CSV, group rows into nested JSON, and write the result.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    nested: dict[str, dict[str, list[dict[str, Any]]]] = {}

    with input_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)

        if reader.fieldnames is None:
            raise ValueError("CSV appears to have no header row.")

        for raw_row in reader:
            row = build_normalized_row(raw_row)

            category = choose_category(row)
            product_name = choose_product_name(row, category)
            child_record = build_child_record(row)

            if category not in nested:
                nested[category] = {}

            if product_name not in nested[category]:
                nested[category][product_name] = []

            nested[category][product_name].append(child_record)

    result = sort_and_collapse(nested)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as json_file:
        json.dump(result, json_file, indent=2, ensure_ascii=False)

    total_categories = len(result)
    total_products = sum(len(products) for products in result.values())

    total_rows = 0
    for products in result.values():
        for value in products.values():
            if isinstance(value, list):
                total_rows += len(value)
            else:
                total_rows += 1

    print(f"Wrote nested JSON to: {output_path}")
    print(f"Categories: {total_categories}")
    print(f"Products:   {total_products}")
    print(f"Rows:       {total_rows}")


def main() -> None:
    """
    Script entrypoint.
    """
    convert_csv_to_nested_json(INPUT_PATH, OUTPUT_PATH)


if __name__ == "__main__":
    main()