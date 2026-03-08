#!/usr/bin/env python3
# File: scripts/build-coa-json.py

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
import shutil
import subprocess
from typing import Any
from urllib.parse import unquote


@dataclass(frozen=True)
class Paths:
    SCRIPT_DIR: Path
    BASE_DIR: Path
    CSV_SOURCE_PATH: Path
    CSV_TARGET_PATH: Path
    COA_SOURCE_DIR: Path
    COA_TARGET_DIR: Path
    FLOWER_SOURCE_DIR: Path
    FLOWER_TARGET_DIR: Path
    EDIBLES_SOURCE_DIR: Path
    EDIBLES_TARGET_DIR: Path
    BEVERAGES_SOURCE_DIR: Path
    BEVERAGES_TARGET_DIR: Path
    OUTPUT_JSON_PATH: Path


SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parent
COA_SOURCE_DIR = BASE_DIR / "COAs"
COA_TARGET_DIR = BASE_DIR / "public" / "coas"

PATHS = Paths(
    SCRIPT_DIR=SCRIPT_DIR,
    BASE_DIR=BASE_DIR,
    CSV_SOURCE_PATH=BASE_DIR / "skus.csv",
    CSV_TARGET_PATH=BASE_DIR / "data" / "skus.csv",
    COA_SOURCE_DIR=COA_SOURCE_DIR,
    COA_TARGET_DIR=COA_TARGET_DIR,
    FLOWER_SOURCE_DIR=COA_SOURCE_DIR / "flower",
    FLOWER_TARGET_DIR=COA_TARGET_DIR / "flower",
    EDIBLES_SOURCE_DIR=COA_SOURCE_DIR / "edibles",
    EDIBLES_TARGET_DIR=COA_TARGET_DIR / "edibles",
    BEVERAGES_SOURCE_DIR=COA_SOURCE_DIR / "beverages",
    BEVERAGES_TARGET_DIR=COA_TARGET_DIR / "beverages",
    OUTPUT_JSON_PATH=BASE_DIR / "public" / "coa-data.json",
)


@dataclass(frozen=True)
class CoaRef:
    lot: str
    file: str
    url: str


@dataclass
class Row:
    product_name: str
    sku: str
    raw_tags: str
    product_category: str
    thc: float
    coa: str
    netwt: str
    coa_refs: list[CoaRef]


def run_command(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


def parse_tags(raw_tags: str) -> dict[str, Any]:
    tags_dict: dict[str, Any] = {}

    if not raw_tags:
        return tags_dict

    for part in raw_tags.split(";"):
        tag = part.strip()
        if not tag:
            continue

        if "=" in tag:
            key, value = tag.split("=", 1)
            tags_dict[key.strip()] = value.strip()
        else:
            tags_dict[tag] = True

    return tags_dict


def parse_thc(tags_dict: dict[str, Any]) -> float:
    raw_value = str(tags_dict.get("thc", "")).strip()
    if not raw_value:
        return 0.0

    try:
        return float(raw_value.removesuffix("%").strip())
    except ValueError:
        return 0.0


def parse_coa_refs(tags_dict: dict[str, Any]) -> list[CoaRef]:
    coa_refs: list[CoaRef] = []

    raw_json = str(tags_dict.get("json", "")).strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid json tag: {raw_json}") from exc

        if not isinstance(parsed, list):
            raise ValueError(f"json tag must decode to a list, got {type(parsed).__name__}")

        for item in parsed:
            if not isinstance(item, dict):
                continue

            lot = str(item.get("lot", "")).strip()
            encoded_file = str(item.get("file", "")).strip()
            file_name = unquote(encoded_file)
            url = str(item.get("url", "")).strip()

            if not lot and not file_name and not url:
                continue

            coa_refs.append(CoaRef(lot=lot, file=file_name, url=url))

        return coa_refs

    lot = str(tags_dict.get("lot", "")).strip()
    encoded_file = str(tags_dict.get("file", "")).strip()
    file_name = unquote(encoded_file)
    url = str(tags_dict.get("url", "")).strip()

    if lot or file_name or url:
        coa_refs.append(CoaRef(lot=lot, file=file_name, url=url))

    return coa_refs


def extract_product_tags(csv_path: str | Path) -> list[Row]:
    csv_path = Path(csv_path)
    results: list[Row] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            product_name = (row.get("product_name") or row.get("name") or "").strip()
            sku = (row.get("sku") or row.get("SKU") or "").strip()
            raw_tags = (row.get("tags") or "").strip()
            product_category = (row.get("product_category") or "").strip()

            tags_dict = parse_tags(raw_tags)

            results.append(
                Row(
                    product_name=product_name,
                    sku=sku,
                    raw_tags=raw_tags,
                    product_category=product_category,
                    thc=parse_thc(tags_dict),
                    coa=str(tags_dict.get("coa", "")).strip(),
                    netwt=str(tags_dict.get("netwt", "")).strip(),
                    coa_refs=parse_coa_refs(tags_dict),
                )
            )

    return results


def normalize_category(product_category: str, coa_refs: list[CoaRef] | None = None) -> str:
    category = (product_category or "").strip().casefold()
    urls = [ref.url.casefold() for ref in (coa_refs or []) if ref.url]

    if "flower" in category or any("/flower/" in url for url in urls):
        return "Flower"
    if "edible" in category or any("/edibles/" in url for url in urls):
        return "Edibles"
    if "beverage" in category or any("/beverages/" in url for url in urls):
        return "Beverages"

    return "Uncategorized"


def get_source_and_target_dirs(row: Row) -> tuple[Path, Path] | None:
    category = normalize_category(row.product_category, row.coa_refs)

    if category == "Flower":
        return PATHS.FLOWER_SOURCE_DIR, PATHS.FLOWER_TARGET_DIR
    if category == "Edibles":
        return PATHS.EDIBLES_SOURCE_DIR, PATHS.EDIBLES_TARGET_DIR
    if category == "Beverages":
        return PATHS.BEVERAGES_SOURCE_DIR, PATHS.BEVERAGES_TARGET_DIR

    return None


def ensure_directories() -> None:
    PATHS.CSV_TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)
    PATHS.COA_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    PATHS.FLOWER_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    PATHS.EDIBLES_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    PATHS.BEVERAGES_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    PATHS.OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)


def copy_supporting_files(rows: list[Row]) -> None:
    shutil.copy2(PATHS.CSV_SOURCE_PATH, PATHS.CSV_TARGET_PATH)

    for row in rows:
        if not row.coa_refs:
            continue

        dirs = get_source_and_target_dirs(row)
        if dirs is None:
            continue

        source_dir, target_dir = dirs

        for ref in row.coa_refs:
            if not ref.file:
                continue

            src = source_dir / ref.file
            dst = target_dir / ref.file

            if src.is_file():
                shutil.copy2(src, dst)
            else:
                print(f"Warning: source COA file not found: {src}")


def build_nested_tree(rows: list[Row]) -> list[dict[str, Any]]:
    categories: dict[str, dict[str, Any]] = {}

    for row in rows:
        if not row.coa_refs:
            continue

        category = normalize_category(row.product_category, row.coa_refs)
        product_name = row.product_name or "Unnamed Product"

        category_bucket = categories.setdefault(
            category,
            {
                "category": category,
                "products": {},
            },
        )

        product_bucket = category_bucket["products"].setdefault(
            product_name,
            {
                "product": product_name,
                "sku": row.sku,
                "lots": {},
            },
        )

        for ref in row.coa_refs:
            lot_key = ref.lot or "UNSPECIFIED"

            lot_bucket = product_bucket["lots"].setdefault(
                lot_key,
                {
                    "lotNumber": ref.lot or "UNSPECIFIED",
                    "sku": row.sku,
                    "thc": row.thc,
                    "netWeight": row.netwt,
                    "coa": row.coa,
                    "notes": "",
                    "files": [],
                },
            )

            file_entry = {
                "name": ref.file or "COA.pdf",
                "url": ref.url or "#",
            }

            if file_entry not in lot_bucket["files"]:
                lot_bucket["files"].append(file_entry)

        # Keep values refreshed at the product/lot level
        for lot_bucket in product_bucket["lots"].values():
            if row.sku and not lot_bucket.get("sku"):
                lot_bucket["sku"] = row.sku
            if row.thc and not lot_bucket.get("thc"):
                lot_bucket["thc"] = row.thc
            if row.netwt and not lot_bucket.get("netWeight"):
                lot_bucket["netWeight"] = row.netwt
            if row.coa and not lot_bucket.get("coa"):
                lot_bucket["coa"] = row.coa

    nested_tree: list[dict[str, Any]] = []

    for category_name in sorted(categories.keys(), key=str.casefold):
        category_bucket = categories[category_name]
        products_output: list[dict[str, Any]] = []

        for product_name in sorted(category_bucket["products"].keys(), key=str.casefold):
            product_bucket = category_bucket["products"][product_name]
            lots_output: list[dict[str, Any]] = []

            for lot_key in sorted(product_bucket["lots"].keys(), key=str.casefold):
                lots_output.append(product_bucket["lots"][lot_key])

            products_output.append(
                {
                    "product": product_name,
                    "sku": product_bucket["sku"],
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


def write_json(output_path: Path, payload: list[dict[str, Any]]) -> None:
    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2, ensure_ascii=False)
        output_file.write("\n")


def get_current_branch(repo_root: Path) -> str:
    result = run_command(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=repo_root,
    )
    return result.stdout.strip()


def git_commit_and_push(repo_root: Path, commit_message: str) -> None:
    branch_name = get_current_branch(repo_root)

    run_command(["git", "add", "data/skus.csv"], cwd=repo_root)
    run_command(["git", "add", "public/coa-data.json"], cwd=repo_root)
    run_command(["git", "add", "public/coas"], cwd=repo_root)

    status = run_command(["git", "status", "--short"], cwd=repo_root)
    if not status.stdout.strip():
        print("No git changes to commit.")
        return

    run_command(["git", "commit", "-m", commit_message], cwd=repo_root)
    run_command(["git", "push", "origin", branch_name], cwd=repo_root)

    print(f"Committed and pushed to origin/{branch_name}")


def main() -> None:
    if not PATHS.CSV_SOURCE_PATH.is_file():
        raise FileNotFoundError(PATHS.CSV_SOURCE_PATH)

    ensure_directories()

    rows = extract_product_tags(PATHS.CSV_SOURCE_PATH)
    copy_supporting_files(rows)

    payload = build_nested_tree(rows)
    write_json(PATHS.OUTPUT_JSON_PATH, payload)

    print(f"Copied CSV to: {PATHS.CSV_TARGET_PATH}")
    print(f"Wrote JSON to: {PATHS.OUTPUT_JSON_PATH}")
    print(f"Built {len(payload)} categories.")

    # Change this to False if you want to disable auto-push by default.
    AUTO_PUSH = True

    if AUTO_PUSH:
        git_commit_and_push(
            repo_root=PATHS.BASE_DIR,
            commit_message="Update COA data and assets",
        )


if __name__ == "__main__":
    main()