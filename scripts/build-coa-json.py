#!/usr/bin/env python3
# File: scripts/build-coa-json.py

from __future__ import annotations

import csv
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


def log(message: str) -> None:
    print(f'[build-coa-json] {message}', flush=True)


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
    VAPES_SOURCE_DIR: Path
    VAPES_TARGET_DIR: Path
    OUTPUT_JSON_PATH: Path
    BUILD_INFO_PATH: Path


SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parent
COA_SOURCE_DIR = BASE_DIR / 'COAs'
COA_TARGET_DIR = BASE_DIR / 'public' / 'coas'

PATHS = Paths(
    SCRIPT_DIR=SCRIPT_DIR,
    BASE_DIR=BASE_DIR,
    CSV_SOURCE_PATH=BASE_DIR / 'skus.csv',
    CSV_TARGET_PATH=BASE_DIR / 'data' / 'skus.csv',
    COA_SOURCE_DIR=COA_SOURCE_DIR,
    COA_TARGET_DIR=COA_TARGET_DIR,
    FLOWER_SOURCE_DIR=COA_SOURCE_DIR / 'flower',
    FLOWER_TARGET_DIR=COA_TARGET_DIR / 'flower',
    EDIBLES_SOURCE_DIR=COA_SOURCE_DIR / 'edibles',
    EDIBLES_TARGET_DIR=COA_TARGET_DIR / 'edibles',
    BEVERAGES_SOURCE_DIR=COA_SOURCE_DIR / 'beverages',
    BEVERAGES_TARGET_DIR=COA_TARGET_DIR / 'beverages',
    VAPES_SOURCE_DIR=COA_SOURCE_DIR / 'vapes',
    VAPES_TARGET_DIR=COA_TARGET_DIR / 'vapes',
    OUTPUT_JSON_PATH=BASE_DIR / 'public' / 'coa-data.json',
    BUILD_INFO_PATH=BASE_DIR / 'public' / 'build-info.json',
)

DEFAULT_SITE_BASE_URL = os.environ.get('COA_SITE_BASE_URL', 'https://coa.dthemp.com')
AUTO_PUSH = True
VERIFY_DEPLOYMENT = True
VERIFY_TIMEOUT_SECONDS = 600
VERIFY_POLL_INTERVAL_SECONDS = 10
HTTP_TIMEOUT_SECONDS = 20


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
    log(f"Running command: {' '.join(cmd)} [cwd={cwd}]")
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.stdout.strip():
        log(f'stdout:\n{result.stdout.strip()}')
    if result.stderr.strip():
        log(f'stderr:\n{result.stderr.strip()}')
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

    for part in raw_tags.split(';'):
        tag = part.strip()
        if not tag:
            continue
        if '=' in tag:
            key, value = tag.split('=', 1)
            tags_dict[key.strip()] = value.strip()
        else:
            tags_dict[tag] = True
    return tags_dict


def parse_thc(tags_dict: dict[str, Any]) -> float:
    raw_value = str(tags_dict.get('thc', '')).strip()
    if not raw_value:
        return 0.0
    try:
        return float(raw_value.removesuffix('%').strip())
    except ValueError:
        return 0.0


def _decode_file_name(value: str) -> str:
    return unquote((value or '').strip())


def _collect_indexed_coa_refs(tags_dict: dict[str, Any]) -> list[CoaRef]:
    grouped: dict[int, dict[str, str]] = {}
    pattern = re.compile(r'^coa_ref_(\d+)_(lot|file|url)$')

    for key, value in tags_dict.items():
        match = pattern.match(str(key))
        if not match:
            continue
        idx = int(match.group(1))
        field_name = match.group(2)
        grouped.setdefault(idx, {})[field_name] = str(value).strip()

    refs: list[CoaRef] = []
    for idx in sorted(grouped.keys()):
        item = grouped[idx]
        lot = item.get('lot', '').strip()
        file_name = _decode_file_name(item.get('file', ''))
        url = item.get('url', '').strip()
        if not lot and not file_name and not url:
            continue
        refs.append(CoaRef(lot=lot, file=file_name, url=url))
    return refs


def parse_coa_refs(tags_dict: dict[str, Any], raw_tags: str = '') -> list[CoaRef]:
    indexed_refs = _collect_indexed_coa_refs(tags_dict)
    if indexed_refs:
        return indexed_refs

    coa_refs: list[CoaRef] = []
    raw_json = str(tags_dict.get('json', '')).strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            for item in parsed:
                if not isinstance(item, dict):
                    continue
                lot = str(item.get('lot', '')).strip()
                file_name = _decode_file_name(str(item.get('file', '')))
                url = str(item.get('url', '')).strip()
                if lot or file_name or url:
                    coa_refs.append(CoaRef(lot=lot, file=file_name, url=url))
        if coa_refs:
            return coa_refs

    lot = str(tags_dict.get('lot', '')).strip()
    file_name = _decode_file_name(str(tags_dict.get('file', '')))
    url = str(tags_dict.get('url', '')).strip()
    if lot or file_name or url:
        return [CoaRef(lot=lot, file=file_name, url=url)]

    lots = re.findall(r'[^A-Za-z]lot\s*[:=]\s*"?([^";\]\}]*)', f' {raw_tags or ""}')
    files = re.findall(r'[^A-Za-z]file\s*[:=]\s*"?([^";\]\}]*)', f' {raw_tags or ""}')
    urls = re.findall(r'[^A-Za-z]url\s*[:=]\s*"?([^";\]\}]*)', f' {raw_tags or ""}')
    n = max(len(lots), len(files), len(urls), 0)

    for idx in range(n):
        lot = lots[idx].strip() if idx < len(lots) else ''
        file_name = files[idx].strip() if idx < len(files) else ''
        url = urls[idx].strip() if idx < len(urls) else ''

        if 'file=' in lot and not file_name:
            lot, file_name = lot.split('file=', 1)
            lot = lot.strip()
            file_name = file_name.strip()

        file_name = _decode_file_name(file_name)
        if lot or file_name or url:
            coa_refs.append(CoaRef(lot=lot, file=file_name, url=url))

    return coa_refs


def extract_product_tags(csv_path: str | Path) -> list[Row]:
    csv_path = Path(csv_path)
    results: list[Row] = []

    log(f'Reading CSV: {csv_path}')
    with csv_path.open('r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader, start=1):
            product_name = (row.get('product_name') or row.get('name') or '').strip()
            sku = (row.get('sku') or row.get('SKU') or '').strip()
            raw_tags = (row.get('tags') or '').strip()
            product_category = (row.get('product_category') or '').strip()

            tags_dict = parse_tags(raw_tags)
            coa_refs = parse_coa_refs(tags_dict, raw_tags)

            parsed_row = Row(
                product_name=product_name,
                sku=sku,
                raw_tags=raw_tags,
                product_category=product_category,
                thc=parse_thc(tags_dict),
                coa=str(tags_dict.get('coa', '')).strip(),
                netwt=str(tags_dict.get('netwt', '')).strip(),
                coa_refs=coa_refs,
            )
            results.append(parsed_row)

            if row_index <= 5 or coa_refs:
                log(
                    f'Row {row_index}: sku={sku or "<no sku>"} '
                    f'product={product_name or "<unnamed>"} '
                    f'category={product_category or "<none>"} '
                    f'coa_refs={len(coa_refs)}'
                )

    log(f'Finished reading CSV. Parsed {len(results)} rows.')
    return results


def normalize_category(product_category: str, coa_refs: list[CoaRef] | None = None) -> str:
    category = (product_category or '').strip().casefold()
    urls = [ref.url.casefold() for ref in (coa_refs or []) if ref.url]

    if 'flower' in category or any('/flower/' in url for url in urls):
        return 'Flower'
    if 'edible' in category or any('/edibles/' in url for url in urls):
        return 'Edibles'
    if 'beverage' in category or any('/beverages/' in url for url in urls):
        return 'Beverages'
    if 'vape' in category or any('/vapes/' in url for url in urls):
        return 'Vapes'
    return 'Uncategorized'


def get_source_and_target_dirs(row: Row) -> tuple[Path, Path] | None:
    category = normalize_category(row.product_category, row.coa_refs)
    if category == 'Flower':
        return PATHS.FLOWER_SOURCE_DIR, PATHS.FLOWER_TARGET_DIR
    if category == 'Edibles':
        return PATHS.EDIBLES_SOURCE_DIR, PATHS.EDIBLES_TARGET_DIR
    if category == 'Beverages':
        return PATHS.BEVERAGES_SOURCE_DIR, PATHS.BEVERAGES_TARGET_DIR
    if category == 'Vapes':
        return PATHS.VAPES_SOURCE_DIR, PATHS.VAPES_TARGET_DIR
    return None


def ensure_directories() -> None:
    log('Ensuring output directories exist.')
    PATHS.CSV_TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)
    PATHS.COA_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    PATHS.FLOWER_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    PATHS.EDIBLES_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    PATHS.BEVERAGES_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    PATHS.VAPES_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    PATHS.OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    PATHS.BUILD_INFO_PATH.parent.mkdir(parents=True, exist_ok=True)


def copy_supporting_files(rows: list[Row]) -> None:
    log(f'Copying source CSV to {PATHS.CSV_TARGET_PATH}')
    shutil.copy2(PATHS.CSV_SOURCE_PATH, PATHS.CSV_TARGET_PATH)

    copied_files = 0
    missing_files = 0

    for row in rows:
        if not row.coa_refs:
            continue

        dirs = get_source_and_target_dirs(row)
        if dirs is None:
            log(f'Skipping sku={row.sku or "<no sku>"}: could not determine category from product_category/url.')
            continue

        source_dir, target_dir = dirs
        for ref in row.coa_refs:
            if not ref.file:
                log(f'Skipping sku={row.sku or "<no sku>"} lot={ref.lot or "<no lot>"}: no file name present.')
                continue
            src = source_dir / ref.file
            dst = target_dir / ref.file
            if src.is_file():
                shutil.copy2(src, dst)
                copied_files += 1
                log(f'Copied COA for sku={row.sku or "<no sku>"}: {src} -> {dst}')
            else:
                missing_files += 1
                log(f'Warning: source COA file not found for sku={row.sku or "<no sku>"}: {src}')

    log(f'Finished copying files. copied_files={copied_files} missing_files={missing_files}')


def build_nested_tree(rows: list[Row]) -> list[dict[str, Any]]:
    categories: dict[str, dict[str, Any]] = {}

    for row in rows:
        if not row.coa_refs:
            continue

        category = normalize_category(row.product_category, row.coa_refs)
        product_name = row.product_name or 'Unnamed Product'

        category_bucket = categories.setdefault(category, {'category': category, 'products': {}})
        product_bucket = category_bucket['products'].setdefault(
            product_name,
            {'product': product_name, 'sku': row.sku, 'lots': {}},
        )

        for ref in row.coa_refs:
            lot_key = ref.lot or 'UNSPECIFIED'
            lot_bucket = product_bucket['lots'].setdefault(
                lot_key,
                {
                    'lotNumber': ref.lot or 'UNSPECIFIED',
                    'sku': row.sku,
                    'thc': row.thc,
                    'netWeight': row.netwt,
                    'coa': row.coa,
                    'notes': '',
                    'files': [],
                },
            )

            file_entry = {'name': ref.file or 'COA.pdf', 'url': ref.url or '#'}
            if file_entry not in lot_bucket['files']:
                lot_bucket['files'].append(file_entry)

        for lot_bucket in product_bucket['lots'].values():
            if row.sku and not lot_bucket.get('sku'):
                lot_bucket['sku'] = row.sku
            if row.thc and not lot_bucket.get('thc'):
                lot_bucket['thc'] = row.thc
            if row.netwt and not lot_bucket.get('netWeight'):
                lot_bucket['netWeight'] = row.netwt
            if row.coa and not lot_bucket.get('coa'):
                lot_bucket['coa'] = row.coa

    nested_tree: list[dict[str, Any]] = []
    for category_name in sorted(categories.keys(), key=str.casefold):
        category_bucket = categories[category_name]
        products_output: list[dict[str, Any]] = []
        for product_name in sorted(category_bucket['products'].keys(), key=str.casefold):
            product_bucket = category_bucket['products'][product_name]
            lots_output = [product_bucket['lots'][lot_key] for lot_key in sorted(product_bucket['lots'].keys(), key=str.casefold)]
            products_output.append({'product': product_name, 'sku': product_bucket['sku'], 'lots': lots_output})
        nested_tree.append({'category': category_name, 'products': products_output})
    return nested_tree


def write_json(output_path: Path, payload: Any) -> None:
    log(f'Writing JSON output to {output_path}')
    with output_path.open('w', encoding='utf-8') as output_file:
        json.dump(payload, output_file, indent=2, ensure_ascii=False)
        output_file.write('\n')


def generate_build_number() -> str:
    return datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')


def get_current_branch(repo_root: Path) -> str:
    result = run_command(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], cwd=repo_root)
    return result.stdout.strip()


def get_current_commit(repo_root: Path) -> str:
    result = run_command(['git', 'rev-parse', '--short', 'HEAD'], cwd=repo_root)
    return result.stdout.strip()


def build_build_info(repo_root: Path, build_number: str, row_count: int, tagged_row_count: int, payload: list[dict[str, Any]]) -> dict[str, Any]:
    product_count = sum(len(category.get('products', [])) for category in payload)
    lot_count = sum(len(product.get('lots', [])) for category in payload for product in category.get('products', []))
    file_count = sum(
        len(lot.get('files', []))
        for category in payload
        for product in category.get('products', [])
        for lot in product.get('lots', [])
    )
    branch = get_current_branch(repo_root)
    commit = get_current_commit(repo_root)
    built_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    return {
        'buildNumber': build_number,
        'builtAtUtc': built_at_utc,
        'branch': branch,
        'sourceCommit': commit,
        'rowCount': row_count,
        'rowsWithCoaRefs': tagged_row_count,
        'categoryCount': len(payload),
        'productCount': product_count,
        'lotCount': lot_count,
        'fileCount': file_count,
    }


def git_commit_and_push(repo_root: Path, commit_message: str) -> None:
    log('Preparing git add/commit/push.')
    branch_name = get_current_branch(repo_root)
    log(f'Current git branch: {branch_name}')

    run_command(['git', 'add', 'data/skus.csv'], cwd=repo_root)
    run_command(['git', 'add', 'public/coa-data.json'], cwd=repo_root)
    run_command(['git', 'add', 'public/build-info.json'], cwd=repo_root)
    run_command(['git', 'add', 'public/coas'], cwd=repo_root)

    diff_result = subprocess.run(['git', 'diff', '--cached', '--quiet'], cwd=str(repo_root), check=False)
    if diff_result.returncode == 0:
        log('No staged git changes to commit.')
        return
    if diff_result.returncode != 1:
        raise RuntimeError('Failed to check staged git changes.')

    log(f'Creating git commit: {commit_message}')
    run_command(['git', 'commit', '-m', commit_message], cwd=repo_root)
    log(f'Pushing to origin/{branch_name}')
    run_command(['git', 'push', 'origin', branch_name], cwd=repo_root)
    log(f'Committed and pushed to origin/{branch_name}')


def http_get_json(url: str) -> dict[str, Any]:
    request = Request(url, headers={'Cache-Control': 'no-cache', 'Pragma': 'no-cache', 'User-Agent': 'coa-build-check/1.0'})
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        body = response.read().decode('utf-8')
    return json.loads(body)


def http_check_url(url: str) -> tuple[int, str]:
    request = Request(url, headers={'Cache-Control': 'no-cache', 'Pragma': 'no-cache', 'User-Agent': 'coa-build-check/1.0'})
    try:
        with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            return int(getattr(response, 'status', 200) or 200), response.geturl()
    except HTTPError as exc:
        return int(exc.code), url
    except URLError as exc:
        raise RuntimeError(f'Network error for {url}: {exc}') from exc


def collect_unique_coa_urls(rows: list[Row]) -> list[str]:
    urls: set[str] = set()
    for row in rows:
        for ref in row.coa_refs:
            url = (ref.url or '').strip()
            if url:
                urls.add(url)
    return sorted(urls)


def wait_for_deployed_build(site_base_url: str, expected_build_number: str) -> None:
    build_info_url = urljoin(site_base_url.rstrip('/') + '/', 'build-info.json')
    deadline = time.time() + VERIFY_TIMEOUT_SECONDS
    attempt = 0

    while time.time() < deadline:
        attempt += 1
        log(f'Verify attempt {attempt}: checking deployed build info at {build_info_url}')
        try:
            remote_build_info = http_get_json(build_info_url)
            remote_build_number = str(remote_build_info.get('buildNumber', '')).strip()
            log(f'Deployed buildNumber={remote_build_number or "<missing>"}')
            if remote_build_number == expected_build_number:
                log(f'Deployed site is on expected buildNumber={expected_build_number}')
                return
        except Exception as exc:
            log(f'Verify attempt {attempt} could not read build info yet: {exc}')

        time.sleep(VERIFY_POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        f'Deployed site did not update to buildNumber={expected_build_number} '
        f'within {VERIFY_TIMEOUT_SECONDS} seconds.'
    )


def verify_coa_urls(site_base_url: str, rows: list[Row]) -> None:
    relative_urls = collect_unique_coa_urls(rows)
    if not relative_urls:
        log('No COA URLs found to verify.')
        return

    log(f'Verifying {len(relative_urls)} unique COA URLs against {site_base_url}')
    failures: list[tuple[str, int]] = []

    for index, relative_url in enumerate(relative_urls, start=1):
        absolute_url = urljoin(site_base_url.rstrip('/') + '/', relative_url.lstrip('/'))
        status_code, final_url = http_check_url(absolute_url)
        if 200 <= status_code < 400:
            log(f'URL OK [{index}/{len(relative_urls)}] {status_code} {absolute_url} -> {final_url}')
        else:
            log(f'URL FAIL [{index}/{len(relative_urls)}] {status_code} {absolute_url}')
            failures.append((absolute_url, status_code))

    if failures:
        failure_lines = '\n'.join(f'  - {status} {url}' for url, status in failures)
        raise RuntimeError(f'COA URL verification failed for {len(failures)} URL(s):\n{failure_lines}')

    log(f'All {len(relative_urls)} COA URLs returned non-error HTTP status codes.')


def main() -> None:
    site_base_url = DEFAULT_SITE_BASE_URL
    build_number = generate_build_number()

    log('Starting build-coa-json run.')
    log(f'Base dir: {PATHS.BASE_DIR}')
    log(f'CSV source path: {PATHS.CSV_SOURCE_PATH}')
    log(f'Site base URL: {site_base_url}')
    log(f'Build number: {build_number}')

    if not PATHS.CSV_SOURCE_PATH.is_file():
        raise FileNotFoundError(PATHS.CSV_SOURCE_PATH)

    ensure_directories()

    rows = extract_product_tags(PATHS.CSV_SOURCE_PATH)
    tagged_rows = sum(1 for row in rows if row.coa_refs)
    log(f'Rows with COA refs: {tagged_rows}')

    copy_supporting_files(rows)

    log('Building nested COA payload.')
    payload = build_nested_tree(rows)
    write_json(PATHS.OUTPUT_JSON_PATH, payload)

    build_info = build_build_info(
        repo_root=PATHS.BASE_DIR,
        build_number=build_number,
        row_count=len(rows),
        tagged_row_count=tagged_rows,
        payload=payload,
    )
    write_json(PATHS.BUILD_INFO_PATH, build_info)

    log(f'Copied CSV to: {PATHS.CSV_TARGET_PATH}')
    log(f'Wrote JSON to: {PATHS.OUTPUT_JSON_PATH}')
    log(f'Wrote build info to: {PATHS.BUILD_INFO_PATH}')
    log(f'Built {len(payload)} categories.')

    if AUTO_PUSH:
        git_commit_and_push(repo_root=PATHS.BASE_DIR, commit_message=f'Update COA data and assets build {build_number}')

    if VERIFY_DEPLOYMENT:
        wait_for_deployed_build(site_base_url=site_base_url, expected_build_number=build_number)
        verify_coa_urls(site_base_url=site_base_url, rows=rows)

    log('Done.')


if __name__ == '__main__':
    main()