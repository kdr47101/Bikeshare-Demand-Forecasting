import os
import json
from pathlib import Path
from urllib.parse import urlparse
import requests
import csv
import time

from config import BASE_URL, PACKAGE_ID_RIDERSHIP, PACKAGE_ID_STATION, API
 

# This file handles downloading metadata and files from Toronto's Open Data Portal CKAN instance.
# It saves package metadata, resource metadata, and downloads non-datastore resources
# It also downloads station information and status from the GBFS feed and saves as CSV.

RAW_DIR = Path("data") / "raw"

DOWNLOAD_FORMAT_WHITELIST = {"CSV", "JSON", "ZIP", "XLS", "XLSX", "GEOJSON", "PARQUET"}
DOWNLOAD_SUFFIX_WHITELIST = {".csv", ".json", ".zip", ".xls", ".xlsx", ".geojson", ".parquet"}

def get_package_json(package_id: str):
    """
    Mirrors the original 'package_show' request:
    returns the JSON for package metadata (including its resources list).
    """
    url = BASE_URL + API["package_show"]
    params = {"id": package_id}
    return requests.get(url, params=params).json()

def _safe_write_json(obj, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _filename_from_url(url: str, fallback: str) -> str:
    name = os.path.basename(urlparse(url).path)
    return name if name else fallback

def _collect_fieldnames(rows):
    ordered = []
    for row in rows:
        for key in row.keys():
            if key not in ordered:
                ordered.append(key)
    return ordered

def _write_csv(path: Path, rows, fieldnames):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

def _dedupe_by_station_id(rows):
    seen = set()
    deduped = []
    for row in rows:
        sid = row.get("station_id")
        if sid and sid in seen:
            continue
        if sid:
            seen.add(sid)
        deduped.append(row)
    return deduped

def _locate_cached_file(filename: str):
    for candidate in RAW_DIR.rglob(filename):
        if candidate.is_file():
            return candidate
    return None

def _is_downloadable_resource(file_format: str, file_url: str) -> bool:
    suffix = Path(urlparse(file_url).path).suffix.lower()
    return (
        file_format in DOWNLOAD_FORMAT_WHITELIST
        or suffix in DOWNLOAD_SUFFIX_WHITELIST
    )

def _head_content_length(url: str) -> int | None:
    try:
        r = requests.head(url, timeout=15, allow_redirects=True)
        if r.ok:
            return int(r.headers.get("content-length") or 0) or None
    except Exception:
        pass
    return None

def _download_with_resume(file_url: str, dest: Path, max_retries: int = 4) -> bool:
    """
    Stream download with HTTP Range resume support and basic size verification.
    Returns True if file is present and looks valid.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    expected_size = _head_content_length(file_url)

    for attempt in range(1, max_retries + 1):
        try:
            # Resume if partial exists
            headers = {}
            mode = "wb"
            existing = dest.stat().st_size if dest.exists() else 0
            if existing and (expected_size is None or existing < expected_size):
                headers["Range"] = f"bytes={existing}-"
                mode = "ab"

            with requests.get(file_url, stream=True, headers=headers, timeout=60) as r:
                r.raise_for_status()
                with dest.open(mode) as f:
                    for chunk in r.iter_content(chunk_size=2 * 1024 * 1024):
                        if chunk:
                            f.write(chunk)

            # Size check if we know expected_size
            if expected_size is not None and dest.stat().st_size != expected_size:
                # Backoff and retry
                time.sleep(1.5 * attempt)
                continue

            return True
        except Exception as e:
            if attempt == max_retries:
                print(f"Download failed: {file_url} -> {e}")
                break
            # Remove corrupt partial if server doesn't support range
            time.sleep(1.5 * attempt)
    return dest.exists() and (expected_size is None or dest.stat().st_size >= (expected_size * 0.95))

def print_and_save_non_datastore_resources(package_json, package_id: str):
    """
    Keeps the original behavior (prints resource metadata), and ALSO:
      - writes package.json to data/raw/
      - writes each resource_show JSON to data/raw/resource_{idx}_{id}_metadata.json
      - if resource_show has a 'result.url', downloads that file into data/raw/
    """
    pkg_dir = RAW_DIR / package_id
    metadata_dir = pkg_dir / "metadata"
    download_dir = pkg_dir / "downloads"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    download_dir.mkdir(parents=True, exist_ok=True)

    _safe_write_json(package_json, pkg_dir / "package.json")

    for idx, resource in enumerate(package_json["result"]["resources"]):
        if not resource.get("datastore_active", False):
            # Fetch resource_show metadata
            url = BASE_URL + API["resource_show"]
            params = {"id": resource["id"]}
            resource_metadata = requests.get(url, params=params).json()

            # Original behavior: print it
            print(resource_metadata)

            meta_path = metadata_dir / f"{idx:02d}_{resource['id']}_metadata.json"
            _safe_write_json(resource_metadata, meta_path)

            result = resource_metadata.get("result", {})
            file_url = result.get("url")
            file_format = (result.get("format") or resource.get("format") or "").upper()
            if file_url and _is_downloadable_resource(file_format, file_url):
                filename = _filename_from_url(file_url, f"{resource['id']}.bin")
                dest = download_dir / filename

                # Skip if already complete
                expected = result.get("size") or 0
                try:
                    expected = int(expected)
                except Exception:
                    expected = 0
                if dest.exists() and expected > 0 and dest.stat().st_size == expected:
                    print(f"Skipping {filename} (already complete)")
                    continue

                ok = _download_with_resume(file_url, dest, max_retries=5)
                if ok:
                    print(f"Saved file: {dest}")
                else:
                    if dest.exists():
                        dest.unlink(missing_ok=True)
                    print(f"Could not download {file_url}")
            elif file_url:
                print(f"Skipping non-data resource '{resource.get('name')}' ({file_format or 'unknown'}).")

def download_station_data():
    """
    Download station information and status from Toronto's bike share GBFS feed.
    Saves the data as CSV files.
    """
    gbfs_url = "https://tor.publicbikesystem.net/ube/gbfs/v1/gbfs.json"
    gbfs_discovery_path = RAW_DIR / "bike-share-gbfs.json"

    gbfs_data = None
    for candidate_name in ("bike-share-gbfs.json", "bike-share-json.json"):
        cached_path = _locate_cached_file(candidate_name)
        if not cached_path:
            continue
        try:
            with cached_path.open("r", encoding="utf-8") as f:
                gbfs_data = json.load(f)
            break
        except Exception as exc:
            print(f"Could not read {cached_path}: {exc}")

    if not gbfs_data:
        try:
            response = requests.get(gbfs_url, timeout=30)
            response.raise_for_status()
            gbfs_data = response.json()
            _safe_write_json(gbfs_data, gbfs_discovery_path)
        except Exception as exc:
            print(f"Failed to download GBFS discovery feed: {exc}")
            return

    feeds = gbfs_data.get("data", {}).get("en", {}).get("feeds", [])
    feed_lookup = {feed.get("name"): feed.get("url") for feed in feeds if feed.get("name") and feed.get("url")}

    def _fetch_feed(name: str):
        url = feed_lookup.get(name)
        if not url:
            raise ValueError(f"Missing '{name}' feed URL in GBFS discovery.")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        payload = response.json()
        _safe_write_json(payload, RAW_DIR / f"{name}.json")
        return payload

    try:
        station_info = _fetch_feed("station_information")
        station_status = _fetch_feed("station_status")
    except Exception as exc:
        print(f"Failed to download GBFS feeds: {exc}")
        return

    info_stations = _dedupe_by_station_id(station_info.get("data", {}).get("stations", []))
    status_stations = _dedupe_by_station_id(station_status.get("data", {}).get("stations", []))
    if not info_stations or not status_stations:
        print("Station feeds returned no station records.")
        return

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    info_csv = RAW_DIR / "station_information.csv"
    status_csv = RAW_DIR / "station_status.csv"
    combined_csv = RAW_DIR / "station_details.csv"

    info_fields = _collect_fieldnames(info_stations)
    _write_csv(info_csv, info_stations, info_fields)
    print(f"Saved station information CSV: {info_csv}")

    status_fields = _collect_fieldnames(status_stations)
    _write_csv(status_csv, status_stations, status_fields)
    print(f"Saved station status CSV: {status_csv}")

    info_lookup = {row["station_id"]: row for row in info_stations if "station_id" in row}
    combined_fieldnames = info_fields + [field for field in status_fields if field not in info_fields]
    combined_rows = []
    for status_row in status_stations:
        station_id = status_row.get("station_id")
        combined_row = {**info_lookup.get(station_id, {}), **status_row}
        combined_rows.append(combined_row)

    _write_csv(combined_csv, combined_rows, combined_fieldnames)
    print(f"Saved combined station details CSV: {combined_csv}")

if __name__ == "__main__":
    for PID in (PACKAGE_ID_RIDERSHIP, PACKAGE_ID_STATION):
        pkg = get_package_json(PID)
        print_and_save_non_datastore_resources(pkg, PID)
    download_station_data()
