import zipfile
from pathlib import Path
import shutil
import json
import requests
import time
from collections import defaultdict
import re

RAW_DIR = Path("data") / "raw" / "bike-share-toronto-ridership-data" / "downloads"
META_DIR = Path("data") / "raw" / "bike-share-toronto-ridership-data" / "metadata"
INTERIM_DIR = Path("data") / "interim"

def _find_year_resource_url(year: int) -> str | None:
    """
    Inspect saved CKAN resource_show metadata to locate the download URL for a given year.
    """
    if not META_DIR.exists():
        return None
    for meta in sorted(META_DIR.glob("*_metadata.json")):
        try:
            with meta.open("r", encoding="utf-8") as f:
                doc = json.load(f)
            res = doc.get("result", {}) or {}
            name = (res.get("name") or "").lower()
            url = res.get("url")
            if str(year) in name and url:
                return url
        except Exception:
            continue
    return None

def _head_content_length(url: str) -> int | None:
    try:
        r = requests.head(url, timeout=15, allow_redirects=True)
        if r.ok:
            return int(r.headers.get("content-length") or 0) or None
    except Exception:
        pass
    return None

def _download_with_resume(file_url: str, dest: Path, max_retries: int = 5) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    expected_size = _head_content_length(file_url)
    for attempt in range(1, max_retries + 1):
        try:
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
            if expected_size is not None and dest.stat().st_size != expected_size:
                time.sleep(1.2 * attempt)
                continue
            return True
        except Exception:
            if attempt == max_retries:
                break
            time.sleep(1.2 * attempt)
    return dest.exists() and (expected_size is None or dest.stat().st_size >= (expected_size * 0.95))

def _infer_start_datetime_field(headers: list[str]) -> str | None:
    """
    Try to infer the 'start time' column across schema variants.
    """
    candidates = [h for h in headers if h]  # keep order
    low = [h.lower() for h in candidates]
    for i, h in enumerate(low):
        if "start" in h and ("time" in h or "date" in h or "datetime" in h):
            return candidates[i]
    # Common exact names fallback
    for name in (
        "Start Time", "Trip Start Time", "Start time", "start_time", "starttime", "start", "Start Date", "StartDate",
        "Started At", "started_at", "Started at",
    ):
        if name in candidates:
            return name
    return None

# Replace simple regex with robust parsing of multiple formats
_y_m_d = re.compile(r"(?P<year>\d{4})[-/](?P<month>\d{1,2})(?:[-/]\d{1,2})?")
_m_d_y = re.compile(r"(?P<month>\d{1,2})[-/](?P<day>\d{1,2})[-/](?P<year>\d{4})")
_d_m_y = re.compile(r"(?P<day>\d{1,2})[-/](?P<month>\d{1,2})[-/](?P<year>\d{4})")

def _parse_year_month_any(value: str) -> tuple[int, int] | None:
    """
    Extract (year, month) from a variety of date strings:
    - YYYY-MM-DD or YYYY/MM/DD
    - MM/DD/YYYY
    - DD/MM/YYYY
    - ISO-like strings containing those patterns
    """
    if not value:
        return None
    v = str(value).strip()
    # Try YYYY-MM
    m = _y_m_d.search(v)
    if m:
        y = int(m.group("year"))
        mm = int(m.group("month"))
        if 1 <= mm <= 12:
            return (y, mm)
    # Try MM/DD/YYYY
    m = _m_d_y.search(v)
    if m:
        y = int(m.group("year"))
        mm = int(m.group("month"))
        if 1 <= mm <= 12:
            return (y, mm)
    # Try DD/MM/YYYY
    m = _d_m_y.search(v)
    if m:
        y = int(m.group("year"))
        mm = int(m.group("month"))
        if 1 <= mm <= 12:
            return (y, mm)
    return None

def _extract_year_month(value: str, expected_year: int) -> str | None:
    """
    Return 'YYYY-MM' if a parsable date is found and matches expected_year.
    """
    parsed = _parse_year_month_any(value)
    if not parsed:
        return None
    y, m = parsed
    if y != expected_year:
        return None
    return f"{y}-{m:02d}"

def _split_quarterly_to_monthly(year_dir: Path, quarter_files: list[Path], year_int: int) -> int:
    """
    Read quarterly CSVs and write 12 monthly CSVs into year_dir.
    Returns number of monthly files written.
    """
    monthly_buckets: dict[str, list[dict]] = defaultdict(list)
    header_order: list[str] | None = None

    for qf in quarter_files:
        try:
            # Try utf-8-sig first, then latin-1 if needed
            try:
                f = qf.open("r", encoding="utf-8-sig", newline="")
                close_f = True
            except UnicodeDecodeError:
                f = qf.open("r", encoding="latin-1", newline="")
                close_f = True

            with f:
                import csv as _csv
                reader = _csv.DictReader(f)
                if reader.fieldnames and header_order is None:
                    header_order = list(reader.fieldnames)

                start_col = _infer_start_datetime_field(reader.fieldnames or [])

                for row in reader:
                    ym = None

                    # Prefer inferred start column if available
                    if start_col:
                        ym = _extract_year_month(row.get(start_col, ""), year_int)

                    # Fallback: scan all columns for a parsable date
                    if not ym:
                        for v in row.values():
                            ym = _extract_year_month(v, year_int)
                            if ym:
                                break

                    if ym:
                        monthly_buckets[ym].append(row)
        except Exception as e:
            print(f"  -> Failed reading {qf.name}: {e}")

    if not monthly_buckets or header_order is None:
        return 0

    # Write monthly CSVs
    written = 0
    for ym, rows in sorted(monthly_buckets.items()):
        out_path = year_dir / f"{ym}.csv"
        try:
            with out_path.open("w", encoding="utf-8", newline="") as f:
                import csv as _csv
                writer = _csv.DictWriter(f, fieldnames=header_order, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            written += 1
        except Exception as e:
            print(f"  -> Failed writing {out_path.name}: {e}")

    # Remove quarterly CSVs after successful split
    if written > 0:
        for qf in quarter_files:
            try:
                qf.unlink(missing_ok=True)
            except Exception:
                pass

    return written

def unzip_ridership_files():
    """
    Unzip all ridership ZIP files from 2017-2024 and extract to data/interim.
    Each ZIP contains monthly CSV files. If only quarterly CSVs are present,
    they are split into monthly CSVs.
    """
    INTERIM_DIR.mkdir(parents=True, exist_ok=True)
    zip_files = sorted(RAW_DIR.glob("bikeshare-ridership-20*.zip"))

    for zip_path in zip_files:
        year = zip_path.stem.split('-')[-1]
        try:
            year_int = int(year)
            if year_int < 2017 or year_int > 2024:
                continue
        except ValueError:
            continue

        year_dir = INTERIM_DIR / f"ridership_{year}"
        year_dir.mkdir(exist_ok=True)
        print(f"Extracting {zip_path.name} to {year_dir}")

        # Validate ZIP; if broken, try to re-download it first
        if not zipfile.is_zipfile(zip_path):
            print(f"  -> {zip_path.name} appears corrupted. Attempting re-download...")
            url = _find_year_resource_url(year_int)
            if not url:
                print(f"  -> No metadata URL found for {year}. Skipping.")
                continue
            if not _download_with_resume(url, zip_path, max_retries=6):
                print(f"  -> Re-download failed for {year}. Skipping.")
                continue
            if not zipfile.is_zipfile(zip_path):
                print(f"  -> File still invalid after re-download. Skipping.")
                continue

        try:
            monthly_files = []
            quarterly_files = []
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    # Skip directories
                    if member.endswith('/'):
                        continue
                    name = Path(member).name
                    name_lower = name.lower()
                    # Only CSV files (case-insensitive)
                    if not name_lower.endswith('.csv'):
                        continue
                    # Flatten extraction
                    target_path = year_dir / name
                    with zip_ref.open(member) as source, target_path.open('wb') as target:
                        shutil.copyfileobj(source, target)
                    # Track monthly vs quarterly
                    if any(q in name_lower for q in ('q1', 'q2', 'q3', 'q4')):
                        quarterly_files.append(target_path)
                    else:
                        monthly_files.append(target_path)
            # If no monthly but we have quarterly, split into monthly files
            if not monthly_files and quarterly_files:
                print(f"  -> No monthly CSVs found; splitting {len(quarterly_files)} quarterly CSVs into months")
                written = _split_quarterly_to_monthly(year_dir, quarterly_files, year_int)
                print(f"  -> Created {written} monthly CSV files from quarterly data")
                csv_count = len(list(year_dir.glob('*.csv')))
            else:
                csv_count = len(list(year_dir.glob('*.csv')))
            print(f"  -> Extracted {csv_count} monthly CSV files")
        except Exception as e:
            print(f"  -> Error extracting {zip_path.name}: {e}")

if __name__ == "__main__":
    unzip_ridership_files()