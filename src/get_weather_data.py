import os
import json
from pathlib import Path
import csv
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time



RAW_DIR = Path("data") / "raw"
load_dotenv()

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

def _daterange_chunks(start_str: str, end_str: str, chunk_days: int = 30):
    """
    Yield (chunk_start, chunk_end) ISO date strings in inclusive windows of up to chunk_days.
    """
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_str, "%Y-%m-%d").date()
    cur = start
    while cur <= end:
        chunk_end = min(end, cur + timedelta(days=chunk_days - 1))
        yield cur.isoformat(), chunk_end.isoformat()
        cur = chunk_end + timedelta(days=1)

def _request_with_retries(url, headers, params, retries: int = 3, base_sleep: float = 1.0):
    """
    Basic retry for 429/5xx or connection issues.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as http_err:
            code = getattr(http_err.response, "status_code", None)
            if code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(base_sleep * attempt)
                continue
            raise
        except Exception:
            if attempt < retries:
                time.sleep(base_sleep * attempt)
                continue
            raise

def download_hourly_weather_data(
    station_id: str = "10637",
    start: str = "2017-01-01",
    end: str = "2024-12-01",
):
    """
    Download hourly Meteostat weather data and persist aggregated JSON and CSV snapshots.
    Handles API 30-day maximum window by chunking the date range.
    RAPIDAPI_METEOSTAT_KEY must be set in the environment.
    """
    api_key = os.getenv("RAPIDAPI_METEOSTAT_KEY")
    if not api_key:
        print("Missing RAPIDAPI_METEOSTAT_KEY environment variable.")
        return

    url = "https://meteostat.p.rapidapi.com/stations/hourly"
    headers = {
        "x-rapidapi-host": "meteostat.p.rapidapi.com",
        "x-rapidapi-key": api_key,
    }

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    json_path = RAW_DIR / f"weather_hourly_{station_id}.json"
    csv_path = RAW_DIR / f"weather_hourly_{station_id}.csv"

    all_rows = []
    chunk_count = 0
    try:
        for c_start, c_end in _daterange_chunks(start, end, chunk_days=30):
            params = {"station": station_id, "start": c_start, "end": c_end}
            try:
                payload = _request_with_retries(url, headers, params, retries=3, base_sleep=1.5)
            except Exception as exc:
                print(f"Failed chunk {c_start} to {c_end}: {exc}")
                continue

            rows = payload.get("data", []) or []
            if rows:
                all_rows.extend(rows)
            chunk_count += 1
            # Gentle pacing to be nice to the API
            time.sleep(0.2)

    except Exception as exc:
        print(f"Failed to download Meteostat data: {exc}")
        return

    aggregated = {
        "meta": {
            "station": station_id,
            "start": start,
            "end": end,
            "chunks": chunk_count,
            "source_url": url,
        },
        "data": all_rows,
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(aggregated, f, ensure_ascii=False, indent=2)

    if all_rows:
        _write_csv(csv_path, all_rows, _collect_fieldnames(all_rows))
        print(f"Saved weather CSV: {csv_path} ({len(all_rows)} rows across {chunk_count} chunks)")
    else:
        print(f"No Meteostat data rows returned across {chunk_count} chunks.")

if __name__ == "__main__":
    download_hourly_weather_data()
