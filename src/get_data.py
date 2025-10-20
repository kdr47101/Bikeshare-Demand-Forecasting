import os
import json
from pathlib import Path
from urllib.parse import urlparse
import requests
from config import BASE_URL, PACKAGE_ID_RIDERSHIP, PACKAGE_ID_STATION, API

RAW_DIR = Path("data") / "raw"

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

def print_and_save_non_datastore_resources(package_json, package_id: str):
    """
    Keeps the original behavior (prints resource metadata), and ALSO:
      - writes package.json to data/raw/
      - writes each resource_show JSON to data/raw/resource_{idx}_{id}_metadata.json
      - if resource_show has a 'result.url', downloads that file into data/raw/
    """
    # Save package metadata
    _safe_write_json(package_json, RAW_DIR / "package.json")

    for idx, resource in enumerate(package_json["result"]["resources"]):
        if not resource.get("datastore_active", False):
            # Fetch resource_show metadata
            url = BASE_URL + API["resource_show"]
            params = {"id": resource["id"]}
            resource_metadata = requests.get(url, params=params).json()

            # Original behavior: print it
            print(resource_metadata)

            # Save resource_show metadata
            meta_path = RAW_DIR / f"resource_{idx}_{resource['id']}_metadata.json"
            _safe_write_json(resource_metadata, meta_path)

            # If there's a direct file URL, download it
            result = resource_metadata.get("result", {})
            file_url = result.get("url")
            if file_url:
                filename = _filename_from_url(file_url, f"{resource['id']}.bin")
                dest = RAW_DIR / filename
                try:
                    with requests.get(file_url, stream=True) as r:
                        r.raise_for_status()
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        with dest.open("wb") as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                    print(f"Saved file: {dest}")
                except Exception as e:
                    print(f"Could not download {file_url}: {e}")

if __name__ == "__main__":
    for PID in (PACKAGE_ID_RIDERSHIP, PACKAGE_ID_STATION):
        pkg = get_package_json(PID)
        print_and_save_non_datastore_resources(pkg, PID)
