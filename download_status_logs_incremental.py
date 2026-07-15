"""
Incremental Greenbyte status logs downloader.

What this version does:
1. Reads assets from assets.json.
2. Checks Azure Blob Storage before downloading each monthly parquet.
3. Skips historical months that already exist in Blob Storage.
4. Refreshes the current month, because current-month status logs can still change.
5. Saves a local parquet copy and uploads it to Azure Blob Storage.

Expected local files:
- API_key.env
- assets.json

Expected API_key.env values:
GREENBYTE_API_KEY=...
AZURE_STORAGE_CONNECTION_STRING=...

Optional API_key.env values:
STATUSLOGS_CONTAINER_NAME=statuslogs
START_YEAR=2026
START_MONTH=1
PROCESS_ALL_ASSETS=true
OVERWRITE_CURRENT_MONTH=true
PAGE_SIZE=50
"""

import json
import os
from datetime import date
from pathlib import Path

import pandas as pd
import requests
import truststore
import urllib3
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


# --------------------------------------------------
# TEMPORARY SSL FIX FOR COMPANY NETWORK
# --------------------------------------------------
truststore.inject_into_ssl()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# --------------------------------------------------
# LOCAL / CLOUD SETTINGS
# --------------------------------------------------
BASE_DIR = Path(__file__).parent
ENV_PATH = BASE_DIR / "API_key.env"
ASSETS_PATH = BASE_DIR / "assets.json"

load_dotenv(ENV_PATH)

API_KEY = os.getenv("GREENBYTE_API_KEY")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

if not API_KEY:
    raise ValueError(f"GREENBYTE_API_KEY not found. Checked: {ENV_PATH}")

if not AZURE_STORAGE_CONNECTION_STRING:
    raise ValueError(f"AZURE_STORAGE_CONNECTION_STRING not found. Checked: {ENV_PATH}")

blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

# Azure Blob container used by your previous script
CONTAINER_NAME = os.getenv("STATUSLOGS_CONTAINER_NAME", "statuslogs")

# Local output folder
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "greenbyte_backfill")

# Status endpoint settings
URL = "https://cubico.greenbyte.cloud/api/2/status"
HEADERS = {
    "X-Api-Key": API_KEY,
    "Accept": "application/json",
}

START_YEAR = int(os.getenv("START_YEAR", "2026"))
START_MONTH = int(os.getenv("START_MONTH", "1"))
INCLUDE_CURRENT_MONTH = os.getenv("INCLUDE_CURRENT_MONTH", "true").lower() == "true"
OVERWRITE_CURRENT_MONTH = os.getenv("OVERWRITE_CURRENT_MONTH", "true").lower() == "true"
PROCESS_ALL_ASSETS = os.getenv("PROCESS_ALL_ASSETS", "true").lower() == "true"
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "50"))

# Fallback if you want to run only one asset instead of assets.json
DEFAULT_ASSET_NAME = os.getenv("ASSET_NAME", "Avloi")
DEFAULT_DEVICE_IDS = os.getenv("DEVICE_IDS", "24710,24711,24712,24713")
DEFAULT_LOST_PRODUCTION_SIGNAL_ID = os.getenv("LOST_PRODUCTION_SIGNAL_ID", "6951")


# --------------------------------------------------
# ASSET HELPERS
# --------------------------------------------------
def load_assets():
    """Load assets from assets.json, or fall back to a single asset."""
    if PROCESS_ALL_ASSETS and ASSETS_PATH.exists():
        with open(ASSETS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    return [
        {
            "AssetName": DEFAULT_ASSET_NAME,
            "WindFarm": DEFAULT_ASSET_NAME,
            "SubPark": DEFAULT_ASSET_NAME,
            "DeviceIds": DEFAULT_DEVICE_IDS,
            "LostProductionSignalId": DEFAULT_LOST_PRODUCTION_SIGNAL_ID,
        }
    ]


def asset_folder_name(asset):
    """Return a stable folder-safe asset name."""
    name = asset.get("AssetName") or asset.get("SubPark") or asset.get("WindFarm")
    return str(name).strip().replace(" ", "_")


def asset_print_name(asset):
    """Return a nice name for logs and table output."""
    return asset.get("SubPark") or asset.get("WindFarm") or asset.get("AssetName")


# --------------------------------------------------
# DATE HELPERS
# --------------------------------------------------
def first_day_of_next_month(year, month):
    if month == 12:
        return date(year + 1, 1, 1)
    return date(year, month + 1, 1)


def generate_month_ranges(start_year, start_month, include_current_month=True):
    """
    Generate monthly date windows from START_YEAR/START_MONTH up to today.

    Historical months use the first day of the next month as timestampEnd.
    Current month uses today as timestampEnd.
    """
    ranges = []
    today = date.today()

    year = start_year
    month = start_month

    while True:
        month_start = date(year, month, 1)
        next_month_start = first_day_of_next_month(year, month)

        if include_current_month:
            if month_start > today:
                break
            month_end = today if (year == today.year and month == today.month) else next_month_start
        else:
            current_month_start = date(today.year, today.month, 1)
            if next_month_start > current_month_start:
                break
            month_end = next_month_start

        ranges.append((month_start, month_end))
        year = next_month_start.year
        month = next_month_start.month

    return ranges


def is_current_month(month_start):
    today = date.today()
    return month_start.year == today.year and month_start.month == today.month


# --------------------------------------------------
# BLOB HELPERS
# --------------------------------------------------
def get_file_name(asset, month_start):
    asset_name = asset_folder_name(asset)
    return f"{asset_name}_status_logs_{month_start.year}_{month_start.month:02d}.parquet"


def get_blob_name(asset, month_start):
    asset_name = asset_folder_name(asset)
    file_name = get_file_name(asset, month_start)
    return (
        f"asset={asset_name}/"
        f"year={month_start.year}/"
        f"month={month_start.month:02d}/"
        f"{file_name}"
    )


def blob_exists(blob_name):
    blob_client = blob_service.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    return blob_client.exists()


def should_download_month(month_start, blob_name):
    if is_current_month(month_start) and OVERWRITE_CURRENT_MONTH:
        print(f"Current month will be refreshed: {month_start:%Y-%m}")
        return True

    if blob_exists(blob_name):
        print(f"Already exists in Blob Storage. Skipping: {blob_name}")
        return False

    print(f"Missing month. Will download: {month_start:%Y-%m}")
    return True


def upload_file_to_blob(local_file_path, blob_name):
    blob_client = blob_service.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded to Azure Blob Storage: {CONTAINER_NAME}/{blob_name}")


# --------------------------------------------------
# GREENBYTE DOWNLOAD
# --------------------------------------------------
def download_status_page(asset, start_date, end_date, page):
    device_ids = asset.get("DeviceIds")
    lost_production_signal_id = asset.get("LostProductionSignalId") or DEFAULT_LOST_PRODUCTION_SIGNAL_ID

    params = {
        "deviceIds": device_ids,
        "timestampStart": f"{start_date.isoformat()}T00:00:00Z",
        "timestampEnd": f"{end_date.isoformat()}T00:00:00Z",
        "category": "stop,curtailment",
        "categoryGlobalContract": "stop,curtailment",
        "lostProductionSignalId": lost_production_signal_id,
        "fields": "deviceId,message,lostProduction,timestampStart,timestampEnd,category,categoryGlobalContract,code",
        "sortAsc": "false",
        "pageSize": str(PAGE_SIZE),
        "page": str(page),
        "useUtc": "false",
        "contractType": "global",
    }

    response = requests.get(
        URL,
        params=params,
        headers=HEADERS,
        timeout=600,
        verify=False,
    )

    response.raise_for_status()
    return response.json()


def download_status_month(asset, start_date, end_date):
    all_rows = []
    page = 1

    print()
    print("Downloading status logs")
    print("Asset:", asset_print_name(asset))
    print("Device IDs:", asset.get("DeviceIds"))
    print("Start:", start_date)
    print("End:  ", end_date)

    while True:
        print("Page:", page)
        data = download_status_page(asset, start_date, end_date, page)

        if not data:
            print("No more rows.")
            break

        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            if "data" in data and isinstance(data["data"], list):
                rows = data["data"]
            elif "items" in data and isinstance(data["items"], list):
                rows = data["items"]
            elif "results" in data and isinstance(data["results"], list):
                rows = data["results"]
            else:
                rows = [data]
        else:
            rows = []

        if len(rows) == 0:
            print("No rows on this page.")
            break

        all_rows.extend(rows)

        if len(rows) < PAGE_SIZE:
            print("Last page reached.")
            break

        page += 1

    return all_rows


# --------------------------------------------------
# CONVERT STATUS LOGS TO TABLE
# --------------------------------------------------
def status_json_to_dataframe(rows, asset):
    clean_rows = []
    asset_name = asset_folder_name(asset)
    wind_farm = asset.get("WindFarm")
    sub_park = asset.get("SubPark")

    for row in rows:
        clean_rows.append(
            {
                "Asset": asset_name,
                "WindFarm": wind_farm,
                "SubPark": sub_park,
                "DeviceID": row.get("deviceId"),
                "Code": row.get("code"),
                "Message": row.get("message"),
                "LostProduction": row.get("lostProduction"),
                "TimestampStart": row.get("timestampStart"),
                "TimestampEnd": row.get("timestampEnd"),
                "Category": row.get("category"),
                "CategoryGlobalContract": row.get("categoryGlobalContract"),
            }
        )

    df = pd.DataFrame(clean_rows)

    if not df.empty:
        df["TimestampStart"] = pd.to_datetime(df["TimestampStart"], errors="coerce")
        df["TimestampEnd"] = pd.to_datetime(df["TimestampEnd"], errors="coerce")
        df["StartDate"] = df["TimestampStart"].dt.date
        df["StartYear"] = df["TimestampStart"].dt.year
        df["StartMonth"] = df["TimestampStart"].dt.month
        df["StartDay"] = df["TimestampStart"].dt.day

    return df


# --------------------------------------------------
# SAVE MONTHLY FILE
# --------------------------------------------------
def save_month_file(df, asset, month_start):
    asset_name = asset_folder_name(asset)
    year = month_start.year
    month = month_start.month

    folder = (
        Path(OUTPUT_FOLDER)
        / "status_logs"
        / f"asset={asset_name}"
        / f"year={year}"
        / f"month={month:02d}"
    )
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / get_file_name(asset, month_start)
    df.to_parquet(file_path, index=False)

    print("Saved local parquet:", file_path)
    print("Rows:", len(df))

    return file_path


# --------------------------------------------------
# MAIN SCRIPT
# --------------------------------------------------
def main():
    print("Starting INCREMENTAL Greenbyte status logs download...")
    print("Container:", CONTAINER_NAME)
    print("Start:", f"{START_YEAR}-{START_MONTH:02d}")
    print("Overwrite current month:", OVERWRITE_CURRENT_MONTH)
    print("Page size:", PAGE_SIZE)

    assets = load_assets()
    month_ranges = generate_month_ranges(START_YEAR, START_MONTH, INCLUDE_CURRENT_MONTH)

    print("Assets to process:", len(assets))
    print("Months considered per asset:", len(month_ranges))

    for asset in assets:
        print()
        print("=" * 80)
        print("Processing asset:", asset_print_name(asset))
        print("Asset folder:", asset_folder_name(asset))
        print("=" * 80)

        for start_date, end_date in month_ranges:
            blob_name = get_blob_name(asset, start_date)

            try:
                if not should_download_month(start_date, blob_name):
                    continue

                rows = download_status_month(asset, start_date, end_date)
                df = status_json_to_dataframe(rows, asset)

                if df.empty:
                    print(f"No status log rows for {asset_print_name(asset)} {start_date:%Y-%m}. Skipping upload.")
                    continue

                local_file_path = save_month_file(df, asset, start_date)
                upload_file_to_blob(local_file_path, blob_name)

            except Exception as e:
                print()
                print("ERROR")
                print("Asset:", asset_print_name(asset))
                print("Month:", start_date.strftime("%Y-%m"))
                print("Details:", e)
                print("Continuing with next month...")

    print()
    print("DONE. Incremental Greenbyte status log files created and uploaded.")


if __name__ == "__main__":
    main()
