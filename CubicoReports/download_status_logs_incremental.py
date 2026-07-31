"""
download_status_logs_incremental.py

Incremental Greenbyte status logs downloader.

Adjusted version:
1. Reads assets from assets.json.
2. Checks Azure Blob Storage before downloading each monthly parquet.
3. Skips historical months that already exist in Blob Storage.
4. Always refreshes the current month, because current-month status logs can still change.
5. Also refreshes the previous month during the reporting validation window,
   so late Greenbyte status categorisation changes by site managers are picked up.
6. Converts Greenbyte timestamps from UTC/company time to Greek site time.
7. Saves a local parquet copy and uploads it to Azure Blob Storage.

Why previous-month refresh exists:
- Site managers may update status categorisation/comments after month end.
- Example: July event categorised on August 2.
- If July parquet is skipped after August 1, the report will keep the old category.
- This script refreshes previous month until PREVIOUS_MONTH_REFRESH_UNTIL_DAY.

Timezone behaviour:
- The script requests Greenbyte timestamps with useUtc=true.
- Site-local month boundaries are converted to UTC for the API call.
- API timestamps are converted to TARGET_TIMEZONE.
- Saved parquet timestamps are timezone-naive site-local timestamps.

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
INCLUDE_CURRENT_MONTH=true
OVERWRITE_CURRENT_MONTH=true
REFRESH_PREVIOUS_MONTH=true
PREVIOUS_MONTH_REFRESH_UNTIL_DAY=7
TARGET_TIMEZONE=Europe/Athens
PAGE_SIZE=50
OUTPUT_FOLDER=greenbyte_backfill
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

CONTAINER_NAME = os.getenv("STATUSLOGS_CONTAINER_NAME", "statuslogs")
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "greenbyte_backfill")

URL = "https://cubico.greenbyte.cloud/api/2/status"
HEADERS = {
    "X-Api-Key": API_KEY,
    "Accept": "application/json",
}

START_YEAR = int(os.getenv("START_YEAR", "2026"))
START_MONTH = int(os.getenv("START_MONTH", "1"))
INCLUDE_CURRENT_MONTH = os.getenv("INCLUDE_CURRENT_MONTH", "true").lower() == "true"
OVERWRITE_CURRENT_MONTH = os.getenv("OVERWRITE_CURRENT_MONTH", "true").lower() == "true"
REFRESH_PREVIOUS_MONTH = os.getenv("REFRESH_PREVIOUS_MONTH", "true").lower() == "true"
PREVIOUS_MONTH_REFRESH_UNTIL_DAY = int(os.getenv("PREVIOUS_MONTH_REFRESH_UNTIL_DAY", "7"))
TARGET_TIMEZONE = os.getenv("TARGET_TIMEZONE", "Europe/Athens")
PROCESS_ALL_ASSETS = os.getenv("PROCESS_ALL_ASSETS", "true").lower() == "true"
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "50"))

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
    return str(name).strip().replace(" ", "_").lower()


def asset_print_name(asset):
    """Return a nice name for logs and table output."""
    return asset.get("SubPark") or asset.get("WindFarm") or asset.get("AssetName")


# --------------------------------------------------
# DATE / TIMEZONE HELPERS
# --------------------------------------------------
def site_today():
    return pd.Timestamp.now(tz=TARGET_TIMEZONE).date()


def first_day_of_next_month(year, month):
    if month == 12:
        return date(year + 1, 1, 1)
    return date(year, month + 1, 1)


def first_day_of_previous_month(today=None):
    today = today or site_today()
    if today.month == 1:
        return date(today.year - 1, 12, 1)
    return date(today.year, today.month - 1, 1)


def generate_month_ranges(start_year, start_month, include_current_month=True):
    """
    Generate monthly date windows from START_YEAR/START_MONTH up to site-local today.

    Historical months use the first day of the next month as timestampEnd.
    Current month uses site-local today as timestampEnd.
    """
    ranges = []
    today = site_today()

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
    today = site_today()
    return month_start.year == today.year and month_start.month == today.month


def is_previous_month(month_start):
    prev = first_day_of_previous_month()
    return month_start.year == prev.year and month_start.month == prev.month


def should_refresh_previous_month_today():
    today = site_today()
    return REFRESH_PREVIOUS_MONTH and today.day <= PREVIOUS_MONTH_REFRESH_UNTIL_DAY


def site_window_to_utc_strings(start_date, end_date):
    """
    Convert site-local month boundaries to UTC strings for Greenbyte API query.

    Example in Greek summer time:
    2026-07-01 00:00 Athens -> 2026-06-30T21:00:00Z
    2026-08-01 00:00 Athens -> 2026-07-31T21:00:00Z
    """
    start_local = pd.Timestamp(start_date).tz_localize(TARGET_TIMEZONE)
    end_local = pd.Timestamp(end_date).tz_localize(TARGET_TIMEZONE)

    start_utc = start_local.tz_convert("UTC")
    end_utc = end_local.tz_convert("UTC")

    return (
        start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def to_site_time(series):
    """Convert Greenbyte UTC timestamps to timezone-naive site-local timestamps."""
    return (
        pd.to_datetime(series, errors="coerce", utc=True)
        .dt.tz_convert(TARGET_TIMEZONE)
        .dt.tz_localize(None)
    )


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
    """
    Decide whether to download a monthly status-log parquet.

    Rules:
    - Current month is refreshed if OVERWRITE_CURRENT_MONTH=true.
    - Previous month is refreshed during the configured post-month validation window.
    - Older historical months are skipped if the blob already exists.
    """
    if is_current_month(month_start) and OVERWRITE_CURRENT_MONTH:
        print(f"Current month will be refreshed: {month_start:%Y-%m}")
        return True

    if is_previous_month(month_start) and should_refresh_previous_month_today():
        print(f"Previous month will be refreshed for late categorisation updates: {month_start:%Y-%m}")
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
    timestamp_start_utc, timestamp_end_utc = site_window_to_utc_strings(start_date, end_date)

    params = {
        "deviceIds": device_ids,
        "timestampStart": timestamp_start_utc,
        "timestampEnd": timestamp_end_utc,
        "category": "stop,curtailment",
        "categoryGlobalContract": "stop,curtailment",
        "lostProductionSignalId": lost_production_signal_id,
        "fields": "deviceId,message,lostProduction,timestampStart,timestampEnd,category,categoryGlobalContract,code",
        "sortAsc": "false",
        "pageSize": str(PAGE_SIZE),
        "page": str(page),
        "useUtc": "true",
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
    timestamp_start_utc, timestamp_end_utc = site_window_to_utc_strings(start_date, end_date)

    print()
    print("Downloading status logs")
    print("Asset:", asset_print_name(asset))
    print("Device IDs:", asset.get("DeviceIds"))
    print("Site Start:", start_date)
    print("Site End:  ", end_date)
    print("API Start UTC:", timestamp_start_utc)
    print("API End UTC:  ", timestamp_end_utc)
    print("Target timezone:", TARGET_TIMEZONE)

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
def status_json_to_dataframe(rows, asset, month_start=None, month_end=None):
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
        df["TimestampStart"] = to_site_time(df["TimestampStart"])
        df["TimestampEnd"] = to_site_time(df["TimestampEnd"])

        # Keep only rows that overlap the requested site-local month window.
        # This protects month boundaries after UTC -> site-time conversion.
        if month_start is not None and month_end is not None:
            site_start = pd.Timestamp(month_start)
            site_end = pd.Timestamp(month_end)
            overlap_end = df["TimestampEnd"].fillna(site_end)
            df = df[
                (df["TimestampStart"] < site_end)
                & (overlap_end > site_start)
            ].copy()

        df["StartDate"] = df["TimestampStart"].dt.date
        df["StartYear"] = df["TimestampStart"].dt.year
        df["StartMonth"] = df["TimestampStart"].dt.month
        df["StartDay"] = df["TimestampStart"].dt.day
        df["TargetTimezone"] = TARGET_TIMEZONE

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
    print("Refresh previous month:", REFRESH_PREVIOUS_MONTH)
    print("Previous month refresh until day:", PREVIOUS_MONTH_REFRESH_UNTIL_DAY)
    print("Target timezone:", TARGET_TIMEZONE)
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
                df = status_json_to_dataframe(rows, asset, start_date, end_date)

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
