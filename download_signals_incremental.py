"""
Incremental Greenbyte downloader - hourly signals + monthly site KPI table.

What this version does:
1. Reads all assets from assets.json by default.
2. Keeps the existing hourly signal pull unchanged.
3. Checks Azure Blob Storage before downloading each hourly monthly parquet.
4. Skips historical hourly months that already exist in Blob Storage.
5. Refreshes the current month because current-month data can still change.
6. Pulls Greenbyte hourly signal data at device level.
7. Converts hourly long-format data into a Power BI-ready wide table.
8. Saves hourly monthly parquet locally and uploads to the signals container.
9. Adds a separate monthly KPI pull for Greenbyte site-level KPI values.
10. Saves monthly KPI parquet locally and uploads to the monthly-kpis container.

Expected local files:
- API_key.env
- assets.json

Expected API_key.env values:
GREENBYTE_API_KEY=...
AZURE_STORAGE_CONNECTION_STRING=...

Optional API_key.env values:
SIGNALS_CONTAINER_NAME=signals
MONTHLY_KPIS_CONTAINER_NAME=monthly-kpis
START_YEAR=2026
START_MONTH=1
DATA_SIGNAL_IDS=1,4,5,60,281,3192,430,445,446,5384,6951,6957
MONTHLY_KPI_SIGNAL_IDS=6957
PROCESS_ALL_ASSETS=true
OVERWRITE_CURRENT_MONTH=true
OUTPUT_FOLDER=greenbyte_backfill
DATA_RESOLUTION=hourly
RUN_HOURLY_SIGNALS=true
RUN_MONTHLY_KPIS=true
MAX_RETRIES=3
RETRY_SLEEP_SECONDS=10
"""

import json
import os
import re
import time
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

CONTAINER_NAME = os.getenv("SIGNALS_CONTAINER_NAME", "signals")
MONTHLY_KPIS_CONTAINER_NAME = os.getenv("MONTHLY_KPIS_CONTAINER_NAME", "monthly-kpis")
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "greenbyte_backfill")

URL = "https://cubico.greenbyte.cloud/api/2/data"

HEADERS = {
    "X-Api-Key": API_KEY,
    "Accept": "application/json",
}

DATA_SIGNAL_IDS = os.getenv(
    "DATA_SIGNAL_IDS",
    "1,4,5,60,281,3192,430,445,446,5384,6951,6957",
)

# Proven against Greenbyte UI for Production-based Contractual Avail. (Global).
# 6957 + monthly + site + sum returned 0.7546502642510733 = 75.47% for Zarakes June 2026.
MONTHLY_KPI_SIGNAL_IDS = os.getenv(
    "MONTHLY_KPI_SIGNAL_IDS",
    "1,4,248,281,431,6957"
)

START_YEAR = int(os.getenv("START_YEAR", "2026"))
START_MONTH = int(os.getenv("START_MONTH", "1"))

INCLUDE_CURRENT_MONTH = os.getenv("INCLUDE_CURRENT_MONTH", "true").lower() == "true"
OVERWRITE_CURRENT_MONTH = os.getenv("OVERWRITE_CURRENT_MONTH", "true").lower() == "true"
PROCESS_ALL_ASSETS = os.getenv("PROCESS_ALL_ASSETS", "true").lower() == "true"

RUN_HOURLY_SIGNALS = os.getenv("RUN_HOURLY_SIGNALS", "true").lower() == "true"
RUN_MONTHLY_KPIS = os.getenv("RUN_MONTHLY_KPIS", "true").lower() == "true"

# Greenbyte accepted value is "hourly", not "hour".
DATA_RESOLUTION = os.getenv("DATA_RESOLUTION", "hourly")

# Fallback single-asset test settings, only used if PROCESS_ALL_ASSETS=false.
DEFAULT_ASSET_NAME = os.getenv("ASSET_NAME", "avloi")
DEFAULT_WIND_FARM = os.getenv("WIND_FARM", "Avloi")
DEFAULT_SUB_PARK = os.getenv("SUB_PARK", "Avloi")
DEFAULT_DEVICE_IDS = os.getenv("DEVICE_IDS", "24844,24845,24846,24847")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_SLEEP_SECONDS = int(os.getenv("RETRY_SLEEP_SECONDS", "10"))


# --------------------------------------------------
# ASSET HELPERS
# --------------------------------------------------
def load_assets():
    """
    Load assets from assets.json if PROCESS_ALL_ASSETS=true.
    Otherwise use one fallback asset for testing.
    """
    if PROCESS_ALL_ASSETS:
        if not ASSETS_PATH.exists():
            raise FileNotFoundError(f"assets.json not found. Checked: {ASSETS_PATH}")

        with open(ASSETS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    return [
        {
            "AssetName": DEFAULT_ASSET_NAME,
            "WindFarm": DEFAULT_WIND_FARM,
            "SubPark": DEFAULT_SUB_PARK,
            "DeviceIds": DEFAULT_DEVICE_IDS,
            "LostProductionSignalId": "6951",
        }
    ]


def asset_folder_name(asset):
    """
    Return a stable lowercase folder-safe asset name.

    Example:
    Avloi -> avloi
    Rachi Gioni -> rachi_gioni
    """
    name = asset.get("AssetName") or asset.get("SubPark") or asset.get("WindFarm")
    return str(name).strip().lower().replace(" ", "_")


def asset_print_name(asset):
    """Return a nice display name for logs."""
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
    Generate monthly date windows from START_YEAR / START_MONTH up to today.
    Historical months: timestampEnd = first day of next month.
    Current month: timestampEnd = today.
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

            if year == today.year and month == today.month:
                month_end = today
            else:
                month_end = next_month_start

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
# BLOB HELPERS - HOURLY SIGNALS
# --------------------------------------------------
def get_file_name(asset, month_start):
    asset_name = asset_folder_name(asset)

    return (
        f"{asset_name}_signals_"
        f"{month_start.year}_"
        f"{month_start.month:02d}_"
        f"{DATA_RESOLUTION}_powerbi.parquet"
    )


def get_blob_name(asset, month_start):
    asset_name = asset_folder_name(asset)
    file_name = get_file_name(asset, month_start)

    return (
        f"asset={asset_name}/"
        f"year={month_start.year}/"
        f"month={month_start.month:02d}/"
        f"{file_name}"
    )


# --------------------------------------------------
# BLOB HELPERS - MONTHLY KPIS
# --------------------------------------------------
def get_monthly_kpi_file_name(asset, month_start):
    asset_name = asset_folder_name(asset)

    return (
        f"{asset_name}_monthly_kpis_"
        f"{month_start.year}_"
        f"{month_start.month:02d}_"
        f"powerbi.parquet"
    )


def get_monthly_kpi_blob_name(asset, month_start):
    asset_name = asset_folder_name(asset)
    file_name = get_monthly_kpi_file_name(asset, month_start)

    return (
        f"asset={asset_name}/"
        f"year={month_start.year}/"
        f"month={month_start.month:02d}/"
        f"{file_name}"
    )


def blob_exists(container_name, blob_name):
    blob_client = blob_service.get_blob_client(
        container=container_name,
        blob=blob_name,
    )
    return blob_client.exists()


def should_download_month(month_start, container_name, blob_name, label):
    if is_current_month(month_start) and OVERWRITE_CURRENT_MONTH:
        print(f"Current month will be refreshed for {label}: {month_start:%Y-%m}")
        return True

    if blob_exists(container_name, blob_name):
        print(f"Already exists in Blob Storage. Skipping {label}: {container_name}/{blob_name}")
        return False

    print(f"Missing {label}. Will download: {month_start:%Y-%m}")
    return True


def upload_file_to_blob(local_file_path, container_name, blob_name):
    blob_client = blob_service.get_blob_client(
        container=container_name,
        blob=blob_name,
    )

    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    print(f"Uploaded to Azure Blob Storage: {container_name}/{blob_name}")


# --------------------------------------------------
# GREENBYTE DOWNLOAD - HOURLY SIGNALS
# --------------------------------------------------
def download_signals_month(asset, start_date, end_date):
    device_ids = asset.get("DeviceIds")

    params = {
        "deviceIds": device_ids,
        "dataSignalIds": DATA_SIGNAL_IDS,
        "timestampStart": f"{start_date.isoformat()}T00:00:00Z",
        "timestampEnd": f"{end_date.isoformat()}T00:00:00Z",
        "useUtc": "false",
        "resolution": DATA_RESOLUTION,
        "aggregate": "device",
        "aggregateLevel": "0",
        "calculation": "sum",
    }

    print()
    print("Downloading hourly signals")
    print("Asset:", asset_print_name(asset))
    print("Device IDs:", device_ids)
    print("Start:", params["timestampStart"])
    print("End:  ", params["timestampEnd"])
    print("Resolution:", DATA_RESOLUTION)
    print("Aggregate: device")

    return call_greenbyte(params)


# --------------------------------------------------
# GREENBYTE DOWNLOAD - MONTHLY SITE KPIS
# --------------------------------------------------
def download_monthly_kpis(asset, start_date, end_date):
    device_ids = asset.get("DeviceIds")

    params = {
        "deviceIds": device_ids,
        "dataSignalIds": MONTHLY_KPI_SIGNAL_IDS,
        "timestampStart": f"{start_date.isoformat()}T00:00:00Z",
        "timestampEnd": f"{end_date.isoformat()}T00:00:00Z",
        "resolution": "monthly",
        "aggregate": "site",
        "aggregateLevel": "0",
        "calculation": "sum",
    }

    print()
    print("Downloading monthly site KPIs")
    print("Asset:", asset_print_name(asset))
    print("Device IDs:", device_ids)
    print("Signal IDs:", MONTHLY_KPI_SIGNAL_IDS)
    print("Start:", params["timestampStart"])
    print("End:  ", params["timestampEnd"])
    print("Resolution: monthly")
    print("Aggregate: site")
    print("Calculation: sum")

    return call_greenbyte(params)


def call_greenbyte(params):
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                URL,
                params=params,
                headers=HEADERS,
                timeout=600,
                verify=False,
            )

            print("Status Code:", response.status_code)

            if response.status_code >= 500:
                raise requests.HTTPError(
                    f"{response.status_code} server error from Greenbyte"
                )

            response.raise_for_status()
            return response.json()

        except Exception as e:
            last_error = e
            print(f"Attempt {attempt}/{MAX_RETRIES} failed:")
            print(e)

            if attempt < MAX_RETRIES:
                print(f"Retrying after {RETRY_SLEEP_SECONDS} seconds...")
                time.sleep(RETRY_SLEEP_SECONDS)

    raise last_error


# --------------------------------------------------
# CONVERT GREENBYTE JSON TO LONG TABLE
# --------------------------------------------------
def signals_json_to_dataframe(data, asset):
    rows = []

    asset_name = asset_folder_name(asset)
    wind_farm = asset.get("WindFarm")
    sub_park = asset.get("SubPark")

    if not isinstance(data, list):
        print("Unexpected signals JSON structure. Saving raw normalized output.")

        df = pd.json_normalize(data)
        df["Asset"] = asset_name
        df["WindFarm"] = wind_farm
        df["SubPark"] = sub_park

        return df

    for block in data:
        if not isinstance(block, dict):
            continue

        device_id = block.get("aggregateId")
        aggregate = block.get("aggregate")
        resolution = block.get("resolution")
        calculation = block.get("calculation")
        aggregate_path_names = block.get("aggregatePathNames")

        data_signal = block.get("dataSignal", {}) or {}

        signal_id = data_signal.get("dataSignalId")
        signal_name = data_signal.get("title")
        signal_unit = data_signal.get("unit")

        values = block.get("data", {}) or {}

        if isinstance(values, dict):
            for timestamp, value in values.items():
                rows.append(
                    {
                        "Asset": asset_name,
                        "WindFarm": wind_farm,
                        "SubPark": sub_park,
                        "DeviceID": device_id,
                        "Timestamp": timestamp,
                        "DataSignalID": signal_id,
                        "Signal": signal_name,
                        "Unit": signal_unit,
                        "Value": value,
                        "Aggregate": aggregate,
                        "AggregatePathNames": aggregate_path_names,
                        "Resolution": resolution,
                        "Calculation": calculation,
                    }
                )

    df = pd.DataFrame(rows)

    if not df.empty:
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df["Date"] = df["Timestamp"].dt.date
        df["Year"] = df["Timestamp"].dt.year
        df["Month"] = df["Timestamp"].dt.month
        df["Day"] = df["Timestamp"].dt.day
        df["Hour"] = df["Timestamp"].dt.hour

    return df


# --------------------------------------------------
# CLEANING / POWER BI READY PIVOT
# --------------------------------------------------
def clean_signal_name(name):
    """
    Convert Greenbyte signal names into Power BI-friendly column names.

    Examples:
    Energy Export -> Energy_Export
    Production-based Contractual Avail. (Global) -> Production_Based_Contractual_Avail_Global
    """
    name = str(name)
    name = re.sub(r"[()]", "", name)
    name = re.sub(r"[^A-Za-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    parts = name.split("_")
    parts = [p[:1].upper() + p[1:] for p in parts if p]
    return "_".join(parts)


def pivot_signals_for_powerbi(df):
    """
    Convert hourly long signal data into Power BI-ready wide format.
    One row = one turbine + one timestamp.
    """
    if df.empty:
        return df

    required_columns = [
        "Asset",
        "WindFarm",
        "SubPark",
        "DeviceID",
        "Timestamp",
        "Date",
        "Year",
        "Month",
        "Day",
        "Hour",
        "Signal",
        "Value",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Cannot pivot signals. Missing columns: {missing_columns}")

    df = df.copy()
    df["Signal_Column"] = df["Signal"].apply(clean_signal_name)

    index_columns = [
        "Asset",
        "WindFarm",
        "SubPark",
        "DeviceID",
        "Timestamp",
        "Date",
        "Year",
        "Month",
        "Day",
        "Hour",
    ]

    pivot_df = df.pivot_table(
        index=index_columns,
        columns="Signal_Column",
        values="Value",
        aggfunc="first",
    ).reset_index()

    pivot_df.columns.name = None

    pivot_df = pivot_df.sort_values(
        by=["Asset", "DeviceID", "Timestamp"]
    ).reset_index(drop=True)

    signal_columns = [col for col in pivot_df.columns if col not in index_columns]

    for col in signal_columns:
        pivot_df[col] = pd.to_numeric(pivot_df[col], errors="coerce")

    print()
    print("Power BI hourly pivot completed.")
    print(f"Rows before pivot: {len(df):,}")
    print(f"Rows after pivot : {len(pivot_df):,}")
    print(f"Signal columns   : {len(signal_columns)}")

    print()
    print("Power BI signal columns:")
    for col in signal_columns:
        print(f" - {col}")

    nulls = (
        pivot_df[signal_columns]
        .isna()
        .sum()
        .sort_values(ascending=False)
    )

    print()
    print("Top null counts after pivot:")
    print(nulls.head(20).to_string())

    return pivot_df


def pivot_monthly_kpis_for_powerbi(df, month_start):
    """
    Convert monthly site KPI long data into Power BI-ready wide format.
    One row = one asset + one month.
    """
    if df.empty:
        return df

    required_columns = [
        "Asset",
        "WindFarm",
        "SubPark",
        "DeviceID",
        "Timestamp",
        "Signal",
        "Value",
        "DataSignalID",
        "Unit",
        "Aggregate",
        "Resolution",
        "Calculation",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Cannot pivot monthly KPIs. Missing columns: {missing_columns}")

    df = df.copy()
    df["Signal_Column"] = df["Signal"].apply(clean_signal_name)
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")

    df["KPI_Year"] = month_start.year
    df["KPI_Month"] = month_start.month
    df["KPI_MonthStart"] = pd.Timestamp(month_start)

    index_columns = [
        "Asset",
        "WindFarm",
        "SubPark",
        "KPI_Year",
        "KPI_Month",
        "KPI_MonthStart",
    ]

    pivot_df = df.pivot_table(
        index=index_columns,
        columns="Signal_Column",
        values="Value",
        aggfunc="first",
    ).reset_index()

    pivot_df.columns.name = None

    signal_columns = [col for col in pivot_df.columns if col not in index_columns]

    for col in signal_columns:
        pivot_df[col] = pd.to_numeric(pivot_df[col], errors="coerce")

    print()
    print("Power BI monthly KPI pivot completed.")
    print(f"Rows before pivot: {len(df):,}")
    print(f"Rows after pivot : {len(pivot_df):,}")
    print(f"KPI columns      : {len(signal_columns)}")

    print()
    print("Power BI monthly KPI columns:")
    for col in signal_columns:
        print(f" - {col}")

    return pivot_df


# --------------------------------------------------
# SAVE MONTHLY FILES
# --------------------------------------------------
def save_month_file(df, asset, month_start):
    asset_name = asset_folder_name(asset)
    year = month_start.year
    month = month_start.month

    folder = (
        Path(OUTPUT_FOLDER)
        / "signals"
        / f"asset={asset_name}"
        / f"year={year}"
        / f"month={month:02d}"
    )

    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / get_file_name(asset, month_start)
    df.to_parquet(file_path, index=False)

    print("Saved local Power BI-ready hourly parquet:", file_path)
    print("Rows:", len(df))
    print("Columns:", len(df.columns))

    return file_path


def save_monthly_kpi_file(df, asset, month_start):
    asset_name = asset_folder_name(asset)
    year = month_start.year
    month = month_start.month

    folder = (
        Path(OUTPUT_FOLDER)
        / "monthly_kpis"
        / f"asset={asset_name}"
        / f"year={year}"
        / f"month={month:02d}"
    )

    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / get_monthly_kpi_file_name(asset, month_start)
    df.to_parquet(file_path, index=False)

    print("Saved local Power BI-ready monthly KPI parquet:", file_path)
    print("Rows:", len(df))
    print("Columns:", len(df.columns))

    return file_path


# --------------------------------------------------
# MONTH PROCESSORS
# --------------------------------------------------
def process_hourly_signals(asset, start_date, end_date):
    blob_name = get_blob_name(asset, start_date)

    if not should_download_month(start_date, CONTAINER_NAME, blob_name, "hourly signals"):
        return

    data = download_signals_month(asset, start_date, end_date)
    long_df = signals_json_to_dataframe(data, asset)

    if long_df.empty:
        print(
            f"No hourly signal rows for {asset_print_name(asset)} "
            f"{start_date:%Y-%m}. Skipping upload."
        )
        return

    powerbi_df = pivot_signals_for_powerbi(long_df)

    if powerbi_df.empty:
        print(
            f"No Power BI-ready hourly rows for {asset_print_name(asset)} "
            f"{start_date:%Y-%m}. Skipping upload."
        )
        return

    local_file_path = save_month_file(powerbi_df, asset, start_date)
    upload_file_to_blob(local_file_path, CONTAINER_NAME, blob_name)


def process_monthly_kpis(asset, start_date, end_date):
    blob_name = get_monthly_kpi_blob_name(asset, start_date)

    if not should_download_month(start_date, MONTHLY_KPIS_CONTAINER_NAME, blob_name, "monthly KPIs"):
        return

    data = download_monthly_kpis(asset, start_date, end_date)
    long_df = signals_json_to_dataframe(data, asset)

    if long_df.empty:
        print(
            f"No monthly KPI rows for {asset_print_name(asset)} "
            f"{start_date:%Y-%m}. Skipping upload."
        )
        return

    powerbi_df = pivot_monthly_kpis_for_powerbi(long_df, start_date)

    if powerbi_df.empty:
        print(
            f"No Power BI-ready monthly KPI rows for {asset_print_name(asset)} "
            f"{start_date:%Y-%m}. Skipping upload."
        )
        return

    local_file_path = save_monthly_kpi_file(powerbi_df, asset, start_date)
    upload_file_to_blob(local_file_path, MONTHLY_KPIS_CONTAINER_NAME, blob_name)


# --------------------------------------------------
# MAIN SCRIPT
# --------------------------------------------------
def main():
    print("Starting INCREMENTAL Greenbyte download...")
    print("Hourly signals container:", CONTAINER_NAME)
    print("Monthly KPIs container:", MONTHLY_KPIS_CONTAINER_NAME)
    print("Hourly signal IDs:", DATA_SIGNAL_IDS)
    print("Monthly KPI signal IDs:", MONTHLY_KPI_SIGNAL_IDS)
    print("Hourly resolution:", DATA_RESOLUTION)
    print("Monthly KPI resolution: monthly")
    print("Monthly KPI aggregate: site")
    print("Monthly KPI calculation: sum")
    print("Start:", f"{START_YEAR}-{START_MONTH:02d}")
    print("Overwrite current month:", OVERWRITE_CURRENT_MONTH)
    print("Process all assets:", PROCESS_ALL_ASSETS)
    print("Run hourly signals:", RUN_HOURLY_SIGNALS)
    print("Run monthly KPIs:", RUN_MONTHLY_KPIS)

    assets = load_assets()
    month_ranges = generate_month_ranges(
        START_YEAR,
        START_MONTH,
        INCLUDE_CURRENT_MONTH,
    )

    print("Assets to process:", len(assets))
    print("Months considered per asset:", len(month_ranges))

    for asset in assets:
        print()
        print("=" * 80)
        print("Processing asset:", asset_print_name(asset))
        print("Asset folder:", asset_folder_name(asset))
        print("=" * 80)

        for start_date, end_date in month_ranges:
            try:
                if RUN_HOURLY_SIGNALS:
                    process_hourly_signals(asset, start_date, end_date)

                if RUN_MONTHLY_KPIS:
                    process_monthly_kpis(asset, start_date, end_date)

            except Exception as e:
                print()
                print("ERROR")
                print("Asset:", asset_print_name(asset))
                print("Month:", start_date.strftime("%Y-%m"))
                print("Details:", e)
                print("Continuing with next month...")

    print()
    print("DONE. Incremental hourly signal files and monthly KPI files created/uploaded.")


if __name__ == "__main__":
    main()
