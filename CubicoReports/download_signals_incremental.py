"""
Incremental Greenbyte downloader - Power BI-ready hourly signals + monthly site KPI table.

This version keeps the existing hourly signals logic and adds a separate monthly KPI pipeline.

What this version does:
1. Reads all assets from assets.json by default.
2. Checks Azure Blob Storage before downloading each hourly monthly parquet.
3. Skips historical hourly months that already exist in the signals container.
4. Refreshes current month because current-month data can still change.
5. Refreshes previous month during the reporting validation window.
6. Pulls Greenbyte signal data at hourly resolution.
7. Converts Greenbyte timestamps from UTC/company time to Greek site time.
8. Converts Greenbyte long-format data into a Power BI-ready wide table.
9. Saves hourly monthly parquet locally and uploads to the signals container.
10. Separately checks Azure Blob Storage before downloading each monthly KPI parquet.
11. Skips historical monthly KPI months that already exist in the monthly-kpis container.
12. Refreshes current/previous-month monthly KPIs using the same incremental strategy.
13. Pulls Greenbyte monthly site-level KPI values.
14. Converts monthly KPI long-format data into a Power BI-ready wide table.
15. Saves monthly KPI parquet locally and uploads to the monthly-kpis container.

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
DATA_SIGNAL_IDS=1,4,5,60,240,281,3192,430,445,446,5384,6792,6951,6957
MONTHLY_KPI_SIGNAL_IDS=1,4,248,281,431,6957
PROCESS_ALL_ASSETS=true
INCLUDE_CURRENT_MONTH=true
OVERWRITE_CURRENT_MONTH=true
REFRESH_PREVIOUS_MONTH=true
PREVIOUS_MONTH_REFRESH_UNTIL_DAY=7
TARGET_TIMEZONE=Europe/Athens
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
    "1,4,5,60,240,281,3192,430,445,446,5384,6792,6951,6957",
)

MONTHLY_KPI_SIGNAL_IDS = os.getenv(
    "MONTHLY_KPI_SIGNAL_IDS",
    "1,4,248,281,431,6957,6951",
)

START_YEAR = int(os.getenv("START_YEAR", "2026"))
START_MONTH = int(os.getenv("START_MONTH", "1"))

INCLUDE_CURRENT_MONTH = os.getenv("INCLUDE_CURRENT_MONTH", "true").lower() == "true"
OVERWRITE_CURRENT_MONTH = os.getenv("OVERWRITE_CURRENT_MONTH", "true").lower() == "true"

REFRESH_PREVIOUS_MONTH = os.getenv("REFRESH_PREVIOUS_MONTH", "true").lower() == "true"
PREVIOUS_MONTH_REFRESH_UNTIL_DAY = int(os.getenv("PREVIOUS_MONTH_REFRESH_UNTIL_DAY", "7"))
TARGET_TIMEZONE = os.getenv("TARGET_TIMEZONE", "Europe/Athens")

PROCESS_ALL_ASSETS = os.getenv("PROCESS_ALL_ASSETS", "true").lower() == "true"
RUN_HOURLY_SIGNALS = os.getenv("RUN_HOURLY_SIGNALS", "true").lower() == "true"
RUN_MONTHLY_KPIS = os.getenv("RUN_MONTHLY_KPIS", "true").lower() == "true"

DATA_RESOLUTION = os.getenv("DATA_RESOLUTION", "hourly")

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
    Generate monthly date windows from START_YEAR / START_MONTH up to site-local today.

    Historical months:
        timestampEnd = first day of next month

    Current month:
        timestampEnd = site-local today
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
    today = site_today()
    return month_start.year == today.year and month_start.month == today.month


def is_previous_month(month_start):
    previous_month_start = first_day_of_previous_month()
    return (
        month_start.year == previous_month_start.year
        and month_start.month == previous_month_start.month
    )


def should_refresh_previous_month_today():
    today = site_today()
    return (
        REFRESH_PREVIOUS_MONTH
        and today.day <= PREVIOUS_MONTH_REFRESH_UNTIL_DAY
    )


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
# BLOB HELPERS - GENERIC
# --------------------------------------------------
def blob_exists(container_name, blob_name):
    blob_client = blob_service.get_blob_client(
        container=container_name,
        blob=blob_name,
    )
    return blob_client.exists()


def should_download_blob(month_start, container_name, blob_name, label):
    if is_current_month(month_start) and OVERWRITE_CURRENT_MONTH:
        print(f"Current month will be refreshed for {label}: {month_start:%Y-%m}")
        return True

    if is_previous_month(month_start) and should_refresh_previous_month_today():
        print(f"Previous month will be refreshed for {label}: {month_start:%Y-%m}")
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


# --------------------------------------------------
# GREENBYTE DOWNLOAD - SHARED RETRY
# --------------------------------------------------
def call_greenbyte(params, timeout=600):
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                URL,
                params=params,
                headers=HEADERS,
                timeout=timeout,
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
# GREENBYTE DOWNLOAD - HOURLY SIGNALS
# --------------------------------------------------
def download_signals_month(asset, start_date, end_date):
    device_ids = asset.get("DeviceIds")
    timestamp_start_utc, timestamp_end_utc = site_window_to_utc_strings(start_date, end_date)

    params = {
        "deviceIds": device_ids,
        "dataSignalIds": DATA_SIGNAL_IDS,
        "timestampStart": timestamp_start_utc,
        "timestampEnd": timestamp_end_utc,
        "useUtc": "true",
        "resolution": DATA_RESOLUTION,
        "aggregate": "device",
        "aggregateLevel": "0",
        "calculation": "sum",
    }

    print()
    print("Downloading hourly signals")
    print("Asset:", asset_print_name(asset))
    print("Device IDs:", device_ids)
    print("Site Start:", start_date)
    print("Site End:  ", end_date)
    print("API Start UTC:", params["timestampStart"])
    print("API End UTC:  ", params["timestampEnd"])
    print("Resolution:", DATA_RESOLUTION)
    print("Aggregate: device")
    print("Target timezone:", TARGET_TIMEZONE)

    return call_greenbyte(params, timeout=600)


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
    print("API Start UTC:", params["timestampStart"])
    print("API End UTC:  ", params["timestampEnd"])
    print("Resolution: monthly")
    print("Aggregate: site")
    print("Calculation: sum")

    return call_greenbyte(params, timeout=600)


# --------------------------------------------------
# CONVERT GREENBYTE JSON TO LONG TABLE
# --------------------------------------------------
def signals_json_to_dataframe(data, asset, month_start=None, month_end=None):
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
                        "Resolution": resolution,
                        "Calculation": calculation,
                    }
                )

    df = pd.DataFrame(rows)

    if not df.empty:
        df["Timestamp"] = to_site_time(df["Timestamp"])

        if month_start is not None and month_end is not None:
            site_start = pd.Timestamp(month_start)
            site_end = pd.Timestamp(month_end)
            df = df[(df["Timestamp"] >= site_start) & (df["Timestamp"] < site_end)].copy()

        df["Date"] = df["Timestamp"].dt.date
        df["Year"] = df["Timestamp"].dt.year
        df["Month"] = df["Timestamp"].dt.month
        df["Day"] = df["Timestamp"].dt.day
        df["Hour"] = df["Timestamp"].dt.hour
        df["TargetTimezone"] = TARGET_TIMEZONE

    return df


# --------------------------------------------------
# CLEANING / POWER BI READY PIVOT
# --------------------------------------------------
def clean_signal_name(name):
    """
    Convert Greenbyte signal names into Power BI-friendly column names.

    Examples:
    Energy Export -> Energy_Export
    Lost Production (Contractual Global) -> Lost_Production_Contractual_Global
    Energy Budget (weather adjusted) -> Energy_Budget_Weather_Adjusted
    Wind speed -> Wind_Speed
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
    Convert long signal data:
        Timestamp | DeviceID | Signal | Value

    into Power BI-ready wide format:
        Timestamp | DeviceID | Energy_Export | Wind_Speed | ...

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
        "TargetTimezone",
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
    print("Power BI hourly signal pivot completed.")
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

    One row = one asset/subpark + one KPI month.
    """
    if df.empty:
        return df

    required_columns = [
        "Asset",
        "WindFarm",
        "SubPark",
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

    if not should_download_blob(start_date, CONTAINER_NAME, blob_name, "hourly signals"):
        return

    data = download_signals_month(asset, start_date, end_date)
    long_df = signals_json_to_dataframe(data, asset, start_date, end_date)

    if long_df.empty:
        print(
            f"No signal rows for {asset_print_name(asset)} "
            f"{start_date:%Y-%m}. Skipping hourly upload."
        )
        return

    powerbi_df = pivot_signals_for_powerbi(long_df)

    if powerbi_df.empty:
        print(
            f"No Power BI-ready rows for {asset_print_name(asset)} "
            f"{start_date:%Y-%m}. Skipping hourly upload."
        )
        return

    local_file_path = save_month_file(powerbi_df, asset, start_date)
    upload_file_to_blob(local_file_path, CONTAINER_NAME, blob_name)


def process_monthly_kpis(asset, start_date, end_date):
    blob_name = get_monthly_kpi_blob_name(asset, start_date)

    if not should_download_blob(start_date, MONTHLY_KPIS_CONTAINER_NAME, blob_name, "monthly KPIs"):
        return

    data = download_monthly_kpis(asset, start_date, end_date)
    long_df = signals_json_to_dataframe(data, asset)

    if long_df.empty:
        print(
            f"No monthly KPI rows for {asset_print_name(asset)} "
            f"{start_date:%Y-%m}. Skipping monthly KPI upload."
        )
        return

    powerbi_df = pivot_monthly_kpis_for_powerbi(long_df, start_date)

    if powerbi_df.empty:
        print(
            f"No Power BI-ready monthly KPI rows for {asset_print_name(asset)} "
            f"{start_date:%Y-%m}. Skipping monthly KPI upload."
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
    print("Target timezone:", TARGET_TIMEZONE)
    print("Start:", f"{START_YEAR}-{START_MONTH:02d}")
    print("Overwrite current month:", OVERWRITE_CURRENT_MONTH)
    print("Refresh previous month:", REFRESH_PREVIOUS_MONTH)
    print("Previous month refresh until day:", PREVIOUS_MONTH_REFRESH_UNTIL_DAY)
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
                print()
                print("-" * 80)
                print("Processing month:", start_date.strftime("%Y-%m"))
                print("-" * 80)

                # IMPORTANT:
                # These are independent incremental flows.
                # If hourly signals already exist, this must NOT skip monthly KPIs.
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
    print("DONE. Incremental hourly signal files and monthly KPI files created and uploaded.")


if __name__ == "__main__":
    main()
