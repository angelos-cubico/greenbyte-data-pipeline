"""
download_dirtydozen_incremental.py

Builds a Dirty Dozen reporting layer from:
1. Raw Greenbyte status logs in Azure Blob container: statuslogs
2. Hourly Greenbyte signals in Azure Blob container: signals

Purpose:
- Use hourly Lost Production signal 6951 as the driving table.
- For each hourly lost-production timestamp/device, find the active status-log event.
- Attribute that hour's lost production to the winning event/code/message/category.
- Save monthly parquet outputs to Azure Blob container: dirtydozen.

Important refresh behaviour:
- Current month is rebuilt every daily run when OVERWRITE_CURRENT_MONTH=true.
- Previous month is also rebuilt until PREVIOUS_MONTH_REFRESH_UNTIL_DAY when REFRESH_PREVIOUS_MONTH=true.
- This means late status categorisation updates made by site managers after month end are picked up before the monthly PDF export.

Expected local files:
- API_key.env
- assets.json

Expected API_key.env values:
AZURE_STORAGE_CONNECTION_STRING=...

Optional API_key.env values:
STATUSLOGS_CONTAINER_NAME=statuslogs
SIGNALS_CONTAINER_NAME=signals
DIRTYDOZEN_CONTAINER_NAME=dirtydozen
START_YEAR=2026
START_MONTH=1
PROCESS_ALL_ASSETS=true
INCLUDE_CURRENT_MONTH=true
OVERWRITE_CURRENT_MONTH=true
REFRESH_PREVIOUS_MONTH=true
PREVIOUS_MONTH_REFRESH_UNTIL_DAY=7
LOOKBACK_MONTHS=6
OUTPUT_FOLDER=greenbyte_backfill
LOST_PRODUCTION_SIGNAL_ID=6951
SIGNALS_TIMEZONE_MODE=naive
"""

import json
import os
from datetime import date, datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


# --------------------------------------------------
# SETTINGS
# --------------------------------------------------
BASE_DIR = Path(__file__).parent
ENV_PATH = BASE_DIR / "API_key.env"
ASSETS_PATH = BASE_DIR / "assets.json"

load_dotenv(ENV_PATH)

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not AZURE_STORAGE_CONNECTION_STRING:
    raise ValueError(f"AZURE_STORAGE_CONNECTION_STRING not found. Checked: {ENV_PATH}")

blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

STATUSLOGS_CONTAINER_NAME = os.getenv("STATUSLOGS_CONTAINER_NAME", "statuslogs")
SIGNALS_CONTAINER_NAME = os.getenv("SIGNALS_CONTAINER_NAME", "signals")
DIRTYDOZEN_CONTAINER_NAME = os.getenv("DIRTYDOZEN_CONTAINER_NAME", "dirtydozen")

OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "greenbyte_backfill")

START_YEAR = int(os.getenv("START_YEAR", "2026"))
START_MONTH = int(os.getenv("START_MONTH", "1"))
INCLUDE_CURRENT_MONTH = os.getenv("INCLUDE_CURRENT_MONTH", "true").lower() == "true"
OVERWRITE_CURRENT_MONTH = os.getenv("OVERWRITE_CURRENT_MONTH", "true").lower() == "true"
REFRESH_PREVIOUS_MONTH = os.getenv("REFRESH_PREVIOUS_MONTH", "true").lower() == "true"
PREVIOUS_MONTH_REFRESH_UNTIL_DAY = int(os.getenv("PREVIOUS_MONTH_REFRESH_UNTIL_DAY", "7"))
PROCESS_ALL_ASSETS = os.getenv("PROCESS_ALL_ASSETS", "true").lower() == "true"
LOOKBACK_MONTHS = int(os.getenv("LOOKBACK_MONTHS", "6"))

LOST_PRODUCTION_SIGNAL_ID = os.getenv("LOST_PRODUCTION_SIGNAL_ID", "6951")
SIGNALS_TIMEZONE_MODE = os.getenv("SIGNALS_TIMEZONE_MODE", "naive").lower()

DEFAULT_ASSET_NAME = os.getenv("ASSET_NAME", "Avloi")
DEFAULT_DEVICE_IDS = os.getenv("DEVICE_IDS", "24710,24711,24712,24713")
DEFAULT_LOST_PRODUCTION_SIGNAL_ID = os.getenv("LOST_PRODUCTION_SIGNAL_ID", "6951")


# --------------------------------------------------
# ASSET HELPERS
# --------------------------------------------------
def load_assets():
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
    name = asset.get("AssetName") or asset.get("SubPark") or asset.get("WindFarm")
    return str(name).strip().replace(" ", "_")


def asset_print_name(asset):
    return asset.get("SubPark") or asset.get("WindFarm") or asset.get("AssetName")


def asset_device_ids(asset):
    raw = asset.get("DeviceIds") or asset.get("DeviceIDs") or ""
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    return [x.strip() for x in str(raw).split(",") if x.strip()]


# --------------------------------------------------
# DATE HELPERS
# --------------------------------------------------
def first_day_of_next_month(year, month):
    if month == 12:
        return date(year + 1, 1, 1)
    return date(year, month + 1, 1)


def first_day_of_previous_month(today=None):
    today = today or date.today()
    if today.month == 1:
        return date(today.year - 1, 12, 1)
    return date(today.year, today.month - 1, 1)


def add_months(d, months):
    y = d.year + ((d.month - 1 + months) // 12)
    m = ((d.month - 1 + months) % 12) + 1
    return date(y, m, 1)


def generate_month_ranges(start_year, start_month, include_current_month=True):
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


def is_previous_month(month_start):
    prev = first_day_of_previous_month()
    return month_start.year == prev.year and month_start.month == prev.month


def should_refresh_previous_month_today():
    today = date.today()
    return REFRESH_PREVIOUS_MONTH and today.day <= PREVIOUS_MONTH_REFRESH_UNTIL_DAY


def date_to_ts(d):
    return pd.Timestamp(datetime(d.year, d.month, d.day))


def normalize_timestamp_series(s):
    ts = pd.to_datetime(s, errors="coerce")

    try:
        if getattr(ts.dt, "tz", None) is not None:
            ts = ts.dt.tz_convert("UTC").dt.tz_localize(None)
    except Exception:
        pass

    return ts


# --------------------------------------------------
# BLOB HELPERS
# --------------------------------------------------
def list_blob_names(container_name, prefix):
    container_client = blob_service.get_container_client(container_name)
    return [b.name for b in container_client.list_blobs(name_starts_with=prefix)]


def blob_exists(container_name, blob_name):
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
    return blob_client.exists()


def read_parquet_blob(container_name, blob_name):
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
    payload = blob_client.download_blob().readall()
    return pd.read_parquet(BytesIO(payload))


def upload_file_to_blob(local_file_path, container_name, blob_name):
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded to Azure Blob Storage: {container_name}/{blob_name}")


# --------------------------------------------------
# PATH HELPERS
# --------------------------------------------------
def status_prefix(asset, month_start):
    asset_name = asset_folder_name(asset)
    return f"asset={asset_name}/year={month_start.year}/month={month_start.month:02d}/"


def signals_prefix(asset, month_start):
    asset_name = asset_folder_name(asset)
    return f"asset={asset_name}/year={month_start.year}/month={month_start.month:02d}/"


def dirty_file_name(asset, month_start):
    asset_name = asset_folder_name(asset)
    return f"{asset_name}_dirtydozen_{month_start.year}_{month_start.month:02d}.parquet"


def dirty_blob_name(asset, month_start):
    asset_name = asset_folder_name(asset)
    file_name = dirty_file_name(asset, month_start)
    return (
        f"asset={asset_name}/"
        f"year={month_start.year}/"
        f"month={month_start.month:02d}/"
        f"{file_name}"
    )


def should_build_month(month_start, target_blob_name):
    """
    Decide whether to rebuild a monthly Dirty Dozen parquet.

    Rules:
    - Current month is rebuilt if OVERWRITE_CURRENT_MONTH=true.
    - Previous month is rebuilt during the configured post-month validation window.
    - Older historical months are skipped if the blob already exists.
    """
    if is_current_month(month_start) and OVERWRITE_CURRENT_MONTH:
        print(f"Current month will be rebuilt: {month_start:%Y-%m}")
        return True

    if is_previous_month(month_start) and should_refresh_previous_month_today():
        print(f"Previous month will be rebuilt for late status categorisation updates: {month_start:%Y-%m}")
        return True

    if blob_exists(DIRTYDOZEN_CONTAINER_NAME, target_blob_name):
        print(f"Already exists in DirtyDozen container. Skipping: {target_blob_name}")
        return False

    print(f"DirtyDozen month missing. Will build: {month_start:%Y-%m}")
    return True


# --------------------------------------------------
# RAW STATUS LOG LOADING
# --------------------------------------------------
def load_statuslogs_for_month_window(asset, target_month_start):
    dfs = []
    loaded = []
    missing = []

    first_lookup = add_months(target_month_start, -LOOKBACK_MONTHS)
    current = first_lookup

    while current <= target_month_start:
        prefix = status_prefix(asset, current)
        blobs = [b for b in list_blob_names(STATUSLOGS_CONTAINER_NAME, prefix) if b.lower().endswith(".parquet")]

        if not blobs:
            missing.append(current.strftime("%Y-%m"))
        else:
            for blob_name in blobs:
                print(f"  Loading statuslogs: {STATUSLOGS_CONTAINER_NAME}/{blob_name}")
                try:
                    df = read_parquet_blob(STATUSLOGS_CONTAINER_NAME, blob_name)
                    if not df.empty:
                        dfs.append(df)
                except Exception as e:
                    print(f"  WARNING: Failed to read status blob {blob_name}: {e}")
            loaded.append(current.strftime("%Y-%m"))

        current = add_months(current, 1)

    if missing:
        print("  Missing status months:", ", ".join(missing))

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def normalize_statuslogs(df, asset):
    df = df.copy()

    rename_map = {
        "deviceId": "DeviceID",
        "deviceID": "DeviceID",
        "DeviceId": "DeviceID",
        "code": "Code",
        "message": "Message",
        "comment": "Comment",
        "lostProduction": "EventLostProduction",
        "timestampStart": "TimestampStart",
        "timestampEnd": "TimestampEnd",
        "category": "Category",
        "categoryGlobalContract": "CategoryGlobalContract",
    }

    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df[new] = df[old]

    required_cols = [
        "Asset", "WindFarm", "SubPark", "DeviceID", "Code", "Message", "Comment",
        "TimestampStart", "TimestampEnd", "Category", "CategoryGlobalContract",
        "EventLostProduction",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    asset_name = asset_folder_name(asset)
    df["Asset"] = df["Asset"].fillna(asset_name)
    df["WindFarm"] = df["WindFarm"].fillna(asset.get("WindFarm"))
    df["SubPark"] = df["SubPark"].fillna(asset.get("SubPark"))

    df["DeviceID"] = df["DeviceID"].astype(str).str.strip()
    df["TimestampStart"] = normalize_timestamp_series(df["TimestampStart"])
    df["TimestampEnd"] = normalize_timestamp_series(df["TimestampEnd"])
    df["EventLostProduction"] = pd.to_numeric(df["EventLostProduction"], errors="coerce")

    df = df[df["TimestampStart"].notna()].copy()

    if "CategoryGlobalContract" in df.columns:
        df["Priority"] = (
            df["CategoryGlobalContract"]
            .astype(str)
            .str.extract(r"^(\d+)", expand=False)
            .astype(float)
            .fillna(999)
            .astype(int)
        )
    else:
        df["Priority"] = 999

    category_priority = {
        "stop": 1,
        "curtailment": 2,
        "warning": 3,
    }
    df["CategoryPriority"] = (
        df["Category"].astype(str).str.lower().map(category_priority).fillna(999).astype(int)
    )

    return df


# --------------------------------------------------
# SIGNAL LOADING / NORMALIZATION
# --------------------------------------------------
def load_signals_for_month(asset, month_start):
    prefix = signals_prefix(asset, month_start)
    blobs = [b for b in list_blob_names(SIGNALS_CONTAINER_NAME, prefix) if b.lower().endswith(".parquet")]

    if not blobs:
        print(f"  No signal parquet blobs found under: {SIGNALS_CONTAINER_NAME}/{prefix}")
        return pd.DataFrame()

    dfs = []
    for blob_name in blobs:
        print(f"  Loading signals: {SIGNALS_CONTAINER_NAME}/{blob_name}")
        try:
            df = read_parquet_blob(SIGNALS_CONTAINER_NAME, blob_name)
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print(f"  WARNING: Failed to read signal blob {blob_name}: {e}")

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def find_first_existing_column(df, candidates):
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in df.columns:
            return c
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def normalize_signals_lp6951(df, asset):
    """
    Converts a potentially wide or long signals dataframe into:
    Asset, WindFarm, SubPark, DeviceID, Time, LP6951.
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()

    device_col = find_first_existing_column(df, ["DeviceID", "DeviceId", "deviceId", "deviceID", "TurbineID", "AssetDeviceId"])
    time_col = find_first_existing_column(df, ["Time", "time", "Timestamp", "timestamp", "DateTime", "datetime", "Date", "date"])

    if not device_col or not time_col:
        raise ValueError(
            "Could not identify DeviceID/time columns in signals file. "
            f"Columns found: {df.columns.tolist()}"
        )

    signal_id_col = find_first_existing_column(df, ["SignalID", "SignalId", "signalId", "signalID", "Signal", "signal"])
    value_col = find_first_existing_column(df, ["Value", "value", "SignalValue", "signalValue", "Actual", "actual"])

    if signal_id_col and value_col:
        tmp = df.copy()
        tmp[signal_id_col] = tmp[signal_id_col].astype(str)
        tmp = tmp[tmp[signal_id_col].str.contains(str(LOST_PRODUCTION_SIGNAL_ID), case=False, na=False)].copy()
        if tmp.empty:
            return pd.DataFrame()

        out = pd.DataFrame({
            "DeviceID": tmp[device_col].astype(str).str.strip(),
            "Time": normalize_timestamp_series(tmp[time_col]),
            "LP6951": pd.to_numeric(tmp[value_col], errors="coerce"),
        })
    else:
        lp_candidates = []
        for c in df.columns:
            cl = str(c).lower().replace(" ", "")
            if str(LOST_PRODUCTION_SIGNAL_ID) in str(c):
                lp_candidates.append(c)
            elif cl in ["lostproduction", "lostproduction6951", "lp6951", "lossproduction", "productionloss"]:
                lp_candidates.append(c)
            elif "lost" in cl and "production" in cl:
                lp_candidates.append(c)

        if not lp_candidates:
            raise ValueError(
                "Could not identify LP6951 column in signals file. "
                f"Signal id searched: {LOST_PRODUCTION_SIGNAL_ID}. Columns found: {df.columns.tolist()}"
            )

        lp_col = lp_candidates[0]
        print(f"  Using signal LP column: {lp_col}")

        out = pd.DataFrame({
            "DeviceID": df[device_col].astype(str).str.strip(),
            "Time": normalize_timestamp_series(df[time_col]),
            "LP6951": pd.to_numeric(df[lp_col], errors="coerce"),
        })

    asset_name = asset_folder_name(asset)
    out["Asset"] = asset_name
    out["WindFarm"] = asset.get("WindFarm")
    out["SubPark"] = asset.get("SubPark")

    out = out[out["Time"].notna()].copy()
    out = out.drop_duplicates(subset=["DeviceID", "Time"], keep="last")
    out = out.sort_values(["DeviceID", "Time"]).reset_index(drop=True)

    return out[["Asset", "WindFarm", "SubPark", "DeviceID", "Time", "LP6951"]]


# --------------------------------------------------
# EVENT ATTRIBUTION LOGIC
# --------------------------------------------------
def assign_events_to_hourly_lp(signals_df, status_df, month_start, month_end):
    """
    For each hourly LP signal row, find the status event active in that hour.

    Hour interval:
      [Time, Time + 1 hour)

    Event overlaps hour if:
      TimestampStart < HourEnd AND EventEndForOverlap > HourStart

    If multiple events overlap same device/hour:
      1. Lowest Priority wins.
      2. stop > curtailment > warning.
      3. Longest overlap inside the hour wins.
    """
    if signals_df.empty:
        return pd.DataFrame()

    month_start_ts = date_to_ts(month_start)
    month_end_ts = date_to_ts(month_end)
    now_ts = pd.Timestamp.now("UTC").tz_localize(None)

    sig = signals_df.copy()
    sig["HourStart"] = sig["Time"]
    sig["HourEnd"] = sig["Time"] + pd.Timedelta(hours=1)

    sig = sig[(sig["HourStart"] >= month_start_ts) & (sig["HourStart"] < month_end_ts)].copy()

    if sig.empty:
        return pd.DataFrame()

    if status_df.empty:
        out = sig.copy()
        for c in ["Code", "Message", "Comment", "Category", "CategoryGlobalContract"]:
            out[c] = pd.NA
        out["Priority"] = 999
        out["CategoryPriority"] = 999
        out["EventStartExact"] = pd.NaT
        out["EventEndExact"] = pd.NaT
        out["EventOverlapMinutesInHour"] = 0.0
        out["HasMatchedEvent"] = False
        return finalize_dirty_columns(out, month_start, month_end)

    events = status_df.copy()
    events["EventEndForOverlap"] = events["TimestampEnd"]
    events["IsOngoingEvent"] = events["TimestampEnd"].isna()
    events.loc[events["IsOngoingEvent"], "EventEndForOverlap"] = now_ts

    events = events[
        (events["TimestampStart"] < month_end_ts)
        & (events["EventEndForOverlap"] > month_start_ts)
    ].copy()

    if events.empty:
        out = sig.copy()
        for c in ["Code", "Message", "Comment", "Category", "CategoryGlobalContract"]:
            out[c] = pd.NA
        out["Priority"] = 999
        out["CategoryPriority"] = 999
        out["EventStartExact"] = pd.NaT
        out["EventEndExact"] = pd.NaT
        out["IsOngoingEvent"] = False
        out["EventOverlapMinutesInHour"] = 0.0
        out["HasMatchedEvent"] = False
        return finalize_dirty_columns(out, month_start, month_end)

    outputs = []

    for device_id, sig_dev in sig.groupby("DeviceID", sort=False):
        ev_dev = events[events["DeviceID"] == str(device_id)].copy()

        if ev_dev.empty:
            tmp = sig_dev.copy()
            for c in ["Code", "Message", "Comment", "Category", "CategoryGlobalContract"]:
                tmp[c] = pd.NA
            tmp["Priority"] = 999
            tmp["CategoryPriority"] = 999
            tmp["EventStartExact"] = pd.NaT
            tmp["EventEndExact"] = pd.NaT
            tmp["IsOngoingEvent"] = False
            tmp["EventOverlapMinutesInHour"] = 0.0
            tmp["HasMatchedEvent"] = False
            outputs.append(tmp)
            continue

        a = sig_dev.reset_index(drop=True).copy()
        b = ev_dev.reset_index(drop=True).copy()
        a["_key"] = 1
        b["_key"] = 1
        joined = a.merge(b, on="_key", how="left", suffixes=("", "_event")).drop(columns=["_key"])

        joined = joined[
            (joined["TimestampStart"] < joined["HourEnd"])
            & (joined["EventEndForOverlap"] > joined["HourStart"])
        ].copy()

        if joined.empty:
            tmp = sig_dev.copy()
            for c in ["Code", "Message", "Comment", "Category", "CategoryGlobalContract"]:
                tmp[c] = pd.NA
            tmp["Priority"] = 999
            tmp["CategoryPriority"] = 999
            tmp["EventStartExact"] = pd.NaT
            tmp["EventEndExact"] = pd.NaT
            tmp["IsOngoingEvent"] = False
            tmp["EventOverlapMinutesInHour"] = 0.0
            tmp["HasMatchedEvent"] = False
            outputs.append(tmp)
            continue

        overlap_start = joined["HourStart"].where(joined["HourStart"] > joined["TimestampStart"], joined["TimestampStart"])
        overlap_end = joined["HourEnd"].where(joined["HourEnd"] < joined["EventEndForOverlap"], joined["EventEndForOverlap"])
        joined["EventOverlapMinutesInHour"] = (
            (overlap_end - overlap_start).dt.total_seconds() / 60.0
        ).clip(lower=0, upper=60).fillna(0)

        joined = joined.sort_values(
            ["DeviceID", "Time", "Priority", "CategoryPriority", "EventOverlapMinutesInHour"],
            ascending=[True, True, True, True, False],
        )

        winners = joined.drop_duplicates(subset=["DeviceID", "Time"], keep="first").copy()
        winners["EventStartExact"] = winners["TimestampStart"]
        winners["EventEndExact"] = winners["TimestampEnd"]
        winners["HasMatchedEvent"] = True

        base_keys = sig_dev[["DeviceID", "Time"]].copy()
        winners_keys = winners[["DeviceID", "Time"]].copy()
        missing_keys = base_keys.merge(winners_keys, on=["DeviceID", "Time"], how="left", indicator=True)
        missing_keys = missing_keys[missing_keys["_merge"] == "left_only"][["DeviceID", "Time"]]

        if not missing_keys.empty:
            missing = sig_dev.merge(missing_keys, on=["DeviceID", "Time"], how="inner")
            for c in ["Code", "Message", "Comment", "Category", "CategoryGlobalContract"]:
                missing[c] = pd.NA
            missing["Priority"] = 999
            missing["CategoryPriority"] = 999
            missing["EventStartExact"] = pd.NaT
            missing["EventEndExact"] = pd.NaT
            missing["IsOngoingEvent"] = False
            missing["EventOverlapMinutesInHour"] = 0.0
            missing["HasMatchedEvent"] = False
            winners = pd.concat([winners, missing], ignore_index=True)

        outputs.append(winners)

    out = pd.concat(outputs, ignore_index=True)
    out = finalize_dirty_columns(out, month_start, month_end)
    return out


def finalize_dirty_columns(df, month_start, month_end):
    df = df.copy()

    df["ReportYear"] = month_start.year
    df["ReportMonth"] = month_start.month
    df["ReportMonthStart"] = date_to_ts(month_start)
    df["ReportMonthEnd"] = date_to_ts(month_end)

    df["LP6951"] = pd.to_numeric(df["LP6951"], errors="coerce").fillna(0)

    df["DirtyDozenCause"] = df["Code"].astype("string").fillna("No matched event")
    msg = df["Message"].astype("string").fillna("")
    df.loc[msg != "", "DirtyDozenCause"] = df.loc[msg != "", "DirtyDozenCause"] + " - " + msg[msg != ""]

    df["HasLostProduction"] = df["LP6951"].fillna(0) != 0
    df["DirtyDate"] = pd.to_datetime(df["Time"], errors="coerce").dt.date

    df["RowRankByLP"] = df["LP6951"].rank(method="dense", ascending=False).astype(int)

    ordered_cols = [
        "Asset", "WindFarm", "SubPark", "DeviceID", "Time", "DirtyDate",
        "LP6951", "HasLostProduction",
        "Code", "Message", "Comment", "Category", "CategoryGlobalContract",
        "Priority", "CategoryPriority",
        "EventStartExact", "EventEndExact", "IsOngoingEvent",
        "EventOverlapMinutesInHour", "HasMatchedEvent",
        "DirtyDozenCause",
        "ReportYear", "ReportMonth", "ReportMonthStart", "ReportMonthEnd",
        "HourStart", "HourEnd", "RowRankByLP",
    ]

    extra_cols = [c for c in df.columns if c not in ordered_cols and not c.endswith("_event")]
    existing_ordered = [c for c in ordered_cols if c in df.columns]

    return df[existing_ordered + extra_cols].sort_values(["Asset", "DeviceID", "Time"]).reset_index(drop=True)


# --------------------------------------------------
# SAVE DIRTYDOZEN
# --------------------------------------------------
def save_dirty_month_file(df, asset, month_start):
    asset_name = asset_folder_name(asset)
    year = month_start.year
    month = month_start.month

    folder = (
        Path(OUTPUT_FOLDER)
        / "dirtydozen"
        / f"asset={asset_name}"
        / f"year={year}"
        / f"month={month:02d}"
    )
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / dirty_file_name(asset, month_start)
    df.to_parquet(file_path, index=False)

    print("Saved local DirtyDozen parquet:", file_path)
    print("Rows:", len(df))
    print("Matched event rows:", int(df["HasMatchedEvent"].sum()) if "HasMatchedEvent" in df else "n/a")
    print("Total LP6951:", float(df["LP6951"].sum()) if "LP6951" in df else "n/a")

    return file_path


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("Starting INCREMENTAL DirtyDozen builder...")
    print("Statuslogs container:", STATUSLOGS_CONTAINER_NAME)
    print("Signals container:", SIGNALS_CONTAINER_NAME)
    print("DirtyDozen container:", DIRTYDOZEN_CONTAINER_NAME)
    print("Lost production signal id:", LOST_PRODUCTION_SIGNAL_ID)
    print("Start:", f"{START_YEAR}-{START_MONTH:02d}")
    print("Lookback months:", LOOKBACK_MONTHS)
    print("Overwrite current month:", OVERWRITE_CURRENT_MONTH)
    print("Refresh previous month:", REFRESH_PREVIOUS_MONTH)
    print("Previous month refresh until day:", PREVIOUS_MONTH_REFRESH_UNTIL_DAY)

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

        for month_start, month_end in month_ranges:
            target_blob_name = dirty_blob_name(asset, month_start)

            try:
                if not should_build_month(month_start, target_blob_name):
                    continue

                print()
                print("Building DirtyDozen")
                print("Asset:", asset_print_name(asset))
                print("Month:", month_start.strftime("%Y-%m"))
                print("Window:", month_start, "to", month_end)

                raw_signals = load_signals_for_month(asset, month_start)
                if raw_signals.empty:
                    print(f"No signals found for {asset_print_name(asset)} {month_start:%Y-%m}. Skipping.")
                    continue

                lp_signals = normalize_signals_lp6951(raw_signals, asset)
                if lp_signals.empty:
                    print(f"No LP6951 rows found for {asset_print_name(asset)} {month_start:%Y-%m}. Skipping.")
                    continue

                print("LP6951 signal rows:", len(lp_signals))
                print("LP6951 total in source month:", float(lp_signals["LP6951"].fillna(0).sum()))

                raw_status = load_statuslogs_for_month_window(asset, month_start)
                status = normalize_statuslogs(raw_status, asset) if not raw_status.empty else pd.DataFrame()
                print("Status rows loaded:", len(status))

                dirty = assign_events_to_hourly_lp(lp_signals, status, month_start, month_end)

                if dirty.empty:
                    print(f"DirtyDozen output empty for {asset_print_name(asset)} {month_start:%Y-%m}. Skipping upload.")
                    continue

                local_file_path = save_dirty_month_file(dirty, asset, month_start)
                upload_file_to_blob(local_file_path, DIRTYDOZEN_CONTAINER_NAME, target_blob_name)

            except Exception as e:
                print()
                print("ERROR")
                print("Asset:", asset_print_name(asset))
                print("Month:", month_start.strftime("%Y-%m"))
                print("Details:", e)
                print("Continuing with next month...")

    print()
    print("DONE. DirtyDozen files created and uploaded.")


if __name__ == "__main__":
    main()
