import requests
import urllib3
import pandas as pd
from pathlib import Path
from datetime import date
from dotenv import load_dotenv
import os
from pathlib import Path
import truststore
truststore.inject_into_ssl()
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os



# --------------------------------------------------
# TEMPORARY SSL FIX FOR COMPANY NETWORK
# --------------------------------------------------
urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)

# --------------------------------------------------
# USER SETTINGS
# --------------------------------------------------

env_path = Path(__file__).parent / "API_key.env"

load_dotenv(env_path)
env_path = Path(__file__).parent / "API_key.env"
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

API_KEY = os.getenv("GREENBYTE_API_KEY")

if not API_KEY:
    raise ValueError(f"GREENBYTE_API_KEY not found in {env_path}")

ASSET_NAME = "Avloi"

DEVICE_IDS = "24710,24711,24712,24713"

DATA_SIGNAL_IDS = "4,248,281,431,6951,9252,1"

START_YEAR = 2026
START_MONTH = 1

# True = downloads from Jan 2026 up to current month-to-date
INCLUDE_CURRENT_MONTH = True

OUTPUT_FOLDER = "greenbyte_backfill"

# --------------------------------------------------
# API SETTINGS
# --------------------------------------------------

URL = "https://cubico.greenbyte.cloud/api/2/data"

HEADERS = {
    "X-Api-Key": API_KEY,
    "Accept": "application/json"
}

# --------------------------------------------------
# DATE HELPERS
# --------------------------------------------------

def first_day_of_next_month(year, month):
    if month == 12:
        return date(year + 1, 1, 1)
    return date(year, month + 1, 1)


def generate_month_ranges(start_year, start_month, include_current_month=True):
    ranges = []

    today = date.today()
    current_year = today.year
    current_month = today.month

    year = start_year
    month = start_month

    while True:
        month_start = date(year, month, 1)
        next_month_start = first_day_of_next_month(year, month)

        if include_current_month:
            if month_start > today:
                break

            if year == current_year and month == current_month:
                month_end = today
            else:
                month_end = next_month_start

        else:
            if next_month_start > date(current_year, current_month, 1):
                break

            month_end = next_month_start

        ranges.append((month_start, month_end))

        year = next_month_start.year
        month = next_month_start.month

    return ranges


# --------------------------------------------------
# DOWNLOAD SIGNALS FOR ONE MONTH
# --------------------------------------------------

def download_signals_month(start_date, end_date):
    params = {
        "deviceIds": DEVICE_IDS,
        "dataSignalIds": DATA_SIGNAL_IDS,
        "timestampStart": f"{start_date.isoformat()}T00:00:00Z",
        "timestampEnd": f"{end_date.isoformat()}T00:00:00Z",
        "useUtc": "false",
        "resolution": "10minute",
        "aggregate": "device",
        "aggregateLevel": "0",
        "calculation": "sum"
    }

    print()
    print("Downloading signals:")
    print("Start:", params["timestampStart"])
    print("End:  ", params["timestampEnd"])

    response = requests.get(
        URL,
        params=params,
        headers=HEADERS,
        timeout=600,
        verify=False
    )

    print("Status Code:", response.status_code)

    response.raise_for_status()

    return response.json()


# --------------------------------------------------
# CONVERT GREENBYTE JSON TO TABLE
# --------------------------------------------------

def signals_json_to_dataframe(data):
    rows = []

    for block in data:
        device_id = block.get("aggregateId")
        aggregate = block.get("aggregate")
        resolution = block.get("resolution")
        calculation = block.get("calculation")

        data_signal = block.get("dataSignal", {})
        signal_id = data_signal.get("dataSignalId")
        signal_name = data_signal.get("title")
        signal_unit = data_signal.get("unit")

        values = block.get("data", {})

        for timestamp, value in values.items():
            rows.append({
                "Asset": ASSET_NAME,
                "DeviceID": device_id,
                "Timestamp": timestamp,
                "DataSignalID": signal_id,
                "Signal": signal_name,
                "Unit": signal_unit,
                "Value": value,
                "Aggregate": aggregate,
                "Resolution": resolution,
                "Calculation": calculation
            })

    df = pd.DataFrame(rows)

    if not df.empty:
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

        df["Date"] = df["Timestamp"].dt.date
        df["Year"] = df["Timestamp"].dt.year
        df["Month"] = df["Timestamp"].dt.month
        df["Day"] = df["Timestamp"].dt.day
        df["Hour"] = df["Timestamp"].dt.hour
        df["Minute"] = df["Timestamp"].dt.minute

    return df

# --------------------------------------------------
# SAVE MONTHLY FILE
# --------------------------------------------------

def save_month_file(df, month_start):
    year = month_start.year
    month = month_start.month

    folder = (
        Path(OUTPUT_FOLDER)
        / "signals"
        / f"asset={ASSET_NAME}"
        / f"year={year}"
        / f"month={month:02d}"
    )

    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / f"{ASSET_NAME}_signals_{year}_{month:02d}_10minute.parquet"

    df.to_parquet(
        file_path,
        index=False
    )

    print("Saved:", file_path)
    print("Rows:", len(df))

    blob_service = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )

    blob_name = (
        f"asset={ASSET_NAME}/"
        f"year={year}/"
        f"month={month:02d}/"
        f"{os.path.basename(file_path)}"
    )

    with open(file_path, "rb") as data:
        blob_service.get_blob_client(
            container="signals",
            blob=blob_name
        ).upload_blob(data, overwrite=True)

    print(f"Uploaded to Azure: {blob_name}")
# --------------------------------------------------
# MAIN SCRIPT
# --------------------------------------------------

def main():
    print("Starting Greenbyte signals backfill...")
    print("Asset:", ASSET_NAME)
    print("Devices:", DEVICE_IDS)
    print("Signals:", DATA_SIGNAL_IDS)

    month_ranges = generate_month_ranges(
        START_YEAR,
        START_MONTH,
        INCLUDE_CURRENT_MONTH
    )

    print()
    print("Months to download:", len(month_ranges))

    for start_date, end_date in month_ranges:
        try:
            data = download_signals_month(start_date, end_date)

            df = signals_json_to_dataframe(data)

            save_month_file(df, start_date)

        except Exception as e:
            print()
            print("ERROR for month:", start_date.strftime("%Y-%m"))
            print(e)
            print("Continuing with next month...")

    print()
    print("DONE. Monthly Greenbyte signal files created.")


if __name__ == "__main__":
    main()