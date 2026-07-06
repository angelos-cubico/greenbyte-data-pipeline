import requests
import urllib3
import pandas as pd
from pathlib import Path
from datetime import date
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

API_KEY = os.getenv("GREENBYTE_API_KEY")

if not API_KEY:
    raise ValueError(f"GREENBYTE_API_KEY not found in {env_path}")

ASSET_NAME = "Avloi"

DEVICE_IDS = "24710,24711,24712,24713"

START_YEAR = 2026
START_MONTH = 1

INCLUDE_CURRENT_MONTH = True

OUTPUT_FOLDER = "greenbyte_backfill"

PAGE_SIZE = 50

# --------------------------------------------------
# API SETTINGS
# --------------------------------------------------

url = "https://cubico.greenbyte.cloud/api/2/status"

headers = {
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
# DOWNLOAD ONE PAGE
# --------------------------------------------------

def download_status_page(start_date, end_date, page):
    params = {
        "deviceIds": DEVICE_IDS,
        "timestampStart": f"{start_date.isoformat()}T00:00:00Z",
        "timestampEnd": f"{end_date.isoformat()}T23:59:59Z",
        "category": "stop,curtailment",
        "categoryGlobalContract": "stop,curtailment",
        "lostProductionSignalId": "6951",
        "fields": "deviceId,message,lostProduction,timestampStart,timestampEnd,category,categoryGlobalContract,code",
        "sortAsc": "false",
        "pageSize": str(PAGE_SIZE),
        "page": str(page),
        "useUtc": "false",
        "contractType": "global"
    }

    response = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=600,
        verify=False
    )

    response.raise_for_status()

    return response.json()


# --------------------------------------------------
# DOWNLOAD ALL PAGES FOR ONE MONTH
# --------------------------------------------------

def download_status_month(start_date, end_date):
    all_rows = []
    page = 1

    print()
    print("Downloading status logs:")
    print("Start:", start_date)
    print("End:  ", end_date)

    while True:
        print("Page:", page)

        data = download_status_page(start_date, end_date, page)

        if not data:
            print("No more rows.")
            break

        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            # Safety handling in case Greenbyte wraps results in a key
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

def status_json_to_dataframe(rows):
    clean_rows = []

    for row in rows:
        clean_rows.append({
            "Asset": ASSET_NAME,
            "DeviceID": row.get("deviceId"),
            "Code": row.get("code"),
            "Message": row.get("message"),
            "LostProduction": row.get("lostProduction"),
            "TimestampStart": row.get("timestampStart"),
            "TimestampEnd": row.get("timestampEnd"),
            "Category": row.get("category"),
            "CategoryGlobalContract": row.get("categoryGlobalContract")
        })

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

def save_month_file(df, month_start):
    year = month_start.year
    month = month_start.month

    folder = (
        Path(OUTPUT_FOLDER)
        / "status_logs"
        / f"asset={ASSET_NAME}"
        / f"year={year}"
        / f"month={month:02d}"
    )

    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / f"{ASSET_NAME}_status_logs_{year}_{month:02d}.parquet"

    df.to_parquet(
    file_path,
    index=False
)

    print("Saved:", file_path)
    print("Rows:", len(df))


# --------------------------------------------------
# MAIN SCRIPT
# --------------------------------------------------

def main():
    print("Starting Greenbyte status logs backfill...")
    print("Asset:", ASSET_NAME)
    print("Devices:", DEVICE_IDS)

    month_ranges = generate_month_ranges(
        START_YEAR,
        START_MONTH,
        INCLUDE_CURRENT_MONTH
    )

    print()
    print("Months to download:", len(month_ranges))

    for start_date, end_date in month_ranges:
        try:
            rows = download_status_month(start_date, end_date)

            df = status_json_to_dataframe(rows)

            save_month_file(df, start_date)

        except Exception as e:
            print()
            print("ERROR for month:", start_date.strftime("%Y-%m"))
            print(e)
            print("Continuing with next month...")

    print()
    print("DONE. Monthly Greenbyte status log files created.")


if __name__ == "__main__":
    main()