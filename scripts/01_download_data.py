from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
PAK_MIN_LAT = 23.0
PAK_MAX_LAT = 37.0
PAK_MIN_LON = 60.0
PAK_MAX_LON = 77.0


def build_date_range(days: int = 30) -> tuple[str, str]:
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days)
    return start_date.isoformat(), end_date.isoformat()


def fetch_earthquake_data(params: dict, label: str) -> pd.DataFrame:
    try:
        response = requests.get(USGS_API_URL, params=params, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as error:
        raise RuntimeError(f"Failed to fetch {label} earthquake data: {error}") from error

    if not response.text.strip():
        raise RuntimeError(f"USGS returned an empty response for {label} request.")

    try:
        data = pd.read_csv(StringIO(response.text))
    except pd.errors.EmptyDataError as error:
        raise RuntimeError(f"USGS returned no parsable CSV rows for {label} request.") from error
    except Exception as error:  # noqa: BLE001
        raise RuntimeError(f"Could not parse USGS CSV for {label} request: {error}") from error

    if data.empty:
        raise RuntimeError(f"USGS returned 0 rows for {label} request.")

    return data


def assign_region(data: pd.DataFrame) -> pd.DataFrame:
    if "latitude" not in data.columns or "longitude" not in data.columns:
        raise RuntimeError("Required columns 'latitude' and/or 'longitude' are missing in the downloaded data.")

    region_mask = (
        data["latitude"].between(PAK_MIN_LAT, PAK_MAX_LAT)
        & data["longitude"].between(PAK_MIN_LON, PAK_MAX_LON)
    )
    data["region"] = region_mask.map({True: "Pakistan", False: "Global"})
    return data


def main() -> None:
    start_date, end_date = build_date_range(days=30)

    global_params = {
        "format": "csv",
        "starttime": start_date,
        "endtime": end_date,
        "minmagnitude": 2.5,
    }

    pakistan_params = {
        "format": "csv",
        "starttime": start_date,
        "endtime": end_date,
        "minmagnitude": 2.0,
        "minlatitude": PAK_MIN_LAT,
        "maxlatitude": PAK_MAX_LAT,
        "minlongitude": PAK_MIN_LON,
        "maxlongitude": PAK_MAX_LON,
    }

    try:
        print("Fetching global earthquake data...")
        global_df = fetch_earthquake_data(global_params, "global")
        print(f"Global earthquakes downloaded: {len(global_df):,} rows\n")

        print("Fetching Pakistan region earthquake data...")
        pakistan_df = fetch_earthquake_data(pakistan_params, "Pakistan")
        print(f"Pakistan region earthquakes downloaded: {len(pakistan_df):,} rows\n")

        print("Removing duplicates...")
        combined_df = pd.concat([global_df, pakistan_df], ignore_index=True)

        dedupe_columns = [column for column in ["time", "mag"] if column in combined_df.columns]
        if dedupe_columns:
            combined_df = combined_df.drop_duplicates(subset=dedupe_columns)
        else:
            combined_df = combined_df.drop_duplicates()

        combined_df = assign_region(combined_df)

        project_root = Path(__file__).resolve().parents[1]
        data_dir = project_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        output_file = data_dir / "earthquakes.csv"
        combined_df.to_csv(output_file, index=False)

        pakistan_rows = int((combined_df["region"] == "Pakistan").sum())
        global_rows = int((combined_df["region"] == "Global").sum())

        print(f"Final dataset: {len(combined_df):,} rows\n")
        print("Summary:")
        print(f"  Global rows   : {global_rows:,}")
        print(f"  Pakistan rows : {pakistan_rows:,}\n")

        preview_columns = [
            column
            for column in ["time", "latitude", "longitude", "depth", "mag", "place", "region"]
            if column in combined_df.columns
        ]

        print("Preview:")
        if preview_columns:
            print(combined_df[preview_columns].head(5).to_string(index=False))
        else:
            print(combined_df.head(5).to_string(index=False))

        print(f"\nFile saved to: {output_file}")
    except Exception as error:  # noqa: BLE001
        print(f"Error: {error}")


if __name__ == "__main__":
    main()
