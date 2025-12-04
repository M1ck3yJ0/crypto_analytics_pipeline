"""
scripts/api_pull.py

This script is to pull market data from the CoinGecko API and append it to data/coingecko_markets.csv.
It's intended to run on a schedule by GitHub Actions.
"""

import os
import sys
from datetime import datetime, timezone

import requests
import pandas as pd

API_URL = "https://api.coingecko.com/api/v3/coins/markets"

VS_CURRENCY = "usd"
PER_PAGE = 50
PAGES = 1            # PAGES * PER_PAGE = top 50 coins

OUTPUT_PATH = os.path.join("data", "coingecko_markets.csv")


def fetch_markets() -> pd.DataFrame:
    """Fetch market data from CoinGecko and return as a pandas DataFrame."""
    all_rows = []

    for page in range(1, PAGES + 1):
        params = {
            "vs_currency": VS_CURRENCY,
            "order": "market_cap_desc",
            "per_page": PER_PAGE,
            "page": page,
            "sparkline": "false",
            # include multiple price change windows
            "price_change_percentage": "1h,24h,7d,30d,200d,1y",
        }
        resp = requests.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        all_rows.extend(data)

    if not all_rows:
        raise RuntimeError("No data returned from CoinGecko API.")

    df = pd.json_normalize(all_rows)

    # Create timestamp_utc from last_updated (ISO 8601)
    if "last_updated" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["last_updated"], utc=True)
        # derive ms since epoch to align with backfill's timestamp_ms
        df["timestamp_ms"] = df["timestamp_utc"].view("int64") // 10**6
    else:
        # fallback: use run time (will be overwritten later)
        run_dt = datetime.now(timezone.utc)
        df["timestamp_utc"] = run_dt
        df["timestamp_ms"] = df["timestamp_utc"].view("int64") // 10**6

    # Columns (matching backfill)
    aligned_cols = [
        "id",
        "symbol",
        "name",
        "market_cap_rank",
        "timestamp_utc",
        "current_price",
        "market_cap",
        "total_volume",
        "price_change_percentage_24h_in_currency",
        "price_change_percentage_7d_in_currency",
        "price_change_percentage_30d_in_currency",
        "timestamp_ms",
    ]

    # Keep only the aligned columns that exist in the dataframe
    existing_cols = [c for c in aligned_cols if c in df.columns]
    df = df[existing_cols]

    # Add metadata column: when this snapshot was taken by the pipeline
    run_dt = datetime.now(timezone.utc)
    df["run_datetime_utc"] = run_dt.isoformat()

    return df


def append_to_csv(new_df: pd.DataFrame, output_path: str = OUTPUT_PATH) -> None:
    """
    Append new data to CSV, creating it if it doesn't exist, and de-duplicate.
    De-duplicates by (id, timestamp_utc) so multiple runs with the
    same snapshot don't create duplicate rows.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Ensure timestamp_utc is a string/ISO in the final CSV
    if "timestamp_utc" in new_df.columns and not pd.api.types.is_string_dtype(new_df["timestamp_utc"]):
        new_df["timestamp_utc"] = new_df["timestamp_utc"].astype("datetime64[ns, UTC]").astype(str)

    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)

        # Ensure the combined dataframe has the union of columns
        for col in new_df.columns:
            if col not in existing_df.columns:
                existing_df[col] = pd.NA
        for col in existing_df.columns:
            if col not in new_df.columns:
                new_df[col] = pd.NA

        combined = pd.concat([existing_df, new_df], ignore_index=True)

        # De-duplicate by coin id + timestamp_utc if both exist
        if {"id", "timestamp_utc"}.issubset(combined.columns):
            combined = combined.drop_duplicates(subset=["id", "timestamp_utc"])
    else:
        # First time: write the new dataframe
        combined = new_df

    combined.to_csv(output_path, index=False)


def main() -> int:
    try:
        new_df = fetch_markets()
        append_to_csv(new_df, OUTPUT_PATH)
        print(f"Successfully updated {OUTPUT_PATH} with {len(new_df)} new snapshot rows.")
        return 0
    except Exception as e:
        print(f"Error updating data: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
