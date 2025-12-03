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
PAGES = 1
OUTPUT_PATH = os.path.join("data", "coingecko_markets.csv")


def fetch_markets() -> pd.DataFrame:
    all_rows = []

    for page in range(1, PAGES + 1):
        params = {
            "vs_currency": VS_CURRENCY,
            "order": "market_cap_desc",
            "per_page": PER_PAGE,
            "page": page,
            "sparkline": "false",
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

    cols = [
        "id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "total_volume",
        "price_change_percentage_24h",
        "high_24h",
        "low_24h",
        "circulating_supply",
        "total_supply",
        "max_supply",
        "ath",
        "atl",
        "last_updated",
    ]
    existing_cols = [c for c in cols if c in df.columns]
    df = df[existing_cols]

    # Add metadata columns
    run_dt = datetime.now(timezone.utc)
    df["run_datetime_utc"] = run_dt.isoformat()

    return df


def append_to_csv(new_df: pd.DataFrame, output_path: str = OUTPUT_PATH) -> None:
    """Append new data to CSV and de-duplicate."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        combined = pd.concat([existing_df, new_df], ignore_index=True)

        # De-duplicate by coin id + run_datetime_utc in case of re-runs
        if {"id", "run_datetime_utc"}.issubset(combined.columns):
            combined = combined.drop_duplicates(subset=["id", "run_datetime_utc"])
    else:
        combined = new_df

    combined.to_csv(output_path, index=False)


def main() -> int:
    try:
        new_df = fetch_markets()
        append_to_csv(new_df, OUTPUT_PATH)
        print(f"Successfully updated {OUTPUT_PATH} with {len(new_df)} rows.")
        return 0
    except Exception as e:
        print(f"Error updating data: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

