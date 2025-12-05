#!/usr/bin/env python3
"""
scripts/backfill_specific_dates.py

One-off script to:
1. Remove rows in data/coingecko_markets.csv for specific dates.
2. Fetch historical prices around midnight UTC for those dates from CoinGecko.
3. Append those rows back into the CSV, matching the existing schema.

Intended use:
- Run ONCE via a GitHub Actions workflow.
"""

import os
import sys
import time
from datetime import datetime, timezone, timedelta, date

import requests
import pandas as pd

BASE_URL = "https://api.coingecko.com/api/v3"
VS_CURRENCY = "usd"

OUTPUT_PATH = os.path.join("data", "coingecko_markets.csv")

# Dates we want to replace (UTC calendar dates)
TARGET_DATES = [
    date(2025, 12, 4),
    date(2025, 12, 5),
]

# How many days of history to pull around the target dates
# (must be <= 90, otherwise CoinGecko switches to 1-day resolution)
HISTORY_DAYS = 10


def _request_with_retry(url: str, params: dict, max_retries: int = 5, base_sleep: float = 5.0) -> requests.Response:
    """
    Helper to call requests.get with simple retry/backoff logic
    to handle 429 Too Many Requests etc.
    """
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, params=params, timeout=60)
        if resp.status_code == 429:
            wait_time = base_sleep * attempt
            print(f"⚠️  429 Too Many Requests (attempt {attempt}/{max_retries}). "
                  f"Sleeping {wait_time} seconds...")
            time.sleep(wait_time)
            continue
        try:
            resp.raise_for_status()
            return resp
        except requests.HTTPError as e:
            if attempt == max_retries:
                print(f"❌ HTTP error on {url} after {max_retries} attempts: {e}")
                raise
            wait_time = base_sleep * attempt
            print(f"⚠️  HTTP error (attempt {attempt}/{max_retries}): {e}. "
                  f"Sleeping {wait_time} seconds...")
            time.sleep(wait_time)

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts.")


def get_history_for_coin(coin_id: str, vs_currency="usd", days=HISTORY_DAYS) -> pd.DataFrame:
    """
    Use /coins/{id}/market_chart to fetch recent historical price, market cap, and volume.
    For days <= 90, CoinGecko returns prices at sub-daily intervals (e.g., hourly).
    We'll pick the point closest to midnight UTC for our target dates.
    """
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": vs_currency,
        "days": days,
    }

    resp = _request_with_retry(url, params)
    data = resp.json()

    prices = pd.DataFrame(data["prices"], columns=["timestamp_ms", "price"])
    mcaps = pd.DataFrame(data["market_caps"], columns=["timestamp_ms", "market_cap"])
    vols = pd.DataFrame(data["total_volumes"], columns=["timestamp_ms", "total_volume"])

    df = prices.merge(mcaps, on="timestamp_ms").merge(vols, on="timestamp_ms")
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df = df.sort_values("timestamp_utc").reset_index(drop=True)
    return df


def add_return_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 1d, 7d, 30d percentage change columns based on 'price'.
    Note: with only 10 days of history, these are approximate, but fine
    for plugging the two days we care about.
    """
    df = df.copy()
    df["price_change_percentage_24h_in_currency"] = (
        df["price"].pct_change(periods=1) * 100
    )
    df["price_change_percentage_7d_in_currency"] = (
        df["price"].pct_change(periods=7) * 100
    )
    df["price_change_percentage_30d_in_currency"] = (
        df["price"].pct_change(periods=30) * 100
    )
    return df


def pick_midnight_rows(hist_df: pd.DataFrame, targets: list[date]) -> pd.DataFrame:
    """
    For each target date, pick the row whose timestamp_utc is closest to that day's midnight UTC.
    """
    results = []

    for d in targets:
        midnight = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
        # add a column of absolute time difference
        tmp = hist_df.copy()
        tmp["abs_diff"] = (tmp["timestamp_utc"] - midnight).abs()
        # pick the row with the minimum difference
        best = tmp.loc[tmp["abs_diff"].idxmin()]
        results.append(best)

    out_df = pd.DataFrame(results).drop(columns=["abs_diff"])
    return out_df


def main() -> int:
    if not os.path.exists(OUTPUT_PATH):
        print(f"❌ {OUTPUT_PATH} does not exist. Run initial backfill first.")
        return 1

    print(f"Loading existing data from {OUTPUT_PATH} ...")
    df = pd.read_csv(OUTPUT_PATH)
    
    # Ensure timestamp_utc is proper datetime with timezone
    if "timestamp_utc" not in df.columns:
        print("❌ Column 'timestamp_utc' not found in CSV.")
        return 1
    
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    
    # Optionally drop any rows where timestamp_utc couldn't be parsed
    invalid_ts = df["timestamp_utc"].isna().sum()
    if invalid_ts > 0:
        print(f"⚠️ Found {invalid_ts} rows with invalid 'timestamp_utc' - dropping them.")
        df = df.dropna(subset=["timestamp_utc"])
    
    # 1) Remove rows for the target dates
    print(f"Initial rows: {len(df)}")
    df["date_only"] = df["timestamp_utc"].dt.date
    before = len(df)
    df = df[~df["date_only"].isin(TARGET_DATES)].copy()
    after = len(df)
    print(f"Removed {before - after} rows for dates {TARGET_DATES}")

    # We'll need unique coin metadata from existing data
    meta_cols = ["id", "symbol", "name", "market_cap_rank"]
    coin_meta = df[meta_cols].drop_duplicates()

    # 2) For each coin, fetch recent history and pick midnight points for target dates
    all_new_rows = []
    stats = []

    for _, row in coin_meta.iterrows():
        coin_id = row["id"]
        symbol = row["symbol"]
        name = row["name"]
        rank = row["market_cap_rank"]

        print(f"\n=== Backfilling specific dates for {name} ({symbol}) [{coin_id}] ===")

        try:
            hist_df = get_history_for_coin(coin_id, VS_CURRENCY, HISTORY_DAYS)
        except Exception as e:
            msg = str(e)
            print(f"❌ Error fetching history for {name} ({symbol}) [{coin_id}]: {msg}")
            stats.append({
                "id": coin_id,
                "symbol": symbol,
                "name": name,
                "market_cap_rank": rank,
                "status": "error",
                "rows": 0,
                "error": msg,
            })
            time.sleep(5)
            continue

        # Add return columns on the full history window
        hist_df = add_return_columns(hist_df)

        # Pick rows closest to midnight for each target date
        # (Note: we assume the 10-day window includes those target dates)
        picked = pick_midnight_rows(hist_df, TARGET_DATES)

        # Add metadata columns and align schema
        picked["id"] = coin_id
        picked["symbol"] = symbol
        picked["name"] = name
        picked["market_cap_rank"] = rank
        picked = picked.rename(columns={"price": "current_price"})

        picked = picked[
            [
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
        ]

        row_count = len(picked)
        print(f"✅ Retrieved {row_count} replacement rows for {name} ({symbol})")
        stats.append({
            "id": coin_id,
            "symbol": symbol,
            "name": name,
            "market_cap_rank": rank,
            "status": "ok",
            "rows": row_count,
            "error": "",
        })

        all_new_rows.append(picked)
        time.sleep(2)

    if not all_new_rows:
        print("No new rows created for any coin. Aborting without writing.")
        return 1

    new_rows_df = pd.concat(all_new_rows, ignore_index=True)

    # Drop helper column
    df = df.drop(columns=["date_only"])

    # 3) Append and deduplicate by (id, timestamp_utc)
    # Ensure union of columns
    for col in new_rows_df.columns:
        if col not in df.columns:
            df[col] = pd.NA
    for col in df.columns:
        if col not in new_rows_df.columns:
            new_rows_df[col] = pd.NA

    combined = pd.concat([df, new_rows_df], ignore_index=True)

    if {"id", "timestamp_utc"}.issubset(combined.columns):
        combined = combined.drop_duplicates(subset=["id", "timestamp_utc"])

    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"\n✅ Saved updated file with backfilled dates to {OUTPUT_PATH}")

    # Print quick summary
    stats_df = pd.DataFrame(stats)
    print("\nBackfill-specific-dates summary:")
    print(stats_df[["name", "symbol", "market_cap_rank", "status", "rows"]].to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
