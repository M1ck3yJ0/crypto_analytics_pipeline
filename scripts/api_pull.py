#!/usr/bin/env python3
"""
scripts/api_pull.py

This is a daily update script for the fixed-universe crypto dataset.
A "frozen universe" of the top 50 crypto coins as of 2025-12-01 was selected
for a Power BI crypto analytics project. 
This script uses the CoinGecko API to read the fixed universe of coins from 
config/universe_top50_dec01_2025.csv (id, symbol, name, rank_on_2025_12_01).
For each coin, it fetches the recent history via /coins/{id}/market_chart.
For today's UTC calendar date, it selects the data point closest to 00:00:00 UTC.
It then ppends one row per coin with a pure 'date' column (YYYY-MM-DD, no time) 
to data/coingecko_markets.csv and computes daily (1d), 7d, and 30d returns.
If some coins fail due to rate limiting or other errors, 
it waits for a longer period and retries those coins only.

"""

import os
import sys
import time
from datetime import datetime, timezone, date

import requests
import pandas as pd

BASE_URL = "https://api.coingecko.com/api/v3"
VS_CURRENCY = "usd"

OUTPUT_PATH = os.path.join("data", "coingecko_markets.csv")
UNIVERSE_PATH = os.path.join("config", "universe_top50_dec01_2025.csv")

# Days of history to fetch around today
HISTORY_DAYS = 5

# Seconds to wait before a second attempt for failed coins
SECOND_PASS_SLEEP_SECONDS = 1200


def _request_with_retry(
    url: str,
    params: dict,
    max_retries: int = 5,
    base_sleep: float = 5.0,
) -> requests.Response:
    """
    Call requests.get with simple retry/backoff logic to handle
    429 (Too Many Requests) and transient HTTP errors.
    """
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, params=params, timeout=60)
        if resp.status_code == 429:
            wait_time = base_sleep * attempt
            print(
                f"429 Too Many Requests (attempt {attempt}/{max_retries}). "
                f"Sleeping {wait_time} seconds..."
            )
            time.sleep(wait_time)
            continue
        try:
            resp.raise_for_status()
            return resp
        except requests.HTTPError as e:
            if attempt == max_retries:
                print(f"HTTP error on {url} after {max_retries} attempts: {e}")
                raise
            wait_time = base_sleep * attempt
            print(
                f"HTTP error (attempt {attempt}/{max_retries}): {e}. "
                f"Sleeping {wait_time} seconds..."
            )
            time.sleep(wait_time)

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts.")


def load_universe(path: str = UNIVERSE_PATH) -> pd.DataFrame:
    """
    Load fixed-universe coin list from CSV.

    Expected columns: id, symbol, name, rank_on_2025_12_01
    The rank is not written into the fact table but is kept in the
    universe file for documentation.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Universe file not found at {path}")

    uni = pd.read_csv(path)
    required = {"id", "symbol", "name", "rank_on_2025_12_01"}
    missing = required - set(uni.columns)
    if missing:
        raise ValueError(f"Universe CSV missing columns: {missing}")

    return uni[["id", "symbol", "name"]]


def get_recent_history_for_coin(
    coin_id: str,
    vs_currency: str = VS_CURRENCY,
    days: int = HISTORY_DAYS,
) -> pd.DataFrame:
    """
    Use /coins/{id}/market_chart to fetch recent price, market cap, and volume.

    For days <= 90, CoinGecko returns sub-daily intervals (for example, hourly).
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


def pick_midnight_for_date(hist_df: pd.DataFrame, target_date: date) -> pd.Series:
    """
    From a history dataframe, pick the row whose timestamp_utc is closest to
    the target date's midnight (00:00:00 UTC).

    The caller uses only price, market_cap, and total_volume from the selected row.
    """
    midnight = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        0,
        0,
        0,
        tzinfo=timezone.utc,
    )
    tmp = hist_df.copy()
    tmp["abs_diff"] = (tmp["timestamp_utc"] - midnight).abs()
    best = tmp.loc[tmp["abs_diff"].idxmin()]
    return best.drop(labels=["abs_diff"])


def recompute_returns(combined: pd.DataFrame) -> pd.DataFrame:
    """
    Recompute 1d, 7d, and 30d percentage changes per coin based on current_price
    across the entire dataset, grouped by id and ordered by date.

    Overwrites/creates:
        - price_change_percentage_24h_in_currency
        - price_change_percentage_7d_in_currency
        - price_change_percentage_30d_in_currency
    """
    for col in ["date", "id", "current_price"]:
        if col not in combined.columns:
            raise ValueError(f"{col} column missing; cannot compute returns.")

    df = combined.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values(["id", "date"], ascending=[True, True])

    for col in [
        "price_change_percentage_24h_in_currency",
        "price_change_percentage_7d_in_currency",
        "price_change_percentage_30d_in_currency",
    ]:
        if col not in df.columns:
            df[col] = pd.NA

    def _add_returns(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        g["price_change_percentage_24h_in_currency"] = (
            g["current_price"].pct_change(periods=1) * 100
        )
        g["price_change_percentage_7d_in_currency"] = (
            g["current_price"].pct_change(periods=7) * 100
        )
        g["price_change_percentage_30d_in_currency"] = (
            g["current_price"].pct_change(periods=30) * 100
        )
        return g

    df = df.groupby("id", group_keys=False).apply(_add_returns)
    return df


def process_coin_for_date(
    coin_id: str,
    symbol: str,
    name: str,
    target_date: date,
) -> dict:
    """
    Fetch recent history for a single coin and construct a row for target_date.

    Returns a dictionary with:
        {
            "status": "ok" | "error",
            "rows": 1 or 0,
            "error": error_message or "",
            "new_row": dict or None
        }
    """
    try:
        hist_df = get_recent_history_for_coin(coin_id, VS_CURRENCY, HISTORY_DAYS)
    except Exception as e:
        msg = str(e)
        print(
            f"Error fetching history for {name} ({symbol}) [{coin_id}]: {msg}"
        )
        return {
            "status": "error",
            "rows": 0,
            "error": msg,
            "new_row": None,
        }

    picked = pick_midnight_for_date(hist_df, target_date)
    print(
        f"Using source timestamp {picked['timestamp_utc']} for {name} ({symbol}) on {target_date}."
    )

    new_row = {
        "id": coin_id,
        "symbol": symbol,
        "name": name,
        "date": target_date,  # pure date, no time component
        "current_price": picked["price"],
        "market_cap": picked["market_cap"],
        "total_volume": picked["total_volume"],
    }

    return {
        "status": "ok",
        "rows": 1,
        "error": "",
        "new_row": new_row,
    }


def main() -> int:
    # Today (UTC) is the date we will store. The workflow should run after midnight UTC.
    target_date = datetime.now(timezone.utc).date()
    print(f"Target date for daily update (midnight UTC): {target_date}")

    # Load universe of coins
    universe = load_universe(UNIVERSE_PATH)
    print(f"Loaded universe with {len(universe)} coins.")

    # Load existing CSV (if present)
    if os.path.exists(OUTPUT_PATH):
        existing = pd.read_csv(OUTPUT_PATH)
        print(f"Loaded existing data: {len(existing)} rows.")
    else:
        existing = pd.DataFrame()
        print("No existing data file found; a new one will be created.")

    # Ensure we have a 'date' column in the existing data
    if not existing.empty:
        if "date" not in existing.columns:
            raise ValueError(
                "Existing data is expected to have a 'date' column after cleaning."
            )
        existing["date"] = pd.to_datetime(existing["date"]).dt.date
        already_have = existing[existing["date"] == target_date]["id"].nunique()
        print(f"Existing rows for {target_date}: {already_have} coins.")
    else:
        already_have = 0

    all_new_rows = []
    stats = []

    # First pass
    first_pass_errors = []

    for _, row in universe.iterrows():
        coin_id = row["id"]
        symbol = row["symbol"]
        name = row["name"]

        # If we already have a row for this coin+date, skip
        if not existing.empty:
            mask = (existing["id"] == coin_id) & (existing["date"] == target_date)
            if mask.any():
                print(
                    f"Skipping {name} ({symbol}) [{coin_id}] - data for {target_date} already exists."
                )
                stats.append(
                    {
                        "id": coin_id,
                        "symbol": symbol,
                        "name": name,
                        "status": "already_have",
                        "rows": 0,
                        "error": "",
                    }
                )
                continue

        print(
            f"\n[First pass] Fetching data for {name} ({symbol}) [{coin_id}] on {target_date}"
        )
        result = process_coin_for_date(coin_id, symbol, name, target_date)

        if result["status"] == "ok":
            all_new_rows.append(result["new_row"])
            stats.append(
                {
                    "id": coin_id,
                    "symbol": symbol,
                    "name": name,
                    "status": "ok",
                    "rows": result["rows"],
                    "error": "",
                }
            )
        else:
            first_pass_errors.append((coin_id, symbol, name))
            stats.append(
                {
                    "id": coin_id,
                    "symbol": symbol,
                    "name": name,
                    "status": "error_first_pass",
                    "rows": 0,
                    "error": result["error"],
                }
            )

        time.sleep(1.5)  # space out requests

    # Second pass for errors, if any
    if first_pass_errors:
        print(
            f"\n{len(first_pass_errors)} coin(s) failed in the first pass. "
            f"Sleeping {SECOND_PASS_SLEEP_SECONDS} seconds before retrying..."
        )
        time.sleep(SECOND_PASS_SLEEP_SECONDS)

        for coin_id, symbol, name in first_pass_errors:
            # Re-check in case the row exists (in case of manual fixes or reruns)
            if not existing.empty:
                mask = (existing["id"] == coin_id) & (existing["date"] == target_date)
                if mask.any():
                    print(
                        f"Skipping retry for {name} ({symbol}) [{coin_id}] - data for "
                        f"{target_date} already exists."
                    )
                    continue

            print(
                f"\n[Second pass] Retrying data fetch for {name} ({symbol}) [{coin_id}] "
                f"on {target_date}"
            )
            result = process_coin_for_date(coin_id, symbol, name, target_date)

            if result["status"] == "ok":
                all_new_rows.append(result["new_row"])
                stats.append(
                    {
                        "id": coin_id,
                        "symbol": symbol,
                        "name": name,
                        "status": "ok_second_pass",
                        "rows": result["rows"],
                        "error": "",
                    }
                )
            else:
                # Record final failure
                stats.append(
                    {
                        "id": coin_id,
                        "symbol": symbol,
                        "name": name,
                        "status": "error_second_pass",
                        "rows": 0,
                        "error": result["error"],
                    }
                )

            time.sleep(1.5)

    if not all_new_rows:
        print("No new rows created for any coin. Nothing to append.")
        return 0

    new_df = pd.DataFrame(all_new_rows)
    print(f"\nNew rows to append for {target_date}: {len(new_df)}")

    # Combine with existing
    if existing.empty:
        combined = new_df.copy()
    else:
        # Ensure union of columns
        for col in new_df.columns:
            if col not in existing.columns:
                existing[col] = pd.NA
        for col in existing.columns:
            if col not in new_df.columns:
                new_df[col] = pd.NA

        combined = pd.concat([existing, new_df], ignore_index=True)

    # De-duplicate by (id, date)
    if {"id", "date"}.issubset(combined.columns):
        combined["date"] = pd.to_datetime(combined["date"]).dt.date
        combined = combined.drop_duplicates(subset=["id", "date"])

    # Recompute returns across the full dataset
    combined = recompute_returns(combined)

    # Optional: add a pipeline run timestamp column (same for all rows)
    run_dt = datetime.now(timezone.utc).isoformat()
    combined["last_pipeline_run_utc"] = run_dt

    # Save
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved updated daily data to {OUTPUT_PATH}")

    # Print summary
    stats_df = pd.DataFrame(stats)
    if not stats_df.empty:
        print("\nDaily update summary:")
        print(
            stats_df[["name", "symbol", "status", "rows", "error"]]
            .to_string(index=False)
        )

    # If any coins still failed after the second pass, return a non-zero exit code
    if not stats_df.empty:
        error_statuses = {"error_first_pass", "error_second_pass"}
        had_errors = stats_df["status"].isin(error_statuses).any()
        if had_errors:
            print(
                "\nOne or more coins failed after retries. "
                "The workflow will exit with a non-zero status to highlight the issue."
            )
            return 1

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
