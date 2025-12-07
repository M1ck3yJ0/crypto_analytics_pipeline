#!/usr/bin/env python3
"""
scripts/backfill_dec4_5_6.py

One-off cleanup + backfill script for specific dates using the fixed-universe CSV.

It:
1) Removes rows for 2025-12-04, 2025-12-05, 2025-12-06 from data/coingecko_markets.csv.
2) For each coin in config/universe_top50_dec01_2025.csv, fetches recent history
   via /coins/{id}/market_chart.
3) For each of those three dates, picks the data point closest to 00:00:00 UTC.
4) Appends those rows and recomputes 1d / 7d / 30d returns across the full dataset.

Run ONCE via a GitHub Actions workflow.
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

# Dates we want to clean and backfill
TARGET_DATES = [
    date(2025, 12, 4),
    date(2025, 12, 5),
    date(2025, 12, 6),
]

# Days of history to fetch (must be <= 90 for sub-daily points)
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


def load_universe(path: str = UNIVERSE_PATH) -> pd.DataFrame:
    """
    Load fixed-universe coin list from CSV.
    Expected columns: id, symbol, name, rank_on_2025_12_01
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Universe file not found at {path}")

    uni = pd.read_csv(path)
    required = {"id", "symbol", "name", "rank_on_2025_12_01"}
    missing = required - set(uni.columns)
    if missing:
        raise ValueError(f"Universe CSV missing columns: {missing}")

    uni = uni.copy()
    uni = uni.rename(columns={"rank_on_2025_12_01": "market_cap_rank"})
    return uni[["id", "symbol", "name", "market_cap_rank"]]


def get_recent_history_for_coin(coin_id: str, vs_currency: str = VS_CURRENCY, days: int = HISTORY_DAYS) -> pd.DataFrame:
    """
    Use /coins/{id}/market_chart to fetch recent historical price, market cap, and volume.
    For days <= 90, CoinGecko returns prices at sub-daily intervals.
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
    """
    midnight = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=timezone.utc)
    tmp = hist_df.copy()
    tmp["abs_diff"] = (tmp["timestamp_utc"] - midnight).abs()
    best = tmp.loc[tmp["abs_diff"].idxmin()]
    return best.drop(labels=["abs_diff"])


def recompute_returns(combined: pd.DataFrame) -> pd.DataFrame:
    """
    Recompute 1d, 7d, 30d percentage changes per coin based on current_price
    across the ENTIRE dataset, grouped by id and ordered by timestamp_utc.
    This overwrites/creates:
        - price_change_percentage_24h_in_currency
        - price_change_percentage_7d_in_currency
        - price_change_percentage_30d_in_currency
    """
    for col in ["timestamp_utc", "id", "current_price"]:
        if col not in combined.columns:
            raise ValueError(f"{col} column missing; cannot compute returns.")

    df = combined.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_utc"])

    df = df.sort_values(["id", "timestamp_utc"], ascending=[True, True])

    def _add_returns(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        g["price_change_percentage_24h_in_currency"] = g["current_price"].pct_change(periods=1) * 100
        g["price_change_percentage_7d_in_currency"] = g["current_price"].pct_change(periods=7) * 100
        g["price_change_percentage_30d_in_currency"] = g["current_price"].pct_change(periods=30) * 100
        return g

    df = df.groupby("id", group_keys=False).apply(_add_returns)
    return df


def main() -> int:
    if not os.path.exists(OUTPUT_PATH):
        print(f"❌ {OUTPUT_PATH} does not exist.")
        return 1

    print(f"Loading existing data from {OUTPUT_PATH} ...")
    df = pd.read_csv(OUTPUT_PATH)

    if "timestamp_utc" not in df.columns:
        print("❌ 'timestamp_utc' column missing in CSV.")
        return 1

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_utc"])
    df["date_only"] = df["timestamp_utc"].dt.date

    before = len(df)
    df = df[~df["date_only"].isin(TARGET_DATES)].copy()
    after = len(df)
    print(f"Removed {before - after} rows for dates {TARGET_DATES}")

    # Load universe
    universe = load_universe(UNIVERSE_PATH)
    print(f"Loaded universe with {len(universe)} coins.")

    all_new_rows = []
    stats = []

    for _, row in universe.iterrows():
        coin_id = row["id"]
        symbol = row["symbol"]
        name = row["name"]
        rank = row["market_cap_rank"]

        print(f"\n=== Backfilling 4/5/6 Dec for {name} ({symbol}) [{coin_id}] ===")

        try:
            hist_df = get_recent_history_for_coin(coin_id, VS_CURRENCY, HISTORY_DAYS)
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

        # For each target date, pick closest to midnight
        for target_date in TARGET_DATES:
            picked = pick_midnight_for_date(hist_df, target_date)
            picked_date = picked["timestamp_utc"].date()
            if picked_date != target_date:
                print(f"⚠️ Closest timestamp for {name} ({symbol}) on {target_date} "
                      f"is {picked['timestamp_utc']} (date {picked_date}). Using it anyway.")
            else:
                print(f"✅ Using timestamp {picked['timestamp_utc']} for {name} ({symbol}) on {target_date}.")

            new_row = {
                "id": coin_id,
                "symbol": symbol,
                "name": name,
                "market_cap_rank": rank,
                "timestamp_utc": picked["timestamp_utc"],
                "current_price": picked["price"],
                "market_cap": picked["market_cap"],
                "total_volume": picked["total_volume"],
                "timestamp_ms": picked["timestamp_ms"],
            }
            all_new_rows.append(new_row)

        stats.append({
            "id": coin_id,
            "symbol": symbol,
            "name": name,
            "market_cap_rank": rank,
            "status": "ok",
            "rows": len(TARGET_DATES),
            "error": "",
        })

        time.sleep(1.5)

    if not all_new_rows:
        print("No new rows created for any coin. Nothing to append.")
        return 0

    new_df = pd.DataFrame(all_new_rows)
    print(f"\nNew rows to append for dates {TARGET_DATES}: {len(new_df)}")

    # Drop helper column
    df = df.drop(columns=["date_only"])

    # Ensure union of columns
    for col in new_df.columns:
        if col not in df.columns:
            df[col] = pd.NA
    for col in df.columns:
        if col not in new_df.columns:
            new_df[col] = pd.NA

    combined = pd.concat([df, new_df], ignore_index=True)

    # De-duplicate by (id, timestamp_utc)
    if {"id", "timestamp_utc"}.issubset(combined.columns):
        combined["timestamp_utc"] = pd.to_datetime(combined["timestamp_utc"], utc=True, errors="coerce")
        combined = combined.dropna(subset=["timestamp_utc"])
        combined = combined.drop_duplicates(subset=["id", "timestamp_utc"])

    # Recompute returns
    combined = recompute_returns(combined)

    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"\n✅ Saved updated file with backfilled dates to {OUTPUT_PATH}")

    stats_df = pd.DataFrame(stats)
    print("\nBackfill Dec 4/5/6 summary:")
    print(stats_df[["name", "symbol", "market_cap_rank", "status", "rows"]].to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
