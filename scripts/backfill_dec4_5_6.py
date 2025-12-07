#!/usr/bin/env python3
"""
scripts/backfill_dec4_5_6.py

One-off cleanup + backfill script for specific dates using the fixed-universe CSV.

NEW BEHAVIOUR:
- Does NOT delete any existing rows.
- For each coin in the universe and each target date (2025-12-04, 05, 06),
  it ONLY backfills if there is currently NO row for that (id, date).

It:
1) Loads data/coingecko_markets.csv and derives a 'date_only' column from timestamp_utc.
2) Loads config/universe_top50_dec01_2025.csv.
3) For each id in the universe and each target date:
   - If (id, date) already exists in the data: skip.
   - Else: fetch history via /coins/{id}/market_chart, pick the point closest to midnight UTC,
     and append a new row using that value.
4) Recomputes 1d / 7d / 30d returns across the full dataset.

Run ONCE via a GitHub Actions workflow. Safe to re-run: it only fills missing (id, date) combos.
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

# Dates we want to ensure are present
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

    print(f"Existing rows: {len(df)}")
    # Just report how many rows we *already* have for target dates
    existing_counts = (
        df[df["date_only"].isin(TARGET_DATES)]
        .groupby("date_only")["id"]
        .nunique()
        .to_dict()
    )
    for d in TARGET_DATES:
        print(f"Existing coins with data on {d}: {existing_counts.get(d, 0)}")

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

        # Determine which of the target dates are missing for this coin
        existing_dates_for_coin = set(df.loc[df["id"] == coin_id, "date_only"].unique())
        missing_dates = [d for d in TARGET_DATES if d not in existing_dates_for_coin]

        if not missing_dates:
            print(f"\n{ name } ({symbol}) [{coin_id}] already has rows for all target dates. Skipping.")
            stats.append({
                "id": coin_id,
                "symbol": symbol,
                "name": name,
                "market_cap_rank": rank,
                "status": "already_complete",
                "rows": 0,
                "error": "",
            })
            continue

        print(f"\n=== Backfilling Dec 4/5/6 for {name} ({symbol}) [{coin_id}] ===")
        print(f"Missing dates for this coin: {missing_dates}")

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

        rows_added_for_coin = 0

        for target_date in missing_dates:
            picked = pick_midnight_for_date(hist_df, target_date)
            picked_date = picked["timestamp_utc"].date()

            # We'll normalise to exact midnight for that date for consistency
            midnight = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=timezone.utc)
            print(
                f"Using source timestamp {picked['timestamp_utc']} "
                f"but normalising to {midnight} for {name} ({symbol}) on {target_date}."
            )

            new_row = {
                "id": coin_id,
                "symbol": symbol,
                "name": name,
                "market_cap_rank": rank,
                "timestamp_utc": midnight,
                "current_price": picked["price"],
                "market_cap": picked["market_cap"],
                "total_volume": picked["total_volume"],
                "timestamp_ms": int(midnight.timestamp() * 1000),
            }
            all_new_rows.append(new_row)
            rows_added_for_coin += 1

        stats.append({
            "id": coin_id,
            "symbol": symbol,
            "name": name,
            "market_cap_rank": rank,
            "status": "ok" if rows_added_for_coin > 0 else "no_missing",
            "rows": rows_added_for_coin,
            "error": "",
        })

        time.sleep(1.5)

    if not all_new_rows:
        print("No new rows created for any coin. Nothing to append.")
        return 0

    new_df = pd.DataFrame(all_new_rows)
    print(f"\nNew rows to append (missing id/date combos): {len(new_df)}")

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

    # De-duplicate by (id, timestamp_utc) just in case
    if {"id", "timestamp_utc"}.issubset(combined.columns):
        combined["timestamp_utc"] = pd.to_datetime(combined["timestamp_utc"], utc=True, errors="coerce")
        combined = combined.dropna(subset=["timestamp_utc"])
        combined = combined.drop_duplicates(subset=["id", "timestamp_utc"])

    # Recompute returns
    combined = recompute_returns(combined)

    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"\n✅ Saved updated file with backfilled missing dates to {OUTPUT_PATH}")

    stats_df = pd.DataFrame(stats)
    print("\nBackfill Dec 4/5/6 summary:")
    print(stats_df[["name", "symbol", "market_cap_rank", "status", "rows"]].to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
