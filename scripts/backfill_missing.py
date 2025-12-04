"""
scripts/backfill_missing.py

This is a one-off script intended as an extension to the main backfill script. 
It backfills ~365 days of historical data for specific "missing" coins 
that were skipped in the previous run due to API rate limiting.
Data is appended to data/coingecko_markets.csv
"""

import os
import sys
import time

import requests
import pandas as pd

BASE_URL = "https://api.coingecko.com/api/v3"

VS_CURRENCY = "usd"
DAYS_HISTORY = 365
OUTPUT_PATH = os.path.join("data", "coingecko_markets.csv")

# Symbols of coins that previously errored in the backfill summary
MISSING_SYMBOLS = {"bnb", "ada", "link", "xlm", "avax", "susds", "dot", "cc"}

def get_top_coins(vs_currency="usd", per_page=100) -> pd.DataFrame:
    """
    Get a reasonably large list of top coins by market cap
    so we can map symbols -> ids.
    """
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
        "sparkline": "false",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    df = pd.json_normalize(data)
    return df[["id", "symbol", "name", "market_cap_rank"]]


def _request_with_retry(url: str, params: dict, max_retries: int = 5, base_sleep: float = 5.0) -> requests.Response:
    """
    Helper to call requests.get with simple retry/backoff logic
    to handle 429 Too Many Requests etc.
    """
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, params=params, timeout=60)
        if resp.status_code == 429:
            wait_time = base_sleep * attempt
            print(f"429 Too Many Requests (attempt {attempt}/{max_retries}). "
                  f"Sleeping {wait_time} seconds...")
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
            print(f"HTTP error (attempt {attempt}/{max_retries}): {e}. "
                  f"Sleeping {wait_time} seconds...")
            time.sleep(wait_time)

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts.")


def get_history_for_coin(coin_id: str, vs_currency="usd", days=365) -> pd.DataFrame:
    """
    Use /coins/{id}/market_chart to fetch historical
    price, market cap, and volume.
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


def main() -> int:
    if not os.path.exists(OUTPUT_PATH):
        print(f"{OUTPUT_PATH} does not exist. Run the main backfill first.")
        return 1

    top_coins = get_top_coins(VS_CURRENCY, per_page=100)
    print(f"Fetched {len(top_coins)} top coins.")

    # Filter to only the missing symbols we care about
    missing_df = top_coins[top_coins["symbol"].isin(MISSING_SYMBOLS)].copy()

    if missing_df.empty:
        print("No matching missing symbols found in top coins. Nothing to do.")
        return 0

    print("Will attempt backfill for the following symbols:")
    print(missing_df[["name", "symbol", "market_cap_rank"]].to_string(index=False))

    all_new_frames = []
    stats = []

    for idx, row in missing_df.iterrows():
        coin_id = row["id"]
        symbol = row["symbol"]
        name = row["name"]
        rank = row["market_cap_rank"]

        print(f"\n=== Backfilling {name} ({symbol}) [{coin_id}] ===")

        try:
            hist_df = get_history_for_coin(coin_id, VS_CURRENCY, DAYS_HISTORY)
        except Exception as e:
            msg = str(e)
            print(f"Error fetching history for {name} ({symbol}) [{coin_id}]: {msg}")
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

        hist_df["id"] = coin_id
        hist_df["symbol"] = symbol
        hist_df["name"] = name
        hist_df["market_cap_rank"] = rank

        hist_df = add_return_columns(hist_df)
        hist_df = hist_df.rename(columns={"price": "current_price"})

        hist_df = hist_df[
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

        row_count = len(hist_df)
        print(f"Retrieved {row_count} rows for {name} ({symbol})")
        stats.append({
            "id": coin_id,
            "symbol": symbol,
            "name": name,
            "market_cap_rank": rank,
            "status": "ok",
            "rows": row_count,
            "error": "",
        })

        all_new_frames.append(hist_df)
        time.sleep(3)

    if not all_new_frames:
        print("No new data retrieved for any missing coin. Nothing to append.")
        return 0

    new_data = pd.concat(all_new_frames, ignore_index=True)

    # Load existing CSV and append
    existing = pd.read_csv(OUTPUT_PATH)

    # Ensure union of columns
    for col in new_data.columns:
        if col not in existing.columns:
            existing[col] = pd.NA
    for col in existing.columns:
        if col not in new_data.columns:
            new_data[col] = pd.NA

    combined = pd.concat([existing, new_data], ignore_index=True)

    # De-duplicate by (id, timestamp_utc)
    if {"id", "timestamp_utc"}.issubset(combined.columns):
        combined = combined.drop_duplicates(subset=["id", "timestamp_utc"])

    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"\nAppended missing coins and saved updated file to {OUTPUT_PATH}")

    # Print a little summary of what happened
    stats_df = pd.DataFrame(stats)
    if not stats_df.empty:
        print("\nBackfill-missing summary:")
        print(stats_df[["name", "symbol", "market_cap_rank", "status", "rows"]].to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
