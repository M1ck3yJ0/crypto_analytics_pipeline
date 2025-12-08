#!/usr/bin/env python3
"""
scripts/backfill_history.py

This is a one-off script to backfill 365 days of historical data for 
a "frozen universe" of the top 50 crypto coins as of 2025-12-01
using the CoinGecko API, and save it to data/coingecko_markets.csv.

This is intended to be run just once to create the initial CSV,
after which the daily pipeline (api_pull.py) will append new data.
"""

import os
import sys
import time
from typing import Optional

import requests
import pandas as pd

BASE_URL = "https://api.coingecko.com/api/v3"

VS_CURRENCY = "usd"
TOP_N_COINS = 50
DAYS_HISTORY = 365

OUTPUT_PATH = os.path.join("data", "coingecko_markets.csv")


def get_top_coins(vs_currency="usd", top_n=20) -> pd.DataFrame:
    """Get top N coins by market cap."""
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": top_n,
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
    to handle 429 Too Many Requests.
    """
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, params=params, timeout=60)
        if resp.status_code == 429:
            # Too Many Requests - wait and retry
            wait_time = base_sleep * attempt
            print(f"Got 429 Too Many Requests (attempt {attempt}/{max_retries}). "
                  f"Sleeping {wait_time} seconds before retrying...")
            time.sleep(wait_time)
            continue
        try:
            resp.raise_for_status()
            return resp
        except requests.HTTPError as e:
            # For non-429 errors, either retry or fail fast on last attempt
            if attempt == max_retries:
                print(f"❌ HTTP error on {url} after {max_retries} attempts: {e}")
                raise
            wait_time = base_sleep * attempt
            print(f"⚠️  HTTP error (attempt {attempt}/{max_retries}): {e}. "
                  f"Sleeping {wait_time} seconds before retrying...")
            time.sleep(wait_time)

    # If we exit the loop without returning, something went wrong
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

    # lists of [timestamp_ms, value]
    prices = pd.DataFrame(data["prices"], columns=["timestamp_ms", "price"])
    mcaps = pd.DataFrame(data["market_caps"], columns=["timestamp_ms", "market_cap"])
    vols = pd.DataFrame(data["total_volumes"], columns=["timestamp_ms", "total_volume"])

    df = prices.merge(mcaps, on="timestamp_ms").merge(vols, on="timestamp_ms")

    # Convert ms timestamp to datetime (UTC)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)

    # Sort by time
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
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    top_coins = get_top_coins(VS_CURRENCY, TOP_N_COINS)
    print(f"Fetched top {len(top_coins)} coins by market cap.")

    all_frames = []
    stats = []  # track per-coin status and row counts

    for idx, row in top_coins.iterrows():
        coin_id = row["id"]
        symbol = row["symbol"]
        name = row["name"]
        rank = row["market_cap_rank"]

        print(f"\n=== ({idx+1}/{len(top_coins)}) Fetching history for {name} ({symbol}) [{coin_id}] ===")

        try:
            hist_df = get_history_for_coin(coin_id, VS_CURRENCY, DAYS_HISTORY)
        except Exception as e:
            # log error + skip
            msg = str(e)
            print(f"Error fetching history for {name} ({symbol}) [{coin_id}]: {msg}")
            print("   -> Skipping this coin and continuing with the others.\n")
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

        # Add metadata columns
        hist_df["id"] = coin_id
        hist_df["symbol"] = symbol
        hist_df["name"] = name
        hist_df["market_cap_rank"] = rank

        # Add return columns
        hist_df = add_return_columns(hist_df)

        # Align column names with live pipeline
        hist_df = hist_df.rename(columns={"price": "current_price"})

        # Reorder columns
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

        all_frames.append(hist_df)

        # sleep between coins
        time.sleep(3)

    if not all_frames:
        print("No data retrieved for any coin. Aborting without writing CSV.")
        return 1

    full_history = pd.concat(all_frames, ignore_index=True)
    print(f"\nTotal rows in history (all successful coins combined): {len(full_history)}")

    full_history.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved history to {OUTPUT_PATH}")

    # Create and save a summary CSV with per-coin stats
    stats_df = pd.DataFrame(stats)
    summary_path = os.path.join("data", "backfill_summary.csv")
    stats_df.to_csv(summary_path, index=False)
    print(f"\n📊 Backfill summary written to {summary_path}")
    print("\nBackfill summary (per coin):")
    try:
        # pretty-print a compact table to the Actions log
        print(stats_df[["name", "symbol", "market_cap_rank", "status", "rows"]].to_string(index=False))
    except Exception:
        print(stats_df.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
