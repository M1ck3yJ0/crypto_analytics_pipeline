#!/usr/bin/env python3
"""
scripts/retry_missing.py

Retry worker for backfilling missing (id, date) rows.

Behavior:
- Reads data/missing_queue.csv (durable retry queue).
- For each queued (id, date), attempts to fetch CoinGecko market_chart history and
  select the point closest to 00:00:00 UTC for that date.
- Appends a date-only row to data/coingecko_markets.csv if missing.
- On success, removes that (id, date) row from the queue.
- On failure, updates attempts, last_error, last_attempt_utc.
- Recomputes 1d/7d/30d returns after appending any rows.

This script is intended to run on a schedule (e.g., twice daily) via GitHub Actions.
"""

import os
import sys
import time
from datetime import datetime, timezone, date

import requests
import pandas as pd

BASE_URL = "https://api.coingecko.com/api/v3"
VS_CURRENCY = "usd"

DATA_PATH = os.path.join("data", "coingecko_markets.csv")
QUEUE_PATH = os.path.join("data", "missing_queue.csv")

HISTORY_DAYS = 10  # <= 90 gives sub-daily points; enough coverage for most gaps
PER_ITEM_SLEEP_SECONDS = 1.5


def _request_with_retry(url: str, params: dict, max_retries: int = 5, base_sleep: float = 5.0) -> requests.Response:
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, params=params, timeout=60)
        if resp.status_code == 429:
            time.sleep(base_sleep * attempt)
            continue
        try:
            resp.raise_for_status()
            return resp
        except requests.HTTPError:
            if attempt == max_retries:
                raise
            time.sleep(base_sleep * attempt)
    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts.")


def get_recent_history_for_coin(coin_id: str, days: int = HISTORY_DAYS) -> pd.DataFrame:
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": VS_CURRENCY, "days": days}
    resp = _request_with_retry(url, params)
    data = resp.json()

    prices = pd.DataFrame(data["prices"], columns=["timestamp_ms", "price"])
    mcaps = pd.DataFrame(data["market_caps"], columns=["timestamp_ms", "market_cap"])
    vols = pd.DataFrame(data["total_volumes"], columns=["timestamp_ms", "total_volume"])

    df = prices.merge(mcaps, on="timestamp_ms").merge(vols, on="timestamp_ms")
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    return df.sort_values("timestamp_utc").reset_index(drop=True)


def pick_midnight_for_date(hist_df: pd.DataFrame, target_date: date) -> pd.Series:
    midnight = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=timezone.utc)
    tmp = hist_df.copy()
    tmp["abs_diff"] = (tmp["timestamp_utc"] - midnight).abs()
    return tmp.loc[tmp["abs_diff"].idxmin()].drop(labels=["abs_diff"])


def recompute_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values(["id", "date"], ascending=[True, True])

    for col in [
        "price_change_percentage_24h_in_currency",
        "price_change_percentage_7d_in_currency",
        "price_change_percentage_30d_in_currency",
    ]:
        if col not in df.columns:
            df[col] = pd.NA

    def _add(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()
        g["price_change_percentage_24h_in_currency"] = g["current_price"].pct_change(1) * 100
        g["price_change_percentage_7d_in_currency"] = g["current_price"].pct_change(7) * 100
        g["price_change_percentage_30d_in_currency"] = g["current_price"].pct_change(30) * 100
        return g

    return df.groupby("id", group_keys=False).apply(_add)


def main() -> int:
    if not os.path.exists(QUEUE_PATH):
        print(f"No queue found at {QUEUE_PATH}. Nothing to retry.")
        return 0

    queue = pd.read_csv(QUEUE_PATH)
    if queue.empty:
        print("Queue is empty. Nothing to retry.")
        return 0

    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Main data file not found at {DATA_PATH}")

    data = pd.read_csv(DATA_PATH)
    if "date" not in data.columns:
        raise ValueError("Main data file must have a 'date' column (date-only format).")

    data["date"] = pd.to_datetime(data["date"]).dt.date

    # Standardise queue types
    queue["date"] = pd.to_datetime(queue["date"]).dt.date
    if "attempts" not in queue.columns:
        queue["attempts"] = 0
    if "last_error" not in queue.columns:
        queue["last_error"] = ""
    if "status" not in queue.columns:
        queue["status"] = "queued"

    now_utc = datetime.now(timezone.utc).isoformat()

    successes = []
    failures = 0
    new_rows = []

    # Process oldest first (optional)
    if "first_seen_utc" in queue.columns:
        queue = queue.sort_values(["first_seen_utc"])
    else:
        queue = queue.sort_values(["date", "id"])

    for idx, r in queue.iterrows():
        coin_id = r["id"]
        symbol = r.get("symbol", "")
        name = r.get("name", "")
        d = r["date"]

        # If already filled, remove from queue
        if ((data["id"] == coin_id) & (data["date"] == d)).any():
            successes.append(idx)
            continue

        print(f"Retrying {name} ({symbol}) [{coin_id}] for date {d} ...")

        try:
            hist = get_recent_history_for_coin(coin_id, days=HISTORY_DAYS)
            picked = pick_midnight_for_date(hist, d)

            new_rows.append({
                "id": coin_id,
                "symbol": symbol,
                "name": name,
                "date": d,
                "current_price": picked["price"],
                "market_cap": picked["market_cap"],
                "total_volume": picked["total_volume"],
            })

            successes.append(idx)
            print(f"Filled {coin_id} on {d}.")
        except Exception as e:
            failures += 1
            queue.at[idx, "attempts"] = int(queue.at[idx, "attempts"]) + 1
            queue.at[idx, "last_error"] = str(e)
            queue.at[idx, "last_attempt_utc"] = now_utc
            queue.at[idx, "status"] = "error"
            print(f"Failed {coin_id} on {d}: {e}")

        time.sleep(PER_ITEM_SLEEP_SECONDS)

    if new_rows:
        new_df = pd.DataFrame(new_rows)

        # Union columns
        for col in new_df.columns:
            if col not in data.columns:
                data[col] = pd.NA
        for col in data.columns:
            if col not in new_df.columns:
                new_df[col] = pd.NA

        combined = pd.concat([data, new_df], ignore_index=True)
        combined["date"] = pd.to_datetime(combined["date"]).dt.date
        combined = combined.drop_duplicates(subset=["id", "date"])
        combined = recompute_returns(combined)

        combined["last_pipeline_run_utc"] = now_utc
        combined.to_csv(DATA_PATH, index=False)
        print(f"Appended {len(new_df)} row(s) to {DATA_PATH}.")
    else:
        print("No missing rows were filled in this run.")

    # Remove successful queue entries
    queue_remaining = queue.drop(index=successes).reset_index(drop=True)
    queue_remaining.to_csv(QUEUE_PATH, index=False)

    print(f"Queue updated. Removed {len(successes)} item(s). Remaining: {len(queue_remaining)}.")
    if failures:
        print(f"Retry run completed with {failures} failure(s).")
        # Do not fail the workflow by default; the queue preserves state for next run.

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
