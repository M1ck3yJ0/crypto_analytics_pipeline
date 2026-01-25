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

This script is intended to run on a schedule (twice daily) via GitHub Actions.

"""

import os
import time
from datetime import datetime, timezone, date, timedelta

import requests
import pandas as pd

BASE_URL = "https://api.coingecko.com/api/v3"
VS_CURRENCY = "usd"

DATA_PATH = os.path.join("data", "coingecko_markets.csv")
QUEUE_PATH = os.path.join("data", "missing_queue.csv")

# Max days to request from market_chart to avoid huge payloads.
# 90 keeps sub-daily granularity on CoinGecko market_chart.
MAX_HISTORY_DAYS = 90

# Buffer days around oldest missing date (helps midnight selection near edges)
BUFFER_DAYS = 2

# Sleep to space out per-coin requests (not per item)
PER_COIN_SLEEP_SECONDS = 1.5


def _request_with_retry(
    url: str,
    params: dict,
    max_retries: int = 5,
    base_sleep: float = 5.0,
) -> requests.Response:
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


def get_recent_history_for_coin(coin_id: str, days: int) -> pd.DataFrame:
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


def _ensure_output_header(output_path: str, columns: list[str]) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if not os.path.exists(output_path):
        pd.DataFrame(columns=columns).to_csv(output_path, index=False)


def _append_row_to_csv(output_path: str, row_dict: dict) -> None:
    pd.DataFrame([row_dict]).to_csv(output_path, mode="a", header=False, index=False)


def build_existing_keyset_and_price_lookup(data_df: pd.DataFrame) -> tuple[set[tuple[str, date]], dict[str, dict[date, float]]]:
    """
    Returns:
      existing_keys: {(id, date), ...}
      price_lookup: { id: { date: current_price } }
    """
    if data_df.empty:
        return set(), {}

    df = data_df.copy()
    df["id"] = df["id"].astype(str)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    existing_keys = set(zip(df["id"], df["date"]))

    lookup: dict[str, dict[date, float]] = {}
    if "current_price" in df.columns:
        for cid, g in df.groupby("id"):
            gg = g.sort_values("date").drop_duplicates(subset=["date"], keep="last")
            lookup[cid] = dict(zip(gg["date"], gg["current_price"]))
    return existing_keys, lookup


def compute_returns_for_new_row(
    coin_id: str,
    target_date: date,
    current_price: float,
    price_lookup: dict[str, dict[date, float]],
) -> tuple[float | None, float | None, float | None]:
    """
    Exact-date returns:
      1d uses D-1
      7d uses D-7
      30d uses D-30
    Returns None if the required historical date is missing.
    """
    by_date = price_lookup.get(str(coin_id), {})

    def pct(now: float, past: float | None) -> float | None:
        if past is None or past == 0:
            return None
        return (now / past - 1.0) * 100.0

    r1 = pct(current_price, by_date.get(target_date - timedelta(days=1)))
    r7 = pct(current_price, by_date.get(target_date - timedelta(days=7)))
    r30 = pct(current_price, by_date.get(target_date - timedelta(days=30)))
    return r1, r7, r30


def upsert_missing_queue(missing_items: list[dict], queue_path: str = QUEUE_PATH) -> None:
    """
    Same behavior as your original: unique by (id, date), last row wins.
    """
    if not missing_items:
        return

    now_utc = datetime.now(timezone.utc).isoformat()

    incoming = pd.DataFrame(missing_items)
    incoming["date"] = pd.to_datetime(incoming["date"]).dt.date.astype(str)
    incoming["last_attempt_utc"] = now_utc
    incoming["status"] = incoming.get("status", "queued")

    for col in ["attempts", "last_error", "first_seen_utc"]:
        if col not in incoming.columns:
            incoming[col] = pd.NA

    if os.path.exists(queue_path):
        existing = pd.read_csv(queue_path)
        if "date" in existing.columns:
            existing["date"] = pd.to_datetime(existing["date"]).dt.date.astype(str)

        for col in incoming.columns:
            if col not in existing.columns:
                existing[col] = pd.NA
        for col in existing.columns:
            if col not in incoming.columns:
                incoming[col] = pd.NA

        combined = pd.concat([existing, incoming], ignore_index=True)
        combined = combined.sort_values(["id", "date", "last_attempt_utc"])
        combined = combined.drop_duplicates(subset=["id", "date"], keep="last")
    else:
        incoming["first_seen_utc"] = now_utc
        combined = incoming.drop_duplicates(subset=["id", "date"], keep="last")

    combined["first_seen_utc"] = combined["first_seen_utc"].fillna(now_utc)
    os.makedirs(os.path.dirname(queue_path), exist_ok=True)
    combined.to_csv(queue_path, index=False)


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

    # Ensure the output has a header that supports append-only filled rows
    out_cols = [
        "id", "symbol", "name", "date",
        "current_price", "market_cap", "total_volume",
        "price_change_percentage_24h_in_currency",
        "price_change_percentage_7d_in_currency",
        "price_change_percentage_30d_in_currency",
        "last_pipeline_run_utc",
    ]
    _ensure_output_header(DATA_PATH, out_cols)

    data = pd.read_csv(DATA_PATH)
    if not data.empty and "date" not in data.columns:
        raise ValueError("Main data file must have a 'date' column (date-only format).")

    # Standardize data types
    if not data.empty:
        data["id"] = data["id"].astype(str)
        data["date"] = pd.to_datetime(data["date"]).dt.date

    existing_keys, price_lookup = build_existing_keyset_and_price_lookup(data)

    # Standardize queue types + ensure cols
    queue["id"] = queue["id"].astype(str)
    queue["date"] = pd.to_datetime(queue["date"]).dt.date
    if "attempts" not in queue.columns:
        queue["attempts"] = 0
    if "last_error" not in queue.columns:
        queue["last_error"] = ""
    if "status" not in queue.columns:
        queue["status"] = "queued"

    now_utc = datetime.now(timezone.utc).isoformat()

    # Process oldest first
    if "first_seen_utc" in queue.columns:
        queue = queue.sort_values(["first_seen_utc"])
    else:
        queue = queue.sort_values(["date", "id"])

    # We'll drop successful queue entries by index at the end
    success_indices: list[int] = []
    failures = 0

    # Group queue by coin to reduce API calls
    for coin_id, g in queue.groupby("id", sort=False):
        # Filter out items already present
        pending_rows = []
        for idx, r in g.iterrows():
            d = r["date"]
            if (coin_id, d) in existing_keys:
                success_indices.append(idx)
            else:
                pending_rows.append((idx, r))

        if not pending_rows:
            continue

        # Determine history window needed for this coin
        oldest_date = min(r["date"] for _, r in pending_rows)
        today_utc = datetime.now(timezone.utc).date()
        days_needed = (today_utc - oldest_date).days + BUFFER_DAYS
        days_needed = max(2, min(MAX_HISTORY_DAYS, days_needed))

        symbol_any = str(pending_rows[0][1].get("symbol", ""))
        name_any = str(pending_rows[0][1].get("name", ""))

        print(f"\nCoin {name_any} ({symbol_any}) [{coin_id}] - {len(pending_rows)} item(s), requesting {days_needed} day(s) history...")

        try:
            hist = get_recent_history_for_coin(coin_id, days=days_needed)
        except Exception as e:
            # If we cannot fetch history, mark all pending as failed once
            msg = str(e)
            print(f"Failed fetching history for {coin_id}: {msg}")
            failures += len(pending_rows)
            for idx, r in pending_rows:
                queue.at[idx, "attempts"] = int(queue.at[idx, "attempts"]) + 1
                queue.at[idx, "last_error"] = msg
                queue.at[idx, "last_attempt_utc"] = now_utc
                queue.at[idx, "status"] = "error"
            time.sleep(PER_COIN_SLEEP_SECONDS)
            continue

        # Fill each missing date for this coin from the one history response
        for idx, r in pending_rows:
            d = r["date"]
            symbol = r.get("symbol", "")
            name = r.get("name", "")

            try:
                picked = pick_midnight_for_date(hist, d)

                current_price = float(picked["price"])
                r1, r7, r30 = compute_returns_for_new_row(coin_id, d, current_price, price_lookup)

                new_row = {
                    "id": coin_id,
                    "symbol": symbol,
                    "name": name,
                    "date": d,
                    "current_price": current_price,
                    "market_cap": float(picked["market_cap"]),
                    "total_volume": float(picked["total_volume"]),
                    "price_change_percentage_24h_in_currency": r1,
                    "price_change_percentage_7d_in_currency": r7,
                    "price_change_percentage_30d_in_currency": r30,
                    "last_pipeline_run_utc": now_utc,
                }

                _append_row_to_csv(DATA_PATH, new_row)

                # Update in-memory structures to prevent duplicates and allow subsequent return calcs
                existing_keys.add((coin_id, d))
                price_lookup.setdefault(coin_id, {})[d] = current_price

                success_indices.append(idx)
                print(f"Filled {coin_id} on {d}.")

            except Exception as e:
                failures += 1
                msg = str(e)
                queue.at[idx, "attempts"] = int(queue.at[idx, "attempts"]) + 1
                queue.at[idx, "last_error"] = msg
                queue.at[idx, "last_attempt_utc"] = now_utc
                queue.at[idx, "status"] = "error"
                print(f"Failed {coin_id} on {d}: {msg}")

        time.sleep(PER_COIN_SLEEP_SECONDS)

    # Remove successful queue entries and persist queue
    queue_remaining = queue.drop(index=success_indices).reset_index(drop=True)
    queue_remaining.to_csv(QUEUE_PATH, index=False)

    print(f"\nQueue updated. Removed {len(success_indices)} item(s). Remaining: {len(queue_remaining)}.")
    if failures:
        print(f"Retry run completed with {failures} failure(s). Queue preserves state for next run.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
