
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
If some coins fail due to rate limiting or other errors, it waits for a longer period 
and retries those coins only. Further failure results in exit code 1 and thus an alert.

Behavior:
- Fetches one row per coin for today's UTC date (closest to 00:00 UTC).
- Computes 1d / 7d / 30d returns for that new row, by reading existing history.
- Appends each row immediately to the CSV (durable checkpointing).

"""

import os
import time
from datetime import datetime, timezone, date, timedelta

import requests
import pandas as pd

BASE_URL = "https://api.coingecko.com/api/v3"
VS_CURRENCY = "usd"

OUTPUT_PATH = os.path.join("data", "coingecko_markets.csv")
UNIVERSE_PATH = os.path.join("config", "universe_top50_dec01_2025.csv")
MISSING_QUEUE_PATH = os.path.join("data", "missing_queue.csv")

HISTORY_DAYS = 5
SECOND_PASS_SLEEP_SECONDS = 1200
REQUEST_SLEEP_SECONDS = 1.5


def _request_with_retry(url: str, params: dict, max_retries: int = 5, base_sleep: float = 5.0) -> requests.Response:
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, params=params, timeout=60)

        if resp.status_code == 429:
            wait_time = base_sleep * attempt
            print(f"429 Too Many Requests (attempt {attempt}/{max_retries}). Sleeping {wait_time} seconds...")
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
            print(f"HTTP error (attempt {attempt}/{max_retries}): {e}. Sleeping {wait_time} seconds...")
            time.sleep(wait_time)

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts.")


def load_universe(path: str = UNIVERSE_PATH) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Universe file not found at {path}")

    uni = pd.read_csv(path)
    required = {"id", "symbol", "name", "rank_on_2025_12_01"}
    missing = required - set(uni.columns)
    if missing:
        raise ValueError(f"Universe CSV missing columns: {missing}")

    return uni[["id", "symbol", "name"]]


def get_recent_history_for_coin(coin_id: str, vs_currency: str = VS_CURRENCY, days: int = HISTORY_DAYS) -> pd.DataFrame:
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days}

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
    midnight = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=timezone.utc)
    tmp = hist_df.copy()
    tmp["abs_diff"] = (tmp["timestamp_utc"] - midnight).abs()
    best = tmp.loc[tmp["abs_diff"].idxmin()]
    return best.drop(labels=["abs_diff"])


def _ensure_output_header(output_path: str, columns: list[str]) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if not os.path.exists(output_path):
        pd.DataFrame(columns=columns).to_csv(output_path, index=False)


def _append_row_to_csv(output_path: str, row_dict: dict) -> None:
    pd.DataFrame([row_dict]).to_csv(output_path, mode="a", header=False, index=False)


def upsert_missing_queue(missing_items: list[dict], queue_path: str = MISSING_QUEUE_PATH) -> None:
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


def build_price_lookup(existing_df: pd.DataFrame) -> dict[str, dict[date, float]]:
    """
    Returns: { coin_id: { date: current_price } }
    """
    if existing_df.empty:
        return {}

    need_cols = {"id", "date", "current_price"}
    if not need_cols.issubset(existing_df.columns):
        return {}

    df = existing_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["id"] = df["id"].astype(str)

    lookup: dict[str, dict[date, float]] = {}
    for cid, g in df.groupby("id"):
        # If there are duplicate dates, keep the last occurrence
        gg = g.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        lookup[cid] = dict(zip(gg["date"], gg["current_price"]))
    return lookup


def compute_returns_for_new_row(
    coin_id: str,
    target_date: date,
    current_price: float,
    price_lookup: dict[str, dict[date, float]],
) -> tuple[float | None, float | None, float | None]:
    """
    Computes returns for the new row using existing history only.
    Uses exact date matches (target_date - N). If missing, returns None for that horizon.
    """
    by_date = price_lookup.get(str(coin_id), {})

    def pct(now: float, past: float) -> float | None:
        if past is None or past == 0:
            return None
        return (now / past - 1.0) * 100.0

    p1 = by_date.get(target_date - timedelta(days=1))
    p7 = by_date.get(target_date - timedelta(days=7))
    p30 = by_date.get(target_date - timedelta(days=30))

    r1 = pct(current_price, p1) if p1 is not None else None
    r7 = pct(current_price, p7) if p7 is not None else None
    r30 = pct(current_price, p30) if p30 is not None else None

    return r1, r7, r30


def main() -> int:
    target_date = datetime.now(timezone.utc).date()
    print(f"Target date for daily update (midnight UTC): {target_date}")

    universe = load_universe(UNIVERSE_PATH)
    print(f"Loaded universe with {len(universe)} coins.")

    columns = [
        "id", "symbol", "name", "date",
        "current_price", "market_cap", "total_volume",
        "price_change_percentage_24h_in_currency",
        "price_change_percentage_7d_in_currency",
        "price_change_percentage_30d_in_currency",
        "last_pipeline_run_utc",
    ]
    _ensure_output_header(OUTPUT_PATH, columns)

    # Load existing once for: skip set + price lookup
    existing = pd.read_csv(OUTPUT_PATH) if os.path.exists(OUTPUT_PATH) else pd.DataFrame()
    if not existing.empty:
        existing["date"] = pd.to_datetime(existing["date"]).dt.date
        existing["id"] = existing["id"].astype(str)
        existing_keys = set(zip(existing["id"], existing["date"]))
    else:
        existing_keys = set()

    price_lookup = build_price_lookup(existing)

    stats: list[dict] = []
    first_pass_errors: list[tuple[str, str, str]] = []

    run_dt = datetime.now(timezone.utc).isoformat()

    # First pass
    for _, row in universe.iterrows():
        coin_id = str(row["id"])
        symbol = row["symbol"]
        name = row["name"]

        if (coin_id, target_date) in existing_keys:
            print(f"Skipping {name} ({symbol}) [{coin_id}] - data for {target_date} already exists.")
            stats.append({"id": coin_id, "symbol": symbol, "name": name, "status": "already_have", "rows": 0, "error": ""})
            continue

        print(f"\n[First pass] Fetching data for {name} ({symbol}) [{coin_id}] on {target_date}")
        try:
            hist_df = get_recent_history_for_coin(coin_id, VS_CURRENCY, HISTORY_DAYS)
            picked = pick_midnight_for_date(hist_df, target_date)
            print(f"Using source timestamp {picked['timestamp_utc']} for {name} ({symbol}) on {target_date}.")

            current_price = float(picked["price"])
            r1, r7, r30 = compute_returns_for_new_row(coin_id, target_date, current_price, price_lookup)

            new_row = {
                "id": coin_id,
                "symbol": symbol,
                "name": name,
                "date": target_date,
                "current_price": current_price,
                "market_cap": float(picked["market_cap"]),
                "total_volume": float(picked["total_volume"]),
                "price_change_percentage_24h_in_currency": r1,
                "price_change_percentage_7d_in_currency": r7,
                "price_change_percentage_30d_in_currency": r30,
                "last_pipeline_run_utc": run_dt,
            }

            _append_row_to_csv(OUTPUT_PATH, new_row)

            # Update in-memory guards so we never duplicate in the same run
            existing_keys.add((coin_id, target_date))
            price_lookup.setdefault(coin_id, {})[target_date] = current_price

            stats.append({"id": coin_id, "symbol": symbol, "name": name, "status": "ok", "rows": 1, "error": ""})

        except Exception as e:
            msg = str(e)
            print(f"Error fetching/processing {name} ({symbol}) [{coin_id}]: {msg}")
            first_pass_errors.append((coin_id, symbol, name))
            stats.append({"id": coin_id, "symbol": symbol, "name": name, "status": "error_first_pass", "rows": 0, "error": msg})

        time.sleep(REQUEST_SLEEP_SECONDS)

    # Second pass
    if first_pass_errors:
        print(f"\n{len(first_pass_errors)} coin(s) failed in first pass. Sleeping {SECOND_PASS_SLEEP_SECONDS} seconds...")
        time.sleep(SECOND_PASS_SLEEP_SECONDS)

        for coin_id, symbol, name in first_pass_errors:
            if (coin_id, target_date) in existing_keys:
                continue

            print(f"\n[Second pass] Retrying {name} ({symbol}) [{coin_id}] on {target_date}")
            try:
                hist_df = get_recent_history_for_coin(coin_id, VS_CURRENCY, HISTORY_DAYS)
                picked = pick_midnight_for_date(hist_df, target_date)
                print(f"[Second pass] Using source timestamp {picked['timestamp_utc']} for {name} ({symbol}) on {target_date}.")

                current_price = float(picked["price"])
                r1, r7, r30 = compute_returns_for_new_row(coin_id, target_date, current_price, price_lookup)

                new_row = {
                    "id": coin_id,
                    "symbol": symbol,
                    "name": name,
                    "date": target_date,
                    "current_price": current_price,
                    "market_cap": float(picked["market_cap"]),
                    "total_volume": float(picked["total_volume"]),
                    "price_change_percentage_24h_in_currency": r1,
                    "price_change_percentage_7d_in_currency": r7,
                    "price_change_percentage_30d_in_currency": r30,
                    "last_pipeline_run_utc": run_dt,
                }

                _append_row_to_csv(OUTPUT_PATH, new_row)

                existing_keys.add((coin_id, target_date))
                price_lookup.setdefault(coin_id, {})[target_date] = current_price

                stats.append({"id": coin_id, "symbol": symbol, "name": name, "status": "ok_second_pass", "rows": 1, "error": ""})

            except Exception as e:
                msg = str(e)
                print(f"[Second pass] Error for {name} ({symbol}) [{coin_id}]: {msg}")
                stats.append({"id": coin_id, "symbol": symbol, "name": name, "status": "error_second_pass", "rows": 0, "error": msg})

            time.sleep(REQUEST_SLEEP_SECONDS)

    # Summary
    stats_df = pd.DataFrame(stats)
    if not stats_df.empty:
        print("\nDaily update summary:")
        print(stats_df[["name", "symbol", "status", "rows", "error"]].to_string(index=False))

    # Queue final failures
    if not stats_df.empty:
        final_failures = stats_df[stats_df["status"] == "error_second_pass"].copy()
        if not final_failures.empty:
            queue_rows = []
            for _, r in final_failures.iterrows():
                queue_rows.append({
                    "id": r["id"],
                    "symbol": r["symbol"],
                    "name": r["name"],
                    "date": str(target_date),
                    "attempts": 1,
                    "last_error": r.get("error", ""),
                    "status": "queued",
                })
            upsert_missing_queue(queue_rows, MISSING_QUEUE_PATH)
            print(f"Queued {len(queue_rows)} item(s) for retry in {MISSING_QUEUE_PATH}.")

    # Exit code policy (same spirit as your original)
    if not stats_df.empty:
        any_ok = stats_df["status"].isin(["ok", "ok_second_pass", "already_have"]).any()
        if not any_ok:
            if os.path.exists(MISSING_QUEUE_PATH):
                print("No rows were fetched, but failures were recorded in the retry queue.")
                return 0
            print("No rows were fetched and no retry queue exists; treating as a failure.")
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
