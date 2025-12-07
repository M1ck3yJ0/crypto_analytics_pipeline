#!/usr/bin/env python3
"""
scripts/clean_to_date.py

One-off script to:
- Convert 'timestamp_utc' to a pure 'date' column (YYYY-MM-DD)
- Ensure one row per (id, date) (last row wins if duplicates)
- Drop timestamp_utc and timestamp_ms
- Recompute 1d, 7d, 30d returns from current_price for each coin

Run ONCE via GitHub Actions or locally.
"""

import os
import pandas as pd

OUTPUT_PATH = os.path.join("data", "coingecko_markets.csv")


def recompute_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recompute 1d, 7d, 30d percentage changes per coin based on current_price.
    """
    for col in ["id", "date", "current_price"]:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values(["id", "date"])

    # Ensure the return columns exist
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


def main() -> int:
    if not os.path.exists(OUTPUT_PATH):
        print(f"❌ {OUTPUT_PATH} does not exist.")
        return 1

    print(f"Loading existing data from {OUTPUT_PATH} ...")
    df = pd.read_csv(OUTPUT_PATH)

    if "timestamp_utc" not in df.columns:
        print("❌ 'timestamp_utc' column not found. Nothing to clean.")
        return 1

    # Parse timestamp_utc and derive pure date
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    invalid = df["timestamp_utc"].isna().sum()
    if invalid:
        print(f"⚠️ Found {invalid} rows with invalid 'timestamp_utc' - dropping them.")
        df = df.dropna(subset=["timestamp_utc"])

    df["date"] = df["timestamp_utc"].dt.date

    # Sort and keep LAST row per (id, date) if duplicates exist
    df = df.sort_values(["id", "timestamp_utc"])
    before = len(df)
    df = df.drop_duplicates(subset=["id", "date"], keep="last")
    after = len(df)
    print(f"De-duplicated by (id, date): {before} -> {after} rows")

    # Drop timestamp columns – from now on we are date-only
    for col in ["timestamp_utc", "timestamp_ms"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Recompute returns cleanly
    df = recompute_returns(df)

    # Reorder columns a bit (optional)
    cols = list(df.columns)
    preferred_order = [
        "id",
        "symbol",
        "name",
        "date",
        "current_price",
        "market_cap",
        "total_volume",
        "price_change_percentage_24h_in_currency",
        "price_change_percentage_7d_in_currency",
        "price_change_percentage_30d_in_currency",
    ]
    ordered = [c for c in preferred_order if c in cols] + [c for c in cols if c not in preferred_order]
    df = df[ordered]

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Clean daily data saved to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
