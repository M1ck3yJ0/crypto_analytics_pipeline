def main() -> int:
    # ONE-TIME BACKFILL: hardcoded dates (REMOVE AFTER RUN)
    TARGET_DATES = [
        date(2025, 12, 9),
        date(2025, 12, 10),
        date(2025, 12, 11),
        date(2025, 12, 12),
    ]

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
            raise ValueError("Existing data is expected to have a 'date' column after cleaning.")
        existing["date"] = pd.to_datetime(existing["date"]).dt.date

    all_new_rows = []
    stats = []
    first_pass_errors = []  # list of tuples: (coin_id, symbol, name, target_date)

    # First pass: try to fill all missing (coin, date)
    for target_date in TARGET_DATES:
        already_have = 0
        if not existing.empty:
            already_have = existing[existing["date"] == target_date]["id"].nunique()
        print(f"\n=== Processing date {target_date} (already have {already_have} coins) ===")

        for _, row in universe.iterrows():
            coin_id = row["id"]
            symbol = row["symbol"]
            name = row["name"]

            # Skip if already exists
            if not existing.empty:
                mask = (existing["id"] == coin_id) & (existing["date"] == target_date)
                if mask.any():
                    stats.append({
                        "id": coin_id,
                        "symbol": symbol,
                        "name": name,
                        "date": str(target_date),
                        "status": "already_have",
                        "rows": 0,
                        "error": "",
                    })
                    continue

            print(f"[First pass] {target_date} - {name} ({symbol}) [{coin_id}]")
            result = process_coin_for_date(coin_id, symbol, name, target_date)

            if result["status"] == "ok":
                all_new_rows.append(result["new_row"])
                stats.append({
                    "id": coin_id,
                    "symbol": symbol,
                    "name": name,
                    "date": str(target_date),
                    "status": "ok",
                    "rows": 1,
                    "error": "",
                })
            else:
                first_pass_errors.append((coin_id, symbol, name, target_date))
                stats.append({
                    "id": coin_id,
                    "symbol": symbol,
                    "name": name,
                    "date": str(target_date),
                    "status": "error_first_pass",
                    "rows": 0,
                    "error": result["error"],
                })

            time.sleep(1.5)

    # Second pass for failures
    if first_pass_errors:
        print(
            f"\n{len(first_pass_errors)} item(s) failed in the first pass. "
            f"Sleeping {SECOND_PASS_SLEEP_SECONDS} seconds before retrying..."
        )
        time.sleep(SECOND_PASS_SLEEP_SECONDS)

        for coin_id, symbol, name, target_date in first_pass_errors:
            # Re-check in case it was filled by another run
            if not existing.empty:
                mask = (existing["id"] == coin_id) & (existing["date"] == target_date)
                if mask.any():
                    continue

            print(f"[Second pass] {target_date} - {name} ({symbol}) [{coin_id}]")
            result = process_coin_for_date(coin_id, symbol, name, target_date)

            if result["status"] == "ok":
                all_new_rows.append(result["new_row"])
                stats.append({
                    "id": coin_id,
                    "symbol": symbol,
                    "name": name,
                    "date": str(target_date),
                    "status": "ok_second_pass",
                    "rows": 1,
                    "error": "",
                })
            else:
                stats.append({
                    "id": coin_id,
                    "symbol": symbol,
                    "name": name,
                    "date": str(target_date),
                    "status": "error_second_pass",
                    "rows": 0,
                    "error": result["error"],
                })

            time.sleep(1.5)

    # If nothing new, still write queue and exit
    stats_df = pd.DataFrame(stats)

    # Persist final failures to the durable retry queue (one row per id+date)
    if not stats_df.empty:
        final_failures = stats_df[stats_df["status"] == "error_second_pass"].copy()
        if not final_failures.empty:
            queue_rows = []
            for _, r in final_failures.iterrows():
                queue_rows.append({
                    "id": r["id"],
                    "symbol": r["symbol"],
                    "name": r["name"],
                    "date": r["date"],  # IMPORTANT: per-row date
                    "attempts": 1,
                    "last_error": r.get("error", ""),
                    "status": "queued",
                })
            upsert_missing_queue(queue_rows, MISSING_QUEUE_PATH)
            print(f"Queued {len(queue_rows)} item(s) for retry in {MISSING_QUEUE_PATH}.")

    if not all_new_rows:
        print("\nNo new rows created in this run.")
        if not stats_df.empty:
            print("\nBackfill summary:")
            print(stats_df[["date", "name", "symbol", "status", "error"]].to_string(index=False))
        return 0

    # Append and recompute once
    new_df = pd.DataFrame(all_new_rows)
    new_df["date"] = pd.to_datetime(new_df["date"]).dt.date

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
    combined["date"] = pd.to_datetime(combined["date"]).dt.date
    combined = combined.drop_duplicates(subset=["id", "date"])

    # Recompute returns across the full dataset
    combined = recompute_returns(combined)

    # Add pipeline run timestamp
    combined["last_pipeline_run_utc"] = datetime.now(timezone.utc).isoformat()

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved updated data to {OUTPUT_PATH}")

    if not stats_df.empty:
        print("\nBackfill summary:")
        print(stats_df[["date", "name", "symbol", "status", "error"]].to_string(index=False))

    return 0
