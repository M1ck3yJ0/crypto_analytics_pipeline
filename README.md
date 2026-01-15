# Crypto Analytics 
# Data Pipeline

[![Update CoinGecko Data](https://github.com/M1ck3yJ0/crypto_analytics_pipeline/actions/workflows/update_coingecko.yml/badge.svg)]

[![Retry Missing Rows](https://github.com/M1ck3yJ0/crypto_analytics_pipeline/actions/workflows/retry_missing.yml/badge.svg)]

An automated, fault-tolerant cryptocurrency data pipeline that ingests daily market data from the CoinGecko API and feeds a Power BI analytics dashboard.

This project focuses on robust pipeline design rather than simple data extraction.

---

## Overview

- **Source:** CoinGecko API
- **Universe:** Fixed top 50 cryptocurrencies as of 2025-12-01
- **Storage:** Version-controlled CSV dataset (GitHub)
- **Orchestration:** GitHub Actions
- **Analytics:** Power BI

---

## Architecture

```text

CoinGecko API
   |
   v
[Daily Pull Workflow (GitHub Actions)]
   |--> data/coingecko_markets.csv   (append + dedupe by id+date)
   |--> data/missing_queue.csv       (only failures)
   |
   v
[Retry Worker Workflow (GitHub Actions)]
   |--> reads: data/missing_queue.csv
   |--> writes: data/coingecko_markets.csv (fills gaps)
   |--> updates: data/missing_queue.csv    (removes/marks resolved)
   |
   v
[Power BI Dashboard]

```
---

## Pipeline Design

- Runs daily after UTC midnight
- Stores one row per coin per day (date-level grain)
- Normalises timestamps for clean analytics
- Safe to re-run without duplicating data
- Automatically recomputes rolling returns (1d / 7d / 30d)

---

## Failure Handling

- API failures are written to a durable retry queue
- A separate retry workflow runs independently
- Missing data is backfilled automatically once available
- The pipeline favours eventual consistency over hard failure

---

## Repository Structure

```text
config/   # Fixed universe definition
data/     # Fact table and retry queue
scripts/  # Ingestion and retry logic
.github/  # GitHub Actions workflows
