# Crypto Analytics Data Pipeline

[![Update CoinGecko Data](https://github.com/M1ck3yJ0/crypto-data-pipeline/actions/workflows/update_coingecko.yml/badge.svg)]
[![Retry Missing Rows](https://github.com/M1ck3yJ0/crypto-data-pipeline/actions/workflows/retry_missing.yml/badge.svg)]

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

```mermaid
flowchart TD
    classDef api fill:#ffffff,stroke:#444444,stroke-width:2px,color:#212529
    classDef action fill:#f0f0f0,stroke:#666666,stroke-width:1.5px,color:#212529,rx:12,ry:12
    classDef storage fill:#d9d9d9,stroke:#444444,stroke-width:2px,color:#212529
    classDef output fill:#ffffff,stroke:#444444,stroke-width:2px,color:#212529
    classDef label fill:#e8e8e8,stroke:#e8e8e8,color:#555555,font-size:12px

    A([CoinGecko API]):::api

    subgraph GHA1 ["GitHub Actions: Daily Workflow"]
        B["Fetch market data\nfor top 50 coins"]:::action
    end

    subgraph GHA2 ["GitHub Actions: Retry Workflow"]
        E["Read missing queue\nAttempt backfill"]:::action
    end

    A -->|runs daily after UTC midnight| B
    B -->|  success  | C[(coingecko_markets.csv)]:::storage
    B -->|  failure  | D[(missing_queue.csv)]:::storage
    D -->|  triggers retry  | E
    E -->|  resolved  | C
    E -->|  still failing  | D
    C -->|  feeds  | F([Power BI Dashboard]):::output
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
```

---

## Acknowledgements

This project was developed with assistance from ChatGPT, 
which supported rapid code development/debugging and README drafting.
All analytical decisions, pipeline choices, and results interpretation are the author's own.
