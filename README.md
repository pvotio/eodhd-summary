# EODHD Summary Data Pipeline

This project implements a high-throughput data ingestion and transformation pipeline using the **EOD Historical Data (EODHD) API**, focusing on fundamental financial data retrieval for large batches of tickers. It is built to support scalable and fault-tolerant data extraction and persistence into a Microsoft SQL Server database.

## Overview

### Purpose

This application automates:
- Connecting to the EODHD API using token-based authentication.
- Fetching fundamental summaries for a large universe of tickers.
- Executing threaded data retrieval with configurable batching.
- Structuring and cleaning the data using custom transformation logic.
- Efficiently inserting data into SQL Server tables with chunking.

It is designed to support investment analysts, data engineers, and financial research teams in managing up-to-date equity fundamentals.

## Source of Data

The application queries the **[EOD Historical Data API](https://eodhistoricaldata.com/)**, specifically targeting the `fundamentals/{ticker}` endpoint. This API provides detailed company financials and metadata, including:

- General profile and business description
- Financial ratios
- Dividend history
- Earnings summaries

Authentication is performed via an API token.

## Application Flow

The pipeline, managed through `main.py`, runs the following stages:

1. **Load Tickers**:
   - A SQL query retrieves the list of target tickers from the database.

2. **Batch Processing**:
   - Tickers are divided into batches of size `CLIENT_BATCH_SIZE`.
   - Each batch is processed sequentially, but internally utilizes threading.

3. **Data Fetching via Engine**:
   - The `Engine` initializes a threaded execution model.
   - For each ticker, a call is made to the EODHD `fundamentals/` endpoint.
   - Failed or missing tickers are skipped without halting the batch.

4. **Transformation**:
   - Raw data is passed into the `Agent` transformer.
   - Structured tables are derived and validated.

5. **Database Insertion**:
   - Data is inserted using `insert_table()` with chunking logic.
   - Table-specific flags avoid duplicate inserts during multi-batch runs.

## Project Structure

```
eodhd-summary-main/
├── client/               # API engine, session logic, and EODHD client
│   ├── engine.py         # Threaded processing logic
│   └── eodhd.py          # Endpoint request wrappers
├── config/               # Logging and settings loader
├── database/             # SQL Server interaction and helper functions
├── transformer/          # Transformation and cleaning layer
├── main.py               # Primary pipeline entrypoint
├── .env.sample           # Sample environment configuration
├── Dockerfile            # Container configuration
```

## Environment Variables

Copy `.env.sample` to `.env` and configure the following:

| Variable | Description |
|----------|-------------|
| `TOKEN` | EODHD API token |
| `CLIENT_BATCH_SIZE` | Number of tickers to process per batch |
| `INSERT_CHUNK_COUNT` | Number of DB chunks to split each insert into |
| `SUMMARY_OUTPUT_TABLE`, `SUMMARY_HIST_OUTPUT_TABLE` | Output SQL Server tables |
| `DB_TICKERS_QUERY` | SQL query for retrieving tickers |
| `MSSQL_*` | Server, database, username, password |
| `INSERTER_MAX_RETRIES`, `REQUEST_MAX_RETRIES`, `REQUEST_BACKOFF_FACTOR` | Retry/backoff tuning |

Use `.env` or inject via environment-secure secrets for production deployments.

## Docker Support

Containerization is supported via Docker.

### Build Image
```bash
docker build -t eodhd-summary .
```

### Run Container
```bash
docker run --env-file .env eodhd-summary
```

## Requirements

Install dependencies with:

```bash
pip install -r requirements.txt
```

Core packages include:
- `requests`: for API interaction
- `pandas`: for tabular transformations
- `fast-to-sql`: optimized SQL insertion
- `pyodbc`, `SQLAlchemy`: MSSQL integration

## Running the App

After setting up your environment, launch the pipeline:

```bash
python main.py
```

Console logs will trace batch progress, API performance, and database activity.

## License

This project is provided under the MIT License. Please consult the EODHD terms for usage limits, access control, and data entitlements.
