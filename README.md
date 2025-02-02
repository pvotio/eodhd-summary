# EODHD Summary
## Overview
EODHD Summary is a Python-based data pipeline that retrieves fundamental stock data, including company financials, using the EOD Historical Data API. The collected data is processed, transformed, and stored in a Microsoft SQL Server database.

## Features
- Fetches company fundamental data from EODHD API.
- Implements a retry mechanism for stable API communication.
- Utilizes multi-threading for efficient data retrieval.
- Stores structured data in a Microsoft SQL Server database.
- Supports logging and configurable settings via environment variables.
- Dockerized for easy deployment.

## Installation
### Prerequisites
- Python 3.10+
- Microsoft SQL Server
- Docker (optional, for containerized execution)

### Setup
Clone the repository:

```bash
git clone https://github.com/arqs-io/eodhd-summary.git
cd eodhd-summary
```

Install dependencies:

`pip install -r requirements.txt`

Set up environment variables:

- Copy .env.sample to .env
- Edit .env to include your database and API credentials.

Run the application:
`python main.py`

## Docker Usage

To run the application using Docker:


```bash
docker build -t eodhd-summary .
docker run --env-file .env eodhd-summary
```

## Contributing
- Fork the repository.
- Create a feature branch: git checkout -b feature-branch
- Commit changes: git commit -m "Add new feature"
- Push to the branch: git push origin feature-branch
- Open a Pull Request.