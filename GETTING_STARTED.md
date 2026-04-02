# Getting Started

This guide walks you through setting up your local environment to work on the assessment. You do not need to install Python, Java, or Spark manually — everything runs inside Docker.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running

That's it.

---

## Starting the Environment

From the root of this repository, run:

```bash
docker-compose up
```

This starts two services:

| Service | What it does | URL |
|---|---|---|
| `mock-api` | The Blue Owls Data API | http://localhost:8000 |
| `notebook` | Jupyter Lab with PySpark | http://localhost:8888 |

The notebook service will not start until the API is healthy, so both will be ready by the time Jupyter opens.

To stop everything:

```bash
docker-compose down
```

---

## Opening Jupyter Lab

Once the containers are running, open **http://localhost:8888** in your browser.

Your work lives in the `submission/` folder in this repo. Inside Jupyter, this maps to the `work/` folder — **create all your notebooks inside `work/`** and they will persist on your machine after the containers stop.

Output files (Bronze / Silver / Gold CSVs) should be written to `work/output/`, which maps to `submission/output/` in the repo.

---

## Starting a PySpark Session

At the top of every notebook, initialise a Spark session before doing anything else:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("blue-owls-assessment") \
    .getOrCreate()

spark.version  # confirm Spark is running
```

`local[*]` runs Spark locally using all available CPU cores — no cluster required.

---

## Connecting to the API

The API is reachable from inside the notebook container at `http://mock-api:8000`. An environment variable is pre-configured for you:

```python
import os

API_BASE_URL = os.environ.get("API_BASE_URL", "http://mock-api:8000")
API_USERNAME = os.environ.get("API_USERNAME", "candidate")
API_PASSWORD = os.environ.get("API_PASSWORD", "blue-owls-2026")
```

To get a token and make your first request:

```python
import requests

# Authenticate
resp = requests.post(f"{API_BASE_URL}/api/v1/auth/token", json={
    "username": API_USERNAME,
    "password": API_PASSWORD
})
token = resp.json()["access_token"]

# Fetch first page of orders
headers = {"Authorization": f"Bearer {token}"}
data = requests.get(f"{API_BASE_URL}/api/v1/data/orders", headers=headers, params={"page": 1, "page_size": 1000})
print(data.json()["pagination"])
```

To inspect the shape of any endpoint without authenticating:

```bash
curl http://localhost:8000/api/v1/data/schema/orders
```

---

## SQL Queries

Save each SQL query as a separate file under `submission/sql/`:

```
submission/
└── sql/
    ├── query_1.sql    # Revenue Trend Analysis
    └── query_2.sql    # Seller Performance Scorecard
```

Each file should contain a single query written against the Gold layer table names
(`fact_order_items`, `dim_customers`, `dim_products`, `dim_sellers`).

---

## Output Structure

Write your pipeline outputs to these paths inside the notebook container:

```
/home/jovyan/work/output/
├── bronze/    # Raw API responses — one CSV per endpoint
├── silver/    # Cleaned & typed tables
└── gold/      # Star schema tables
```

These map to `submission/output/bronze/`, `submission/output/silver/`, and `submission/output/gold/` on your machine.

---

## Troubleshooting

**Jupyter won't open** — the notebook container waits for the API healthcheck to pass. Wait 20–30 seconds after running `docker-compose up` and try again.

**Can't reach the API from the notebook** — use `http://mock-api:8000`, not `http://localhost:8000`. `localhost` inside the container refers to the container itself, not your machine.

**Token expired mid-run** — tokens expire after 15 minutes by default. This is intentional — your pipeline should handle 401 responses by re-authenticating automatically.

**API returning 500 or 429 errors** — also intentional. See the assessment README for details on the failure injection behaviour and what your pipeline is expected to handle.
