# Blue Owls Mock Data API

FastAPI application serving the Olist Brazilian E-Commerce dataset through authenticated, paginated REST endpoints. Simulates real-world API conditions by injecting HTTP failures at a configurable rate.

---

## Setup

### 1. Create and activate a virtual environment

```bash
cd mock-api
python -m venv .venv

# macOS / Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the server

```bash
fastapi dev app/main.py
```

The API will be available at `http://localhost:8000`.

Interactive docs: `http://localhost:8000/docs`

---

## Configuration

Copy `.env.example` to `.env` and adjust as needed:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `API_PORT` | `8000` | Port the API runs on |
| `TOKEN_EXPIRY_MINUTES` | `15` | Token lifetime in minutes |
| `FAILURE_RATE` | `0.12` | Probability of injecting a failure per request (0.0–1.0) |
| `FAILURE_500_WEIGHT` | `0.6` | Proportion of failures that are 500s |
| `FAILURE_429_WEIGHT` | `0.4` | Proportion of failures that are 429s |
| `RATE_LIMIT_RETRY_AFTER` | `2` | Seconds in 429 `Retry-After` header |
| `DEFAULT_PAGE_SIZE` | `1000` | Default records per page |
| `MAX_PAGE_SIZE` | `5000` | Maximum allowed page size |
| `API_USERNAME` | `candidate` | Auth username |
| `API_PASSWORD` | `blue-owls-2026` | Auth password |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `DATA_DIR` | `./data` | Path to CSV directory |
| `JWT_SECRET` | *(see .env.example)* | JWT signing secret — change in production |

### Evaluator tuning

```bash
# Harder for senior candidates
FAILURE_RATE=0.25

# Force frequent re-authentication
TOKEN_EXPIRY_MINUTES=5

# Test patience in backoff logic
RATE_LIMIT_RETRY_AFTER=10

# Disable chaos for debugging
FAILURE_RATE=0.0
```

---

## Endpoints

### Authentication

```
POST /api/v1/auth/token
```

```json
{
  "username": "candidate",
  "password": "blue-owls-2026"
}
```

Returns a JWT Bearer token valid for `TOKEN_EXPIRY_MINUTES` minutes. This endpoint is **never** subject to failure injection.

### Data

All data endpoints require a valid `Authorization: Bearer <token>` header.

```
GET /api/v1/data/{endpoint}?page=1&page_size=1000
```

Where `{endpoint}` is one of: `orders`, `order_items`, `customers`, `products`, `sellers`, `payments`, `reviews`, `geolocation`.

### Schema (no auth required)

```
GET /api/v1/data/schema/{endpoint}
```

Returns column names, a sample record, and total record count. Useful for inspecting data shape before building your pipeline.

### Health (no auth required)

```
GET /api/v1/health
```

---

## Failure Injection

The chaos middleware intercepts every authenticated `/data` request and rolls against `FAILURE_RATE`:

- **500 Internal Server Error** — `{"detail": "Internal server error. Please retry."}`
- **429 Too Many Requests** — `{"detail": "Rate limit exceeded..."}` with `Retry-After` header

Auth validation always runs before chaos, so expired/invalid tokens always produce a clean 401 rather than a random failure.

---

## Docker

```bash
docker-compose up --build
```
