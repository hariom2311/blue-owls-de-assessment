import csv
import logging
from pathlib import Path

from app.config import settings

logger = logging.getLogger("mock-api")

ENDPOINT_FILE_MAP: dict[str, str] = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
}

_cache: dict[str, list[dict]] = {}


def load_all() -> None:
    """Load all Olist CSVs into memory at startup. Raises if any file is missing."""
    data_dir = Path(settings.data_dir)
    for endpoint, filename in ENDPOINT_FILE_MAP.items():
        filepath = data_dir / filename
        if not filepath.exists():
            raise FileNotFoundError(
                f"Required data file not found: {filepath}\n"
                f"Run `python scripts/download_data.py` to fetch the Olist dataset."
            )

        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            records = [dict(row) for row in reader]

        _cache[endpoint] = records
        logger.info(f"Loaded {len(records):,} records for endpoint '{endpoint}'")

    logger.info("All datasets loaded and cached.")


def get_data(endpoint: str) -> list[dict] | None:
    """Return cached records for an endpoint, or None if endpoint is unknown."""
    return _cache.get(endpoint)


def get_available_endpoints() -> list[str]:
    return list(ENDPOINT_FILE_MAP.keys())
