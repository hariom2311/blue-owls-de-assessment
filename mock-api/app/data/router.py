import math

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.config import settings
from app.data.loader import get_available_endpoints, get_data
from app.data.models import (
    Customer,
    DataResponse,
    Geolocation,
    Order,
    OrderItem,
    PaginationInfo,
    Payment,
    Product,
    Review,
    SchemaResponse,
    Seller,
)

router = APIRouter()


# ── Shared helpers ────────────────────────────────────────────────────────────

def _apply_date_filter(
    data: list[dict],
    date_field: str,
    date_from: str | None,
    date_to: str | None,
) -> list[dict]:
    """Filter records by a date field using ISO string prefix comparison (YYYY-MM-DD)."""
    if not date_from and not date_to:
        return data
    result = []
    for row in data:
        val = (row.get(date_field) or "")[:10]  # take YYYY-MM-DD prefix
        if not val:
            continue
        if date_from and val < date_from:
            continue
        if date_to and val > date_to:
            continue
        result.append(row)
    return result


def _paginate(
    endpoint: str,
    page: int,
    page_size: int | None,
    date_field: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> DataResponse | JSONResponse:
    data = get_data(endpoint)
    if data is None:
        available = ", ".join(get_available_endpoints())
        return JSONResponse(
            status_code=404,
            content={"detail": f"Endpoint '{endpoint}' not found. Available: {available}"},
        )

    if date_field:
        data = _apply_date_filter(data, date_field, date_from, date_to)

    effective_size = min(
        page_size if page_size is not None else settings.default_page_size,
        settings.max_page_size,
    )
    total = len(data)
    total_pages = max(1, math.ceil(total / effective_size))
    start = (page - 1) * effective_size

    return DataResponse(
        data=data[start : start + effective_size],
        pagination=PaginationInfo(
            page=page,
            page_size=effective_size,
            total_records=total,
            total_pages=total_pages,
            has_next=page < total_pages,
        ),
    )


# ── Schema (no auth) ──────────────────────────────────────────────────────────

@router.get("/schema/{endpoint}", response_model=SchemaResponse, tags=["schema"])
async def get_schema(endpoint: str):
    """Column names and a sample record for any endpoint. No authentication required."""
    data = get_data(endpoint)
    if data is None:
        available = ", ".join(get_available_endpoints())
        return JSONResponse(
            status_code=404,
            content={"detail": f"Endpoint '{endpoint}' not found. Available: {available}"},
        )
    return SchemaResponse(
        endpoint=endpoint,
        columns=list(data[0].keys()) if data else [],
        sample_record=data[0] if data else None,
        total_records=len(data),
    )


# ── Data endpoints ────────────────────────────────────────────────────────────

@router.get("/orders", response_model=DataResponse[Order])
async def get_orders(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=None),
    date_from: str | None = Query(default=None, description="Filter by order_purchase_timestamp ≥ YYYY-MM-DD"),
    date_to: str | None = Query(default=None, description="Filter by order_purchase_timestamp ≤ YYYY-MM-DD"),
):
    return _paginate("orders", page, page_size, "order_purchase_timestamp", date_from, date_to)


@router.get("/order_items", response_model=DataResponse[OrderItem])
async def get_order_items(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=None),
    date_from: str | None = Query(default=None, description="Filter by shipping_limit_date ≥ YYYY-MM-DD"),
    date_to: str | None = Query(default=None, description="Filter by shipping_limit_date ≤ YYYY-MM-DD"),
):
    return _paginate("order_items", page, page_size, "shipping_limit_date", date_from, date_to)


@router.get("/customers", response_model=DataResponse[Customer])
async def get_customers(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=None),
):
    return _paginate("customers", page, page_size)


@router.get("/products", response_model=DataResponse[Product])
async def get_products(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=None),
):
    return _paginate("products", page, page_size)


@router.get("/sellers", response_model=DataResponse[Seller])
async def get_sellers(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=None),
):
    return _paginate("sellers", page, page_size)


@router.get("/payments", response_model=DataResponse[Payment])
async def get_payments(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=None),
):
    return _paginate("payments", page, page_size)


@router.get("/reviews", response_model=DataResponse[Review])
async def get_reviews(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=None),
    date_from: str | None = Query(default=None, description="Filter by review_creation_date ≥ YYYY-MM-DD"),
    date_to: str | None = Query(default=None, description="Filter by review_creation_date ≤ YYYY-MM-DD"),
):
    return _paginate("reviews", page, page_size, "review_creation_date", date_from, date_to)


@router.get("/geolocation", response_model=DataResponse[Geolocation])
async def get_geolocation(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=None),
):
    return _paginate("geolocation", page, page_size)
