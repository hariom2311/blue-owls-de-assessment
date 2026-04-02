from typing import Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


# ── Pagination ────────────────────────────────────────────────────────────────

class PaginationInfo(BaseModel):
    page: int
    page_size: int
    total_records: int
    total_pages: int
    has_next: bool


class DataResponse(BaseModel, Generic[T]):
    data: list[T]
    pagination: PaginationInfo


# ── Record models (all fields str | None — API serves raw CSV strings) ────────

class Order(BaseModel):
    order_id: str | None = None
    customer_id: str | None = None
    order_status: str | None = None
    order_purchase_timestamp: str | None = None
    order_approved_at: str | None = None
    order_delivered_carrier_date: str | None = None
    order_delivered_customer_date: str | None = None
    order_estimated_delivery_date: str | None = None


class OrderItem(BaseModel):
    order_id: str | None = None
    order_item_id: str | None = None
    product_id: str | None = None
    seller_id: str | None = None
    shipping_limit_date: str | None = None
    price: str | None = None
    freight_value: str | None = None


class Customer(BaseModel):
    customer_id: str | None = None
    customer_unique_id: str | None = None
    customer_zip_code_prefix: str | None = None
    customer_city: str | None = None
    customer_state: str | None = None


class Product(BaseModel):
    product_id: str | None = None
    product_category_name: str | None = None
    product_name_lenght: str | None = None        # sic — typo is in the source dataset
    product_description_lenght: str | None = None  # sic — typo is in the source dataset
    product_photos_qty: str | None = None
    product_weight_g: str | None = None
    product_length_cm: str | None = None
    product_height_cm: str | None = None
    product_width_cm: str | None = None


class Seller(BaseModel):
    seller_id: str | None = None
    seller_zip_code_prefix: str | None = None
    seller_city: str | None = None
    seller_state: str | None = None


class Payment(BaseModel):
    order_id: str | None = None
    payment_sequential: str | None = None
    payment_type: str | None = None
    payment_installments: str | None = None
    payment_value: str | None = None


class Review(BaseModel):
    review_id: str | None = None
    order_id: str | None = None
    review_score: str | None = None
    review_comment_title: str | None = None
    review_comment_message: str | None = None
    review_creation_date: str | None = None
    review_answer_timestamp: str | None = None


class Geolocation(BaseModel):
    geolocation_zip_code_prefix: str | None = None
    geolocation_lat: str | None = None
    geolocation_lng: str | None = None
    geolocation_city: str | None = None
    geolocation_state: str | None = None


# ── Schema endpoint response ───────────────────────────────────────────────────

class SchemaResponse(BaseModel):
    endpoint: str
    columns: list[str]
    sample_record: dict | None
    total_records: int
