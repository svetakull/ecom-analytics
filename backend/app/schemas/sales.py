from datetime import date
from typing import Optional

from pydantic import BaseModel


class OrderOut(BaseModel):
    id: int
    sku_id: int
    seller_article: str
    sku_name: str
    channel: str
    order_date: date
    qty: int
    price: float
    status: str

    model_config = {"from_attributes": True}


class SalesSummary(BaseModel):
    date_from: date
    date_to: date
    total_orders_qty: int
    total_orders_rub: float
    total_sales_qty: int
    total_sales_rub: float
    total_returns_qty: int
    buyout_rate_pct: float
    avg_order_price: float


class SalesDynamicPoint(BaseModel):
    date: date
    orders_qty: int
    orders_rub: float
    sales_qty: int
    sales_rub: float


class SalesFilters(BaseModel):
    channel_type: Optional[str] = None
    sku_id: Optional[int] = None
    date_from: Optional[date] = None
    date_to: Optional[date] = None
