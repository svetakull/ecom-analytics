from datetime import date
from typing import Optional

from pydantic import BaseModel


class RnPSKURow(BaseModel):
    sku_id: int
    seller_article: str
    name: str
    channel: str
    channel_type: str

    # Продажи
    orders_qty: int
    orders_rub: float
    sales_forecast_qty: float
    sales_forecast_rub: float
    buyout_rate_pct: float

    # Цены
    price_before_spp: float
    price_after_spp: float
    spp_pct: float

    # Остатки
    stock_qty: int
    turnover_days: float

    # Экономика
    margin_forecast_pct: float
    gross_margin_per_unit: float
    gross_margin_rub: float

    # Реклама
    tacos: float
    ad_spend: float


class RnPDailyResponse(BaseModel):
    date: date
    total_orders_qty: int
    total_orders_rub: float
    total_margin_pct: float
    total_tacos: float
    rows: list[RnPSKURow]


class RnPFilters(BaseModel):
    channel_type: Optional[str] = None
    date_from: Optional[date] = None
    date_to: Optional[date] = None
