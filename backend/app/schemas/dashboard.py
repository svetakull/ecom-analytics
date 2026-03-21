from pydantic import BaseModel


class KPICard(BaseModel):
    title: str
    value: float
    unit: str
    trend_pct: float
    trend_direction: str  # up / down / flat


class StockAlert(BaseModel):
    sku_id: int
    seller_article: str
    name: str
    channel: str
    stock_qty: int
    turnover_days: float


class DashboardResponse(BaseModel):
    orders_today: KPICard
    revenue_today: KPICard
    margin_avg: KPICard
    tacos_avg: KPICard
    stock_alerts: list[StockAlert]
    sales_chart: list[dict]
