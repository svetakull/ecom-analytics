from typing import Optional

from pydantic import BaseModel


class SKUOut(BaseModel):
    id: int
    seller_article: str
    name: str
    category: Optional[str]
    brand: Optional[str]
    color: Optional[str]
    is_active: bool
    channels: list[str]
    total_stock: int
    avg_cogs: float

    model_config = {"from_attributes": True}


class SKUDetail(SKUOut):
    batches_count: int
    last_batch_cost: float
    turnover_days: float
    monthly_orders: int
    monthly_sales_rub: float
