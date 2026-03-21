"""Ценовая аналитика — эластичность и прогноз."""
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.catalog import Channel, ChannelType
from app.models.user import User
from app.services.elasticity_service import (
    get_elasticity_dashboard,
    get_price_optimization,
    get_sku_unit_economics,
    calculate_elasticity,
    forecast_scenario,
    _current_stock,
)

router = APIRouter()


@router.get("/dashboard")
def elasticity_dashboard(
    channel: str = Query("wb"),
    limit: int = Query(20),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return get_elasticity_dashboard(db, channel=channel, limit=limit)


@router.get("/sku/{sku_id}")
def sku_elasticity(
    sku_id: int,
    channel: str = Query("wb"),
    spp_pct: Optional[float] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    ch = db.query(Channel).filter(
        Channel.type == (ChannelType.WB if channel == "wb" else ChannelType.OZON)
    ).first()
    if not ch:
        return {"error": "Канал не найден"}
    return get_price_optimization(db, sku_id, ch.id, spp_pct=spp_pct)


@router.get("/forecast")
def price_forecast(
    sku_id: int,
    new_price: float,
    channel: str = Query("wb"),
    spp_pct: Optional[float] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    ch = db.query(Channel).filter(
        Channel.type == (ChannelType.WB if channel == "wb" else ChannelType.OZON)
    ).first()
    if not ch:
        return {"error": "Канал не найден"}

    unit_econ = get_sku_unit_economics(db, sku_id, ch.id)
    elast = calculate_elasticity(db, sku_id, ch.id)

    if spp_pct is None:
        spp_pct = unit_econ["spp_pct"]

    stock = _current_stock(db, sku_id)
    return forecast_scenario(unit_econ, elast["elasticity"], new_price, spp_pct, stock)
