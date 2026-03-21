from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.catalog import SKUChannel
from app.models.user import User
from app.schemas.rnp import RnPDailyResponse
from app.services.rnp_service import get_rnp_daily
from app.services.rnp_pivot_service import get_rnp_pivot

router = APIRouter()


class SKUOverrideUpdate(BaseModel):
    sku_id: int
    channel_id: int
    # None = сбросить на расчётное значение
    buyout_rate_pct: Optional[float] = Field(None, ge=0, le=100)
    logistics_rub: Optional[float] = Field(None, ge=0)
    # Комиссия + эквайринг % от цены до СПП (из финотчёта WB)
    # Формула: (Реализация - К_перечислению) / Реализация * 100
    commission_pct: Optional[float] = Field(None, ge=0, le=100)


@router.get("/daily", response_model=RnPDailyResponse)
def rnp_daily(
    channel_type: Optional[str] = Query(None, description="wb | ozon"),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return get_rnp_daily(db, channel_type=channel_type)


@router.get("/pivot")
def rnp_pivot(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    days: int = Query(7, ge=1, le=90, description="Кол-во дней (фолбэк если не заданы даты)"),
    channels: Optional[List[str]] = Query(None, description="wb | ozon (множественный, ?channels=wb&channels=ozon)"),
    article: Optional[str] = Query(None, description="Фильтр по артикулу (частичный поиск)"),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Pivot-данные РнП: каждый SKU × каждый день (формат WB Аналитики)."""
    return get_rnp_pivot(db, date_from=date_from, date_to=date_to, days=days, channels=channels, article=article)


@router.patch("/sku-overrides")
def update_sku_overrides(
    payload: SKUOverrideUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """
    Установить или сбросить ручные значения для SKU × канал:
    - buyout_rate_pct: % выкупа (null = сбросить на расчётный)
    - logistics_rub: логистика ₽/ед (null = сбросить на расчётный)
    - commission_pct: комиссия + эквайринг % от цены до СПП (null = сбросить)
    """
    sc = (
        db.query(SKUChannel)
        .filter(
            SKUChannel.sku_id == payload.sku_id,
            SKUChannel.channel_id == payload.channel_id,
        )
        .first()
    )
    if not sc:
        raise HTTPException(status_code=404, detail="SKU-Channel not found")

    # buyout_rate: передаём ключ = обновляем; не передаём = не трогаем
    if "buyout_rate_pct" in payload.model_fields_set:
        sc.buyout_rate_override = (
            None if payload.buyout_rate_pct is None
            else round(payload.buyout_rate_pct / 100, 4)
        )

    if "logistics_rub" in payload.model_fields_set:
        sc.logistics_override = payload.logistics_rub  # None или float

    if "commission_pct" in payload.model_fields_set:
        sc.commission_pct_override = payload.commission_pct  # None или float

    db.commit()
    return {
        "sku_id": payload.sku_id,
        "channel_id": payload.channel_id,
        "buyout_rate_override": float(sc.buyout_rate_override) * 100 if sc.buyout_rate_override else None,
        "buyout_rate_is_manual": sc.buyout_rate_override is not None,
        "logistics_override": float(sc.logistics_override) if sc.logistics_override else None,
        "logistics_is_manual": sc.logistics_override is not None,
        "commission_pct_override": float(sc.commission_pct_override) if sc.commission_pct_override else None,
        "commission_is_manual": sc.commission_pct_override is not None,
    }
