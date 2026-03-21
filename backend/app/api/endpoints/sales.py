from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.user import User
from app.schemas.sales import OrderOut, SalesDynamicPoint, SalesSummary
from app.services.sales_service import get_orders, get_sales_dynamic, get_sales_summary

router = APIRouter()


@router.get("/orders", response_model=list[OrderOut])
def orders(
    channel_type: Optional[str] = Query(None),
    sku_id: Optional[int] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return get_orders(db, channel_type, sku_id, date_from, date_to, limit, offset)


@router.get("/summary", response_model=SalesSummary)
def sales_summary(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    channel_type: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return get_sales_summary(db, date_from, date_to, channel_type)


@router.get("/dynamic", response_model=list[SalesDynamicPoint])
def sales_dynamic(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    channel_type: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return get_sales_dynamic(db, date_from, date_to, channel_type)
