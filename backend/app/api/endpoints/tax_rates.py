"""Налоговые ставки по периодам и каналам."""
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.finance import TaxRate
from app.models.catalog import Channel
from app.models.user import User

router = APIRouter()


class TaxRateItem(BaseModel):
    year: int
    month: Optional[int] = None
    quarter: Optional[int] = None
    channel_id: Optional[int] = None
    usn_pct: float = 0
    nds_pct: float = 0


class TaxRatesBulkUpdate(BaseModel):
    items: List[TaxRateItem]


@router.get("")
def list_tax_rates(
    year: Optional[int] = Query(None),
    channel_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = db.query(TaxRate)
    if year is not None:
        q = q.filter(TaxRate.year == year)
    if channel_id is not None:
        q = q.filter(TaxRate.channel_id == channel_id)
    rows = q.order_by(TaxRate.year, TaxRate.channel_id, TaxRate.quarter, TaxRate.month).all()
    return [
        {
            "id": r.id,
            "year": r.year,
            "month": r.month,
            "quarter": r.quarter,
            "channel_id": r.channel_id,
            "usn_pct": float(r.usn_pct or 0),
            "nds_pct": float(r.nds_pct or 0),
        }
        for r in rows
    ]


@router.post("/bulk")
def bulk_update_tax_rates(
    payload: TaxRatesBulkUpdate,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Массовое обновление ставок: upsert по (year, month, quarter, channel_id)."""
    for item in payload.items:
        existing = (
            db.query(TaxRate)
            .filter(
                TaxRate.year == item.year,
                TaxRate.month.is_(item.month) if item.month is None else TaxRate.month == item.month,
                TaxRate.quarter.is_(item.quarter) if item.quarter is None else TaxRate.quarter == item.quarter,
                TaxRate.channel_id.is_(item.channel_id) if item.channel_id is None else TaxRate.channel_id == item.channel_id,
            )
            .first()
        )
        if existing:
            existing.usn_pct = item.usn_pct
            existing.nds_pct = item.nds_pct
        else:
            db.add(TaxRate(
                year=item.year,
                month=item.month,
                quarter=item.quarter,
                channel_id=item.channel_id,
                usn_pct=item.usn_pct,
                nds_pct=item.nds_pct,
                created_by=user.id,
            ))
    db.commit()
    return {"ok": True, "count": len(payload.items)}


def get_effective_tax_rates(db: Session, year: int, month: int, channel_id: Optional[int] = None) -> dict:
    """Получить эффективные ставки УСН/НДС для данного месяца и канала.
    Приоритет: месяц(канал) → месяц(все) → квартал(канал) → квартал(все) → год(канал) → год(все) → 0.
    """
    quarter = (month - 1) // 3 + 1

    def _find(m: Optional[int], q: Optional[int], ch: Optional[int]) -> Optional[TaxRate]:
        query = db.query(TaxRate).filter(TaxRate.year == year)
        query = query.filter(TaxRate.month.is_(m) if m is None else TaxRate.month == m)
        query = query.filter(TaxRate.quarter.is_(q) if q is None else TaxRate.quarter == q)
        query = query.filter(TaxRate.channel_id.is_(ch) if ch is None else TaxRate.channel_id == ch)
        return query.first()

    candidates = []
    if channel_id is not None:
        candidates += [(month, None, channel_id), (None, quarter, channel_id), (None, None, channel_id)]
    candidates += [(month, None, None), (None, quarter, None), (None, None, None)]

    for m, q, ch in candidates:
        r = _find(m, q, ch)
        if r:
            return {"usn_pct": float(r.usn_pct or 0), "nds_pct": float(r.nds_pct or 0)}
    return {"usn_pct": 0.0, "nds_pct": 0.0}
