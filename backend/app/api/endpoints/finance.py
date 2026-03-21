"""Финансы: ОПиУ (PnL), история себестоимости."""
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.catalog import SKU
from app.models.finance import PnLRecord
from app.models.inventory import SKUCostHistory
from app.models.user import User

router = APIRouter()


# ── Схемы ────────────────────────────────────────────────────────────────────

class PnLRecordOut(BaseModel):
    id: int
    period: str
    line_item: str
    parent_line: Optional[str]
    amount: float
    pct_of_revenue: Optional[float]
    sort_order: int

    class Config:
        from_attributes = True


class CostHistoryIn(BaseModel):
    sku_id: int
    effective_from: date
    cost_per_unit: float = Field(gt=0)
    comment: Optional[str] = None


class CostHistoryOut(BaseModel):
    id: int
    sku_id: int
    seller_article: str
    effective_from: date
    cost_per_unit: float
    comment: Optional[str]

    class Config:
        from_attributes = True


# ── ОПиУ ─────────────────────────────────────────────────────────────────────

@router.get("/pnl", response_model=List[PnLRecordOut])
def get_pnl(
    period: Optional[str] = None,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Получить строки ОПиУ. period = 'YYYY-MM' или 'YYYY' или None (все)."""
    q = db.query(PnLRecord).order_by(PnLRecord.period.desc(), PnLRecord.sort_order)
    if period:
        q = q.filter(PnLRecord.period == period)
    return q.all()


@router.get("/pnl/periods")
def get_pnl_periods(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Список доступных периодов ОПиУ."""
    from sqlalchemy import distinct
    periods = db.query(distinct(PnLRecord.period)).order_by(PnLRecord.period.desc()).all()
    return [p[0] for p in periods]


# ── История себестоимости ─────────────────────────────────────────────────────

@router.get("/cost-history", response_model=List[CostHistoryOut])
def get_cost_history(
    sku_id: Optional[int] = None,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = (
        db.query(SKUCostHistory, SKU.seller_article)
        .join(SKU, SKU.id == SKUCostHistory.sku_id)
        .order_by(SKUCostHistory.sku_id, SKUCostHistory.effective_from.desc())
    )
    if sku_id:
        q = q.filter(SKUCostHistory.sku_id == sku_id)
    rows = q.all()
    return [
        CostHistoryOut(
            id=r.SKUCostHistory.id,
            sku_id=r.SKUCostHistory.sku_id,
            seller_article=r.seller_article,
            effective_from=r.SKUCostHistory.effective_from,
            cost_per_unit=float(r.SKUCostHistory.cost_per_unit),
            comment=r.SKUCostHistory.comment,
        )
        for r in rows
    ]


@router.post("/cost-history", response_model=CostHistoryOut)
def add_cost_history(
    payload: CostHistoryIn,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    sku = db.query(SKU).filter(SKU.id == payload.sku_id).first()
    if not sku:
        raise HTTPException(status_code=404, detail="SKU not found")
    record = SKUCostHistory(
        sku_id=payload.sku_id,
        effective_from=payload.effective_from,
        cost_per_unit=payload.cost_per_unit,
        comment=payload.comment,
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    return CostHistoryOut(
        id=record.id,
        sku_id=record.sku_id,
        seller_article=sku.seller_article,
        effective_from=record.effective_from,
        cost_per_unit=float(record.cost_per_unit),
        comment=record.comment,
    )


@router.delete("/cost-history/{record_id}")
def delete_cost_history(
    record_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    record = db.query(SKUCostHistory).filter(SKUCostHistory.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    db.delete(record)
    db.commit()
    return {"deleted": record_id}
