"""Управленческий баланс — API."""
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.finance import BalanceSheetManualEntry
from app.models.user import User
from app.services.balance_sheet_service import get_balance_sheet

router = APIRouter()


class ManualEntryCreate(BaseModel):
    date: date
    category: str
    name: str
    amount: float
    section: str  # assets / liabilities / equity


class ManualEntryUpdate(BaseModel):
    date: Optional[date] = None
    category: Optional[str] = None
    name: Optional[str] = None
    amount: Optional[float] = None
    section: Optional[str] = None


@router.get("")
def balance_sheet_report(
    as_of_date: Optional[date] = Query(None),
    compare_date: Optional[date] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return get_balance_sheet(db, as_of_date=as_of_date, compare_date=compare_date)


@router.get("/manual")
def list_manual(
    section: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = db.query(BalanceSheetManualEntry)
    if section:
        q = q.filter(BalanceSheetManualEntry.section == section)
    entries = q.order_by(BalanceSheetManualEntry.date.desc()).all()
    return [{"id": e.id, "date": e.date.isoformat(), "category": e.category, "name": e.name,
             "amount": float(e.amount), "section": e.section} for e in entries]


@router.post("/manual")
def create_manual(payload: ManualEntryCreate, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    entry = BalanceSheetManualEntry(
        date=payload.date, category=payload.category, name=payload.name,
        amount=payload.amount, section=payload.section, created_by=user.id,
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
    return {"id": entry.id, "ok": True}


@router.put("/manual/{entry_id}")
def update_manual(entry_id: int, payload: ManualEntryUpdate, db: Session = Depends(get_db), _: User = Depends(get_current_user)):
    entry = db.query(BalanceSheetManualEntry).get(entry_id)
    if not entry:
        raise HTTPException(404)
    for k, v in payload.dict(exclude_unset=True).items():
        setattr(entry, k, v)
    db.commit()
    return {"id": entry.id, "ok": True}


@router.delete("/manual/{entry_id}")
def delete_manual(entry_id: int, db: Session = Depends(get_db), _: User = Depends(get_current_user)):
    entry = db.query(BalanceSheetManualEntry).get(entry_id)
    if not entry:
        raise HTTPException(404)
    db.delete(entry)
    db.commit()
    return {"ok": True}
