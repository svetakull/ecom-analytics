"""ДДС — Отчёт о движении денежных средств (Cash Flow Statement)."""
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.finance import DDSBalance, DDSManualEntry
from app.models.user import User
from app.services.dds_service import get_dds

router = APIRouter()


# --- Schemas ---

class DDSManualEntryCreate(BaseModel):
    date: date
    category: str
    name: str
    amount: float
    section: str = "operating"
    channel_id: Optional[int] = None


class DDSManualEntryUpdate(BaseModel):
    date: Optional[date] = None
    category: Optional[str] = None
    name: Optional[str] = None
    amount: Optional[float] = None
    section: Optional[str] = None
    channel_id: Optional[int] = None


class DDSBalanceCreate(BaseModel):
    date: date
    account_name: str
    amount: float


# --- Endpoints ---

@router.get("")
def dds_report(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    channels: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    if date_from is None:
        date_from = date(date.today().year, 1, 1)
    if date_to is None:
        date_to = date.today()
    return get_dds(db, date_from=date_from, date_to=date_to, channels=channels)


# --- Manual entries CRUD ---

@router.get("/manual")
def list_manual_entries(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    category: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = db.query(DDSManualEntry)
    if date_from:
        q = q.filter(DDSManualEntry.date >= date_from)
    if date_to:
        q = q.filter(DDSManualEntry.date <= date_to)
    if category:
        q = q.filter(DDSManualEntry.category == category)
    entries = q.order_by(DDSManualEntry.date.desc(), DDSManualEntry.id.desc()).all()
    return [
        {
            "id": e.id,
            "date": e.date.isoformat() if e.date else None,
            "category": e.category,
            "name": e.name,
            "amount": float(e.amount) if e.amount else 0,
            "section": e.section,
            "channel_id": e.channel_id,
            "created_by": e.created_by,
            "created_at": e.created_at.isoformat() if e.created_at else None,
            "updated_at": e.updated_at.isoformat() if e.updated_at else None,
        }
        for e in entries
    ]


@router.post("/manual")
def create_manual_entry(
    body: DDSManualEntryCreate,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    entry = DDSManualEntry(
        date=body.date,
        category=body.category,
        name=body.name,
        amount=body.amount,
        section=body.section,
        channel_id=body.channel_id,
        created_by=user.id,
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
    return {
        "id": entry.id,
        "date": entry.date.isoformat() if entry.date else None,
        "category": entry.category,
        "name": entry.name,
        "amount": float(entry.amount) if entry.amount else 0,
        "section": entry.section,
        "channel_id": entry.channel_id,
    }


@router.put("/manual/{entry_id}")
def update_manual_entry(
    entry_id: int,
    body: DDSManualEntryUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    entry = db.query(DDSManualEntry).filter(DDSManualEntry.id == entry_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="Entry not found")
    if body.date is not None:
        entry.date = body.date
    if body.category is not None:
        entry.category = body.category
    if body.name is not None:
        entry.name = body.name
    if body.amount is not None:
        entry.amount = body.amount
    if body.section is not None:
        entry.section = body.section
    if body.channel_id is not None:
        entry.channel_id = body.channel_id
    db.commit()
    db.refresh(entry)
    return {
        "id": entry.id,
        "date": entry.date.isoformat() if entry.date else None,
        "category": entry.category,
        "name": entry.name,
        "amount": float(entry.amount) if entry.amount else 0,
        "section": entry.section,
        "channel_id": entry.channel_id,
    }


@router.delete("/manual/{entry_id}")
def delete_manual_entry(
    entry_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    entry = db.query(DDSManualEntry).filter(DDSManualEntry.id == entry_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="Entry not found")
    db.delete(entry)
    db.commit()
    return {"ok": True, "deleted_id": entry_id}


# --- Balances CRUD ---

@router.get("/balances")
def list_balances(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = db.query(DDSBalance)
    if date_from:
        q = q.filter(DDSBalance.date >= date_from)
    if date_to:
        q = q.filter(DDSBalance.date <= date_to)
    rows = q.order_by(DDSBalance.date.desc(), DDSBalance.id.desc()).all()
    return [
        {
            "id": r.id,
            "date": r.date.isoformat() if r.date else None,
            "account_name": r.account_name,
            "amount": float(r.amount) if r.amount else 0,
            "created_by": r.created_by,
        }
        for r in rows
    ]


@router.post("/balances")
def create_balance(
    body: DDSBalanceCreate,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    entry = DDSBalance(
        date=body.date,
        account_name=body.account_name,
        amount=body.amount,
        created_by=user.id,
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
    return {
        "id": entry.id,
        "date": entry.date.isoformat() if entry.date else None,
        "account_name": entry.account_name,
        "amount": float(entry.amount) if entry.amount else 0,
    }


@router.delete("/balances/{balance_id}")
def delete_balance(
    balance_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    entry = db.query(DDSBalance).filter(DDSBalance.id == balance_id).first()
    if not entry:
        raise HTTPException(status_code=404, detail="Balance entry not found")
    db.delete(entry)
    db.commit()
    return {"ok": True, "deleted_id": balance_id}
