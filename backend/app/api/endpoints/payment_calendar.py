"""Платёжный календарь — прогноз поступлений и расходов."""
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.finance import PaymentCalendarEntry
from app.models.user import User
from app.services.payment_calendar_service import get_payment_calendar

router = APIRouter()


# --- Schemas ---

class EntryCreate(BaseModel):
    entry_type: str  # inflow / outflow
    category: str
    name: str
    amount: float
    scheduled_date: date
    is_recurring: bool = False
    recurrence_rule: Optional[str] = None
    channel_id: Optional[int] = None


class EntryUpdate(BaseModel):
    entry_type: Optional[str] = None
    category: Optional[str] = None
    name: Optional[str] = None
    amount: Optional[float] = None
    scheduled_date: Optional[date] = None
    is_recurring: Optional[bool] = None
    recurrence_rule: Optional[str] = None
    channel_id: Optional[int] = None


# --- Endpoints ---

@router.get("")
def calendar_report(
    weeks_ahead: int = Query(8, ge=1, le=26),
    channels: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Платёжный календарь: факт + прогноз на N недель вперёд."""
    return get_payment_calendar(db, weeks_ahead=weeks_ahead, channels=channels)


@router.get("/entries")
def list_entries(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    category: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = db.query(PaymentCalendarEntry)
    if date_from:
        q = q.filter(PaymentCalendarEntry.scheduled_date >= date_from)
    if date_to:
        q = q.filter(PaymentCalendarEntry.scheduled_date <= date_to)
    if category:
        q = q.filter(PaymentCalendarEntry.category == category)
    entries = q.order_by(PaymentCalendarEntry.scheduled_date).all()
    return [
        {
            "id": e.id, "entry_type": e.entry_type, "category": e.category,
            "name": e.name, "amount": float(e.amount), "scheduled_date": e.scheduled_date.isoformat(),
            "is_recurring": e.is_recurring, "recurrence_rule": e.recurrence_rule,
            "channel_id": e.channel_id, "is_auto": e.is_auto, "is_confirmed": e.is_confirmed,
        }
        for e in entries
    ]


@router.post("/entries")
def create_entry(
    payload: EntryCreate,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    entry = PaymentCalendarEntry(
        entry_type=payload.entry_type, category=payload.category,
        name=payload.name, amount=payload.amount,
        scheduled_date=payload.scheduled_date,
        is_recurring=payload.is_recurring, recurrence_rule=payload.recurrence_rule,
        channel_id=payload.channel_id, created_by=user.id,
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
    return {"id": entry.id, "ok": True}


@router.put("/entries/{entry_id}")
def update_entry(
    entry_id: int,
    payload: EntryUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    entry = db.query(PaymentCalendarEntry).get(entry_id)
    if not entry:
        raise HTTPException(404, "Entry not found")
    for k, v in payload.dict(exclude_unset=True).items():
        setattr(entry, k, v)
    db.commit()
    return {"id": entry.id, "ok": True}


@router.delete("/entries/{entry_id}")
def delete_entry(
    entry_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    entry = db.query(PaymentCalendarEntry).get(entry_id)
    if not entry:
        raise HTTPException(404, "Entry not found")
    db.delete(entry)
    db.commit()
    return {"ok": True}


@router.post("/entries/{entry_id}/confirm")
def confirm_entry(
    entry_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    entry = db.query(PaymentCalendarEntry).get(entry_id)
    if not entry:
        raise HTTPException(404, "Entry not found")
    entry.is_confirmed = True
    db.commit()
    return {"id": entry.id, "is_confirmed": True}
