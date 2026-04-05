"""Кредиты: учёт тела, процентов, платежей и остатка."""
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.finance import Credit, CreditPayment
from app.models.user import User

router = APIRouter()


class CreditCreate(BaseModel):
    name: str
    bank: Optional[str] = None
    principal: float = 0
    interest_rate: Optional[float] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    monthly_payment: Optional[float] = None
    note: Optional[str] = None
    is_active: bool = True


class CreditUpdate(BaseModel):
    name: Optional[str] = None
    bank: Optional[str] = None
    principal: Optional[float] = None
    interest_rate: Optional[float] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    monthly_payment: Optional[float] = None
    note: Optional[str] = None
    is_active: Optional[bool] = None


class PaymentCreate(BaseModel):
    payment_date: date
    body_amount: float = 0
    interest_amount: float = 0
    total_amount: Optional[float] = None  # если не задано — считаем как body+interest
    balance_after: Optional[float] = None
    note: Optional[str] = None


class PaymentUpdate(BaseModel):
    payment_date: Optional[date] = None
    body_amount: Optional[float] = None
    interest_amount: Optional[float] = None
    total_amount: Optional[float] = None
    balance_after: Optional[float] = None
    note: Optional[str] = None


def _serialize_credit(c: Credit, summary: dict = None) -> dict:
    return {
        "id": c.id,
        "name": c.name,
        "bank": c.bank,
        "principal": float(c.principal or 0),
        "interest_rate": float(c.interest_rate) if c.interest_rate is not None else None,
        "start_date": c.start_date.isoformat() if c.start_date else None,
        "end_date": c.end_date.isoformat() if c.end_date else None,
        "monthly_payment": float(c.monthly_payment) if c.monthly_payment is not None else None,
        "note": c.note,
        "is_active": c.is_active,
        **(summary or {}),
    }


def _serialize_payment(p: CreditPayment) -> dict:
    return {
        "id": p.id,
        "credit_id": p.credit_id,
        "payment_date": p.payment_date.isoformat() if p.payment_date else None,
        "body_amount": float(p.body_amount or 0),
        "interest_amount": float(p.interest_amount or 0),
        "total_amount": float(p.total_amount or 0),
        "balance_after": float(p.balance_after) if p.balance_after is not None else None,
        "note": p.note,
    }


def _summary(db: Session, credit: Credit) -> dict:
    agg = (
        db.query(
            func.coalesce(func.sum(CreditPayment.body_amount), 0).label("body_paid"),
            func.coalesce(func.sum(CreditPayment.interest_amount), 0).label("interest_paid"),
            func.coalesce(func.sum(CreditPayment.total_amount), 0).label("total_paid"),
            func.count(CreditPayment.id).label("payments_count"),
        )
        .filter(CreditPayment.credit_id == credit.id)
        .first()
    )
    body_paid = float(agg.body_paid or 0)
    balance = max(float(credit.principal or 0) - body_paid, 0.0)
    return {
        "body_paid": body_paid,
        "interest_paid": float(agg.interest_paid or 0),
        "total_paid": float(agg.total_paid or 0),
        "payments_count": int(agg.payments_count or 0),
        "balance": balance,
    }


@router.get("")
def list_credits(
    active_only: bool = False,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = db.query(Credit)
    if active_only:
        q = q.filter(Credit.is_active == True)
    credits = q.order_by(Credit.is_active.desc(), Credit.start_date.desc().nullslast(), Credit.id.desc()).all()
    return [_serialize_credit(c, _summary(db, c)) for c in credits]


@router.post("")
def create_credit(
    body: CreditCreate,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    c = Credit(**body.model_dump(), created_by=user.id)
    db.add(c)
    db.commit()
    db.refresh(c)
    return _serialize_credit(c, _summary(db, c))


@router.patch("/{credit_id}")
def update_credit(
    credit_id: int,
    body: CreditUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    c = db.query(Credit).filter(Credit.id == credit_id).first()
    if not c:
        raise HTTPException(404, "Credit not found")
    for k, v in body.model_dump(exclude_unset=True).items():
        setattr(c, k, v)
    db.commit()
    db.refresh(c)
    return _serialize_credit(c, _summary(db, c))


@router.delete("/{credit_id}")
def delete_credit(
    credit_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    c = db.query(Credit).filter(Credit.id == credit_id).first()
    if not c:
        raise HTTPException(404, "Credit not found")
    db.delete(c)
    db.commit()
    return {"ok": True}


@router.get("/{credit_id}/payments")
def list_payments(
    credit_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    c = db.query(Credit).filter(Credit.id == credit_id).first()
    if not c:
        raise HTTPException(404, "Credit not found")
    rows = (
        db.query(CreditPayment)
        .filter(CreditPayment.credit_id == credit_id)
        .order_by(CreditPayment.payment_date.asc(), CreditPayment.id.asc())
        .all()
    )
    # Добавляем динамический остаток после каждого платежа
    balance = float(c.principal or 0)
    result = []
    for p in rows:
        balance -= float(p.body_amount or 0)
        item = _serialize_payment(p)
        item["balance_calc"] = max(balance, 0.0)
        result.append(item)
    return result


@router.post("/{credit_id}/payments")
def create_payment(
    credit_id: int,
    body: PaymentCreate,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    c = db.query(Credit).filter(Credit.id == credit_id).first()
    if not c:
        raise HTTPException(404, "Credit not found")
    total = body.total_amount if body.total_amount is not None else (body.body_amount + body.interest_amount)
    p = CreditPayment(
        credit_id=credit_id,
        payment_date=body.payment_date,
        body_amount=body.body_amount,
        interest_amount=body.interest_amount,
        total_amount=total,
        balance_after=body.balance_after,
        note=body.note,
        created_by=user.id,
    )
    db.add(p)
    db.commit()
    db.refresh(p)
    return _serialize_payment(p)


@router.patch("/payments/{payment_id}")
def update_payment(
    payment_id: int,
    body: PaymentUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    p = db.query(CreditPayment).filter(CreditPayment.id == payment_id).first()
    if not p:
        raise HTTPException(404, "Payment not found")
    data = body.model_dump(exclude_unset=True)
    for k, v in data.items():
        setattr(p, k, v)
    # пересчитаем total если не задано
    if "total_amount" not in data:
        p.total_amount = float(p.body_amount or 0) + float(p.interest_amount or 0)
    db.commit()
    db.refresh(p)
    return _serialize_payment(p)


@router.delete("/payments/{payment_id}")
def delete_payment(
    payment_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    p = db.query(CreditPayment).filter(CreditPayment.id == payment_id).first()
    if not p:
        raise HTTPException(404, "Payment not found")
    db.delete(p)
    db.commit()
    return {"ok": True}
