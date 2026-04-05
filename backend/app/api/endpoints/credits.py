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
    from datetime import date as _date_cls
    today = _date_cls.today()
    # Фактические платежи (payment_date <= сегодня)
    agg = (
        db.query(
            func.coalesce(func.sum(CreditPayment.body_amount), 0).label("body_paid"),
            func.coalesce(func.sum(CreditPayment.interest_amount), 0).label("interest_paid"),
            func.coalesce(func.sum(CreditPayment.total_amount), 0).label("total_paid"),
            func.count(CreditPayment.id).label("payments_count"),
        )
        .filter(CreditPayment.credit_id == credit.id)
        .filter(CreditPayment.payment_date <= today)
        .first()
    )
    # Плановые (будущие) проценты
    future_int = (
        db.query(func.coalesce(func.sum(CreditPayment.interest_amount), 0))
        .filter(CreditPayment.credit_id == credit.id)
        .filter(CreditPayment.payment_date > today)
        .scalar()
    )
    body_paid = float(agg.body_paid or 0)
    balance = max(float(credit.principal or 0) - body_paid, 0.0)
    return {
        "future_interest": float(future_int or 0),
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


@router.get("/summary-by-period")
def summary_by_period(
    period: str = "month",  # year | month | week
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Сводная таблица платежей по кредитам сгруппированная по периодам.
    period: 'year', 'month' или 'week'. Возвращает per-credit + total строки."""
    trunc_fn = period if period in ('year', 'month', 'week') else 'month'
    q = db.query(
        CreditPayment.credit_id,
        func.date_trunc(trunc_fn, CreditPayment.payment_date).label('period'),
        func.sum(CreditPayment.body_amount).label('body'),
        func.sum(CreditPayment.interest_amount).label('interest'),
        func.sum(CreditPayment.total_amount).label('total'),
        func.count(CreditPayment.id).label('count'),
    )
    if date_from:
        q = q.filter(CreditPayment.payment_date >= date_from)
    if date_to:
        q = q.filter(CreditPayment.payment_date <= date_to)
    q = q.group_by(CreditPayment.credit_id, 'period').order_by('period', CreditPayment.credit_id)
    rows = q.all()

    # Соберём dict period → {credit_id: data}
    credits = {c.id: c for c in db.query(Credit).all()}
    periods: dict = {}
    for r in rows:
        p_str = (r.period.date() if hasattr(r.period, 'date') else r.period).strftime('%Y-%m-%d')
        periods.setdefault(p_str, {})[r.credit_id] = {
            'body': float(r.body or 0),
            'interest': float(r.interest or 0),
            'total': float(r.total or 0),
            'count': int(r.count or 0),
        }

    # Итого по периоду (сумма всех кредитов)
    result = []
    for p_str in sorted(periods.keys()):
        per_credit = []
        sum_body = sum_interest = sum_total = 0
        for cid, data in periods[p_str].items():
            c = credits.get(cid)
            per_credit.append({
                'credit_id': cid,
                'credit_name': c.name if c else f'#{cid}',
                'body': data['body'],
                'interest': data['interest'],
                'total': data['total'],
                'count': data['count'],
            })
            sum_body += data['body']
            sum_interest += data['interest']
            sum_total += data['total']
        result.append({
            'period': p_str,
            'credits': per_credit,
            'sum_body': sum_body,
            'sum_interest': sum_interest,
            'sum_total': sum_total,
        })
    return result


@router.get("/wb-deductions")
def wb_deductions(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Список удержаний WB (credit_deduction) агрегированных по НЕДЕЛЯМ
    (еженедельный финотчёт). WB удерживает кредит одним платежом за неделю."""
    from app.models.sales import SkuDailyExpense
    from app.models.catalog import Channel, ChannelType
    from datetime import timedelta as _td
    q = (
        db.query(
            func.date_trunc('week', SkuDailyExpense.date).label('week_start'),
            func.sum(SkuDailyExpense.credit_deduction).label("amount"),
        )
        .join(Channel, Channel.id == SkuDailyExpense.channel_id)
        .filter(Channel.type == ChannelType.WB)
        .filter(SkuDailyExpense.credit_deduction > 0)
    )
    if date_from:
        q = q.filter(SkuDailyExpense.date >= date_from)
    if date_to:
        q = q.filter(SkuDailyExpense.date <= date_to)
    rows = q.group_by('week_start').order_by('week_start').all()
    result = []
    for r in rows:
        amt = float(r.amount or 0)
        if amt <= 0:
            continue
        ws = r.week_start.date() if hasattr(r.week_start, 'date') else r.week_start
        week_end = ws + _td(days=6)
        result.append({
            "date": week_end.isoformat(),
            "week_start": ws.isoformat(),
            "amount": amt,
        })
    return result


class AutoImportPayload(BaseModel):
    source: str = "wb"  # wb | ozon
    date_from: Optional[date] = None
    date_to: Optional[date] = None


@router.post("/{credit_id}/auto-import")
def auto_import_payments(
    credit_id: int,
    payload: AutoImportPayload,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Автоматически импортировать платежи из МП для указанного кредита.
    WB: из SkuDailyExpense.credit_deduction (агрегировано по дням).
    Разбивка тело/проценты: если есть фактические платежи с теми же датами
    или такой же суммой — используем их. Иначе пропорционально остатку.
    """
    c = db.query(Credit).filter(Credit.id == credit_id).first()
    if not c:
        raise HTTPException(404, "Credit not found")

    # Пропустим платежи, которые уже созданы на эту дату
    existing_dates = {
        p.payment_date for p in
        db.query(CreditPayment).filter(CreditPayment.credit_id == credit_id).all()
    }

    created = 0
    if payload.source == "wb":
        from app.models.sales import SkuDailyExpense
        from app.models.catalog import Channel, ChannelType
        # Агрегируем удержания WB по НЕДЕЛЯМ (из еженедельного финотчёта
        # они записываются на разные даты операций — группируем в недельные
        # платежи, т.к. WB удерживает кредит одним платежом за неделю)
        q = (
            db.query(
                func.date_trunc('week', SkuDailyExpense.date).label('week_start'),
                func.sum(SkuDailyExpense.credit_deduction).label("amount"),
            )
            .join(Channel, Channel.id == SkuDailyExpense.channel_id)
            .filter(Channel.type == ChannelType.WB)
            .filter(SkuDailyExpense.credit_deduction > 0)
        )
        df = payload.date_from or c.start_date
        dt = payload.date_to
        if df:
            q = q.filter(SkuDailyExpense.date >= df)
        if dt:
            q = q.filter(SkuDailyExpense.date <= dt)
        raw_rows = q.group_by('week_start').order_by('week_start').all()
        # Приводим к payment_date = воскресенье недели (конец недели)
        from datetime import timedelta as _td
        rows = [
            type('Row', (), {
                'date': (r.week_start.date() if hasattr(r.week_start, 'date') else r.week_start) + _td(days=6),
                'amount': r.amount,
            })()
            for r in raw_rows
        ]

        # Оцениваем дневную ставку процентов = interest_rate% / 30 дней
        monthly_rate = float(c.interest_rate or 0) / 100  # % в месяц
        prev_payment = (
            db.query(CreditPayment)
            .filter(CreditPayment.credit_id == credit_id)
            .order_by(CreditPayment.payment_date.desc())
            .first()
        )
        running_body_paid = sum(
            float(p.body_amount or 0) for p in
            db.query(CreditPayment).filter(CreditPayment.credit_id == credit_id).all()
        )

        for r in rows:
            if r.date in existing_dates:
                continue
            total = float(r.amount or 0)
            if total <= 0:
                continue
            remaining = max(float(c.principal or 0) - running_body_paid, 0)
            # Приблизительно: проценты = остаток × ставка × (дни от пред.платежа / 30)
            if prev_payment and monthly_rate > 0:
                days = (r.date - prev_payment.payment_date).days
                est_interest = round(remaining * monthly_rate * days / 30, 2)
            else:
                est_interest = round(remaining * monthly_rate, 2)
            est_interest = min(est_interest, total)
            est_body = round(total - est_interest, 2)
            running_body_paid += est_body

            p = CreditPayment(
                credit_id=credit_id,
                payment_date=r.date,
                body_amount=est_body,
                interest_amount=est_interest,
                total_amount=total,
                note="Авто-импорт из WB (credit_deduction)",
                created_by=user.id,
            )
            db.add(p)
            created += 1
            prev_payment = p

    db.commit()
    return {"ok": True, "created": created}


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
