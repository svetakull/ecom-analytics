"""
Платёжный календарь — прогноз поступлений и расходов.
Графики выплат МП:
  WB:     расч. неделя пн-вс → деньги через ~28 дней (4 недели)
  Ozon:   расч. неделя пн-вс → деньги через ~24 дня (среда +3 недели)
  Lamoda: расч. неделя пн-вс → деньги через ~8 дней (вторник след. недели)
Налоги:
  УСН: ежеквартально 25-е число (апр, июл, окт, янв)
  НДС: с марта 2026, ⅓ ежемесячно 28-го числа следующего квартала
"""
from collections import defaultdict
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.catalog import Channel, ChannelType
from app.models.finance import DDSBalance, PaymentCalendarEntry
from app.models.sales import SkuDailyExpense


# --- Константы налогов (из opiu_service.py) ---
NDS_START = date(2026, 3, 1)
NDS_RATE = 0.05


def _usn_rate(d: date) -> float:
    if d.year <= 2025:
        return 0.01
    if d.year == 2026 and d.month <= 2:
        return 0.03
    return 0.01


# --- Графики выплат МП ---
WB_PAYMENT_DELAY_DAYS = 28   # 4 недели от конца расч. недели
OZON_PAYMENT_DELAY_DAYS = 24  # ~3.5 недели
LAMODA_PAYMENT_DELAY_DAYS = 8  # вторник след. недели


def _week_monday(d: date) -> date:
    """Понедельник недели, к которой принадлежит дата."""
    return d - timedelta(days=d.weekday())


def _week_sunday(d: date) -> date:
    return _week_monday(d) + timedelta(days=6)


def _payment_date_for_week(week_end: date, channel_type: str) -> date:
    """Дата зачисления денег для расчётной недели (по воскресенье)."""
    if channel_type == "wb":
        return week_end + timedelta(days=WB_PAYMENT_DELAY_DAYS)
    elif channel_type == "ozon":
        return week_end + timedelta(days=OZON_PAYMENT_DELAY_DAYS)
    elif channel_type == "lamoda":
        return week_end + timedelta(days=LAMODA_PAYMENT_DELAY_DAYS)
    return week_end + timedelta(days=14)


def _get_channel_ids(db: Session, channels: list[str] | None) -> dict[int, str]:
    """Возвращает {channel_id: type_name}."""
    type_map = {"wb": ChannelType.WB, "ozon": ChannelType.OZON, "lamoda": ChannelType.LAMODA}
    result = {}
    if channels:
        for c in channels:
            ct = type_map.get(c.lower())
            if ct:
                ch = db.query(Channel).filter(Channel.type == ct).first()
                if ch:
                    result[ch.id] = c.lower()
    if not result:
        for ch in db.query(Channel).all():
            t = ch.type.value if hasattr(ch.type, "value") else str(ch.type).lower()
            if t in ("wb", "ozon", "lamoda"):
                result[ch.id] = t
    return result


def _current_balance(db: Session) -> float:
    """Текущий остаток ДС (последний DDSBalance)."""
    last_date = db.query(func.max(DDSBalance.date)).scalar()
    if not last_date:
        return 0.0
    total = db.query(func.sum(DDSBalance.amount)).filter(DDSBalance.date == last_date).scalar()
    return float(total or 0)


def _usn_quarterly_dates(year: int) -> list[date]:
    """Даты уплаты УСН за год."""
    return [
        date(year, 4, 25), date(year, 7, 25),
        date(year, 10, 25), date(year + 1, 1, 25),
    ]


def _nds_payment_dates(quarter_start: date) -> list[date]:
    """Даты уплаты НДС за квартал (⅓ ежемесячно 28-го след. квартала)."""
    q_month = quarter_start.month
    year = quarter_start.year
    if q_month <= 3:
        return [date(year, 4, 28), date(year, 5, 28), date(year, 6, 28)]
    elif q_month <= 6:
        return [date(year, 7, 28), date(year, 8, 28), date(year, 9, 28)]
    elif q_month <= 9:
        return [date(year, 10, 28), date(year, 11, 28), date(year, 12, 28)]
    else:
        return [date(year + 1, 1, 28), date(year + 1, 2, 28), date(year + 1, 3, 28)]


def get_payment_calendar(
    db: Session,
    weeks_ahead: int = 8,
    channels: list[str] | None = None,
) -> dict:
    """Платёжный календарь: факт + прогноз."""
    today = date.today()
    channel_map = _get_channel_ids(db, channels)
    if not channel_map:
        return {"current_balance": 0, "weeks": [], "warnings": []}

    # Определяем диапазон: 4 недели назад (факт) + weeks_ahead вперёд (прогноз)
    history_weeks = 4
    start_monday = _week_monday(today) - timedelta(weeks=history_weeks)
    end_sunday = _week_sunday(today) + timedelta(weeks=weeks_ahead)

    # --- 1. Поступления от МП (ppvz_for_pay по расчётным неделям) ---
    # Загружаем ppvz_for_pay за последние 12 недель (для факта + прогноза)
    lookback = today - timedelta(weeks=12)
    ppvz_rows = (
        db.query(
            func.date_trunc("week", SkuDailyExpense.date).label("week"),
            SkuDailyExpense.channel_id,
            func.sum(SkuDailyExpense.ppvz_for_pay).label("ppvz"),
        )
        .filter(
            SkuDailyExpense.channel_id.in_(channel_map.keys()),
            SkuDailyExpense.date >= lookback,
            SkuDailyExpense.date <= today,
        )
        .group_by("week", SkuDailyExpense.channel_id)
        .all()
    )

    # Маппим: (payment_week_monday, channel_type) → amount
    mp_inflows: dict[tuple[date, str], float] = defaultdict(float)
    weekly_ppvz: dict[str, list[float]] = defaultdict(list)  # для прогноза

    for row in ppvz_rows:
        week_start = row.week.date() if hasattr(row.week, "date") else row.week
        week_end = week_start + timedelta(days=6)
        ch_type = channel_map.get(row.channel_id, "wb")
        amount = float(row.ppvz or 0)

        if amount <= 0:
            continue

        weekly_ppvz[ch_type].append(amount)

        # Дата зачисления
        payment_dt = _payment_date_for_week(week_end, ch_type)
        payment_monday = _week_monday(payment_dt)
        mp_inflows[(payment_monday, ch_type)] += amount

    # Прогноз для будущих расчётных недель (среднее за 4 последних)
    for ch_type, amounts in weekly_ppvz.items():
        last4 = amounts[-4:] if len(amounts) >= 4 else amounts
        avg_weekly = sum(last4) / len(last4) if last4 else 0
        if avg_weekly <= 0:
            continue

        # Будущие расчётные недели (от текущей + weeks_ahead)
        for w in range(weeks_ahead + 5):  # +5 чтобы покрыть задержку выплат
            future_week_start = _week_monday(today) + timedelta(weeks=w)
            future_week_end = future_week_start + timedelta(days=6)

            # Только если расчётная неделя ещё не имеет факта
            if future_week_start <= today:
                continue

            payment_dt = _payment_date_for_week(future_week_end, ch_type)
            payment_monday = _week_monday(payment_dt)

            if payment_monday <= end_sunday:
                mp_inflows[(payment_monday, ch_type)] += avg_weekly

    # --- 2. Налоги (авто) ---
    # УСН за текущий квартал
    tax_outflows: dict[date, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    # Рассчитаем продажи за квартал для УСН
    q_start_month = ((today.month - 1) // 3) * 3 + 1
    q_start = date(today.year, q_start_month, 1)
    q_end = date(today.year, q_start_month + 2, 28)

    prodazhi_q = (
        db.query(func.sum(SkuDailyExpense.compensation - SkuDailyExpense.commission))
        .filter(
            SkuDailyExpense.channel_id.in_(channel_map.keys()),
            SkuDailyExpense.date >= q_start,
            SkuDailyExpense.date <= today,
        )
        .scalar()
    )
    prodazhi_q = float(prodazhi_q or 0)

    # УСН = prodazhi × rate
    usn_rate = _usn_rate(today)
    usn_amount = prodazhi_q * usn_rate
    usn_dates = _usn_quarterly_dates(today.year)
    for usn_dt in usn_dates:
        if start_monday <= usn_dt <= end_sunday:
            tax_outflows[usn_dt]["usn"] = usn_amount

    # НДС (с марта 2026): ⅓ ежемесячно
    if today >= NDS_START:
        nds_base = prodazhi_q * NDS_RATE
        nds_third = round(nds_base / 3, 2)
        nds_dates = _nds_payment_dates(q_start)
        for nds_dt in nds_dates:
            if start_monday <= nds_dt <= end_sunday:
                tax_outflows[nds_dt]["nds"] = nds_third

    # --- 3. Ручные записи (PaymentCalendarEntry) ---
    manual_entries = (
        db.query(PaymentCalendarEntry)
        .filter(
            PaymentCalendarEntry.scheduled_date >= start_monday,
            PaymentCalendarEntry.scheduled_date <= end_sunday,
        )
        .all()
    )

    # Также генерируем экземпляры повторяющихся записей
    recurring = (
        db.query(PaymentCalendarEntry)
        .filter(PaymentCalendarEntry.is_recurring == True)
        .all()
    )

    manual_by_week: dict[date, list[dict]] = defaultdict(list)

    for e in manual_entries:
        monday = _week_monday(e.scheduled_date)
        manual_by_week[monday].append({
            "id": e.id, "entry_type": e.entry_type, "category": e.category,
            "name": e.name, "amount": float(e.amount),
            "is_confirmed": e.is_confirmed, "is_recurring": e.is_recurring,
        })

    # Разворачиваем повторяющиеся на весь горизонт
    for e in recurring:
        interval = {
            "weekly": timedelta(weeks=1),
            "biweekly": timedelta(weeks=2),
            "monthly": timedelta(days=30),
            "quarterly": timedelta(days=91),
        }.get(e.recurrence_rule, timedelta(weeks=4))

        dt = e.scheduled_date
        while dt <= end_sunday:
            if dt >= start_monday:
                monday = _week_monday(dt)
                # Не дублируем если уже есть ручная запись на эту неделю
                existing = any(
                    m["category"] == e.category and _week_monday(date.fromisoformat(str(e.scheduled_date))) == monday
                    for m in manual_by_week.get(monday, [])
                )
                if not existing:
                    manual_by_week[monday].append({
                        "id": e.id, "entry_type": e.entry_type, "category": e.category,
                        "name": e.name, "amount": float(e.amount),
                        "is_confirmed": False, "is_recurring": True,
                    })
            dt += interval

    # --- 4. Собираем календарь по неделям ---
    current_balance = _current_balance(db)
    balance = current_balance
    weeks_result = []
    warnings = []

    week_start = start_monday
    while week_start <= _week_monday(end_sunday):
        week_end = week_start + timedelta(days=6)
        is_forecast = week_start > _week_monday(today)
        is_current = week_start == _week_monday(today)

        lines = []

        # --- ПОСТУПЛЕНИЯ ---
        total_inflows = 0.0
        lines.append({"key": "section_inflows", "name": "ПОСТУПЛЕНИЯ", "amount": 0, "level": 0, "bold": True, "editable": False, "source": "auto"})

        for ch_type_label, ch_type_key in [("WB", "wb"), ("Ozon", "ozon"), ("Lamoda", "lamoda")]:
            amt = mp_inflows.get((week_start, ch_type_key), 0)
            if amt > 0 or ch_type_key in [v for v in channel_map.values()]:
                lines.append({
                    "key": f"mp_{ch_type_key}", "name": f"Оплата {ch_type_label}",
                    "amount": round(amt, 2), "level": 1, "bold": False,
                    "editable": False, "source": "auto",
                })
                total_inflows += amt

        # Ручные поступления
        manual_inflows = sum(
            m["amount"] for m in manual_by_week.get(week_start, [])
            if m["entry_type"] == "inflow"
        )
        if manual_inflows > 0:
            lines.append({"key": "other_inflows", "name": "Прочие поступления", "amount": round(manual_inflows, 2), "level": 1, "bold": False, "editable": True, "source": "manual"})
            total_inflows += manual_inflows

        lines.append({"key": "total_inflows", "name": "Итого поступления", "amount": round(total_inflows, 2), "level": 0, "bold": True, "editable": False, "source": "auto"})

        # --- РАСХОДЫ ---
        total_outflows = 0.0
        lines.append({"key": "section_outflows", "name": "РАСХОДЫ", "amount": 0, "level": 0, "bold": True, "editable": False, "source": "auto"})

        # Ручные расходы по категориям
        outflow_cats = defaultdict(float)
        for m in manual_by_week.get(week_start, []):
            if m["entry_type"] == "outflow":
                outflow_cats[m["category"]] += m["amount"]

        cat_labels = {
            "salary": "ФОТ", "rent": "Аренда", "purchase": "Закупки",
            "credit": "Кредиты", "subscription": "Подписки",
            "external_ads": "Внешняя реклама", "other": "Прочие расходы",
        }
        for cat, label in cat_labels.items():
            amt = outflow_cats.get(cat, 0)
            lines.append({"key": f"out_{cat}", "name": label, "amount": round(amt, 2), "level": 1, "bold": False, "editable": True, "source": "manual", "category": cat})
            total_outflows += amt

        # Налоги (авто)
        usn_this_week = 0.0
        nds_this_week = 0.0
        for dt, taxes in tax_outflows.items():
            if _week_monday(dt) == week_start:
                usn_this_week += taxes.get("usn", 0)
                nds_this_week += taxes.get("nds", 0)

        lines.append({"key": "out_usn", "name": "УСН", "amount": round(usn_this_week, 2), "level": 1, "bold": False, "editable": False, "source": "auto"})
        lines.append({"key": "out_nds", "name": "НДС (⅓)", "amount": round(nds_this_week, 2), "level": 1, "bold": False, "editable": False, "source": "auto"})
        total_outflows += usn_this_week + nds_this_week

        lines.append({"key": "total_outflows", "name": "Итого расходы", "amount": round(total_outflows, 2), "level": 0, "bold": True, "editable": False, "source": "auto"})

        # --- БАЛАНС ---
        net_flow = total_inflows - total_outflows
        balance += net_flow
        cash_gap = balance < 0

        lines.append({"key": "net_flow", "name": "Чистый поток", "amount": round(net_flow, 2), "level": 0, "bold": True, "editable": False, "source": "auto"})
        lines.append({"key": "balance_end", "name": "Баланс на конец недели", "amount": round(balance, 2), "level": 0, "bold": True, "editable": False, "source": "auto", "cash_gap": cash_gap})

        if cash_gap:
            warnings.append({
                "week": week_start.isoformat(),
                "deficit": round(balance, 2),
                "message": f"Кассовый разрыв! Дефицит {abs(balance):,.0f} ₽",
            })

        weeks_result.append({
            "period": week_start.isoformat(),
            "period_end": week_end.isoformat(),
            "is_forecast": is_forecast,
            "is_current": is_current,
            "lines": lines,
        })

        week_start += timedelta(weeks=1)

    return {
        "current_balance": round(current_balance, 2),
        "weeks_ahead": weeks_ahead,
        "weeks": weeks_result,
        "warnings": warnings,
    }
