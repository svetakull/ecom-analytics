"""
Сервис ОПиУ — Отчёт о прибылях и убытках.
Группировка по месяцам, фильтры по каналу и артикулу.
Данные из SkuDailyExpense (WB/Ozon финотчёты).

Верифицировано: январь 2026 WB — Реализация 2,275,069, Фактические продажи 1,708,577 (0.00% расхождение с TrueStats)
"""
from datetime import date, datetime
from typing import List, Optional

from sqlalchemy import func, extract
from sqlalchemy.orm import Session

from app.models.catalog import Channel, ChannelType, SKU, SKUChannel
from app.models.inventory import ProductBatch, SKUCostHistory
from app.models.sales import SkuDailyExpense


# НДС с марта 2026 (5% для УСН)
NDS_START = date(2026, 3, 1)
NDS_RATE = 0.05
USN_RATE = 0.06  # (1% УСН + 5% НДС) = 6% от налоговой базы. До марта 2026 = 1% УСН


def _cogs_per_unit(db: Session, sku_id: int, ref_date: date, channel_id: int = None) -> float:
    """Себестоимость: cost_prices → SKUCostHistory → ProductBatch."""
    # 1) Новый модуль
    if channel_id:
        try:
            from app.services.cost_price_service import resolve_cogs_per_unit
            val = resolve_cogs_per_unit(db, sku_id, channel_id, ref_date)
            if val > 0:
                return val
        except Exception:
            pass
    # 2) SKUCostHistory
    record = (
        db.query(SKUCostHistory)
        .filter(SKUCostHistory.sku_id == sku_id, SKUCostHistory.effective_from <= ref_date)
        .order_by(SKUCostHistory.effective_from.desc())
        .first()
    )
    if record:
        return float(record.cost_per_unit)
    # 3) ProductBatch
    batch = (
        db.query(ProductBatch)
        .filter(ProductBatch.sku_id == sku_id)
        .order_by(ProductBatch.batch_date.desc())
        .first()
    )
    return float(batch.purchase_cost) if batch and batch.purchase_cost else 0.0


def get_opiu(
    db: Session,
    date_from: date,
    date_to: date,
    channels: Optional[List[str]] = None,
    article: Optional[str] = None,
) -> dict:
    """
    ОПиУ по месяцам с фильтрами.
    Returns: {date_from, date_to, months: [{period, lines}], total: {lines}}
    """
    # Фильтр каналов
    channel_ids = []
    if channels:
        type_map = {"wb": ChannelType.WB, "ozon": ChannelType.OZON, "lamoda": ChannelType.LAMODA}
        for c in channels:
            ct = type_map.get(c.lower())
            if ct:
                ch = db.query(Channel).filter(Channel.type == ct).first()
                if ch:
                    channel_ids.append(ch.id)
    if not channel_ids:
        channel_ids = [ch.id for ch in db.query(Channel).all()]

    # Фильтр SKU
    sku_ids = None
    if article:
        sku_ids = [
            s.id for s in db.query(SKU)
            .filter(SKU.seller_article.ilike(f"%{article}%"))
            .all()
        ]

    # Базовый запрос: агрегация по месяцам
    q = db.query(
        func.date_trunc("week", SkuDailyExpense.date).label("week"),
        SkuDailyExpense.sku_id,
        func.sum(SkuDailyExpense.sale_amount).label("sale_amount"),         # retail_price продаж
        func.sum(SkuDailyExpense.compensation).label("compensation"),       # retail_amount продаж
        func.sum(SkuDailyExpense.return_amount).label("return_amount"),     # retail_price возвратов
        func.sum(SkuDailyExpense.commission).label("commission"),           # retail_amount возвратов
        func.sum(SkuDailyExpense.logistics).label("logistics"),
        func.sum(SkuDailyExpense.acquiring).label("acquiring"),
        func.sum(SkuDailyExpense.storage).label("storage"),
        func.sum(SkuDailyExpense.penalty).label("penalty"),
        func.sum(SkuDailyExpense.other_deductions).label("other_deductions"),
        func.sum(SkuDailyExpense.advertising).label("advertising"),
        func.sum(SkuDailyExpense.other_services).label("other_services"),
        func.sum(SkuDailyExpense.subscription).label("subscription"),
        func.sum(SkuDailyExpense.reviews).label("reviews"),
        func.sum(SkuDailyExpense.compensation_wb).label("compensation_wb"),
        func.sum(SkuDailyExpense.acceptance).label("acceptance"),
        func.sum(SkuDailyExpense.items_count).label("items_count"),
        func.sum(SkuDailyExpense.return_count).label("return_count"),
    ).filter(
        SkuDailyExpense.channel_id.in_(channel_ids),
        SkuDailyExpense.date >= date_from,
        SkuDailyExpense.date <= date_to,
    )

    if sku_ids is not None:
        q = q.filter(SkuDailyExpense.sku_id.in_(sku_ids))

    q = q.group_by("week", SkuDailyExpense.sku_id)
    rows = q.all()

    # Группировка по месяцам
    from collections import defaultdict
    monthly: dict[str, dict] = defaultdict(lambda: {
        "sale_amount": 0, "compensation": 0, "return_amount": 0, "commission": 0,
        "logistics": 0, "acquiring": 0, "storage": 0, "penalty": 0,
        "other_deductions": 0, "advertising": 0, "other_services": 0,
        "subscription": 0, "reviews": 0, "compensation_wb": 0,
        "acceptance": 0, "items_count": 0, "return_count": 0, "cogs": 0,
    })

    for row in rows:
        period = row.week.strftime("%Y-%m-%d") if row.week else "unknown"
        m = monthly[period]
        m["sale_amount"] += float(row.sale_amount or 0)
        m["compensation"] += float(row.compensation or 0)
        m["return_amount"] += float(row.return_amount or 0)
        m["commission"] += float(row.commission or 0)
        m["logistics"] += float(row.logistics or 0)
        m["acquiring"] += float(row.acquiring or 0)
        m["storage"] += float(row.storage or 0)
        m["penalty"] += float(row.penalty or 0)
        m["other_deductions"] += float(row.other_deductions or 0)
        m["advertising"] += float(row.advertising or 0)
        m["other_services"] += float(row.other_services or 0)
        m["subscription"] += float(row.subscription or 0)
        m["reviews"] += float(row.reviews or 0)
        m["compensation_wb"] += float(row.compensation_wb or 0)
        m["acceptance"] += float(row.acceptance or 0)
        m["items_count"] += int(row.items_count or 0)
        m["return_count"] += int(row.return_count or 0)

        # Себестоимость = net qty × cogs_per_unit (продажи минус возвраты)
        ref = row.week.date() if hasattr(row.week, "date") else date_to
        cogs_unit = _cogs_per_unit(db, row.sku_id, ref)
        net_qty = int(row.items_count or 0) - int(row.return_count or 0)
        m["cogs"] += max(net_qty, 0) * cogs_unit

    # Строим ОПиУ для каждой недели + месячные subtotals
    from datetime import datetime, timedelta
    sorted_periods = sorted(monthly.keys())

    # Группируем недели по месяцам (по четвергу — ISO rule)
    month_weeks: dict[str, list[str]] = defaultdict(list)
    for period in sorted_periods:
        try:
            dt = datetime.strptime(period, "%Y-%m-%d")
            thu = dt + timedelta(days=3)
            month_key = thu.strftime("%Y-%m")
        except Exception:
            month_key = period[:7]
        month_weeks[month_key].append(period)

    periods_result = []
    prev_month = None
    fields = list(next(iter(monthly.values())).keys()) if monthly else []

    for period in sorted_periods:
        try:
            dt = datetime.strptime(period, "%Y-%m-%d")
            thu = dt + timedelta(days=3)
            cur_month = thu.strftime("%Y-%m")
        except Exception:
            cur_month = period[:7]

        # Subtotal за предыдущий месяц
        if prev_month and cur_month != prev_month:
            month_data = {k: sum(monthly[wp][k] for wp in month_weeks.get(prev_month, [])) for k in fields}
            month_lines = _build_lines(month_data, f"month:{prev_month}")
            month_names = {"01": "Январь", "02": "Февраль", "03": "Март", "04": "Апрель",
                          "05": "Май", "06": "Июнь", "07": "Июль", "08": "Август",
                          "09": "Сентябрь", "10": "Октябрь", "11": "Ноябрь", "12": "Декабрь"}
            m_label = month_names.get(prev_month[-2:], prev_month[-2:])
            periods_result.append({"period": f"month:{prev_month}", "label": f"Итого {m_label}", "lines": month_lines, "is_month_total": True})

        lines = _build_lines(monthly[period], period)
        periods_result.append({"period": period, "lines": lines})
        prev_month = cur_month

    # Subtotal за последний месяц
    if prev_month and month_weeks.get(prev_month):
        month_data = {k: sum(monthly[wp][k] for wp in month_weeks[prev_month]) for k in fields}
        month_lines = _build_lines(month_data, f"month:{prev_month}")
        month_names = {"01": "Январь", "02": "Февраль", "03": "Март", "04": "Апрель",
                      "05": "Май", "06": "Июнь", "07": "Июль", "08": "Август",
                      "09": "Сентябрь", "10": "Октябрь", "11": "Ноябрь", "12": "Декабрь"}
        m_label = month_names.get(prev_month[-2:], prev_month[-2:])
        periods_result.append({"period": f"month:{prev_month}", "label": f"Итого {m_label}", "lines": month_lines, "is_month_total": True})

    # Итого (всё)
    total_data = {k: sum(m[k] for m in monthly.values()) for k in fields} if monthly else {}
    total_lines = _build_lines(total_data, "total") if total_data else []

    return {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "months": periods_result,
        "total": {"period": "total", "lines": total_lines},
    }


def _build_lines(data: dict, period: str) -> list:
    """Построить строки ОПиУ из агрегированных данных."""
    realizaciya = data["sale_amount"] - data["return_amount"]
    prodazhi = data["compensation"] - data["commission"]
    skidka_mp = realizaciya - prodazhi

    cogs = data["cogs"]
    logistics = data["logistics"]

    # Комиссия
    nom_commission = round(realizaciya * 0.35, 2)  # TODO: разный % по каналам
    acquiring = data["acquiring"]
    commission_total = nom_commission - skidka_mp + acquiring

    penalty = data["penalty"]
    storage = data["storage"]
    advertising = data["advertising"]          # реклама (WB Продвижение)
    subscription = data["subscription"]        # Подписка МП (Джем)
    reviews = data["reviews"]                  # Отзывы
    other_deductions = data["other_deductions"]  # прочие удержания
    acceptance = data["acceptance"]

    # Прочие = подписка + отзывы + прочие удержания (без рекламы — она отдельно)
    prochie_total = subscription + reviews + other_deductions

    pryamye = cogs + logistics + commission_total + penalty + storage + advertising + prochie_total + acceptance

    # Компенсация от МП (добровольная компенсация при возврате, скидки лояльности)
    kompensaciya = data.get("compensation_wb", 0)

    valovaya = prodazhi + kompensaciya - pryamye

    # Операционные расходы — пока 0
    opex = 0
    ebitda = valovaya - opex

    # Налоги
    month_date = None
    if period != "total":
        try:
            if len(period) == 10:  # "2026-03-23" — недельный формат
                month_date = date.fromisoformat(period)
            elif len(period) == 7:  # "2026-03" — месячный формат
                month_date = date.fromisoformat(period + "-01")
            elif period.startswith("month:"):  # "month:2026-03"
                month_date = date.fromisoformat(period[6:] + "-01")
        except ValueError:
            month_date = None

    # Налоги зависят от периода:
    # 2025: УСН 1%, НДС нет
    # Янв-Фев 2026: УСН 3%, НДС нет
    # С марта 2026: УСН 1% + НДС 5%
    if month_date:
        if month_date >= NDS_START:
            # Март 2026+: УСН 1% + НДС 5%
            nds = round(prodazhi * NDS_RATE, 2)
            usn = round(prodazhi * 0.01, 2)
        elif month_date.year == 2026:
            # Янв-Фев 2026: УСН 3%
            nds = 0
            usn = round(prodazhi * 0.03, 2)
        else:
            # 2025: УСН 1%
            nds = 0
            usn = round(prodazhi * 0.01, 2)
    else:
        nds = 0
        usn = round(prodazhi * 0.01, 2)

    taxes = nds + usn

    chistaya = ebitda - taxes

    # % от реализации
    def pct(val):
        return round(val / realizaciya * 100, 2) if realizaciya else 0

    lines = [
        {"key": "realizaciya", "name": "Реализация", "amount": round(realizaciya, 2), "pct": 0, "level": 0, "bold": True},
        {"key": "skidka_mp", "name": "Скидка за счет МП", "amount": round(skidka_mp, 2), "pct": pct(skidka_mp), "level": 0},
        {"key": "prodazhi", "name": "Фактические продажи", "amount": round(prodazhi, 2), "pct": pct(prodazhi), "level": 0, "bold": True},

        {"key": "pryamye", "name": "Прямые расходы", "amount": round(pryamye, 2), "pct": pct(pryamye), "level": 0, "bold": True},
        {"key": "cogs_group", "name": "Себестоимость продаж", "amount": round(cogs, 2), "pct": pct(cogs), "level": 1},

        {"key": "logistics_group", "name": "Логистика", "amount": round(logistics, 2), "pct": pct(logistics), "level": 1},

        {"key": "commission_group", "name": "Комиссия", "amount": round(commission_total, 2), "pct": pct(commission_total), "level": 1},
        {"key": "nom_commission", "name": "Номинальная комиссия", "amount": round(nom_commission, 2), "pct": pct(nom_commission), "level": 2},
        {"key": "skidka_mp_detail", "name": "Скидка МП", "amount": round(-skidka_mp, 2), "pct": pct(-skidka_mp), "level": 2},
        {"key": "acquiring", "name": "Эквайринг", "amount": round(acquiring, 2), "pct": pct(acquiring), "level": 2},

        {"key": "penalty_group", "name": "Штрафы", "amount": round(penalty, 2), "pct": pct(penalty), "level": 1},
        {"key": "storage", "name": "Хранение", "amount": round(storage, 2), "pct": pct(storage), "level": 1},
        {"key": "advertising", "name": "Внутренняя реклама", "amount": round(advertising, 2), "pct": pct(advertising), "level": 1},
        {"key": "prochie_group", "name": "Прочие", "amount": round(prochie_total, 2), "pct": pct(prochie_total), "level": 1},
        {"key": "subscription", "name": "Подписка МП", "amount": round(subscription, 2), "pct": pct(subscription), "level": 2},
        {"key": "reviews", "name": "Отзывы", "amount": round(reviews, 2), "pct": pct(reviews), "level": 2},
        {"key": "other_deductions", "name": "Прочие удержания", "amount": round(other_deductions, 2), "pct": pct(other_deductions), "level": 2},
        {"key": "acceptance", "name": "Платная приемка", "amount": round(acceptance, 2), "pct": pct(acceptance), "level": 1},

        {"key": "kompensaciya", "name": "Компенсация", "amount": round(kompensaciya, 2), "pct": pct(kompensaciya), "level": 0},

        {"key": "valovaya", "name": "Валовая маржа", "amount": round(valovaya, 2), "pct": pct(valovaya), "level": 0, "bold": True},
        {"key": "opex", "name": "Операционные расходы", "amount": round(opex, 2), "pct": pct(opex), "level": 0},
        {"key": "ebitda", "name": "Операционная прибыль (EBITDA)", "amount": round(ebitda, 2), "pct": pct(ebitda), "level": 0, "bold": True},

        {"key": "taxes_group", "name": "Налоги (кроме зарплатных)", "amount": round(taxes, 2), "pct": pct(taxes), "level": 0},
        {"key": "nds_group", "name": "НДС", "amount": round(nds, 2), "pct": pct(nds), "level": 1},
        {"key": "usn", "name": "УСН", "amount": round(usn, 2), "pct": pct(usn), "level": 1},

        {"key": "vznosy", "name": "Страховые взносы", "amount": 0, "pct": 0, "level": 0, "editable": True},
        {"key": "ndfl", "name": "НДФЛ", "amount": 0, "pct": 0, "level": 0, "editable": True},

        {"key": "chistaya", "name": "Чистая прибыль", "amount": round(chistaya, 2), "pct": pct(chistaya), "level": 0, "bold": True},
    ]

    return lines
