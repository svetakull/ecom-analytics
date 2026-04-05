"""
Сервис ДДС — Отчёт о движении денежных средств (Cash Flow Statement).
Группировка по месяцам, фильтры по каналу.
Авто-данные из SkuDailyExpense (ppvz_for_pay, compensation_wb).
Ручные данные из DDSManualEntry и DDSBalance.
"""
from collections import defaultdict
from datetime import date, timedelta, datetime
from typing import List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.catalog import Channel, ChannelType
from app.models.finance import DDSBalance, DDSManualEntry
from app.models.sales import SkuDailyExpense


def get_dds(
    db: Session,
    date_from: date,
    date_to: date,
    channels: list[str] | None = None,
) -> dict:
    """
    ДДС по месяцам с фильтрами.
    Returns: {date_from, date_to, months: [{period, lines}], total: {period, lines}}
    """
    # Фильтр каналов
    channel_ids = []
    channel_map: dict[int, str] = {}  # id -> type name (wb/ozon/lamoda)
    if channels:
        type_map = {"wb": ChannelType.WB, "ozon": ChannelType.OZON, "lamoda": ChannelType.LAMODA}
        for c in channels:
            ct = type_map.get(c.lower())
            if ct:
                ch = db.query(Channel).filter(Channel.type == ct).first()
                if ch:
                    channel_ids.append(ch.id)
                    channel_map[ch.id] = c.lower()
    if not channel_ids:
        for ch in db.query(Channel).all():
            channel_ids.append(ch.id)
            channel_map[ch.id] = ch.type.value if hasattr(ch.type, "value") else str(ch.type).lower()

    # --- Авто-данные из SkuDailyExpense ---
    auto_q = db.query(
        func.date_trunc("week", SkuDailyExpense.date).label("week"),
        SkuDailyExpense.channel_id,
        SkuDailyExpense.sku_id,
        func.sum(SkuDailyExpense.ppvz_for_pay).label("ppvz_for_pay"),
        func.sum(SkuDailyExpense.compensation_wb).label("compensation_wb"),
        func.sum(SkuDailyExpense.items_count).label("items_count"),
        func.sum(SkuDailyExpense.return_count).label("return_count"),
    ).filter(
        SkuDailyExpense.channel_id.in_(channel_ids),
        SkuDailyExpense.date >= date_from,
        SkuDailyExpense.date <= date_to,
    ).group_by("week", SkuDailyExpense.channel_id, SkuDailyExpense.sku_id)

    auto_rows = auto_q.all()

    # Себестоимость на единицу для каждого SKU
    from app.models.inventory import SKUCostHistory
    def _cogs_for_sku(sku_id: int) -> float:
        rec = db.query(SKUCostHistory).filter(
            SKUCostHistory.sku_id == sku_id,
            SKUCostHistory.effective_from <= date_to,
        ).order_by(SKUCostHistory.effective_from.desc()).first()
        return float(rec.cost_per_unit) if rec else 0.0

    cogs_cache: dict[int, float] = {}

    # Группировка авто-данных по неделе
    _empty = {
        "ppvz_for_pay": 0.0, "ppvz_for_pay_wb": 0.0,
        "ppvz_for_pay_ozon": 0.0, "compensation_wb": 0.0,
        "purchase_rub": 0.0, "purchase_qty": 0,
    }
    auto_weekly: dict[str, dict] = defaultdict(lambda: dict(_empty))

    for row in auto_rows:
        period = row.week.strftime("%Y-%m-%d") if row.week else "unknown"
        m = auto_weekly[period]
        ppvz = float(row.ppvz_for_pay or 0)
        comp = float(row.compensation_wb or 0)
        items = int(row.items_count or 0)
        returns = int(row.return_count or 0)
        net_qty = max(items - returns, 0)
        m["ppvz_for_pay"] += ppvz
        m["compensation_wb"] += comp
        ch_type = channel_map.get(row.channel_id, "")
        if ch_type == "wb":
            m["ppvz_for_pay_wb"] += ppvz
        elif ch_type == "ozon":
            m["ppvz_for_pay_ozon"] += ppvz
        # Поступление товара по цене закупки — пока отключено, логика будет позже
        # if row.sku_id not in cogs_cache:
        #     cogs_cache[row.sku_id] = _cogs_for_sku(row.sku_id)
        # cogs = cogs_cache[row.sku_id]
        # m["purchase_rub"] += net_qty * cogs
        # m["purchase_qty"] += net_qty

    # --- Ручные данные из DDSManualEntry ---
    manual_q = db.query(
        func.date_trunc("week", DDSManualEntry.date).label("week"),
        DDSManualEntry.category,
        func.sum(DDSManualEntry.amount).label("amount"),
    ).filter(
        DDSManualEntry.date >= date_from,
        DDSManualEntry.date <= date_to,
    )
    if channels:
        # Ручные записи могут быть без канала (channel_id IS NULL) — включаем их всегда
        manual_q = manual_q.filter(
            (DDSManualEntry.channel_id.in_(channel_ids)) | (DDSManualEntry.channel_id.is_(None))
        )
    manual_q = manual_q.group_by("week", DDSManualEntry.category)
    manual_rows = manual_q.all()

    manual_weekly: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in manual_rows:
        period = row.week.strftime("%Y-%m-%d") if row.week else "unknown"
        manual_weekly[period][row.category] += float(row.amount or 0)

    # --- Остатки из DDSBalance ---
    balance_q = db.query(
        func.date_trunc("week", DDSBalance.date).label("week"),
        DDSBalance.account_name,
        DDSBalance.amount,
    ).filter(
        DDSBalance.date >= date_from,
        DDSBalance.date <= date_to,
    ).order_by(DDSBalance.date)
    balance_rows = balance_q.all()

    # --- Остаток на складе по цене закупки (на конец КАЖДОЙ недели периода) ---
    from app.models.inventory import Stock
    from app.models.catalog import SKU

    def _stock_on_date(end_date: date) -> tuple[float, int]:
        """Остаток на складе на конкретную дату (ближайшая ≤ end_date)."""
        latest = db.query(func.max(Stock.date)).filter(Stock.date <= end_date).scalar()
        if not latest:
            return 0.0, 0
        rows = db.query(Stock.sku_id, func.sum(Stock.qty).label("qty")).filter(
            Stock.date == latest
        ).group_by(Stock.sku_id).all()
        total_rub = 0.0
        total_qty = 0
        for sr in rows:
            if sr.sku_id not in cogs_cache:
                cogs_cache[sr.sku_id] = _cogs_for_sku(sr.sku_id)
            qty = int(sr.qty or 0)
            total_qty += qty
            total_rub += qty * cogs_cache[sr.sku_id]
        return total_rub, total_qty

    # Остаток на самую последнюю дату (для "Итого" колонки)
    stock_total_rub, stock_total_qty = _stock_on_date(date_to)

    balance_weekly: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in balance_rows:
        period = row.week.strftime("%Y-%m-%d") if row.week else "unknown"
        balance_weekly[period][row.account_name] = float(row.amount or 0)

    # Собираем все периоды
    all_periods = sorted(set(
        list(auto_weekly.keys()) +
        list(manual_weekly.keys()) +
        list(balance_weekly.keys())
    ))

    # Строим ДДС для каждой недели + месячные subtotals
    periods_result = []
    # Группируем недели по месяцам для subtotals
    from datetime import datetime
    month_weeks: dict[str, list[str]] = defaultdict(list)
    for period in all_periods:
        try:
            dt = datetime.strptime(period, "%Y-%m-%d")
            # Неделя принадлежит месяцу по четвергу (ISO rule)
            thu = dt + timedelta(days=3)
            month_key = thu.strftime("%Y-%m")
        except Exception:
            month_key = period[:7]
        month_weeks[month_key].append(period)

    empty_auto = {"ppvz_for_pay": 0.0, "ppvz_for_pay_wb": 0.0, "ppvz_for_pay_ozon": 0.0, "compensation_wb": 0.0}

    prev_month = None
    for period in all_periods:
        try:
            dt = datetime.strptime(period, "%Y-%m-%d")
            thu = dt + timedelta(days=3)
            cur_month = thu.strftime("%Y-%m")
        except Exception:
            cur_month = period[:7]

        # Если перешли в новый месяц — вставляем subtotal за предыдущий
        if prev_month and cur_month != prev_month:
            month_auto = {k: 0.0 for k in empty_auto}
            month_manual: dict[str, float] = defaultdict(float)
            for wp in month_weeks.get(prev_month, []):
                for k in empty_auto:
                    month_auto[k] += auto_weekly.get(wp, {}).get(k, 0)
                for cat, amt in manual_weekly.get(wp, {}).items():
                    month_manual[cat] += amt
            # Остаток на конец последней недели месяца
            last_wp = month_weeks.get(prev_month, [])[-1] if month_weeks.get(prev_month) else None
            month_stock_rub, month_stock_qty = 0.0, 0
            if last_wp:
                try:
                    month_end = datetime.strptime(last_wp, "%Y-%m-%d").date() + timedelta(days=6)
                    month_stock_rub, month_stock_qty = _stock_on_date(month_end)
                except Exception:
                    pass
            lines = _build_lines(month_auto, month_manual, defaultdict(float), month_stock_rub, month_stock_qty)
            month_names = {"01": "Январь", "02": "Февраль", "03": "Март", "04": "Апрель",
                          "05": "Май", "06": "Июнь", "07": "Июль", "08": "Август",
                          "09": "Сентябрь", "10": "Октябрь", "11": "Ноябрь", "12": "Декабрь"}
            m_label = month_names.get(prev_month[-2:], prev_month[-2:])
            periods_result.append({"period": f"month:{prev_month}", "label": f"Итого {m_label}", "lines": lines, "is_month_total": True})

        auto = auto_weekly.get(period, dict(empty_auto))
        manual = manual_weekly.get(period, defaultdict(float))
        balances = balance_weekly.get(period, defaultdict(float))
        # Остаток на конец этой недели (неделя начинается в period, конец = period+6 дней)
        try:
            week_end = datetime.strptime(period, "%Y-%m-%d").date() + timedelta(days=6)
        except Exception:
            week_end = date_to
        week_stock_rub, week_stock_qty = _stock_on_date(week_end)
        lines = _build_lines(auto, manual, balances, week_stock_rub, week_stock_qty)
        periods_result.append({"period": period, "lines": lines})
        prev_month = cur_month

    # Subtotal для последнего месяца
    if prev_month and month_weeks.get(prev_month):
        month_auto = {k: 0.0 for k in empty_auto}
        month_manual_last: dict[str, float] = defaultdict(float)
        for wp in month_weeks[prev_month]:
            for k in empty_auto:
                month_auto[k] += auto_weekly.get(wp, {}).get(k, 0)
            for cat, amt in manual_weekly.get(wp, {}).items():
                month_manual_last[cat] += amt
        last_wp = month_weeks[prev_month][-1]
        try:
            month_end = datetime.strptime(last_wp, "%Y-%m-%d").date() + timedelta(days=6)
            month_stock_rub, month_stock_qty = _stock_on_date(month_end)
        except Exception:
            month_stock_rub, month_stock_qty = stock_total_rub, stock_total_qty
        lines = _build_lines(month_auto, month_manual_last, defaultdict(float), month_stock_rub, month_stock_qty)
        month_names = {"01": "Январь", "02": "Февраль", "03": "Март", "04": "Апрель",
                      "05": "Май", "06": "Июнь", "07": "Июль", "08": "Август",
                      "09": "Сентябрь", "10": "Октябрь", "11": "Ноябрь", "12": "Декабрь"}
        m_label = month_names.get(prev_month[-2:], prev_month[-2:])
        periods_result.append({"period": f"month:{prev_month}", "label": f"Итого {m_label}", "lines": lines, "is_month_total": True})

    # Итого (всё)
    if all_periods:
        total_auto = {k: sum(auto_weekly.get(p, {}).get(k, 0) for p in all_periods) for k in empty_auto}
        total_manual_all: dict[str, float] = defaultdict(float)
        for p in all_periods:
            for cat, amt in manual_weekly.get(p, {}).items():
                total_manual_all[cat] += amt
        total_balances: dict[str, float] = defaultdict(float)
        if all_periods:
            last_period = all_periods[-1]
            total_balances = balance_weekly.get(last_period, defaultdict(float))
        total_lines = _build_lines(total_auto, total_manual_all, total_balances, stock_total_rub, stock_total_qty)
    else:
        total_lines = []

    return {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "periods": periods_result,
        "total": {"period": "total", "lines": total_lines},
    }


def _manual(manual: dict[str, float], category: str) -> float:
    """Получить сумму ручной записи по категории."""
    return manual.get(category, 0.0)


def _build_lines(auto: dict, manual: dict[str, float], balances: dict[str, float], stock_rub: float = 0, stock_qty: int = 0) -> list:
    """Построить строки ДДС из авто + ручных данных."""

    # === ИНФОРМАЦИЯ ===
    purchase_rub = auto.get("purchase_rub", 0)
    purchase_qty = auto.get("purchase_qty", 0)
    ostatok_nachalo = _manual(manual, "balance_start")

    # === I. ПОСТУПЛЕНИЯ ===
    postuplenie_na_schet = auto.get("ppvz_for_pay", 0)
    postuplenie_wb = auto.get("ppvz_for_pay_wb", 0)
    postuplenie_ozon = auto.get("ppvz_for_pay_ozon", 0)
    kompensacii = auto.get("compensation_wb", 0)
    itogo_postupleniya = postuplenie_na_schet + kompensacii

    # === II. РАСХОДЫ ===
    content = _manual(manual, "content")
    external_ads = _manual(manual, "external_ads")
    buyout_services = _manual(manual, "buyout_services")
    buyout_goods = _manual(manual, "buyout_goods")

    salary_manager = _manual(manual, "salary_manager")
    salary_employee = _manual(manual, "salary_employee")
    salary_smm = _manual(manual, "salary_smm")
    salary_reels = _manual(manual, "salary_reels")
    fot = _manual(manual, "salary") + salary_manager + salary_employee + salary_smm + salary_reels

    outsource_accountant = _manual(manual, "outsource_accountant")
    outsource_it = _manual(manual, "outsource_it")
    outsource_other = _manual(manual, "outsource_other")
    outsource = _manual(manual, "outsource") + outsource_accountant + outsource_it + outsource_other

    warehouse_main = _manual(manual, "warehouse")
    warehouse_kalmykia = _manual(manual, "warehouse_kalmykia")
    warehouse = warehouse_main + warehouse_kalmykia
    courier = _manual(manual, "courier")
    travel = _manual(manual, "travel")
    bank_fees = _manual(manual, "bank_fees")
    office = _manual(manual, "office")
    equipment = _manual(manual, "equipment")
    education = _manual(manual, "education")
    subscriptions = _manual(manual, "subscriptions")
    new_products = _manual(manual, "new_products")
    pvz = _manual(manual, "pvz")

    delivery_rf = _manual(manual, "delivery_rf")

    itogo_rashody = (
        content + external_ads + buyout_services + buyout_goods +
        fot + outsource + warehouse + courier + travel + delivery_rf +
        bank_fees + office + equipment + education +
        subscriptions + new_products + pvz
    )

    # === III. НАЛОГИ ===
    usn = _manual(manual, "usn")
    insurance = _manual(manual, "insurance")
    ndfl = _manual(manual, "ndfl")
    itogo_nalogi = usn + insurance + ndfl

    # === IV. АВАНСЫ (ЗАКУПКА) ===
    purchase_china = _manual(manual, "purchase_china")
    delivery_china = _manual(manual, "delivery_china")
    ff = _manual(manual, "ff")
    ff_storage = _manual(manual, "ff_storage")
    delivery_mp = _manual(manual, "delivery_mp")
    itogo_avansy = purchase_china + delivery_china + ff + ff_storage + delivery_mp

    # === V. КРЕДИТЫ И УДЕРЖАНИЯ ===
    wb_deductions = _manual(manual, "wb_deductions")
    bank_credit = _manual(manual, "bank_credit")
    credit_interest = _manual(manual, "credit_interest")
    itogo_kredity = wb_deductions + bank_credit + credit_interest

    # === VI. ДИВИДЕНДЫ ===
    dividend_investor = _manual(manual, "dividend_investor")
    dividend_manager = _manual(manual, "dividend_manager")
    dividend_other = _manual(manual, "dividend_other")
    itogo_dividendy = dividend_investor + dividend_manager + dividend_other

    # === ИТОГО ===
    total_rashody = itogo_rashody + itogo_nalogi + itogo_avansy + itogo_kredity + itogo_dividendy
    chisty_potok = itogo_postupleniya - total_rashody

    ostatok_nachalo = _manual(manual, "balance_start")
    ostatok_konec = ostatok_nachalo + chisty_potok

    # Балансы по счетам
    balance_accounts = sorted(balances.keys()) if balances else []

    lines = [
        # ИНФОРМАЦИЯ
        {"key": "purchase_rub", "name": "Поступление товара по цене закупки, руб.", "amount": round(purchase_rub, 2), "level": 0, "bold": False, "editable": False, "section": "info", "category": None},
        {"key": "purchase_qty", "name": "Поступление товара, шт.", "amount": purchase_qty, "level": 0, "bold": False, "editable": False, "section": "info", "category": None},
        {"key": "stock_rub", "name": "Доступно на складе по цене закупки", "amount": round(stock_rub, 2), "level": 0, "bold": False, "editable": False, "section": "info", "category": None},
        {"key": "ostatok_nachalo", "name": "Остаток ДС на начало периода", "amount": round(ostatok_nachalo, 2), "level": 0, "bold": True, "editable": True, "section": "info", "category": "balance_start"},

        # I. ПОСТУПЛЕНИЯ
        {"key": "section_income", "name": "I. ПОСТУПЛЕНИЯ", "amount": 0, "level": 0, "bold": True, "editable": False, "section": "income", "category": None},
        {"key": "postuplenie_na_schet", "name": "Поступление на счёт", "amount": round(postuplenie_na_schet, 2), "level": 1, "bold": False, "editable": False, "section": "income", "category": None},
        {"key": "postuplenie_wb", "name": "в т.ч. ВБ", "amount": round(postuplenie_wb, 2), "level": 2, "bold": False, "editable": False, "section": "income", "category": None},
        {"key": "postuplenie_ozon", "name": "в т.ч. Озон", "amount": round(postuplenie_ozon, 2), "level": 2, "bold": False, "editable": False, "section": "income", "category": None},
        {"key": "kompensacii", "name": "Компенсации", "amount": round(kompensacii, 2), "level": 1, "bold": False, "editable": False, "section": "income", "category": None},
        {"key": "itogo_postupleniya", "name": "Итого поступления", "amount": round(itogo_postupleniya, 2), "level": 0, "bold": True, "editable": False, "section": "income", "category": None},

        # II. РАСХОДЫ
        {"key": "section_expenses", "name": "II. РАСХОДЫ — ФАКТ СПИСАНИЯ", "amount": 0, "level": 0, "bold": True, "editable": False, "section": "expenses", "category": None},
        {"key": "content", "name": "Контент", "amount": round(content, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "content"},
        {"key": "external_ads", "name": "Продвижение внешнее", "amount": round(external_ads, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "external_ads"},
        {"key": "buyout_services", "name": "Выкупы-услуги", "amount": round(buyout_services, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "buyout_services"},
        {"key": "buyout_goods", "name": "Выкупы товар", "amount": round(buyout_goods, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "buyout_goods"},
        {"key": "fot", "name": "ФОТ", "amount": round(fot, 2), "level": 1, "bold": True, "editable": True, "section": "expenses", "category": "salary"},
        {"key": "salary_manager", "name": "Управляющий", "amount": round(salary_manager, 2), "level": 2, "bold": False, "editable": True, "section": "expenses", "category": "salary_manager"},
        {"key": "salary_employee", "name": "Менеджер", "amount": round(salary_employee, 2), "level": 2, "bold": False, "editable": True, "section": "expenses", "category": "salary_employee"},
        {"key": "salary_smm", "name": "СММ-менеджер", "amount": round(salary_smm, 2), "level": 2, "bold": False, "editable": True, "section": "expenses", "category": "salary_smm"},
        {"key": "salary_reels", "name": "Рилзмейкер", "amount": round(salary_reels, 2), "level": 2, "bold": False, "editable": True, "section": "expenses", "category": "salary_reels"},
        {"key": "outsource", "name": "Аутсорс", "amount": round(outsource, 2), "level": 1, "bold": True, "editable": True, "section": "expenses", "category": "outsource"},
        {"key": "outsource_accountant", "name": "Бухгалтер", "amount": round(outsource_accountant, 2), "level": 2, "bold": False, "editable": True, "section": "expenses", "category": "outsource_accountant"},
        {"key": "outsource_it", "name": "ИТ-программист", "amount": round(outsource_it, 2), "level": 2, "bold": False, "editable": True, "section": "expenses", "category": "outsource_it"},
        {"key": "outsource_other", "name": "Другое", "amount": round(outsource_other, 2), "level": 2, "bold": False, "editable": True, "section": "expenses", "category": "outsource_other"},
        {"key": "warehouse", "name": "Склад", "amount": round(warehouse, 2), "level": 1, "bold": True, "editable": False, "section": "expenses", "category": "warehouse"},
        {"key": "warehouse_main", "name": "Склад основной", "amount": round(warehouse_main, 2), "level": 2, "bold": False, "editable": True, "section": "expenses", "category": "warehouse"},
        {"key": "warehouse_kalmykia", "name": "Склад Калмыкия", "amount": round(warehouse_kalmykia, 2), "level": 2, "bold": False, "editable": True, "section": "expenses", "category": "warehouse_kalmykia"},
        {"key": "courier", "name": "Курьерская доставка", "amount": round(courier, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "courier"},
        {"key": "travel", "name": "Командировки", "amount": round(travel, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "travel"},
        {"key": "bank_fees", "name": "Комиссии банков", "amount": round(bank_fees, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "bank_fees"},
        {"key": "office", "name": "Офисные нужды", "amount": round(office, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "office"},
        {"key": "equipment", "name": "Оборудование", "amount": round(equipment, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "equipment"},
        {"key": "education", "name": "Обучение", "amount": round(education, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "education"},
        {"key": "subscriptions", "name": "Подписка на сервисы", "amount": round(subscriptions, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "subscriptions"},
        {"key": "new_products", "name": "Новинки", "amount": round(new_products, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "new_products"},
        {"key": "pvz", "name": "ПВЗ расходы", "amount": round(pvz, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "pvz"},
        {"key": "delivery_rf", "name": "Доставка внутри РФ", "amount": round(delivery_rf, 2), "level": 1, "bold": False, "editable": True, "section": "expenses", "category": "delivery_rf"},
        {"key": "itogo_rashody", "name": "Итого расходы", "amount": round(itogo_rashody, 2), "level": 0, "bold": True, "editable": False, "section": "expenses", "category": None},

        # III. НАЛОГИ
        {"key": "section_taxes", "name": "III. НАЛОГИ", "amount": 0, "level": 0, "bold": True, "editable": False, "section": "taxes", "category": None},
        {"key": "usn", "name": "УСН и взнос 1%", "amount": round(usn, 2), "level": 1, "bold": False, "editable": True, "section": "taxes", "category": "usn"},
        {"key": "insurance", "name": "Страховые взносы", "amount": round(insurance, 2), "level": 1, "bold": False, "editable": True, "section": "taxes", "category": "insurance"},
        {"key": "ndfl", "name": "НДФЛ", "amount": round(ndfl, 2), "level": 1, "bold": False, "editable": True, "section": "taxes", "category": "ndfl"},
        {"key": "itogo_nalogi", "name": "Итого налоги", "amount": round(itogo_nalogi, 2), "level": 0, "bold": True, "editable": False, "section": "taxes", "category": None},

        # IV. АВАНСЫ (ЗАКУПКА)
        {"key": "section_advances", "name": "IV. АВАНСЫ (ЗАКУПКА)", "amount": 0, "level": 0, "bold": True, "editable": False, "section": "advances", "category": None},
        {"key": "purchase_china", "name": "Закупка Китай", "amount": round(purchase_china, 2), "level": 1, "bold": False, "editable": True, "section": "advances", "category": "purchase_china"},
        {"key": "delivery_china", "name": "Доставка Китай", "amount": round(delivery_china, 2), "level": 1, "bold": False, "editable": True, "section": "advances", "category": "delivery_china"},
        {"key": "ff", "name": "ФФ", "amount": round(ff, 2), "level": 1, "bold": False, "editable": True, "section": "advances", "category": "ff"},
        {"key": "ff_storage", "name": "Хранение на ФФ", "amount": round(ff_storage, 2), "level": 1, "bold": False, "editable": True, "section": "advances", "category": "ff_storage"},
        {"key": "delivery_mp", "name": "Доставка до МП", "amount": round(delivery_mp, 2), "level": 1, "bold": False, "editable": True, "section": "advances", "category": "delivery_mp"},
        {"key": "itogo_avansy", "name": "Итого авансы", "amount": round(itogo_avansy, 2), "level": 0, "bold": True, "editable": False, "section": "advances", "category": None},

        # V. КРЕДИТЫ И УДЕРЖАНИЯ
        {"key": "section_credits", "name": "V. КРЕДИТЫ И УДЕРЖАНИЯ", "amount": 0, "level": 0, "bold": True, "editable": False, "section": "credits", "category": None},
        {"key": "wb_deductions", "name": "Удержания ВБ", "amount": round(wb_deductions, 2), "level": 1, "bold": False, "editable": True, "section": "credits", "category": "wb_deductions"},
        {"key": "bank_credit", "name": "Банковские кредиты", "amount": round(bank_credit, 2), "level": 1, "bold": False, "editable": True, "section": "credits", "category": "bank_credit"},
        {"key": "credit_interest", "name": "% по кредитам", "amount": round(credit_interest, 2), "level": 1, "bold": False, "editable": True, "section": "credits", "category": "credit_interest"},
        {"key": "itogo_kredity", "name": "Итого кредиты", "amount": round(itogo_kredity, 2), "level": 0, "bold": True, "editable": False, "section": "credits", "category": None},

        # VI. ДИВИДЕНДЫ
        {"key": "section_dividends", "name": "VI. ДИВИДЕНДЫ", "amount": 0, "level": 0, "bold": True, "editable": False, "section": "dividends", "category": None},
        {"key": "dividend_investor", "name": "Инвестор", "amount": round(dividend_investor, 2), "level": 1, "bold": False, "editable": True, "section": "dividends", "category": "dividend_investor"},
        {"key": "dividend_manager", "name": "Управляющий", "amount": round(dividend_manager, 2), "level": 1, "bold": False, "editable": True, "section": "dividends", "category": "dividend_manager"},
        {"key": "dividend_other", "name": "Прочее", "amount": round(dividend_other, 2), "level": 1, "bold": False, "editable": True, "section": "dividends", "category": "dividend_other"},
        {"key": "itogo_dividendy", "name": "Итого дивиденды", "amount": round(itogo_dividendy, 2), "level": 0, "bold": True, "editable": False, "section": "dividends", "category": None},

        # ИТОГО
        {"key": "section_total", "name": "ИТОГО", "amount": 0, "level": 0, "bold": True, "editable": False, "section": "total", "category": None},
        {"key": "chisty_potok", "name": "Чистый денежный поток", "amount": round(chisty_potok, 2), "level": 0, "bold": True, "editable": False, "section": "total", "category": None},
        {"key": "ostatok_nachalo", "name": "Остаток на начало", "amount": round(ostatok_nachalo, 2), "level": 1, "bold": False, "editable": True, "section": "total", "category": "balance_start"},
        {"key": "ostatok_konec", "name": "Остаток на конец", "amount": round(ostatok_konec, 2), "level": 1, "bold": True, "editable": False, "section": "total", "category": None},
    ]

    # Балансы по счетам
    if balance_accounts:
        lines.append({"key": "section_balances", "name": "Баланс по счетам", "amount": 0, "level": 0, "bold": True, "editable": False, "section": "balances", "category": None})
        for acc_name in balance_accounts:
            safe_key = "balance_" + acc_name.lower().replace(" ", "_").replace("-", "_")
            lines.append({
                "key": safe_key,
                "name": acc_name,
                "amount": round(balances[acc_name], 2),
                "level": 1,
                "bold": False,
                "editable": True,
                "section": "balances",
                "category": f"balance:{acc_name}",
            })

    return lines
