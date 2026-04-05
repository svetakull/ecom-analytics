"""Управленческий баланс — snapshot активов, пассивов, капитала."""
from collections import defaultdict
from datetime import date
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.catalog import Channel, ChannelType
from app.models.finance import BalanceSheetManualEntry, DDSBalance
from app.models.inventory import SKUCostHistory, Stock
from app.models.sales import SkuDailyExpense


def _cogs_map(db: Session, as_of: date) -> dict[int, float]:
    """SKU ID → себестоимость за единицу.
    Приоритет как в РнП: cost_prices → SKUCostHistory → ProductBatch."""
    from app.models.inventory import ProductBatch
    result: dict[int, float] = {}

    # 1) Новый модуль cost_prices
    try:
        from app.services.cost_price_service import resolve_cogs_per_unit
        # Получаем все sku_channels чтобы вызвать resolve по каждому
        from app.models.catalog import SKUChannel
        sc_rows = db.query(SKUChannel.sku_id, SKUChannel.channel_id).all()
        for sku_id, channel_id in sc_rows:
            if sku_id in result:
                continue
            try:
                val = resolve_cogs_per_unit(db, sku_id, channel_id, as_of)
                if val > 0:
                    result[sku_id] = float(val)
            except Exception:
                pass
    except Exception:
        pass

    # 2) SKUCostHistory — для тех, кого не нашли в cost_prices
    rows = db.query(SKUCostHistory).filter(SKUCostHistory.effective_from <= as_of).order_by(
        SKUCostHistory.sku_id, SKUCostHistory.effective_from.desc()
    ).all()
    for r in rows:
        if r.sku_id not in result:
            result[r.sku_id] = float(r.cost_per_unit)

    # 3) ProductBatch fallback — для тех, кого нет в cost_prices/SKUCostHistory
    batches = db.query(ProductBatch).order_by(
        ProductBatch.sku_id, ProductBatch.batch_date.desc()
    ).all()
    for b in batches:
        if b.sku_id not in result:
            result[b.sku_id] = float(b.total_cost_per_unit or 0)

    return result


def _stock_snapshot(db: Session, as_of: date) -> dict:
    """Остатки на дату: qty, in_way_to, in_way_from по SKU."""
    last_date = db.query(func.max(Stock.date)).filter(Stock.date <= as_of).scalar()
    if not last_date:
        return {"qty": {}, "in_way_to": {}, "in_way_from": {}}

    rows = db.query(
        Stock.sku_id,
        func.sum(Stock.qty).label("qty"),
        func.sum(Stock.in_way_to_client).label("to_client"),
        func.sum(Stock.in_way_from_client).label("from_client"),
    ).filter(Stock.date == last_date).group_by(Stock.sku_id).all()

    qty, to_cl, from_cl = {}, {}, {}
    for r in rows:
        qty[r.sku_id] = int(r.qty or 0)
        to_cl[r.sku_id] = int(r.to_client or 0)
        from_cl[r.sku_id] = int(r.from_client or 0)
    return {"qty": qty, "in_way_to": to_cl, "in_way_from": from_cl}


def _cash_balances(db: Session, as_of: date) -> dict[str, float]:
    """Остатки по счетам. Источники:
    1) DDSManualEntry с категориями balance_acc:*, mp_balance_*, mp_transit (последнее значение ≤ as_of)
    2) DDSBalance (устаревший)."""
    from app.models.finance import DDSManualEntry
    balances: dict[str, float] = {}

    # Из DDS manual entries (balance_acc:Счёт — это банковские счета)
    # mp_balance_* и mp_transit идут в ДЕБИТОРКУ МП, а не в ДС
    entries = db.query(DDSManualEntry).filter(
        DDSManualEntry.date <= as_of,
        DDSManualEntry.category.like("balance_acc:%"),
    ).order_by(DDSManualEntry.date.desc(), DDSManualEntry.id.desc()).all()
    seen = set()
    for e in entries:
        if e.category in seen:
            continue
        seen.add(e.category)
        name = e.category.split(":", 1)[1]
        balances[name] = float(e.amount or 0)

    # Фолбэк: устаревший DDSBalance
    if not balances:
        last_date = db.query(func.max(DDSBalance.date)).filter(DDSBalance.date <= as_of).scalar()
        if last_date:
            rows = db.query(DDSBalance.account_name, DDSBalance.amount).filter(DDSBalance.date == last_date).all()
            balances = {r.account_name: float(r.amount) for r in rows}
    return balances


def _mp_manual_balances(db: Session, as_of: date) -> dict[str, float]:
    """Ручные остатки МП из DDS: mp_balance_wb/ozon/mp_transit."""
    from app.models.finance import DDSManualEntry
    result: dict[str, float] = {}
    entries = db.query(DDSManualEntry).filter(
        DDSManualEntry.date <= as_of,
        DDSManualEntry.category.in_(["mp_balance_wb", "mp_balance_ozon", "mp_transit"]),
    ).order_by(DDSManualEntry.date.desc(), DDSManualEntry.id.desc()).all()
    seen = set()
    for e in entries:
        if e.category in seen:
            continue
        seen.add(e.category)
        result[e.category] = float(e.amount or 0)
    return result


def _mp_receivables(db: Session, as_of: date) -> dict[str, float]:
    """Дебиторская задолженность МП = Итого к оплате за неоплаченные недели.
    Формула: ppvz_for_pay + compensation_wb - logistics - storage - acceptance
             - advertising - subscription - reviews - other_deductions - penalty - credit_deduction
    Это соответствует колонке "Итого к оплате" в еженедельном финотчёте WB."""
    from datetime import timedelta
    delays = {"wb": 28, "ozon": 24, "lamoda": 8}
    result = {}
    type_map = {"wb": ChannelType.WB, "ozon": ChannelType.OZON, "lamoda": ChannelType.LAMODA}

    # Ручные балансы МП — приоритет над автоматическим расчётом
    manual_mp = _mp_manual_balances(db, as_of)
    manual_key_map = {"wb": "mp_balance_wb", "ozon": "mp_balance_ozon"}

    for mp, delay_days in delays.items():
        # Если пользователь задал ручной баланс — используем его
        manual_key = manual_key_map.get(mp)
        if manual_key and manual_key in manual_mp:
            result[mp] = manual_mp[manual_key]
            continue
        ct = type_map.get(mp)
        ch = db.query(Channel).filter(Channel.type == ct).first() if ct else None
        if not ch:
            continue
        cutoff = as_of - timedelta(days=delay_days)
        row = db.query(
            func.sum(SkuDailyExpense.ppvz_for_pay).label("ppvz"),
            func.sum(SkuDailyExpense.sale_amount).label("sale_amount"),
            func.sum(SkuDailyExpense.compensation_wb).label("comp_wb"),
            func.sum(SkuDailyExpense.logistics).label("logistics"),
            func.sum(SkuDailyExpense.storage).label("storage"),
            func.sum(SkuDailyExpense.acceptance).label("acceptance"),
            func.sum(SkuDailyExpense.advertising).label("advertising"),
            func.sum(SkuDailyExpense.subscription).label("subscription"),
            func.sum(SkuDailyExpense.reviews).label("reviews"),
            func.sum(SkuDailyExpense.other_deductions).label("other_ded"),
            func.sum(SkuDailyExpense.penalty).label("penalty"),
            func.sum(SkuDailyExpense.credit_deduction).label("credit"),
            func.sum(SkuDailyExpense.commission).label("commission"),
            func.sum(SkuDailyExpense.acquiring).label("acquiring"),
        ).filter(
            SkuDailyExpense.channel_id == ch.id,
            SkuDailyExpense.date > cutoff,
            SkuDailyExpense.date <= as_of,
        ).first()
        if row:
            # WB: ppvz_for_pay это уже "К перечислению" (чистая сумма)
            # Ozon: sale_amount (accruals) минус commission, logistics, acquiring
            if mp == "wb":
                base = float(row.ppvz or 0)
                itogo = (
                    base
                    + float(row.comp_wb or 0)
                    - float(row.logistics or 0)
                    - float(row.storage or 0)
                    - float(row.acceptance or 0)
                    - float(row.advertising or 0)
                    - float(row.subscription or 0)
                    - float(row.reviews or 0)
                    - float(row.other_ded or 0)
                    - float(row.penalty or 0)
                    - float(row.credit or 0)
                )
            else:
                # Ozon/Lamoda: sale_amount (accruals) минус все удержания
                itogo = (
                    float(row.sale_amount or 0)
                    - float(row.commission or 0)
                    - float(row.logistics or 0)
                    - float(row.acquiring or 0)
                    - float(row.other_ded or 0)
                    - float(row.storage or 0)
                    - float(row.penalty or 0)
                    - float(row.acceptance or 0)
                )
            result[mp] = max(itogo, 0)
        else:
            result[mp] = 0.0
    return result


def _retained_earnings(db: Session, as_of: date) -> float:
    """Нераспределённая прибыль = кумулятивная чистая прибыль.

    = валовая прибыль от МП (accruals − все удержания МП − COGS)
      − операционные расходы (из DDS: ФОТ, аренда, реклама внешн, бухгалтер, и т.д.)
      − налоги (УСН, НДС, страховые, НДФЛ, таможня)
      − % по кредитам (тело не вычитаем — это обязательство)
      − дивиденды выплаченные
      + вложения инвесторов и кредитные средства НЕ добавляем (это пассив/капитал)
    """
    # 1) Валовая прибыль от МП (то что остаётся после прямых удержаний)
    row = db.query(
        func.sum(SkuDailyExpense.compensation - SkuDailyExpense.commission
                 - SkuDailyExpense.logistics - SkuDailyExpense.storage
                 - SkuDailyExpense.advertising - SkuDailyExpense.subscription
                 - SkuDailyExpense.reviews - SkuDailyExpense.other_deductions
                 - SkuDailyExpense.penalty - SkuDailyExpense.acceptance
                 - SkuDailyExpense.acquiring)
    ).filter(SkuDailyExpense.date <= as_of).scalar()
    gross_mp = float(row or 0)

    # 2) COGS проданных товаров = sum(items_count × cogs_per_unit) по каждому SKU
    cogs_map = _cogs_map(db, as_of)
    items_per_sku = db.query(
        SkuDailyExpense.sku_id,
        func.sum(SkuDailyExpense.items_count - SkuDailyExpense.return_count).label("net_items")
    ).filter(SkuDailyExpense.date <= as_of).group_by(SkuDailyExpense.sku_id).all()
    cogs_estimate = 0.0
    for r in items_per_sku:
        c = cogs_map.get(r.sku_id, 900)
        cogs_estimate += max(int(r.net_items or 0), 0) * c

    # 3) Операционные расходы из DDS (за вычетом доходных категорий, капитала и тела кредитов)
    from app.models.finance import DDSManualEntry
    exclude_cats = {
        # Доходы (income) — не уменьшают прибыль
        "income_wb", "income_ozon", "income_lamoda", "income_site",
        "income_opt", "income_pvz", "income_deposit", "mp_payment",
        # Капитал/пассивы (не уменьшают прибыль)
        "investor_contribution", "credit_received",
        # Тело кредита — это погашение обязательства, не расход
        "bank_credit", "wb_deductions",
        # Балансы счетов
    }
    opex_row = db.query(func.sum(DDSManualEntry.amount)).filter(
        DDSManualEntry.date <= as_of,
        ~DDSManualEntry.category.in_(exclude_cats),
        ~DDSManualEntry.category.like("balance_acc:%"),
        ~DDSManualEntry.category.in_(["mp_balance_wb", "mp_balance_ozon", "mp_transit", "balance_start"]),
    ).scalar()
    opex = float(opex_row or 0)

    return gross_mp - cogs_estimate - opex


def _calc_section(db: Session, as_of: date, cogs: dict, stocks: dict) -> dict:
    """Рассчитать все секции баланса."""
    # --- АКТИВЫ ---
    assets_lines = []

    # 1. Денежные средства
    cash = _cash_balances(db, as_of)
    total_cash = sum(cash.values())
    assets_lines.append({"key": "cash", "name": "Денежные средства", "amount": round(total_cash, 2), "source": "auto", "editable": False, "level": 0, "bold": True})
    for acc, amt in sorted(cash.items()):
        assets_lines.append({"key": f"cash_{acc}", "name": acc, "amount": round(amt, 2), "source": "auto", "editable": True, "level": 1, "bold": False})

    # 2. Товары на складах МП
    stock_value = sum(stocks["qty"].get(sid, 0) * cogs.get(sid, 0) for sid in set(stocks["qty"]) | set(cogs))
    assets_lines.append({"key": "stock_mp", "name": "Товары на складах МП", "amount": round(stock_value, 2), "source": "auto", "editable": False, "level": 0, "bold": False})

    # 2b. Товары на собственном складе (ручной ввод)
    own_warehouse_rec = db.query(BalanceSheetManualEntry).filter(
        BalanceSheetManualEntry.section == "assets",
        BalanceSheetManualEntry.category == "own_warehouse",
        BalanceSheetManualEntry.date <= as_of,
    ).order_by(BalanceSheetManualEntry.date.desc(), BalanceSheetManualEntry.id.desc()).first()
    own_warehouse_val = float(own_warehouse_rec.amount) if own_warehouse_rec else 0
    if own_warehouse_val > 0:
        assets_lines.append({
            "key": "stock_own", "name": "Товары на собственном складе",
            "amount": round(own_warehouse_val, 2),
            "source": "manual", "editable": True, "level": 0, "bold": False,
            "entry_id": own_warehouse_rec.id if own_warehouse_rec else None,
        })

    # 3. В пути к клиенту
    transit_to = sum(stocks["in_way_to"].get(sid, 0) * cogs.get(sid, 0) for sid in set(stocks["in_way_to"]) | set(cogs))
    assets_lines.append({"key": "in_transit_to", "name": "Товары в пути к клиенту", "amount": round(transit_to, 2), "source": "auto", "editable": False, "level": 0, "bold": False})

    # 4. В пути от клиента (возвраты)
    transit_from = sum(stocks["in_way_from"].get(sid, 0) * cogs.get(sid, 0) for sid in set(stocks["in_way_from"]) | set(cogs))
    assets_lines.append({"key": "in_transit_from", "name": "Товары в пути (возвраты)", "amount": round(transit_from, 2), "source": "auto", "editable": False, "level": 0, "bold": False})

    # 5. Дебиторская задолженность МП
    receivables = _mp_receivables(db, as_of)
    total_recv = sum(receivables.values())
    assets_lines.append({"key": "receivables", "name": "Дебиторская задолженность МП", "amount": round(total_recv, 2), "source": "auto", "editable": False, "level": 0, "bold": True})
    for mp, amt in receivables.items():
        label = {"wb": "WB", "ozon": "Ozon", "lamoda": "Lamoda"}.get(mp, mp)
        assets_lines.append({"key": f"recv_{mp}", "name": f"в т.ч. {label}", "amount": round(amt, 2), "source": "auto", "editable": False, "level": 1, "bold": False})

    # 6. Ручные активы
    manual_assets = db.query(BalanceSheetManualEntry).filter(
        BalanceSheetManualEntry.section == "assets",
        BalanceSheetManualEntry.date <= as_of,
    ).order_by(BalanceSheetManualEntry.date.desc()).all()
    # Берём последние по каждой категории
    seen_cats = set()
    for e in manual_assets:
        if e.category not in seen_cats:
            seen_cats.add(e.category)
            assets_lines.append({"key": f"manual_{e.category}", "name": e.name, "amount": float(e.amount), "source": "manual", "editable": True, "level": 0, "bold": False, "entry_id": e.id})

    total_assets = total_cash + stock_value + own_warehouse_val + transit_to + transit_from + total_recv + sum(float(e.amount) for e in manual_assets if e.category in seen_cats)
    assets_lines.append({"key": "total_assets", "name": "ИТОГО АКТИВЫ", "amount": round(total_assets, 2), "source": "auto", "editable": False, "level": 0, "bold": True})

    # --- ПАССИВЫ ---
    liabilities_lines = []
    total_liabilities = 0.0

    # Кредиты и займы (автоматически из Credit/CreditPayment)
    from app.models.finance import Credit, CreditPayment
    credits_active = db.query(Credit).filter(Credit.is_active == True).all()
    credits_total = 0.0
    credits_details = []
    for cr in credits_active:
        # Остаток = principal - сумма тела уже выплаченного (payment_date <= as_of)
        body_paid = db.query(func.coalesce(func.sum(CreditPayment.body_amount), 0)).filter(
            CreditPayment.credit_id == cr.id,
            CreditPayment.payment_date <= as_of,
        ).scalar()
        balance = max(float(cr.principal or 0) - float(body_paid or 0), 0.0)
        if balance > 0.01:
            credits_total += balance
            credits_details.append({
                "key": f"liab_credit_{cr.id}",
                "name": cr.name,
                "amount": round(balance, 2),
                "source": "auto",
                "editable": False,
                "level": 1,
                "bold": False,
            })

    if credits_total > 0:
        liabilities_lines.append({
            "key": "liab_credits_group", "name": "Кредиты и займы",
            "amount": round(credits_total, 2), "source": "auto",
            "editable": False, "level": 0, "bold": True,
        })
        liabilities_lines.extend(credits_details)
        total_liabilities += credits_total

    manual_liab = db.query(BalanceSheetManualEntry).filter(
        BalanceSheetManualEntry.section == "liabilities",
        BalanceSheetManualEntry.date <= as_of,
    ).order_by(BalanceSheetManualEntry.date.desc()).all()
    seen_cats = set()
    for e in manual_liab:
        if e.category not in seen_cats:
            seen_cats.add(e.category)
            liabilities_lines.append({"key": f"liab_{e.category}", "name": e.name, "amount": float(e.amount), "source": "manual", "editable": True, "level": 0, "bold": False, "entry_id": e.id})
            total_liabilities += float(e.amount)

    liabilities_lines.append({"key": "total_liabilities", "name": "ИТОГО ПАССИВЫ", "amount": round(total_liabilities, 2), "source": "auto", "editable": False, "level": 0, "bold": True})

    # --- КАПИТАЛ ---
    equity_lines = []
    total_equity = 0.0

    # Ручной уставный капитал
    manual_eq = db.query(BalanceSheetManualEntry).filter(
        BalanceSheetManualEntry.section == "equity",
        BalanceSheetManualEntry.date <= as_of,
    ).order_by(BalanceSheetManualEntry.date.desc()).all()
    seen_cats = set()
    for e in manual_eq:
        if e.category not in seen_cats:
            seen_cats.add(e.category)
            equity_lines.append({"key": f"eq_{e.category}", "name": e.name, "amount": float(e.amount), "source": "manual", "editable": True, "level": 0, "bold": False, "entry_id": e.id})
            total_equity += float(e.amount)

    # Нераспределённая прибыль
    retained = _retained_earnings(db, as_of)
    equity_lines.append({"key": "retained_earnings", "name": "Нераспределённая прибыль", "amount": round(retained, 2), "source": "auto", "editable": False, "level": 0, "bold": False})
    total_equity += retained

    equity_lines.append({"key": "total_equity", "name": "ИТОГО КАПИТАЛ", "amount": round(total_equity, 2), "source": "auto", "editable": False, "level": 0, "bold": True})

    return {
        "assets": {"lines": assets_lines, "total": round(total_assets, 2)},
        "liabilities": {"lines": liabilities_lines, "total": round(total_liabilities, 2)},
        "equity": {"lines": equity_lines, "total": round(total_equity, 2)},
    }


def get_balance_sheet(
    db: Session,
    as_of_date: Optional[date] = None,
    compare_date: Optional[date] = None,
) -> dict:
    """Управленческий баланс на дату с опциональным сравнением."""
    if not as_of_date:
        as_of_date = date.today()

    cogs = _cogs_map(db, as_of_date)
    stocks = _stock_snapshot(db, as_of_date)
    main = _calc_section(db, as_of_date, cogs, stocks)

    # Сравнение
    if compare_date:
        cogs2 = _cogs_map(db, compare_date)
        stocks2 = _stock_snapshot(db, compare_date)
        comp = _calc_section(db, compare_date, cogs2, stocks2)
        # Добавляем compare_amount к main lines
        for section_key in ("assets", "liabilities", "equity"):
            comp_map = {l["key"]: l["amount"] for l in comp[section_key]["lines"]}
            for line in main[section_key]["lines"]:
                line["compare_amount"] = comp_map.get(line["key"], 0)

    total_assets = main["assets"]["total"]
    total_le = main["liabilities"]["total"] + main["equity"]["total"]
    imbalance = round(total_assets - total_le, 2)

    return {
        "as_of_date": as_of_date.isoformat(),
        "compare_date": compare_date.isoformat() if compare_date else None,
        "sections": [
            {"key": "assets", "name": "АКТИВЫ", "lines": main["assets"]["lines"]},
            {"key": "liabilities", "name": "ПАССИВЫ", "lines": main["liabilities"]["lines"]},
            {"key": "equity", "name": "КАПИТАЛ", "lines": main["equity"]["lines"]},
        ],
        "balanced": abs(imbalance) < 1,
        "imbalance": imbalance,
        "total_assets": total_assets,
        "total_liabilities_equity": round(total_le, 2),
    }
