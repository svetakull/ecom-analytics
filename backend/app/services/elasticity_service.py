"""
Ценовая аналитика — эластичность цены и прогноз.
Расчёт на основе исторических данных WB (Orders + SkuDailyExpense).
"""
import math
import logging
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import func, and_
from sqlalchemy.orm import Session

from app.models.catalog import Channel, ChannelType, SKU
from app.models.inventory import SKUCostHistory, ProductBatch, Stock
from app.models.sales import Order, OrderStatus, CardStats, SkuDailyExpense, Price

logger = logging.getLogger(__name__)


# ─── Вспомогательные функции ──────────────────────────────────

def _cogs_per_unit(db: Session, sku_id: int, ref_date: date) -> float:
    """Себестоимость на единицу (из SKUCostHistory → ProductBatch)."""
    rec = (
        db.query(SKUCostHistory)
        .filter(SKUCostHistory.sku_id == sku_id, SKUCostHistory.effective_from <= ref_date)
        .order_by(SKUCostHistory.effective_from.desc())
        .first()
    )
    if rec:
        return float(rec.cost_per_unit)
    batch = (
        db.query(ProductBatch)
        .filter(ProductBatch.sku_id == sku_id)
        .order_by(ProductBatch.batch_date.desc())
        .first()
    )
    return float(batch.purchase_cost) if batch and batch.purchase_cost else 0.0


def _current_stock(db: Session, sku_id: int) -> int:
    """Текущий остаток на складах (последняя дата синхронизации)."""
    last_date = (
        db.query(func.max(Stock.date))
        .filter(Stock.sku_id == sku_id)
        .scalar()
    )
    if not last_date:
        return 0
    result = (
        db.query(func.sum(Stock.qty))
        .filter(Stock.sku_id == sku_id, Stock.date == last_date)
        .scalar()
    )
    return int(result or 0)


def _get_wb_channel(db: Session) -> Optional[Channel]:
    return db.query(Channel).filter(Channel.type == ChannelType.WB).first()


# ─── Расчёт эластичности ─────────────────────────────────────

def calculate_elasticity(db: Session, sku_id: int, channel_id: int, lookback_days: int = 120) -> dict:
    """
    Рассчитать коэффициент эластичности для SKU.

    Метод: берём заказы по дням, группируем в 7-дневные окна.
    Для каждого окна: средняя цена (price_after_spp) и кол-во заказов.
    Log-log регрессия: ln(Q) = a + E * ln(P)
    E = коэффициент эластичности.

    Returns: {elasticity, r_squared, data_points, avg_price, avg_orders_day,
              scatter: [{price, orders}]}
    """
    date_to = date.today()
    date_from = date_to - timedelta(days=lookback_days)

    # Агрегируем заказы по дням
    # Используем price (цена до СПП) для расчёта эластичности
    daily = (
        db.query(
            Order.order_date,
            func.sum(Order.qty).label("qty"),
            func.avg(Order.price).label("avg_price"),
        )
        .filter(
            Order.sku_id == sku_id,
            Order.channel_id == channel_id,
            Order.order_date >= date_from,
            Order.order_date <= date_to,
            Order.price > 0,
            Order.status.notin_([OrderStatus.CANCELLED, OrderStatus.RETURNED]),
        )
        .group_by(Order.order_date)
        .order_by(Order.order_date)
        .all()
    )

    if len(daily) < 14:
        return {"elasticity": -1.0, "r_squared": 0, "data_points": len(daily),
                "avg_price": 0, "avg_orders_day": 0, "scatter": [], "insufficient_data": True}

    # Группируем в 7-дневные окна
    windows = []
    window_size = 7
    for i in range(0, len(daily) - window_size + 1, window_size):
        chunk = daily[i:i + window_size]
        total_qty = sum(float(r.qty) for r in chunk)
        avg_price = sum(float(r.avg_price) for r in chunk) / len(chunk)
        avg_orders = total_qty / len(chunk)
        if avg_price > 0 and avg_orders > 0:
            windows.append({"price": round(avg_price, 2), "orders": round(avg_orders, 2)})

    if len(windows) < 3:
        avg_p = sum(w["price"] for w in windows) / len(windows) if windows else 0
        avg_q = sum(w["orders"] for w in windows) / len(windows) if windows else 0
        return {"elasticity": -1.0, "r_squared": 0, "data_points": len(windows),
                "avg_price": round(avg_p, 2), "avg_orders_day": round(avg_q, 2),
                "scatter": windows, "insufficient_data": True}

    # Log-log регрессия: ln(Q) = a + E * ln(P)
    ln_p = [math.log(w["price"]) for w in windows]
    ln_q = [math.log(w["orders"]) for w in windows]
    n = len(windows)

    sum_x = sum(ln_p)
    sum_y = sum(ln_q)
    sum_xy = sum(x * y for x, y in zip(ln_p, ln_q))
    sum_x2 = sum(x * x for x in ln_p)

    denom = n * sum_x2 - sum_x * sum_x
    if abs(denom) < 1e-10:
        E = -1.0
        r_sq = 0
    else:
        E = (n * sum_xy - sum_x * sum_y) / denom
        a = (sum_y - E * sum_x) / n

        # R²
        y_mean = sum_y / n
        ss_tot = sum((y - y_mean) ** 2 for y in ln_q)
        ss_res = sum((y - (a + E * x)) ** 2 for x, y in zip(ln_p, ln_q))
        r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0

    avg_price = sum(w["price"] for w in windows) / n
    avg_orders = sum(w["orders"] for w in windows) / n

    # Ограничиваем E разумным диапазоном
    # E должен быть отрицательным (рост цены → снижение спроса)
    # Если положительный или слишком экстремальный — используем дефолт
    if E >= 0 or r_sq < 0.05:
        E_final = -1.0  # дефолт — умеренная эластичность
        note = "E скорректирован (данные не показывают чёткую зависимость)"
    elif E < -5:
        E_final = -5.0
        note = "E ограничен -5.0 (экстремальная эластичность)"
    else:
        E_final = E
        note = ""

    return {
        "elasticity": round(E_final, 3),
        "elasticity_raw": round(E, 3),
        "r_squared": round(max(r_sq, 0), 3),
        "data_points": n,
        "avg_price": round(avg_price, 2),
        "avg_orders_day": round(avg_orders, 2),
        "scatter": windows,
        "insufficient_data": False,
        "note": note,
    }


# ─── Юнит-экономика ──────────────────────────────────────────

def get_sku_unit_economics(db: Session, sku_id: int, channel_id: int, days: int = 30) -> dict:
    """
    Средние показатели юнит-экономики за последние N дней из SkuDailyExpense.
    """
    date_to = date.today()
    date_from = date_to - timedelta(days=days)

    agg = db.query(
        func.sum(SkuDailyExpense.sale_amount).label("sale_amount"),
        func.sum(SkuDailyExpense.compensation).label("compensation"),
        func.sum(SkuDailyExpense.return_amount).label("return_amount"),
        func.sum(SkuDailyExpense.commission).label("commission_ret"),
        func.sum(SkuDailyExpense.logistics).label("logistics"),
        func.sum(SkuDailyExpense.storage).label("storage"),
        func.sum(SkuDailyExpense.penalty).label("penalty"),
        func.sum(SkuDailyExpense.advertising).label("advertising"),
        func.sum(SkuDailyExpense.subscription).label("subscription"),
        func.sum(SkuDailyExpense.reviews).label("reviews"),
        func.sum(SkuDailyExpense.other_deductions).label("other_deductions"),
        func.sum(SkuDailyExpense.acceptance).label("acceptance"),
        func.sum(SkuDailyExpense.acquiring).label("acquiring"),
        func.sum(SkuDailyExpense.items_count).label("items"),
        func.sum(SkuDailyExpense.return_count).label("returns"),
    ).filter(
        SkuDailyExpense.sku_id == sku_id,
        SkuDailyExpense.channel_id == channel_id,
        SkuDailyExpense.date >= date_from,
        SkuDailyExpense.date <= date_to,
    ).first()

    items = int(agg.items or 0)
    returns = int(agg.returns or 0)
    net_items = max(items - returns, 1)

    # Средняя цена до СПП из заказов
    avg_price_row = db.query(
        func.avg(Order.price)
    ).filter(
        Order.sku_id == sku_id,
        Order.channel_id == channel_id,
        Order.order_date >= date_from,
        Order.price > 0,
    ).scalar()
    avg_price = float(avg_price_row or 0)

    # Если нет цены из заказов, берём из Price table (price_before_spp)
    if avg_price == 0:
        pr = db.query(Price.price_before_spp).filter(
            Price.sku_id == sku_id, Price.channel_id == channel_id,
            Price.price_before_spp > 0,
        ).order_by(Price.date.desc()).first()
        if pr:
            avg_price = float(pr.price_before_spp)

    # Текущий СПП
    latest_order = db.query(Order.spp_pct).filter(
        Order.sku_id == sku_id, Order.channel_id == channel_id,
    ).order_by(Order.order_date.desc()).first()
    spp_pct = float(latest_order.spp_pct) if latest_order and latest_order.spp_pct else 0

    realizaciya = float(agg.sale_amount or 0) - float(agg.return_amount or 0)
    prodazhi = float(agg.compensation or 0) - float(agg.commission_ret or 0)
    logistics = float(agg.logistics or 0)
    storage = float(agg.storage or 0)
    advertising = float(agg.advertising or 0) + float(agg.subscription or 0) + float(agg.reviews or 0)
    other = float(agg.other_deductions or 0) + float(agg.penalty or 0) + float(agg.acceptance or 0)
    acquiring = float(agg.acquiring or 0)

    cogs_unit = _cogs_per_unit(db, sku_id, date_to)
    cogs_total = net_items * cogs_unit

    # Комиссия WB = (Продажи_gross - К_перечислению) / Продажи_gross
    # НЕ включает СПП — только реальная комиссия маркетплейса
    if prodazhi > 0 and realizaciya > 0:
        # Фактическая доля, которую забирает МП от того что получил продавец
        # = 1 - (к_перечислению / реализация_нетто)
        commission_rate = 1 - (prodazhi / realizaciya)
    else:
        commission_rate = 0.20  # ~20% средняя комиссия WB
    return_rate = returns / items if items > 0 else 0
    avg_orders_day = items / days

    return {
        "avg_price": round(avg_price, 2),
        "spp_pct": round(spp_pct, 1),
        "avg_orders_day": round(avg_orders_day, 2),
        "items_total": items,
        "returns_total": returns,
        "return_rate": round(return_rate, 3),
        "cogs_per_unit": round(cogs_unit, 2),
        "avg_logistics_per_unit": round(logistics / items, 2) if items > 0 else 0,
        "avg_storage_day": round(storage / days, 2),
        "avg_ads_day": round(advertising / days, 2),
        "avg_other_day": round(other / days, 2),
        "avg_acquiring_per_unit": round(acquiring / items, 2) if items > 0 else 0,
        "commission_rate": round(commission_rate, 3),
        "realizaciya": round(realizaciya, 2),
        "prodazhi": round(prodazhi, 2),
        "logistics": round(logistics, 2),
        "storage": round(storage, 2),
        "advertising": round(advertising, 2),
        "cogs_total": round(cogs_total, 2),
        "days": days,
    }


# ─── Прогноз сценария ─────────────────────────────────────────

def forecast_scenario(
    unit_econ: dict,
    elasticity: float,
    new_price: float,
    spp_pct: float,
    stock_qty: int,
    days: int = 30,
) -> dict:
    """
    Прогноз на N дней при новой цене.
    """
    current_price = unit_econ["avg_price"]
    current_orders_day = unit_econ["avg_orders_day"]

    if current_price <= 0 or current_orders_day <= 0:
        return _empty_forecast(new_price, days)

    # Прогноз заказов по эластичности: Q_new = Q_current * (P_new / P_current) ^ E
    price_ratio = new_price / current_price
    if price_ratio > 0:
        orders_day = current_orders_day * (price_ratio ** elasticity)
    else:
        orders_day = current_orders_day

    orders_total = round(orders_day * days)
    returns = round(orders_total * unit_econ["return_rate"])
    net_orders = orders_total - returns

    # new_price = цена ДО СПП (цена продавца, которую он устанавливает)
    # price_after_spp = цена для покупателя после скидки МП
    price_after_spp = new_price * (1 - spp_pct / 100)

    # Реализация = orders × price_before_spp (retail_price = цена продавца)
    realizaciya = orders_total * new_price
    # Фактические продажи = orders × price_after_spp (retail_amount, что поступает от покупателя)
    revenue = orders_total * price_after_spp
    # Комиссия WB ≈ 15% от реализации
    wb_commission_rate = 0.15
    commission = realizaciya * wb_commission_rate
    # К перечислению = revenue - доп.комиссии
    prodazhi = revenue - commission

    # Расходы
    cogs = net_orders * unit_econ["cogs_per_unit"]
    logistics = orders_total * unit_econ["avg_logistics_per_unit"]
    storage = unit_econ["avg_storage_day"] * days
    advertising = unit_econ["avg_ads_day"] * days
    other = unit_econ["avg_other_day"] * days
    acquiring = orders_total * unit_econ["avg_acquiring_per_unit"]

    total_expenses = cogs + logistics + commission + storage + advertising + other + acquiring

    gross_margin = revenue - total_expenses
    margin_pct = gross_margin / revenue * 100 if revenue > 0 else 0

    # Налоги (упрощённо: УСН 3% от к_перечислению)
    taxes = max(prodazhi, 0) * 0.03
    net_profit = gross_margin - taxes

    # Оборачиваемость капитала
    turnover_days = stock_qty / orders_day if orders_day > 0 else 999
    invested_capital = stock_qty * unit_econ["cogs_per_unit"]
    roi_30d = (net_profit / invested_capital * 100) if invested_capital > 0 else 0

    # Годовая оборачиваемость: сколько раз за год прокрутится капитал
    turns_per_year = 365 / turnover_days if turnover_days > 0 else 0
    # Прибыль за 1 оборот = net_profit за turnover_days
    profit_per_turn = net_profit * (turnover_days / days) if days > 0 else 0
    # Годовая прибыль = прибыль за оборот × кол-во оборотов
    annual_profit = profit_per_turn * turns_per_year
    # Годовой ROI = годовая прибыль / вложенный капитал
    annual_roi = (annual_profit / invested_capital * 100) if invested_capital > 0 else 0

    return {
        "price": round(new_price, 2),
        "price_after_spp": round(price_after_spp, 2),
        "orders_day": round(orders_day, 2),
        "orders_total": orders_total,
        "returns": returns,
        "revenue": round(revenue, 2),
        "prodazhi": round(prodazhi, 2),
        "cogs": round(cogs, 2),
        "logistics": round(logistics, 2),
        "commission": round(commission, 2),
        "storage": round(storage, 2),
        "advertising": round(advertising, 2),
        "other": round(other, 2),
        "acquiring": round(acquiring, 2),
        "total_expenses": round(total_expenses, 2),
        "gross_margin": round(gross_margin, 2),
        "margin_pct": round(margin_pct, 1),
        "taxes": round(taxes, 2),
        "net_profit": round(net_profit, 2),
        "turnover_days": round(turnover_days, 1),
        "turns_per_year": round(turns_per_year, 1),
        "invested_capital": round(invested_capital, 2),
        "annual_profit": round(annual_profit, 2),
        "annual_roi_pct": round(annual_roi, 1),
        "roi_pct": round(roi_30d, 1),
    }


def _empty_forecast(price: float, days: int) -> dict:
    return {k: 0 for k in [
        "price", "price_after_spp", "orders_day", "orders_total", "returns",
        "revenue", "prodazhi", "cogs", "logistics", "commission", "storage",
        "advertising", "other", "acquiring", "total_expenses", "gross_margin",
        "margin_pct", "taxes", "net_profit", "turnover_days", "turns_per_year",
        "invested_capital", "annual_profit", "annual_roi_pct", "roi_pct",
    ]}


# ─── Оптимальная цена ────────────────────────────────────────

def find_optimal_price(unit_econ: dict, elasticity: float, spp_pct: float, stock_qty: int) -> dict:
    """Найти цену с максимальной чистой прибылью (перебор ±30%)."""
    current_price = unit_econ["avg_price"]
    if current_price <= 0:
        return {"price": 0, "net_profit": 0, "scenario": _empty_forecast(0, 30)}

    best_price = current_price
    best_profit = -999999
    best_scenario = None

    for pct in range(-30, 31):
        test_price = current_price * (1 + pct / 100)
        scenario = forecast_scenario(unit_econ, elasticity, test_price, spp_pct, stock_qty)
        if scenario["net_profit"] > best_profit:
            best_profit = scenario["net_profit"]
            best_price = test_price
            best_scenario = scenario

    return {"price": round(best_price, 2), "net_profit": round(best_profit, 2), "scenario": best_scenario}


# ─── Полная аналитика SKU ────────────────────────────────────

def get_price_optimization(db: Session, sku_id: int, channel_id: int, spp_pct: Optional[float] = None) -> dict:
    """Полная ценовая аналитика для одного SKU."""
    sku = db.get(SKU, sku_id)
    if not sku:
        return {"error": "SKU не найден"}

    elast = calculate_elasticity(db, sku_id, channel_id)
    unit_econ = get_sku_unit_economics(db, sku_id, channel_id)
    stock = _current_stock(db, sku_id)

    if spp_pct is None:
        spp_pct = unit_econ["spp_pct"]

    E = elast["elasticity"]
    current_price = unit_econ["avg_price"]

    # Сценарии
    scenarios = []
    for pct_label, pct in [("-20%", -20), ("-10%", -10), ("Текущая", 0), ("+10%", 10), ("+20%", 20)]:
        price = current_price * (1 + pct / 100)
        sc = forecast_scenario(unit_econ, E, price, spp_pct, stock)
        sc["label"] = pct_label
        scenarios.append(sc)

    # Оптимум
    optimal = find_optimal_price(unit_econ, E, spp_pct, stock)
    opt_sc = optimal["scenario"]
    opt_sc["label"] = "Оптимум"
    scenarios.append(opt_sc)

    return {
        "sku_id": sku_id,
        "seller_article": sku.seller_article,
        "name": sku.name,
        "elasticity": elast,
        "unit_economics": unit_econ,
        "stock": stock,
        "spp_pct": spp_pct,
        "scenarios": scenarios,
        "optimal_price": optimal["price"],
        "optimal_profit": optimal["net_profit"],
    }


# ─── Дашборд ─────────────────────────────────────────────────

def get_elasticity_dashboard(db: Session, channel: str = "wb", limit: int = 20) -> dict:
    """Таблица топ SKU с эластичностью и рекомендациями."""
    ch = db.query(Channel).filter(Channel.type == ChannelType.WB if channel == "wb" else ChannelType.OZON).first()
    if not ch:
        return {"channel": channel, "skus": []}

    date_from = date.today() - timedelta(days=90)

    # Топ SKU по items_count
    top = (
        db.query(
            SkuDailyExpense.sku_id,
            func.sum(SkuDailyExpense.items_count).label("total"),
        )
        .filter(
            SkuDailyExpense.channel_id == ch.id,
            SkuDailyExpense.date >= date_from,
        )
        .group_by(SkuDailyExpense.sku_id)
        .order_by(func.sum(SkuDailyExpense.items_count).desc())
        .limit(limit)
        .all()
    )

    skus = []
    for sku_id, total_items in top:
        sku = db.get(SKU, sku_id)
        if not sku or sku.seller_article.startswith("_"):
            continue

        elast = calculate_elasticity(db, sku_id, ch.id)
        unit_econ = get_sku_unit_economics(db, sku_id, ch.id)
        stock = _current_stock(db, sku_id)
        spp_pct = unit_econ["spp_pct"]

        # Текущий прогноз
        current = forecast_scenario(unit_econ, elast["elasticity"], unit_econ["avg_price"], spp_pct, stock)

        # Оптимум
        optimal = find_optimal_price(unit_econ, elast["elasticity"], spp_pct, stock)

        profit_delta = 0
        if current["net_profit"] != 0:
            profit_delta = (optimal["net_profit"] - current["net_profit"]) / abs(current["net_profit"]) * 100

        skus.append({
            "sku_id": sku_id,
            "seller_article": sku.seller_article,
            "name": sku.name,
            "current_price": unit_econ["avg_price"],
            "orders_day": unit_econ["avg_orders_day"],
            "elasticity": elast["elasticity"],
            "r_squared": elast["r_squared"],
            "data_points": elast["data_points"],
            "margin_pct": current["margin_pct"],
            "net_profit_30d": current["net_profit"],
            "optimal_price": optimal["price"],
            "optimal_profit": optimal["net_profit"],
            "profit_delta_pct": round(profit_delta, 1),
            "turnover_days": current["turnover_days"],
            "stock": stock,
        })

    return {"channel": channel, "skus": skus}
