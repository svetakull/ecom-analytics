"""
Сервис аналитики РнП: зоны, рекомендации, приоритизация SKU.

Работает поверх get_rnp_pivot() — один вызов, далее обработка в памяти.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from sqlalchemy.orm import Session

from app.services.analytics_settings_service import get_thresholds
from app.services.rnp_pivot_service import get_rnp_pivot

logger = logging.getLogger(__name__)

# ─── Типы зон ───────────────────────────────────────────────────────────
ZONE_GREEN = "green"
ZONE_YELLOW = "yellow"
ZONE_RED = "red"

# ─── Типы рекомендаций ──────────────────────────────────────────────────
REC_RAISE_PRICE = "raise_price"
REC_LOWER_PRICE = "lower_price"
REC_RESTOCK = "restock"
REC_REDUCE_ADS = "reduce_ads"
REC_INCREASE_ADS = "increase_ads"
REC_CHECK_LISTING = "check_listing"


# ═════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════

def get_rnp_analytics(
    db: Session,
    channels: list[str] | None = None,
    article: str | None = None,
) -> dict:
    """
    Главная функция аналитики РнП.
    Возвращает: store_summary + enriched SKU list с зонами и рекомендациями.
    """
    thresholds = get_thresholds(db)

    # Один вызов — все метрики за 45 дней
    pivot = get_rnp_pivot(db, days=45, channels=channels, article=article)

    ref_date_str = pivot["ref_date"]
    ref_date_val = date.fromisoformat(ref_date_str)

    enriched_skus: list[dict] = []

    for sku in pivot["skus"]:
        days_data = sku.get("days", {})

        # 1. Вычисляем метрики по 3 периодам
        periods = _compute_periods(days_data, ref_date_val)

        # 2. Классифицируем зоны
        metrics_zones = _classify_zones(periods, thresholds)
        overall_zone = _worst_zone(metrics_zones)

        # 3. Генерируем рекомендации
        recommendations = _generate_recommendations(sku, periods, thresholds)

        # 4. Диагностика маржи (если маржа упала)
        margin_diag = _margin_diagnostics(periods, thresholds)

        # 5. Собираем enriched SKU
        enriched_skus.append({
            # Идентификация
            "sku_id": sku["sku_id"],
            "channel_id": sku["channel_id"],
            "seller_article": sku["seller_article"],
            "name": sku["name"],
            "channel_type": sku["channel_type"],
            "photo_url": sku.get("photo_url", ""),
            "wb_rating": sku.get("wb_rating"),
            # Остатки
            "current_stock": sku.get("current_stock", 0),
            "turnover_days": sku.get("turnover_days", 999),
            # Периоды
            "periods": periods,
            # Зоны
            "metrics_zones": metrics_zones,
            "overall_zone": overall_zone,
            # Рекомендации
            "recommendations": recommendations,
            # Диагностика
            "margin_diagnostic": margin_diag,
            # Для сортировки
            "_orders_rub_14d": _sum_metric(days_data, ref_date_val, 14, "orders_rub"),
        })

    # 6. Приоритизация
    prioritized = _prioritize_skus(enriched_skus, thresholds)

    # 7. Сводка магазина
    summary = _store_summary(enriched_skus)

    # Группировка по зонам
    critical = [s for s in prioritized if s["priority_level"] == 1]
    warning = [s for s in prioritized if s["priority_level"] == 2 and s["overall_zone"] == ZONE_YELLOW]
    normal = [s for s in prioritized if s["priority_level"] == 2 and s["overall_zone"] == ZONE_GREEN]

    return {
        "ref_date": ref_date_str,
        "store_summary": summary,
        "critical_skus": critical,
        "critical_count": len(critical),
        "warning_skus": warning,
        "warning_count": len(warning),
        "normal_skus": normal,
        "normal_count": len(normal),
    }


# ═════════════════════════════════════════════════════════════════════════
#  ПЕРИОДЫ
# ═════════════════════════════════════════════════════════════════════════

def _compute_periods(days_data: dict[str, dict], ref_date: date) -> dict:
    """
    Вычисляет метрики для 3 периодов:
    - yesterday: ref_date vs avg(ref_date-13..ref_date-1)
    - week: avg(ref_date-6..ref_date) vs avg(ref_date-13..ref_date-7)
    - month: 1-го текущего..ref_date vs 1-го прошлого..тот же день
    """
    result = {}

    # --- Вчера vs среднее 14 дней ---
    current_y = _day_metrics(days_data, ref_date)
    baseline_days = [ref_date - timedelta(days=i) for i in range(1, 14)]
    baseline_y = _avg_metrics(days_data, baseline_days)
    result["yesterday"] = _build_period(current_y, baseline_y, "day")

    # --- 7 дней vs предыдущие 7 ---
    week1_days = [ref_date - timedelta(days=i) for i in range(0, 7)]
    week2_days = [ref_date - timedelta(days=i) for i in range(7, 14)]
    current_w = _avg_metrics(days_data, week1_days)
    baseline_w = _avg_metrics(days_data, week2_days)
    result["week"] = _build_period(current_w, baseline_w, "avg")

    # --- Месяц vs прошлый месяц ---
    month_start = ref_date.replace(day=1)
    current_m_days = _date_range(month_start, ref_date)
    # Прошлый месяц: аналогичный период
    if ref_date.month == 1:
        prev_month_start = date(ref_date.year - 1, 12, 1)
    else:
        prev_month_start = date(ref_date.year, ref_date.month - 1, 1)
    prev_day = min(ref_date.day, _month_last_day(prev_month_start))
    prev_month_end = prev_month_start.replace(day=prev_day)
    prev_m_days = _date_range(prev_month_start, prev_month_end)

    current_m = _sum_metrics(days_data, current_m_days)
    baseline_m = _sum_metrics(days_data, prev_m_days)
    result["month"] = _build_period(current_m, baseline_m, "sum")

    return result


def _build_period(current: dict, baseline: dict, agg_type: str) -> dict:
    """Строит период: current, baseline, deltas."""
    deltas = {}

    # Заказы — % изменения
    deltas["orders_qty"] = _pct_change(current.get("orders_qty", 0), baseline.get("orders_qty", 0))
    deltas["orders_rub"] = _pct_change(current.get("orders_rub", 0), baseline.get("orders_rub", 0))

    # Выкуп, маржа, ДРР — п.п. (абсолютная разница)
    deltas["buyout_rate_pct"] = _pp_diff(current.get("buyout_rate_pct", 0), baseline.get("buyout_rate_pct", 0))
    deltas["margin_pct"] = _pp_diff(current.get("margin_pct", 0), baseline.get("margin_pct", 0))
    deltas["drr_pct"] = _pp_diff(current.get("drr_pct", 0), baseline.get("drr_pct", 0))

    # Показы/переходы — % изменения
    deltas["impressions"] = _pct_change(current.get("impressions", 0), baseline.get("impressions", 0))

    return {
        "current": current,
        "baseline": baseline,
        "deltas": deltas,
        "agg_type": agg_type,
    }


# ── Извлечение метрик из daily data ─────────────────────────────────────

def _day_metrics(days_data: dict, dt: date) -> dict:
    """Метрики за один день."""
    key = dt.isoformat()
    d = days_data.get(key, {})
    return _extract(d)


def _avg_metrics(days_data: dict, dates: list[date]) -> dict:
    """Средние метрики за список дат."""
    all_m = [_extract(days_data.get(dt.isoformat(), {})) for dt in dates]
    if not all_m:
        return _empty_metrics()
    result = {}
    for k in _empty_metrics():
        vals = [m[k] for m in all_m]
        result[k] = sum(vals) / len(vals) if vals else 0
    # ДРР пересчитываем через суммы
    total_ad = sum(m.get("ad_spend", 0) for m in all_m)
    total_rub = sum(m.get("orders_rub", 0) for m in all_m)
    result["drr_pct"] = round(total_ad / total_rub * 100, 2) if total_rub > 0 else 0
    return result


def _sum_metrics(days_data: dict, dates: list[date]) -> dict:
    """Суммарные метрики за период (для месяца)."""
    all_m = [_extract(days_data.get(dt.isoformat(), {})) for dt in dates]
    if not all_m:
        return _empty_metrics()
    result = {}
    for k in _empty_metrics():
        result[k] = sum(m[k] for m in all_m)
    # Выкуп и маржа — средневзвешенные
    total_qty = result.get("orders_qty", 0)
    total_rub = result.get("orders_rub", 0)
    buyout_vals = [m["buyout_rate_pct"] for m in all_m if m["orders_qty"] > 0]
    result["buyout_rate_pct"] = sum(buyout_vals) / len(buyout_vals) if buyout_vals else 0
    margin_vals = [(m["margin_pct"], m["orders_rub"]) for m in all_m if m["orders_rub"] > 0]
    if margin_vals:
        w_sum = sum(v * w for v, w in margin_vals)
        w_total = sum(w for _, w in margin_vals)
        result["margin_pct"] = round(w_sum / w_total, 2) if w_total > 0 else 0
    else:
        result["margin_pct"] = 0
    # ДРР
    total_ad = sum(m.get("ad_spend", 0) for m in all_m)
    result["drr_pct"] = round(total_ad / total_rub * 100, 2) if total_rub > 0 else 0
    return result


def _extract(d: dict) -> dict:
    """Извлекает нужные метрики из daily dict pivot."""
    return {
        "orders_qty": d.get("orders_qty", 0),
        "orders_rub": d.get("orders_rub", 0),
        "buyout_rate_pct": d.get("buyout_rate_pct", 0),
        "margin_pct": d.get("margin_pct", 0),
        "impressions": d.get("open_card_count", 0),
        "add_to_cart": d.get("add_to_cart_count", 0),
        "drr_pct": d.get("drr_orders_pct", 0),
        "ad_spend": d.get("ad_spend", 0),
        "cart_from_card_pct": d.get("cart_from_card_pct", 0),
        "order_from_cart_pct": d.get("order_from_cart_pct", 0),
        "logistics_per_unit": d.get("logistics_per_unit", 0),
        "commission_pct": d.get("commission_pct", 0),
    }


def _empty_metrics() -> dict:
    return {
        "orders_qty": 0, "orders_rub": 0, "buyout_rate_pct": 0,
        "margin_pct": 0, "impressions": 0, "add_to_cart": 0,
        "drr_pct": 0, "ad_spend": 0, "cart_from_card_pct": 0,
        "order_from_cart_pct": 0, "logistics_per_unit": 0, "commission_pct": 0,
    }


def _sum_metric(days_data: dict, ref_date: date, n_days: int, metric: str) -> float:
    """Сумма метрики за n_days от ref_date назад."""
    total = 0
    for i in range(n_days):
        dt = (ref_date - timedelta(days=i)).isoformat()
        total += days_data.get(dt, {}).get(metric, 0)
    return total


# ═════════════════════════════════════════════════════════════════════════
#  ЗОНЫ
# ═════════════════════════════════════════════════════════════════════════

def _classify_zones(periods: dict, th: dict) -> list[dict]:
    """Классифицирует зоны для 5 метрик × 3 периодов."""
    zones = []

    for period_name in ("yesterday", "week", "month"):
        p = periods.get(period_name, {})
        deltas = p.get("deltas", {})

        # 1. Заказы (% изменения, падение = плохо)
        d_orders = deltas.get("orders_qty", 0)
        zones.append(_make_zone("orders", period_name, d_orders,
                                -th["orders_yellow_pct"], -th["orders_red_pct"], "lower_bad"))

        # 2. Выкуп (п.п., падение = плохо)
        d_buyout = deltas.get("buyout_rate_pct", 0)
        zones.append(_make_zone("buyout", period_name, d_buyout,
                                -th["buyout_yellow_pp"], -th["buyout_red_pp"], "lower_bad"))

        # 3. Маржа (п.п., падение = плохо)
        d_margin = deltas.get("margin_pct", 0)
        zones.append(_make_zone("margin", period_name, d_margin,
                                -th["margin_yellow_pp"], -th["margin_red_pp"], "lower_bad"))

        # 4. Показы/переходы (% изменения, отклонение в обе стороны = плохо)
        d_traffic = deltas.get("impressions", 0)
        zones.append(_make_zone("traffic", period_name, abs(d_traffic),
                                th["traffic_yellow_pct"], th["traffic_red_pct"], "higher_bad"))

        # 5. ДРР (п.п., рост = плохо)
        d_drr = deltas.get("drr_pct", 0)
        zones.append(_make_zone("drr", period_name, d_drr,
                                th["drr_yellow_pp"], th["drr_red_pp"], "higher_bad"))

    return zones


def _make_zone(metric: str, period: str, value: float,
               yellow_threshold: float, red_threshold: float, direction: str) -> dict:
    """
    direction:
      'lower_bad' — value < yellow = жёлтый, value < red = красный
      'higher_bad' — value > yellow = жёлтый, value > red = красный
    """
    if direction == "lower_bad":
        if value <= red_threshold:
            zone = ZONE_RED
        elif value <= yellow_threshold:
            zone = ZONE_YELLOW
        else:
            zone = ZONE_GREEN
    else:  # higher_bad
        if value >= red_threshold:
            zone = ZONE_RED
        elif value >= yellow_threshold:
            zone = ZONE_YELLOW
        else:
            zone = ZONE_GREEN

    return {"metric": metric, "period": period, "delta": round(value, 2), "zone": zone}


def _worst_zone(zones: list[dict]) -> str:
    """Наихудшая зона из списка."""
    if any(z["zone"] == ZONE_RED for z in zones):
        return ZONE_RED
    if any(z["zone"] == ZONE_YELLOW for z in zones):
        return ZONE_YELLOW
    return ZONE_GREEN


# ═════════════════════════════════════════════════════════════════════════
#  РЕКОМЕНДАЦИИ
# ═════════════════════════════════════════════════════════════════════════

def _generate_recommendations(sku: dict, periods: dict, th: dict) -> list[dict]:
    """Генерирует все применимые рекомендации для SKU."""
    recs: list[dict] = []

    r = _rec_raise_price(periods, th)
    if r:
        recs.append(r)

    r = _rec_lower_price(sku, periods, th)
    if r:
        recs.append(r)

    r = _rec_restock(sku, th)
    if r:
        recs.append(r)

    r = _rec_reduce_ads(periods, th)
    if r:
        recs.append(r)

    r = _rec_increase_ads(periods, th)
    if r:
        recs.append(r)

    r = _rec_check_listing(periods)
    if r:
        recs.append(r)

    return recs


def _rec_raise_price(periods: dict, th: dict) -> dict | None:
    """🔼 Повысить цену: выкуп > 55% И заказы растут +10%."""
    y = periods.get("yesterday", {})
    curr_buyout = y.get("current", {}).get("buyout_rate_pct", 0)
    if curr_buyout <= th["buyout_high_pct"]:
        return None

    # Проверяем рост заказов хотя бы в 2 из 3 периодов
    growth_count = 0
    for pn in ("yesterday", "week", "month"):
        d = periods.get(pn, {}).get("deltas", {}).get("orders_qty", 0)
        if d >= 10:
            growth_count += 1
    if growth_count < 2:
        return None

    return {
        "type": REC_RAISE_PRICE,
        "icon": "TrendingUp",
        "title": "Повысить цену",
        "description": f"Высокий спрос и выкуп ({curr_buyout:.0f}%) — протестируй повышение цены на 5–10%",
        "severity": "info",
    }


def _rec_lower_price(sku: dict, periods: dict, th: dict) -> dict | None:
    """🔽 Снизить цену / распродажа: заказы падают −15% 2 периода, трафик стабилен."""
    decline_count = 0
    for pn in ("yesterday", "week", "month"):
        d = periods.get(pn, {}).get("deltas", {}).get("orders_qty", 0)
        if d <= -15:
            decline_count += 1
    if decline_count < 2:
        return None

    # Трафик стабилен (не падает сильно)
    y_traffic = periods.get("yesterday", {}).get("deltas", {}).get("impressions", 0)
    if y_traffic < -15:
        return None  # Трафик тоже падает — не карточка виновата

    turnover = sku.get("turnover_days", 999)
    if turnover > 30:
        branch = "A"
        desc = "Спрос падает при стабильном трафике — запусти распродажу −10–15%, подключи акцию WB"
    elif turnover > 15:
        branch = "B"
        desc = "Спрос падает, остаток 15–30 дней — снизь рекламу, дай естественно распродаться"
    else:
        branch = "C"
        desc = "Спрос падает, но остаток менее 15 дней — ничего не делать, уйдёт само"

    return {
        "type": REC_LOWER_PRICE,
        "icon": "TrendingDown",
        "title": "Снизить цену / распродажа",
        "description": desc,
        "severity": "warning" if branch == "A" else "info",
        "branch": branch,
    }


def _rec_restock(sku: dict, th: dict) -> dict | None:
    """📦 Пополнить склад: остаток < 14 дней (жёлтый) / < 7 дней (красный)."""
    turnover = sku.get("turnover_days", 999)
    warning_days = th["stock_warning_days"]
    critical_days = th["stock_critical_days"]

    if turnover >= warning_days:
        return None

    if turnover < critical_days:
        return {
            "type": REC_RESTOCK,
            "icon": "Package",
            "title": "Пополнить склад",
            "description": f"Критично: осталось ~{turnover:.0f} дней продаж — риск потери позиций в поиске WB",
            "severity": "critical",
        }
    else:
        return {
            "type": REC_RESTOCK,
            "icon": "Package",
            "title": "Пополнить склад",
            "description": f"Осталось ~{turnover:.0f} дней продаж — пора размещать поставку",
            "severity": "warning",
        }


def _rec_reduce_ads(periods: dict, th: dict) -> dict | None:
    """📉 Снизить рекламу: ДРР > 15% И заказы не растут (<+5%)."""
    y_drr = periods.get("yesterday", {}).get("current", {}).get("drr_pct", 0)
    if y_drr <= th["drr_high_pct"]:
        return None

    # Заказы не растут в 2+ периодах
    no_growth = 0
    for pn in ("yesterday", "week", "month"):
        d = periods.get(pn, {}).get("deltas", {}).get("orders_qty", 0)
        if d < 5:
            no_growth += 1
    if no_growth < 2:
        return None

    return {
        "type": REC_REDUCE_ADS,
        "icon": "TrendingDown",
        "title": "Снизить рекламный бюджет",
        "description": f"Реклама съедает маржу (ДРР {y_drr:.0f}%) без роста заказов — снизь ставки или останови кампанию",
        "severity": "warning",
    }


def _rec_increase_ads(periods: dict, th: dict) -> dict | None:
    """📈 Увеличить рекламу: заказы +15% И 2/3 конверсий↑ И ДРР < 15%."""
    y_drr = periods.get("yesterday", {}).get("current", {}).get("drr_pct", 0)
    if y_drr >= th["drr_high_pct"]:
        return None

    # Заказы растут в 2+ периодах
    growth = 0
    for pn in ("yesterday", "week", "month"):
        d = periods.get(pn, {}).get("deltas", {}).get("orders_qty", 0)
        if d >= 15:
            growth += 1
    if growth < 2:
        return None

    # 2/3 конверсий улучшаются vs baseline
    y_curr = periods.get("yesterday", {}).get("current", {})
    y_base = periods.get("yesterday", {}).get("baseline", {})
    conv_improved = 0
    # CTR: impressions → clicks (используем cart_from_card как proxy)
    if y_curr.get("cart_from_card_pct", 0) > y_base.get("cart_from_card_pct", 0):
        conv_improved += 1
    if y_curr.get("order_from_cart_pct", 0) > y_base.get("order_from_cart_pct", 0):
        conv_improved += 1
    if y_curr.get("buyout_rate_pct", 0) > y_base.get("buyout_rate_pct", 0):
        conv_improved += 1
    if conv_improved < 2:
        return None

    return {
        "type": REC_INCREASE_ADS,
        "icon": "TrendingUp",
        "title": "Увеличить рекламу",
        "description": "Спрос растёт, конверсия улучшается — увеличь рекламный бюджет пока тренд живой",
        "severity": "info",
    }


def _rec_check_listing(periods: dict) -> dict | None:
    """🔍 Проверить карточку: показы ок, заказы упали >20%."""
    # Показы стабильны (abs delta < 15%)
    y_traffic = periods.get("yesterday", {}).get("deltas", {}).get("impressions", 0)
    if abs(y_traffic) > 15:
        return None

    # Заказы упали >20% в 2+ периодах
    drop_count = 0
    for pn in ("yesterday", "week", "month"):
        d = periods.get(pn, {}).get("deltas", {}).get("orders_qty", 0)
        if d <= -20:
            drop_count += 1
    if drop_count < 2:
        return None

    return {
        "type": REC_CHECK_LISTING,
        "icon": "Search",
        "title": "Проверить карточку",
        "description": "Трафик есть, заказов нет — проблема в карточке: фото, цена, отзывы, размерная сетка",
        "severity": "warning",
    }


# ═════════════════════════════════════════════════════════════════════════
#  ДИАГНОСТИКА МАРЖИ
# ═════════════════════════════════════════════════════════════════════════

def _margin_diagnostics(periods: dict, th: dict) -> dict | None:
    """Дерево диагностики если маржа упала."""
    y_delta_margin = periods.get("yesterday", {}).get("deltas", {}).get("margin_pct", 0)
    if y_delta_margin >= -th["margin_yellow_pp"]:
        return None  # Маржа в норме

    y_delta_orders = periods.get("yesterday", {}).get("deltas", {}).get("orders_rub", 0)
    y_delta_orders_qty = periods.get("yesterday", {}).get("deltas", {}).get("orders_qty", 0)
    y_delta_buyout = periods.get("yesterday", {}).get("deltas", {}).get("buyout_rate_pct", 0)
    y_delta_drr = periods.get("yesterday", {}).get("deltas", {}).get("drr_pct", 0)

    # Выручка упала + заказы упали → спрос
    if y_delta_orders < -10 and y_delta_orders_qty < -10:
        return {
            "diagnosis": "demand_issue",
            "title": "Снижение спроса",
            "description": "Выручка и заказы падают — возможно конец сезона или рост конкуренции",
            "linked_rec": REC_LOWER_PRICE,
        }

    # Выручка упала + выкуп упал → качество
    if y_delta_orders < -10 and y_delta_buyout < -5:
        return {
            "diagnosis": "quality_issue",
            "title": "Проблема с качеством/карточкой",
            "description": "Выкуп снизился — возможны проблемы с качеством товара или описанием",
            "linked_rec": REC_CHECK_LISTING,
        }

    # Выручка в норме, ДРР вырос → реклама
    if y_delta_drr > 3:
        return {
            "diagnosis": "drr_issue",
            "title": "Рост расходов на рекламу",
            "description": "Реклама дорожает без отдачи — ДРР растёт",
            "linked_rec": REC_REDUCE_ADS,
        }

    # Общий рост расходов
    return {
        "diagnosis": "cost_issue",
        "title": "Рост расходных статей",
        "description": "Маржа упала при стабильной выручке — детализируй расходы (логистика, комиссия, штрафы)",
        "linked_rec": None,
    }


# ═════════════════════════════════════════════════════════════════════════
#  ПРИОРИТИЗАЦИЯ
# ═════════════════════════════════════════════════════════════════════════

def _prioritize_skus(skus: list[dict], th: dict) -> list[dict]:
    """
    Level 1: любая красная метрика ИЛИ остаток < critical_days ИЛИ ДРР > красного порога
    Level 2: всё остальное, по выручке 14д ↓
    """
    critical_days = th["stock_critical_days"]

    for s in skus:
        is_critical = (
            s["overall_zone"] == ZONE_RED
            or s.get("turnover_days", 999) < critical_days
        )
        s["priority_level"] = 1 if is_critical else 2

    # Сортировка: Level 1 первые (по кол-ву красных зон ↓, потом по выручке ↓),
    # затем Level 2 (по выручке ↓)
    def sort_key(s):
        red_count = sum(1 for z in s["metrics_zones"] if z["zone"] == ZONE_RED)
        return (s["priority_level"], -red_count, -s.get("_orders_rub_14d", 0))

    skus.sort(key=sort_key)

    # Убираем внутреннее поле
    for s in skus:
        s.pop("_orders_rub_14d", None)

    return skus


# ═════════════════════════════════════════════════════════════════════════
#  СВОДКА МАГАЗИНА
# ═════════════════════════════════════════════════════════════════════════

def _store_summary(skus: list[dict]) -> dict:
    """Агрегация по всем SKU для 3 периодов."""
    summary = {}

    for period_name in ("yesterday", "week", "month"):
        total_orders_qty = 0
        total_orders_rub = 0
        total_ad_spend = 0
        margin_weighted = 0
        buyout_sum = 0
        buyout_count = 0
        baseline_orders_qty = 0
        baseline_orders_rub = 0
        baseline_ad_spend = 0
        baseline_margin_w = 0
        baseline_buyout_sum = 0

        for s in skus:
            p = s.get("periods", {}).get(period_name, {})
            curr = p.get("current", {})
            base = p.get("baseline", {})

            oq = curr.get("orders_qty", 0)
            orub = curr.get("orders_rub", 0)
            total_orders_qty += oq
            total_orders_rub += orub
            total_ad_spend += curr.get("ad_spend", 0)
            if orub > 0:
                margin_weighted += curr.get("margin_pct", 0) * orub
            if oq > 0:
                buyout_sum += curr.get("buyout_rate_pct", 0)
                buyout_count += 1

            boq = base.get("orders_qty", 0)
            borub = base.get("orders_rub", 0)
            baseline_orders_qty += boq
            baseline_orders_rub += borub
            baseline_ad_spend += base.get("ad_spend", 0)
            if borub > 0:
                baseline_margin_w += base.get("margin_pct", 0) * borub
            if boq > 0:
                baseline_buyout_sum += base.get("buyout_rate_pct", 0)

        avg_margin = round(margin_weighted / total_orders_rub, 2) if total_orders_rub > 0 else 0
        avg_buyout = round(buyout_sum / buyout_count, 2) if buyout_count > 0 else 0
        drr = round(total_ad_spend / total_orders_rub * 100, 2) if total_orders_rub > 0 else 0

        base_margin = round(baseline_margin_w / baseline_orders_rub, 2) if baseline_orders_rub > 0 else 0
        base_drr = round(baseline_ad_spend / baseline_orders_rub * 100, 2) if baseline_orders_rub > 0 else 0

        summary[period_name] = {
            "orders_qty": total_orders_qty,
            "orders_rub": round(total_orders_rub, 2),
            "margin_pct": avg_margin,
            "buyout_pct": avg_buyout,
            "drr_pct": drr,
            "deltas": {
                "orders_qty": _pct_change(total_orders_qty, baseline_orders_qty),
                "orders_rub": _pct_change(total_orders_rub, baseline_orders_rub),
                "margin_pct": round(avg_margin - base_margin, 2),
                "drr_pct": round(drr - base_drr, 2),
            },
        }

    # Подсчёт SKU по зонам
    summary["critical_count"] = sum(1 for s in skus if s.get("priority_level") == 1)
    summary["warning_count"] = sum(1 for s in skus if s.get("priority_level") == 2 and s["overall_zone"] == ZONE_YELLOW)
    summary["normal_count"] = sum(1 for s in skus if s.get("priority_level") == 2 and s["overall_zone"] == ZONE_GREEN)
    summary["total_count"] = len(skus)

    return summary


# ═════════════════════════════════════════════════════════════════════════
#  УТИЛИТЫ
# ═════════════════════════════════════════════════════════════════════════

def _pct_change(current: float, baseline: float) -> float:
    """Процентное изменение."""
    if baseline == 0:
        return 0.0 if current == 0 else 100.0
    return round((current - baseline) / abs(baseline) * 100, 2)


def _pp_diff(current: float, baseline: float) -> float:
    """Разница в п.п."""
    return round(current - baseline, 2)


def _date_range(start: date, end: date) -> list[date]:
    """Список дат от start до end включительно."""
    days = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    return days


def _month_last_day(d: date) -> int:
    """Последний день месяца."""
    if d.month == 12:
        return 31
    return (d.replace(month=d.month + 1, day=1) - timedelta(days=1)).day
