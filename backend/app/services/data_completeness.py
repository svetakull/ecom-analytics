"""
Мониторинг полноты данных и авто-дозагрузка.

Проверяет наличие всех типов данных за каждый день.
Если данные отсутствуют — автоматически запускает дозагрузку.

РАСПИСАНИЕ (НЕ МЕНЯТЬ БЕЗ ПОДТВЕРЖДЕНИЯ):
- Остатки: 00:05 AM (данные за предыдущий день)
- Остальные данные: 08:00 AM (данные за предыдущий день)
- Retry: каждые 20 минут пока данные не придут
"""
import logging
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.sales import Order, Price, CardStats, SkuDailyExpense
from app.models.inventory import Stock
from app.models.catalog import Channel, ChannelType, SKUChannel
from app.models.ads import AdMetrics

logger = logging.getLogger(__name__)


def check_data_completeness(
    db: Session,
    target_date: date,
    channel_type: ChannelType = ChannelType.WB,
) -> dict:
    """
    Проверяет полноту данных за указанную дату.
    Возвращает dict с результатами проверки и списком пропусков.
    """
    channel = db.query(Channel).filter(Channel.type == channel_type).first()
    if not channel:
        return {"ok": False, "missing": ["channel_not_found"], "details": {}}

    ch_id = channel.id

    # Считаем количество SKU для этого канала (ожидаемый минимум)
    sku_count = db.query(func.count(SKUChannel.id)).filter(
        SKUChannel.channel_id == ch_id
    ).scalar() or 0

    # Минимальные пороги: если есть хотя бы 1 запись — считаем загруженным
    # Для заказов: может быть 0 в выходные, поэтому проверяем через флаг "sync attempted"
    checks = {}

    # Заказы — сравниваем с средним за предыдущие 7 дней
    orders_count = db.query(func.count(Order.id)).filter(
        Order.channel_id == ch_id, Order.order_date == target_date
    ).scalar() or 0
    avg_orders_7d = db.query(func.count(Order.id)).filter(
        Order.channel_id == ch_id,
        Order.order_date >= target_date - timedelta(days=7),
        Order.order_date < target_date,
    ).scalar() or 0
    avg_orders_day = avg_orders_7d / 7 if avg_orders_7d > 0 else 0
    # Считаем ОК если есть хотя бы 30% от среднего (или >0 если среднее маленькое)
    orders_ok = orders_count > 0 if avg_orders_day < 5 else orders_count >= avg_orders_day * 0.3
    checks["orders"] = {"count": orders_count, "avg_7d": round(avg_orders_day, 1), "ok": orders_ok}

    # Цены
    prices_count = db.query(func.count(Price.id)).filter(
        Price.channel_id == ch_id, Price.date == target_date
    ).scalar() or 0
    checks["prices"] = {"count": prices_count, "ok": prices_count > 0}

    # Воронка (CardStats) — просмотры карточек
    # Проверяем что загружено достаточное кол-во SKU (>50% от активных)
    funnel_count = db.query(func.count(CardStats.id)).filter(
        CardStats.channel_id == ch_id, CardStats.date == target_date
    ).scalar() or 0
    funnel_sku_count = db.query(func.count(func.distinct(CardStats.sku_id))).filter(
        CardStats.channel_id == ch_id, CardStats.date == target_date
    ).scalar() or 0
    # Сравниваем с предыдущим днём (более надёжно чем sku_count)
    prev_funnel = db.query(func.count(func.distinct(CardStats.sku_id))).filter(
        CardStats.channel_id == ch_id,
        CardStats.date == target_date - timedelta(days=1),
    ).scalar() or 0
    min_expected = max(prev_funnel * 0.5, 10) if prev_funnel > 0 else sku_count * 0.5
    checks["funnel"] = {
        "count": funnel_count,
        "sku_count": funnel_sku_count,
        "expected_min": int(min_expected),
        "ok": funnel_sku_count >= min_expected,
    }

    # Реклама (AdMetrics)
    ads_count = db.query(func.count(AdMetrics.id)).filter(
        AdMetrics.date == target_date
    ).scalar() or 0
    checks["ads"] = {"count": ads_count, "ok": ads_count > 0}

    # Остатки (Stock)
    stocks_count = db.query(func.count(Stock.id)).filter(
        Stock.date == target_date
    ).scalar() or 0
    checks["stocks"] = {"count": stocks_count, "ok": stocks_count > 0}

    # Финотчёт (SkuDailyExpense) — может быть только по дням финотчёта
    expenses_count = db.query(func.count(SkuDailyExpense.id)).filter(
        SkuDailyExpense.channel_id == ch_id,
        SkuDailyExpense.date == target_date,
    ).scalar() or 0
    checks["expenses"] = {"count": expenses_count, "ok": expenses_count > 0}

    missing = [k for k, v in checks.items() if not v["ok"]]
    all_ok = len(missing) == 0

    return {
        "date": target_date.isoformat(),
        "channel": channel_type.value,
        "ok": all_ok,
        "missing": missing,
        "details": checks,
        "sku_count": sku_count,
    }


def check_and_fix_gaps(
    db: Session,
    days_back: int = 3,
    channel_type: ChannelType = ChannelType.WB,
) -> list[dict]:
    """
    Проверяет полноту данных за последние N дней.
    Для каждого пропуска запускает дозагрузку.
    Возвращает список результатов.
    """
    results = []
    today = date.today()

    for i in range(1, days_back + 1):
        target = today - timedelta(days=i)
        status = check_data_completeness(db, target, channel_type)

        if not status["ok"]:
            logger.warning(
                "data_completeness: %s %s — пропуски: %s",
                target, channel_type.value, status["missing"]
            )
            # Авто-дозагрузка
            fix_result = _fix_missing_data(db, target, status["missing"], channel_type)
            status["fix_result"] = fix_result
            results.append(status)
        else:
            results.append(status)

    return results


def _fix_missing_data(
    db: Session,
    target_date: date,
    missing: list[str],
    channel_type: ChannelType,
) -> dict:
    """Дозагружает отсутствующие данные."""
    fix_results = {}

    if channel_type == ChannelType.WB:
        fix_results = _fix_wb_data(db, target_date, missing)
    elif channel_type == ChannelType.OZON:
        fix_results = _fix_ozon_data(db, target_date, missing)

    return fix_results


def _fix_wb_data(db: Session, target_date: date, missing: list[str]) -> dict:
    """Дозагрузка WB данных."""
    from app.models.integration import Integration, IntegrationType
    from app.services.wb_api import WBClient, WBApiError

    integration = (
        db.query(Integration)
        .filter(Integration.type == IntegrationType.WB, Integration.is_active == True)
        .first()
    )
    if not integration:
        return {"error": "WB integration not found"}

    results = {}
    days_ago = (date.today() - target_date).days

    if "stocks" in missing:
        try:
            from app.services.wb_sync import sync_stocks
            client = WBClient(integration.api_key)
            r = sync_stocks(db, client, target_date=target_date)
            results["stocks"] = {"fixed": True, "result": str(r)}
            logger.info("fix_wb: stocks for %s — OK", target_date)
        except Exception as e:
            results["stocks"] = {"fixed": False, "error": str(e)}
            logger.error("fix_wb: stocks for %s — %s", target_date, e)

    if "orders" in missing:
        try:
            from app.services.wb_sync import run_full_sync
            r = run_full_sync(db, integration, days_back=max(days_ago + 1, 2))
            results["orders"] = {"fixed": True, "result": str(r)}
            logger.info("fix_wb: orders for %s — OK", target_date)
        except Exception as e:
            results["orders"] = {"fixed": False, "error": str(e)}
            logger.error("fix_wb: orders for %s — %s", target_date, e)

    if "prices" in missing:
        try:
            from app.services.wb_sync import sync_prices
            client = WBClient(
                integration.api_key,
                ads_api_key=integration.ads_api_key,
                prices_api_key=integration.prices_api_key,
            )
            r = sync_prices(db, client)
            results["prices"] = {"fixed": True, "result": str(r)}
            logger.info("fix_wb: prices for %s — OK", target_date)
        except Exception as e:
            results["prices"] = {"fixed": False, "error": str(e)}
            logger.error("fix_wb: prices for %s — %s", target_date, e)

    if "funnel" in missing:
        try:
            from app.services.wb_sync import sync_nm_report
            client = WBClient(integration.api_key)
            r = sync_nm_report(db, client, days_back=max(days_ago + 1, 14))
            results["funnel"] = {"fixed": True, "result": str(r)}
            logger.info("fix_wb: funnel for %s — OK", target_date)
        except Exception as e:
            results["funnel"] = {"fixed": False, "error": str(e)}
            logger.error("fix_wb: funnel for %s — %s", target_date, e)

    if "ads" in missing:
        try:
            from app.services.wb_sync import sync_ads
            client = WBClient(
                integration.api_key,
                ads_api_key=integration.ads_api_key,
                prices_api_key=integration.prices_api_key,
            )
            r = sync_ads(db, client, days_back=max(days_ago + 1, 3))
            results["ads"] = {"fixed": True, "result": str(r)}
            logger.info("fix_wb: ads for %s — OK", target_date)
        except Exception as e:
            results["ads"] = {"fixed": False, "error": str(e)}
            logger.error("fix_wb: ads for %s — %s", target_date, e)

    if "expenses" in missing:
        try:
            from app.services.wb_sync import sync_wb_expenses
            client = WBClient(integration.api_key)
            r = sync_wb_expenses(db, client, days_back=max(days_ago + 1, 14))
            results["expenses"] = {"fixed": True, "result": str(r)}
            logger.info("fix_wb: expenses for %s — OK", target_date)
        except Exception as e:
            results["expenses"] = {"fixed": False, "error": str(e)}
            logger.error("fix_wb: expenses for %s — %s", target_date, e)

    return results


def _fix_ozon_data(db: Session, target_date: date, missing: list[str]) -> dict:
    """Дозагрузка Ozon данных."""
    from app.models.integration import Integration, IntegrationType
    from app.services.ozon_api import OzonClient, OzonApiError

    integration = (
        db.query(Integration)
        .filter(Integration.type == IntegrationType.OZON, Integration.is_active == True)
        .first()
    )
    if not integration:
        return {"error": "Ozon integration not found"}

    results = {}
    days_ago = (date.today() - target_date).days
    client = OzonClient(integration.api_key, integration.client_id or "")

    if "stocks" in missing:
        try:
            from app.services.ozon_sync import sync_stocks
            r = sync_stocks(db, client)
            results["stocks"] = {"fixed": True, "result": str(r)}
        except Exception as e:
            results["stocks"] = {"fixed": False, "error": str(e)}

    if "orders" in missing:
        try:
            from app.services.ozon_sync import sync_orders
            r = sync_orders(db, client, days_back=max(days_ago + 1, 2))
            results["orders"] = {"fixed": True, "result": str(r)}
        except Exception as e:
            results["orders"] = {"fixed": False, "error": str(e)}

    return results
