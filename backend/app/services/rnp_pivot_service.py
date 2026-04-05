"""
Pivot-сервис РнП: данные по каждому SKU × каждый день.
Формат аналогичен WB Аналитике — Юнит-экономика.
"""
from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.ads import AdCampaign, AdMetrics
from app.models.catalog import Channel, ChannelType, SKU, SKUChannel
from app.models.finance import PnLRecord  # noqa: F401 – ensure model is imported
from app.models.inventory import ProductBatch, SKUCostHistory, Stock, StorageCost
from app.models.sales import CardStats, Order, OrderStatus, Price, Sale, Return, SkuDailyExpense
from app.services.ozon_finance import get_ozon_fin_ratios


# ────────────────────────────────────────────────
# Хелперы
# ────────────────────────────────────────────────

def _last_order_date(db: Session) -> date:
    result = db.query(func.max(Order.order_date)).scalar()
    return result if result else date.today()


def wb_photo_url(nm_id: str) -> str:
    """Строит URL превью-изображения WB по nm_id (числовой артикул маркетплейса)."""
    try:
        nm = int(nm_id)
    except (TypeError, ValueError):
        return ""
    vol = nm // 100000
    part = nm // 1000
    # Определяем номер basket по диапазонам vol.
    # WB периодически добавляет новые baskets.
    # Таблица откалибрована по реальным nm_id на март 2026:
    #   789→05, 4979→27, 5500→28, 5503→29, 5820→30, 6126→31, 6445→32, 6951→33
    # Фронтенд автоматически пробует basket±1 при ошибке 404.
    if   vol <= 143:  basket = "01"
    elif vol <= 287:  basket = "02"
    elif vol <= 431:  basket = "03"
    elif vol <= 719:  basket = "04"
    elif vol <= 1007: basket = "05"
    elif vol <= 1061: basket = "06"
    elif vol <= 1115: basket = "07"
    elif vol <= 1169: basket = "08"
    elif vol <= 1313: basket = "09"
    elif vol <= 1601: basket = "10"
    elif vol <= 1655: basket = "11"
    elif vol <= 1919: basket = "12"
    elif vol <= 2045: basket = "13"
    elif vol <= 2189: basket = "14"
    elif vol <= 2405: basket = "15"
    elif vol <= 2621: basket = "16"
    elif vol <= 2837: basket = "17"
    elif vol <= 3053: basket = "18"
    elif vol <= 3269: basket = "19"
    elif vol <= 3485: basket = "20"
    elif vol <= 3701: basket = "21"
    elif vol <= 3917: basket = "22"
    elif vol <= 4133: basket = "23"
    elif vol <= 4349: basket = "24"
    elif vol <= 4565: basket = "25"
    elif vol <= 4871: basket = "26"
    elif vol <= 5185: basket = "27"
    elif vol <= 5501: basket = "28"
    elif vol <= 5819: basket = "29"
    elif vol <= 6125: basket = "30"
    elif vol <= 6444: basket = "31"
    elif vol <= 6749: basket = "32"
    else:             basket = "33"
    return f"https://basket-{basket}.wbbasket.ru/vol{vol}/part{part}/{nm}/images/tm/1.webp"


def _cogs_per_unit(db: Session, sku_id: int, target_date: date = None, channel_id: int = None) -> float:
    """
    Себестоимость на ед.
    Приоритет: 1) cost_prices (новый модуль) → 2) SKUCostHistory → 3) ProductBatch.
    """
    # 1) Новый модуль себестоимости (cost_prices)
    if target_date and channel_id:
        try:
            from app.services.cost_price_service import resolve_cogs_per_unit
            val = resolve_cogs_per_unit(db, sku_id, channel_id, target_date)
            if val > 0:
                return val
        except Exception:
            pass  # таблица может не существовать — fallback

    # 2) SKUCostHistory (старый модуль)
    if target_date:
        record = (
            db.query(SKUCostHistory)
            .filter(SKUCostHistory.sku_id == sku_id, SKUCostHistory.effective_from <= target_date)
            .order_by(SKUCostHistory.effective_from.desc())
            .first()
        )
        if record:
            return float(record.cost_per_unit)

    # 3) ProductBatch (fallback)
    batch = (
        db.query(ProductBatch)
        .filter(ProductBatch.sku_id == sku_id)
        .order_by(ProductBatch.batch_date.desc())
        .first()
    )
    return float(batch.total_cost_per_unit) if batch else 0.0


def _stock_by_date(db: Session, sku_id: int, target_date: date, channel_type: str = None) -> dict:
    """
    Остаток на дату: возвращает dict с qty (на складах), in_way_to_client, in_way_from_client.
    Берём ближайшую запись ≤ target_date.

    Фильтруем по каналу через имя склада:
    - WB: только «WB склад» (агрегированный) — без детальных WB складов
    - Ozon: склады с именем «Ozon ...»
    - Lamoda: «Lamoda FBO»
    Если канал не указан — суммируем агрегированные склады каналов.
    """
    from app.models.catalog import Warehouse

    # Фильтр складов по каналу (применяется и к поиску latest, и к агрегации)
    def apply_warehouse_filter(query):
        if channel_type == "wb":
            return query.filter(Warehouse.name == "WB склад")
        elif channel_type == "ozon":
            return query.filter(Warehouse.name.like("Ozon%"))
        elif channel_type == "lamoda":
            return query.filter(Warehouse.name.like("Lamoda%"))
        else:
            return query.filter(
                (Warehouse.name == "WB склад") |
                Warehouse.name.like("Ozon%") |
                Warehouse.name.like("Lamoda%")
            )

    # Ищем последнюю дату остатков ≤ target_date В РАМКАХ КАНАЛА
    latest_q = (
        db.query(func.max(Stock.date))
        .join(Warehouse, Stock.warehouse_id == Warehouse.id)
        .filter(Stock.sku_id == sku_id, Stock.date <= target_date)
    )
    latest = apply_warehouse_filter(latest_q).scalar()
    if not latest:
        return {"qty": 0, "in_way_to_client": 0, "in_way_from_client": 0}

    q = (
        db.query(
            func.sum(Stock.qty),
            func.sum(Stock.in_way_to_client),
            func.sum(Stock.in_way_from_client),
        )
        .join(Warehouse, Stock.warehouse_id == Warehouse.id)
        .filter(Stock.sku_id == sku_id, Stock.date == latest)
    )
    q = apply_warehouse_filter(q)

    row = q.first()
    return {
        "qty": int(row[0] or 0) if row else 0,
        "in_way_to_client": int(row[1] or 0) if row else 0,
        "in_way_from_client": int(row[2] or 0) if row else 0,
    }


def _current_stock(db: Session, sku_id: int, channel_type: str = None) -> int:
    """Текущий остаток — фильтрация по каналу (агрегированный склад)."""
    # Используем сегодня — _stock_by_date сам найдёт latest в рамках канала
    return _stock_by_date(db, sku_id, date.today(), channel_type=channel_type)["qty"]


def _build_logistics_map(db: Session, sku_id: int, channel_id: int, start: date, end: date) -> dict:
    """
    Возвращает dict {date: (sum_logistics, count_sales)} для диапазона [start, end].
    Используется для построения скользящего 14-дневного среднего логистики.
    """
    rows = db.query(Sale.sale_date, func.sum(Sale.logistics), func.count(Sale.id)).filter(
        Sale.sku_id == sku_id,
        Sale.channel_id == channel_id,
        Sale.sale_date >= start,
        Sale.sale_date <= end,
    ).group_by(Sale.sale_date).all()
    return {r[0]: (float(r[1] or 0), int(r[2] or 0)) for r in rows}


def _rolling_logistics(logistics_map: dict, day: date, window: int = 14) -> Optional[float]:
    """
    14-дневное скользящее среднее логистики, заканчивающееся на day (включительно).
    Формула: SUM(logistics, D-13..D) / COUNT(sales, D-13..D).
    Возвращает None если за окно нет ни одной продажи (→ нужен ручной ввод).
    """
    total_log = 0.0
    total_cnt = 0
    for i in range(window):
        d = day - timedelta(days=i)
        if d in logistics_map:
            total_log += logistics_map[d][0]
            total_cnt += logistics_map[d][1]
    if total_cnt == 0:
        return None
    return round(total_log / total_cnt, 2)


def _storage_cost_per_unit_on_day(db: Session, sku_id: int, target_date: date) -> float:
    """
    Суммарная стоимость хранения по ВСЕМ складам для артикула за конкретный день.
    Данные из отчёта платного хранения WB (StorageCost.cost = warehousePrice).
    Возвращает сумму, а не стоимость на единицу — WB уже считает cost как
    суммарную стоимость хранения партии на складе за день.
    """
    total_cost = db.query(func.sum(StorageCost.cost)).filter(
        StorageCost.sku_id == sku_id,
        StorageCost.date == target_date,
    ).scalar()
    return round(float(total_cost), 4) if total_cost else 0.0


def _avg_storage_cost_per_unit(db: Session, sku_id: int, ref_date: date, days: int = 14) -> float:
    """Среднее суточное хранение по всем складам за последние N дней (фолбэк для дней без данных)."""
    costs = []
    for i in range(days):
        d = ref_date - timedelta(days=i)
        c = _storage_cost_per_unit_on_day(db, sku_id, d)
        if c > 0:
            costs.append(c)
    return round(sum(costs) / len(costs), 4) if costs else 0.0


def _build_ad_metrics_map(db: Session, sku_id: int, channel_id: int, start: date, end: date) -> dict:
    """
    Возвращает dict {date: {"total": {...}, "search": {...}, "recommend": {...}}}
    по РЕАЛЬНЫМ кампаниям SKU × канал за период.
    Seed-кампании исключены: их impressions/clicks фейковые.
    Для WB: name LIKE 'WB_%'; для Ozon: name LIKE 'OZ_%' или содержит 'Ozon'.
    Разбивка по размещению из полей search_*/recommend_* (appType из WB fullstats):
      appType=1 → Поиск, appType 32/64/128/... → Полки/Рекомендации.
    """
    from sqlalchemy import or_

    rows = (
        db.query(
            AdMetrics.date,
            func.sum(AdMetrics.budget),
            func.sum(AdMetrics.impressions),
            func.sum(AdMetrics.clicks),
            func.sum(AdMetrics.orders),
            func.sum(AdMetrics.search_budget),
            func.sum(AdMetrics.search_impressions),
            func.sum(AdMetrics.search_clicks),
            func.sum(AdMetrics.search_orders),
            func.sum(AdMetrics.recommend_budget),
            func.sum(AdMetrics.recommend_impressions),
            func.sum(AdMetrics.recommend_clicks),
            func.sum(AdMetrics.recommend_orders),
        )
        .join(AdCampaign, AdCampaign.id == AdMetrics.campaign_id)
        .filter(
            AdCampaign.sku_id == sku_id,
            AdCampaign.channel_id == channel_id,
            or_(
                AdCampaign.name.like("WB_%"),
                AdCampaign.name.like("OZ_%"),
                AdCampaign.name.contains("Ozon"),
            ),
            AdMetrics.date >= start,
            AdMetrics.date <= end,
        )
        .group_by(AdMetrics.date)
        .all()
    )
    result: dict = {}
    for (dt, tot_b, tot_i, tot_c, tot_o,
         s_b, s_i, s_c, s_o,
         r_b, r_i, r_c, r_o) in rows:
        result[dt] = {
            "total": {
                "budget":      float(tot_b or 0),
                "impressions": int(tot_i or 0),
                "clicks":      int(tot_c or 0),
                "orders":      int(tot_o or 0),
            },
            "search": {
                "budget":      float(s_b or 0),
                "impressions": int(s_i or 0),
                "clicks":      int(s_c or 0),
                "orders":      int(s_o or 0),
            },
            "recommend": {
                "budget":      float(r_b or 0),
                "impressions": int(r_i or 0),
                "clicks":      int(r_c or 0),
                "orders":      int(r_o or 0),
            },
        }
    return result


def _build_price_map(db: Session, sku_id: int, channel_id: int, start: date, end: date) -> dict:
    """
    Возвращает dict {date: price_before_spp} из таблицы prices (синк WB Prices API).
    Источник: discountedPrice из WB Prices API = «Цена со скидкой» в WB Аналитике.
    Используется как приоритетный источник цены вместо Order.price (priceWithDisc),
    так как priceWithDisc в Orders API включает авто-акции WB и может занижать цену.

    Если для конкретной даты нет записи, берём последнюю запись ≤ end (т.к. цены обновляются раз в день).
    Это позволяет корректно показывать цены за прошедшие дни при первом синке.
    """
    # Ищем записи за период плюс последнюю запись до start (для заполнения пробелов)
    rows = (
        db.query(Price.date, Price.price_before_spp)
        .filter(
            Price.sku_id == sku_id,
            Price.channel_id == channel_id,
            Price.price_before_spp > 0,
        )
        .order_by(Price.date.asc())
        .all()
    )
    if not rows:
        return {}

    # Строим полный словарь известных цен
    known = {r[0]: float(r[1]) for r in rows}

    # Заполняем каждую дату в [start..end] ближайшей известной ценой.
    # Сначала пробуем «последнюю цену ≤ d», затем «ближайшую цену > d» (forward-fill).
    # Это позволяет корректно работать при первом синке, когда цена есть только на сегодня.
    result = {}
    all_dates = sorted(known.keys())
    for d in (start + timedelta(days=i) for i in range((end - start).days + 1)):
        if d in known:
            result[d] = known[d]
        else:
            # Последняя цена ≤ d (backward fill)
            best = None
            for kd in all_dates:
                if kd <= d:
                    best = known[kd]
                else:
                    break
            if best is None:
                # Ближайшая цена > d (forward fill — на случай первого синка)
                for kd in all_dates:
                    if kd > d:
                        best = known[kd]
                        break
            if best is not None:
                result[d] = best
    return result


def _build_price_after_map(db: Session, sku_id: int, channel_id: int, start: date, end: date) -> dict:
    """
    Возвращает dict {date: price_after_spp} из таблицы prices.
    Backward/forward fill аналогично _build_price_map.
    """
    rows = (
        db.query(Price.date, Price.price_after_spp)
        .filter(Price.sku_id == sku_id, Price.channel_id == channel_id, Price.price_after_spp > 0)
        .order_by(Price.date.asc())
        .all()
    )
    if not rows:
        return {}
    known = {r[0]: float(r[1]) for r in rows}
    result = {}
    all_dates = sorted(known.keys())
    for d in (start + timedelta(days=i) for i in range((end - start).days + 1)):
        if d in known:
            result[d] = known[d]
        else:
            best = None
            for kd in all_dates:
                if kd <= d:
                    best = known[kd]
                else:
                    break
            if best is None:
                for kd in all_dates:
                    if kd > d:
                        best = known[kd]
                        break
            if best is not None:
                result[d] = best
    return result


def _build_card_stats_map(db: Session, sku_id: int, channel_id: int, start: date, end: date) -> dict:
    """
    Возвращает dict {date: {"open_card": N, "add_to_cart": N, "orders": N}}
    из таблицы CardStats (nm-report WB) за период [start, end].
    """
    rows = (
        db.query(
            CardStats.date,
            CardStats.open_card_count,
            CardStats.add_to_cart_count,
            CardStats.orders_count,
            CardStats.avg_price_rub,
        )
        .filter(
            CardStats.sku_id == sku_id,
            CardStats.channel_id == channel_id,
            CardStats.date >= start,
            CardStats.date <= end,
        )
        .all()
    )
    return {
        r[0]: {
            "open_card":    int(r[1] or 0),
            "add_to_cart":  int(r[2] or 0),
            "orders":       int(r[3] or 0),
            "avg_price_rub": float(r[4]) if r[4] is not None else None,
        }
        for r in rows
    }


def _ad_derived(b: float, imp: int, clk: int, ord_: int, all_orders: int, sales: int) -> dict:
    """Производные рекламные метрики из базовых."""
    return {
        "budget":       round(b, 2),
        "impressions":  imp,
        "clicks":       clk,
        "orders":       ord_,
        "ctr":          round(clk / imp * 100, 2) if imp else 0.0,
        "cr":           round(ord_ / clk * 100, 2) if clk else 0.0,
        "cpc":          round(b / clk, 2) if clk else 0.0,
        "cpm":          round(b / imp * 1000, 2) if imp else 0.0,
        "cpo_all":      round(b / all_orders, 2) if all_orders else 0.0,
        "cpo_ad":       round(b / ord_, 2) if ord_ else 0.0,
        "cps":          round(b / sales, 2) if sales else 0.0,
    }


def _buyout_rate_period(db: Session, sku_id: int, channel_id: int, ref_date: date, days: int = 14) -> float:
    """
    Процент выкупа за последние N дней.
    Формула: Продажи / (Продажи + Возвраты + Отказы).
    Возвращает 0.0 если данных нет (признак — показать прочерк / разрешить ручной ввод).

    Источник «Продаж» зависит от канала:
    - WB: таблица sales (есть записи о выкупе)
    - Ozon/Lamoda: Order.status=DELIVERED (таблицы sales нет)
    """
    start = ref_date - timedelta(days=days - 1)

    # Определяем тип канала один раз
    ch = db.query(Channel.type).filter(Channel.id == channel_id).scalar()
    use_orders_delivered = ch in (ChannelType.OZON, ChannelType.LAMODA)

    if use_orders_delivered:
        sales = db.query(func.count(Order.id)).filter(
            Order.sku_id == sku_id, Order.channel_id == channel_id,
            Order.order_date >= start, Order.order_date <= ref_date,
            Order.status == OrderStatus.DELIVERED,
        ).scalar() or 0
    else:
        sales = db.query(func.count(Sale.id)).filter(
            Sale.sku_id == sku_id, Sale.channel_id == channel_id,
            Sale.sale_date >= start, Sale.sale_date <= ref_date,
        ).scalar() or 0

    returns = db.query(func.count(Return.id)).filter(
        Return.sku_id == sku_id, Return.channel_id == channel_id,
        Return.return_date >= start, Return.return_date <= ref_date,
    ).scalar() or 0

    cancellations = db.query(func.count(Order.id)).filter(
        Order.sku_id == sku_id, Order.channel_id == channel_id,
        Order.order_date >= start, Order.order_date <= ref_date,
        Order.status == OrderStatus.CANCELLED,
    ).scalar() or 0

    denominator = sales + returns + cancellations
    if not denominator:
        return 0.0
    return round(min(sales / denominator, 1.0), 4)


# ────────────────────────────────────────────────
# Основная функция
# ────────────────────────────────────────────────

def get_rnp_pivot(
    db: Session,
    date_from=None,
    date_to=None,
    days: int = 7,
    channels: Optional[List[str]] = None,
    article: Optional[str] = None,
) -> dict:
    """
    Возвращает pivot-данные: для каждого SKU — метрики за каждый из последних N дней.
    Поддерживает явный диапазон дат (date_from / date_to) или количество дней.
    channels: список типов каналов ["wb", "ozon"] — если None, то wb+ozon.
    article: фильтр по seller_article (частичное совпадение).
    """
    last_date = _last_order_date(db)
    ref_date = min(last_date, date.today() - timedelta(days=1))  # текущий день не включаем

    if date_from is not None and date_to is not None:
        # Явный диапазон: day_list = [date_to, date_to-1, ..., date_from]
        end = min(date_to, ref_date)
        start = date_from
        num_days = (end - start).days + 1
        day_list: List[date] = [start + timedelta(days=i) for i in range(max(num_days, 1))]
    else:
        start_d = ref_date - timedelta(days=days - 1)
        day_list: List[date] = [start_d + timedelta(days=i) for i in range(days)]

    # РнП — только WB/Ozon (прогнозная аналитика). Lamoda — в разделе «Оцифровка».
    default_types = [ChannelType.WB, ChannelType.OZON]
    if channels:
        parsed = []
        for c in channels:
            try:
                parsed.append(ChannelType(c.lower()))
            except ValueError:
                pass
        allowed_types = parsed if parsed else default_types
    else:
        allowed_types = default_types

    q = (
        db.query(SKUChannel)
        .join(SKU)
        .join(Channel)
        .filter(SKU.is_active == True, Channel.is_active == True)
        .filter(Channel.type.in_(allowed_types))
    )
    if article:
        q = q.filter(SKU.seller_article.ilike(f"%{article}%"))
    sku_channels = q.all()

    skus_result = []

    for sc in sku_channels:
        sku: SKU = sc.sku
        channel: Channel = sc.channel

        cogs = _cogs_per_unit(db, sku.id, ref_date, channel.id)

        # Процент выкупа: ручное переопределение → исторические данные 14 дней → дефолт 50%
        buyout_rate_is_manual = sc.buyout_rate_override is not None
        if buyout_rate_is_manual:
            buyout_rate = float(sc.buyout_rate_override)
        else:
            calc = _buyout_rate_period(db, sku.id, channel.id, ref_date, 14)
            buyout_rate = calc if calc > 0 else 0.5  # дефолт 50% если нет истории

        # logistics_override — обновляется еженедельно из финотчёта WB (sync_logistics_weekly)
        # или вручную. Используется как ПРИОРИТЕТНЫЙ источник вместо rolling avg из Sale.logistics.
        logistics_is_manual = sc.logistics_override is not None
        logistics_override_val = float(sc.logistics_override) if logistics_is_manual else None

        # commission_pct_override — (Реализация - К_перечислению) / Реализация * 100 из финотчёта WB.
        # Обновляется еженедельно или вручную через интерфейс.
        # None = использовать channel.commission_pct (базовый процент канала).
        commission_is_manual = sc.commission_pct_override is not None
        commission_pct_override_val = float(sc.commission_pct_override) if commission_is_manual else None

        # Предвычисляем карту логистики для скользящего 14-дневного окна
        wide_start = min(day_list) - timedelta(days=13)
        logistics_map = _build_logistics_map(db, sku.id, channel.id, wide_start, ref_date)

        # Ozon: предвычисляем расходы по дням из SkuDailyExpense
        ozon_expense_map: Dict[date, dict] = {}
        # Средние значения комиссии/логистики Ozon за 30 дней (на единицу)
        ozon_avg_commission_per_unit: float = 0.0
        ozon_avg_logistics_per_unit: float = 0.0
        if channel.type == ChannelType.OZON:
            for exp in db.query(SkuDailyExpense).filter(
                SkuDailyExpense.sku_id == sku.id,
                SkuDailyExpense.channel_id == channel.id,
                SkuDailyExpense.date >= wide_start,
                SkuDailyExpense.date <= ref_date,
            ).all():
                ozon_expense_map[exp.date] = {
                    "commission": float(exp.commission),
                    "logistics": float(exp.logistics),
                    "storage": float(exp.storage),
                    "items": int(exp.items_count),
                }
            # 30-дневная средняя на единицу (Ozon начисляет комиссию асинхронно,
            # поэтому дневные значения скачут от 0% до 15% — среднее за 30 дней стабильное)
            _total_comm = sum(v["commission"] for v in ozon_expense_map.values())
            _total_log = sum(v["logistics"] for v in ozon_expense_map.values())
            _total_items = sum(v["items"] for v in ozon_expense_map.values())
            if _total_items > 0:
                ozon_avg_commission_per_unit = _total_comm / _total_items
                ozon_avg_logistics_per_unit = _total_log / _total_items

        # Предвычисляем рекламные метрики по дням (по типам кампаний)
        ad_metrics_map = _build_ad_metrics_map(db, sku.id, channel.id, min(day_list), ref_date)

        # Воронка карточки из nm-report (переходы, корзина)
        card_stats_map = _build_card_stats_map(db, sku.id, channel.id, min(day_list), ref_date)

        # Цены продавца из WB Prices API (discountedPrice = «Цена со скидкой» = «Цена до СПП»)
        # Приоритет над Order.price (priceWithDisc), который занижается авто-акциями WB
        price_map = _build_price_map(db, sku.id, channel.id, min(day_list), ref_date)
        price_map_after = _build_price_after_map(db, sku.id, channel.id, min(day_list), ref_date)

        # Средняя стоимость хранения за 14 дней из отчёта платного хранения
        avg_storage_real = _avg_storage_cost_per_unit(db, sku.id, ref_date, 14)

        # Итоговое среднее логистики для шапки SKU.
        if channel.type == ChannelType.OZON and ozon_expense_map:
            _total_log = sum(v["logistics"] for v in ozon_expense_map.values())
            _total_itm = sum(v["items"] for v in ozon_expense_map.values())
            avg_logistics_real = _total_log / _total_itm if _total_itm > 0 else 0.0
        elif logistics_override_val is not None:
            avg_logistics_real = logistics_override_val
        else:
            avg_logistics_real = _rolling_logistics(logistics_map, ref_date, 14) or 0.0

        # ── Данные по дням ──────────────────────
        days_data: Dict[str, dict] = {}

        total_orders_qty = 0
        total_orders_rub = 0.0
        total_sales_qty = 0
        total_returns_qty = 0
        price_samples = []
        price_after_spp_samples = []
        spp_samples = []
        commission_pct_samples = []

        for d in day_list:
            ds = d.isoformat()

            # Заказы за день (только не отменённые)
            row_orders = db.query(
                func.count(Order.id),
                func.sum(Order.price),
                func.avg(Order.price),
                func.avg(Order.price_after_spp),
                func.avg(Order.spp_pct),
            ).filter(
                Order.sku_id == sku.id,
                Order.channel_id == channel.id,
                Order.order_date == d,
            ).first()

            o_qty = int(row_orders[0] or 0)
            o_rub = float(row_orders[1] or 0)
            avg_price_before = float(row_orders[2] or 0)
            avg_price_after = float(row_orders[3] or 0)
            avg_spp = float(row_orders[4] or 0)

            # Продажи за день (факт из таблицы sales — только для WB)
            row_sales = db.query(
                func.count(Sale.id),
                func.sum(Sale.price),
                func.avg(Sale.commission),
                func.avg(Sale.logistics),
            ).filter(
                Sale.sku_id == sku.id,
                Sale.channel_id == channel.id,
                Sale.sale_date == d,
            ).first()

            s_qty = int(row_sales[0] or 0)
            s_rub = float(row_sales[1] or 0)
            avg_commission = float(row_sales[2] or 0)
            avg_logistics = float(row_sales[3] or 0)

            # Возвраты за день
            r_qty = db.query(func.count(Return.id)).filter(
                Return.sku_id == sku.id,
                Return.channel_id == channel.id,
                Return.return_date == d,
            ).scalar() or 0

            # Отказы (отменённые заказы) за день
            c_qty = db.query(func.count(Order.id)).filter(
                Order.sku_id == sku.id,
                Order.channel_id == channel.id,
                Order.order_date == d,
                Order.status == OrderStatus.CANCELLED,
            ).scalar() or 0

            # Остаток на эту дату
            stock_data = _stock_by_date(db, sku.id, d, channel_type=channel.type.value)
            stock = stock_data["qty"]
            in_way_to = stock_data["in_way_to_client"]
            in_way_from = stock_data["in_way_from_client"]

            # p_before = «Цена до СПП» / «Цена со скидкой продавца» (WB Analytics)
            # Приоритет источников:
            # 1) WB Prices API discountedPrice (price_map) — самый точный
            # 2) nm-report avgPriceRub — точный, но API может быть недоступен (404)
            # 3) avg(Order.price) — priceWithDisc, занижен авто-акциями; использовать как запас
            _cs_early = card_stats_map.get(d, {})
            _nm_price = _cs_early.get("avg_price_rub")
            _prices_api_price = price_map.get(d)

            if _prices_api_price and _prices_api_price > 0:
                p_before = _prices_api_price
            elif _nm_price and _nm_price > 0:
                p_before = _nm_price
            elif avg_price_before > 0:
                p_before = avg_price_before
            else:
                p_before = 0

            # p_after = finishedPrice (цена покупателя)
            # Приоритет: 1) Order.price_after_spp за день
            #            2) Рассчитать из p_before × (1 - avg_spp/100)
            #            3) Последнее известное значение из Price table
            p_after = avg_price_after if avg_price_after > 0 else (o_rub / o_qty if o_qty else 0)

            if p_after == 0 and p_before > 0:
                # Нет заказов за день — вычисляем из цены до СПП и среднего СПП
                if avg_spp > 0:
                    p_after = round(p_before * (1 - avg_spp / 100), 2)
                else:
                    # Ищем последнюю известную цену после СПП из Price table
                    _price_rec = price_map_after.get(d)
                    if _price_rec and _price_rec > 0:
                        p_after = _price_rec

            if p_before == 0 and p_after > 0:
                p_before = p_after  # крайний запасной вариант

            spp = avg_spp
            # Пересчитываем СПП относительно скорректированной базовой цены
            if p_before > 0 and p_after > 0 and p_before > p_after:
                spp = round((1 - p_after / p_before) * 100, 2)

            # Приоритет источника «Факт. заказы»: nm-report (card_stats.orders_count)
            # WB Аналитика использует именно этот источник, а не Supplier API.
            # Supplier API (таблица orders) может расходиться из-за задержек синхронизации.
            _nm_orders_count = _cs_early.get("orders", 0)
            if _nm_orders_count > 0:
                o_qty = _nm_orders_count
                o_rub = round(o_qty * p_before, 2) if p_before > 0 else o_rub

            # Комиссия и логистика на единицу
            if channel.type == ChannelType.OZON:
                # Ozon: приоритет override → базовый процент канала → cash-flow API
                if commission_pct_override_val is not None:
                    commission_pct = commission_pct_override_val
                else:
                    # Базовый % канала (брутто по договору Ozon) — основной источник
                    commission_pct = float(channel.commission_pct)
                commission_per_unit = round(p_before * commission_pct / 100, 2)
                logistics_per_unit = ozon_avg_logistics_per_unit
            else:
                # WB / Lamoda: стандартная логика
                if commission_pct_override_val is not None:
                    commission_pct = commission_pct_override_val
                    commission_per_unit = round(p_before * commission_pct / 100, 2)
                elif avg_commission > 0:
                    commission_per_unit = avg_commission
                    commission_pct = (avg_commission / p_before * 100) if p_before > 0 else float(channel.commission_pct)
                else:
                    commission_pct = float(channel.commission_pct)
                    commission_per_unit = p_before * commission_pct / 100

                if logistics_override_val is not None:
                    logistics_per_unit = logistics_override_val
                else:
                    rolling_log = _rolling_logistics(logistics_map, d, 14)
                    logistics_per_unit = rolling_log if rolling_log is not None else 0.0

            # Процент выкупа на день: 14-дневное скользящее среднее, заканчивающееся на D
            # Если нет данных → ручное переопределение или 0
            buyout_day_calc = _buyout_rate_period(db, sku.id, channel.id, d, 14)
            if buyout_day_calc > 0:
                buyout_day = buyout_day_calc
            elif buyout_rate_is_manual:
                buyout_day = buyout_rate
            else:
                buyout_day = 0.0

            # Прогноз продажи = заказы * buyout дня, округляем до целого (товар не делится).
            # int(x + 0.5) = стандартное математическое округление: 0.5 → 1, 1.5 → 2 и т.д.
            forecast_sales_qty = int(o_qty * buyout_day + 0.5) if o_qty else 0
            forecast_sales_rub = round(forecast_sales_qty * p_before, 2)

            # Хранение: источник зависит от канала
            if channel.type == ChannelType.OZON:
                # Ozon: из SkuDailyExpense.storage (операции Ozon)
                _oz = ozon_expense_map.get(d, {})
                storage_per_unit = _oz.get("storage", 0.0)
            else:
                # WB/Lamoda: из отчёта платного хранения WB
                storage_on_day = _storage_cost_per_unit_on_day(db, sku.id, d)
                storage_per_unit = storage_on_day if storage_on_day > 0 else (avg_storage_real if avg_storage_real > 0 else 0.0)

            # Рекламные метрики за день
            _zero = {"budget": 0, "impressions": 0, "clicks": 0, "orders": 0}
            ad_day = ad_metrics_map.get(d, {})
            ad_t = ad_day.get("total",     _zero)  # total: реальные расходы (включает старые данные)
            ad_s = ad_day.get("search",    _zero)  # Поиск (appType=1)
            ad_r = ad_day.get("recommend", _zero)  # Полки (appType=32/64/128/…)

            # Для сводных расчётов (ДРР, прибыль) — total budget, чтобы учесть все кампании
            ad_spend      = ad_t["budget"]
            ad_impressions = ad_t["impressions"]
            ad_clicks      = ad_t["clicks"]
            ad_orders_qty  = ad_t["orders"]

            ad_total  = _ad_derived(ad_spend, ad_impressions, ad_clicks, ad_orders_qty, o_qty, forecast_sales_qty)
            ad_search = _ad_derived(ad_s["budget"], ad_s["impressions"], ad_s["clicks"], ad_s["orders"], o_qty, forecast_sales_qty)
            ad_recom  = _ad_derived(ad_r["budget"], ad_r["impressions"], ad_r["clicks"], ad_r["orders"], o_qty, forecast_sales_qty)

            # Воронка карточки (nm-report)
            cs = _cs_early  # уже получен выше при корректировке цены
            open_card_total   = cs.get("open_card", 0)
            add_to_cart_total = cs.get("add_to_cart", 0)
            # Рекламные переходы = клики из рекламы (AdMetrics)
            ad_clicks_day     = ad_clicks
            # Органические переходы = всего − рекламные (не уходим в минус)
            organic_clicks    = max(0, open_card_total - ad_clicks_day)
            # Доля органики
            organic_pct       = round(organic_clicks / open_card_total * 100, 0) if open_card_total > 0 else 0.0
            # Конверсия: переход → корзина
            cart_from_card_pct = round(add_to_cart_total / open_card_total * 100, 0) if open_card_total > 0 else 0.0
            # Конверсия: корзина → заказ (используем фактические заказы дня)
            order_from_cart_pct = round(o_qty / add_to_cart_total * 100, 0) if add_to_cart_total > 0 else 0.0

            # Налоги: 1% УСН + 5% НДС от цены до СПП (суммы продаж)
            tax_usn_per_unit   = round(p_before * 0.01, 2)
            tax_nds_per_unit   = round(p_before * 0.05, 2)
            tax_total_per_unit = round(p_before * 0.06, 2)

            # Прогноз прибыли, ₽:
            #   = (Цена до СПП − комиссия − логистика − налоги − себестоимость) × прогноз продаж, шт
            #     − хранение, ₽ (суммарно за день, не на единицу)
            #     − бюджет РК, ₽ (суммарно за день)
            # Прогноз прибыли на 1 ед. = прогноз прибыли / прогноз продаж
            # storage_per_unit здесь — суммарная стоимость хранения за день (не на ед., название историческое)
            variable_margin = p_before - commission_per_unit - logistics_per_unit - tax_total_per_unit - cogs
            profit_total = round(
                variable_margin * forecast_sales_qty - storage_per_unit - ad_spend, 2
            ) if forecast_sales_qty else round(-storage_per_unit - ad_spend, 2)
            profit_per_unit = round(profit_total / forecast_sales_qty, 2) if forecast_sales_qty > 0 else 0.0

            margin_pct = round(profit_per_unit / p_before * 100, 2) if p_before > 0 else 0
            roi_pct = round(profit_per_unit / cogs * 100, 2) if cogs > 0 else 0

            # ДРР (Доля Рекламных Расходов)
            drr_orders_pct = round(ad_spend / o_rub * 100, 2) if o_rub > 0 else 0.0
            drr_sales_pct = round(ad_spend / forecast_sales_rub * 100, 2) if forecast_sales_rub > 0 else 0.0

            # Сбор за возврат (Lamoda 29 ₽/ед.) — в РнП всегда 0 (только WB/Ozon)
            return_fee_rub = 0.0

            days_data[ds] = {
                "orders_qty": o_qty,
                "orders_rub": round(o_rub, 2),
                "sales_qty": s_qty,
                "sales_rub": round(s_rub, 2),
                "returns_qty": int(r_qty),
                "cancellations_qty": int(c_qty),
                "forecast_sales_qty": forecast_sales_qty,
                "forecast_sales_rub": forecast_sales_rub,
                "price_before_spp": round(p_before, 2),
                "price_after_spp": round(p_after, 2),
                "spp_pct": round(spp, 2),
                "stock_wb": stock,
                "in_way_to_client": in_way_to,
                "in_way_from_client": in_way_from,
                "frozen_capital": round((stock + in_way_to + in_way_from) * cogs, 2),
                "buyout_rate_pct": round(buyout_day * 100, 2),
                "margin_pct": margin_pct,
                "roi_pct": roi_pct,
                "profit_per_unit": round(profit_per_unit, 2),
                "profit_total": profit_total,
                "commission_per_unit": round(commission_per_unit, 2),
                "commission_pct": round(commission_pct, 2),
                "logistics_per_unit": round(logistics_per_unit, 2),
                "storage_per_unit": storage_per_unit,
                "cogs_per_unit": round(cogs, 2),
                "tax_usn_per_unit": tax_usn_per_unit,
                "tax_nds_per_unit": tax_nds_per_unit,
                "tax_total_per_unit": tax_total_per_unit,
                "return_fee_rub": return_fee_rub,
                "drr_orders_pct": drr_orders_pct,
                "drr_sales_pct": drr_sales_pct,
                "ad_spend": round(ad_spend, 2),
                "ad_orders_qty": ad_orders_qty,
                # Детальные метрики рекламы (для раздела РК)
                "ad_total":   ad_total,
                "ad_search":  ad_search,
                "ad_recommend": ad_recom,
                # Воронка карточки (nm-report)
                "open_card_count":      open_card_total,
                "ad_clicks_count":      ad_clicks_day,
                "organic_clicks_count": organic_clicks,
                "organic_clicks_pct":   organic_pct,
                "add_to_cart_count":    add_to_cart_total,
                "cart_from_card_pct":   cart_from_card_pct,
                "order_from_cart_pct":  order_from_cart_pct,
            }

            total_orders_qty += o_qty
            total_orders_rub += o_rub
            total_sales_qty += s_qty
            total_returns_qty += int(r_qty)
            if p_before > 0:
                price_samples.append(p_before)
            if p_after > 0:
                price_after_spp_samples.append(p_after)
            if spp > 0:
                spp_samples.append(spp)
            if commission_pct > 0:
                commission_pct_samples.append(commission_pct)

        # Итого / средние за период
        avg_p_before = round(sum(price_samples) / len(price_samples), 2) if price_samples else 0
        avg_p_after = round(sum(price_after_spp_samples) / len(price_after_spp_samples), 2) if price_after_spp_samples else 0
        avg_spp_total = round(sum(spp_samples) / len(spp_samples), 2) if spp_samples else 0
        avg_commission_pct = round(sum(commission_pct_samples) / len(commission_pct_samples), 4) if commission_pct_samples else float(channel.commission_pct)
        current_stock = _current_stock(db, sku.id, channel_type=channel.type.value)

        # Процент выкупа
        buyout_pct = round(buyout_rate * 100, 2)

        # Средняя маржа за период
        margins = [days_data[d.isoformat()]["margin_pct"] for d in day_list if days_data[d.isoformat()]["orders_qty"] > 0]
        avg_margin = round(sum(margins) / len(margins), 2) if margins else 0

        avg_daily_orders = total_orders_qty / days
        turnover_days = round(current_stock / avg_daily_orders, 1) if avg_daily_orders > 0 else 999

        # Скрыть SKU без активности за период:
        # не было остатков И не было заказов/продаж/возвратов
        had_stock_in_period = current_stock > 0 or any(
            (days_data[d.isoformat()]["stock_wb"] or 0) > 0
            or (days_data[d.isoformat()]["in_way_to_client"] or 0) > 0
            or (days_data[d.isoformat()]["in_way_from_client"] or 0) > 0
            for d in day_list
        )
        had_activity = (total_orders_qty + total_sales_qty + total_returns_qty) > 0
        if not had_stock_in_period and not had_activity:
            continue

        sc_row = (
            db.query(SKUChannel.mp_article, SKUChannel.photo_url)
            .filter(SKUChannel.sku_id == sku.id, SKUChannel.channel_id == channel.id)
            .first()
        )
        mp_article = sc_row[0] if sc_row else ""
        sc_photo_url = sc_row[1] if sc_row else None

        # photo_url: для WB вычисляем по формуле, для остальных — берём из SKUChannel.photo_url
        if channel.type == ChannelType.WB:
            photo_url_final = wb_photo_url(mp_article)
        else:
            photo_url_final = sc_photo_url or ""

        skus_result.append({
            "sku_id": sku.id,
            "channel_id": channel.id,
            "seller_article": sku.seller_article,
            "name": sku.name,
            "channel_type": channel.type.value,
            "channel_name": channel.name,
            "wb_article": mp_article,
            "photo_url": photo_url_final,
            # Итого за период
            "total_orders_qty": total_orders_qty,
            "total_orders_rub": round(total_orders_rub, 2),
            "total_sales_qty": total_sales_qty,
            "total_returns_qty": total_returns_qty,
            "avg_price_before_spp": avg_p_before,
            "avg_price_after_spp": avg_p_after,
            "avg_spp_pct": avg_spp_total,
            "current_stock": current_stock,
            "turnover_days": turnover_days,
            "buyout_rate_pct": buyout_pct,
            "buyout_rate_is_manual": buyout_rate_is_manual,
            "logistics_per_unit_avg": round(avg_logistics_real, 2),
            "logistics_is_manual": logistics_is_manual,
            "commission_pct_avg": round(commission_pct_override_val, 4) if commission_is_manual else round(avg_commission_pct, 4),
            "commission_is_manual": commission_is_manual,
            "avg_margin_pct": avg_margin,
            "cogs_per_unit": round(cogs, 2),
            "wb_rating": round(float(sku.wb_rating), 2) if sku.wb_rating else None,
            # По дням
            "days": days_data,
        })

    return {
        "ref_date": ref_date.isoformat(),
        "days": [d.isoformat() for d in day_list],
        "skus": skus_result,
    }
