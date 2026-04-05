"""
Сервис «Оцифровка» — фактическая P&L-аналитика по SKU × каналу.

В отличие от РнП (прогнозная), здесь только ФАКТ:
  — заказы, продажи (доставлено), возвраты
  — выручка (Sale.price), комиссия (Sale.commission), логистика (Sale.logistics)
  — хранение (StorageCost, только WB)
  — сбор за возврат (Lamoda: 29 ₽/ед.)
  — реклама (AdMetrics.budget)
  — налоги: 1% УСН + 5% НДС = 6% от выручки
  — себестоимость = продажи × COGS/ед.
  — прибыль = выручка - все затраты
  — маржинальность = прибыль / выручка × 100
  — оборачиваемость = текущий остаток / (продажи / дней)
"""
from datetime import date, timedelta
from typing import List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.ads import AdCampaign, AdMetrics
from app.models.catalog import Channel, ChannelType, SKU, SKUChannel
from app.models.inventory import ProductBatch, SKUCostHistory, Stock, StorageCost
from app.models.sales import Order, OrderStatus, Price, Return, Sale, SkuDailyExpense
from app.services.rnp_pivot_service import wb_photo_url  # reuse CDN formula

from app.services.ozon_finance import get_ozon_customer_price_ratio, get_ozon_fin_ratios

LAMODA_RETURN_FEE_RUB = 29.0
TAX_PCT = 0.06  # 1% УСН + 5% НДС


def _get_ozon_customer_price_ratio_UNUSED(db: Session) -> float:
    """Получить коэффициент (цена покупателя / цена продавца) из отчёта реализации Ozon.
    Отчёт доступен помесячно, за прошлый месяц. Кешируется на сессию."""
    cache_key = "ozon_cpr"
    if cache_key in _ozon_customer_price_ratio_cache:
        return _ozon_customer_price_ratio_cache[cache_key]

    try:
        from app.models.integration import Integration, IntegrationType
        import httpx
        from datetime import date

        integ = db.query(Integration).filter(
            Integration.type == IntegrationType.OZON,
            Integration.is_active == True,
        ).first()
        if not integ:
            return 0.489  # fallback

        headers = {
            "Client-Id": str(integ.client_id),
            "Api-Key": str(integ.api_key),
            "Content-Type": "application/json",
        }

        # Берём данные за прошлый месяц
        today = date.today()
        if today.month == 1:
            m, y = 12, today.year - 1
        else:
            m, y = today.month - 1, today.year

        r = httpx.post(
            "https://api-seller.ozon.ru/v2/finance/realization",
            headers=headers,
            json={"month": m, "year": y},
            timeout=30,
        )
        if r.status_code != 200:
            _ozon_customer_price_ratio_cache[cache_key] = 0.489
            return 0.489

        rows = r.json().get("result", {}).get("rows", [])
        total_seller = 0.0
        total_customer = 0.0
        for row in rows:
            dc = row.get("delivery_commission") or {}
            qty = dc.get("quantity") or 0
            if qty > 0:
                total_seller += (row.get("seller_price_per_instance") or 0) * qty
                total_customer += dc.get("amount") or 0

        ratio = total_customer / total_seller if total_seller > 0 else 0.489
        _ozon_customer_price_ratio_cache[cache_key] = ratio
        return ratio
    except Exception:
        _ozon_customer_price_ratio_cache[cache_key] = 0.489
        return 0.489


# Кеш фактических % комиссии и эквайринга Ozon из финансового отчёта
_ozon_fin_ratios_cache: dict[str, dict] = {}


def _get_ozon_fin_ratios_UNUSED(db: Session) -> dict:
    """Фактические % комиссии и эквайринга Ozon из /v1/finance/cash-flow-statement/list.
    Возвращает {'commission_pct': float, 'acquiring_pct': float, 'total_pct': float}.
    Берётся из последних периодов (30 дней). Кешируется на сессию."""
    cache_key = "ozon_fin"
    if cache_key in _ozon_fin_ratios_cache:
        return _ozon_fin_ratios_cache[cache_key]

    fallback = {"commission_pct": 39.2, "acquiring_pct": 0.8, "total_pct": 40.0}

    try:
        from app.models.integration import Integration, IntegrationType
        import httpx
        from datetime import date, timedelta

        integ = db.query(Integration).filter(
            Integration.type == IntegrationType.OZON,
            Integration.is_active == True,
        ).first()
        if not integ:
            _ozon_fin_ratios_cache[cache_key] = fallback
            return fallback

        headers = {
            "Client-Id": str(integ.client_id),
            "Api-Key": str(integ.api_key),
            "Content-Type": "application/json",
        }

        today = date.today()
        r = httpx.post(
            "https://api-seller.ozon.ru/v1/finance/cash-flow-statement/list",
            headers=headers,
            json={
                "date": {
                    "from": f"{(today - timedelta(days=30)).isoformat()}T00:00:00.000Z",
                    "to": f"{today.isoformat()}T23:59:59.000Z",
                },
                "with_details": True,
                "page": 1,
                "page_size": 10,
            },
            timeout=30,
        )
        if r.status_code != 200:
            _ozon_fin_ratios_cache[cache_key] = fallback
            return fallback

        result = r.json().get("result", {})
        cash_flows = result.get("cash_flows", [])
        details_list = result.get("details", [])
        if not isinstance(details_list, list):
            details_list = [details_list] if details_list else []

        total_orders = sum(abs(cf.get("orders_amount", 0)) for cf in cash_flows)
        total_commission = sum(abs(cf.get("commission_amount", 0)) for cf in cash_flows)

        total_acq = 0.0
        for det in details_list:
            if not det:
                continue
            for item in (det.get("others") or {}).get("items", []):
                if "acquiring" in (item.get("name") or "").lower():
                    total_acq += abs(item.get("price", 0))

        if total_orders > 0:
            comm_pct = round(total_commission / total_orders * 100, 2)
            acq_pct = round(total_acq / total_orders * 100, 2)
            ratios = {
                "commission_pct": comm_pct,
                "acquiring_pct": acq_pct,
                "total_pct": round(comm_pct + acq_pct, 2),
            }
        else:
            ratios = fallback

        _ozon_fin_ratios_cache[cache_key] = ratios
        return ratios
    except Exception:
        _ozon_fin_ratios_cache[cache_key] = fallback
        return fallback


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _cogs_per_unit(db: Session, sku_id: int, ref_date: date) -> float:
    record = (
        db.query(SKUCostHistory)
        .filter(SKUCostHistory.sku_id == sku_id, SKUCostHistory.effective_from <= ref_date)
        .order_by(SKUCostHistory.effective_from.desc())
        .first()
    )
    if record:
        return float(record.cost_per_unit)
    batch = (
        db.query(ProductBatch)
        .filter(ProductBatch.sku_id == sku_id)
        .order_by(ProductBatch.batch_date.desc())
        .first()
    )
    return float(batch.total_cost_per_unit) if batch else 0.0


def _current_stock(db: Session, sku_id: int, channel_type: Optional["ChannelType"] = None) -> int:
    """
    Возвращает текущий остаток SKU.
    Если передан channel_type — берём только склады соответствующего маркетплейса
    (имена складов начинаются с WB/Ozon/Lamoda).
    """
    from app.models.catalog import Warehouse  # local import to avoid circular
    q = db.query(func.max(Stock.date)).filter(Stock.sku_id == sku_id)
    if channel_type is not None:
        prefix_map = {
            ChannelType.WB: "WB",
            ChannelType.OZON: "Ozon",
            ChannelType.LAMODA: "Lamoda",
        }
        prefix = prefix_map.get(channel_type)
        if prefix:
            wh_ids = [
                wh.id
                for wh in db.query(Warehouse.id).filter(Warehouse.name.ilike(f"{prefix}%")).all()
            ]
            if wh_ids:
                q = q.filter(Stock.warehouse_id.in_(wh_ids))
    latest = q.scalar()
    if not latest:
        return 0
    sq = db.query(func.sum(Stock.qty)).filter(Stock.sku_id == sku_id, Stock.date == latest)
    if channel_type is not None:
        prefix_map = {
            ChannelType.WB: "WB",
            ChannelType.OZON: "Ozon",
            ChannelType.LAMODA: "Lamoda",
        }
        prefix = prefix_map.get(channel_type)
        if prefix:
            from app.models.catalog import Warehouse
            wh_ids = [
                wh.id
                for wh in db.query(Warehouse.id).filter(Warehouse.name.ilike(f"{prefix}%")).all()
            ]
            if wh_ids:
                sq = sq.filter(Stock.warehouse_id.in_(wh_ids))
    return int(sq.scalar() or 0)


def _had_stock_in_period(
    db: Session,
    sku_id: int,
    channel_type: Optional["ChannelType"],
    date_from: date,
    date_to: date,
) -> bool:
    """Был ли ненулевой остаток SKU хотя бы на один день в периоде."""
    from app.models.catalog import Warehouse
    q = db.query(func.max(Stock.qty)).filter(
        Stock.sku_id == sku_id,
        Stock.date >= date_from,
        Stock.date <= date_to,
    )
    if channel_type is not None:
        prefix_map = {
            ChannelType.WB: "WB",
            ChannelType.OZON: "Ozon",
            ChannelType.LAMODA: "Lamoda",
        }
        prefix = prefix_map.get(channel_type)
        if prefix:
            wh_ids = [
                wh.id
                for wh in db.query(Warehouse.id).filter(Warehouse.name.ilike(f"{prefix}%")).all()
            ]
            if wh_ids:
                q = q.filter(Stock.warehouse_id.in_(wh_ids))
    return int(q.scalar() or 0) > 0


def _current_price(db: Session, sku_id: int, channel_id: int, ref_date: date) -> float:
    """Последняя известная цена продавца (до соинвеста) ≤ ref_date."""
    row = (
        db.query(Price.price_before_spp)
        .filter(
            Price.sku_id == sku_id,
            Price.channel_id == channel_id,
            Price.price_before_spp > 0,
            Price.date <= ref_date,
        )
        .order_by(Price.date.desc())
        .first()
    )
    return float(row[0]) if row else 0.0


def _customer_price(db: Session, sku_id: int, channel_id: int, ref_date: date) -> float:
    """Последняя известная цена покупателя (после соинвеста/скидок) ≤ ref_date.
    Для налога УСН: база = цена, по которой клиент купил."""
    row = (
        db.query(Price.price_after_spp)
        .filter(
            Price.sku_id == sku_id,
            Price.channel_id == channel_id,
            Price.price_after_spp > 0,
            Price.date <= ref_date,
        )
        .order_by(Price.date.desc())
        .first()
    )
    return float(row[0]) if row else 0.0


# ─── Основная функция ─────────────────────────────────────────────────────────

def get_otsifrovka(
    db: Session,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    days: int = 30,
    channel_type: Optional[str] = None,
    channels: Optional[List[str]] = None,
    article: Optional[str] = None,
) -> dict:
    """
    Возвращает фактическую P&L-аналитику по всем активным SKU × каналу за период.

    Приоритет: date_from/date_to → days (если даты не переданы).
    channels: список ["wb", "ozon", "lamoda"] — если None/пусто, все каналы.
    channel_type: устаревший одиночный фильтр (для обратной совместимости).
    article: фильтр по seller_article (частичное совпадение).
    """
    today = date.today()
    if date_from is None:
        date_from = today - timedelta(days=days)
    if date_to is None:
        date_to = today
    days = (date_to - date_from).days or 1
    ref_date = date_to - timedelta(days=1) if date_to == today else date_to

    # Фильтр каналов: новый мультиселект (channels) или старый одиночный (channel_type)
    default_types = [ChannelType.WB, ChannelType.OZON, ChannelType.LAMODA]
    if channels:
        parsed = []
        for c in channels:
            try:
                parsed.append(ChannelType(c.lower()))
            except ValueError:
                pass
        allowed_types = parsed if parsed else default_types
    elif channel_type:
        try:
            allowed_types = [ChannelType(channel_type.lower())]
        except ValueError:
            allowed_types = default_types
    else:
        allowed_types = default_types

    q = (
        db.query(SKUChannel)
        .join(SKU, SKU.id == SKUChannel.sku_id)
        .join(Channel, Channel.id == SKUChannel.channel_id)
        .filter(SKU.is_active == True, Channel.is_active == True)
        .filter(Channel.type.in_(allowed_types))
    )
    if article:
        q = q.filter(SKU.seller_article.ilike(f"%{article}%"))
    sku_channels = q.all()

    rows = []

    for sc in sku_channels:
        sku: SKU = sc.sku
        channel: Channel = sc.channel

        # ── Заказы (все, включая отменённые — полный спрос) ──────────────────
        ord_row = db.query(
            func.count(Order.id),
            func.sum(Order.price),
        ).filter(
            Order.sku_id == sku.id,
            Order.channel_id == channel.id,
            Order.order_date >= date_from,
            Order.order_date <= date_to,
        ).first()
        orders_qty = int(ord_row[0] or 0)
        orders_rub = float(ord_row[1] or 0)

        # ── Продажи (доставлено) ────────────────────
        if channel.type == ChannelType.LAMODA:
            # Lamoda: продажи = заказы со статусом DELIVERED (реально выкупленные).
            sal_row_lm = db.query(
                func.count(Order.id),
                func.sum(Order.price),
            ).filter(
                Order.sku_id == sku.id,
                Order.channel_id == channel.id,
                Order.order_date >= date_from,
                Order.order_date <= date_to,
                Order.status == OrderStatus.DELIVERED,
            ).first()
            sales_qty = int(sal_row_lm[0] or 0)
            sales_rub = float(sal_row_lm[1] or 0)
            sal_row = (0, 0, 0, 0)  # заглушка — Lamoda не использует Sale для расходов
        else:
            sal_row = db.query(
                func.count(Sale.id),
                func.sum(Sale.price),
                func.sum(Sale.commission),
                func.sum(Sale.logistics),
            ).filter(
                Sale.sku_id == sku.id,
                Sale.channel_id == channel.id,
                Sale.sale_date >= date_from,
                Sale.sale_date <= date_to,
            ).first()
            sales_qty = int(sal_row[0] or 0)
            sales_rub = float(sal_row[1] or 0)

        # ── Расходы МП ────────────────────────────────────────────────────
        penalty_rub = 0.0
        acceptance_rub = 0.0
        other_ded_rub = 0.0
        wb_has_data = False  # будет True если WB SkuDailyExpense содержит данные
        realization_rub = 0.0
        compensation_rub = 0.0
        returns_rub = 0.0
        returns_rub_raw = 0.0

        if channel.type == ChannelType.OZON:
            # Ozon: все данные из SkuDailyExpense (транзакции по operation_date)
            # Формулы по стандарту TrueStats (оцифровка для селлеров)
            from app.services.ozon_finance import get_ozon_bonus_ratio
            exp_row = db.query(
                func.sum(SkuDailyExpense.commission),         # [0] sale_commission
                func.sum(SkuDailyExpense.logistics),          # [1]
                func.sum(SkuDailyExpense.storage),            # [2]
                func.sum(SkuDailyExpense.penalty),            # [3]
                func.sum(SkuDailyExpense.acceptance),         # [4]
                func.sum(SkuDailyExpense.other_deductions),   # [5]
                func.sum(SkuDailyExpense.return_amount),      # [6] возвраты (в ценах реализации)
                func.sum(SkuDailyExpense.compensation),       # [7] реализация = accruals_for_sale
                func.sum(SkuDailyExpense.acquiring),          # [8] эквайринг
                func.sum(SkuDailyExpense.items_count),        # [9] кол-во доставленных единиц
            ).filter(
                SkuDailyExpense.sku_id == sku.id,
                SkuDailyExpense.channel_id == channel.id,
                SkuDailyExpense.date >= date_from,
                SkuDailyExpense.date <= date_to,
            ).first()

            # Реализация = accruals_for_sale (совпадает с TrueStats для свежих периодов)
            # Для 30+ дней TrueStats накапливает данные ежедневно — наш ежедневный sync делает то же
            realization_rub = float(exp_row[7] or 0)
            accruals_rub = realization_rub  # для К перечислению

            # Компенсация = соинвест Ozon (bonus balls, банк. соинвест)
            bonus_ratio = get_ozon_bonus_ratio(db)
            compensation_rub = round(realization_rub * bonus_ratio, 2)

            # Продажи = цена покупателя = Реализация - Компенсация
            sales_rub = round(realization_rub - compensation_rub, 2)
            # sales_qty будет рассчитан ниже после returns_rub_raw
            _items_total = int(exp_row[9] or 0)

            # Комиссия = sale_commission + эквайринг (как в TrueStats)
            commission_rub = float(exp_row[0] or 0) + float(exp_row[8] or 0)
            logistics_rub = float(exp_row[1] or 0)
            storage_rub = float(exp_row[2] or 0)
            penalty_rub = float(exp_row[3] or 0)
            acceptance_rub = float(exp_row[4] or 0)
            other_ded_rub = float(exp_row[5] or 0)

            # Возвраты в ценах покупателя (пропорционально bonus_ratio)
            returns_rub_raw = float(exp_row[6] or 0)  # в ценах реализации
            returns_rub = round(returns_rub_raw * (1 - bonus_ratio), 2)  # в ценах покупателя
        elif channel.type == ChannelType.LAMODA:
            # Lamoda FBO: комиссия 40.3% от чистых продаж (после возвратов), логистика включена
            # commission_rub пересчитается ниже после расчёта returns_rub
            commission_rub = 0.0  # placeholder
            logistics_rub = 0.0
            storage_rub = 0.0
        elif channel.type == ChannelType.WB:
            # WB: данные из SkuDailyExpense (синхронизированы из reportDetailByPeriod)
            wb_exp = db.query(
                func.sum(SkuDailyExpense.compensation),       # [0] ppvz_for_pay продаж
                func.sum(SkuDailyExpense.sale_amount),        # [1] retail_price продаж (Реализация)
                func.sum(SkuDailyExpense.logistics),          # [2] delivery_rub
                func.sum(SkuDailyExpense.penalty),            # [3] штрафы
                func.sum(SkuDailyExpense.other_deductions),   # [4] прочие
                func.sum(SkuDailyExpense.acquiring),          # [5] эквайринг
                func.sum(SkuDailyExpense.return_amount),      # [6] retail_price возвратов
                func.sum(SkuDailyExpense.items_count),        # [7] кол-во продаж
                func.sum(SkuDailyExpense.commission),         # [8] ppvz_for_pay возвратов
            ).filter(
                SkuDailyExpense.sku_id == sku.id,
                SkuDailyExpense.channel_id == channel.id,
                SkuDailyExpense.date >= date_from,
                SkuDailyExpense.date <= date_to,
            ).first()

            wb_has_data = wb_exp and wb_exp[0] is not None and float(wb_exp[0] or 0) > 0

            if wb_has_data:
                # Данные из финотчёта WB (reportDetailByPeriod)
                # [0] compensation = ppvz_for_pay продаж
                # [1] sale_amount = retail_price продаж (Реализация)
                # [2] logistics = delivery_rub
                # [5] acquiring = acquiring_fee
                # [6] return_amount = retail_price возвратов (Возвраты)
                # [7] items_count = кол-во строк Продажа
                # commission field = ppvz_for_pay возвратов (для вычета из NET)
                retail_amount_sales = float(wb_exp[0] or 0)    # compensation = retail_amount продаж
                retail_sales = float(wb_exp[1] or 0)            # sale_amount = retail_price продаж
                retail_amount_returns = float(wb_exp[8] or 0) if wb_exp[8] else 0  # commission = retail_amount возвратов
                retail_returns = float(wb_exp[6] or 0)          # return_amount = retail_price возвратов

                # Продажи = retail_amount_sales - retail_amount_returns (TrueStats формула)
                sales_rub = retail_amount_sales - retail_amount_returns
                # Реализация NET = retail_sales - retail_returns (TrueStats "Реализация")
                realization_rub = retail_sales - retail_returns
                # Возвраты = retail возвратов (TrueStats "Возвраты")
                returns_rub = retail_returns
                # Комиссия нетто = Продажи - ppvz_net ≈ маленькая (СПП компенсация)
                commission_rub = 0.0  # нетто-комиссия WB ≈ 0 после СПП

                logistics_rub = float(wb_exp[2] or 0)
                penalty_rub = float(wb_exp[3] or 0)
                other_ded_rub = float(wb_exp[4] or 0)
                # NET продаж = gross deliveries - returns (из Return таблицы)
                gross_qty = int(wb_exp[7] or 0)
                wb_returns_qty = int(
                    db.query(func.count(Return.id)).filter(
                        Return.sku_id == sku.id,
                        Return.channel_id == channel.id,
                        Return.return_date >= date_from,
                        Return.return_date <= date_to,
                    ).scalar() or 0
                )
                sales_qty = max(gross_qty - wb_returns_qty, 0)
                compensation_rub = 0.0
                # Хранение из финотчёта (SkuDailyExpense.storage)
                storage_rub_fin = float(
                    db.query(func.sum(SkuDailyExpense.storage)).filter(
                        SkuDailyExpense.sku_id == sku.id,
                        SkuDailyExpense.channel_id == channel.id,
                        SkuDailyExpense.date >= date_from,
                        SkuDailyExpense.date <= date_to,
                    ).scalar() or 0
                )
                storage_rub = storage_rub_fin if storage_rub_fin > 0 else 0.0
            else:
                # Fallback: данные из Sale, но логистику берём из финотчёта если есть
                commission_rub = float(sal_row[2] or 0)
                wb_log_fallback = float(
                    db.query(func.sum(SkuDailyExpense.logistics)).filter(
                        SkuDailyExpense.sku_id == sku.id,
                        SkuDailyExpense.channel_id == channel.id,
                        SkuDailyExpense.date >= date_from,
                        SkuDailyExpense.date <= date_to,
                    ).scalar() or 0
                )
                logistics_rub = wb_log_fallback if wb_log_fallback > 0 else float(sal_row[3] or 0)
                storage_rub = 0.0
                realization_rub = sales_rub
                compensation_rub = 0.0
                returns_rub = 0.0
        else:
            # Другие каналы: из Sale
            commission_rub = float(sal_row[2] or 0)
            logistics_rub = float(sal_row[3] or 0)
            storage_rub = 0.0

        # Если комиссия не записана и нет WB expenses — считаем по % канала
        if commission_rub == 0 and sales_rub > 0 and channel.type not in (ChannelType.OZON, ChannelType.WB):
            commission_rub = round(sales_rub * float(sc.commission_pct_override or channel.commission_pct) / 100, 2)

        # ── Возвраты ─────────────────────────────────────────────────────
        if channel.type == ChannelType.LAMODA:
            # Lamoda: возвраты = записи из returns, где И return_date И order_date в периоде.
            # Это соответствует аналитике Lamoda: возврат засчитывается только для заказов
            # этого периода, фактически вернувшихся в этом же периоде.
            from sqlalchemy import text
            returns_qty = int(
                db.execute(text("""
                    SELECT count(*) FROM returns r
                    JOIN orders o ON o.external_id = replace(r.external_id, 'lm_ret_', '')
                    WHERE r.sku_id = :sku_id AND r.channel_id = :ch_id
                      AND r.return_date BETWEEN :d1 AND :d2
                      AND o.order_date BETWEEN :d1 AND :d2
                """), {"sku_id": sku.id, "ch_id": channel.id, "d1": date_from, "d2": date_to}
                ).scalar() or 0
            )
        else:
            # WB / Ozon: отказы (информационно) + физические возвраты (финансово)
            cancellations_qty = int(
                db.query(func.count(Order.id)).filter(
                    Order.sku_id == sku.id,
                    Order.channel_id == channel.id,
                    Order.order_date >= date_from,
                    Order.order_date <= date_to,
                    Order.status == OrderStatus.CANCELLED,
                ).scalar() or 0
            )
            refunds_qty = int(
                db.query(func.count(Return.id)).filter(
                    Return.sku_id == sku.id,
                    Return.channel_id == channel.id,
                    Return.return_date >= date_from,
                    Return.return_date <= date_to,
                ).scalar() or 0
            )
            returns_qty = cancellations_qty + refunds_qty

        # ── Возвраты в рублях (ТОЛЬКО физические возвраты — влияют на P&L) ──
        # Отменённые заказы НЕ вычитаются: деньги по ним не поступали.
        if channel.type == ChannelType.LAMODA:
            returns_rub = float(
                db.execute(text("""
                    SELECT coalesce(sum(o.price), 0) FROM returns r
                    JOIN orders o ON o.external_id = replace(r.external_id, 'lm_ret_', '')
                    WHERE r.sku_id = :sku_id AND r.channel_id = :ch_id
                      AND r.return_date BETWEEN :d1 AND :d2
                      AND o.order_date BETWEEN :d1 AND :d2
                """), {"sku_id": sku.id, "ch_id": channel.id, "d1": date_from, "d2": date_to}
                ).scalar() or 0
            )
        elif channel.type == ChannelType.OZON:
            # Ozon: returns_rub уже рассчитан выше (в ценах покупателя)
            pass
        elif channel.type == ChannelType.WB and wb_has_data:
            # WB: returns_rub уже рассчитан из SkuDailyExpense
            pass
        else:
            # Fallback: оцениваем по средней цене
            avg_sale_price = (sales_rub / sales_qty) if sales_qty > 0 else 0.0
            returns_rub = round(refunds_qty * avg_sale_price, 2)

        # ── Пересчёт комиссии Lamoda ──
        if channel.type == ChannelType.LAMODA:
            # Комиссия от продаж (DELIVERED). Возвраты — из прошлых периодов,
            # их стоимость не вычитается из текущих продаж. Финансовый эффект возвратов =
            # только сбор за возврат (29₽/шт), который считается отдельно.
            commission_rub = round(sales_rub * float(sc.commission_pct_override or channel.commission_pct) / 100, 2)
            returns_rub = 0.0  # не влияет на P&L текущего периода

        # ── Хранение (WB из StorageCost, Ozon уже выше из SkuDailyExpense) ──
        storage_rub_val = 0.0
        if channel.type == ChannelType.WB:
            storage_rub_val = float(
                db.query(func.sum(StorageCost.cost)).filter(
                    StorageCost.sku_id == sku.id,
                    StorageCost.date >= date_from,
                    StorageCost.date <= date_to,
                ).scalar() or 0
            )
            storage_rub = storage_rub_val

        # ── Сбор за возврат (Lamoda) ────────────────
        return_fee_rub = round(returns_qty * LAMODA_RETURN_FEE_RUB, 2) if channel.type == ChannelType.LAMODA else 0.0

        # ── Реклама ─────────────────────────────────
        ad_agg = db.query(
            func.sum(AdMetrics.budget),
            func.sum(AdMetrics.search_budget),
            func.sum(AdMetrics.recommend_budget),
        ).join(
            AdCampaign, AdCampaign.id == AdMetrics.campaign_id
        ).filter(
            AdCampaign.sku_id == sku.id,
            AdCampaign.channel_id == channel.id,
            AdMetrics.date >= date_from,
            AdMetrics.date <= date_to,
        ).one()
        ad_spend_rub = float(ad_agg[0] or 0)
        ad_search_rub = float(ad_agg[1] or 0)
        ad_recommend_rub = float(ad_agg[2] or 0)
        ad_search_pct = round(ad_search_rub / ad_spend_rub * 100, 1) if ad_spend_rub > 0 else 0.0
        ad_recommend_pct = round(ad_recommend_rub / ad_spend_rub * 100, 1) if ad_spend_rub > 0 else 0.0

        # ── Ozon sales_qty (NET = items - returns) ──
        if channel.type == ChannelType.OZON:
            if _items_total > 0 and realization_rub > 0 and returns_rub_raw > 0:
                avg_unit_price = realization_rub / _items_total
                return_items = round(returns_rub_raw / avg_unit_price)
                sales_qty = max(_items_total - return_items, 0)
            else:
                sales_qty = _items_total

        # ── Штрафы / приёмка / прочие удержания МП ──
        fines_rub = penalty_rub

        # ── Налоги, себестоимость, К перечислению, прибыль ──────────
        if channel.type == ChannelType.OZON:
            # TrueStats формулы:
            # Налоговая база = Продажи(покупат.) - Возвраты(покупат.)
            tax_base_rub = max(sales_rub - returns_rub, 0)
            tax_rub = round(tax_base_rub * TAX_PCT, 2)

            # Себестоимость от продаж (шт) — НЕ вычитаем returns_qty
            # (returns_qty включает отмены, которые не были доставлены)
            cogs = _cogs_per_unit(db, sku.id, ref_date)
            cogs_rub = round(sales_qty * cogs, 2)

            # К перечислению = accruals - Комиссия - Логистика - Возвраты - Прочие
            # (от реальных денег accruals_for_sale, не от seller_price)
            payout_rub = round(accruals_rub - abs(commission_rub) - logistics_rub
                               - returns_rub_raw - storage_rub - fines_rub
                               - acceptance_rub - other_ded_rub, 2)

            # Прибыль = К_перечислению - Реклама - Налоги - Себестоимость
            # (К_перечислению уже содержит вычет комиссии, логистики, возвратов, прочих)
            total_costs = (commission_rub + logistics_rub + returns_rub_raw
                          + ad_spend_rub + tax_rub + cogs_rub
                          + storage_rub + fines_rub + acceptance_rub + other_ded_rub)
            profit_rub = round(payout_rub - ad_spend_rub - tax_rub - cogs_rub, 2)
            margin_pct = round(profit_rub / realization_rub * 100, 2) if realization_rub > 0 else 0.0
        elif channel.type == ChannelType.WB and wb_has_data:
            # WB: формулы TrueStats
            tax_base_rub = max(sales_rub - returns_rub, 0)
            tax_rub = round(tax_base_rub * TAX_PCT, 2)

            # Себестоимость от продаж (шт)
            cogs = _cogs_per_unit(db, sku.id, ref_date)
            cogs_rub = round(sales_qty * cogs, 2)

            # К перечислению = Продажи - Комиссия (acquiring)
            payout_rub = round(sales_rub - commission_rub, 2)

            # Прибыль = К_переч - Логистика - Хранение - Штрафы - Реклама - Налоги - Себест - Прочие
            profit_rub = round(payout_rub - logistics_rub - storage_rub - fines_rub
                               - ad_spend_rub - tax_rub - cogs_rub - other_ded_rub, 2)
            total_costs = round(commission_rub + logistics_rub + storage_rub + fines_rub
                               + ad_spend_rub + tax_rub + cogs_rub + other_ded_rub + returns_rub, 2)
            margin_pct = round(profit_rub / realization_rub * 100, 2) if realization_rub > 0 else 0.0
            returns_rub_raw = returns_rub
        else:
            # Lamoda / WB fallback
            net_sales_rub = sales_rub - returns_rub
            net_sales_qty = max(sales_qty - returns_qty, 0)
            tax_base_rub = max(net_sales_rub, 0)
            tax_rub = round(net_sales_rub * TAX_PCT, 2) if net_sales_rub > 0 else 0.0

            cogs = _cogs_per_unit(db, sku.id, ref_date)
            cogs_rub = round(sales_qty * cogs, 2)

            payout_rub = round(net_sales_rub - commission_rub - logistics_rub - storage_rub
                               - return_fee_rub - fines_rub - acceptance_rub - other_ded_rub, 2)

            total_costs = (returns_rub + commission_rub + logistics_rub + storage_rub
                          + return_fee_rub + fines_rub + acceptance_rub + other_ded_rub
                          + ad_spend_rub + tax_rub + cogs_rub)
            profit_rub = round(sales_rub - total_costs, 2)
            margin_pct = round(profit_rub / net_sales_rub * 100, 2) if net_sales_rub > 0 else 0.0

            realization_rub = sales_rub
            compensation_rub = 0.0
            returns_rub_raw = returns_rub

        # ── Остаток и оборачиваемость ────────────────
        current_stock = _current_stock(db, sku.id, channel.type)
        avg_daily_sales = sales_qty / days if days > 0 else 0
        turnover_days = round(current_stock / avg_daily_sales, 1) if avg_daily_sales > 0 else 999.0

        # ── ДРР ──────────────────────────────────────
        drr_orders_pct = round(ad_spend_rub / orders_rub * 100, 2) if orders_rub > 0 else 0.0
        drr_sales_pct = round(ad_spend_rub / sales_rub * 100, 2) if sales_rub > 0 else 0.0

        # ── Цена и % возвратов ────────────────────────
        avg_price = round(sales_rub / sales_qty, 2) if sales_qty > 0 else round(orders_rub / orders_qty, 2) if orders_qty > 0 else _current_price(db, sku.id, channel.id, ref_date)
        return_rate_pct = round(returns_qty / orders_qty * 100, 1) if orders_qty > 0 else 0.0

        # Скрыть SKU без активности за период:
        # не было остатков И не было заказов/продаж/возвратов
        had_activity = (orders_qty + sales_qty + returns_qty) > 0
        if not had_activity and current_stock == 0:
            if not _had_stock_in_period(db, sku.id, channel.type, date_from, date_to):
                continue

        # ── Фото ─────────────────────────────────────
        mp_article = sc.mp_article or ""
        if channel.type == ChannelType.WB:
            photo_url = wb_photo_url(mp_article)
        else:
            photo_url = sc.photo_url or ""

        rows.append({
            "sku_id": sku.id,
            "channel_id": channel.id,
            "seller_article": sku.seller_article,
            "name": sku.name,
            "channel_type": channel.type.value,
            "channel_name": channel.name,
            "photo_url": photo_url,
            "mp_article": mp_article,
            # Заказы
            "orders_qty": orders_qty,
            "orders_rub": round(orders_rub, 2),
            # Продажи (цена покупателя для Ozon, цена продавца для WB/Lamoda)
            "sales_qty": sales_qty,
            "sales_rub": round(sales_rub, 2),
            "avg_price": avg_price,
            # Реализация и компенсация (Ozon: accruals_for_sale и соинвест)
            "realization_rub": round(realization_rub, 2),
            "compensation_rub": round(compensation_rub, 2),
            # Возвраты
            "returns_qty": returns_qty,
            "returns_rub": round(returns_rub, 2),
            "return_rate_pct": return_rate_pct,
            # Затраты
            "commission_rub": round(commission_rub, 2),
            "logistics_rub": round(logistics_rub, 2),
            "storage_rub": round(storage_rub, 2),
            "return_fee_rub": round(return_fee_rub, 2),
            "fines_rub": round(fines_rub, 2),
            "acceptance_rub": round(acceptance_rub, 2),
            "other_deductions_rub": round(other_ded_rub, 2),
            "ad_spend_rub": round(ad_spend_rub, 2),
            "ad_search_pct": ad_search_pct,
            "ad_recommend_pct": ad_recommend_pct,
            "tax_base_rub": round(tax_base_rub, 2),
            "tax_rub": round(tax_rub, 2),
            "cogs_rub": round(cogs_rub, 2),
            "cogs_per_unit": round(cogs, 2),
            # Перечисление и ДРР
            "payout_rub": payout_rub,
            "drr_orders_pct": drr_orders_pct,
            "drr_sales_pct": drr_sales_pct,
            # P&L
            "total_costs_rub": round(total_costs, 2),
            "profit_rub": profit_rub,
            "margin_pct": margin_pct,
            # Остаток
            "current_stock": current_stock,
            "turnover_days": turnover_days,
        })

    # ── Постобработка: доля выручки, ABC-анализ, % выкупа ────────────────────
    total_sales_for_share = sum(r["sales_rub"] for r in rows)
    total_profit_for_abc  = sum(r["profit_rub"] for r in rows if r["profit_rub"] > 0)

    # Процент выкупа: продажи / (продажи + возвраты + незавершённые заказы в окне).
    # Упрощённая версия: sales / orders (если заказов больше продаж — учитываем неоформленные)
    for r in rows:
        o, s, ret = r["orders_qty"], r["sales_qty"], r["returns_qty"]
        # buyout = продажи / все заказы (включая отменённые)
        r["buyout_rate_pct"] = round(s / o * 100, 1) if o > 0 else 0.0
        # Доля в выручке
        r["revenue_share_pct"] = round(r["sales_rub"] / total_sales_for_share * 100, 1) if total_sales_for_share > 0 else 0.0

    # ABC по выручке (нисходящая сортировка)
    sorted_by_rev = sorted(rows, key=lambda r: r["sales_rub"], reverse=True)
    cum, total_rev = 0.0, total_sales_for_share or 1
    for r in sorted_by_rev:
        cum += r["sales_rub"]
        pct = cum / total_rev * 100
        r["abc_revenue"] = "A" if pct <= 80 else ("B" if pct <= 95 else "C")

    # ABC по прибыли (только строки с положительной прибылью; убыточные → C)
    sorted_by_profit = sorted(rows, key=lambda r: r["profit_rub"], reverse=True)
    cum_p = 0.0
    for r in sorted_by_profit:
        if r["profit_rub"] <= 0:
            r["abc_profit"] = "C"
            continue
        cum_p += r["profit_rub"]
        pct_p = cum_p / (total_profit_for_abc or 1) * 100
        r["abc_profit"] = "A" if pct_p <= 80 else ("B" if pct_p <= 95 else "C")

    # Сводные итоги
    summary = {
        "orders_qty": sum(r["orders_qty"] for r in rows),
        "orders_rub": round(sum(r["orders_rub"] for r in rows), 2),
        "sales_qty": sum(r["sales_qty"] for r in rows),
        "sales_rub": round(sum(r["sales_rub"] for r in rows), 2),
        "realization_rub": round(sum(r["realization_rub"] for r in rows), 2),
        "compensation_rub": round(sum(r["compensation_rub"] for r in rows), 2),
        "returns_qty": sum(r["returns_qty"] for r in rows),
        "returns_rub": round(sum(r["returns_rub"] for r in rows), 2),
        "commission_rub": round(sum(r["commission_rub"] for r in rows), 2),
        "logistics_rub": round(sum(r["logistics_rub"] for r in rows), 2),
        "storage_rub": round(sum(r["storage_rub"] for r in rows), 2),
        "return_fee_rub": round(sum(r["return_fee_rub"] for r in rows), 2),
        "fines_rub": round(sum(r["fines_rub"] for r in rows), 2),
        "acceptance_rub": round(sum(r["acceptance_rub"] for r in rows), 2),
        "other_deductions_rub": round(sum(r["other_deductions_rub"] for r in rows), 2),
        "ad_spend_rub": round(sum(r["ad_spend_rub"] for r in rows), 2),
        "ad_search_pct": round(
            sum(r["ad_spend_rub"] * r["ad_search_pct"] / 100 for r in rows if r["ad_spend_rub"] > 0) /
            sum(r["ad_spend_rub"] for r in rows if r["ad_spend_rub"] > 0) * 100, 1
        ) if any(r["ad_spend_rub"] > 0 for r in rows) else 0.0,
        "ad_recommend_pct": round(
            sum(r["ad_spend_rub"] * r["ad_recommend_pct"] / 100 for r in rows if r["ad_spend_rub"] > 0) /
            sum(r["ad_spend_rub"] for r in rows if r["ad_spend_rub"] > 0) * 100, 1
        ) if any(r["ad_spend_rub"] > 0 for r in rows) else 0.0,
        "tax_base_rub": round(sum(r["tax_base_rub"] for r in rows), 2),
        "tax_rub": round(sum(r["tax_rub"] for r in rows), 2),
        "cogs_rub": round(sum(r["cogs_rub"] for r in rows), 2),
        "profit_rub": round(sum(r["profit_rub"] for r in rows), 2),
        "payout_rub": round(sum(r["payout_rub"] for r in rows), 2),
    }
    total_sales_rub = summary["sales_rub"]
    summary["margin_pct"] = round(summary["profit_rub"] / total_sales_rub * 100, 2) if total_sales_rub > 0 else 0.0

    return {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "days": days,
        "summary": summary,
        "rows": rows,
    }
