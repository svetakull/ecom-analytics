"""
Синхронизация данных из WB API в базу.
"""
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.models.ads import AdCampaign, AdMetrics, AdType
from app.models.catalog import Channel, ChannelType, SKU, SKUChannel, Warehouse, WarehouseType
from app.models.integration import Integration
from app.models.inventory import Stock, StorageCost
from app.models.sales import CardStats, Order, OrderStatus, Price, Sale, Return
from app.services.wb_api import WBClient, WBApiError


def _get_or_create_sku(db: Session, seller_article: str, nm_id: str, name: str = "") -> SKU:
    sku = db.query(SKU).filter(SKU.seller_article == seller_article).first()
    if not sku:
        sku = SKU(seller_article=seller_article, name=name or seller_article)
        db.add(sku)
        db.flush()
    return sku


def _get_or_create_sku_channel(db: Session, sku: SKU, channel: Channel, mp_article: str) -> SKUChannel:
    sc = (
        db.query(SKUChannel)
        .filter(SKUChannel.sku_id == sku.id, SKUChannel.channel_id == channel.id)
        .first()
    )
    if not sc:
        sc = SKUChannel(sku_id=sku.id, channel_id=channel.id, mp_article=mp_article)
        db.add(sc)
        db.flush()
    return sc


def _get_wb_channel(db: Session) -> Channel:
    ch = db.query(Channel).filter(Channel.type == ChannelType.WB).first()
    if not ch:
        ch = Channel(name="Wildberries", type=ChannelType.WB, commission_pct=16.5)
        db.add(ch)
        db.flush()
    return ch


def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def sync_orders(db: Session, client: WBClient, days_back: int = 30) -> dict:
    """Синхронизировать заказы за последние N дней.
    Два запроса:
      flag=1 — по дате создания заказа (полнота: все заказы за период)
      flag=0 — по дате последнего изменения (актуальность: обновления статусов)
    """
    date_from = date.today() - timedelta(days=days_back)
    # flag=1: все заказы созданные после dateFrom (полнота)
    raw_by_creation = client.get_orders(date_from, flag=1)
    # flag=0: заказы изменённые после dateFrom (статусы отмен)
    raw_by_update = client.get_orders(date_from, flag=0)
    # Объединяем: по srid, приоритет flag=0 (свежие статусы)
    by_srid = {}
    for item in raw_by_creation:
        srid = str(item.get("srid") or "")
        if srid:
            by_srid[srid] = item
    for item in raw_by_update:
        srid = str(item.get("srid") or "")
        if srid:
            by_srid[srid] = item  # перезаписываем свежим статусом
    raw = list(by_srid.values())
    channel = _get_wb_channel(db)

    new_orders = 0
    updated = 0
    for item in raw:
        seller_article = str(item.get("supplierArticle") or item.get("article") or "")
        if not seller_article:
            continue

        # С flag=0: поле date = фактическая дата размещения заказа
        order_date = _parse_date(item.get("date") or item.get("dateCreated"))
        if not order_date:
            continue

        # srid — уникальный ID позиции заказа в WB
        srid = str(item.get("srid") or "")

        # priceWithDisc = цена после скидки продавца (до СПП)
        # finishedPrice = цена после СПП (что заплатил покупатель)
        # spp = процент скидки СПП
        price = float(item.get("priceWithDisc") or item.get("totalPrice") or 0)
        finished_raw = item.get("finishedPrice")
        price_after_spp = float(finished_raw) if finished_raw else price
        spp_raw = item.get("spp")
        if spp_raw is not None:
            spp_pct_val = float(spp_raw)
        elif price > 0 and price_after_spp < price:
            spp_pct_val = round((1 - price_after_spp / price) * 100, 2)
        else:
            spp_pct_val = 0.0

        is_cancel = item.get("isCancel") or "cancel" in str(item.get("status") or "").lower()
        status = OrderStatus.CANCELLED if is_cancel else OrderStatus.CONFIRMED

        if srid:
            existing = db.query(Order).filter(Order.external_id == srid).first()
            if existing:
                # Обновляем статус и price_after_spp/spp_pct (ранее могли не быть заполнены)
                new_status = OrderStatus.CANCELLED if is_cancel else OrderStatus.CONFIRMED
                changed = False
                if existing.status != new_status:
                    existing.status = new_status
                    changed = True
                if existing.price_after_spp == 0 and price_after_spp > 0:
                    existing.price_after_spp = price_after_spp
                    existing.spp_pct = spp_pct_val
                    changed = True
                if changed:
                    updated += 1
                continue

        nm_id = str(item.get("nmId") or "")
        name = item.get("subject") or item.get("category") or seller_article
        sku = _get_or_create_sku(db, seller_article, nm_id, name)
        _get_or_create_sku_channel(db, sku, channel, nm_id)

        db.add(Order(
            sku_id=sku.id,
            channel_id=channel.id,
            external_id=srid,
            order_date=order_date,
            qty=1,
            price=price,
            price_after_spp=price_after_spp,
            spp_pct=spp_pct_val,
            status=status,
        ))
        new_orders += 1

    db.commit()
    return {"synced_orders": new_orders, "updated_orders": updated, "total_raw": len(raw)}


def sync_sales(db: Session, client: WBClient, days_back: int = 30) -> dict:
    """Синхронизировать продажи и возвраты."""
    date_from = date.today() - timedelta(days=days_back)
    # flag=0: возвращает продажи по дате продажи (поле date = реальная дата)
    raw = client.get_sales(date_from, flag=0)
    channel = _get_wb_channel(db)

    new_sales = 0
    new_returns = 0

    for item in raw:
        seller_article = str(item.get("supplierArticle") or item.get("article") or "")
        if not seller_article:
            continue

        # С flag=0: поле date = фактическая дата продажи/возврата
        sale_date = _parse_date(item.get("date") or item.get("saleDate"))
        if not sale_date:
            continue

        sale_id_raw = str(item.get("saleID") or "")
        is_return = sale_id_raw.startswith("R")

        # Дедупликация по saleID (уникален для каждой продажи/возврата)
        if sale_id_raw:
            existing_sale = db.query(Sale).filter(Sale.external_id == sale_id_raw).first()
            existing_return = db.query(Return).filter(Return.external_id == sale_id_raw).first()
            if existing_sale or existing_return:
                continue

        nm_id = str(item.get("nmId") or "")
        name = item.get("subject") or item.get("category") or seller_article
        sku = _get_or_create_sku(db, seller_article, nm_id, name)
        _get_or_create_sku_channel(db, sku, channel, nm_id)

        # WB начисляет комиссию с priceWithDisc (после скидки продавца, до СПП)
        # forPay = что получает продавец после всех вычетов WB
        price = float(item.get("priceWithDisc") or item.get("finishedPrice") or 0)
        for_pay = float(item.get("forPay") or 0)
        actual_commission = max(0.0, price - for_pay)

        if is_return:
            db.add(Return(
                sku_id=sku.id,
                channel_id=channel.id,
                return_date=sale_date,
                qty=1,
                reason=item.get("subject"),
                external_id=sale_id_raw or None,
            ))
            new_returns += 1
        else:
            db.add(Sale(
                sku_id=sku.id,
                channel_id=channel.id,
                sale_date=sale_date,
                qty=1,
                price=price,
                commission=actual_commission,
                logistics=float(item.get("deliveryRub") or 0),
                storage=0.0,
                external_id=sale_id_raw or None,
            ))
            new_sales += 1

    db.commit()
    return {"synced_sales": new_sales, "synced_returns": new_returns, "total_raw": len(raw)}


def sync_stocks(db: Session, client: WBClient, target_date: Optional[date] = None) -> dict:
    """
    Синхронизировать остатки за target_date (по умолчанию — вчера).
    WB API возвращает одну строку на каждый баркод × склад WB.
    Суммируем все qty по seller_article перед записью в БД.
    INSERT-only: если запись на эту дату уже существует — пропускаем (история не перезаписывается).
    """
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    # dateFrom = 90 дней назад: WB API возвращает только склады с изменениями с dateFrom.
    # При коротком окне (1 день) склады без движения выпадают → занижение остатков.
    date_from = target_date - timedelta(days=90)
    raw = client.get_stocks(date_from)
    channel = _get_wb_channel(db)

    wh_wb = db.query(Warehouse).filter(Warehouse.name == "WB склад").first()
    if not wh_wb:
        wh_wb = Warehouse(name="WB склад", type=WarehouseType.MP)
        db.add(wh_wb)
        db.flush()

    # Суммируем по артикулу: qty=quantity (на складах), in_way_to/from=inWayToClient/inWayFromClient
    # quantityFull = quantity + inWayToClient + inWayFromClient (полный остаток)
    # Нам нужно раздельно: "Всего находится на складах" и "В пути"
    StockAgg = tuple[int, int, int, str]  # (qty_on_wh, in_way_to, in_way_from, nm_id)
    agg: dict[str, StockAgg] = {}
    for item in raw:
        seller_article = str(item.get("supplierArticle") or item.get("article") or "")
        if not seller_article:
            continue
        qty = int(item.get("quantity") or 0)
        in_way_to = int(item.get("inWayToClient") or 0)
        in_way_from = int(item.get("inWayFromClient") or 0)
        nm_id = str(item.get("nmId") or "")
        prev = agg.get(seller_article, (0, 0, 0, nm_id))
        agg[seller_article] = (prev[0] + qty, prev[1] + in_way_to, prev[2] + in_way_from, prev[3] or nm_id)

    inserted = 0
    skipped = 0
    for seller_article, (total_qty, total_to, total_from, nm_id) in agg.items():
        sku = _get_or_create_sku(db, seller_article, nm_id)
        _get_or_create_sku_channel(db, sku, channel, nm_id)

        already = (
            db.query(Stock)
            .filter(Stock.sku_id == sku.id, Stock.warehouse_id == wh_wb.id, Stock.date == target_date)
            .first()
        )
        if already:
            skipped += 1
            continue

        db.add(Stock(
            sku_id=sku.id, warehouse_id=wh_wb.id, date=target_date,
            qty=total_qty, in_way_to_client=total_to, in_way_from_client=total_from,
        ))
        inserted += 1

    db.commit()
    return {"target_date": str(target_date), "inserted": inserted, "skipped_existing": skipped}


def sync_paid_storage(db: Session, client: WBClient, days_back: int = 14) -> dict:
    """
    Синхронизировать платное хранение WB (task-based API).
    warehousePrice — суммарная стоимость хранения одного chrtId (размера) за день на складе.
    Один артикул может иметь несколько chrtId на одном складе → агрегируем по
    (vendorCode, date, warehouse) перед записью в БД.
    """
    date_from = date.today() - timedelta(days=days_back)
    date_to = date.today()

    try:
        raw = client.get_paid_storage(date_from, date_to)
    except WBApiError as e:
        return {"error": str(e), "synced": 0}

    # Агрегируем в памяти: (seller_article, date, warehouse) → {cost, qty, nm_id}
    # чтобы избежать дублей, когда несколько размеров одного артикула на одном складе
    aggregated: dict[tuple, dict] = {}
    for item in raw:
        seller_article = str(item.get("vendorCode") or "")
        if not seller_article:
            continue
        cost_date = _parse_date(str(item.get("date") or ""))
        if not cost_date:
            continue

        warehouse_name = str(item.get("warehouse") or "")
        cost_val = float(item.get("warehousePrice") or 0)
        qty_on_wh = int(item.get("barcodesCount") or 0)
        nm_id = str(item.get("nmId") or "")

        key = (seller_article, cost_date, warehouse_name)
        if key not in aggregated:
            aggregated[key] = {"cost": 0.0, "qty": 0, "nm_id": nm_id}
        aggregated[key]["cost"] += cost_val
        aggregated[key]["qty"] += qty_on_wh

    new_rec = 0
    updated_rec = 0

    # Upsert через PostgreSQL ON CONFLICT
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    for (seller_article, cost_date, warehouse_name), agg in aggregated.items():
        sku = _get_or_create_sku(db, seller_article, agg["nm_id"])

        stmt = pg_insert(StorageCost).values(
            sku_id=sku.id,
            date=cost_date,
            warehouse_name=warehouse_name,
            cost=round(agg["cost"], 4),
            qty_on_warehouse=agg["qty"],
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_storage_cost_sku_date_wh",
            set_={
                "cost": round(agg["cost"], 4),
                "qty_on_warehouse": agg["qty"],
            },
        )
        db.execute(stmt)
        new_rec += 1

    db.commit()

    return {"storage_new": new_rec, "storage_updated": updated_rec, "total_raw": len(raw)}


def sync_financial_report(db: Session, client: WBClient, days_back: int = 30) -> dict:
    """
    Синхронизировать финансовый отчёт WB v5.
    Из него извлекаем только логистику:
      - delivery_rub  → обновляем Sale.logistics по sa_name + sale_dt
    Хранение берётся исключительно из отчёта платного хранения (sync_paid_storage),
    чтобы избежать дублей и некорректных значений из финотчёта.
    """
    date_from = date.today() - timedelta(days=days_back)
    date_to = date.today()

    try:
        raw = client.get_report_detail(date_from, date_to)
    except WBApiError as e:
        return {"error": str(e), "logistics_updated": 0}

    channel = _get_wb_channel(db)

    logistics_updated = 0

    for item in raw:
        seller_article = str(item.get("sa_name") or item.get("supplierArticle") or "")
        if not seller_article:
            continue

        # ── Логистика ──────────────────────────────────────────────
        # Строки с типом операции "Логистика" содержат delivery_rub на единицу.
        # Обновляем Sale.logistics для продаж этого артикула в этот день.
        delivery_rub = float(item.get("delivery_rub") or 0)
        sale_date_str = item.get("sale_dt") or item.get("order_dt") or ""
        sale_date = _parse_date(sale_date_str)

        if delivery_rub > 0 and sale_date:
            sku_obj = db.query(SKU).filter(SKU.seller_article == seller_article).first()
            if sku_obj:
                # Обновляем Sale.logistics = 0 на реальное значение
                sales_to_update = (
                    db.query(Sale)
                    .filter(
                        Sale.sku_id == sku_obj.id,
                        Sale.channel_id == channel.id,
                        Sale.sale_date == sale_date,
                        Sale.logistics == 0,
                    )
                    .all()
                )
                for s in sales_to_update:
                    s.logistics = delivery_rub
                    logistics_updated += 1

    db.commit()
    return {
        "logistics_updated": logistics_updated,
        "total_raw": len(raw),
    }


def sync_ads(db: Session, client: WBClient, days_back: int = 14) -> dict:
    """
    Синхронизировать рекламные кампании и их ежедневную статистику.
    1. Получаем список всех активных / приостановленных кампаний из WB.
    2. Для каждой кампании создаём/обновляем запись AdCampaign (привязка по WB ID в имени).
    3. Загружаем ежедневную статистику за последние days_back дней.
    4. Upsert в AdMetrics.
    """
    channel = _get_wb_channel(db)
    date_from = date.today() - timedelta(days=days_back)
    date_to = date.today()

    # ── 1. Получаем список кампаний ──────────────────────────────────
    # Запрашиваем все статусы чтобы не пропустить архивные кампании
    raw_campaigns = client.get_ad_campaigns(statuses=[7, 9, 11])

    # WB Ad type → наш AdType
    # type 8 = автоматическая, type 9 = аукцион (поиск)  → SEARCH
    # type 11 = поиск+каталог, type 4 = каталог           → RECOMMEND
    def wb_type_to_ad_type(wb_type: int) -> AdType:
        return AdType.SEARCH if wb_type in (8, 9) else AdType.RECOMMEND

    campaign_map: dict[int, AdCampaign] = {}  # wb_advert_id → AdCampaign

    campaigns_synced = 0
    api_returned_empty = not raw_campaigns

    for c in (raw_campaigns or []):
        wb_id = int(c.get("advertId") or 0)
        if not wb_id:
            continue

        # nmId может быть int или list — берём первый
        nm_raw = c.get("nmId") or c.get("nms") or []
        if isinstance(nm_raw, list):
            nm_id = str(nm_raw[0]) if nm_raw else ""
        else:
            nm_id = str(nm_raw)

        # seller_article через nmId → SKUChannel
        sku_obj = None
        if nm_id:
            sc = db.query(SKUChannel).filter(SKUChannel.mp_article == nm_id).first()
            if sc:
                sku_obj = db.query(SKU).filter(SKU.id == sc.sku_id).first()

        # Ищем существующую кампанию по WB_ID в имени
        wb_tag = f"WB_{wb_id}"
        existing = db.query(AdCampaign).filter(
            AdCampaign.name.like(f"{wb_tag}%"),
            AdCampaign.channel_id == channel.id,
        ).first()

        ad_type = wb_type_to_ad_type(int(c.get("type") or 8))
        camp_name = f"{wb_tag} {c.get('name') or ''}"[:300]

        if existing:
            existing.type = ad_type
            existing.is_active = int(c.get("status") or 0) == 7
            if sku_obj and not existing.sku_id:
                existing.sku_id = sku_obj.id
            campaign_map[wb_id] = existing
        else:
            new_camp = AdCampaign(
                sku_id=sku_obj.id if sku_obj else None,
                channel_id=channel.id,
                name=camp_name,
                type=ad_type,
                is_active=int(c.get("status") or 0) == 7,
            )
            db.add(new_camp)
            db.flush()
            campaign_map[wb_id] = new_camp
            campaigns_synced += 1

    db.flush()

    # ── Фолбэк: если API вернул пустой список — используем кампании из БД ─
    # Это бывает когда все кампании приостановлены/архивированы или есть
    # временная проблема с API. В этом случае обновляем только метрики.
    if api_returned_empty:
        existing_camps = db.query(AdCampaign).filter(
            AdCampaign.channel_id == channel.id
        ).all()
        if not existing_camps:
            return {"campaigns_synced": 0, "metrics_upserted": 0, "error": "no campaigns in API or DB"}
        for camp in existing_camps:
            # Извлекаем wb_advert_id из имени "WB_12345678 ..."
            name_parts = camp.name.split("_", 1)
            if len(name_parts) == 2:
                try:
                    wb_id = int(name_parts[1].split(" ")[0])
                    campaign_map[wb_id] = camp
                except (ValueError, IndexError):
                    pass

    # ── 2. Загружаем статистику для ВСЕХ кампаний ────────────────────
    # Синхронизируем все кампании — из ответа извлекаем nmId
    # и автоматически привязываем кампанию к SKU.
    wb_ids = list(campaign_map.keys())
    raw_stats = client.get_ad_fullstats(wb_ids, date_from, date_to)

    metrics_upserted = 0
    skus_linked = 0
    for stat in raw_stats:
        wb_id = int(stat.get("advertId") or 0)
        camp = campaign_map.get(wb_id)
        if not camp:
            continue

        # ── Автолинковка кампании к доминирующему SKU (max показы по nmId) ─
        # Всегда обновляем привязку если нашли dominant nm_id в статистике.
        # Это корректирует неверные привязки для мультиартикульных кампаний.
        nm_views: dict[str, int] = {}
        for day in (stat.get("days") or []):
            for app in (day.get("apps") or []):
                for nm in (app.get("nms") or []):
                    nm_id_raw = nm.get("nmId")
                    views = int(nm.get("views") or 0)
                    if nm_id_raw and views > 0:
                        key = str(nm_id_raw)
                        nm_views[key] = nm_views.get(key, 0) + views
        if nm_views:
            dominant_nm = max(nm_views, key=lambda x: nm_views[x])
            sc = db.query(SKUChannel).filter(SKUChannel.mp_article == dominant_nm).first()
            if sc and camp.sku_id != sc.sku_id:
                camp.sku_id = sc.sku_id
                skus_linked += 1

        for day_stat in (stat.get("days") or []):
            day_date_str = (day_stat.get("date") or "")[:10]
            try:
                day_date = date.fromisoformat(day_date_str)
            except Exception:
                continue

            budget = float(day_stat.get("sum") or 0)
            impressions = int(day_stat.get("views") or 0)
            clicks = int(day_stat.get("clicks") or 0)
            orders = int(day_stat.get("orders") or 0)
            ctr_val = min(float(day_stat.get("ctr") or 0), 9999.9999)  # guard overflow
            cpc_val = float(day_stat.get("cpc") or 0)
            cpm_val = round(budget / impressions * 1000, 2) if impressions else 0.0
            order_cost = float(day_stat.get("sum_price") or 0)

            # Разбивка по зоне размещения из apps (подтверждено на реальных данных):
            # appType=1  → Поиск (веб/десктоп)
            # appType=32 → Поиск (мобильное приложение, iOS/Android)
            # appType=0,64,128,... → Полки (каталог, карточки, главная)
            _SEARCH_APP_TYPES = {1, 32}
            s_budget = s_imp = s_clk = s_ord = 0.0
            r_budget = r_imp = r_clk = r_ord = 0.0
            apps = day_stat.get("apps") or []
            if apps:
                for app in apps:
                    at = int(app.get("appType") or 0)
                    a_b = float(app.get("sum") or 0)
                    a_i = int(app.get("views") or 0)
                    a_c = int(app.get("clicks") or 0)
                    a_o = int(app.get("orders") or 0)
                    if at in _SEARCH_APP_TYPES:
                        s_budget += a_b; s_imp += a_i; s_clk += a_c; s_ord += a_o
                    else:
                        r_budget += a_b; r_imp += a_i; r_clk += a_c; r_ord += a_o
            else:
                # apps пустые — фолбэк по типу кампании
                if camp.type == AdType.SEARCH:
                    s_budget, s_imp, s_clk, s_ord = budget, impressions, clicks, orders
                else:
                    r_budget, r_imp, r_clk, r_ord = budget, impressions, clicks, orders

            existing_m = db.query(AdMetrics).filter(
                AdMetrics.campaign_id == camp.id,
                AdMetrics.date == day_date,
            ).first()

            if existing_m:
                existing_m.budget = budget
                existing_m.impressions = impressions
                existing_m.clicks = clicks
                existing_m.orders = orders
                existing_m.ctr = ctr_val
                existing_m.cpc = cpc_val
                existing_m.cpm = cpm_val
                existing_m.order_cost = order_cost
                existing_m.search_budget = round(s_budget, 2)
                existing_m.search_impressions = int(s_imp)
                existing_m.search_clicks = int(s_clk)
                existing_m.search_orders = int(s_ord)
                existing_m.recommend_budget = round(r_budget, 2)
                existing_m.recommend_impressions = int(r_imp)
                existing_m.recommend_clicks = int(r_clk)
                existing_m.recommend_orders = int(r_ord)
            else:
                db.add(AdMetrics(
                    campaign_id=camp.id,
                    date=day_date,
                    budget=budget,
                    impressions=impressions,
                    clicks=clicks,
                    orders=orders,
                    ctr=ctr_val,
                    cpc=cpc_val,
                    cpm=cpm_val,
                    order_cost=order_cost,
                    search_budget=round(s_budget, 2),
                    search_impressions=int(s_imp),
                    search_clicks=int(s_clk),
                    search_orders=int(s_ord),
                    recommend_budget=round(r_budget, 2),
                    recommend_impressions=int(r_imp),
                    recommend_clicks=int(r_clk),
                    recommend_orders=int(r_ord),
                ))
            metrics_upserted += 1

    # ── 3. Fallback: только для реальных WB-кампаний (name LIKE 'WB_%') без разбивки по apps.
    # Seed/legacy-кампании исключены — у них фейковые impressions которые искажают статистику.
    db.execute(text("""
        UPDATE ad_metrics am
        SET search_budget      = am.budget,
            search_impressions = am.impressions,
            search_clicks      = am.clicks,
            search_orders      = am.orders
        FROM ad_campaigns ac
        WHERE ac.id = am.campaign_id
          AND ac.name LIKE 'WB_%'
          AND ac.type = 'SEARCH'::adtype
          AND am.search_budget = 0
          AND am.recommend_budget = 0
          AND am.budget > 0
    """))
    db.execute(text("""
        UPDATE ad_metrics am
        SET recommend_budget      = am.budget,
            recommend_impressions = am.impressions,
            recommend_clicks      = am.clicks,
            recommend_orders      = am.orders
        FROM ad_campaigns ac
        WHERE ac.id = am.campaign_id
          AND ac.name LIKE 'WB_%'
          AND ac.type = 'RECOMMEND'::adtype
          AND am.search_budget = 0
          AND am.recommend_budget = 0
          AND am.budget > 0
    """))

    db.commit()
    return {
        "campaigns_found": len(raw_campaigns),
        "campaigns_synced": campaigns_synced,
        "skus_linked": skus_linked,
        "metrics_upserted": metrics_upserted,
    }


def sync_logistics_weekly(db: Session, client: WBClient, week_offset: int = 1) -> dict:
    """
    Рассчитать среднюю логистику на единицу из финансового отчёта WB
    за прошлую полную неделю (пн–вс) и сохранить в SKUChannel.logistics_override.

    Алгоритм (соответствует расчёту WB в отчёте «Ср. стоимость логистики»):
      Числитель:   SUM(delivery_rub)  из строк supplier_oper_name = 'Логистика'
      Знаменатель: COUNT строк supplier_oper_name = 'Продажа'  (число отгруженных единиц)
    """
    today = date.today()
    days_since_monday = today.weekday()          # 0=пн, 6=вс
    last_monday = today - timedelta(days=days_since_monday + 7 * week_offset)
    last_sunday = last_monday + timedelta(days=6)

    try:
        raw = client.get_report_detail(last_monday, last_sunday)
    except WBApiError as e:
        return {"error": str(e), "articles_updated": 0}

    logistics_sum: dict[str, float] = {}   # sa_name → суммарная логистика
    sales_count: dict[str, int] = {}        # sa_name → количество продаж

    for item in raw:
        seller_article = str(item.get("sa_name") or "").strip()
        if not seller_article:
            continue
        oper = str(item.get("supplier_oper_name") or "")
        if oper == "Логистика":
            delivery_rub = float(item.get("delivery_rub") or 0)
            if delivery_rub > 0:
                logistics_sum[seller_article] = logistics_sum.get(seller_article, 0.0) + delivery_rub
        elif oper == "Продажа":
            sales_count[seller_article] = sales_count.get(seller_article, 0) + 1

    channel = _get_wb_channel(db)
    articles_updated = 0

    for sa, total in logistics_sum.items():
        n_sales = sales_count.get(sa, 0)
        if not n_sales:
            continue  # нет продаж за неделю — не обновляем
        avg_logistics = round(total / n_sales, 2)
        sku = db.query(SKU).filter(SKU.seller_article == sa).first()
        if not sku:
            continue
        sc = (
            db.query(SKUChannel)
            .filter(SKUChannel.sku_id == sku.id, SKUChannel.channel_id == channel.id)
            .first()
        )
        if sc:
            sc.logistics_override = avg_logistics
            articles_updated += 1

    db.commit()
    return {
        "week_from": str(last_monday),
        "week_to": str(last_sunday),
        "articles_updated": articles_updated,
        "articles_in_report": len(logistics_sum),
        "total_raw": len(raw),
    }


def sync_commission_weekly(db: Session, client: WBClient, week_offset: int = 1) -> dict:
    """
    Рассчитать комиссию + эквайринг (%) из детального финансового отчёта WB
    за прошлую полную неделю (пн–вс) и сохранить в SKUChannel.commission_pct_override.

    Формула: (sum(Реализация) - sum(К_перечислению)) / sum(Реализация) * 100
      Реализация       = retail_price_withdisc_rub  (цена до СПП)
      К_перечислению   = ppvz_for_pay               (что получает продавец)

    Только строки с doc_type_name = 'Продажа'.
    ppvz_sales_commission НЕ используется: он может быть отрицательным,
    когда WB субсидирует скидку («Скидка МП»), что не отражает реальный % комиссии.
    """
    today = date.today()
    days_since_monday = today.weekday()          # 0=пн, 6=вс
    last_monday = today - timedelta(days=days_since_monday + 7 * week_offset)
    last_sunday = last_monday + timedelta(days=6)

    try:
        raw = client.get_report_detail(last_monday, last_sunday)
    except WBApiError as e:
        return {"error": str(e), "articles_updated": 0}

    # Накапливаем Реализацию и К_перечислению по артикулу
    retail_sum: dict[str, float] = {}    # sa_name → sum(retail_price_withdisc_rub)
    for_pay_sum: dict[str, float] = {}   # sa_name → sum(ppvz_for_pay)

    for item in raw:
        # Учитываем только «Продажа» (не возвраты, не хранение, не логистика)
        doc_type = str(item.get("doc_type_name") or "")
        if doc_type != "Продажа":
            continue

        seller_article = str(item.get("sa_name") or "").strip()
        if not seller_article:
            continue

        retail_price = float(item.get("retail_price_withdisc_rub") or 0)
        if retail_price <= 0:
            continue

        ppvz_for_pay = float(item.get("ppvz_for_pay") or 0)

        retail_sum[seller_article] = retail_sum.get(seller_article, 0.0) + retail_price
        for_pay_sum[seller_article] = for_pay_sum.get(seller_article, 0.0) + ppvz_for_pay

    commission_sum = {sa: retail_sum[sa] - for_pay_sum.get(sa, 0.0) for sa in retail_sum}

    channel = _get_wb_channel(db)
    articles_updated = 0
    skipped_no_sku = 0

    for sa, total_commission in commission_sum.items():
        total_retail = retail_sum.get(sa, 0.0)
        if total_retail <= 0:
            continue

        pct = round(total_commission / total_retail * 100, 4)

        sku = db.query(SKU).filter(SKU.seller_article == sa).first()
        if not sku:
            skipped_no_sku += 1
            continue

        sc = (
            db.query(SKUChannel)
            .filter(SKUChannel.sku_id == sku.id, SKUChannel.channel_id == channel.id)
            .first()
        )
        if sc:
            sc.commission_pct_override = pct
            articles_updated += 1

    db.commit()
    return {
        "week_from": str(last_monday),
        "week_to": str(last_sunday),
        "articles_updated": articles_updated,
        "articles_in_report": len(commission_sum),
        "skipped_no_sku": skipped_no_sku,
        "total_raw": len(raw),
    }


def sync_wb_expenses(db: Session, client: WBClient, days_back: int = 14) -> dict:
    """
    Синхронизировать расходы WB из финансового отчёта reportDetailByPeriod
    в SkuDailyExpense (channel_id = WB). Комбинирует weekly + daily отчёты.

    Еженедельный отчёт (period=weekly): точные данные за пн-вс прошлой недели.
    Ежедневный (period=daily): оперативные данные за текущую неделю.
    """
    from collections import defaultdict
    from app.models.sales import SkuDailyExpense
    import logging
    import time as _time

    logger = logging.getLogger(__name__)
    channel = _get_wb_channel(db)
    date_from = date.today() - timedelta(days=days_back)
    date_to = date.today()
    # Расширяем на полную неделю назад для захвата еженедельного отчёта
    fetch_from = date_from - timedelta(days=date_from.weekday())

    def _fetch_all_pages(period: str) -> list:
        """Загрузить все страницы через rrdid-пагинацию с retry."""
        all_rows = []
        rrd_id = 0
        while True:
            page = None
            for attempt in range(3):
                try:
                    page = client.get_report_detail(fetch_from, date_to, rrd_id=rrd_id, period=period)
                    break
                except (WBApiError, Exception) as e:
                    logger.warning("sync_wb_expenses: %s attempt %d error: %s", period, attempt + 1, e)
                    if attempt < 2:
                        _time.sleep(30)
            if page is None:
                logger.error("sync_wb_expenses: %s failed after 3 retries at rrdid=%d", period, rrd_id)
                break
            if not page:
                break
            all_rows.extend(page)
            logger.info("sync_wb_expenses: %s page rrdid=%d got %d rows (total %d)",
                        period, rrd_id, len(page), len(all_rows))
            # Следующая страница — rrd_id из последней записи
            last_rrd = page[-1].get("rrd_id") or 0
            if last_rrd == rrd_id or len(page) < 100000:
                break
            rrd_id = last_rrd
            _time.sleep(61)  # Rate limit: 1 req/min
        return all_rows

    # Получаем еженедельный отчёт (с пагинацией)
    wb_weekly = _fetch_all_pages("weekly")

    # Rate limit между weekly и daily
    _time.sleep(61)

    # Получаем ежедневный отчёт (с пагинацией)
    wb_daily = _fetch_all_pages("daily")

    # Находим последнюю дату покрытую еженедельным отчётом
    weekly_max_date = ""
    for item in wb_weekly:
        dt_to = str(item.get("date_to") or "")[:10]
        if dt_to > weekly_max_date:
            weekly_max_date = dt_to

    logger.info("sync_wb_expenses: weekly covers up to %s (%d rows), daily %d rows",
                weekly_max_date, len(wb_weekly), len(wb_daily))

    if not wb_weekly and not wb_daily:
        return {"upserted": 0, "skipped": 0, "total_raw": 0, "weekly": 0, "daily": 0}

    # --- Стратегия агрегации ---
    # Хранение (storage_fee): ТОЛЬКО из weekly (точные данные за полные недели)
    # Всё остальное: из daily (полнее по удержаниям/рекламе),
    #   а для дат ДО начала daily — из weekly как fallback

    _ZERO = lambda: {
        "sale_amount": 0.0, "commission": 0.0, "logistics": 0.0, "storage": 0.0,
        "penalty": 0.0, "acceptance": 0.0, "other_deductions": 0.0,
        "advertising": 0.0, "other_services": 0.0, "subscription": 0.0, "reviews": 0.0,
        "acquiring": 0.0, "compensation": 0.0, "return_amount": 0.0, "compensation_wb": 0.0,
        "ppvz_for_pay": 0.0, "credit_deduction": 0.0,
        "items_count": 0, "return_count": 0,
    }

    SKIP_OPS = {"Возмещение издержек по перевозке/по складским операциям с товаром",
                "Возмещение за выдачу и возврат товаров на ПВЗ"}

    def _parse_sale_dt(item):
        """Дата транзакции: rr_dt (Дата продажи МСК), fallback на sale_dt."""
        rr_dt = str(item.get("rr_dt") or "")[:10]
        if rr_dt and len(rr_dt) >= 10:
            try:
                return date.fromisoformat(rr_dt)
            except ValueError:
                pass
        sale_dt = str(item.get("sale_dt") or "")[:10]
        if not sale_dt or len(sale_dt) < 10:
            return None
        try:
            return date.fromisoformat(sale_dt)
        except ValueError:
            return None

    def _get_sa(item):
        sa = str(item.get("sa_name") or "").strip()
        return sa if sa else "_WB_ОБЩИЕ"

    # 1) Агрегируем хранение из weekly (приоритетный источник)
    storage_weekly: dict[tuple[str, date], float] = defaultdict(float)
    for item in wb_weekly:
        op = str(item.get("supplier_oper_name") or "")
        if op == "Хранение":
            d = _parse_sale_dt(item)
            if d:
                sa = _get_sa(item)
                storage_weekly[(sa, d)] += abs(float(item.get("storage_fee") or 0))

    # 1b) Агрегируем хранение из daily (fallback для дат без weekly)
    storage_daily: dict[tuple[str, date], float] = defaultdict(float)
    for item in wb_daily:
        op = str(item.get("supplier_oper_name") or "")
        if op == "Хранение":
            d = _parse_sale_dt(item)
            if d:
                sa = _get_sa(item)
                storage_daily[(sa, d)] += abs(float(item.get("storage_fee") or 0))

    # Определяем даты покрытые weekly хранением
    weekly_storage_dates = set(d for (_, d) in storage_weekly.keys())
    logger.info("sync_wb_expenses: weekly storage covers %d dates (up to %s)",
                len(weekly_storage_dates), max(weekly_storage_dates) if weekly_storage_dates else "none")

    # 2) Находим мин. дату daily-отчёта
    daily_min_date = None
    for item in wb_daily:
        d = _parse_sale_dt(item)
        if d and (daily_min_date is None or d < daily_min_date):
            daily_min_date = d

    logger.info("sync_wb_expenses: daily min date: %s", daily_min_date)

    # 3) Агрегируем всё кроме хранения: из daily + weekly fallback (для дат до daily)
    agg: dict[tuple[str, date], dict] = defaultdict(_ZERO)

    def _process_item(item):
        op = str(item.get("supplier_oper_name") or "")
        if op in SKIP_OPS or op == "Хранение":
            return  # хранение уже из weekly
        d = _parse_sale_dt(item)
        if not d:
            return
        sa = _get_sa(item)
        entry = agg[(sa, d)]

        if op == "Продажа":
            entry["compensation"] += float(item.get("retail_amount") or 0)
            entry["sale_amount"] += float(item.get("retail_price_withdisc_rub") or 0)
            entry["acquiring"] += float(item.get("acquiring_fee") or 0)
            entry["ppvz_for_pay"] += abs(float(item.get("ppvz_for_pay") or 0))
            entry["items_count"] += 1
        elif op == "Возврат":
            entry["return_amount"] += abs(float(item.get("retail_price_withdisc_rub") or 0))
            entry["commission"] += abs(float(item.get("retail_amount") or 0))
            entry["ppvz_for_pay"] -= abs(float(item.get("ppvz_for_pay") or 0))
            entry["return_count"] += 1
        elif op in ("Логистика", "Коррекция логистики"):
            entry["logistics"] += abs(float(item.get("delivery_rub") or 0))
        elif "компенсац" in op.lower() or "Добровольная" in op:
            amount = float(item.get("ppvz_for_pay") or 0)
            entry["compensation_wb"] += abs(amount)
        elif op in ("Удержание", "Штрафы", "Штраф"):
            bonus = str(item.get("bonus_type_name") or "").lower()
            deduction = abs(float(item.get("deduction") or 0))
            if not deduction:
                deduction = abs(float(item.get("penalty") or 0))
            if op in ("Штрафы", "Штраф") or "штраф" in bonus:
                entry["penalty"] += deduction
            elif "продвижен" in bonus or "реклам" in bonus:
                entry["advertising"] += deduction
            elif "приемка" in bonus or "приёмка" in bonus:
                entry["acceptance"] += deduction
            elif "заёмщик" in bonus or "займ" in bonus or "кредит" in bonus:
                entry["credit_deduction"] += deduction  # кредит WB — НЕ в ОПиУ, но в ДДС/дебиторке
            elif "джем" in bonus or "подписк" in bonus:
                entry["subscription"] += deduction
            elif "отзыв" in bonus:
                entry["reviews"] += deduction
            else:
                entry["other_deductions"] += deduction

    # Daily — основной источник для всего кроме хранения
    for item in wb_daily:
        _process_item(item)

    # Weekly fallback — только для дат, не покрытых daily
    if daily_min_date:
        for item in wb_weekly:
            d = _parse_sale_dt(item)
            if d and d < daily_min_date:
                _process_item(item)
    else:
        # Нет daily → всё из weekly
        for item in wb_weekly:
            _process_item(item)

    # 4) Вливаем storage: weekly если есть для этой даты, иначе daily
    all_storage_keys = set(storage_weekly.keys()) | set(storage_daily.keys())
    for key in all_storage_keys:
        sa, d = key
        if d in weekly_storage_dates:
            # Есть weekly за эту дату — используем weekly
            agg[(sa, d)]["storage"] = storage_weekly.get(key, 0)
        else:
            # Нет weekly — fallback на daily
            agg[(sa, d)]["storage"] = storage_daily.get(key, 0)

    # UPSERT в SkuDailyExpense
    upserted = 0
    skipped = 0
    for (sa_name, op_date), vals in agg.items():
        sku = db.query(SKU).filter(func.lower(SKU.seller_article) == sa_name.lower()).first()
        if not sku:
            skipped += 1
            continue

        existing = (
            db.query(SkuDailyExpense)
            .filter(
                SkuDailyExpense.sku_id == sku.id,
                SkuDailyExpense.channel_id == channel.id,
                SkuDailyExpense.date == op_date,
            )
            .first()
        )
        if existing:
            for k, v in vals.items():
                setattr(existing, k, v)
        else:
            db.add(SkuDailyExpense(sku_id=sku.id, channel_id=channel.id, date=op_date, **vals))
        upserted += 1

    db.commit()
    logger.info("sync_wb_expenses: upserted=%d skipped=%d weekly=%d daily=%d",
                upserted, skipped, len(wb_weekly), len(wb_daily))
    return {"upserted": upserted, "skipped": skipped, "weekly": len(wb_weekly), "daily": len(wb_daily)}


def sync_prices(db: Session, client: WBClient) -> dict:
    """
    Синхронизировать актуальные цены продавца из WB Prices API.
    discountedPrice = Цена со скидкой продавца = «Цена до СПП» в WB Аналитике.
    Записывает в таблицу prices (upsert по sku+channel+date = сегодня).
    """
    goods = client.get_prices()
    if not goods:
        return {"synced": 0, "total": 0}

    channel = _get_wb_channel(db)
    today = date.today()
    synced = 0

    # Строим маппинг nm_id → sku_id из SKUChannel
    nm_to_sku: dict[str, int] = {}
    sc_rows = (
        db.query(SKUChannel.mp_article, SKUChannel.sku_id)
        .filter(SKUChannel.channel_id == channel.id)
        .all()
    )
    for mp_article, sku_id in sc_rows:
        if mp_article:
            nm_to_sku[str(mp_article)] = sku_id

    for good in goods:
        nm_id = str(good.get("nmID") or "")
        if not nm_id or nm_id not in nm_to_sku:
            continue

        sku_id = nm_to_sku[nm_id]

        # Берём цену из первого доступного размера
        sizes = good.get("sizes") or []
        if not sizes:
            continue
        size = sizes[0]
        price_full = float(size.get("price") or 0)
        price_discounted = float(size.get("discountedPrice") or 0)
        if price_discounted <= 0:
            continue

        # spp_pct оставляем 0 — SPP задаётся WB, не продавцом
        existing = (
            db.query(Price)
            .filter(Price.sku_id == sku_id, Price.channel_id == channel.id, Price.date == today)
            .first()
        )
        if existing:
            existing.price_before_spp = price_discounted
            existing.price_after_spp = price_full  # сохраняем РРЦ для справки
        else:
            db.add(Price(
                sku_id=sku_id,
                channel_id=channel.id,
                price_before_spp=price_discounted,
                price_after_spp=price_full,
                spp_pct=0,
                date=today,
            ))
        synced += 1

    db.commit()
    return {"synced": synced, "total": len(goods)}


def sync_nm_report(db: Session, client: WBClient, days_back: int = 14) -> dict:
    """
    Синхронизировать воронку карточки из WB sales-funnel API (v3):
      - переходы в карточку (openCount → open_card_count)
      - добавления в корзину (cartCount → add_to_cart_count)
      - заказы (orderCount → orders_count)
    Записывает в CardStats (upsert по sku+channel+date).
    Также обновляет SKU.wb_rating из /sales-funnel/products (сводный).

    WB ограничения /history:
      - max 7 дней за запрос
      - max 20 nmIds за запрос
      - rate limit: 3 запроса/20 сек
    Для days_back > 7 делаем несколько запросов с паузами.
    """
    import time as _time

    channel = _get_wb_channel(db)
    date_to = date.today() - timedelta(days=1)   # вчера — последний завершённый день
    date_from = date_to - timedelta(days=days_back - 1)

    # Собираем все nm_id для WB-карточек
    sc_list = (
        db.query(SKUChannel)
        .filter(SKUChannel.channel_id == channel.id, SKUChannel.mp_article.isnot(None))
        .all()
    )
    nm_to_sc: dict[int, SKUChannel] = {}
    for sc in sc_list:
        try:
            nm_to_sc[int(sc.mp_article)] = sc
        except (TypeError, ValueError):
            continue

    if not nm_to_sc:
        return {"error": "нет nm_id для WB SKU", "upserted": 0}

    nm_ids = list(nm_to_sc.keys())

    # ── 1. Ежедневная воронка ──────────────────────────────────────────
    # Разбиваем period на окна по 7 дней (ограничение WB /history)
    new_rec = 0
    updated_rec = 0
    request_count = 0  # счётчик для rate limit (3 req / 20 sec)

    # Строим список 7-дневных окон от date_from до date_to
    windows: list[tuple[date, date]] = []
    win_start = date_from
    while win_start <= date_to:
        win_end = min(win_start + timedelta(days=6), date_to)
        windows.append((win_start, win_end))
        win_start = win_end + timedelta(days=1)

    for win_from, win_to in windows:
        # Батчи по 20 nmId
        for i in range(0, len(nm_ids), 20):
            chunk = nm_ids[i:i + 20]

            # Rate limit: 3 запроса per 20 sec → пауза каждые 3 запроса
            if request_count > 0 and request_count % 3 == 0:
                _time.sleep(22)

            try:
                items = client.get_nm_report(chunk, date_from=win_from, date_to=win_to)
                request_count += 1
            except WBApiError:
                request_count += 1
                continue

            # Новый формат: прямой список [{product: {nmId: N}, history: [...]}]
            for item in items:
                nm_id = int((item.get("product") or {}).get("nmId") or 0)
                sc = nm_to_sc.get(nm_id)
                if not sc:
                    continue

                for day in (item.get("history") or []):
                    dt_str = str(day.get("date") or "")[:10]
                    try:
                        dt = date.fromisoformat(dt_str)
                    except ValueError:
                        continue

                    open_card = int(day.get("openCount") or 0)
                    add_to_cart = int(day.get("cartCount") or 0)
                    orders_cnt = int(day.get("orderCount") or 0)
                    order_sum = float(day.get("orderSum") or 0)
                    # Средняя цена = orderSum / orderCount
                    avg_price_val: float | None = None
                    if orders_cnt > 0 and order_sum > 0:
                        avg_price_val = round(order_sum / orders_cnt, 2)

                    existing = (
                        db.query(CardStats)
                        .filter(
                            CardStats.sku_id == sc.sku_id,
                            CardStats.channel_id == channel.id,
                            CardStats.date == dt,
                        )
                        .first()
                    )
                    if existing:
                        existing.open_card_count = open_card
                        existing.add_to_cart_count = add_to_cart
                        existing.orders_count = orders_cnt
                        if avg_price_val is not None:
                            existing.avg_price_rub = avg_price_val
                        updated_rec += 1
                    else:
                        db.add(CardStats(
                            sku_id=sc.sku_id,
                            channel_id=channel.id,
                            date=dt,
                            open_card_count=open_card,
                            add_to_cart_count=add_to_cart,
                            orders_count=orders_cnt,
                            avg_price_rub=avg_price_val,
                        ))
                        new_rec += 1

    db.commit()

    # ── 2. Рейтинги из /sales-funnel/products (сводный) ───────────────
    ratings_updated = 0
    try:
        # rate limit пауза перед сводным запросом
        if request_count > 0 and request_count % 3 == 0:
            _time.sleep(22)
        products_g = client.get_nm_report_grouped(nm_ids, date_from=date_from, date_to=date_to)
        for prod in products_g:
            product_info = prod.get("product") or {}
            nm_id = int(product_info.get("nmId") or 0)
            sc = nm_to_sc.get(nm_id)
            if not sc:
                continue
            # feedbackRating — оценка по отзывам 0-5 (аналог reviewRating в v2)
            # productRating — внутренний рейтинг WB 1-10 (не используем — не влезает в Numeric(3,2))
            rating_raw = product_info.get("feedbackRating")
            if rating_raw is not None:
                try:
                    rating_val = float(rating_raw)
                    sku_obj = db.query(SKU).filter(SKU.id == sc.sku_id).first()
                    if sku_obj and rating_val > 0:
                        sku_obj.wb_rating = round(rating_val, 2)
                        ratings_updated += 1
                except (TypeError, ValueError):
                    pass
        db.commit()
    except WBApiError:
        pass

    return {
        "date_from": str(date_from),
        "date_to": str(date_to),
        "new": new_rec,
        "updated": updated_rec,
        "ratings_updated": ratings_updated,
    }


def run_full_sync(db: Session, integration: Integration, days_back: int = 30) -> dict:
    """Полная синхронизация: заказы + продажи + остатки."""
    from datetime import datetime
    client = WBClient(integration.api_key, ads_api_key=integration.ads_api_key, prices_api_key=integration.prices_api_key)

    results = {}
    errors = []

    try:
        results["prices"] = sync_prices(db, client)
    except WBApiError as e:
        errors.append(f"prices: {e}")

    try:
        results["orders"] = sync_orders(db, client, days_back)
    except WBApiError as e:
        errors.append(f"orders: {e}")

    try:
        results["sales"] = sync_sales(db, client, days_back)
    except WBApiError as e:
        errors.append(f"sales: {e}")

    try:
        results["stocks"] = sync_stocks(db, client)
    except WBApiError as e:
        errors.append(f"stocks: {e}")

    try:
        results["financial"] = sync_financial_report(db, client, days_back)
    except WBApiError as e:
        errors.append(f"financial: {e}")

    try:
        results["storage"] = sync_paid_storage(db, client, min(days_back, 14))
    except Exception as e:
        errors.append(f"storage: {e}")

    try:
        results["ads"] = sync_ads(db, client, min(days_back, 31))
    except WBApiError as e:
        errors.append(f"ads: {e}")

    try:
        results["nm_report"] = sync_nm_report(db, client, min(days_back, 14))
    except WBApiError as e:
        errors.append(f"nm_report: {e}")

    try:
        results["commission"] = sync_commission_weekly(db, client, week_offset=1)
    except WBApiError as e:
        errors.append(f"commission: {e}")

    integration.last_sync_at = datetime.utcnow()
    if errors:
        integration.last_error = "; ".join(errors)
        from app.models.integration import IntegrationStatus
        integration.status = IntegrationStatus.ERROR
    else:
        integration.last_error = None
        from app.models.integration import IntegrationStatus
        integration.status = IntegrationStatus.ACTIVE

    db.commit()
    return {"results": results, "errors": errors}
