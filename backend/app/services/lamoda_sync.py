"""
Синхронизация данных Lamoda в базу.

Коммерческие условия FBO 2025:
  Комиссия:          37%
  Обработка платежей: 1.4%
  Итого удержаний:   38.4% от цены
  Возврат покупателем: 29 руб./ед.
"""
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from app.models.catalog import Channel, ChannelType, SKU, SKUChannel, Warehouse, WarehouseType
from app.models.inventory import Stock
from app.models.sales import Order, OrderStatus, Price, Return, Sale
from app.services.lamoda_api import LamodaApiError, LamodaClient

LAMODA_COMMISSION_PCT = 40.3   # ~37% комиссия + ~3% эквайринг (по отчёту комиссионера февраль 2026)
LAMODA_RETURN_FEE_RUB = 29.0  # фиксированный сбор за обработку возврата, ₽/ед.


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_lamoda_channel(db: Session) -> Channel:
    ch = db.query(Channel).filter(Channel.type == ChannelType.LAMODA).first()
    if not ch:
        ch = Channel(name="Lamoda", type=ChannelType.LAMODA, commission_pct=LAMODA_COMMISSION_PCT)
        db.add(ch)
        db.flush()
    return ch


def _get_lamoda_warehouse(db: Session) -> Warehouse:
    """Единый склад Lamoda FBO — используется для записи остатков."""
    wh = db.query(Warehouse).filter(Warehouse.name == "Lamoda FBO").first()
    if not wh:
        wh = Warehouse(name="Lamoda FBO", type=WarehouseType.MP, is_active=True)
        db.add(wh)
        db.flush()
    return wh


def _get_or_create_sku(db: Session, vendor_code: str, name: str = "") -> SKU:
    sku = db.query(SKU).filter(SKU.seller_article == vendor_code).first()
    if not sku:
        sku = SKU(seller_article=vendor_code, name=name or vendor_code)
        db.add(sku)
        db.flush()
    return sku


def _get_or_create_sku_channel(db: Session, sku: SKU, channel: Channel) -> SKUChannel:
    sc = (
        db.query(SKUChannel)
        .filter(SKUChannel.sku_id == sku.id, SKUChannel.channel_id == channel.id)
        .first()
    )
    if not sc:
        sc = SKUChannel(sku_id=sku.id, channel_id=channel.id)
        db.add(sc)
        db.flush()
    return sc


def _parse_dt(s: Optional[str]) -> Optional[date]:
    """Разобрать ISO-дату/дату-время Lamoda в date."""
    if not s:
        return None
    try:
        # "2025-01-15T10:00:00+03:00"  или  "2025-01-15T10:00:00Z"  или  "2025-01-15"
        cleaned = s.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned).date()
    except Exception:
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            return None


def _map_status(status_raw: str) -> OrderStatus:
    s = status_raw.lower()
    if s in ("cancelled", "canceled", "rejected", "cancel"):
        return OrderStatus.CANCELLED
    if s in ("returned", "return", "refunded"):
        return OrderStatus.RETURNED
    if s in ("delivered", "complete", "completed", "closed"):
        return OrderStatus.DELIVERED
    if s in ("shipped", "delivery", "in_delivery"):
        return OrderStatus.SHIPPED
    return OrderStatus.CONFIRMED


# ─── Синк заказов ─────────────────────────────────────────────────────────────

def sync_lamoda_orders(db: Session, client: LamodaClient, days_back: int = 30) -> dict:
    """
    Синхронизировать заказы/продажи/возвраты Lamoda за последние N дней.

    Стратегия:
    - Получаем список заказов с фильтром updatedAt >= N дней назад.
    - Для каждого заказа пробуем взять items из embedded-поля.
    - Если items нет, запрашиваем детальную страницу заказа.
    - Каждая позиция заказа → Order (+ Sale если DELIVERED, + Return если RETURNED).
    """
    channel = _get_lamoda_channel(db)
    updated_from = datetime.now() - timedelta(days=days_back)
    raw_orders = client.get_orders(updated_from=updated_from)

    new_count = 0
    updated_count = 0
    error_count = 0

    for order in raw_orders:
        # Lamoda использует 2 идентификатора:
        #   id      = "RU260315-490197-005C"  (позиция, нужен для detail URL)
        #   orderNr = "RU260315-490197"        (номер заказа)
        order_id = str(order.get("id") or "")          # для detail endpoint
        order_nr = str(order.get("orderNr") or order.get("id") or "")
        if not order_nr:
            continue

        status_raw = str(order.get("status") or order.get("statusText") or "confirmed")
        status = _map_status(status_raw)
        created_at = _parse_dt(order.get("createdAt") or order.get("created_at")) or date.today()

        # Items в списке отсутствуют — запрашиваем детали по id позиции
        items = (
            order.get("items")
            or order.get("_embedded", {}).get("items", [])
            or []
        )
        if not items and order_id:
            try:
                detail = client.get_order_detail(order_id)
                items = (
                    detail.get("items")
                    or detail.get("_embedded", {}).get("items", [])
                    or []
                )
                # Обновляем статус из детальной записи (может быть точнее)
                if detail.get("status"):
                    status = _map_status(str(detail["status"]))
            except LamodaApiError as e:
                error_count += 1
                continue

        for item in items:
            # Lamoda: поле "sku" = vendor code продавца (seller_article)
            vendor_code = str(
                item.get("sku")
                or item.get("vendorCode")
                or item.get("vendor_code")
                or item.get("articleNumber")
                or ""
            )
            if not vendor_code:
                continue

            # Ищем только среди существующих SKU — не создаём неизвестные артикулы
            sku = db.query(SKU).filter(SKU.seller_article == vendor_code).first()
            if not sku:
                continue

            _get_or_create_sku_channel(db, sku, channel)

            # partnerAgreedPrice = цена поставщика (до вычета комиссии), приоритетный источник
            price = float(
                item.get("partnerAgreedPrice")
                or item.get("salePrice")
                or item.get("paidPrice")
                or item.get("price")
                or item.get("sellingPrice")
                or 0
            )
            qty = int(item.get("qty") or item.get("quantity") or 1)

            # Уникальный ID позиции: item["id"] или "orderId_sku"
            item_id = str(
                item.get("id")
                or item.get("itemId")
                or f"{order_id}_{vendor_code}"
            )

            # Upsert заказа
            existing = db.query(Order).filter(Order.external_id == item_id).first()
            if existing:
                existing.status = status
                updated_count += 1
            else:
                order_rec = Order(
                    sku_id=sku.id,
                    channel_id=channel.id,
                    external_id=item_id,
                    order_date=created_at,
                    qty=qty,
                    price=price,
                    price_after_spp=price,  # у Lamoda нет СПП
                    spp_pct=0,
                    status=status,
                )
                db.add(order_rec)
                db.flush()
                new_count += 1
                existing = order_rec

            # Создаём Sale если статус DELIVERED
            if status == OrderStatus.DELIVERED:
                delivered_at = (
                    _parse_dt(order.get("deliveredAt") or order.get("updatedAt"))
                    or created_at
                )
                sale_ext = f"lm_sale_{item_id}"
                if not db.query(Sale).filter(Sale.external_id == sale_ext).first():
                    commission = round(price * LAMODA_COMMISSION_PCT / 100, 2)
                    db.add(Sale(
                        order_id=existing.id,
                        sku_id=sku.id,
                        channel_id=channel.id,
                        external_id=sale_ext,
                        sale_date=delivered_at,
                        qty=qty,
                        price=price,
                        commission=commission,
                        logistics=0,   # Lamoda FBO: логистика включена
                        storage=0,
                    ))

            # Создаём Return если статус RETURNED
            if status == OrderStatus.RETURNED:
                returned_at = (
                    _parse_dt(order.get("returnedAt") or order.get("updatedAt"))
                    or created_at
                )
                ret_ext = f"lm_ret_{item_id}"
                if not db.query(Return).filter(Return.external_id == ret_ext).first():
                    db.add(Return(
                        sku_id=sku.id,
                        channel_id=channel.id,
                        external_id=ret_ext,
                        return_date=returned_at,
                        qty=qty,
                        reason=status_raw,
                    ))

    db.commit()
    return {
        "new_orders": new_count,
        "updated_orders": updated_count,
        "errors": error_count,
    }


# ─── Синк остатков ────────────────────────────────────────────────────────────

def sync_lamoda_stock(db: Session, client: LamodaClient, target_date: Optional[date] = None) -> dict:
    """
    Синхронизировать остатки FBO Lamoda.
    INSERT-only: если запись на эту дату уже существует — пропускаем.
    По умолчанию записывает за вчера (наиболее ранний снимок начала дня).
    """
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    channel = _get_lamoda_channel(db)
    warehouse = _get_lamoda_warehouse(db)

    raw = client.get_stock()
    inserted = 0
    skipped  = 0

    for item in raw:
        vendor_code = str(
            item.get("vendorCode")
            or item.get("vendor_code")
            or item.get("sku")
            or ""
        )
        if not vendor_code:
            continue

        sku = db.query(SKU).filter(SKU.seller_article == vendor_code).first()
        if not sku:
            continue

        _get_or_create_sku_channel(db, sku, channel)

        qty = int(item.get("quantity") or item.get("available") or 0)
        reserved = int(item.get("reserved") or 0)

        already = (
            db.query(Stock)
            .filter(
                Stock.sku_id == sku.id,
                Stock.warehouse_id == warehouse.id,
                Stock.date == target_date,
            )
            .first()
        )
        if already:
            skipped += 1
            continue

        db.add(Stock(
            sku_id=sku.id,
            warehouse_id=warehouse.id,
            date=target_date,
            qty=qty,
            in_way_to_client=reserved,
            in_way_from_client=0,
        ))
        inserted += 1

    db.commit()
    return {"inserted": inserted, "skipped": skipped, "date": target_date.isoformat()}


# ─── Синк номенклатур (цены + фото) ──────────────────────────────────────────

def sync_lamoda_nomenclatures(db: Session, client: LamodaClient) -> dict:
    """
    Синхронизировать номенклатуры: цены и URL первого фото.
    Обновляет Price на сегодня + SKUChannel.photo_url.
    """
    channel = _get_lamoda_channel(db)
    today = date.today()
    raw = client.get_nomenclatures()

    updated = 0
    for item in raw:
        # JSON-RPC возвращает список номенклатур напрямую или обёрнутых в {"nomenclature": {...}}
        nomenclature = item.get("nomenclature") or item
        vendor_code = str(
            nomenclature.get("vendorCode")
            or nomenclature.get("vendor_code")
            or nomenclature.get("articleNumber")
            or ""
        )
        if not vendor_code:
            continue

        sku = db.query(SKU).filter(SKU.seller_article == vendor_code).first()
        if not sku:
            continue

        sc = _get_or_create_sku_channel(db, sku, channel)

        # Обновляем имя SKU из номенклатуры, если оно ещё не задано
        nomen_name = str(nomenclature.get("name") or nomenclature.get("title") or "")
        if nomen_name and sku.name == vendor_code:
            sku.name = nomen_name

        # Фото: первый элемент из массива photos
        photos = nomenclature.get("photos") or nomenclature.get("images") or []
        if photos:
            photo = photos[0]
            if isinstance(photo, dict):
                photo_url = str(
                    photo.get("url")
                    or photo.get("photoUrl")
                    or photo.get("src")
                    or ""
                )
            else:
                photo_url = str(photo)
            if photo_url:
                sc.photo_url = photo_url

        # Цена
        price_val = None
        prices = nomenclature.get("prices") or []
        if prices:
            p = prices[0]
            if isinstance(p, dict):
                price_val = float(
                    p.get("price")
                    or p.get("value")
                    or p.get("amount")
                    or 0
                )
        if not price_val:
            raw_price = nomenclature.get("price") or nomenclature.get("sellingPrice")
            if raw_price:
                price_val = float(raw_price)

        if price_val and price_val > 0:
            existing_price = (
                db.query(Price)
                .filter(
                    Price.sku_id == sku.id,
                    Price.channel_id == channel.id,
                    Price.date == today,
                )
                .first()
            )
            if existing_price:
                existing_price.price_before_spp = price_val
                existing_price.price_after_spp = price_val
            else:
                db.add(Price(
                    sku_id=sku.id,
                    channel_id=channel.id,
                    date=today,
                    price_before_spp=price_val,
                    price_after_spp=price_val,
                    spp_pct=0,
                ))

        updated += 1

    db.commit()
    return {"updated": updated, "date": today.isoformat()}


# ─── Full sync ────────────────────────────────────────────────────────────────

def run_lamoda_full_sync(
    db: Session,
    client: LamodaClient,
    days_back: int = 30,
) -> dict:
    """Полный синк Lamoda: заказы + сток + номенклатуры.
    Номенклатуры (Seller API) опциональны — могут быть недоступны из-за сетевых ограничений.
    """
    import logging
    log = logging.getLogger(__name__)

    r_orders = sync_lamoda_orders(db, client, days_back=days_back)
    r_stock = sync_lamoda_stock(db, client)

    # Seller Partner API (lk.lamoda.ru) может быть недоступен из Docker/облака
    try:
        r_nomen = sync_lamoda_nomenclatures(db, client)
    except Exception as e:
        log.warning("lamoda_sync: nomenclatures skipped — %s", e)
        r_nomen = {"updated": 0, "error": str(e)}

    return {
        "orders": r_orders,
        "stock": r_stock,
        "nomenclatures": r_nomen,
    }
