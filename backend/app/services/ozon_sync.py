"""
Синхронизация данных из Ozon Seller API в базу.
"""
from datetime import date, timedelta
from typing import Optional
import logging

from sqlalchemy.orm import Session

from app.models.ads import AdCampaign, AdMetrics, AdType
from app.models.catalog import Channel, ChannelType, SKU, SKUChannel, Warehouse, WarehouseType
from app.models.integration import Integration
from app.models.inventory import Stock
from app.models.sales import Order, OrderStatus, Price, Return, Sale, SkuDailyExpense
from app.services.ozon_api import OzonClient, OzonApiError, OzonPerformanceClient, OzonPerformanceError

logger = logging.getLogger(__name__)

CANCELLED_STATUSES = {
    "cancelled", "cancelled_from_split_pending",
    "awaiting_registration", "not_accepted",
}
DELIVERED_STATUSES = {"delivered", "sent_by_seller"}
SHIPPED_STATUSES   = {"delivering", "driver_pickup", "arbitration", "client_arbitration"}


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def _get_ozon_channel(db: Session) -> Channel:
    ch = db.query(Channel).filter(Channel.type == ChannelType.OZON).first()
    if not ch:
        ch = Channel(name="Ozon", type=ChannelType.OZON, commission_pct=15.0)
        db.add(ch)
        db.flush()
    return ch


def _get_ozon_warehouse(db: Session, name: str) -> Warehouse:
    """Создаёт/находит склад Ozon. Имя всегда prefixed 'Ozon ' для фильтрации по каналу."""
    prefixed = f"Ozon {name}" if not name.startswith("Ozon ") else name
    wh = db.query(Warehouse).filter(Warehouse.name == prefixed).first()
    if not wh:
        # Проверим старое имя (без префикса) — переименуем если нашли
        old = db.query(Warehouse).filter(Warehouse.name == name).first()
        if old:
            old.name = prefixed
            db.flush()
            return old
        wh = Warehouse(name=prefixed, type=WarehouseType.MP)
        db.add(wh)
        db.flush()
    return wh


def _get_or_create_sku(db: Session, offer_id: str, name: str = "") -> SKU:
    sku = db.query(SKU).filter(SKU.seller_article == offer_id).first()
    if not sku:
        sku = SKU(seller_article=offer_id, name=name or offer_id)
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


def _posting_status(status: str) -> OrderStatus:
    s = status.lower()
    if s in CANCELLED_STATUSES:
        return OrderStatus.CANCELLED
    if s in DELIVERED_STATUSES:
        return OrderStatus.DELIVERED
    if s in SHIPPED_STATUSES:
        return OrderStatus.SHIPPED
    return OrderStatus.CONFIRMED


def _collect_all_postings(fetch_fn, date_from: date, date_to: date, limit: int = 1000) -> list[dict]:
    """Пагинация: собирает все постинги через offset."""
    all_items = []
    offset = 0
    while True:
        batch = fetch_fn(date_from, date_to, offset=offset, limit=limit)
        if not batch:
            break
        all_items.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return all_items


# ──────────────────────────────────────────────
# Sync orders (FBO + FBS)
# ──────────────────────────────────────────────

def sync_orders(db: Session, client: OzonClient, days_back: int = 30) -> dict:
    """Синхронизировать заказы Ozon за последние N дней (FBO + FBS)."""
    date_from = date.today() - timedelta(days=days_back)
    date_to   = date.today()
    channel   = _get_ozon_channel(db)

    fbo_postings = _collect_all_postings(client.get_fbo_postings, date_from, date_to)
    fbs_postings = _collect_all_postings(client.get_fbs_postings, date_from, date_to)
    all_postings = fbo_postings + fbs_postings

    new_orders = 0
    updated    = 0

    for posting in all_postings:
        posting_number = posting.get("posting_number", "")
        raw_status     = posting.get("status", "")
        order_date     = _parse_date(posting.get("created_at") or posting.get("in_process_at"))
        if not order_date:
            continue

        status = _posting_status(raw_status)

        for item in posting.get("products", []):
            offer_id = str(item.get("offer_id") or "").strip()
            if not offer_id:
                continue

            name     = item.get("name", offer_id)
            qty      = int(item.get("quantity") or 1)
            price    = float(item.get("price") or 0)
            ext_id   = f"{posting_number}_{offer_id}"

            sku    = _get_or_create_sku(db, offer_id, name)
            sc     = _get_or_create_sku_channel(db, sku, channel, str(item.get("sku") or offer_id))

            existing = db.query(Order).filter(Order.external_id == ext_id).first()
            if existing:
                if existing.status != status:
                    existing.status = status
                    updated += 1
            else:
                db.add(Order(
                    sku_id=sku.id,
                    channel_id=channel.id,
                    external_id=ext_id,
                    order_date=order_date,
                    qty=qty,
                    price=price,
                    price_after_spp=price,  # у Ozon нет СПП, цена = конечная
                    spp_pct=0.0,
                    status=status,
                ))
                new_orders += 1

    db.commit()
    logger.info("ozon sync_orders: новых=%d обновлено=%d из %d постингов",
                new_orders, updated, len(all_postings))
    return {"new_orders": new_orders, "updated": updated, "total_postings": len(all_postings)}


# ──────────────────────────────────────────────
# Sync sales (delivered postings → Sale records)
# ──────────────────────────────────────────────

def sync_sales(db: Session, client: OzonClient, days_back: int = 30) -> dict:
    """
    Для доставленных постингов создаём/обновляем Sale-записи.
    Все расходы МП берутся из финансовых транзакций v3/finance/transaction/list:
      - commission:       sale_commission
      - logistics:        доставка, возврат, last-mile, drop-off
      - storage:          хранение FBO/FBS
      - penalty:          штрафы
      - acceptance:       платная приёмка
      - other_deductions: прочие удержания (всё остальное)
      - compensation:     компенсации (accruals_for_sale)
    """
    date_from = date.today() - timedelta(days=days_back)
    date_to   = date.today()
    channel   = _get_ozon_channel(db)

    tx_date_from = date.today() - timedelta(days=90)
    tx_date_to   = date.today()

    # ── Классификация сервисов Ozon ──────────────────────────────────────
    LOGISTICS_SERVICES = {
        "MarketplaceServiceItemDirectFlowLogistic",
        "MarketplaceServiceItemReturnFlowLogistic",
        "MarketplaceServiceItemRedistributionLastMileCourier",
        "MarketplaceServiceItemRedistributionLastMilePVZ",
        "MarketplaceServiceItemRedistributionReturnsPVZ",
        "MarketplaceServiceItemPackageMaterialsProvision",
        "MarketplaceServiceItemPackageRedistribution",
        "MarketplaceServiceItemRedistributionDropOffApvz",
        "MarketplaceServiceItemDropoffPVZ",
        "MarketplaceServiceItemTemporaryStorageRedistribution",
        "MarketplaceServiceItemReturnAfterDelivToCustomer",
        "MarketplaceServiceItemReturnNotDelivToCustomer",
        "MarketplaceServiceItemReturnPartGoodsCustomer",
        "MarketplaceServiceItemDelivToCustomer",
    }

    STORAGE_SERVICES = {
        "MarketplaceServiceItemStorageFBO",
        "MarketplaceServiceItemStorageFBS",
        "MarketplaceServiceItemStorageExpressFBO",
    }

    PENALTY_SERVICES = {
        "MarketplaceServiceItemFine",
        "MarketplaceServiceItemFineReturn",
        "MarketplaceServiceItemFineNonReturn",
    }

    ACCEPTANCE_SERVICES = {
        "MarketplaceServiceItemReceptionPaid",
        "MarketplaceServiceItemReceptionProcessing",
        "MarketplaceServiceItemReceptionAtWarehouse",
    }

    KNOWN_SERVICES = LOGISTICS_SERVICES | STORAGE_SERVICES | PENALTY_SERVICES | ACCEPTANCE_SERVICES

    # ── Загрузка транзакций (чанки по месяцам) ───────────────────────────
    import calendar as _cal
    tx_ops: list[dict] = []
    chunk_from = tx_date_from
    while chunk_from <= tx_date_to:
        last_day = _cal.monthrange(chunk_from.year, chunk_from.month)[1]
        chunk_to = min(date(chunk_from.year, chunk_from.month, last_day), tx_date_to)
        try:
            for page in range(1, 51):
                tx_data = client.get_transactions(chunk_from, chunk_to, page=page, page_size=1000)
                ops = tx_data.get("result", {}).get("operations", [])
                if not ops:
                    break
                tx_ops.extend(ops)
                page_count = tx_data.get("result", {}).get("page_count", 1)
                if page >= page_count:
                    break
        except OzonApiError as e:
            logger.warning("ozon sync_sales: транзакции %s–%s недоступны — %s", chunk_from, chunk_to, e)
        if chunk_from.month == 12:
            chunk_from = date(chunk_from.year + 1, 1, 1)
        else:
            chunk_from = date(chunk_from.year, chunk_from.month + 1, 1)

    # ── Агрегация по posting_number ──────────────────────────────────────
    _ZERO = {"commission": 0.0, "logistics": 0.0, "storage": 0.0,
             "penalty": 0.0, "acceptance": 0.0, "other_deductions": 0.0,
             "compensation": 0.0}

    tx_map: dict[str, dict] = {}
    for op in tx_ops:
        pn = op.get("posting", {}).get("posting_number") or ""
        if not pn:
            continue
        entry = tx_map.setdefault(pn, dict(_ZERO))

        # Комиссия — отдельное поле
        sale_commission = abs(float(op.get("sale_commission") or 0))
        if sale_commission > 0:
            entry["commission"] += sale_commission

        # Компенсация (положительное начисление за доставку)
        accruals = float(op.get("accruals_for_sale") or 0)
        if accruals > 0:
            entry["compensation"] += accruals

        # Разбор services
        for s in op.get("services", []):
            sname = s.get("name", "")
            amt   = abs(float(s.get("price") or 0))
            if amt == 0:
                continue

            if sname in LOGISTICS_SERVICES:
                entry["logistics"] += amt
            elif sname in STORAGE_SERVICES:
                entry["storage"] += amt
            elif sname in PENALTY_SERVICES:
                entry["penalty"] += amt
            elif sname in ACCEPTANCE_SERVICES:
                entry["acceptance"] += amt
            else:
                entry["other_deductions"] += amt

    # ── Создание / обновление Sale ───────────────────────────────────────
    fbo = _collect_all_postings(client.get_fbo_postings, date_from, date_to)
    fbs = _collect_all_postings(client.get_fbs_postings, date_from, date_to)

    new_sales = 0
    updated   = 0
    for posting in fbo + fbs:
        raw_status = posting.get("status", "")
        if _posting_status(raw_status) not in (OrderStatus.DELIVERED, OrderStatus.SHIPPED):
            continue

        posting_number = posting.get("posting_number", "")
        sale_date      = _parse_date(posting.get("shipment_date") or posting.get("created_at"))
        if not sale_date:
            continue

        fin     = tx_map.get(posting_number, _ZERO)
        n_items = max(sum(i.get("quantity", 1) for i in posting.get("products", [])), 1)

        for item in posting.get("products", []):
            offer_id = str(item.get("offer_id") or "").strip()
            if not offer_id:
                continue

            qty   = int(item.get("quantity") or 1)
            price = float(item.get("price") or 0)
            share = qty / n_items

            commission       = fin["commission"]       * share
            logistics        = fin["logistics"]        * share
            storage          = fin["storage"]          * share
            penalty          = fin["penalty"]          * share
            acceptance       = fin["acceptance"]       * share
            other_deductions = fin["other_deductions"] * share
            compensation     = fin["compensation"]     * share

            ext_id   = f"sale_{posting_number}_{offer_id}"
            existing = db.query(Sale).filter(Sale.external_id == ext_id).first()

            if existing:
                needs_update = (
                    (existing.commission == 0 and commission > 0) or
                    (existing.logistics == 0 and logistics > 0) or
                    (existing.storage == 0 and storage > 0) or
                    (existing.penalty == 0 and penalty > 0) or
                    (existing.acceptance == 0 and acceptance > 0) or
                    (existing.other_deductions == 0 and other_deductions > 0) or
                    (existing.compensation == 0 and compensation > 0)
                )
                if needs_update:
                    if commission > 0:       existing.commission = commission
                    if logistics > 0:        existing.logistics = logistics
                    if storage > 0:          existing.storage = storage
                    if penalty > 0:          existing.penalty = penalty
                    if acceptance > 0:       existing.acceptance = acceptance
                    if other_deductions > 0: existing.other_deductions = other_deductions
                    if compensation > 0:     existing.compensation = compensation
                    updated += 1
                continue

            sku = db.query(SKU).filter(SKU.seller_article == offer_id).first()
            if not sku:
                continue

            order = db.query(Order).filter(
                Order.sku_id == sku.id,
                Order.channel_id == channel.id,
                Order.external_id == f"{posting_number}_{offer_id}",
            ).first()

            db.add(Sale(
                order_id=order.id if order else None,
                sku_id=sku.id,
                channel_id=channel.id,
                external_id=ext_id,
                sale_date=sale_date,
                qty=qty,
                price=price * qty,
                commission=commission,
                logistics=logistics,
                storage=storage,
                penalty=penalty,
                acceptance=acceptance,
                other_deductions=other_deductions,
                compensation=compensation,
            ))
            new_sales += 1

    db.commit()
    logger.info("ozon sync_sales: новых=%d обновлено=%d из %d транзакций",
                new_sales, updated, len(tx_ops))
    return {"new_sales": new_sales, "updated": updated, "tx_count": len(tx_ops)}


# ──────────────────────────────────────────────
# Sync expenses (transaction-date based)
# ──────────────────────────────────────────────

def sync_expenses(db: Session, client: OzonClient, days_back: int = 90) -> dict:
    """
    Агрегация расходов МП из Ozon-транзакций по SKU + operation_date.
    Данные записываются в SkuDailyExpense — 1-в-1 с отчётами Ozon,
    потому что группируются по дате транзакции, а не по дате отгрузки.
    UPSERT: полностью перезаписывает записи за период.
    """
    import calendar as _cal
    from collections import defaultdict

    channel = _get_ozon_channel(db)

    tx_date_from = date.today() - timedelta(days=days_back)
    tx_date_to   = date.today()

    # ── Классификация сервисов ───────────────────────────────────────────
    LOGISTICS_SERVICES = {
        "MarketplaceServiceItemDirectFlowLogistic",
        "MarketplaceServiceItemReturnFlowLogistic",
        "MarketplaceServiceItemRedistributionLastMileCourier",
        "MarketplaceServiceItemRedistributionLastMilePVZ",
        "MarketplaceServiceItemRedistributionReturnsPVZ",
        "MarketplaceServiceItemPackageMaterialsProvision",
        "MarketplaceServiceItemPackageRedistribution",
        "MarketplaceServiceItemRedistributionDropOffApvz",
        "MarketplaceServiceItemDropoffPVZ",
        "MarketplaceServiceItemTemporaryStorageRedistribution",
        "MarketplaceServiceItemReturnAfterDelivToCustomer",
        "MarketplaceServiceItemReturnNotDelivToCustomer",
        "MarketplaceServiceItemReturnPartGoodsCustomer",
        "MarketplaceServiceItemDelivToCustomer",
    }
    STORAGE_SERVICES = {
        "MarketplaceServiceItemStorageFBO",
        "MarketplaceServiceItemStorageFBS",
        "MarketplaceServiceItemStorageExpressFBO",
    }
    PENALTY_SERVICES = {
        "MarketplaceServiceItemFine",
        "MarketplaceServiceItemFineReturn",
        "MarketplaceServiceItemFineNonReturn",
    }
    ACCEPTANCE_SERVICES = {
        "MarketplaceServiceItemReceptionPaid",
        "MarketplaceServiceItemReceptionProcessing",
        "MarketplaceServiceItemReceptionAtWarehouse",
    }

    # ── Загрузка транзакций ──────────────────────────────────────────────
    tx_ops: list[dict] = []
    chunk_from = tx_date_from
    while chunk_from <= tx_date_to:
        last_day = _cal.monthrange(chunk_from.year, chunk_from.month)[1]
        chunk_to = min(date(chunk_from.year, chunk_from.month, last_day), tx_date_to)
        try:
            for page in range(1, 51):
                tx_data = client.get_transactions(chunk_from, chunk_to, page=page, page_size=1000)
                ops = tx_data.get("result", {}).get("operations", [])
                if not ops:
                    break
                tx_ops.extend(ops)
                page_count = tx_data.get("result", {}).get("page_count", 1)
                if page >= page_count:
                    break
        except OzonApiError as e:
            logger.warning("sync_expenses: транзакции %s–%s: %s", chunk_from, chunk_to, e)
        if chunk_from.month == 12:
            chunk_from = date(chunk_from.year + 1, 1, 1)
        else:
            chunk_from = date(chunk_from.year, chunk_from.month + 1, 1)

    # ── Операции по типам (маппинг как в отчёте Ozon) ─────────────────
    # «Доставка покупателю» / «отмена начисления» → sale: commission = sale_commission,
    #   logistics = services, sale_amount = accruals_for_sale
    # «Получение возврата...» → return: return commission (отрицательная),
    #   return_amount = abs(accruals_for_sale)
    # «Доставка и обработка возврата...» → logistics только (amount = services)
    # «Оплата эквайринга» → other_deductions (acquiring)
    # «Оплата за клик» → рекламный расход (amount, не через services)
    # «Звёздные товары» → other_deductions
    # «Обеспечение/Упаковка» → logistics
    # «Кросс-докинг» → other_deductions (amount)
    # «Декомпенсация» → return_amount (amount)

    SALE_OPS = {"Доставка покупателю", "Доставка покупателю — отмена начисления"}
    RETURN_OPS = {"Получение возврата, отмены, невыкупа от покупателя"}
    LOGISTICS_OPS = {
        "Доставка и обработка возврата, отмены, невыкупа",
        "Обеспечение материалами для упаковки товара",
        "Упаковка товара партнёрами",
        "Временное размещение товара партнерами",
    }

    # ── Агрегация: (offer_id, operation_date) → суммы ────────────────────
    ACQUIRING_SERVICES = {
        "MarketplaceRedistributionOfAcquiringOperation",
    }

    _ZERO = lambda: {
        "sale_amount": 0.0, "commission": 0.0, "logistics": 0.0, "storage": 0.0,
        "penalty": 0.0, "acceptance": 0.0, "other_deductions": 0.0, "acquiring": 0.0,
        "compensation": 0.0, "return_amount": 0.0, "items_count": 0,
    }

    agg: dict[tuple[str, date], dict] = defaultdict(_ZERO)

    for op in tx_ops:
        op_date_str = op.get("operation_date") or ""
        op_date = _parse_date(op_date_str)
        if not op_date:
            continue

        op_type_name = str(op.get("operation_type_name") or "")
        amount = float(op.get("amount") or 0)

        # Товары — Ozon использует числовой sku (mp_article)
        items = op.get("items", []) or op.get("posting", {}).get("products", []) or []
        offer_ids = []
        for itm in items:
            oid = str(itm.get("sku") or itm.get("offer_id") or "").strip()
            if oid:
                offer_ids.append(oid)

        if not offer_ids:
            continue

        share = 1.0 / len(offer_ids)

        for offer_id in offer_ids:
            key = (offer_id, op_date)
            entry = agg[key]

            if op_type_name in SALE_OPS:
                entry["items_count"] += 1  # считаем только продажи
                # ── Продажа: комиссия + логистика из services + выплата ────
                commission = abs(float(op.get("sale_commission") or 0))
                entry["commission"] += commission * share

                accruals = float(op.get("accruals_for_sale") or 0)
                entry["sale_amount"] += accruals * share

                # Компенсация = accruals_for_sale (что выплатили продавцу)
                if accruals > 0:
                    entry["compensation"] += accruals * share

                for s in op.get("services", []):
                    sname = s.get("name", "")
                    amt = abs(float(s.get("price") or 0))
                    if amt == 0:
                        continue
                    if sname in LOGISTICS_SERVICES:
                        entry["logistics"] += amt * share
                    elif sname in STORAGE_SERVICES:
                        entry["storage"] += amt * share
                    elif sname in PENALTY_SERVICES:
                        entry["penalty"] += amt * share
                    elif sname in ACCEPTANCE_SERVICES:
                        entry["acceptance"] += amt * share
                    else:
                        entry["other_deductions"] += amt * share

            elif op_type_name in RETURN_OPS:
                # ── Возврат: забираем комиссию обратно, считаем сумму возврата ──
                accruals = float(op.get("accruals_for_sale") or 0)
                if accruals < 0:
                    entry["return_amount"] += abs(accruals) * share
                # Комиссия возврата — вычитаем (Ozon возвращает комиссию)
                ret_commission = abs(float(op.get("sale_commission") or 0))
                entry["commission"] -= ret_commission * share

            elif op_type_name in LOGISTICS_OPS:
                # ── Логистические операции (возвраты, упаковка) ────
                for s in op.get("services", []):
                    amt = abs(float(s.get("price") or 0))
                    if amt > 0:
                        entry["logistics"] += amt * share

            elif op_type_name == "Оплата за клик":
                # ── Реклама → НЕ сюда (идёт в AdMetrics) ────
                pass

            else:
                # ── Всё остальное → other_deductions или acquiring ────
                if amount < 0:
                    # Проверяем services на эквайринг
                    acq_amt = 0.0
                    other_amt = 0.0
                    for s in op.get("services", []):
                        sname = s.get("name") or ""
                        sprice = abs(float(s.get("price") or 0))
                        if sname in ACQUIRING_SERVICES or "acquiring" in sname.lower():
                            acq_amt += sprice
                        elif sprice > 0:
                            other_amt += sprice
                    if acq_amt > 0:
                        entry["acquiring"] += acq_amt * share
                        entry["other_deductions"] += other_amt * share
                    else:
                        # Нет детализации services — всё в other
                        entry["other_deductions"] += abs(amount) * share

    # ── UPSERT в SkuDailyExpense ─────────────────────────────────────────
    # Кеш: mp_article → sku_id для Ozon
    mp_to_sku: dict[str, int] = {}
    for sc in db.query(SKUChannel).filter(SKUChannel.channel_id == channel.id).all():
        if sc.mp_article:
            mp_to_sku[sc.mp_article] = sc.sku_id

    upserted = 0
    skipped  = 0

    for (offer_id, op_date), vals in agg.items():
        # offer_id может быть mp_article (числовой Ozon SKU) или seller_article
        sku_id = mp_to_sku.get(offer_id)
        if not sku_id:
            sku = db.query(SKU).filter(SKU.seller_article == offer_id).first()
            sku_id = sku.id if sku else None
        if not sku_id:
            skipped += 1
            continue

        existing = (
            db.query(SkuDailyExpense)
            .filter(
                SkuDailyExpense.sku_id == sku_id,
                SkuDailyExpense.channel_id == channel.id,
                SkuDailyExpense.date == op_date,
            )
            .first()
        )

        if existing:
            existing.sale_amount = vals["sale_amount"]
            existing.commission = vals["commission"]
            existing.logistics = vals["logistics"]
            existing.storage = vals["storage"]
            existing.penalty = vals["penalty"]
            existing.acceptance = vals["acceptance"]
            existing.other_deductions = vals["other_deductions"]
            existing.acquiring = vals["acquiring"]
            existing.compensation = vals["compensation"]
            existing.return_amount = vals["return_amount"]
            existing.items_count = vals["items_count"]
        else:
            db.add(SkuDailyExpense(
                sku_id=sku_id,
                channel_id=channel.id,
                date=op_date,
                **vals,
            ))
        upserted += 1

    db.commit()
    logger.info("sync_expenses: upserted=%d skipped=%d from %d tx", upserted, skipped, len(tx_ops))
    return {"upserted": upserted, "skipped": skipped, "tx_count": len(tx_ops)}


# ──────────────────────────────────────────────
# Sync returns
# ──────────────────────────────────────────────

def sync_returns(db: Session, client: OzonClient, days_back: int = 30) -> dict:
    """Синхронизировать возвраты Ozon через /v1/returns/list."""
    channel = _get_ozon_channel(db)

    # Получаем все возвраты с пагинацией через has_next
    all_returns = []
    offset = 0
    MAX_PAGES = 20  # не более 10 000 возвратов за раз
    for _ in range(MAX_PAGES):
        try:
            batch, has_next = client.get_returns(offset=offset, limit=500)
        except OzonApiError as e:
            logger.warning("ozon sync_returns: %s", e)
            break
        if not batch:
            break
        all_returns.extend(batch)
        if not has_next:
            break
        offset += 500

    new_returns = 0
    for r in all_returns:
        product     = r.get("product") or {}
        offer_id    = str(product.get("offer_id") or "").strip()
        ext_id      = str(r.get("id") or "")
        logistic    = r.get("logistic") or {}
        return_date = _parse_date(
            logistic.get("return_date") or logistic.get("final_moment") or ""
        )
        if not offer_id or not return_date:
            continue

        ext_key = f"oz_ret_{ext_id}_{offer_id}"
        if db.query(Return).filter(Return.external_id == ext_key).first():
            continue

        sku = db.query(SKU).filter(SKU.seller_article == offer_id).first()
        if not sku:
            continue

        db.add(Return(
            sku_id=sku.id,
            channel_id=channel.id,
            external_id=ext_key,
            return_date=return_date,
            qty=int(product.get("quantity") or 1),
            reason=str(r.get("return_reason_name") or "")[:300],
        ))
        new_returns += 1

    db.commit()
    logger.info("ozon sync_returns: новых=%d", new_returns)
    return {"new_returns": new_returns}


# ──────────────────────────────────────────────
# Sync stocks
# ──────────────────────────────────────────────

def sync_stocks(db: Session, client: OzonClient, target_date: Optional[date] = None) -> dict:
    """
    Синхронизировать остатки на складах Ozon.
    INSERT-only: если запись на эту дату уже существует — пропускаем.
    По умолчанию записывает за вчера (наиболее ранний момент начала дня).
    """
    if target_date is None:
        target_date = date.today() - timedelta(days=1)
    channel = _get_ozon_channel(db)

    rows = []
    offset = 0
    while True:
        batch = client.get_stocks(limit=1000, offset=offset)
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000

    inserted = 0
    skipped  = 0

    from collections import defaultdict
    agg: dict[tuple, dict] = defaultdict(lambda: {"qty": 0, "in_way": 0, "from_client": 0, "wh_name": ""})
    for row in rows:
        offer_id = str(row.get("item_code") or "").strip()
        wh_name  = str(row.get("warehouse_name") or "Ozon склад").strip()
        if not offer_id:
            continue
        key = (offer_id, wh_name)
        agg[key]["qty"]         += int(row.get("free_to_sell_amount") or 0)
        agg[key]["in_way"]      += int(row.get("promised_amount") or 0)
        agg[key]["from_client"] += int(row.get("reserved_amount") or 0)
        agg[key]["wh_name"]      = wh_name

    for (offer_id, wh_name), vals in agg.items():
        sku = db.query(SKU).filter(SKU.seller_article == offer_id).first()
        if not sku:
            continue

        wh = _get_ozon_warehouse(db, wh_name)

        already = db.query(Stock).filter(
            Stock.sku_id == sku.id,
            Stock.warehouse_id == wh.id,
            Stock.date == target_date,
        ).first()

        if already:
            skipped += 1
            continue

        db.add(Stock(
            sku_id=sku.id,
            warehouse_id=wh.id,
            date=target_date,
            qty=vals["qty"],
            in_way_to_client=vals["in_way"],
            in_way_from_client=vals["from_client"],
        ))
        inserted += 1

    db.commit()
    logger.info("ozon sync_stocks: inserted=%d skipped=%d (date=%s)", inserted, skipped, target_date)
    return {"inserted": inserted, "skipped": skipped, "total_rows": len(rows)}


# ──────────────────────────────────────────────
# Sync prices
# ──────────────────────────────────────────────

def sync_prices(db: Session, client: OzonClient) -> dict:
    """Синхронизировать актуальные цены Ozon."""
    today   = date.today()
    channel = _get_ozon_channel(db)

    # v5: пагинация через cursor (передаётся в last_id следующего запроса)
    all_items = []
    cursor    = ""
    while True:
        data = client.get_prices(limit=1000, last_id=cursor)
        items = data.get("items", [])
        if not items:
            break
        all_items.extend(items)
        cursor = data.get("cursor", "")
        if not cursor or len(items) < 1000:
            break

    updated = 0
    for item in all_items:
        offer_id = str(item.get("offer_id") or "").strip()
        if not offer_id:
            continue

        sku = db.query(SKU).filter(SKU.seller_article == offer_id).first()
        if not sku:
            continue

        prices_info = item.get("price", {})
        # marketing_seller_price — цена продажи ДО соинвеста Ozon
        # price — финальная цена покупателя (после соинвеста)
        # old_price — зачёркнутая цена (не используем как базовую)
        seller_price = float(prices_info.get("marketing_seller_price") or prices_info.get("price") or 0)
        final_price  = float(prices_info.get("price") or seller_price)
        coinvest_pct = round((1 - final_price / seller_price) * 100, 2) if seller_price > 0 and final_price < seller_price else 0.0

        existing = db.query(Price).filter(
            Price.sku_id == sku.id,
            Price.channel_id == channel.id,
            Price.date == today,
        ).first()

        if existing:
            existing.price_before_spp = seller_price
            existing.price_after_spp  = final_price
            existing.spp_pct          = coinvest_pct
        else:
            db.add(Price(
                sku_id=sku.id,
                channel_id=channel.id,
                date=today,
                price_before_spp=seller_price,
                price_after_spp=final_price,
                spp_pct=coinvest_pct,
            ))
        updated += 1

    db.commit()
    logger.info("ozon sync_prices: updated=%d", updated)
    return {"prices_updated": updated}


# ──────────────────────────────────────────────
# Sync photos
# ──────────────────────────────────────────────

def sync_photos(db: Session, client: OzonClient) -> dict:
    """
    Загрузить фото товаров Ozon.
    1. /v2/product/list → получаем product_id по offer_id
    2. /v2/product/info → для каждого получаем primary_image
    Обновляет SKUChannel.photo_url только для записей, где фото ещё нет.
    """
    channel = _get_ozon_channel(db)

    # Собираем offer_id всех SKUChannel без фото для Ozon
    scs_without_photo = (
        db.query(SKUChannel)
        .filter(
            SKUChannel.channel_id == channel.id,
            (SKUChannel.photo_url == None) | (SKUChannel.photo_url == ""),  # noqa: E711
        )
        .all()
    )
    if not scs_without_photo:
        return {"photos_updated": 0}

    # Маппинг offer_id → SKUChannel
    sc_by_offer: dict[str, SKUChannel] = {}
    for sc in scs_without_photo:
        sku = db.query(SKU).filter(SKU.id == sc.sku_id).first()
        if sku and sku.seller_article:
            sc_by_offer[sku.seller_article] = sc

    if not sc_by_offer:
        return {"photos_updated": 0}

    # /v4/product/info/attributes → primary_image (батч до 100 штук)
    offer_ids = list(sc_by_offer.keys())
    photos_updated = 0

    for i in range(0, len(offer_ids), 100):
        chunk = offer_ids[i:i + 100]
        try:
            last_id = ""
            while True:
                data = client.get_product_attributes(chunk, limit=100, last_id=last_id)
                items = data.get("result", [])
                for item in items:
                    offer_id = str(item.get("offer_id") or "").strip()
                    photo_url = str(item.get("primary_image") or "").strip()
                    if not photo_url:
                        images = item.get("images") or []
                        if images:
                            photo_url = str(images[0]).strip()
                    if offer_id in sc_by_offer and photo_url:
                        sc_by_offer[offer_id].photo_url = photo_url
                        photos_updated += 1
                last_id = data.get("last_id", "")
                if not last_id or len(items) < 100:
                    break
        except OzonApiError as e:
            logger.warning("sync_photos: attributes chunk %d error — %s", i, e)

    db.commit()
    logger.info("ozon sync_photos: updated=%d of %d", photos_updated, len(sc_by_offer))
    return {"photos_updated": photos_updated}


# ──────────────────────────────────────────────
# Sync ads (Performance API)
# ──────────────────────────────────────────────

def _resolve_campaign_sku(
    perf_client: OzonPerformanceClient,
    campaign_id: str,
    mp_to_sku: dict[str, int],
) -> int | None:
    """Один запрос products → sku_id. Если не нашёл — None."""
    try:
        prod_data = perf_client.get_campaign_products(campaign_id, page_size=10)
        for p in (prod_data.get("products") or []):
            ozon_sku = str(p.get("sku") or "")
            if ozon_sku in mp_to_sku:
                return mp_to_sku[ozon_sku]
    except OzonPerformanceError:
        pass
    return None

def sync_ads(db: Session, perf_client: OzonPerformanceClient, days_back: int = 1) -> dict:
    """
    Синхронизировать рекламные кампании и дневную статистику из Ozon Performance API.
    1. Получаем список кампаний → создаём/обновляем AdCampaign.
    2. Получаем дневную статистику → upsert AdMetrics.
    Тип кампании: PLACEMENT_TOP_PROMOTION → SEARCH, остальные → RECOMMEND.
    days_back=1 для ежедневного sync (только вчера),
    days_back=3 для catchup после простоя.
    """
    channel = _get_ozon_channel(db)
    date_from = date.today() - timedelta(days=days_back)
    date_to = date.today()

    # Кеш: Ozon mp_article (числовой SKU) → sku_id нашей БД
    mp_to_sku: dict[str, int] = {
        sc.mp_article: sc.sku_id
        for sc in db.query(SKUChannel).filter(SKUChannel.channel_id == channel.id).all()
        if sc.mp_article
    }

    # ── 1. Список кампаний ─────────────────────────────────────────────
    campaigns_info: dict[str, dict] = {}
    try:
        page = 1
        while True:
            data = perf_client.get_campaigns(page=page, page_size=100)
            camp_list = data.get("list", []) or []
            for c in camp_list:
                cid = str(c.get("id") or "")
                if cid:
                    campaigns_info[cid] = c
            total = int(data.get("total") or 0)
            if page * 100 >= total or not camp_list:
                break
            page += 1
    except OzonPerformanceError as e:
        logger.warning("sync_ads: не удалось получить список кампаний — %s", e)

    # ── 2. Upsert AdCampaign + привязка к SKU ─────────────────────────
    campaign_map: dict[str, AdCampaign] = {}
    campaigns_synced = 0

    for cid, c in campaigns_info.items():
        placement = c.get("placement", "")
        ad_type = AdType.SEARCH if placement == "PLACEMENT_TOP_PROMOTION" else AdType.RECOMMEND

        tag = f"OZ_{cid}"
        name = f"{tag} {c.get('title') or ''}"[:300]
        state = c.get("state", "")
        is_active = state in ("CAMPAIGN_STATE_RUNNING", "CAMPAIGN_STATE_PLANNED")

        existing = db.query(AdCampaign).filter(
            AdCampaign.name.like(f"{tag}%"),
            AdCampaign.channel_id == channel.id,
        ).first()

        if existing:
            existing.type = ad_type
            existing.is_active = is_active
            # Привязка к SKU: запрашиваем products только если sku_id ещё не установлен
            if not existing.sku_id:
                sku_id = _resolve_campaign_sku(perf_client, cid, mp_to_sku)
                if sku_id:
                    existing.sku_id = sku_id
            campaign_map[cid] = existing
        else:
            sku_id = _resolve_campaign_sku(perf_client, cid, mp_to_sku)
            camp = AdCampaign(
                sku_id=sku_id,
                channel_id=channel.id,
                name=name,
                type=ad_type,
                is_active=is_active,
            )
            db.add(camp)
            db.flush()
            campaign_map[cid] = camp
            campaigns_synced += 1

    db.flush()

    # ── 3. Дневная статистика ──────────────────────────────────────────
    try:
        rows = perf_client.get_daily_stats(date_from, date_to)
    except OzonPerformanceError as e:
        logger.error("sync_ads: get_daily_stats error: %s", e)
        db.commit()
        return {"campaigns_synced": campaigns_synced, "metrics_upserted": 0, "error": str(e)}

    metrics_upserted = 0
    for row in rows:
        cid = str(
            row.get("campaignId")
            or row.get("campaign_id")
            or row.get("id")
            or ""
        )
        if not cid:
            continue

        # Найти или создать кампанию (на случай, если не было в списке)
        camp = campaign_map.get(cid)
        if not camp:
            tag = f"OZ_{cid}"
            camp = db.query(AdCampaign).filter(
                AdCampaign.name.like(f"{tag}%"),
                AdCampaign.channel_id == channel.id,
            ).first()
            if not camp:
                title = str(row.get("title") or row.get("campaignName") or cid)
                camp = AdCampaign(
                    sku_id=_resolve_campaign_sku(perf_client, cid, mp_to_sku),
                    channel_id=channel.id,
                    name=f"{tag} {title}"[:300],
                    type=AdType.SEARCH,
                    is_active=False,
                )
                db.add(camp)
                db.flush()
                campaigns_synced += 1
            campaign_map[cid] = camp

        date_str = (row.get("date") or "")[:10]
        try:
            stat_date = date.fromisoformat(date_str)
        except Exception:
            continue

        def _f(v) -> float:
            """Парсит число: поддерживает как '2418.83', так и '2418,83'."""
            if v is None:
                return 0.0
            return float(str(v).replace(",", "."))

        budget      = _f(row.get("moneySpent") or row.get("spend") or row.get("sum"))
        impressions = int(row.get("views") or row.get("impressions") or 0)
        clicks      = int(row.get("clicks") or 0)
        orders      = int(row.get("orders") or 0)
        order_cost  = _f(row.get("ordersMoney") or row.get("revenue") or row.get("sum_price"))

        ctr_val = round(clicks / impressions * 100, 4) if impressions else 0.0
        cpc_val = round(budget / clicks, 2) if clicks else 0.0
        cpm_val = round(budget / impressions * 1000, 2) if impressions else 0.0

        if camp.type == AdType.SEARCH:
            s_budget, s_imp, s_clk, s_ord = budget, impressions, clicks, orders
            r_budget, r_imp, r_clk, r_ord = 0.0, 0, 0, 0
        else:
            s_budget, s_imp, s_clk, s_ord = 0.0, 0, 0, 0
            r_budget, r_imp, r_clk, r_ord = budget, impressions, clicks, orders

        existing_m = db.query(AdMetrics).filter(
            AdMetrics.campaign_id == camp.id,
            AdMetrics.date == stat_date,
        ).first()

        if existing_m:
            existing_m.budget              = budget
            existing_m.impressions         = impressions
            existing_m.clicks              = clicks
            existing_m.orders              = orders
            existing_m.ctr                 = ctr_val
            existing_m.cpc                 = cpc_val
            existing_m.cpm                 = cpm_val
            existing_m.order_cost          = order_cost
            existing_m.search_budget       = round(s_budget, 2)
            existing_m.search_impressions  = int(s_imp)
            existing_m.search_clicks       = int(s_clk)
            existing_m.search_orders       = int(s_ord)
            existing_m.recommend_budget    = round(r_budget, 2)
            existing_m.recommend_impressions = int(r_imp)
            existing_m.recommend_clicks    = int(r_clk)
            existing_m.recommend_orders    = int(r_ord)
        else:
            db.add(AdMetrics(
                campaign_id=camp.id,
                date=stat_date,
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

    db.commit()
    logger.info(
        "ozon sync_ads: campaigns=%d metrics=%d rows=%d",
        campaigns_synced, metrics_upserted, len(rows),
    )
    return {"campaigns_synced": campaigns_synced, "metrics_upserted": metrics_upserted, "rows": len(rows)}


# ──────────────────────────────────────────────
# Full sync
# ──────────────────────────────────────────────

def run_full_sync(db: Session, integration: Integration, days_back: int = 30) -> dict:
    """Запустить полную синхронизацию данных Ozon."""
    from datetime import datetime as dt
    from app.models.integration import IntegrationStatus

    client = OzonClient(
        api_key=integration.api_key,
        client_id=integration.client_id or "",
    )

    results = {}
    errors  = []

    try:
        results["orders"] = sync_orders(db, client, days_back)
    except OzonApiError as e:
        errors.append(f"orders: {e}")
        logger.error("ozon run_full_sync orders error: %s", e)

    try:
        results["sales"] = sync_sales(db, client, days_back)
    except OzonApiError as e:
        errors.append(f"sales: {e}")
        logger.error("ozon run_full_sync sales error: %s", e)

    try:
        results["returns"] = sync_returns(db, client, days_back)
    except OzonApiError as e:
        errors.append(f"returns: {e}")
        logger.error("ozon run_full_sync returns error: %s", e)

    try:
        results["stocks"] = sync_stocks(db, client)
    except OzonApiError as e:
        errors.append(f"stocks: {e}")
        logger.error("ozon run_full_sync stocks error: %s", e)

    try:
        results["prices"] = sync_prices(db, client)
    except OzonApiError as e:
        errors.append(f"prices: {e}")
        logger.error("ozon run_full_sync prices error: %s", e)

    try:
        results["photos"] = sync_photos(db, client)
    except OzonApiError as e:
        errors.append(f"photos: {e}")
        logger.error("ozon run_full_sync photos error: %s", e)

    try:
        results["expenses"] = sync_expenses(db, client, days_back=min(days_back, 90))
    except OzonApiError as e:
        errors.append(f"expenses: {e}")
        logger.error("ozon run_full_sync expenses error: %s", e)

    # Реклама (Performance API) — только если есть credentials
    perf_client_id = getattr(integration, "perf_client_id", None)
    perf_secret    = integration.ads_api_key  # ads_api_key хранит client_secret для Performance API
    if perf_client_id and perf_secret:
        try:
            perf = OzonPerformanceClient(perf_client_id, perf_secret)
            results["ads"] = sync_ads(db, perf, days_back=min(days_back, 3))
        except OzonPerformanceError as e:
            errors.append(f"ads: {e}")
            logger.error("ozon run_full_sync ads error: %s", e)

    # Обновляем интеграцию
    integration.last_sync_at = dt.utcnow()
    if errors:
        integration.status      = IntegrationStatus.ERROR
        integration.last_error  = "; ".join(errors)
    else:
        integration.status      = IntegrationStatus.ACTIVE
        integration.last_error  = None
    db.commit()

    return {"results": results, "errors": errors}
