"""
Сервис сверки поставок МойСклад ↔ WB/Ozon.
Реплицирует логику из Google Apps Script.
"""
import logging
import time
from datetime import date
from typing import Optional

from app.services.moysklad_api import MoySkladClient
from app.services.wb_api import WBClient
from app.services.ozon_api import OzonClient

logger = logging.getLogger(__name__)

EXCLUDED_OZON_SUPPLY_STATES = {"CANCELLED"}


def _normalize_text(value: str) -> str:
    """Нормализация текста для сравнения: lowercase, strip, убрать кавычки."""
    return (
        str(value or "")
        .replace("'", "")
        .replace('"', "")
        .replace("«", "")
        .replace("»", "")
        .strip()
        .lower()
    )


def _normalize_qty(value) -> float:
    """Нормализация количества."""
    try:
        return round(float(str(value or 0).replace(",", ".")), 3)
    except (ValueError, TypeError):
        return 0.0


# ─── МойСклад: загрузка данных ─────────────────────────────

def _fetch_moysklad_positions(
    ms_client: MoySkladClient,
    date_from: date,
    date_to: date,
    agent_name: str,
    organization: Optional[str] = None,
) -> tuple[list[dict], dict]:
    """
    Загрузить заказы и позиции из МойСклад.
    Returns: (ms_rows, ms_map)
        ms_rows: [{order_number, date, article, name, qty, agent, status, store}]
        ms_map: {normalized_key -> {orderNumber, product, quantity, agent, organization}}
    """
    orders = ms_client.get_customer_orders(date_from, date_to, agent_name, organization)

    ms_rows = []
    ms_map = {}

    for i, order in enumerate(orders):
        meta = MoySkladClient.extract_order_meta(order)
        try:
            positions = ms_client.get_order_positions(order["id"])
        except Exception as e:
            logger.warning("Ошибка позиций заказа %s: %s", order.get("id"), e)
            continue

        for pos in positions:
            pos_data = MoySkladClient.extract_position_data(pos)
            ms_rows.append({
                "order_number": meta["order_number"],
                "date": meta["date"],
                "article": pos_data["article"],
                "name": pos_data["name"],
                "quantity": pos_data["quantity"],
                "price": pos_data["price"],
                "uom": pos_data["uom"],
                "agent": meta["agent"],
                "status": meta["status"],
                "organization": meta["organization"],
                "store": meta["store"],
            })

            # Ключ для матчинга: orderNumber || article
            key = _normalize_text(meta["order_number"]) + "||" + _normalize_text(pos_data["article"])
            if key not in ms_map:
                ms_map[key] = {
                    "orderNumber": meta["order_number"],
                    "product": pos_data["article"],
                    "quantity": 0,
                    "agent": meta["agent"],
                    "organization": meta["organization"],
                }
            ms_map[key]["quantity"] = _normalize_qty(ms_map[key]["quantity"] + pos_data["quantity"])

        if i < len(orders) - 1:
            time.sleep(0.25)

    logger.info("MoySklad: %d orders, %d positions, %d unique keys", len(orders), len(ms_rows), len(ms_map))
    return ms_rows, ms_map


# ─── WB: загрузка поставок ──────────────────────────────────

def _fetch_wb_supplies(
    wb_client: WBClient,
    date_from: date,
    date_to: date,
) -> tuple[list[dict], dict]:
    """
    Загрузить поставки и товары из WB.
    Returns: (wb_rows, wb_map)
    """
    supplies = wb_client.get_supplies(date_from, date_to)
    logger.info("WB: found %d supplies", len(supplies))

    wb_rows = []
    wb_map = {}

    for supply in supplies:
        supply_id = str(
            supply.get("id") or supply.get("supplyId") or
            supply.get("supplyID") or supply.get("supply_id") or ""
        ).strip()
        if not supply_id:
            continue

        details = wb_client.get_supply_details(supply_id)
        time.sleep(0.15)
        goods = wb_client.get_supply_goods(supply_id)
        time.sleep(0.2)

        for item in goods:
            vendor_code = str(
                item.get("vendorCode") or item.get("vendor_code") or
                item.get("supplierArticle") or item.get("article") or ""
            ).strip()
            qty = _normalize_qty(item.get("quantity", 0))

            wb_rows.append({
                "supply_id": supply_id,
                "vendor_code": vendor_code,
                "quantity": qty,
                "barcode": str(item.get("barcode", "")),
                "warehouse": details.get("warehouseName", ""),
                "create_date": details.get("createDate", ""),
                "supply_date": details.get("supplyDate", ""),
                "fact_date": details.get("factDate", ""),
            })

            key = _normalize_text(supply_id) + "||" + _normalize_text(vendor_code)
            if key not in wb_map:
                wb_map[key] = {"supplyId": supply_id, "vendorCode": vendor_code, "quantity": 0}
            wb_map[key]["quantity"] = _normalize_qty(wb_map[key]["quantity"] + qty)

    logger.info("WB: %d goods rows, %d unique keys", len(wb_rows), len(wb_map))
    return wb_rows, wb_map


# ─── Ozon: загрузка поставок ────────────────────────────────

def _fetch_ozon_supplies(
    ozon_client: OzonClient,
    date_from: date,
    date_to: date,
) -> tuple[list[dict], dict]:
    """
    Загрузить поставки и товары из Ozon.
    Returns: (oz_rows, oz_map)
    """
    order_ids = ozon_client.get_supply_order_ids()
    if not order_ids:
        return [], {}

    orders = ozon_client.get_supply_order_details(order_ids)

    # Фильтр по дате
    filtered = []
    for order in orders:
        created = str(order.get("created_date") or "")[:10]
        if not created:
            continue
        try:
            created_date = date.fromisoformat(created)
        except ValueError:
            continue
        if date_from <= created_date <= date_to:
            filtered.append(order)

    logger.info("Ozon: %d orders total, %d in date range", len(orders), len(filtered))

    oz_rows = []
    oz_map = {}

    for order in filtered:
        supplies = order.get("supplies") or []
        for supply in supplies:
            bundle_id = supply.get("bundle_id")
            if not bundle_id:
                continue

            supply_state = str(supply.get("state") or "").upper()
            if supply_state in EXCLUDED_OZON_SUPPLY_STATES:
                continue

            supply_id = str(supply.get("supply_id") or "")
            dropoff_wh = (order.get("drop_off_warehouse") or {}).get("warehouse_id", "")
            storage_wh = (supply.get("storage_warehouse") or {}).get("warehouse_id", "")
            storage_name = (supply.get("storage_warehouse") or {}).get("name", "")

            items = ozon_client.get_supply_bundle_items(bundle_id, dropoff_wh, storage_wh)

            for item in items:
                product_id = item.get("product_id")
                offer_id = ""
                if product_id:
                    time.sleep(0.25)
                    offer_id = ozon_client.get_product_offer_id(int(product_id))

                qty = _normalize_qty(item.get("quantity", 0))
                oz_rows.append({
                    "created_date": order.get("created_date", ""),
                    "state_updated_date": order.get("state_updated_date", ""),
                    "order_number": order.get("order_number", ""),
                    "state": order.get("state", ""),
                    "supply_id": supply_id,
                    "supply_state": supply_state,
                    "offer_id": offer_id,
                    "name": item.get("name", ""),
                    "quantity": qty,
                    "barcode": str(item.get("barcode", "")),
                    "storage_warehouse": storage_name,
                })

                if supply_id and offer_id:
                    key = _normalize_text(supply_id) + "||" + _normalize_text(offer_id)
                    if key not in oz_map:
                        oz_map[key] = {"supplyId": supply_id, "offerId": offer_id, "quantity": 0}
                    oz_map[key]["quantity"] = _normalize_qty(oz_map[key]["quantity"] + qty)

            time.sleep(0.35)

    logger.info("Ozon: %d supply rows, %d unique keys", len(oz_rows), len(oz_map))
    return oz_rows, oz_map


# ─── Матчинг ────────────────────────────────────────────────

def _reconcile_maps(ms_map: dict, mp_map: dict, channel: str) -> list[dict]:
    """
    Сверить два мэпа и вернуть список расхождений.
    """
    all_keys = set(ms_map.keys()) | set(mp_map.keys())
    discrepancies = []

    for key in sorted(all_keys):
        ms = ms_map.get(key)
        mp = mp_map.get(key)

        if ms and not mp:
            discrepancies.append({
                "status": "only_moysklad",
                "source": "МойСклад",
                "ms_order": ms["orderNumber"],
                "mp_supply": "",
                "ms_article": ms["product"],
                "mp_article": "",
                "ms_qty": ms["quantity"],
                "mp_qty": None,
                "agent": ms.get("agent", ""),
                "organization": ms.get("organization", ""),
                "comment": "Позиция есть в МойСклад, но отсутствует в " + channel.upper(),
            })
        elif not ms and mp:
            mp_id = mp.get("supplyId", "")
            mp_art = mp.get("vendorCode") or mp.get("offerId") or ""
            discrepancies.append({
                "status": f"only_{channel}",
                "source": channel.upper(),
                "ms_order": "",
                "mp_supply": mp_id,
                "ms_article": "",
                "mp_article": mp_art,
                "ms_qty": None,
                "mp_qty": mp["quantity"],
                "agent": "",
                "organization": "",
                "comment": f"Позиция есть в {channel.upper()}, но отсутствует в МойСклад",
            })
        elif ms and mp:
            if _normalize_qty(ms["quantity"]) != _normalize_qty(mp["quantity"]):
                mp_id = mp.get("supplyId", "")
                mp_art = mp.get("vendorCode") or mp.get("offerId") or ""
                discrepancies.append({
                    "status": "qty_mismatch",
                    "source": "Обе системы",
                    "ms_order": ms["orderNumber"],
                    "mp_supply": mp_id,
                    "ms_article": ms["product"],
                    "mp_article": mp_art,
                    "ms_qty": ms["quantity"],
                    "mp_qty": mp["quantity"],
                    "agent": ms.get("agent", ""),
                    "organization": ms.get("organization", ""),
                    "comment": "Совпали заказ/поставка и товар, но отличается количество",
                })

    return discrepancies


# ─── Основной API ────────────────────────────────────────────

def reconcile_supplies(
    ms_client: MoySkladClient,
    mp_client,
    channel: str,
    date_from: date,
    date_to: date,
    agent_name: str,
    organization: Optional[str] = None,
) -> dict:
    """
    Выполнить полную сверку МойСклад ↔ маркетплейс.

    Returns: {
        channel, date_from, date_to,
        summary: {total_ms, total_mp, matched, only_ms, only_mp, qty_mismatch},
        ms_orders: [...],
        mp_supplies: [...],
        discrepancies: [...]
    }
    """
    # 1. Загрузить МойСклад
    ms_rows, ms_map = _fetch_moysklad_positions(ms_client, date_from, date_to, agent_name, organization)

    # 2. Загрузить маркетплейс
    if channel == "wb":
        mp_rows, mp_map = _fetch_wb_supplies(mp_client, date_from, date_to)
    elif channel == "ozon":
        mp_rows, mp_map = _fetch_ozon_supplies(mp_client, date_from, date_to)
    else:
        raise ValueError(f"Неизвестный канал: {channel}")

    # 3. Сверка
    discrepancies = _reconcile_maps(ms_map, mp_map, channel)

    # 4. Считаем summary
    all_keys = set(ms_map.keys()) | set(mp_map.keys())
    matched = sum(1 for k in all_keys if k in ms_map and k in mp_map and _normalize_qty(ms_map[k]["quantity"]) == _normalize_qty(mp_map[k]["quantity"]))
    only_ms = sum(1 for d in discrepancies if d["status"] == "only_moysklad")
    only_mp = sum(1 for d in discrepancies if d["status"].startswith("only_") and d["status"] != "only_moysklad")
    qty_mismatch = sum(1 for d in discrepancies if d["status"] == "qty_mismatch")

    return {
        "channel": channel,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "summary": {
            "total_ms": len(ms_map),
            "total_mp": len(mp_map),
            "matched": matched,
            "only_ms": only_ms,
            "only_mp": only_mp,
            "qty_mismatch": qty_mismatch,
        },
        "ms_orders": ms_rows,
        "mp_supplies": mp_rows,
        "discrepancies": discrepancies,
    }
