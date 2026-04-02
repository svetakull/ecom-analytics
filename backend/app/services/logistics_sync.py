"""
Синхронизация данных габаритов и логистики WB.
"""
import io
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import openpyxl
from sqlalchemy.orm import Session

from app.models.catalog import SKU, SKUChannel, Channel, ChannelType
from app.models.logistics import (
    WBCardDimensions, WBNomenclatureDimensions, WBWarehouseTariff, LogisticsOperation,
)
from app.services.logistics_calc import (
    calculate_expected_logistics, reverse_calculate_volume,
    determine_operation_status, determine_dimensions_status,
    get_ktr_for_date, get_irp_for_date,
    is_direct_operation, is_reverse_operation,
    DEFAULT_BASE_FIRST, DEFAULT_BASE_PER,
)
from app.services.wb_api import WBClient, WBApiError

logger = logging.getLogger(__name__)


def _get_wb_channel(db: Session) -> Channel:
    ch = db.query(Channel).filter(Channel.type == ChannelType.WB).first()
    if not ch:
        ch = Channel(name="Wildberries", type=ChannelType.WB, commission_pct=16.5)
        db.add(ch)
        db.flush()
    return ch


def sync_card_dimensions(db: Session, client: WBClient) -> dict:
    """Получить габариты карточек товаров через Content API."""
    channel = _get_wb_channel(db)
    sku_channels = (
        db.query(SKUChannel)
        .filter(SKUChannel.channel_id == channel.id, SKUChannel.mp_article.isnot(None))
        .all()
    )

    nm_ids = []
    nm_to_sku = {}
    for sc in sku_channels:
        try:
            nm_id = int(sc.mp_article)
            nm_ids.append(nm_id)
            nm_to_sku[nm_id] = sc.sku_id
        except (ValueError, TypeError):
            continue

    if not nm_ids:
        return {"updated": 0, "total": 0}

    cards = client.get_card_content(nm_ids)
    updated = 0

    for card in cards:
        nm_id = card.get("nmID") or card.get("imtID")
        if not nm_id:
            continue

        dims = card.get("dimensions", {})
        length = float(dims.get("length", 0))
        width = float(dims.get("width", 0))
        height = float(dims.get("height", 0))
        volume = (length * width * height) / 1000.0  # см³ → литры

        existing = db.query(WBCardDimensions).filter(WBCardDimensions.nm_id == nm_id).first()
        if existing:
            existing.length_cm = length
            existing.width_cm = width
            existing.height_cm = height
            existing.volume_liters = volume
            existing.fetched_at = datetime.utcnow()
            existing.sku_id = nm_to_sku.get(nm_id, existing.sku_id)
        else:
            db.add(WBCardDimensions(
                sku_id=nm_to_sku.get(nm_id),
                nm_id=nm_id,
                length_cm=length, width_cm=width, height_cm=height,
                volume_liters=volume,
                fetched_at=datetime.utcnow(),
            ))
        updated += 1

    db.commit()
    return {"updated": updated, "total": len(nm_ids)}


def sync_warehouse_tariffs(db: Session, client: WBClient) -> dict:
    """Получить тарифы складов WB."""
    try:
        tariffs = client.get_warehouse_tariffs()
    except WBApiError as e:
        logger.error(f"Ошибка получения тарифов WB: {e}")
        return {"updated": 0, "error": str(e)}

    updated = 0
    for t in tariffs:
        name = t.get("warehouseName", "")
        if not name:
            continue
        base_first = float(t.get("boxDeliveryBase", DEFAULT_BASE_FIRST))
        base_per = float(t.get("boxDeliveryLiter", DEFAULT_BASE_PER))

        existing = db.query(WBWarehouseTariff).filter(WBWarehouseTariff.warehouse_name == name).first()
        if existing:
            existing.base_first_liter = base_first
            existing.base_per_liter = base_per
            existing.fetched_at = datetime.utcnow()
        else:
            db.add(WBWarehouseTariff(
                warehouse_name=name,
                base_first_liter=base_first,
                base_per_liter=base_per,
                fetched_at=datetime.utcnow(),
            ))
        updated += 1

    db.commit()
    return {"updated": updated}


def import_nomenclature_report(db: Session, file_bytes: bytes, filename: str) -> dict:
    """
    Парсинг файла отчёта номенклатур WB (Excel/CSV) с замерами.
    Ожидаемые столбцы: nmId, Длина (см), Ширина (см), Высота (см), Объём (л)
    """
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return {"imported": 0, "error": "Файл пуст"}

    header = [str(h).strip().lower() if h else "" for h in rows[0]]

    # Определяем индексы столбцов
    nm_idx = next((i for i, h in enumerate(header) if "nmid" in h or "nm_id" in h or "номенклатура" in h), None)
    len_idx = next((i for i, h in enumerate(header) if "длин" in h or "length" in h), None)
    wid_idx = next((i for i, h in enumerate(header) if "ширин" in h or "width" in h), None)
    hei_idx = next((i for i, h in enumerate(header) if "высот" in h or "height" in h), None)
    vol_idx = next((i for i, h in enumerate(header) if "объём" in h or "объем" in h or "volume" in h), None)

    if nm_idx is None:
        return {"imported": 0, "error": "Не найден столбец nmId"}

    # Карта nm_id → sku_id
    channel = _get_wb_channel(db)
    sc_map = {}
    for sc in db.query(SKUChannel).filter(SKUChannel.channel_id == channel.id).all():
        try:
            sc_map[int(sc.mp_article)] = sc.sku_id
        except (ValueError, TypeError):
            pass

    imported = 0
    for row in rows[1:]:
        try:
            nm_id = int(row[nm_idx])
        except (ValueError, TypeError, IndexError):
            continue

        length = float(row[len_idx]) if len_idx is not None and row[len_idx] else 0
        width = float(row[wid_idx]) if wid_idx is not None and row[wid_idx] else 0
        height = float(row[hei_idx]) if hei_idx is not None and row[hei_idx] else 0

        if vol_idx is not None and row[vol_idx]:
            volume = float(row[vol_idx])
        else:
            volume = (length * width * height) / 1000.0

        existing = db.query(WBNomenclatureDimensions).filter(WBNomenclatureDimensions.nm_id == nm_id).first()
        if existing:
            existing.length_cm = length
            existing.width_cm = width
            existing.height_cm = height
            existing.volume_liters = volume
            existing.updated_at = datetime.utcnow()
            existing.sku_id = sc_map.get(nm_id, existing.sku_id)
        else:
            db.add(WBNomenclatureDimensions(
                sku_id=sc_map.get(nm_id),
                nm_id=nm_id,
                length_cm=length, width_cm=width, height_cm=height,
                volume_liters=volume,
                updated_at=datetime.utcnow(),
            ))
        imported += 1

    db.commit()
    wb.close()
    return {"imported": imported}


def process_financial_report(
    db: Session,
    client: WBClient,
    date_from: date,
    date_to: date,
    calc_method: str = "card",  # "card" или "nomenclature"
) -> dict:
    """
    Загрузить финансовый отчёт WB и рассчитать логистику по каждой операции.
    Расширяет существующий парсер — извлекает доп. поля для модуля габаритов.
    """
    channel = _get_wb_channel(db)

    # Загружаем финотчёт
    try:
        rows = client.get_report_detail(date_from, date_to)
    except WBApiError as e:
        logger.error(f"Ошибка загрузки финотчёта: {e}")
        return {"processed": 0, "error": str(e)}

    # Загружаем справочники
    card_dims = {d.nm_id: d for d in db.query(WBCardDimensions).all()}
    nom_dims = {d.nm_id: d for d in db.query(WBNomenclatureDimensions).all()}
    tariffs = {t.warehouse_name: t for t in db.query(WBWarehouseTariff).all()}

    # nm_id → sku
    sc_map = {}
    for sc in db.query(SKUChannel).filter(SKUChannel.channel_id == channel.id).all():
        try:
            sc_map[int(sc.mp_article)] = sc.sku_id
        except (ValueError, TypeError):
            pass

    processed = 0
    warnings = 0

    for row in rows:
        op_type = row.get("supplier_oper_name", "")
        # Фильтруем только логистические операции
        if not (op_type in {"Логистика"} or "клиент" in op_type.lower()):
            continue

        # Нормализуем тип операции
        normalized_type = _normalize_operation_type(op_type)
        if not normalized_type:
            continue

        nm_id = int(row.get("nm_id", 0))
        if not nm_id:
            continue

        seller_article = str(row.get("sa_name", ""))
        warehouse = str(row.get("office_name", ""))
        supply_number = str(row.get("gi_id", ""))
        actual_logistics = abs(float(row.get("delivery_rub", 0)))

        # Дата операции
        rr_dt = row.get("rr_dt", "") or row.get("sale_dt", "")
        try:
            op_date = date.fromisoformat(str(rr_dt)[:10])
        except (ValueError, TypeError):
            continue

        # Коэффициенты из отчёта
        warehouse_coef = float(row.get("kiz", 1.0) or 1.0)
        # Даты фиксации коэффициента
        coef_fix_start = _parse_date_safe(row.get("fix_tariff_date_from"))
        coef_fix_end = _parse_date_safe(row.get("fix_tariff_date_to"))

        retail_price = float(row.get("retail_price_withdisc_rub", 0) or 0)

        # КТР и ИРП из истории
        ktr_value, ktr_needs_check = get_ktr_for_date(db, op_date)
        if ktr_value is None:
            ktr_value = 1.0
            ktr_needs_check = True

        irp_value = get_irp_for_date(db, op_date) or 0.0

        # Тарифы склада
        tariff = tariffs.get(warehouse)
        tariff_missing = tariff is None
        base_first = float(tariff.base_first_liter) if tariff else DEFAULT_BASE_FIRST
        base_per = float(tariff.base_per_liter) if tariff else DEFAULT_BASE_PER

        # Объёмы
        card = card_dims.get(nm_id)
        nom = nom_dims.get(nm_id)
        vol_card = float(card.volume_liters) if card else 0.0
        vol_nom = float(nom.volume_liters) if nom else 0.0

        # Выбор объёма для расчёта
        volume = vol_card if calc_method == "card" else vol_nom

        # Расчёт ожидаемой логистики
        if tariff_missing:
            expected = 0.0
        else:
            expected = calculate_expected_logistics(
                volume=volume,
                warehouse_coef=warehouse_coef,
                ktr=ktr_value,
                irp_pct=irp_value,
                retail_price=retail_price,
                operation_type=normalized_type,
                operation_date=op_date,
                base_first=base_first,
                base_per=base_per,
            )

        difference = round(expected - actual_logistics, 2)
        op_status = determine_operation_status(expected, actual_logistics)
        dim_status = determine_dimensions_status(vol_nom, vol_card)

        # Обратный расчёт объёма WB
        calc_wb_vol = reverse_calculate_volume(
            actual_cost=actual_logistics,
            warehouse_coef=warehouse_coef,
            ktr=ktr_value,
            irp_pct=irp_value,
            retail_price=retail_price,
            operation_type=normalized_type,
            operation_date=op_date,
            base_first=base_first,
            base_per=base_per,
        )

        # Upsert
        existing = (
            db.query(LogisticsOperation)
            .filter(
                LogisticsOperation.nm_id == nm_id,
                LogisticsOperation.operation_date == op_date,
                LogisticsOperation.operation_type == normalized_type,
                LogisticsOperation.supply_number == supply_number,
            )
            .first()
        )

        data = dict(
            sku_id=sc_map.get(nm_id),
            nm_id=nm_id,
            seller_article=seller_article,
            operation_type=normalized_type,
            warehouse=warehouse,
            supply_number=supply_number,
            operation_date=op_date,
            coef_fix_start=coef_fix_start,
            coef_fix_end=coef_fix_end,
            warehouse_coef=warehouse_coef,
            ktr_value=ktr_value,
            irp_value=irp_value,
            base_first_liter=base_first,
            base_per_liter=base_per,
            volume_card_liters=vol_card,
            volume_nomenclature_liters=vol_nom,
            calculated_wb_volume=calc_wb_vol or 0,
            retail_price=retail_price,
            expected_logistics=expected,
            actual_logistics=actual_logistics,
            difference=difference,
            operation_status=op_status,
            dimensions_status=dim_status,
            volume_difference=round(vol_nom - vol_card, 4) if vol_nom and vol_card else 0,
            ktr_needs_check=ktr_needs_check,
            tariff_missing=tariff_missing,
            report_id=str(row.get("rrd_id", "")),
        )

        if existing:
            for k, v in data.items():
                setattr(existing, k, v)
        else:
            db.add(LogisticsOperation(**data))

        processed += 1
        if ktr_needs_check or tariff_missing:
            warnings += 1

    db.commit()
    return {"processed": processed, "warnings": warnings}


def _normalize_operation_type(raw_type: str) -> Optional[str]:
    """Привести тип операции к стандартному виду."""
    raw = raw_type.strip().lower()
    if raw == "логистика":
        return "К клиенту при продаже"
    if "продаж" in raw and "клиент" in raw:
        return "К клиенту при продаже"
    if "отмен" in raw and "к клиент" in raw:
        return "К клиенту при отмене"
    if "возврат" in raw:
        return "От клиента при возврате"
    if "отмен" in raw and "от клиент" in raw:
        return "От клиента при отмене"
    if "логистик" in raw:
        return "К клиенту при продаже"
    return None


def _parse_date_safe(val) -> Optional[date]:
    if not val:
        return None
    try:
        return date.fromisoformat(str(val)[:10])
    except (ValueError, TypeError):
        return None
