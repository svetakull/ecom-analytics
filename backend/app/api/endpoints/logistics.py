"""Модуль сверки габаритов и логистических расходов WB."""
import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.catalog import SKU, SKUChannel, Channel, ChannelType
from app.models.integration import Integration, IntegrationType
from app.models.logistics import (
    KTRHistory, IRPHistory, WBCardDimensions, WBNomenclatureDimensions,
    WBWarehouseTariff, LogisticsOperation,
)
from app.models.user import User
from app.schemas.logistics import (
    KTRHistoryCreate, KTRHistoryUpdate, KTRHistoryOut,
    IRPHistoryCreate, IRPHistoryUpdate, IRPHistoryOut,
    KTRReferenceRow,
    LogisticsOperationOut, LogisticsOperationsResponse,
    LogisticsArticleSummary, LogisticsArticleResponse,
    LogisticsSummary,
    DimensionsComparisonOut, DimensionsResponse,
    LogisticsFilterOptions, SyncResult,
)
from app.services.export_service import export_logistics_xlsx, export_logistics_csv
from app.services.logistics_calc import KTR_REFERENCE_TABLE, determine_dimensions_status
from app.services.logistics_sync import (
    sync_card_dimensions, sync_warehouse_tariffs,
    import_nomenclature_report, process_financial_report,
)
from app.services.wb_api import WBClient

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Операции ──

@router.get("/operations", response_model=LogisticsOperationsResponse)
def get_operations(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    articles: Optional[list[str]] = Query(None),
    status: Optional[str] = Query(None),
    operation_type: Optional[str] = Query(None),
    warehouse: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = db.query(LogisticsOperation)

    if date_from:
        q = q.filter(LogisticsOperation.operation_date >= date_from)
    if date_to:
        q = q.filter(LogisticsOperation.operation_date <= date_to)
    if articles:
        q = q.filter(LogisticsOperation.seller_article.in_(articles))
    if status:
        q = q.filter(LogisticsOperation.operation_status == status)
    if operation_type:
        q = q.filter(LogisticsOperation.operation_type == operation_type)
    if warehouse:
        q = q.filter(LogisticsOperation.warehouse == warehouse)

    total = q.count()
    ops = (
        q.order_by(LogisticsOperation.operation_date.desc(), LogisticsOperation.id)
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return LogisticsOperationsResponse(
        operations=[LogisticsOperationOut.model_validate(op) for op in ops],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/by-article", response_model=LogisticsArticleResponse)
def get_by_article(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    articles: Optional[list[str]] = Query(None),
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = db.query(LogisticsOperation)
    if date_from:
        q = q.filter(LogisticsOperation.operation_date >= date_from)
    if date_to:
        q = q.filter(LogisticsOperation.operation_date <= date_to)
    if articles:
        q = q.filter(LogisticsOperation.seller_article.in_(articles))
    if status:
        q = q.filter(LogisticsOperation.operation_status == status)

    rows = (
        q.with_entities(
            LogisticsOperation.seller_article,
            LogisticsOperation.nm_id,
            func.count(LogisticsOperation.id).label("cnt"),
            func.sum(LogisticsOperation.expected_logistics).label("sum_expected"),
            func.sum(LogisticsOperation.actual_logistics).label("sum_actual"),
            func.sum(LogisticsOperation.difference).label("sum_diff"),
            func.max(LogisticsOperation.volume_card_liters).label("vol_card"),
            func.max(LogisticsOperation.volume_nomenclature_liters).label("vol_nom"),
        )
        .group_by(LogisticsOperation.seller_article, LogisticsOperation.nm_id)
        .all()
    )

    # Подсчёт статусов по артикулам (отдельный запрос)
    all_ops = q.with_entities(
        LogisticsOperation.seller_article,
        LogisticsOperation.nm_id,
        LogisticsOperation.operation_status,
    ).all()
    status_counts: dict[tuple, dict] = {}
    for op in all_ops:
        key = (op.seller_article, op.nm_id)
        if key not in status_counts:
            status_counts[key] = {"overpay": 0, "saving": 0}
        if op.operation_status == "Переплата":
            status_counts[key]["overpay"] += 1
        elif op.operation_status == "Экономия":
            status_counts[key]["saving"] += 1

    result = []
    for r in rows:
        vol_nom = float(r.vol_nom or 0)
        vol_card = float(r.vol_card or 0)
        cnt = int(r.cnt)
        key = (r.seller_article, r.nm_id)
        sc = status_counts.get(key, {"overpay": 0, "saving": 0})
        result.append(LogisticsArticleSummary(
            seller_article=r.seller_article,
            nm_id=r.nm_id,
            operations_count=cnt,
            total_expected=round(float(r.sum_expected or 0), 2),
            total_actual=round(float(r.sum_actual or 0), 2),
            total_difference=round(float(r.sum_diff or 0), 2),
            volume_card=vol_card,
            volume_nomenclature=vol_nom,
            dimensions_status=determine_dimensions_status(vol_nom, vol_card),
            overpay_count=sc["overpay"],
            saving_count=sc["saving"],
            match_count=cnt - overpay - saving,
        ))

    return LogisticsArticleResponse(articles=result, total=len(result))


@router.get("/summary", response_model=LogisticsSummary)
def get_summary(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    articles: Optional[list[str]] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = db.query(LogisticsOperation)
    if date_from:
        q = q.filter(LogisticsOperation.operation_date >= date_from)
    if date_to:
        q = q.filter(LogisticsOperation.operation_date <= date_to)
    if articles:
        q = q.filter(LogisticsOperation.seller_article.in_(articles))

    ops = q.all()
    if not ops:
        return LogisticsSummary(
            total_expected=0, total_actual=0, total_difference=0,
            total_overpay=0, total_saving=0,
            articles_total=0, articles_overpay=0, articles_saving=0, articles_match=0,
            current_ktr=None, current_irp=None, warnings_count=0,
        )

    total_expected = sum(float(o.expected_logistics) for o in ops)
    total_actual = sum(float(o.actual_logistics) for o in ops)
    total_overpay = sum(float(o.difference) for o in ops if o.operation_status == "Переплата")
    total_saving = sum(abs(float(o.difference)) for o in ops if o.operation_status == "Экономия")

    # По артикулам
    article_stats = {}
    for o in ops:
        art = o.seller_article
        if art not in article_stats:
            article_stats[art] = {"overpay": 0, "saving": 0, "match": 0}
        if o.operation_status == "Переплата":
            article_stats[art]["overpay"] += 1
        elif o.operation_status == "Экономия":
            article_stats[art]["saving"] += 1
        else:
            article_stats[art]["match"] += 1

    art_overpay = sum(1 for s in article_stats.values() if s["overpay"] > s["saving"])
    art_saving = sum(1 for s in article_stats.values() if s["saving"] > s["overpay"])
    art_match = len(article_stats) - art_overpay - art_saving

    # Текущий КТР/ИРП
    today = date.today()
    ktr = db.query(KTRHistory).filter(KTRHistory.date_from <= today, KTRHistory.date_to >= today).first()
    irp = db.query(IRPHistory).filter(IRPHistory.date_from <= today, IRPHistory.date_to >= today).first()

    warnings_count = sum(1 for o in ops if o.ktr_needs_check or o.tariff_missing)

    return LogisticsSummary(
        total_expected=round(total_expected, 2),
        total_actual=round(total_actual, 2),
        total_difference=round(total_expected - total_actual, 2),
        total_overpay=round(total_overpay, 2),
        total_saving=round(total_saving, 2),
        articles_total=len(article_stats),
        articles_overpay=art_overpay,
        articles_saving=art_saving,
        articles_match=art_match,
        current_ktr=float(ktr.value) if ktr else None,
        current_irp=float(irp.value) if irp else None,
        warnings_count=warnings_count,
    )


# ── Габариты ──

@router.get("/dimensions", response_model=DimensionsResponse)
def get_dimensions(
    articles: Optional[list[str]] = Query(None),
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    cards = {d.nm_id: d for d in db.query(WBCardDimensions).all()}
    noms = {d.nm_id: d for d in db.query(WBNomenclatureDimensions).all()}

    all_nm_ids = set(cards.keys()) | set(noms.keys())
    items = []

    # nm_id → seller_article, sku_name
    channel = db.query(Channel).filter(Channel.type == ChannelType.WB).first()
    nm_to_info = {}
    if channel:
        for sc in db.query(SKUChannel).filter(SKUChannel.channel_id == channel.id).all():
            try:
                nm_id = int(sc.mp_article)
                sku = db.query(SKU).filter(SKU.id == sc.sku_id).first()
                nm_to_info[nm_id] = {
                    "seller_article": sku.seller_article if sku else "",
                    "sku_name": sku.name if sku else "",
                }
            except (ValueError, TypeError):
                pass

    for nm_id in all_nm_ids:
        card = cards.get(nm_id)
        nom = noms.get(nm_id)
        info = nm_to_info.get(nm_id, {})
        seller_article = info.get("seller_article", "")

        if articles and seller_article not in articles:
            continue

        vol_card = float(card.volume_liters) if card else 0
        vol_nom = float(nom.volume_liters) if nom else 0
        dim_status = determine_dimensions_status(vol_nom if nom else None, vol_card if card else None)

        if status and dim_status != status:
            continue

        items.append(DimensionsComparisonOut(
            seller_article=seller_article,
            nm_id=nm_id,
            sku_name=info.get("sku_name", ""),
            volume_card=vol_card,
            length_card=float(card.length_cm) if card else 0,
            width_card=float(card.width_cm) if card else 0,
            height_card=float(card.height_cm) if card else 0,
            volume_nomenclature=vol_nom,
            length_nom=float(nom.length_cm) if nom else 0,
            width_nom=float(nom.width_cm) if nom else 0,
            height_nom=float(nom.height_cm) if nom else 0,
            volume_difference=round(vol_nom - vol_card, 4),
            dimensions_status=dim_status,
            card_updated_at=card.card_updated_at if card else None,
        ))

    return DimensionsResponse(items=items, total=len(items))


# ── Синхронизация ──

@router.post("/sync", response_model=SyncResult)
def sync_logistics(
    date_from: date = Query(...),
    date_to: date = Query(...),
    calc_method: str = Query("card", pattern="^(card|nomenclature)$"),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    integration = (
        db.query(Integration)
        .filter(Integration.type == IntegrationType.WB, Integration.is_active.is_(True))
        .first()
    )
    if not integration:
        raise HTTPException(status_code=404, detail="WB интеграция не найдена. Добавьте ключ в Настройках.")

    if not integration.api_key:
        raise HTTPException(status_code=400, detail="API-ключ WB пустой. Заполните в Настройках.")

    client = WBClient(integration.api_key)
    errors = []

    # Шаг 1: Габариты карточек (не блокирует остальное)
    try:
        dims_result = sync_card_dimensions(db, client)
    except Exception as e:
        logger.warning(f"Габариты карточек: {e}")
        dims_result = {"updated": 0}
        errors.append(f"Габариты: {str(e)[:100]}")

    # Шаг 2: Тарифы складов (не блокирует остальное)
    try:
        tariffs_result = sync_warehouse_tariffs(db, client)
    except Exception as e:
        logger.warning(f"Тарифы складов: {e}")
        tariffs_result = {}
        errors.append(f"Тарифы: {str(e)[:100]}")

    # Шаг 3: Финансовый отчёт — основной
    try:
        report_result = process_financial_report(db, client, date_from, date_to, calc_method)
    except Exception as e:
        logger.exception("Ошибка обработки финотчёта")
        report_result = {"processed": 0}
        errors.append(f"Финотчёт: {str(e)[:100]}")

    combined_error = "; ".join(errors) if errors else (report_result.get("error") or tariffs_result.get("error"))

    return SyncResult(
        processed=report_result.get("processed", 0),
        updated=dims_result.get("updated", 0),
        warnings=report_result.get("warnings", 0),
        error=combined_error,
    )


@router.post("/upload-nomenclature", response_model=SyncResult)
async def upload_nomenclature(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    content = await file.read()
    result = import_nomenclature_report(db, content, file.filename or "report.xlsx")
    return SyncResult(
        processed=result.get("imported", 0),
        error=result.get("error"),
    )


# ── КТР CRUD ──

@router.get("/ktr", response_model=list[KTRHistoryOut])
def list_ktr(db: Session = Depends(get_db), _: User = Depends(get_current_user)):
    return db.query(KTRHistory).order_by(KTRHistory.date_from.desc()).all()


@router.post("/ktr", response_model=KTRHistoryOut, status_code=201)
def create_ktr(
    data: KTRHistoryCreate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    record = KTRHistory(date_from=data.date_from, date_to=data.date_to, value=data.value)
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


@router.put("/ktr/{record_id}", response_model=KTRHistoryOut)
def update_ktr(
    record_id: int,
    data: KTRHistoryUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    record = db.query(KTRHistory).filter(KTRHistory.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Запись КТР не найдена")
    if data.date_from is not None:
        record.date_from = data.date_from
    if data.date_to is not None:
        record.date_to = data.date_to
    if data.value is not None:
        record.value = data.value
    db.commit()
    db.refresh(record)
    return record


@router.delete("/ktr/{record_id}")
def delete_ktr(
    record_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    record = db.query(KTRHistory).filter(KTRHistory.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Запись КТР не найдена")
    db.delete(record)
    db.commit()
    return {"ok": True}


# ── ИРП CRUD ──

@router.get("/irp", response_model=list[IRPHistoryOut])
def list_irp(db: Session = Depends(get_db), _: User = Depends(get_current_user)):
    return db.query(IRPHistory).order_by(IRPHistory.date_from.desc()).all()


@router.post("/irp", response_model=IRPHistoryOut, status_code=201)
def create_irp(
    data: IRPHistoryCreate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    record = IRPHistory(date_from=data.date_from, date_to=data.date_to, value=data.value)
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


@router.put("/irp/{record_id}", response_model=IRPHistoryOut)
def update_irp(
    record_id: int,
    data: IRPHistoryUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    record = db.query(IRPHistory).filter(IRPHistory.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Запись ИРП не найдена")
    if data.date_from is not None:
        record.date_from = data.date_from
    if data.date_to is not None:
        record.date_to = data.date_to
    if data.value is not None:
        record.value = data.value
    db.commit()
    db.refresh(record)
    return record


@router.delete("/irp/{record_id}")
def delete_irp(
    record_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    record = db.query(IRPHistory).filter(IRPHistory.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Запись ИРП не найдена")
    db.delete(record)
    db.commit()
    return {"ok": True}


# ── Справочник КТР/КРП ──

@router.get("/ktr-reference", response_model=list[KTRReferenceRow])
def get_ktr_reference(_: User = Depends(get_current_user)):
    return [
        KTRReferenceRow(
            localization_min=row[0],
            localization_max=row[1],
            ktr_before=row[2],
            ktr_after=row[3],
            krp_irp=row[4],
        )
        for row in KTR_REFERENCE_TABLE
    ]


# ── Экспорт ──

@router.get("/export")
def export_data(
    format: str = Query("xlsx", pattern="^(xlsx|csv)$"),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    articles: Optional[list[str]] = Query(None),
    status: Optional[str] = Query(None),
    operation_type: Optional[str] = Query(None),
    warehouse: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    q = db.query(LogisticsOperation)
    if date_from:
        q = q.filter(LogisticsOperation.operation_date >= date_from)
    if date_to:
        q = q.filter(LogisticsOperation.operation_date <= date_to)
    if articles:
        q = q.filter(LogisticsOperation.seller_article.in_(articles))
    if status:
        q = q.filter(LogisticsOperation.operation_status == status)
    if operation_type:
        q = q.filter(LogisticsOperation.operation_type == operation_type)
    if warehouse:
        q = q.filter(LogisticsOperation.warehouse == warehouse)

    ops = q.order_by(LogisticsOperation.operation_date.desc()).all()

    rows = []
    for op in ops:
        rows.append({
            "seller_article": op.seller_article,
            "nm_id": op.nm_id,
            "operation_type": op.operation_type,
            "warehouse": op.warehouse,
            "supply_number": op.supply_number,
            "operation_date": str(op.operation_date),
            "coef_fix_start": str(op.coef_fix_start) if op.coef_fix_start else "",
            "coef_fix_end": str(op.coef_fix_end) if op.coef_fix_end else "",
            "warehouse_coef": float(op.warehouse_coef),
            "ktr_value": float(op.ktr_value),
            "irp_value": float(op.irp_value),
            "base_first_liter": float(op.base_first_liter),
            "base_per_liter": float(op.base_per_liter),
            "volume_card_liters": float(op.volume_card_liters),
            "volume_nomenclature_liters": float(op.volume_nomenclature_liters),
            "calculated_wb_volume": float(op.calculated_wb_volume),
            "retail_price": float(op.retail_price),
            "expected_logistics": float(op.expected_logistics),
            "actual_logistics": float(op.actual_logistics),
            "difference": float(op.difference),
            "operation_status": op.operation_status,
            "dimensions_status": op.dimensions_status,
            "volume_difference": float(op.volume_difference),
            "ktr_needs_check": op.ktr_needs_check,
            "tariff_missing": op.tariff_missing,
        })

    if format == "csv":
        content = export_logistics_csv(rows)
        return StreamingResponse(
            iter([content]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=logistics_report.csv"},
        )

    content = export_logistics_xlsx(rows)
    return StreamingResponse(
        iter([content]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=logistics_report.xlsx"},
    )


# ── Фильтры ──

@router.get("/filters", response_model=LogisticsFilterOptions)
def get_filters(db: Session = Depends(get_db), _: User = Depends(get_current_user)):
    warehouses = [
        r[0] for r in
        db.query(LogisticsOperation.warehouse).distinct().order_by(LogisticsOperation.warehouse).all()
        if r[0]
    ]
    articles_list = [
        r[0] for r in
        db.query(LogisticsOperation.seller_article).distinct().order_by(LogisticsOperation.seller_article).all()
        if r[0]
    ]
    op_types = [
        r[0] for r in
        db.query(LogisticsOperation.operation_type).distinct().order_by(LogisticsOperation.operation_type).all()
        if r[0]
    ]

    # Недели из дат операций
    dates = [
        r[0] for r in
        db.query(LogisticsOperation.operation_date).distinct().order_by(LogisticsOperation.operation_date).all()
    ]
    weeks = sorted({d.strftime("%G-W%V") for d in dates if d}, reverse=True)

    return LogisticsFilterOptions(
        warehouses=warehouses,
        articles=articles_list,
        weeks=weeks,
        operation_types=op_types,
    )
