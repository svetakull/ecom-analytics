"""API себестоимости: CRUD + batch + import/export + resolve."""
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.user import User
from app.services.cost_price_service import (
    list_cost_prices, create_cost_price, update_cost_price, delete_cost_price,
    resolve_cost, batch_upsert, export_excel, import_excel,
)

router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────────────

class CostPriceCreate(BaseModel):
    sku_id: int
    channel_id: int
    size: Optional[str] = None
    effective_from: Optional[date] = None
    cost_price: float = Field(..., ge=0)
    fulfillment: float = Field(0, ge=0)
    vat_rate: float = Field(0, ge=0, le=100)


class CostPriceUpdate(BaseModel):
    cost_price: Optional[float] = Field(None, ge=0)
    fulfillment: Optional[float] = Field(None, ge=0)
    vat_rate: Optional[float] = Field(None, ge=0, le=100)


class BatchItem(BaseModel):
    seller_article: str
    marketplace_article: Optional[str] = ""
    marketplace: str = "wb"
    size: Optional[str] = None
    effective_from: Optional[date] = None
    cost_price: float
    fulfillment: float = 0
    vat_rate: float = 0


class BatchRequest(BaseModel):
    mode: str = "update"
    items: List[BatchItem]


class ResolveRequest(BaseModel):
    sku_id: int
    channel_id: int
    target_date: date
    size: Optional[str] = None


# ── Endpoints ────────────────────────────────────────────────────────

@router.get("")
def get_cost_prices(
    channel_id: Optional[int] = Query(None),
    marketplace: Optional[str] = Query(None),
    article: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Список себестоимостей."""
    return list_cost_prices(db, channel_id=channel_id, marketplace=marketplace, article=article)


@router.post("")
def post_cost_price(
    body: CostPriceCreate,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Создать запись себестоимости."""
    return create_cost_price(
        db, sku_id=body.sku_id, channel_id=body.channel_id,
        cost_price=body.cost_price, fulfillment=body.fulfillment,
        vat_rate=body.vat_rate, size=body.size,
        effective_from=body.effective_from, user_id=user.id,
    )


@router.put("/{record_id}")
def put_cost_price(
    record_id: int,
    body: CostPriceUpdate,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Обновить запись."""
    try:
        return update_cost_price(
            db, record_id,
            cost_price=body.cost_price, fulfillment=body.fulfillment,
            vat_rate=body.vat_rate, user_id=user.id,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/{record_id}")
def del_cost_price(
    record_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Удалить историческую запись. Удаление default запрещено (422)."""
    try:
        return delete_cost_price(db, record_id, user_id=user.id)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.post("/batch")
def post_batch(
    body: BatchRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Массовый upsert. mode='overwrite' или 'update'."""
    items = [item.dict() for item in body.items]
    return batch_upsert(db, items, mode=body.mode, user_id=user.id)


@router.post("/resolve")
def post_resolve(
    body: ResolveRequest,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Получить актуальную себестоимость на дату."""
    return resolve_cost(db, body.sku_id, body.channel_id, body.target_date, body.size)


@router.get("/export")
def get_export(
    channel_id: Optional[int] = Query(None),
    marketplace: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Скачать Excel с текущими данными."""
    data = export_excel(db, channel_id=channel_id, marketplace=marketplace)
    return StreamingResponse(
        iter([data]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=cost_prices.xlsx"},
    )


@router.post("/import")
async def post_import(
    file: UploadFile = File(...),
    mode: str = Query("update"),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Загрузить Excel файл с себестоимостями."""
    content = await file.read()
    try:
        return import_excel(db, content, mode=mode, user_id=user.id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Import error: {e}")
