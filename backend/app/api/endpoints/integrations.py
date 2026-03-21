from datetime import datetime, date

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user, require_roles
from app.models.integration import Integration, IntegrationType, IntegrationStatus
from app.models.user import UserRole
from app.services.wb_api import WBClient, WBApiError
from app.services.wb_sync import run_full_sync, sync_ads, sync_stocks, sync_logistics_weekly, sync_commission_weekly, sync_nm_report, sync_prices
from app.services.ozon_api import OzonClient, OzonApiError
from app.services.ozon_sync import run_full_sync as ozon_run_full_sync

router = APIRouter()


class IntegrationCreate(BaseModel):
    type: str
    name: str
    api_key: str
    client_id: str | None = None


class AdsTokenUpdate(BaseModel):
    ads_api_key: str


class PricesTokenUpdate(BaseModel):
    prices_api_key: str


class IntegrationOut(BaseModel):
    id: int
    type: str
    name: str
    status: str
    last_sync_at: datetime | None
    last_error: str | None

    model_config = {"from_attributes": True}


@router.get("/", response_model=list[IntegrationOut])
def list_integrations(
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    items = db.query(Integration).filter(Integration.is_active == True).all()
    return [IntegrationOut(
        id=i.id, type=i.type.value, name=i.name,
        status=i.status.value, last_sync_at=i.last_sync_at, last_error=i.last_error
    ) for i in items]


@router.post("/", response_model=IntegrationOut)
def create_integration(
    payload: IntegrationCreate,
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    # Проверяем ключ
    if payload.type == "wb":
        client = WBClient(payload.api_key)
        try:
            client.test_connection()
        except WBApiError as e:
            raise HTTPException(status_code=400, detail=f"Ошибка подключения к WB: {e}")
    elif payload.type == "ozon":
        if not payload.client_id:
            raise HTTPException(status_code=400, detail="Для Ozon обязателен client_id")
        client = OzonClient(payload.api_key, payload.client_id)
        try:
            if not client.test_connection():
                raise HTTPException(status_code=400, detail="Ошибка подключения к Ozon")
        except OzonApiError as e:
            raise HTTPException(status_code=400, detail=f"Ошибка подключения к Ozon: {e}")

    integration = Integration(
        type=IntegrationType(payload.type),
        name=payload.name,
        api_key=payload.api_key,
        client_id=payload.client_id,
    )
    db.add(integration)
    db.commit()
    db.refresh(integration)
    return IntegrationOut(
        id=integration.id, type=integration.type.value, name=integration.name,
        status=integration.status.value, last_sync_at=integration.last_sync_at,
        last_error=integration.last_error
    )


@router.post("/{integration_id}/sync")
def sync_integration(
    integration_id: int,
    days_back: int = 30,
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    integration = db.query(Integration).filter(
        Integration.id == integration_id, Integration.is_active == True
    ).first()
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")

    if integration.type == IntegrationType.WB:
        result = run_full_sync(db, integration, days_back)
    elif integration.type == IntegrationType.OZON:
        result = ozon_run_full_sync(db, integration, days_back)
    else:
        raise HTTPException(status_code=400, detail="Тип интеграции не поддерживается")

    return result


@router.post("/{integration_id}/sync-ads")
def sync_ads_integration(
    integration_id: int,
    days_back: int = 14,
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    """Синхронизировать только рекламные кампании и их статистику из WB."""
    integration = db.query(Integration).filter(
        Integration.id == integration_id, Integration.is_active == True
    ).first()
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")
    if integration.type.value != "wb":
        raise HTTPException(status_code=400, detail="Только для WB интеграций")

    client = WBClient(integration.api_key, ads_api_key=integration.ads_api_key, prices_api_key=integration.prices_api_key)
    result = sync_ads(db, client, days_back)
    return result


@router.post("/{integration_id}/sync-stocks")
def sync_stocks_integration(
    integration_id: int,
    target_date: date | None = None,
    force: bool = False,
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    """
    Выгрузить остатки за target_date (по умолчанию — вчера).
    force=true — перезаписать, даже если запись уже есть.
    """
    integration = db.query(Integration).filter(
        Integration.id == integration_id, Integration.is_active == True
    ).first()
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")
    if integration.type.value != "wb":
        raise HTTPException(status_code=400, detail="Только для WB интеграций")

    if force and target_date:
        from app.models.inventory import Stock
        from app.models.catalog import Warehouse
        wh = db.query(Warehouse).filter(Warehouse.name == "WB склад").first()
        if wh:
            db.query(Stock).filter(
                Stock.date == target_date,
                Stock.warehouse_id == wh.id,
            ).delete()
            db.commit()

    client = WBClient(integration.api_key)
    result = sync_stocks(db, client, target_date=target_date)
    return result


@router.post("/{integration_id}/sync-logistics")
def sync_logistics_integration(
    integration_id: int,
    week_offset: int = 1,
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    """
    Рассчитать и сохранить среднюю логистику на единицу из финотчёта WB.
    week_offset=1 → прошлая полная неделя (пн–вс), 2 → позапрошлая и т.д.
    Обновляет SKUChannel.logistics_override для каждого артикула.
    """
    integration = db.query(Integration).filter(
        Integration.id == integration_id, Integration.is_active == True
    ).first()
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")
    if integration.type.value != "wb":
        raise HTTPException(status_code=400, detail="Только для WB интеграций")

    client = WBClient(integration.api_key)
    result = sync_logistics_weekly(db, client, week_offset=week_offset)
    return result


@router.post("/{integration_id}/sync-commission")
def sync_commission_integration(
    integration_id: int,
    week_offset: int = 1,
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    """
    Рассчитать комиссию + эквайринг (%) из детального финансового отчёта WB
    за прошлую полную неделю и записать в SKUChannel.commission_pct_override.
    Формула: sum(ppvz_sales_commission + acquiring_fee) / sum(retail_price_withdisc_rub) * 100
    week_offset=1 → прошлая неделя, 2 → позапрошлая.
    """
    integration = db.query(Integration).filter(
        Integration.id == integration_id, Integration.is_active == True
    ).first()
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")
    if integration.type.value != "wb":
        raise HTTPException(status_code=400, detail="Только для WB интеграций")

    client = WBClient(integration.api_key)
    result = sync_commission_weekly(db, client, week_offset=week_offset)
    return result


@router.post("/sync-nm-report-all")
def sync_nm_report_all(
    days_back: int = 14,
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    """Синхронизировать воронку карточки по всем активным WB интеграциям."""
    integrations = db.query(Integration).filter(
        Integration.is_active == True,
        Integration.type == IntegrationType.WB,
    ).all()
    if not integrations:
        raise HTTPException(status_code=404, detail="Нет активных WB интеграций")

    all_results = []
    for integration in integrations:
        client = WBClient(integration.api_key)
        result = sync_nm_report(db, client, days_back)
        all_results.append({"integration": integration.name, **result})
    return all_results


@router.post("/{integration_id}/sync-nm-report")
def sync_nm_report_integration(
    integration_id: int,
    days_back: int = 14,
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    """Синхронизировать данные воронки карточки (переходы, корзина) и рейтинги из WB nm-report."""
    integration = db.query(Integration).filter(
        Integration.id == integration_id, Integration.is_active == True
    ).first()
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")
    if integration.type.value != "wb":
        raise HTTPException(status_code=400, detail="Только для WB интеграций")

    client = WBClient(integration.api_key)
    result = sync_nm_report(db, client, days_back)
    return result


@router.post("/sync-prices-all")
def sync_prices_all(
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    """Синхронизировать актуальные цены продавца (discountedPrice) по всем активным WB интеграциям."""
    integrations = db.query(Integration).filter(
        Integration.is_active == True,
        Integration.type == IntegrationType.WB,
    ).all()
    if not integrations:
        raise HTTPException(status_code=404, detail="Нет активных WB интеграций")

    all_results = []
    for integration in integrations:
        client = WBClient(integration.api_key)
        result = sync_prices(db, client)
        all_results.append({"integration": integration.name, **result})
    return all_results


@router.patch("/{integration_id}/ads-token")
def set_ads_token(
    integration_id: int,
    payload: AdsTokenUpdate,
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    """
    Сохранить WB рекламный токен (новый единый JWT) для интеграции.
    Этот токен используется для запросов к advert-api.wildberries.ru.
    Получить: личный кабинет WB → Настройки → Доступ к новому API → выбрать скоп «Реклама».
    """
    integration = db.query(Integration).filter(
        Integration.id == integration_id, Integration.is_active == True
    ).first()
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")

    integration.ads_api_key = payload.ads_api_key
    db.commit()

    # Проверяем токен сразу
    client = WBClient(integration.api_key, ads_api_key=payload.ads_api_key)
    import httpx as _httpx
    try:
        r = _httpx.get(
            "https://advert-api.wildberries.ru/adv/v1/promotion/count",
            headers=client.ads_headers,
            timeout=10,
        )
        if r.status_code == 401:
            return {"ok": True, "warning": "Токен сохранён, но проверка API вернула 401 — возможно скоп «Реклама» не включён"}
        return {"ok": True, "status_code": r.status_code, "message": "Токен сохранён и проверен успешно"}
    except Exception as e:
        return {"ok": True, "warning": f"Токен сохранён, проверка недоступна: {e}"}


@router.patch("/{integration_id}/prices-token")
def set_prices_token(
    integration_id: int,
    payload: PricesTokenUpdate,
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    """Сохранить WB токен «Цены и скидки» для интеграции."""
    integration = db.query(Integration).filter(
        Integration.id == integration_id, Integration.is_active == True
    ).first()
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")

    integration.prices_api_key = payload.prices_api_key
    db.commit()

    import httpx as _httpx
    try:
        r = _httpx.get(
            "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter?limit=1",
            headers={"Authorization": payload.prices_api_key},
            timeout=10,
        )
        if r.status_code == 401:
            return {"ok": True, "warning": "Токен сохранён, но проверка вернула 401 — возможно скоп «Цены и скидки» не включён"}
        return {"ok": True, "status_code": r.status_code, "message": "Токен сохранён и проверен успешно"}
    except Exception as e:
        return {"ok": True, "warning": f"Токен сохранён, проверка недоступна: {e}"}


@router.delete("/{integration_id}")
def delete_integration(
    integration_id: int,
    db: Session = Depends(get_db),
    _=Depends(require_roles(UserRole.OWNER)),
):
    integration = db.query(Integration).filter(Integration.id == integration_id).first()
    if not integration:
        raise HTTPException(status_code=404, detail="Not found")
    integration.is_active = False
    db.commit()
    return {"ok": True}
