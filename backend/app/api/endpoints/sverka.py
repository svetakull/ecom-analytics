"""Сверка поставок МойСклад ↔ WB/Ozon."""
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.integration import Integration, IntegrationType
from app.models.user import User
from app.services.moysklad_api import MoySkladClient
from app.services.ozon_api import OzonClient
from app.services.sverka_service import reconcile_supplies
from app.services.wb_api import WBClient

router = APIRouter()


@router.get("")
def sverka_postavok(
    date_from: date = Query(...),
    date_to: date = Query(...),
    channel: str = Query(..., description="wb или ozon"),
    agent_name: str = Query("Озон", description="Имя контрагента в МойСклад"),
    organization: Optional[str] = Query(None, description="Организация в МойСклад"),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    # МойСклад интеграция
    ms_integration = (
        db.query(Integration)
        .filter(Integration.type == IntegrationType.MOYSKLAD, Integration.is_active.is_(True))
        .first()
    )
    if not ms_integration:
        raise HTTPException(status_code=404, detail="МойСклад интеграция не найдена. Добавьте в Настройках.")

    # Маркетплейс интеграция
    channel_lower = channel.lower()
    if channel_lower == "wb":
        mp_type = IntegrationType.WB
    elif channel_lower == "ozon":
        mp_type = IntegrationType.OZON
    else:
        raise HTTPException(status_code=400, detail="Канал должен быть 'wb' или 'ozon'")

    mp_integration = (
        db.query(Integration)
        .filter(Integration.type == mp_type, Integration.is_active.is_(True))
        .first()
    )
    if not mp_integration:
        raise HTTPException(status_code=404, detail=f"Интеграция {channel.upper()} не найдена")

    # Создаём клиенты
    ms_client = MoySkladClient(ms_integration.api_key)

    if channel_lower == "wb":
        # Для Supplies API нужен отдельный ключ (другой scope)
        supplies_key = mp_integration.supplies_api_key or mp_integration.api_key
        mp_client = WBClient(supplies_key)
    else:
        mp_client = OzonClient(mp_integration.api_key, mp_integration.client_id or "")

    # Запускаем сверку
    return reconcile_supplies(
        ms_client=ms_client,
        mp_client=mp_client,
        channel=channel_lower,
        date_from=date_from,
        date_to=date_to,
        agent_name=agent_name,
        organization=organization,
    )
