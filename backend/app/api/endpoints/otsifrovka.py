from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.user import User
from app.services.otsifrovka_service import get_otsifrovka

router = APIRouter()


@router.get("")
def otsifrovka(
    date_from: Optional[date] = Query(None, description="Начало периода YYYY-MM-DD"),
    date_to: Optional[date] = Query(None, description="Конец периода YYYY-MM-DD"),
    days: int = Query(30, ge=1, le=365, description="Период в днях (если date_from/date_to не заданы)"),
    channels: Optional[List[str]] = Query(None, description="wb | ozon | lamoda (множественный, ?channels=wb&channels=ozon)"),
    article: Optional[str] = Query(None, description="Фильтр по артикулу (частичный поиск)"),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """
    Фактическая P&L-аналитика по SKU × каналу за период.
    Приоритет: date_from/date_to > days.
    channels: список каналов для фильтрации (множественный выбор).
    article: частичный поиск по артикулу.
    """
    return get_otsifrovka(
        db,
        date_from=date_from,
        date_to=date_to,
        days=days,
        channels=channels,
        article=article,
    )
