"""ОПиУ — Отчёт о прибылях и убытках."""
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.user import User
from app.services.opiu_service import get_opiu

router = APIRouter()


@router.get("")
def opiu(
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    channels: Optional[List[str]] = Query(None),
    article: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    if date_from is None:
        date_from = date(date.today().year, 1, 1)
    if date_to is None:
        date_to = date.today()
    return get_opiu(db, date_from=date_from, date_to=date_to, channels=channels, article=article)
