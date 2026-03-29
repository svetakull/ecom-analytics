from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.user import User
from app.services.analytics_settings_service import (
    get_thresholds_list,
    update_threshold,
    reset_thresholds,
)

router = APIRouter()


class ThresholdUpdate(BaseModel):
    value: float = Field(..., ge=0, le=999)


@router.get("/thresholds")
def list_thresholds(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Список всех настраиваемых порогов аналитики."""
    return get_thresholds_list(db)


@router.patch("/thresholds/{key}")
def patch_threshold(
    key: str,
    body: ThresholdUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Обновить один порог."""
    try:
        return update_threshold(db, key, body.value)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/thresholds/reset")
def post_reset_thresholds(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Сбросить все пороги к значениям по умолчанию."""
    return reset_thresholds(db)
