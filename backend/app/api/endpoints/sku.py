from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.user import User
from app.schemas.sku import SKUDetail, SKUOut
from app.services.sku_service import get_sku_detail, get_skus

router = APIRouter()


@router.get("/", response_model=list[SKUOut])
def sku_list(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return get_skus(db)


@router.get("/{sku_id}", response_model=SKUDetail)
def sku_detail(
    sku_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    result = get_sku_detail(db, sku_id)
    if not result:
        raise HTTPException(status_code=404, detail="SKU not found")
    return result
