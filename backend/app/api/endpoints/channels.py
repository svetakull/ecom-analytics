from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.catalog import Channel

router = APIRouter()


class ChannelOut(BaseModel):
    id: int
    name: str
    type: str
    is_active: bool
    commission_pct: float

    model_config = {"from_attributes": True}


@router.get("/", response_model=list[ChannelOut])
def get_channels(
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    channels = db.query(Channel).filter(Channel.is_active == True).all()
    return [ChannelOut(
        id=c.id, name=c.name, type=c.type.value,
        is_active=c.is_active, commission_pct=c.commission_pct
    ) for c in channels]
