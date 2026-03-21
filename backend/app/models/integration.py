from datetime import datetime
import enum

from sqlalchemy import Boolean, DateTime, Enum, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class IntegrationType(str, enum.Enum):
    WB = "wb"
    OZON = "ozon"
    LAMODA = "lamoda"
    MOYSKLAD = "moysklad"


class IntegrationStatus(str, enum.Enum):
    ACTIVE = "active"
    ERROR = "error"
    DISABLED = "disabled"


class Integration(Base):
    __tablename__ = "integrations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    type: Mapped[IntegrationType] = mapped_column(Enum(IntegrationType), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    api_key: Mapped[str] = mapped_column(Text, nullable=False)
    ads_api_key: Mapped[str] = mapped_column(Text, nullable=True)     # WB: рекламный токен (JWT) | Ozon: Performance API client_secret
    prices_api_key: Mapped[str] = mapped_column(Text, nullable=True)  # WB токен «Цены и скидки» (новый JWT)
    client_id: Mapped[str] = mapped_column(String(200), nullable=True)  # для Ozon Seller API
    perf_client_id: Mapped[str] = mapped_column(String(200), nullable=True)  # для Ozon Performance API client_id
    supplies_api_key: Mapped[str] = mapped_column(Text, nullable=True)        # WB: токен для Поставок (Supplies API)
    status: Mapped[IntegrationStatus] = mapped_column(
        Enum(IntegrationStatus), default=IntegrationStatus.ACTIVE
    )
    last_sync_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
