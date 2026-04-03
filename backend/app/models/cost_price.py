"""
Модуль учёта себестоимости товаров.
Поддерживает: запись по умолчанию + исторические записи с effective_from.
"""
import enum
from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Boolean, Date, DateTime, Enum, ForeignKey, Integer,
    Numeric, String, Text, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class CostPrice(Base):
    """Себестоимость товара: default + историческая по датам."""
    __tablename__ = "cost_prices"
    __table_args__ = (
        UniqueConstraint(
            "sku_id", "channel_id", "size", "effective_from",
            name="uq_cost_price_sku_ch_size_date",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False, index=True)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"), nullable=False)
    size: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)  # NULL = без размера

    is_default: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    effective_from: Mapped[Optional[date]] = mapped_column(Date, nullable=True)  # NULL если is_default=True

    cost_price: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)  # себестоимость без НДС
    fulfillment: Mapped[float] = mapped_column(Numeric(12, 4), default=0, nullable=False)  # фулфилмент без НДС
    vat_rate: Mapped[float] = mapped_column(Numeric(5, 2), default=0, nullable=False)  # ставка НДС %

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    sku: Mapped["SKU"] = relationship()  # type: ignore[name-defined]
    channel: Mapped["Channel"] = relationship()  # type: ignore[name-defined]


class CostPriceAuditAction(str, enum.Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class CostPriceAuditSource(str, enum.Enum):
    UI = "ui"
    EXCEL_IMPORT = "excel_import"
    API = "api"


class CostPriceAudit(Base):
    """Audit-лог изменений себестоимости."""
    __tablename__ = "cost_price_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cost_price_id: Mapped[int] = mapped_column(Integer, nullable=False)  # может ссылаться на удалённую запись
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    action: Mapped[CostPriceAuditAction] = mapped_column(Enum(CostPriceAuditAction), nullable=False)
    old_values: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    new_values: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    source: Mapped[CostPriceAuditSource] = mapped_column(
        Enum(CostPriceAuditSource), default=CostPriceAuditSource.UI
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
