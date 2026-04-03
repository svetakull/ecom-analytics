from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class KTRHistory(Base):
    __tablename__ = "ktr_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    date_from: Mapped[date] = mapped_column(Date, nullable=False)
    date_to: Mapped[date] = mapped_column(Date, nullable=False)
    value: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class IRPHistory(Base):
    __tablename__ = "irp_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    date_from: Mapped[date] = mapped_column(Date, nullable=False)
    date_to: Mapped[date] = mapped_column(Date, nullable=False)
    value: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)  # в процентах
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class WBNomenclatureDimensions(Base):
    __tablename__ = "wb_nomenclature_dims"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[Optional[int]] = mapped_column(ForeignKey("skus.id"), nullable=True)
    nm_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)
    length_cm: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    width_cm: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    height_cm: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    volume_liters: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    sku: Mapped[Optional["SKU"]] = relationship()  # type: ignore[name-defined]


class WBCardDimensions(Base):
    __tablename__ = "wb_card_dims"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[Optional[int]] = mapped_column(ForeignKey("skus.id"), nullable=True)
    nm_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)
    length_cm: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    width_cm: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    height_cm: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    volume_liters: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    card_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    sku: Mapped[Optional["SKU"]] = relationship()  # type: ignore[name-defined]


class WBWarehouseTariff(Base):
    __tablename__ = "wb_warehouse_tariffs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    warehouse_name: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    base_first_liter: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    base_per_liter: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class LogisticsOperation(Base):
    __tablename__ = "logistics_operations"
    __table_args__ = (
        # report_id из rrd_id финотчёта WB — уникален для каждой строки
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[Optional[int]] = mapped_column(ForeignKey("skus.id"), nullable=True)
    nm_id: Mapped[int] = mapped_column(Integer, nullable=False)
    seller_article: Mapped[str] = mapped_column(String(100), nullable=False)
    operation_type: Mapped[str] = mapped_column(String(100), nullable=False)
    warehouse: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    supply_number: Mapped[str] = mapped_column(String(100), nullable=False, default="")

    operation_date: Mapped[date] = mapped_column(Date, nullable=False)
    coef_fix_start: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    coef_fix_end: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    warehouse_coef: Mapped[float] = mapped_column(Numeric(5, 3), default=1.0)
    ktr_value: Mapped[float] = mapped_column(Numeric(5, 2), default=1.0)
    irp_value: Mapped[float] = mapped_column(Numeric(5, 2), default=0)

    base_first_liter: Mapped[float] = mapped_column(Numeric(10, 2), default=46)
    base_per_liter: Mapped[float] = mapped_column(Numeric(10, 2), default=14)

    volume_card_liters: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    volume_nomenclature_liters: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    calculated_wb_volume: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    retail_price: Mapped[float] = mapped_column(Numeric(12, 2), default=0)

    expected_logistics: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    actual_logistics: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    difference: Mapped[float] = mapped_column(Numeric(12, 2), default=0)

    operation_status: Mapped[str] = mapped_column(String(50), default="")
    dimensions_status: Mapped[str] = mapped_column(String(50), default="")
    volume_difference: Mapped[float] = mapped_column(Numeric(10, 4), default=0)

    ktr_needs_check: Mapped[bool] = mapped_column(Boolean, default=False)
    tariff_missing: Mapped[bool] = mapped_column(Boolean, default=False)

    report_id: Mapped[str] = mapped_column(String(100), nullable=True)

    sku: Mapped[Optional["SKU"]] = relationship()  # type: ignore[name-defined]
