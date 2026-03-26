from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class ProductBatch(Base):
    __tablename__ = "product_batches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False)
    batch_date: Mapped[date] = mapped_column(Date, nullable=False)
    factory: Mapped[str] = mapped_column(String(300), nullable=True)
    qty: Mapped[int] = mapped_column(Integer, nullable=False)
    purchase_cost: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    china_logistics: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    duties: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    delivery_to_warehouse: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    packaging: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    fulfillment: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    delivery_to_mp: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    storage_cost: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    other_costs: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    arrived_at: Mapped[date] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    sku: Mapped["SKU"] = relationship(back_populates="batches")  # type: ignore[name-defined]

    @property
    def total_cost_per_unit(self) -> float:
        if not self.qty:
            return 0.0
        total = (
            float(self.purchase_cost)
            + float(self.china_logistics)
            + float(self.duties)
            + float(self.delivery_to_warehouse)
            + float(self.packaging)
            + float(self.fulfillment)
            + float(self.delivery_to_mp)
            + float(self.storage_cost)
            + float(self.other_costs)
        )
        return round(total / self.qty, 2)


class Stock(Base):
    __tablename__ = "stocks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False)
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"), nullable=False)
    qty: Mapped[int] = mapped_column(Integer, default=0)
    in_way_to_client: Mapped[int] = mapped_column(Integer, default=0)    # В пути до получателей
    in_way_from_client: Mapped[int] = mapped_column(Integer, default=0)  # В пути возвраты на склад WB
    date: Mapped[date] = mapped_column(Date, nullable=False)

    sku: Mapped["SKU"] = relationship(back_populates="stocks")  # type: ignore[name-defined]
    warehouse: Mapped["Warehouse"] = relationship(back_populates="stocks")  # type: ignore[name-defined]


class SKUCostHistory(Base):
    """История себестоимости артикула: позволяет привязать себестоимость к дате изменения."""
    __tablename__ = "sku_cost_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False, index=True)
    effective_from: Mapped[date] = mapped_column(Date, nullable=False)  # с какой даты действует
    cost_per_unit: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    comment: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    sku: Mapped["SKU"] = relationship()  # type: ignore[name-defined]


class StorageCost(Base):
    """Платное хранение WB: ежедневные расходы по артикулу × склад."""
    __tablename__ = "storage_costs"
    __table_args__ = (
        UniqueConstraint("sku_id", "date", "warehouse_name", name="uq_storage_cost_sku_date_wh"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    warehouse_name: Mapped[str] = mapped_column(String(200), nullable=True)
    cost: Mapped[float] = mapped_column(Numeric(12, 4), default=0)  # ₽ за этот день по этому складу
    qty_on_warehouse: Mapped[int] = mapped_column(Integer, default=0)  # кол-во единиц на складе в этот день

    sku: Mapped["SKU"] = relationship()  # type: ignore[name-defined]
