import enum
from typing import Optional

from sqlalchemy import Boolean, Enum, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship


from app.core.database import Base


class ChannelType(str, enum.Enum):
    WB = "wb"
    OZON = "ozon"
    LAMODA = "lamoda"
    SITE = "site"
    OPT = "opt"
    PVZ = "pvz"


class WarehouseType(str, enum.Enum):
    OWN = "own"
    FF = "ff"
    MP = "mp"
    TRANSIT = "transit"


class Channel(Base):
    __tablename__ = "channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    type: Mapped[ChannelType] = mapped_column(Enum(ChannelType), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    commission_pct: Mapped[float] = mapped_column(default=0.0)

    sku_channels: Mapped[list["SKUChannel"]] = relationship(back_populates="channel")


class SKU(Base):
    __tablename__ = "skus"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    seller_article: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    category: Mapped[str] = mapped_column(String(200), nullable=True)
    brand: Mapped[str] = mapped_column(String(200), nullable=True)
    color: Mapped[str] = mapped_column(String(100), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    # Рейтинг по отзывам WB (0.00–5.00). Обновляется при sync_nm_report.
    wb_rating: Mapped[Optional[float]] = mapped_column(Numeric(3, 2), nullable=True)

    channels: Mapped[list["SKUChannel"]] = relationship(back_populates="sku")
    batches: Mapped[list["ProductBatch"]] = relationship(back_populates="sku")  # type: ignore[name-defined]
    stocks: Mapped[list["Stock"]] = relationship(back_populates="sku")  # type: ignore[name-defined]
    orders: Mapped[list["Order"]] = relationship(back_populates="sku")  # type: ignore[name-defined]


class SKUChannel(Base):
    __tablename__ = "sku_channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"), nullable=False)
    mp_article: Mapped[str] = mapped_column(String(100), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    # Ручное переопределение % выкупа (0.0–1.0). None = считать из истории.
    buyout_rate_override: Mapped[Optional[float]] = mapped_column(Numeric(5, 4), nullable=True)
    # Ручная логистика (₽/ед). None = считать как среднее за неделю из Sale.logistics.
    logistics_override: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)
    # Комиссия + эквайринг % от цены до СПП (из недельного финотчёта WB).
    # Формула: (Реализация - К_перечислению) / Реализация * 100.
    # None = использовать channel.commission_pct.
    commission_pct_override: Mapped[Optional[float]] = mapped_column(Numeric(6, 4), nullable=True)
    # Прямой URL фото (для Lamoda и других каналов без CDN-формулы).
    # Для WB используется wb_photo_url(mp_article) и это поле игнорируется.
    photo_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    sku: Mapped[SKU] = relationship(back_populates="channels")
    channel: Mapped[Channel] = relationship(back_populates="sku_channels")


class Warehouse(Base):
    __tablename__ = "warehouses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    type: Mapped[WarehouseType] = mapped_column(Enum(WarehouseType), default=WarehouseType.OWN)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    stocks: Mapped[list["Stock"]] = relationship(back_populates="warehouse")  # type: ignore[name-defined]
