import enum
from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Enum, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class AdType(str, enum.Enum):
    SEARCH = "search"
    RECOMMEND = "recommend"


class AdCampaign(Base):
    __tablename__ = "ad_campaigns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=True)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(300), nullable=False)
    type: Mapped[AdType] = mapped_column(Enum(AdType), default=AdType.SEARCH)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    metrics: Mapped[list["AdMetrics"]] = relationship(back_populates="campaign")


class AdMetrics(Base):
    __tablename__ = "ad_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    campaign_id: Mapped[int] = mapped_column(ForeignKey("ad_campaigns.id"), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    # Итоговые метрики (сумма всех размещений)
    budget: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    impressions: Mapped[int] = mapped_column(Integer, default=0)
    clicks: Mapped[int] = mapped_column(Integer, default=0)
    ctr: Mapped[float] = mapped_column(Numeric(8, 4), default=0)
    cpc: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    cpm: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    orders: Mapped[int] = mapped_column(Integer, default=0)
    order_cost: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    sale_cost: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    # Разбивка по размещению: Поиск (appType=1) vs Полки (appType=32/64/128/...)
    search_budget: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    search_impressions: Mapped[int] = mapped_column(Integer, default=0)
    search_clicks: Mapped[int] = mapped_column(Integer, default=0)
    search_orders: Mapped[int] = mapped_column(Integer, default=0)
    recommend_budget: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    recommend_impressions: Mapped[int] = mapped_column(Integer, default=0)
    recommend_clicks: Mapped[int] = mapped_column(Integer, default=0)
    recommend_orders: Mapped[int] = mapped_column(Integer, default=0)

    campaign: Mapped[AdCampaign] = relationship(back_populates="metrics")
