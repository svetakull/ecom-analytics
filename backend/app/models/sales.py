import enum
from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, DateTime, Enum, ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class OrderStatus(str, enum.Enum):
    NEW = "new"
    CONFIRMED = "confirmed"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    RETURNED = "returned"


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"), nullable=False)
    external_id: Mapped[str] = mapped_column(String(100), nullable=True)
    order_date: Mapped[date] = mapped_column(Date, nullable=False)
    qty: Mapped[int] = mapped_column(Integer, default=1)
    price: Mapped[float] = mapped_column(Numeric(12, 2), default=0)           # priceWithDisc (до СПП)
    price_after_spp: Mapped[float] = mapped_column(Numeric(12, 2), default=0)  # finishedPrice (после СПП)
    spp_pct: Mapped[float] = mapped_column(Numeric(5, 2), default=0)           # % СПП
    status: Mapped[OrderStatus] = mapped_column(Enum(OrderStatus), default=OrderStatus.NEW)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    sku: Mapped["SKU"] = relationship(back_populates="orders")  # type: ignore[name-defined]
    channel: Mapped["Channel"] = relationship()  # type: ignore[name-defined]
    sale: Mapped["Sale"] = relationship(back_populates="order", uselist=False)


class Sale(Base):
    __tablename__ = "sales"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"), nullable=False)
    external_id: Mapped[str] = mapped_column(String(100), nullable=True)
    sale_date: Mapped[date] = mapped_column(Date, nullable=False)
    qty: Mapped[int] = mapped_column(Integer, default=1)
    price: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    commission: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    logistics: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    storage: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    penalty: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    acceptance: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    other_deductions: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    compensation: Mapped[float] = mapped_column(Numeric(12, 2), default=0)

    order: Mapped["Order"] = relationship(back_populates="sale")
    sku: Mapped["SKU"] = relationship()  # type: ignore[name-defined]
    channel: Mapped["Channel"] = relationship()  # type: ignore[name-defined]
    returns: Mapped[list["Return"]] = relationship(back_populates="sale")


class Return(Base):
    __tablename__ = "returns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sale_id: Mapped[int] = mapped_column(ForeignKey("sales.id"), nullable=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"), nullable=False)
    external_id: Mapped[str] = mapped_column(String(100), nullable=True)
    return_date: Mapped[date] = mapped_column(Date, nullable=False)
    qty: Mapped[int] = mapped_column(Integer, default=1)
    reason: Mapped[str] = mapped_column(String(300), nullable=True)

    sale: Mapped["Sale"] = relationship(back_populates="returns")


class SkuDailyExpense(Base):
    """
    Ежедневные расходы МП по SKU, агрегированные из транзакций
    по operation_date (дата начисления), а не по дате отгрузки.
    Это позволяет сверять данные с отчётами маркетплейсов 1-в-1.
    """
    __tablename__ = "sku_daily_expenses"
    __table_args__ = (
        UniqueConstraint("sku_id", "channel_id", "date", name="uq_sku_daily_expense"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)

    sale_amount: Mapped[float] = mapped_column(Numeric(12, 2), default=0)       # accruals_for_sale (к перечислению)
    commission: Mapped[float] = mapped_column(Numeric(12, 2), default=0)         # sale_commission
    logistics: Mapped[float] = mapped_column(Numeric(12, 2), default=0)          # прямая + обратная логистика
    storage: Mapped[float] = mapped_column(Numeric(12, 2), default=0)            # хранение FBO/FBS
    penalty: Mapped[float] = mapped_column(Numeric(12, 2), default=0)            # штрафы
    acceptance: Mapped[float] = mapped_column(Numeric(12, 2), default=0)         # платная приёмка
    other_deductions: Mapped[float] = mapped_column(Numeric(12, 2), default=0)   # прочие удержания
    advertising: Mapped[float] = mapped_column(Numeric(12, 2), default=0)        # реклама (WB Продвижение)
    other_services: Mapped[float] = mapped_column(Numeric(12, 2), default=0)     # прочие услуги (устаревшее, для совместимости)
    subscription: Mapped[float] = mapped_column(Numeric(12, 2), default=0)       # Подписка МП (Джем и пр.)
    reviews: Mapped[float] = mapped_column(Numeric(12, 2), default=0)            # Отзывы
    compensation_wb: Mapped[float] = mapped_column(Numeric(12, 2), default=0)    # Компенсация от МП (возвраты, лояльность)
    ppvz_for_pay: Mapped[float] = mapped_column(Numeric(12, 2), default=0)      # К перечислению продавцу (ДДС)
    return_count: Mapped[int] = mapped_column(Integer, default=0)                # кол-во возвратов
    acquiring: Mapped[float] = mapped_column(Numeric(12, 2), default=0)          # эквайринг
    compensation: Mapped[float] = mapped_column(Numeric(12, 2), default=0)       # реализация = accruals_for_sale (что зачислено продавцу)
    return_amount: Mapped[float] = mapped_column(Numeric(12, 2), default=0)      # возвраты (amount)
    items_count: Mapped[int] = mapped_column(Integer, default=0)                 # кол-во единиц в транзакциях

    sku: Mapped["SKU"] = relationship()    # type: ignore[name-defined]
    channel: Mapped["Channel"] = relationship()  # type: ignore[name-defined]


class Price(Base):
    __tablename__ = "prices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"), nullable=False)
    price_before_spp: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    price_after_spp: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    spp_pct: Mapped[float] = mapped_column(Numeric(5, 2), default=0)
    date: Mapped[date] = mapped_column(Date, nullable=False)


class CardStats(Base):
    """
    Ежедневная воронка карточки товара из nm-report WB.
    open_card_count  = переходы в карточку (органика + реклама)
    add_to_cart_count = добавления в корзину
    orders_count     = заказы по данным nm-report (для сверки)
    """
    __tablename__ = "card_stats"
    __table_args__ = (UniqueConstraint("sku_id", "channel_id", "date", name="uq_card_stats_sku_ch_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sku_id: Mapped[int] = mapped_column(ForeignKey("skus.id"), nullable=False)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    open_card_count: Mapped[int] = mapped_column(Integer, default=0)
    add_to_cart_count: Mapped[int] = mapped_column(Integer, default=0)
    orders_count: Mapped[int] = mapped_column(Integer, default=0)
    # Средняя цена товара для покупателя до СПП (avgPriceRub из WB nm-report)
    avg_price_rub: Mapped[Optional[float]] = mapped_column(Numeric(12, 2), nullable=True)
