"""Финансовые модели: ОПиУ (P&L), ДДС (Cash Flow) и т.д."""
from datetime import date, datetime
from typing import Optional

from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.core.database import Base


class PnLRecord(Base):
    """
    Строка отчёта ОПиУ (ОПиУ = Отчёт о прибылях и убытках).
    period — формат "YYYY-MM" (месяц) или "YYYY" (год).
    line_item — название статьи (например "Реализация", "Логистика").
    parent_line — родительская статья (для иерархии).
    """
    __tablename__ = "pnl_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    period: Mapped[str] = mapped_column(String(20), nullable=False, index=True)  # "2026-03"
    line_item: Mapped[str] = mapped_column(String(500), nullable=False)
    parent_line: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    amount: Mapped[float] = mapped_column(Numeric(18, 2), default=0)
    pct_of_revenue: Mapped[Optional[float]] = mapped_column(Numeric(8, 4), nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class DDSManualEntry(Base):
    """Ручные записи ДДС (Cash Flow Statement)."""
    __tablename__ = "dds_manual_entries"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)  # 1st of month
    category = Column(String(50), nullable=False)
    name = Column(String(200), nullable=False)
    amount = Column(Numeric(12, 2), nullable=False, default=0)
    section = Column(String(20), nullable=False, default="operating")  # operating/investing/financing
    channel_id = Column(Integer, ForeignKey("channels.id"), nullable=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class DDSBalance(Base):
    """Остатки по счетам для ДДС."""
    __tablename__ = "dds_balances"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    account_name = Column(String(100), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False, default=0)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
