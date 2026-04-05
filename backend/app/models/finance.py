"""Финансовые модели: ОПиУ (P&L), ДДС (Cash Flow), Платёжный календарь, Управленческий баланс."""
from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, Numeric, String, Text
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


class PaymentCalendarEntry(Base):
    """Запись платёжного календаря — плановый/фактический платёж."""
    __tablename__ = "payment_calendar_entries"

    id = Column(Integer, primary_key=True)
    entry_type = Column(String(10), nullable=False)  # "inflow" / "outflow"
    category = Column(String(50), nullable=False)     # wb_payment, salary, usn, nds, rent, purchase, credit, other
    name = Column(String(200), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False, default=0)
    scheduled_date = Column(Date, nullable=False)     # когда ожидается платёж
    is_recurring = Column(Boolean, default=False)     # повторяющийся
    recurrence_rule = Column(String(20), nullable=True)  # weekly, biweekly, monthly, quarterly
    channel_id = Column(Integer, ForeignKey("channels.id"), nullable=True)
    is_auto = Column(Boolean, default=False)          # авто-рассчитанный (из API данных)
    is_confirmed = Column(Boolean, default=False)     # оплачен/получен
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class JournalEntry(Base):
    """Журнал операций — центральное место для внесения расходных/доходных/переводных операций."""
    __tablename__ = "journal_entries"

    id = Column(Integer, primary_key=True)
    entry_type = Column(String(10), nullable=False)             # 'expense' | 'income' | 'transfer'
    amount = Column(Numeric(14, 2), nullable=False)             # сумма с НДС
    nds_amount = Column(Numeric(14, 2), default=0)              # сумма НДС (отдельно)
    is_recurring = Column(Boolean, default=False)               # разовая / регулярная
    recurrence_rule = Column(String(20), nullable=True)         # 'monthly' | 'weekly'
    recurrence_day = Column(Integer, nullable=True)             # число месяца (1-28) или день недели (1-7)
    scheduled_date = Column(Date, nullable=True)                # дата операции (для разовых)
    backfill_from = Column(Date, nullable=True)                 # создать за прошлый период (опционально)
    account_name = Column(String(100), nullable=False)          # из DDSBalance.account_name
    category = Column(String(100), nullable=True)               # статья расходов/доходов
    counterparty = Column(String(200), nullable=True)           # контрагент
    description = Column(Text, nullable=True)                   # описание
    is_distributed = Column(Boolean, default=False)             # распределить расход
    is_official = Column(Boolean, default=False)                # официальный расход
    channel_id = Column(Integer, ForeignKey("channels.id"), nullable=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class BalanceSheetManualEntry(Base):
    """Ручные записи управленческого баланса."""
    __tablename__ = "balance_sheet_entries"

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    category = Column(String(50), nullable=False)     # ff_stock, charter_capital, supplier_payable, bank_loan, etc.
    name = Column(String(200), nullable=False)
    amount = Column(Numeric(14, 2), nullable=False, default=0)
    section = Column(String(20), nullable=False)      # assets, liabilities, equity
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class TaxRate(Base):
    """Налоговые ставки по периодам и каналам (УСН %, НДС %)."""
    __tablename__ = "tax_rates"

    id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=True)  # 1..12, null = годовая/квартальная ставка
    quarter = Column(Integer, nullable=True)  # 1..4, null = месячная/годовая
    channel_id = Column(Integer, ForeignKey("channels.id"), nullable=True)  # null = для всех каналов
    usn_pct = Column(Numeric(6, 2), nullable=False, default=0)  # УСН %
    nds_pct = Column(Numeric(6, 2), nullable=False, default=0)  # НДС %
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
