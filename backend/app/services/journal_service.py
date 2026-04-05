"""
Сервис Журнал операций — CRUD + синхронизация в ДДС и Платёжный календарь.
"""
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import distinct
from sqlalchemy.orm import Session

from app.models.finance import DDSBalance, DDSManualEntry, JournalEntry, PaymentCalendarEntry


# --- Категории ДДС (из dds_service) ---

DDS_CATEGORIES = sorted([
    # --- ДОХОДЫ ---
    {"key": "income_wb", "name": "Доход от ВБ", "section": "income"},
    {"key": "income_ozon", "name": "Доход от Озон", "section": "income"},
    {"key": "income_lamoda", "name": "Доход от Ламода", "section": "income"},
    {"key": "income_site", "name": "Доход от сайта", "section": "income"},
    {"key": "income_opt", "name": "Доход от опта", "section": "income"},
    {"key": "income_pvz", "name": "Доход от ПВЗ", "section": "income"},
    {"key": "mp_payment", "name": "Доход от МП (прочее)", "section": "income"},
    {"key": "income_deposit", "name": "Доход от депозита", "section": "income"},
    {"key": "investor_contribution", "name": "Вложения инвестора", "section": "income"},
    {"key": "credit_received", "name": "Поступления кредитных средств", "section": "income"},
    # --- РАСХОДЫ ---
    {"key": "content", "name": "Контент", "section": "expenses"},
    {"key": "external_ads", "name": "Продвижение внешнее (общее)", "section": "expenses"},
    {"key": "external_ads_smm_strategy", "name": "СММ стратегия", "section": "expenses"},
    {"key": "external_ads_personal_brand", "name": "Личный бренд", "section": "expenses"},
    {"key": "external_ads_smm_brand", "name": "СММ продвижение бренда", "section": "expenses"},
    {"key": "external_ads_shootings_brand", "name": "Съёмки для бренда", "section": "expenses"},
    {"key": "external_ads_site", "name": "Реклама сайта (Яндекс Директ)", "section": "expenses"},
    {"key": "site_delivery", "name": "Доставка покупателю (сайт)", "section": "expenses"},
    {"key": "buyout_services", "name": "Выкупы-услуги", "section": "expenses"},
    {"key": "buyout_goods", "name": "Выкупы товар", "section": "expenses"},
    {"key": "salary", "name": "ФОТ (общий)", "section": "expenses"},
    {"key": "salary_manager", "name": "ФОТ управляющий", "section": "expenses"},
    {"key": "salary_employee", "name": "ФОТ менеджер МП", "section": "expenses"},
    {"key": "salary_smm", "name": "ФОТ СММ-менеджер", "section": "expenses"},
    {"key": "salary_reels", "name": "ФОТ рилзмейкер", "section": "expenses"},
    {"key": "salary_pvz", "name": "ФОТ ПВЗ", "section": "expenses"},
    {"key": "outsource", "name": "Аутсорс (общий)", "section": "expenses"},
    {"key": "outsource_accountant", "name": "Бухгалтер", "section": "expenses"},
    {"key": "outsource_it", "name": "ИТ-программист", "section": "expenses"},
    {"key": "outsource_other", "name": "Другое (аутсорс)", "section": "expenses"},
    {"key": "warehouse", "name": "Склад", "section": "expenses"},
    {"key": "warehouse_kalmykia", "name": "Склад Калмыкия", "section": "expenses"},
    {"key": "rent_pvz", "name": "Аренда ПВЗ", "section": "expenses"},
    {"key": "pvz", "name": "Расходы ПВЗ", "section": "expenses"},
    {"key": "courier", "name": "Курьерская доставка", "section": "expenses"},
    {"key": "travel", "name": "Командировки", "section": "expenses"},
    {"key": "bank_fees", "name": "Комиссии банков", "section": "expenses"},
    {"key": "office", "name": "Офисные нужды", "section": "expenses"},
    {"key": "equipment", "name": "Оборудование", "section": "expenses"},
    {"key": "education", "name": "Обучение", "section": "expenses"},
    {"key": "subscriptions", "name": "Подписка на сервисы", "section": "expenses"},
    {"key": "new_products", "name": "Новинки", "section": "expenses"},
    # --- НАЛОГИ ---
    {"key": "usn", "name": "УСН и взнос 1%", "section": "taxes"},
    {"key": "insurance", "name": "Страховые взносы", "section": "taxes"},
    {"key": "ndfl", "name": "НДФЛ", "section": "taxes"},
    {"key": "customs", "name": "Таможенные платежи", "section": "taxes"},
    # --- АВАНСЫ (ЗАКУПКА) ---
    {"key": "purchase_china", "name": "Закупка Китай", "section": "advances"},
    {"key": "delivery_china", "name": "Доставка Китай", "section": "advances"},
    {"key": "ff", "name": "ФФ (фулфилмент)", "section": "advances"},
    {"key": "ff_storage", "name": "Хранение на ФФ", "section": "advances"},
    {"key": "delivery_mp", "name": "Доставка до МП", "section": "advances"},
    {"key": "delivery_rf", "name": "Доставка внутри РФ", "section": "expenses"},
    # --- КРЕДИТЫ ---
    {"key": "wb_deductions", "name": "Удержания ВБ", "section": "credits"},
    {"key": "bank_credit", "name": "Банковские кредиты", "section": "credits"},
    {"key": "credit_interest", "name": "% по кредитам", "section": "credits"},
    # --- ДИВИДЕНДЫ ---
    {"key": "dividend_investor", "name": "Инвестор", "section": "dividends"},
    {"key": "dividend_manager", "name": "Управляющий (дивиденды)", "section": "dividends"},
    {"key": "dividend_other", "name": "Прочее (дивиденды)", "section": "dividends"},
    # --- ПРОЧЕЕ ---
    {"key": "other", "name": "Прочее", "section": "expenses"},
], key=lambda x: x["name"])

# Маппинг section для ДДС
CATEGORY_SECTION_MAP = {cat["key"]: cat["section"] for cat in DDS_CATEGORIES}


def get_categories():
    """Список категорий ДДС."""
    return DDS_CATEGORIES


def get_accounts(db: Session):
    """Уникальные счета из JournalEntry + DDSBalance."""
    names = set()
    for r in db.query(distinct(JournalEntry.account_name)).all():
        if r[0]:
            names.add(r[0])
    for r in db.query(distinct(DDSBalance.account_name)).all():
        if r[0]:
            names.add(r[0])
    return sorted(names)


def get_journal(
    db: Session,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    entry_type: Optional[str] = None,
    category: Optional[str] = None,
    account: Optional[str] = None,
):
    """Список операций журнала с фильтрами."""
    q = db.query(JournalEntry)
    if date_from:
        q = q.filter(JournalEntry.scheduled_date >= date_from)
    if date_to:
        q = q.filter(JournalEntry.scheduled_date <= date_to)
    if entry_type:
        q = q.filter(JournalEntry.entry_type == entry_type)
    if category:
        q = q.filter(JournalEntry.category == category)
    if account:
        q = q.filter(JournalEntry.account_name == account)
    entries = q.order_by(JournalEntry.scheduled_date.desc(), JournalEntry.id.desc()).all()
    return [_serialize(e) for e in entries]


def create_entry(db: Session, data: dict, user_id: int) -> dict:
    """
    Создать операцию в журнале.
    Если is_recurring + backfill_from — создать записи за прошлый период.
    Синхронизация в ДДС и Платёжный календарь.
    """
    entry = JournalEntry(
        entry_type=data["entry_type"],
        amount=data["amount"],
        nds_amount=data.get("nds_amount", 0),
        is_recurring=data.get("is_recurring", False),
        recurrence_rule=data.get("recurrence_rule"),
        recurrence_day=data.get("recurrence_day"),
        scheduled_date=data.get("scheduled_date"),
        backfill_from=data.get("backfill_from"),
        account_name=data["account_name"],
        category=data.get("category"),
        counterparty=data.get("counterparty"),
        description=data.get("description"),
        is_distributed=data.get("is_distributed", False),
        is_official=data.get("is_official", False),
        channel_id=data.get("channel_id"),
        created_by=user_id,
    )
    db.add(entry)
    db.flush()  # получить id

    # Sync to DDS
    _sync_to_dds(db, entry)

    # Backfill: создать DDS записи за прошлый период
    if entry.is_recurring and entry.backfill_from:
        _backfill_dds(db, entry)

    # Sync to Payment Calendar (для recurring и будущих разовых)
    _sync_to_calendar(db, entry)

    db.commit()
    db.refresh(entry)
    return _serialize(entry)


def update_entry(db: Session, entry_id: int, data: dict) -> dict:
    """Обновить операцию журнала + пересинхронизировать в ДДС."""
    entry = db.query(JournalEntry).filter(JournalEntry.id == entry_id).first()
    if not entry:
        return None

    for k, v in data.items():
        if hasattr(entry, k) and v is not None:
            setattr(entry, k, v)

    db.commit()
    db.refresh(entry)

    # Пересинкать в ДДС: удалить старые связанные записи и создать заново
    ref_name = f"journal_{entry.id}"
    db.query(DDSManualEntry).filter(DDSManualEntry.name.like(f"{ref_name}%")).delete(synchronize_session=False)
    db.query(PaymentCalendarEntry).filter(PaymentCalendarEntry.name.like(f"{ref_name}%")).delete(synchronize_session=False)
    db.commit()
    _sync_to_dds(db, entry)
    db.commit()

    return _serialize(entry)


def delete_entry(db: Session, entry_id: int) -> bool:
    """Удалить операцию журнала + связанные DDS/calendar записи."""
    entry = db.query(JournalEntry).filter(JournalEntry.id == entry_id).first()
    if not entry:
        return False

    # Удалить связанные DDS записи (по name = "journal_{id}")
    ref_name = f"journal_{entry.id}"
    db.query(DDSManualEntry).filter(DDSManualEntry.name.like(f"{ref_name}%")).delete(synchronize_session=False)

    # Удалить связанные PaymentCalendar записи
    db.query(PaymentCalendarEntry).filter(PaymentCalendarEntry.name.like(f"{ref_name}%")).delete(synchronize_session=False)

    db.delete(entry)
    db.commit()
    return True


def _sync_to_dds(db: Session, entry: JournalEntry):
    """Создать DDSManualEntry из журнальной операции."""
    if not entry.scheduled_date or not entry.category:
        return

    section = _get_dds_section(entry)
    ref_name = f"journal_{entry.id}"
    description = entry.counterparty or entry.description or entry.category

    dds_entry = DDSManualEntry(
        date=entry.scheduled_date,  # реальная дата операции
        category=entry.category,
        name=ref_name,
        amount=float(entry.amount),
        section=section,
        channel_id=entry.channel_id,
        created_by=entry.created_by,
    )
    db.add(dds_entry)


def _backfill_dds(db: Session, entry: JournalEntry):
    """Создать DDS записи за прошлый период (от backfill_from до scheduled_date)."""
    if not entry.backfill_from or not entry.scheduled_date:
        return

    section = _get_dds_section(entry)
    ref_name = f"journal_{entry.id}"
    current = entry.backfill_from.replace(day=1)
    end = entry.scheduled_date.replace(day=1)

    while current < end:
        dds_entry = DDSManualEntry(
            date=current,
            category=entry.category,
            name=f"{ref_name}_bf",
            amount=float(entry.amount),
            section=section,
            channel_id=entry.channel_id,
            created_by=entry.created_by,
        )
        db.add(dds_entry)
        # next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def _sync_to_calendar(db: Session, entry: JournalEntry):
    """Создать PaymentCalendarEntry для recurring/будущих операций."""
    if not entry.scheduled_date:
        return

    today = date.today()

    # Для регулярных — создаём записи на 3 месяца вперёд
    if entry.is_recurring:
        ref_name = f"journal_{entry.id}"
        cal_entry_type = "outflow" if entry.entry_type == "expense" else "inflow"

        dates = _generate_recurring_dates(entry, months_ahead=3)
        for d in dates:
            cal = PaymentCalendarEntry(
                entry_type=cal_entry_type,
                category=entry.category or "other",
                name=ref_name,
                amount=float(entry.amount),
                scheduled_date=d,
                is_recurring=True,
                recurrence_rule=entry.recurrence_rule,
                channel_id=entry.channel_id,
                created_by=entry.created_by,
            )
            db.add(cal)
    elif entry.scheduled_date >= today:
        # Разовая будущая операция
        ref_name = f"journal_{entry.id}"
        cal_entry_type = "outflow" if entry.entry_type == "expense" else "inflow"
        cal = PaymentCalendarEntry(
            entry_type=cal_entry_type,
            category=entry.category or "other",
            name=ref_name,
            amount=float(entry.amount),
            scheduled_date=entry.scheduled_date,
            is_recurring=False,
            channel_id=entry.channel_id,
            created_by=entry.created_by,
        )
        db.add(cal)


def _generate_recurring_dates(entry: JournalEntry, months_ahead: int = 3) -> list[date]:
    """Генерация дат для регулярных операций."""
    dates = []
    today = date.today()
    start = entry.scheduled_date or today

    if entry.recurrence_rule == "monthly":
        day = entry.recurrence_day or start.day
        day = min(day, 28)  # safe day
        current = start
        for _ in range(months_ahead):
            if current.month == 12:
                current = date(current.year + 1, 1, day)
            else:
                current = date(current.year, current.month + 1, day)
            dates.append(current)
    elif entry.recurrence_rule == "weekly":
        current = start
        for _ in range(months_ahead * 4):  # ~4 weeks per month
            current = current + timedelta(weeks=1)
            if current > start + timedelta(days=months_ahead * 31):
                break
            dates.append(current)

    return dates


def _get_dds_section(entry: JournalEntry) -> str:
    """Определить section для ДДС по категории."""
    if entry.category and entry.category in CATEGORY_SECTION_MAP:
        section = CATEGORY_SECTION_MAP[entry.category]
        # Map to DDS sections
        if section in ("expenses", "taxes", "advances", "credits", "dividends"):
            return "operating"
        if section == "income":
            return "operating"
    return "operating"


def _serialize(entry: JournalEntry) -> dict:
    """Сериализация JournalEntry в dict."""
    return {
        "id": entry.id,
        "entry_type": entry.entry_type,
        "amount": float(entry.amount) if entry.amount else 0,
        "nds_amount": float(entry.nds_amount) if entry.nds_amount else 0,
        "is_recurring": entry.is_recurring,
        "recurrence_rule": entry.recurrence_rule,
        "recurrence_day": entry.recurrence_day,
        "scheduled_date": entry.scheduled_date.isoformat() if entry.scheduled_date else None,
        "backfill_from": entry.backfill_from.isoformat() if entry.backfill_from else None,
        "account_name": entry.account_name,
        "category": entry.category,
        "counterparty": entry.counterparty,
        "description": entry.description,
        "is_distributed": entry.is_distributed,
        "is_official": entry.is_official,
        "channel_id": entry.channel_id,
        "created_by": entry.created_by,
        "created_at": entry.created_at.isoformat() if entry.created_at else None,
        "updated_at": entry.updated_at.isoformat() if entry.updated_at else None,
    }
