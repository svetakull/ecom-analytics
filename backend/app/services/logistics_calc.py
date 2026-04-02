"""
Калькулятор логистики WB — формулы расчёта по правилам с марта 2026.
"""
from datetime import date
from typing import Optional

from sqlalchemy.orm import Session

from app.models.logistics import KTRHistory, IRPHistory

# ── Пороговые даты ──
DATE_REVERSE_NEW = date(2026, 3, 20)   # Новая обратная логистика
DATE_IRP_START = date(2026, 3, 23)     # Добавлен ИРП + новая таблица КТР

# ── Дефолтные базовые ставки (если нет данных API) ──
DEFAULT_BASE_FIRST = 46.0
DEFAULT_BASE_PER = 14.0

# ── Справочник КТР / КРП (ИРП) по доле локализации ──
# (min%, max%, ktr_before_23_03, ktr_from_23_03, krp_irp%)
KTR_REFERENCE_TABLE = [
    (0.00, 4.99, 2.00, 2.00, 2.50),
    (5.00, 9.99, 1.95, 1.80, 2.45),
    (10.00, 14.99, 1.90, 1.75, 2.35),
    (15.00, 19.99, 1.85, 1.70, 2.30),
    (20.00, 24.99, 1.75, 1.60, 2.25),
    (25.00, 29.99, 1.65, 1.55, 2.20),
    (30.00, 34.99, 1.55, 1.50, 2.15),
    (35.00, 39.99, 1.45, 1.40, 2.10),
    (40.00, 44.99, 1.35, 1.30, 2.10),
    (45.00, 49.99, 1.25, 1.20, 2.05),
    (50.00, 54.99, 1.15, 1.10, 2.05),
    (55.00, 59.99, 1.05, 1.05, 2.00),
    (60.00, 64.99, 1.00, 1.00, 0.00),
    (65.00, 69.99, 1.00, 1.00, 0.00),
    (70.00, 74.99, 1.00, 1.00, 0.00),
    (75.00, 79.99, 0.95, 0.90, 0.00),
    (80.00, 84.99, 0.85, 0.80, 0.00),
    (85.00, 89.99, 0.75, 0.70, 0.00),
    (90.00, 94.99, 0.65, 0.60, 0.00),
    (95.00, 100.00, 0.50, 0.50, 0.00),
]

# Типы операций
DIRECT_TYPES = {"Логистика", "К клиенту при продаже", "К клиенту при отмене"}
REVERSE_TYPES = {"От клиента при возврате", "От клиента при отмене"}


def _base_logistics_cost(volume: float, base_first: float, base_per: float) -> float:
    """Базовая стоимость логистики по объёму."""
    return base_first * min(volume, 1.0) + base_per * max(volume - 1.0, 0.0)


def is_direct_operation(operation_type: str) -> bool:
    return operation_type in DIRECT_TYPES


def is_reverse_operation(operation_type: str) -> bool:
    return operation_type in REVERSE_TYPES


def calculate_expected_logistics(
    volume: float,
    warehouse_coef: float,
    ktr: float,
    irp_pct: float,
    retail_price: float,
    operation_type: str,
    operation_date: date,
    base_first: float = DEFAULT_BASE_FIRST,
    base_per: float = DEFAULT_BASE_PER,
) -> float:
    """
    Рассчитать ожидаемую стоимость логистики по формулам WB.

    Прямая логистика:
      - До 20.03.2026: base_cost × coef × KTR
      - 20.03–22.03.2026: base_cost × coef × KTR (без ИРП)
      - С 23.03.2026: base_cost × coef × KTR + price × IRP%

    Обратная логистика:
      - До 20.03.2026: 50₽
      - С 20.03.2026: base_cost (без коэф., КТР, ИРП)
    """
    base_cost = _base_logistics_cost(volume, base_first, base_per)

    if is_reverse_operation(operation_type):
        if operation_date < DATE_REVERSE_NEW:
            return 50.0
        return base_cost

    # Прямая логистика
    cost = base_cost * warehouse_coef * ktr
    if operation_date >= DATE_IRP_START and irp_pct > 0:
        cost += retail_price * (irp_pct / 100.0)
    return round(cost, 2)


def reverse_calculate_volume(
    actual_cost: float,
    warehouse_coef: float,
    ktr: float,
    irp_pct: float,
    retail_price: float,
    operation_type: str,
    operation_date: date,
    base_first: float = DEFAULT_BASE_FIRST,
    base_per: float = DEFAULT_BASE_PER,
) -> Optional[float]:
    """
    Обратный расчёт: определить объём, по которому WB фактически рассчитал логистику.
    Только для прямой логистики.
    """
    if is_reverse_operation(operation_type):
        if operation_date < DATE_REVERSE_NEW:
            return None  # фиксированная ставка, объём не определить
        # base_first * min(V,1) + base_per * max(V-1,0) = actual_cost
        if actual_cost <= base_first:
            return actual_cost / base_first if base_first > 0 else 0
        return 1.0 + (actual_cost - base_first) / base_per if base_per > 0 else 0

    # Прямая логистика — вычесть ИРП, разделить на коэф.
    cost = actual_cost
    if operation_date >= DATE_IRP_START and irp_pct > 0:
        cost -= retail_price * (irp_pct / 100.0)

    multiplier = warehouse_coef * ktr
    if multiplier <= 0:
        return None
    base_cost = cost / multiplier

    if base_cost <= base_first:
        return base_cost / base_first if base_first > 0 else 0
    return 1.0 + (base_cost - base_first) / base_per if base_per > 0 else 0


def determine_operation_status(expected: float, actual: float) -> str:
    """Статус операции по разнице ожидаемой и фактической логистики."""
    diff = expected - actual
    if abs(diff) <= 0.01:
        return "Соответствует"
    if diff > 0:
        return "Переплата"
    return "Экономия"


def determine_dimensions_status(vol_nomenclature: Optional[float], vol_card: Optional[float]) -> str:
    """Статус габаритов: сравнение объёма номенклатуры и карточки."""
    if vol_card is None or vol_card <= 0:
        return "Не заполнены"
    if vol_nomenclature is None or vol_nomenclature <= 0:
        return "Не заполнены"
    diff = float(vol_nomenclature) - float(vol_card)
    if abs(diff) <= 0.05:
        return "Соответствует"
    if diff > 0:
        return "Занижение"  # WB замерил больше → переплата
    return "Превышение"    # WB замерил меньше → экономия


def get_ktr_for_date(db: Session, operation_date: date) -> tuple[Optional[float], bool]:
    """
    Найти КТР для даты операции из истории.
    Возвращает (ktr_value, needs_check).
    needs_check=True если отчёт старше 14 недель или КТР не найден.
    """
    record = (
        db.query(KTRHistory)
        .filter(KTRHistory.date_from <= operation_date, KTRHistory.date_to >= operation_date)
        .first()
    )
    if record:
        from datetime import timedelta
        age = (date.today() - operation_date).days
        needs_check = age > 98  # 14 недель
        return float(record.value), needs_check

    # Fallback: ближайший предшествующий
    record = (
        db.query(KTRHistory)
        .filter(KTRHistory.date_to < operation_date)
        .order_by(KTRHistory.date_to.desc())
        .first()
    )
    if record:
        return float(record.value), True

    return None, True


def get_irp_for_date(db: Session, operation_date: date) -> Optional[float]:
    """
    Найти ИРП для даты операции. Применяется только при дате ≥ 23.03.2026.
    """
    if operation_date < DATE_IRP_START:
        return 0.0

    record = (
        db.query(IRPHistory)
        .filter(IRPHistory.date_from <= operation_date, IRPHistory.date_to >= operation_date)
        .first()
    )
    if record:
        return float(record.value)

    # Fallback
    record = (
        db.query(IRPHistory)
        .filter(IRPHistory.date_to < operation_date)
        .order_by(IRPHistory.date_to.desc())
        .first()
    )
    return float(record.value) if record else 0.0
