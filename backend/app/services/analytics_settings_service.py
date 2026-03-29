"""
Сервис настраиваемых порогов аналитики РнП.

При первом чтении — автоматически заполняет таблицу дефолтами.
"""

from datetime import datetime

from sqlalchemy.orm import Session

from app.models.settings import AnalyticsThreshold

# Ключ → (значение_по_умолчанию, описание)
DEFAULTS: dict[str, tuple[float, str]] = {
    # Зоны метрик
    "orders_yellow_pct":    (10.0,  "Заказы: жёлтая зона (−N% от нормы 14 дней)"),
    "orders_red_pct":       (20.0,  "Заказы: красная зона (−N% от нормы 14 дней)"),
    "buyout_yellow_pp":     (5.0,   "% выкупа: жёлтая зона (−N п.п.)"),
    "buyout_red_pp":        (10.0,  "% выкупа: красная зона (−N п.п.)"),
    "margin_yellow_pp":     (3.0,   "Маржа: жёлтая зона (−N п.п.)"),
    "margin_red_pp":        (7.0,   "Маржа: красная зона (−N п.п.)"),
    "traffic_yellow_pct":   (30.0,  "Показы/переходы: жёлтая зона (±N%)"),
    "traffic_red_pct":      (60.0,  "Показы/переходы: красная зона (±N%)"),
    "drr_yellow_pp":        (3.0,   "ДРР: жёлтая зона (+N п.п.)"),
    "drr_red_pp":           (7.0,   "ДРР: красная зона (+N п.п.)"),
    # Рекомендации: остатки
    "stock_warning_days":   (14.0,  "Остаток: триггер пополнения жёлтый (< N дней продаж)"),
    "stock_critical_days":  (7.0,   "Остаток: триггер пополнения красный (< N дней продаж)"),
    # Рекомендации: реклама
    "drr_high_pct":         (15.0,  "ДРР: порог для рекомендаций по рекламе (> N%)"),
    # Рекомендации: цена
    "buyout_high_pct":      (55.0,  "% выкупа: порог для рекомендации 'Повысить цену' (> N%)"),
}


def get_thresholds(db: Session) -> dict[str, float]:
    """
    Возвращает {key: value} для всех порогов.
    Если в БД нет каких-то ключей — создаёт с дефолтами (upsert-on-read).
    """
    existing = {t.key: float(t.value) for t in db.query(AnalyticsThreshold).all()}

    missing = set(DEFAULTS.keys()) - set(existing.keys())
    if missing:
        for key in missing:
            default_val, desc = DEFAULTS[key]
            db.add(AnalyticsThreshold(
                key=key, value=default_val, description=desc,
                updated_at=datetime.utcnow(),
            ))
            existing[key] = default_val
        db.commit()

    return existing


def get_thresholds_list(db: Session) -> list[dict]:
    """Возвращает список порогов с описаниями для UI."""
    # Убеждаемся что все дефолты на месте
    get_thresholds(db)
    rows = db.query(AnalyticsThreshold).order_by(AnalyticsThreshold.key).all()
    return [
        {"key": t.key, "value": float(t.value), "description": t.description}
        for t in rows
    ]


def update_threshold(db: Session, key: str, value: float) -> dict:
    """Обновляет один порог. Возвращает обновлённую запись."""
    if key not in DEFAULTS:
        raise ValueError(f"Неизвестный ключ: {key}")
    row = db.query(AnalyticsThreshold).filter(AnalyticsThreshold.key == key).first()
    if not row:
        # Создадим если нет
        default_val, desc = DEFAULTS[key]
        row = AnalyticsThreshold(key=key, value=value, description=desc)
        db.add(row)
    else:
        row.value = value
        row.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return {"key": row.key, "value": float(row.value), "description": row.description}


def reset_thresholds(db: Session) -> list[dict]:
    """Сбрасывает все пороги к значениям по умолчанию."""
    db.query(AnalyticsThreshold).delete()
    for key, (val, desc) in DEFAULTS.items():
        db.add(AnalyticsThreshold(
            key=key, value=val, description=desc,
            updated_at=datetime.utcnow(),
        ))
    db.commit()
    return get_thresholds_list(db)
