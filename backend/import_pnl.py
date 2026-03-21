"""
Импорт исторических данных ОПиУ из pnl.xlsx.

Формат файла (Worksheet):
  Столбец 0 (Статья): название строки ОПиУ
  Далее пары столбцов: значение + %, для каждого периода
  Периоды в row 0 (header): 2026, Март 2026, Февраль 2026, Январь 2026, 2025, ...

Запуск:
  docker-compose exec backend python import_pnl.py /path/to/pnl.xlsx
"""
import sys
import re
from datetime import date

import pandas as pd
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models.finance import PnLRecord

# Карта русских месяцев
MONTHS_RU = {
    "январь": 1, "февраль": 2, "март": 3, "апрель": 4,
    "май": 5, "июнь": 6, "июль": 7, "август": 8,
    "сентябрь": 9, "октябрь": 10, "ноябрь": 11, "декабрь": 12,
}

# Иерархия статей (→ = дочерняя)
PARENT_MAP = {
    "Себестоимость продаж": "Прямые расходы",
    "Себестоимость": "Себестоимость продаж",
    "Логистика": "Прямые расходы",
    "Возврат брака (к продавцу)": "Логистика",
    "Возврат неопознанного товара (к продавцу)": "Логистика",
    "Возврат по инициативе продавца (к продавцу)": "Логистика",
    "Возврат товара продавцу по отзыву (к продавцу)": "Логистика",
    "К клиенту при отмене": "Логистика",
    "К клиенту при продаже": "Логистика",
    "Коррекция логистики": "Логистика",
    "От клиента при возврате": "Логистика",
    "От клиента при отмене": "Логистика",
    "Комиссия": "Прямые расходы",
    "Номинальная комиссия": "Комиссия",
    "Скидка МП": "Комиссия",
    "Эквайринг": "Комиссия",
    "Штрафы": "Прямые расходы",
    "Хранение": "Прямые расходы",
    "Внутренняя реклама": "Прямые расходы",
    "Прочие удержания": "Прямые расходы",
    "Платная приемка": "Прямые расходы",
    "Компенсация": "Прямые расходы",
}


def parse_period(header: str) -> str | None:
    """Преобразует заголовок столбца в строку периода 'YYYY-MM' или 'YYYY'."""
    header = str(header).strip()
    # "Март 2026" → "2026-03"
    for ru, num in MONTHS_RU.items():
        if ru.lower() in header.lower():
            match = re.search(r"\d{4}", header)
            if match:
                return f"{match.group()}-{num:02d}"
    # "2026" или "2025"
    match = re.match(r"^(\d{4})$", header)
    if match:
        return match.group()
    return None


def import_pnl(filepath: str):
    df = pd.read_excel(filepath)
    # Строки — статьи, столбцы: Статья, 2026, %, Март 2026, %, Февраль 2026, ...
    cols = list(df.columns)

    # Строим маппинг: индекс столбца → (period_str, is_pct)
    period_cols: list[tuple[int, str, bool]] = []
    for i, col in enumerate(cols):
        if i == 0:
            continue  # Статья
        period = parse_period(col)
        if period:
            period_cols.append((i, period, False))
        elif period_cols:
            # Следующий столбец после периода = % к выручке
            prev_i, prev_period, _ = period_cols[-1]
            if i == prev_i + 1:
                period_cols.append((i, prev_period, True))

    db: Session = SessionLocal()
    try:
        # Очищаем старые записи
        db.query(PnLRecord).delete()

        inserted = 0
        for sort_order, row in enumerate(df.itertuples(index=False)):
            line_item = str(row[0]).strip() if row[0] else None
            if not line_item or line_item in ("nan", "None", ""):
                continue
            # Убираем "→ " prefix
            clean = line_item.lstrip("→ ").strip()
            parent = None
            for k, v in PARENT_MAP.items():
                if k.lower() == clean.lower():
                    parent = v
                    break

            for col_i, period, is_pct in period_cols:
                val = row[col_i] if col_i < len(row) else None
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    continue
                try:
                    val = float(val)
                except (TypeError, ValueError):
                    continue

                if is_pct:
                    # Обновляем pct у существующей записи
                    existing = db.query(PnLRecord).filter(
                        PnLRecord.period == period,
                        PnLRecord.line_item == clean,
                    ).first()
                    if existing:
                        existing.pct_of_revenue = val
                else:
                    record = PnLRecord(
                        period=period,
                        line_item=clean,
                        parent_line=parent,
                        amount=val,
                        sort_order=sort_order,
                    )
                    db.add(record)
                    inserted += 1

        db.commit()
        print(f"✅ ОПиУ импортирован: {inserted} записей по {len(set(p for _, p, _ in period_cols if not _))} периодам")
    finally:
        db.close()


if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "/app/pnl.xlsx"
    import_pnl(filepath)
