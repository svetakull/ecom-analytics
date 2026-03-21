"""
Импорт себестоимостей из cost.xlsx в базу данных.

Формат файла:
  Столбец 0 (Дата/Артикул продавца): seller_article
  Столбец 4 (Себестоимость): cost_per_unit

Запуск:
  docker-compose exec backend python import_costs.py /path/to/cost.xlsx [--date 2026-01-01]

По умолчанию effective_from = дата запуска скрипта.
"""
import sys
from datetime import date

import pandas as pd
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models.catalog import SKU
from app.models.inventory import SKUCostHistory


def import_costs(filepath: str, effective_from: date = None):
    if effective_from is None:
        effective_from = date(2020, 1, 1)  # Применяется ко всей истории

    df = pd.read_excel(filepath, header=None)
    # Строка 0 — заголовки, строки 1+ — данные
    data_rows = df.iloc[1:].copy()
    data_rows.columns = range(len(data_rows.columns))

    # Колонки: 0=seller_article, 4=cost
    data_rows = data_rows[[0, 4]].copy()
    data_rows.columns = ["seller_article", "cost"]
    data_rows = data_rows.dropna(subset=["seller_article"])
    data_rows["cost"] = pd.to_numeric(data_rows["cost"], errors="coerce").fillna(0)
    data_rows = data_rows[data_rows["cost"] > 0]

    db: Session = SessionLocal()
    try:
        skus = {s.seller_article: s.id for s in db.query(SKU.seller_article, SKU.id).all()}
        inserted = 0
        skipped = 0

        for _, row in data_rows.iterrows():
            article = str(row["seller_article"]).strip()
            cost = float(row["cost"])
            sku_id = skus.get(article)
            if not sku_id:
                print(f"  ⚠ артикул не найден: {article}")
                skipped += 1
                continue

            # Проверяем, нет ли уже записи на эту дату
            existing = db.query(SKUCostHistory).filter(
                SKUCostHistory.sku_id == sku_id,
                SKUCostHistory.effective_from == effective_from,
            ).first()
            if existing:
                existing.cost_per_unit = cost
                existing.comment = f"Обновлено из {filepath}"
            else:
                db.add(SKUCostHistory(
                    sku_id=sku_id,
                    effective_from=effective_from,
                    cost_per_unit=cost,
                    comment=f"Импортировано из {filepath}",
                ))
            inserted += 1

        db.commit()
        print(f"✅ Импорт завершён: {inserted} записей, {skipped} артикулов не найдено")
    finally:
        db.close()


if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "/app/cost.xlsx"
    eff_date = None
    if "--date" in sys.argv:
        idx = sys.argv.index("--date")
        eff_date = date.fromisoformat(sys.argv[idx + 1])
    import_costs(filepath, eff_date)
