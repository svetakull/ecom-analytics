"""
fill_commission.py
------------------
Заполняет commission_pct_override для каждого SKU-канала WB
на основе данных из таблицы продаж за последние 30 дней.

Формула WB: (Реализация - К_перечислению) / Реализация * 100
В нашей БД: sum(Sale.commission) / sum(Sale.price * Sale.qty) * 100

Если данных продаж недостаточно (<3 записей) — используется категорийная ставка.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from datetime import date, timedelta
from sqlalchemy import func, create_engine
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.catalog import Channel, ChannelType, SKU, SKUChannel
from app.models.sales import Sale

# Категорийные ставки комиссии WB (%)
CATEGORY_RATES = {
    "Одежда":           22.5,
    "Верхняя одежда":   21.0,
    "Обувь":            12.5,
    "Аксессуары":       18.0,
}
DEFAULT_RATE = 17.0  # если категория не найдена


def compute_commission_from_sales(db: Session, sku_id: int, channel_id: int, days: int = 30) -> float | None:
    """Возвращает фактический % комиссии из данных продаж, или None если данных мало."""
    since = date.today() - timedelta(days=days)
    row = (
        db.query(
            func.sum(Sale.commission).label("total_comm"),
            func.sum(Sale.price * Sale.qty).label("total_rev"),
            func.count(Sale.id).label("cnt"),
        )
        .filter(
            Sale.sku_id == sku_id,
            Sale.channel_id == channel_id,
            Sale.sale_date >= since,
        )
        .first()
    )
    if not row or not row.cnt or row.cnt < 3:
        return None
    if not row.total_rev or float(row.total_rev) == 0:
        return None
    return round(float(row.total_comm) / float(row.total_rev) * 100, 4)


def main():
    engine = create_engine(settings.DATABASE_URL)

    with Session(engine) as db:
        wb = db.query(Channel).filter(Channel.type == ChannelType.WB).first()
        if not wb:
            print("WB channel not found")
            return

        sku_channels = (
            db.query(SKUChannel)
            .join(SKU)
            .filter(SKUChannel.channel_id == wb.id, SKUChannel.is_active == True)
            .all()
        )

        updated = 0
        for sc in sku_channels:
            sku = db.query(SKU).get(sc.sku_id)

            # Сначала пробуем посчитать из данных продаж
            pct = compute_commission_from_sales(db, sc.sku_id, wb.id, days=30)

            if pct is None:
                # Берём категорийную ставку
                pct = CATEGORY_RATES.get(sku.category if sku.category else "", DEFAULT_RATE)

            sc.commission_pct_override = pct
            print(f"  {sku.seller_article:<25} [{sku.category}]  →  {pct:.4f}%")
            updated += 1

        db.commit()
        print(f"\nОбновлено: {updated} записей sku_channels")


if __name__ == "__main__":
    main()
