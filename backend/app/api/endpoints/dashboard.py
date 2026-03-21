from datetime import date, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.catalog import Channel, SKU
from app.models.inventory import Stock
from app.models.sales import Order, Sale
from app.models.ads import AdCampaign, AdMetrics
from app.schemas.dashboard import DashboardResponse, KPICard, StockAlert
from app.services.sales_service import get_sales_dynamic

router = APIRouter()


def _last_date_with_data(db) -> date:
    """Последняя дата, на которую есть заказы."""
    result = db.query(func.max(Order.order_date)).scalar()
    return result if result else date.today()


@router.get("/owner", response_model=DashboardResponse)
def dashboard_owner(
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    # Берём последнюю дату с данными (не обязательно сегодня)
    last_date = _last_date_with_data(db)
    prev_date = last_date - timedelta(days=1)
    week_start = last_date - timedelta(days=6)
    month_start = last_date - timedelta(days=29)

    def _orders_period(d_from, d_to):
        q, r = db.query(
            func.sum(Order.qty),
            func.sum(Order.price * Order.qty)
        ).filter(Order.order_date >= d_from, Order.order_date <= d_to).first()
        return int(q or 0), float(r or 0)

    # Последний день vs предыдущий
    oq_last, orub_last = _orders_period(last_date, last_date)
    oq_prev, orub_prev = _orders_period(prev_date, prev_date)

    # 7 дней и предыдущие 7 дней для тренда
    oq_7d, orub_7d = _orders_period(week_start, last_date)
    oq_7d_prev, orub_7d_prev = _orders_period(week_start - timedelta(days=7), week_start - timedelta(days=1))

    # Маржа за 7 дней (упрощённо: выручка - комиссия WB 16.5% - логистика - себестоимость ~40%)
    sales_7d_rub = db.query(func.sum(Sale.price * Sale.qty)).filter(
        Sale.sale_date >= week_start, Sale.sale_date <= last_date
    ).scalar() or 0
    commission_7d = db.query(func.sum(Sale.commission)).filter(
        Sale.sale_date >= week_start, Sale.sale_date <= last_date
    ).scalar() or 0
    logistics_7d = db.query(func.sum(Sale.logistics)).filter(
        Sale.sale_date >= week_start, Sale.sale_date <= last_date
    ).scalar() or 0
    gross_7d = float(sales_7d_rub) - float(commission_7d) - float(logistics_7d)
    margin_pct = (gross_7d / float(sales_7d_rub) * 100) if sales_7d_rub else 0

    # TACoS
    ad_spend_7d = db.query(func.sum(AdMetrics.budget)).filter(
        AdMetrics.date >= week_start, AdMetrics.date <= last_date
    ).scalar() or 0
    tacos_7d = (float(ad_spend_7d) / float(orub_7d) * 100) if orub_7d else 0

    def _trend(new, old):
        if not old:
            return 0.0, "flat"
        pct = (new - old) / old * 100
        return round(pct, 1), "up" if pct > 0 else "down"

    oq_trend, oq_dir = _trend(oq_7d, oq_7d_prev)
    or_trend, or_dir = _trend(orub_7d, orub_7d_prev)

    # Stock alerts — берём актуальные остатки
    latest_stock_date = db.query(func.max(Stock.date)).scalar() or last_date
    all_stocks = (
        db.query(Stock, SKU)
        .join(SKU, Stock.sku_id == SKU.id)
        .filter(Stock.date == latest_stock_date, Stock.qty > 0)
        .all()
    )
    alerts = []
    for stock, sku in all_stocks:
        avg_daily = (
            db.query(func.sum(Order.qty))
            .filter(Order.sku_id == sku.id, Order.order_date >= month_start)
            .scalar() or 0
        ) / 30
        if avg_daily > 0:
            turnover = stock.qty / avg_daily
            if turnover < 15:
                alerts.append(StockAlert(
                    sku_id=sku.id,
                    seller_article=sku.seller_article,
                    name=sku.name,
                    channel="WB",
                    stock_qty=stock.qty,
                    turnover_days=round(turnover, 1),
                ))

    sales_chart = [
        {"date": str(p.date), "orders_qty": p.orders_qty, "orders_rub": p.orders_rub}
        for p in get_sales_dynamic(db, month_start, last_date)
    ]

    # Заголовки карточек с реальными датами
    period_label = f"{week_start.strftime('%d.%m')}–{last_date.strftime('%d.%m')}"

    return DashboardResponse(
        orders_today=KPICard(
            title=f"Заказы за 7 дней",
            value=oq_7d, unit="шт",
            trend_pct=oq_trend, trend_direction=oq_dir
        ),
        revenue_today=KPICard(
            title=f"Выручка за 7 дней",
            value=round(float(orub_7d), 2), unit="₽",
            trend_pct=or_trend, trend_direction=or_dir
        ),
        margin_avg=KPICard(
            title=f"Маржа {period_label}",
            value=round(margin_pct, 1), unit="%",
            trend_pct=0, trend_direction="flat"
        ),
        tacos_avg=KPICard(
            title=f"TACoS {period_label}",
            value=round(tacos_7d, 1), unit="%",
            trend_pct=0, trend_direction="flat"
        ),
        stock_alerts=sorted(alerts, key=lambda x: x.turnover_days)[:10],
        sales_chart=sales_chart,
    )
