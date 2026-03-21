from datetime import date, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.catalog import Channel, SKU
from app.models.sales import Order, Return, Sale
from app.schemas.sales import OrderOut, SalesDynamicPoint, SalesSummary


def get_orders(
    db: Session,
    channel_type: Optional[str] = None,
    sku_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[OrderOut]:
    query = (
        db.query(Order, SKU.seller_article, SKU.name, Channel.name.label("channel_name"))
        .join(SKU, Order.sku_id == SKU.id)
        .join(Channel, Order.channel_id == Channel.id)
    )
    if channel_type:
        query = query.filter(Channel.type == channel_type)
    if sku_id:
        query = query.filter(Order.sku_id == sku_id)
    if date_from:
        query = query.filter(Order.order_date >= date_from)
    if date_to:
        query = query.filter(Order.order_date <= date_to)

    query = query.order_by(Order.order_date.desc()).offset(offset).limit(limit)

    result = []
    for row in query.all():
        order, seller_article, sku_name, channel_name = row
        result.append(OrderOut(
            id=order.id,
            sku_id=order.sku_id,
            seller_article=seller_article,
            sku_name=sku_name,
            channel=channel_name,
            order_date=order.order_date,
            qty=order.qty,
            price=float(order.price),
            status=order.status.value,
        ))
    return result


def get_sales_summary(
    db: Session,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    channel_type: Optional[str] = None,
) -> SalesSummary:
    if not date_from:
        date_from = date.today() - timedelta(days=30)
    if not date_to:
        date_to = date.today()

    orders_q = db.query(func.sum(Order.qty), func.sum(Order.price * Order.qty)).filter(
        Order.order_date >= date_from, Order.order_date <= date_to
    )
    sales_q = db.query(func.sum(Sale.qty), func.sum(Sale.price * Sale.qty)).filter(
        Sale.sale_date >= date_from, Sale.sale_date <= date_to
    )
    returns_q = db.query(func.sum(Return.qty)).filter(
        Return.return_date >= date_from, Return.return_date <= date_to
    )

    if channel_type:
        orders_q = orders_q.join(Channel, Order.channel_id == Channel.id).filter(Channel.type == channel_type)
        sales_q = sales_q.join(Channel, Sale.channel_id == Channel.id).filter(Channel.type == channel_type)
        returns_q = returns_q.join(Channel, Return.channel_id == Channel.id).filter(Channel.type == channel_type)

    orders_qty, orders_rub = orders_q.first()
    sales_qty, sales_rub = sales_q.first()
    returns_qty = returns_q.scalar()

    orders_qty = int(orders_qty or 0)
    orders_rub = float(orders_rub or 0)
    sales_qty = int(sales_qty or 0)
    sales_rub = float(sales_rub or 0)
    returns_qty = int(returns_qty or 0)
    buyout_rate = (sales_qty / orders_qty * 100) if orders_qty > 0 else 0.0
    avg_price = (orders_rub / orders_qty) if orders_qty > 0 else 0.0

    return SalesSummary(
        date_from=date_from,
        date_to=date_to,
        total_orders_qty=orders_qty,
        total_orders_rub=round(orders_rub, 2),
        total_sales_qty=sales_qty,
        total_sales_rub=round(sales_rub, 2),
        total_returns_qty=returns_qty,
        buyout_rate_pct=round(buyout_rate, 1),
        avg_order_price=round(avg_price, 2),
    )


def get_sales_dynamic(
    db: Session,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    channel_type: Optional[str] = None,
) -> list[SalesDynamicPoint]:
    if not date_from:
        date_from = date.today() - timedelta(days=29)
    if not date_to:
        date_to = date.today()

    orders_by_day = (
        db.query(Order.order_date, func.sum(Order.qty), func.sum(Order.price * Order.qty))
        .join(Channel, Order.channel_id == Channel.id)
        .filter(Order.order_date >= date_from, Order.order_date <= date_to)
    )
    sales_by_day = (
        db.query(Sale.sale_date, func.sum(Sale.qty), func.sum(Sale.price * Sale.qty))
        .join(Channel, Sale.channel_id == Channel.id)
        .filter(Sale.sale_date >= date_from, Sale.sale_date <= date_to)
    )

    if channel_type:
        orders_by_day = orders_by_day.filter(Channel.type == channel_type)
        sales_by_day = sales_by_day.filter(Channel.type == channel_type)

    orders_map = {
        row[0]: (int(row[1] or 0), float(row[2] or 0))
        for row in orders_by_day.group_by(Order.order_date).all()
    }
    sales_map = {
        row[0]: (int(row[1] or 0), float(row[2] or 0))
        for row in sales_by_day.group_by(Sale.sale_date).all()
    }

    result = []
    current = date_from
    while current <= date_to:
        oq, orub = orders_map.get(current, (0, 0.0))
        sq, srub = sales_map.get(current, (0, 0.0))
        result.append(SalesDynamicPoint(
            date=current,
            orders_qty=oq,
            orders_rub=round(orub, 2),
            sales_qty=sq,
            sales_rub=round(srub, 2),
        ))
        current += timedelta(days=1)

    return result
