from datetime import date, timedelta

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.catalog import Channel, SKU, SKUChannel
from app.models.inventory import ProductBatch, Stock
from app.models.sales import Order, Sale
from app.schemas.sku import SKUDetail, SKUOut


def get_skus(db: Session) -> list[SKUOut]:
    skus = db.query(SKU).filter(SKU.is_active == True).all()
    result = []
    for sku in skus:
        channels = [sc.channel.name for sc in sku.channels if sc.is_active]
        stock_qty = (
            db.query(func.sum(Stock.qty))
            .filter(Stock.sku_id == sku.id, Stock.date == date.today())
            .scalar() or 0
        )
        batch = (
            db.query(ProductBatch)
            .filter(ProductBatch.sku_id == sku.id)
            .order_by(ProductBatch.batch_date)
            .first()
        )
        avg_cogs = batch.total_cost_per_unit if batch else 0.0

        result.append(SKUOut(
            id=sku.id,
            seller_article=sku.seller_article,
            name=sku.name,
            category=sku.category,
            brand=sku.brand,
            color=sku.color,
            is_active=sku.is_active,
            channels=channels,
            total_stock=int(stock_qty),
            avg_cogs=round(avg_cogs, 2),
        ))
    return result


def get_sku_detail(db: Session, sku_id: int) -> SKUDetail:
    sku = db.query(SKU).filter(SKU.id == sku_id).first()
    if not sku:
        return None

    channels = [sc.channel.name for sc in sku.channels if sc.is_active]
    stock_qty = (
        db.query(func.sum(Stock.qty))
        .filter(Stock.sku_id == sku.id, Stock.date == date.today())
        .scalar() or 0
    )
    batches = db.query(ProductBatch).filter(ProductBatch.sku_id == sku.id).all()
    avg_cogs = batches[0].total_cost_per_unit if batches else 0.0
    last_cost = batches[-1].total_cost_per_unit if batches else 0.0

    thirty_days_ago = date.today() - timedelta(days=30)
    monthly_orders = (
        db.query(func.sum(Order.qty))
        .filter(Order.sku_id == sku.id, Order.order_date >= thirty_days_ago)
        .scalar() or 0
    )
    monthly_sales_rub = (
        db.query(func.sum(Sale.price * Sale.qty))
        .filter(Sale.sku_id == sku.id, Sale.sale_date >= thirty_days_ago)
        .scalar() or 0
    )

    avg_daily = monthly_orders / 30
    turnover = round(int(stock_qty) / avg_daily, 1) if avg_daily > 0 else 999.0

    return SKUDetail(
        id=sku.id,
        seller_article=sku.seller_article,
        name=sku.name,
        category=sku.category,
        brand=sku.brand,
        color=sku.color,
        is_active=sku.is_active,
        channels=channels,
        total_stock=int(stock_qty),
        avg_cogs=round(avg_cogs, 2),
        batches_count=len(batches),
        last_batch_cost=round(last_cost, 2),
        turnover_days=turnover,
        monthly_orders=int(monthly_orders),
        monthly_sales_rub=round(float(monthly_sales_rub), 2),
    )
