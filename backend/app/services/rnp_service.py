from datetime import date, timedelta
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.catalog import Channel, ChannelType, SKU, SKUChannel
from app.models.inventory import ProductBatch, Stock
from app.models.sales import Order, OrderStatus, Price, Return, Sale, SkuDailyExpense
from app.models.ads import AdCampaign, AdMetrics
from app.schemas.rnp import RnPDailyResponse, RnPSKURow


def _last_order_date(db: Session) -> date:
    result = db.query(func.max(Order.order_date)).scalar()
    return result if result else date.today()


def _avg_daily_orders(db: Session, sku_id: int, channel_id: int, ref_date: date, days: int = 30) -> float:
    start = ref_date - timedelta(days=days)
    total = (
        db.query(func.sum(Order.qty))
        .filter(Order.sku_id == sku_id, Order.channel_id == channel_id)
        .filter(Order.order_date >= start, Order.order_date <= ref_date)
        .filter(Order.status != OrderStatus.CANCELLED)
        .scalar() or 0
    )
    return total / days


def _avg_buyout_rate(db: Session, sku_id: int, channel_id: int, ref_date: date, days: int = 14) -> float:
    start = ref_date - timedelta(days=days)
    orders_qty = (
        db.query(func.sum(Order.qty))
        .filter(Order.sku_id == sku_id, Order.channel_id == channel_id)
        .filter(Order.order_date >= start, Order.order_date <= ref_date)
        .filter(Order.status != OrderStatus.CANCELLED)
        .scalar() or 0
    )
    sales_qty = (
        db.query(func.sum(Sale.qty))
        .filter(Sale.sku_id == sku_id, Sale.channel_id == channel_id)
        .filter(Sale.sale_date >= start, Sale.sale_date <= ref_date)
        .scalar() or 0
    )
    if not orders_qty:
        return 0.75
    return min(sales_qty / orders_qty, 1.0)


def _cogs_per_unit(db: Session, sku_id: int) -> float:
    batch = (
        db.query(ProductBatch)
        .filter(ProductBatch.sku_id == sku_id)
        .order_by(ProductBatch.batch_date)
        .first()
    )
    return batch.total_cost_per_unit if batch else 0.0


def _current_stock(db: Session, sku_id: int) -> int:
    latest = db.query(func.max(Stock.date)).filter(Stock.sku_id == sku_id).scalar()
    if not latest:
        return 0
    total = (
        db.query(func.sum(Stock.qty))
        .filter(Stock.sku_id == sku_id, Stock.date == latest)
        .scalar() or 0
    )
    return int(total)


def _ad_spend(db: Session, sku_id: int, channel_id: int, ref_date: date, days: int = 7) -> float:
    start = ref_date - timedelta(days=days - 1)
    spend = (
        db.query(func.sum(AdMetrics.budget))
        .join(AdCampaign)
        .filter(AdCampaign.sku_id == sku_id, AdCampaign.channel_id == channel_id)
        .filter(AdMetrics.date >= start, AdMetrics.date <= ref_date)
        .scalar() or 0
    )
    return float(spend)


def get_rnp_daily(
    db: Session,
    channel_type: Optional[str] = None,
) -> RnPDailyResponse:
    # Используем последнюю дату с данными
    ref_date = _last_order_date(db)
    period_start = ref_date - timedelta(days=6)  # последние 7 дней

    query = (
        db.query(SKUChannel)
        .join(SKU)
        .join(Channel)
        .filter(SKU.is_active == True, Channel.is_active == True)
    )
    if channel_type:
        query = query.filter(Channel.type == channel_type)
    else:
        query = query.filter(Channel.type.in_(["wb", "ozon"]))

    sku_channels = query.all()

    rows: list[RnPSKURow] = []
    total_orders_qty = 0
    total_orders_rub = 0.0
    total_margin_sum = 0.0
    total_ad_spend = 0.0
    total_sales_rub = 0.0

    for sc in sku_channels:
        sku = sc.sku
        channel = sc.channel

        # Заказы за последние 7 дней (без отменённых)
        day_orders = (
            db.query(func.sum(Order.qty), func.sum(Order.price * Order.qty))
            .filter(Order.sku_id == sku.id, Order.channel_id == channel.id)
            .filter(Order.order_date >= period_start, Order.order_date <= ref_date)
            .filter(Order.status != OrderStatus.CANCELLED)
            .first()
        )
        orders_qty = int(day_orders[0] or 0)
        orders_rub = float(day_orders[1] or 0)

        avg_daily = _avg_daily_orders(db, sku.id, channel.id, ref_date, 30)
        buyout_rate = _avg_buyout_rate(db, sku.id, channel.id, ref_date, 14)
        sales_forecast_qty = round(avg_daily * 30, 1)
        cogs = _cogs_per_unit(db, sku.id)

        price_row = (
            db.query(Price)
            .filter(Price.sku_id == sku.id, Price.channel_id == channel.id)
            .order_by(Price.date.desc())
            .first()
        )
        # Если цен нет — берём из заказов
        if price_row:
            price_before = float(price_row.price_before_spp)
            price_after = float(price_row.price_after_spp)
            spp_pct = float(price_row.spp_pct)
        else:
            avg_price = (orders_rub / orders_qty) if orders_qty else 0.0
            price_before = avg_price
            price_after = avg_price
            spp_pct = 0.0

        # Для Ozon юнит-экономика от price_before_spp (цена до соинвеста),
        # для WB/Lamoda — от price_after_spp
        unit_price = price_before if channel.type == ChannelType.OZON else price_after
        sales_forecast_rub = sales_forecast_qty * unit_price

        stock_qty = _current_stock(db, sku.id)
        turnover_days = round(stock_qty / avg_daily, 1) if avg_daily > 0 else 999.0

        ad_spend_period = _ad_spend(db, sku.id, channel.id, ref_date, 7)
        tacos = (ad_spend_period / orders_rub * 100) if orders_rub > 0 else 0.0

        # ── Комиссия и логистика на единицу ────────────────────
        if channel.type == ChannelType.OZON:
            exp_row = db.query(
                func.sum(SkuDailyExpense.commission),
                func.sum(SkuDailyExpense.logistics),
                func.sum(SkuDailyExpense.storage),
                func.sum(SkuDailyExpense.items_count),
            ).filter(
                SkuDailyExpense.sku_id == sku.id,
                SkuDailyExpense.channel_id == channel.id,
                SkuDailyExpense.date >= ref_date - timedelta(days=30),
                SkuDailyExpense.date <= ref_date,
            ).first()
            exp_items = int(exp_row[3] or 0) if exp_row else 0
            if exp_items > 0:
                commission = float(exp_row[0] or 0) / exp_items
                logistics = float(exp_row[1] or 0) / exp_items
                storage = float(exp_row[2] or 0) / exp_items
            else:
                commission = unit_price * float(channel.commission_pct) / 100
                logistics = float(sc.logistics_override) if sc.logistics_override else 0.0
                storage = 0.0
        else:
            commission_pct = float(channel.commission_pct) / 100
            commission = unit_price * commission_pct
            logistics = float(sc.logistics_override) if sc.logistics_override else 0.0
            storage = 5.0

        ad_per_unit = ad_spend_period / max(orders_qty, 1)

        gross_margin_per_unit = unit_price * buyout_rate - commission - logistics - storage - ad_per_unit - cogs
        gross_margin_rub = gross_margin_per_unit * orders_qty
        margin_pct = (gross_margin_per_unit / unit_price * 100) if unit_price > 0 else 0.0

        rows.append(RnPSKURow(
            sku_id=sku.id,
            seller_article=sku.seller_article,
            name=sku.name,
            channel=channel.name,
            channel_type=channel.type.value,
            orders_qty=orders_qty,
            orders_rub=round(orders_rub, 2),
            sales_forecast_qty=sales_forecast_qty,
            sales_forecast_rub=round(sales_forecast_rub, 2),
            buyout_rate_pct=round(buyout_rate * 100, 1),
            price_before_spp=price_before,
            price_after_spp=price_after,
            spp_pct=spp_pct,
            stock_qty=stock_qty,
            turnover_days=turnover_days,
            margin_forecast_pct=round(margin_pct, 1),
            gross_margin_per_unit=round(gross_margin_per_unit, 2),
            gross_margin_rub=round(gross_margin_rub, 2),
            tacos=round(tacos, 1),
            ad_spend=round(ad_spend_period, 2),
        ))

        total_orders_qty += orders_qty
        total_orders_rub += orders_rub
        total_margin_sum += margin_pct
        total_ad_spend += ad_spend_period
        total_sales_rub += orders_rub

    avg_margin = total_margin_sum / len(rows) if rows else 0.0
    avg_tacos = (total_ad_spend / total_sales_rub * 100) if total_sales_rub > 0 else 0.0

    return RnPDailyResponse(
        date=ref_date,
        total_orders_qty=total_orders_qty,
        total_orders_rub=round(total_orders_rub, 2),
        total_margin_pct=round(avg_margin, 1),
        total_tacos=round(avg_tacos, 1),
        rows=rows,
    )
