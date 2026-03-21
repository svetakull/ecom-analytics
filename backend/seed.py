"""
Seed script for mock data.
Run inside Docker: docker-compose exec backend python seed.py
Or locally: DATABASE_URL=... python seed.py
"""
import random
from datetime import date, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import settings
from app.core.database import Base
from app.core.security import get_password_hash
from app.models.ads import AdCampaign, AdMetrics, AdType
from app.models.catalog import Channel, ChannelType, SKU, SKUChannel, Warehouse, WarehouseType
from app.models.integration import Integration, IntegrationType, IntegrationStatus
from app.models.inventory import ProductBatch, Stock
from app.models.sales import Order, OrderStatus, Price, Return, Sale
from app.models.user import AuditLog, User, UserRole

engine = create_engine(settings.DATABASE_URL)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
db = Session()

rnd = random.Random(42)


def run():
    print("Seeding database...")

    # ── Users ──────────────────────────────────────────────────────────────────
    users_data = [
        ("owner@ecom.ru", "Иван Петров", UserRole.OWNER),
        ("finance@ecom.ru", "Мария Иванова", UserRole.FINANCE_MANAGER),
        ("marketer@ecom.ru", "Алексей Сидоров", UserRole.MARKETER),
        ("mp@ecom.ru", "Наталья Козлова", UserRole.MP_MANAGER),
        ("warehouse@ecom.ru", "Дмитрий Новиков", UserRole.WAREHOUSE),
    ]
    users = {}
    for email, name, role in users_data:
        existing = db.query(User).filter(User.email == email).first()
        if not existing:
            u = User(email=email, name=name, password_hash=get_password_hash("demo1234"), role=role)
            db.add(u)
            users[role] = u
        else:
            users[role] = existing
    db.flush()
    print(f"  Users: {len(users_data)}")

    # ── Channels ───────────────────────────────────────────────────────────────
    channels_data = [
        ("Wildberries", ChannelType.WB, 16.5),
        ("Ozon", ChannelType.OZON, 15.0),
        ("Сайт", ChannelType.SITE, 2.0),
    ]
    channels = {}
    for name, ctype, comm in channels_data:
        existing = db.query(Channel).filter(Channel.name == name).first()
        if not existing:
            c = Channel(name=name, type=ctype, commission_pct=comm)
            db.add(c)
            channels[ctype] = c
        else:
            channels[ctype] = existing
    db.flush()

    wb = channels[ChannelType.WB]
    ozon = channels[ChannelType.OZON]
    site = channels[ChannelType.SITE]
    print(f"  Channels: {len(channels_data)}")

    # ── Warehouses ─────────────────────────────────────────────────────────────
    wh_data = [
        ("Краснодар", WarehouseType.OWN),
        ("Калмыкия", WarehouseType.OWN),
        ("ФФ Москва", WarehouseType.FF),
        ("WB Подольск", WarehouseType.MP),
        ("Ozon Хоругвино", WarehouseType.MP),
    ]
    warehouses = []
    for name, wtype in wh_data:
        existing = db.query(Warehouse).filter(Warehouse.name == name).first()
        if not existing:
            w = Warehouse(name=name, type=wtype)
            db.add(w)
            warehouses.append(w)
        else:
            warehouses.append(existing)
    db.flush()
    wh_krd, wh_kalm, wh_ff, wh_wb, wh_ozon = warehouses
    print(f"  Warehouses: {len(warehouses)}")

    # ── SKUs ───────────────────────────────────────────────────────────────────
    sku_catalog = [
        ("HOODIE-BLK-M", "Худи черное M", "Одежда", "BrandX", "Черный"),
        ("HOODIE-BLK-L", "Худи черное L", "Одежда", "BrandX", "Черный"),
        ("HOODIE-WHT-M", "Худи белое M", "Одежда", "BrandX", "Белый"),
        ("HOODIE-WHT-L", "Худи белое L", "Одежда", "BrandX", "Белый"),
        ("TSHIRT-RED-M", "Футболка красная M", "Одежда", "BrandX", "Красный"),
        ("TSHIRT-RED-L", "Футболка красная L", "Одежда", "BrandX", "Красный"),
        ("TSHIRT-BLU-M", "Футболка синяя M", "Одежда", "BrandX", "Синий"),
        ("PANTS-BLK-32", "Брюки черные 32", "Одежда", "BrandY", "Черный"),
        ("PANTS-BLK-34", "Брюки черные 34", "Одежда", "BrandY", "Черный"),
        ("PANTS-GRY-32", "Брюки серые 32", "Одежда", "BrandY", "Серый"),
        ("JACKET-BLK-M", "Куртка черная M", "Верхняя одежда", "BrandZ", "Черный"),
        ("JACKET-BLK-L", "Куртка черная L", "Верхняя одежда", "BrandZ", "Черный"),
        ("JACKET-GRN-M", "Куртка зеленая M", "Верхняя одежда", "BrandZ", "Зеленый"),
        ("DRESS-PNK-S", "Платье розовое S", "Одежда", "BrandW", "Розовый"),
        ("DRESS-PNK-M", "Платье розовое M", "Одежда", "BrandW", "Розовый"),
        ("SNEAKER-WHT-42", "Кроссовки белые 42", "Обувь", "FootBrand", "Белый"),
        ("SNEAKER-WHT-43", "Кроссовки белые 43", "Обувь", "FootBrand", "Белый"),
        ("BAG-BRN-L", "Сумка коричневая L", "Аксессуары", "BagCo", "Коричневый"),
        ("CAP-BLK", "Кепка черная", "Аксессуары", "CapBrand", "Черный"),
        ("SCARF-RED", "Шарф красный", "Аксессуары", "AccessCo", "Красный"),
    ]

    skus = []
    for article, name, cat, brand, color in sku_catalog:
        existing = db.query(SKU).filter(SKU.seller_article == article).first()
        if not existing:
            s = SKU(seller_article=article, name=name, category=cat, brand=brand, color=color)
            db.add(s)
            skus.append(s)
        else:
            skus.append(existing)
    db.flush()
    print(f"  SKUs: {len(skus)}")

    # ── SKUChannels ────────────────────────────────────────────────────────────
    existing_sc = db.query(SKUChannel).count()
    if existing_sc == 0:
        for i, sku in enumerate(skus):
            # Все SKU на WB
            db.add(SKUChannel(sku_id=sku.id, channel_id=wb.id, mp_article=f"WB-{sku.seller_article}"))
            # 14 из 20 на Ozon
            if i < 14:
                db.add(SKUChannel(sku_id=sku.id, channel_id=ozon.id, mp_article=f"OZ-{sku.seller_article}"))
        db.flush()
    print(f"  SKUChannels created")

    # ── Product Batches ────────────────────────────────────────────────────────
    existing_batches = db.query(ProductBatch).count()
    if existing_batches == 0:
        for sku in skus:
            base_cost = rnd.uniform(400, 1500)
            for batch_num in range(rnd.randint(1, 3)):
                batch_date = date.today() - timedelta(days=rnd.randint(60, 180))
                qty = rnd.randint(50, 300)
                db.add(ProductBatch(
                    sku_id=sku.id,
                    batch_date=batch_date,
                    factory=f"Factory-{rnd.randint(1, 5)}",
                    qty=qty,
                    purchase_cost=base_cost * qty * rnd.uniform(0.9, 1.1),
                    china_logistics=rnd.uniform(50, 200) * qty,
                    duties=rnd.uniform(30, 100) * qty,
                    delivery_to_warehouse=rnd.uniform(20, 80) * qty,
                    packaging=rnd.uniform(10, 30) * qty,
                    fulfillment=rnd.uniform(15, 50) * qty,
                    delivery_to_mp=rnd.uniform(20, 60) * qty,
                    storage_cost=rnd.uniform(5, 20) * qty,
                    other_costs=rnd.uniform(5, 30) * qty,
                    arrived_at=batch_date + timedelta(days=rnd.randint(30, 50)),
                ))
        db.flush()
    print(f"  Product Batches created")

    # ── Prices ─────────────────────────────────────────────────────────────────
    existing_prices = db.query(Price).count()
    if existing_prices == 0:
        for sku in skus:
            for ch in [wb, ozon]:
                base_price = rnd.uniform(1500, 8000)
                spp = rnd.uniform(5, 25)
                db.add(Price(
                    sku_id=sku.id,
                    channel_id=ch.id,
                    price_before_spp=round(base_price, 2),
                    price_after_spp=round(base_price * (1 - spp / 100), 2),
                    spp_pct=round(spp, 1),
                    date=date.today(),
                ))
        db.flush()
    print(f"  Prices created")

    # ── Stock ──────────────────────────────────────────────────────────────────
    existing_stocks = db.query(Stock).count()
    if existing_stocks == 0:
        today = date.today()
        for sku in skus:
            # Разные склады
            for wh in [wh_wb, wh_ozon, wh_krd]:
                qty = rnd.randint(0, 200)
                for days_back in range(0, 30):
                    d = today - timedelta(days=days_back)
                    variation = rnd.randint(-5, 5)
                    stock_qty = max(0, qty + variation * days_back)
                    db.add(Stock(sku_id=sku.id, warehouse_id=wh.id, qty=stock_qty, date=d))
        db.flush()
    print(f"  Stocks created")

    # ── Orders & Sales ─────────────────────────────────────────────────────────
    existing_orders = db.query(Order).count()
    if existing_orders == 0:
        today = date.today()
        for sku in skus:
            channels_for_sku = [wb] + ([ozon] if skus.index(sku) < 14 else [])
            for ch in channels_for_sku:
                price_row = (
                    db.query(Price)
                    .filter(Price.sku_id == sku.id, Price.channel_id == ch.id)
                    .first()
                )
                base_price = float(price_row.price_after_spp) if price_row else 2000.0
                commission_pct = ch.commission_pct / 100

                for days_back in range(90):
                    d = today - timedelta(days=days_back)
                    # Случайные заказы: 0-15 в день
                    num_orders = rnd.randint(0, 15)
                    for _ in range(num_orders):
                        price_variation = base_price * rnd.uniform(0.95, 1.05)
                        order = Order(
                            sku_id=sku.id,
                            channel_id=ch.id,
                            order_date=d,
                            qty=1,
                            price=round(price_variation, 2),
                            status=OrderStatus.DELIVERED if days_back > 14 else OrderStatus.CONFIRMED,
                        )
                        db.add(order)
                        db.flush()

                        # 75% конверсия в продажу
                        if rnd.random() < 0.75 and days_back > 7:
                            sale_date = d + timedelta(days=rnd.randint(5, 12))
                            if sale_date <= today:
                                commission = price_variation * commission_pct
                                sale = Sale(
                                    order_id=order.id,
                                    sku_id=sku.id,
                                    channel_id=ch.id,
                                    sale_date=sale_date,
                                    qty=1,
                                    price=round(price_variation, 2),
                                    commission=round(commission, 2),
                                    logistics=rnd.uniform(60, 120),
                                    storage=rnd.uniform(3, 10),
                                )
                                db.add(sale)
                                db.flush()

                                # 10% возвратов
                                if rnd.random() < 0.10:
                                    ret_date = sale_date + timedelta(days=rnd.randint(3, 14))
                                    if ret_date <= today:
                                        db.add(Return(
                                            sale_id=sale.id,
                                            sku_id=sku.id,
                                            channel_id=ch.id,
                                            return_date=ret_date,
                                            qty=1,
                                            reason="Не подошёл размер",
                                        ))
        db.flush()
    print(f"  Orders & Sales created")

    # ── Ad Campaigns & Metrics ─────────────────────────────────────────────────
    existing_camps = db.query(AdCampaign).count()
    if existing_camps == 0:
        today = date.today()
        for sku in skus[:14]:  # Реклама для первых 14 SKU
            for ch in [wb, ozon]:
                for ad_type in [AdType.SEARCH, AdType.RECOMMEND]:
                    camp = AdCampaign(
                        sku_id=sku.id,
                        channel_id=ch.id,
                        name=f"{sku.seller_article} {ch.name} {ad_type.value}",
                        type=ad_type,
                    )
                    db.add(camp)
                    db.flush()

                    for days_back in range(30):
                        d = today - timedelta(days=days_back)
                        budget = rnd.uniform(100, 2000)
                        impressions = int(budget * rnd.uniform(100, 500))
                        clicks = int(impressions * rnd.uniform(0.01, 0.05))
                        ctr = clicks / impressions if impressions > 0 else 0
                        cpc = budget / clicks if clicks > 0 else 0
                        cpm = budget / impressions * 1000 if impressions > 0 else 0
                        orders = int(clicks * rnd.uniform(0.02, 0.10))
                        db.add(AdMetrics(
                            campaign_id=camp.id,
                            date=d,
                            budget=round(budget, 2),
                            impressions=impressions,
                            clicks=clicks,
                            ctr=round(ctr, 4),
                            cpc=round(cpc, 2),
                            cpm=round(cpm, 2),
                            orders=orders,
                            order_cost=round(budget / orders, 2) if orders > 0 else 0,
                            sale_cost=round(budget / max(orders * 0.75, 1), 2),
                        ))
        db.flush()
    print(f"  Ad Campaigns & Metrics created")

    # ── Integrations ──────────────────────────────────────────────────────────
    # Создаём запись WB-интеграции если её нет.
    # api_key НЕ перезаписывается — если уже заполнен, оставляем как есть.
    wb_integration = db.query(Integration).filter(Integration.type == IntegrationType.WB).first()
    if not wb_integration:
        wb_integration = Integration(
            type=IntegrationType.WB,
            name="Wildberries",
            api_key="",  # задаётся через UI настроек
            is_active=True,
            status=IntegrationStatus.INACTIVE,
        )
        db.add(wb_integration)
        print("  WB Integration created (api_key пустой — заполните в настройках)")
    else:
        key_status = f"ключ установлен ({len(wb_integration.api_key)} символов)" if wb_integration.api_key else "ключ НЕ задан"
        print(f"  WB Integration exists — {key_status}")

    db.commit()
    print("\n✓ Seed complete!")
    print("\nTest accounts:")
    print("  owner@ecom.ru      / demo1234  (Собственник)")
    print("  finance@ecom.ru    / demo1234  (Финансовый менеджер)")
    print("  marketer@ecom.ru   / demo1234  (Маркетолог)")
    print("  mp@ecom.ru         / demo1234  (Менеджер МП)")
    print("  warehouse@ecom.ru  / demo1234  (Склад)")


if __name__ == "__main__":
    run()
