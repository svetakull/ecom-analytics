"""
One-time script: re-sync stocks for March 14 and March 15 using 90-day window.
Deletes existing wrong records for those dates and re-fetches from WB API.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from datetime import date
from sqlalchemy import func
from app.core.database import SessionLocal
from app.models.inventory import Stock
from app.models.catalog import SKU
from app.models.integration import Integration, IntegrationType
from app.services.wb_sync import sync_stocks
from app.services.wb_api import WBClient

# Step 1: Get WB token and delete wrong records
db = SessionLocal()
try:
    integ = db.query(Integration).filter(
        Integration.type == IntegrationType.WB,
        Integration.is_active == True
    ).first()
    if not integ:
        print("ERROR: No active WB integration found")
        sys.exit(1)
    token = integ.api_key
    print(f"WB token found: {token[:8]}...")

    for target_date in [date(2026, 3, 14), date(2026, 3, 15)]:
        deleted = db.query(Stock).filter(Stock.date == target_date).delete()
        print(f"Deleted {deleted} stock records for {target_date}")
    db.commit()
    print("Deleted old records. Re-syncing...")
finally:
    db.close()

# Step 2: Re-sync both dates with correct 90-day window
client = WBClient(token)

for target_date in [date(2026, 3, 14), date(2026, 3, 15)]:
    print(f"\nSyncing {target_date}...")
    db2 = SessionLocal()
    try:
        result = sync_stocks(db2, client, target_date=target_date)
        db2.commit()
        print(f"  Result: {result}")
    except Exception as e:
        db2.rollback()
        print(f"  ERROR: {e}")
    finally:
        db2.close()

# Step 3: Verify
print("\nVerifying 0705фламенко...")
db3 = SessionLocal()
try:
    sku = db3.query(SKU).filter(SKU.seller_article == '0705фламенко').first()
    if sku:
        for d in [date(2026, 3, 14), date(2026, 3, 15)]:
            total = db3.query(func.sum(Stock.qty)).filter(Stock.sku_id == sku.id, Stock.date == d).scalar() or 0
            print(f"  {d}: {total} шт")
    else:
        print("  SKU не найден")
finally:
    db3.close()
