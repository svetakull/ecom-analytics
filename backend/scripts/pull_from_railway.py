"""Копирует все таблицы из Railway PostgreSQL в локальную БД.
Запуск из backend контейнера: python scripts/pull_from_railway.py
"""
import os
import sys

import psycopg2
from psycopg2 import sql

RAILWAY_URL = os.environ.get("RAILWAY_DATABASE_URL")
LOCAL_URL = os.environ.get("DATABASE_URL", "postgresql://ecom:ecom_pass@db:5432/ecom_analytics")

if not RAILWAY_URL:
    print("ERROR: RAILWAY_DATABASE_URL not set")
    sys.exit(1)

# Порядок важен: сначала родительские таблицы, потом зависимые
# (foreign keys). Если зависимость нарушается — TRUNCATE RESTART IDENTITY CASCADE.
TRUNCATE_ORDER = [
    # дочерние/связующие (чистим ПЕРВЫМИ)
    "journal_entries", "payment_calendar_entries", "dds_manual_entries", "dds_balances",
    "ad_metrics", "ad_campaigns",
    "card_stats", "storage_costs", "sku_daily_expenses",
    "returns", "sales", "orders", "prices", "logistics_operations",
    "stocks", "product_batches", "sku_cost_history", "sku_channels",
    "cost_prices", "pnl_records", "analytics_settings",
    "warehouses", "skus",
    "channels", "users", "integrations",
]


def get_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_type='BASE TABLE'
        ORDER BY table_name
    """)
    return [r[0] for r in cur.fetchall()]


def get_columns(conn, table):
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ORDER BY ordinal_position
    """, (table,))
    return [r[0] for r in cur.fetchall()]


def copy_table(src_conn, dst_conn, table: str):
    src_cols = get_columns(src_conn, table)
    dst_cols = get_columns(dst_conn, table)
    common = [c for c in src_cols if c in dst_cols]
    if not common:
        return 0, "no common columns"

    cur_src = src_conn.cursor()
    cur_dst = dst_conn.cursor()

    # Выгружаем все строки пачками
    cur_src.execute(
        sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(sql.Identifier(c) for c in common),
            sql.Identifier(table),
        )
    )
    rows = cur_src.fetchall()
    if not rows:
        return 0, "empty"

    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(common))
    insert_q = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in common),
        placeholders,
    )
    cur_dst.executemany(insert_q, rows)
    dst_conn.commit()
    return len(rows), "ok"


def reset_sequences(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT 'SELECT setval(''' || sequence_name || ''', COALESCE((SELECT MAX(id) FROM ' ||
          (SELECT table_name FROM information_schema.columns c
           WHERE c.column_default LIKE '%' || s.sequence_name || '%' LIMIT 1) || '), 1));'
        FROM information_schema.sequences s
        WHERE sequence_schema='public'
    """)
    commands = [r[0] for r in cur.fetchall()]
    for cmd in commands:
        try:
            cur.execute(cmd)
        except Exception as e:
            print(f"  seq warning: {e}")
    conn.commit()


def main():
    print("Connecting to Railway...")
    src = psycopg2.connect(RAILWAY_URL)
    print("Connecting to local...")
    dst = psycopg2.connect(LOCAL_URL)

    src_tables = set(get_tables(src))
    dst_tables = set(get_tables(dst))
    common_tables = src_tables & dst_tables

    print(f"Railway tables: {len(src_tables)}, local: {len(dst_tables)}, common: {len(common_tables)}")

    # TRUNCATE локальных таблиц в порядке зависимостей
    print("\nTruncating local tables...")
    cur = dst.cursor()
    # Добавляем в конец неизвестные таблицы
    to_truncate = [t for t in TRUNCATE_ORDER if t in dst_tables]
    extra = [t for t in dst_tables if t not in set(to_truncate) and t != "alembic_version"]
    full = to_truncate + extra
    for t in full:
        try:
            cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(sql.Identifier(t)))
        except Exception as e:
            print(f"  skip {t}: {e}")
    dst.commit()
    print(f"  truncated {len(full)} tables")

    # Копируем данные в обратном порядке (родительские первыми)
    print("\nCopying data from Railway...")
    insert_order = list(reversed(TRUNCATE_ORDER))
    for t in insert_order:
        if t not in common_tables:
            continue
        try:
            n, status = copy_table(src, dst, t)
            print(f"  {t}: {n} rows ({status})")
        except Exception as e:
            print(f"  ERROR {t}: {e}")
            dst.rollback()

    # Добавляем оставшиеся таблицы
    for t in common_tables:
        if t in set(insert_order) or t == "alembic_version":
            continue
        try:
            n, status = copy_table(src, dst, t)
            print(f"  {t}: {n} rows ({status})")
        except Exception as e:
            print(f"  ERROR {t}: {e}")
            dst.rollback()

    # Сбрасываем sequences
    print("\nResetting sequences...")
    reset_sequences(dst)

    src.close()
    dst.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
