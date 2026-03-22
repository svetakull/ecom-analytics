"""
Планировщик фоновых задач (APScheduler).
Запускается вместе с FastAPI через lifespan.

Расписание:
  Остатки:  WB 00:05, Lamoda 00:10, Ozon 00:15 — INSERT-only (не перезаписываем).
  Основные: 08:00 МСК — цены, заказы, продажи, реклама и т.д.
            Если данные не пришли — retry каждые 20 мин до 23:40.
"""
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

logger = logging.getLogger(__name__)

MSK = ZoneInfo("Europe/Moscow")

scheduler = AsyncIOScheduler(timezone=str(MSK))


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _wb_integration(db):
    """Возвращает активную WB-интеграцию или None."""
    from app.models.integration import Integration, IntegrationType
    return (
        db.query(Integration)
        .filter(Integration.type == IntegrationType.WB, Integration.is_active == True)
        .first()
    )


def _ozon_integration(db):
    from app.models.integration import Integration, IntegrationType
    return (
        db.query(Integration)
        .filter(Integration.type == IntegrationType.OZON, Integration.is_active == True)
        .first()
    )


def _lamoda_integration(db):
    from app.models.integration import Integration, IntegrationType
    return (
        db.query(Integration)
        .filter(Integration.type == IntegrationType.LAMODA, Integration.is_active == True)
        .first()
    )


def _stocks_exist(db, channel_type, target_date) -> bool:
    """Проверяет, есть ли хотя бы одна запись Stock за дату для данного канала."""
    from app.models.catalog import Channel, ChannelType
    from app.models.inventory import Stock
    from app.models.catalog import SKUChannel

    channel = db.query(Channel).filter(Channel.type == channel_type).first()
    if not channel:
        return False

    return (
        db.query(Stock)
        .join(SKUChannel, SKUChannel.sku_id == Stock.sku_id)
        .filter(SKUChannel.channel_id == channel.id, Stock.date == target_date)
        .first()
    ) is not None


# ═══════════════════════════════════════════════════════════════════════════════
# WB — остатки 00:05 (INSERT-only, не перезаписываем)
# ═══════════════════════════════════════════════════════════════════════════════

def _job_sync_stocks() -> None:
    """Ежедневная выгрузка остатков WB за предыдущий день в 00:05 МСК."""
    from app.core.database import SessionLocal
    from app.services.wb_api import WBClient, WBApiError
    from app.services.wb_sync import sync_stocks

    db = SessionLocal()
    try:
        integration = _wb_integration(db)
        if not integration:
            logger.warning("scheduler: WB integration not found, skipping stock sync")
            return

        client = WBClient(integration.api_key)
        result = sync_stocks(db, client)
        logger.info("scheduler: WB stock sync done — %s", result)
    except WBApiError as e:
        logger.error("scheduler: WB API error during stock sync — %s", e)
    except Exception as e:
        logger.exception("scheduler: unexpected error during stock sync — %s", e)
    finally:
        db.close()


def _job_sync_logistics() -> None:
    """
    Еженедельный расчёт логистики на единицу из финотчёта WB.
    Первый запуск — пн 13:00 МСК. Если отчёт ещё не готов (total_raw == 0),
    повторяет каждый час до получения данных, затем возвращается к еженедельному расписанию.
    """
    from app.core.database import SessionLocal
    from app.services.wb_api import WBClient, WBApiError
    from app.services.wb_sync import sync_logistics_weekly

    db = SessionLocal()
    try:
        integration = _wb_integration(db)
        if not integration:
            logger.warning("scheduler: WB integration not found, skipping logistics sync")
            return

        client = WBClient(integration.api_key)
        result = sync_logistics_weekly(db, client, week_offset=1)

        if result.get("total_raw", 0) == 0:
            retry_at = datetime.now(tz=MSK) + timedelta(hours=1)
            scheduler.reschedule_job(
                "weekly_logistics_sync",
                trigger=DateTrigger(run_date=retry_at),
            )
            logger.warning("scheduler: logistics report not ready yet, retry at %s", retry_at.strftime("%H:%M MSK"))
        else:
            scheduler.reschedule_job(
                "weekly_logistics_sync",
                trigger=CronTrigger(day_of_week="mon", hour=13, minute=0, timezone=MSK),
            )
            logger.info("scheduler: logistics sync done — %s", result)
    except WBApiError as e:
        logger.error("scheduler: WB API error during logistics sync — %s", e)
    except Exception as e:
        logger.exception("scheduler: unexpected error during logistics sync — %s", e)
    finally:
        db.close()


def _job_sync_commission() -> None:
    """
    Еженедельный расчёт комиссии + эквайринг из финотчёта WB.
    Первый запуск — пн 13:30 МСК. Если отчёт ещё не готов (total_raw == 0),
    повторяет каждый час до получения данных, затем возвращается к еженедельному расписанию.
    """
    from app.core.database import SessionLocal
    from app.services.wb_api import WBClient, WBApiError
    from app.services.wb_sync import sync_commission_weekly

    db = SessionLocal()
    try:
        integration = _wb_integration(db)
        if not integration:
            logger.warning("scheduler: WB integration not found, skipping commission sync")
            return

        client = WBClient(integration.api_key)
        result = sync_commission_weekly(db, client, week_offset=1)

        if result.get("total_raw", 0) == 0:
            retry_at = datetime.now(tz=MSK) + timedelta(hours=1)
            scheduler.reschedule_job(
                "weekly_commission_sync",
                trigger=DateTrigger(run_date=retry_at),
            )
            logger.warning("scheduler: commission report not ready yet, retry at %s", retry_at.strftime("%H:%M MSK"))
        else:
            scheduler.reschedule_job(
                "weekly_commission_sync",
                trigger=CronTrigger(day_of_week="mon", hour=13, minute=30, timezone=MSK),
            )
            logger.info("scheduler: commission sync done — %s", result)
    except WBApiError as e:
        logger.error("scheduler: WB API error during commission sync — %s", e)
    except Exception as e:
        logger.exception("scheduler: unexpected error during commission sync — %s", e)
    finally:
        db.close()


def _job_wb_expenses_sync() -> None:
    """
    Синхронизация расходов WB из финотчёта в SkuDailyExpense.
    Ежедневно 08:30 МСК (из ежедневного отчёта) + Понедельник 14:00 (еженедельный).
    Объединяет weekly + daily отчёты автоматически.
    """
    from app.core.database import SessionLocal
    from app.services.wb_api import WBClient, WBApiError
    from app.services.wb_sync import sync_wb_expenses

    db = SessionLocal()
    try:
        integration = _wb_integration(db)
        if not integration:
            logger.warning("scheduler: WB integration not found, skipping expenses sync")
            return

        client = WBClient(integration.api_key)
        result = sync_wb_expenses(db, client, days_back=14)
        logger.info("scheduler: wb_expenses sync done — %s", result)
    except WBApiError as e:
        logger.error("scheduler: WB API error during expenses sync — %s", e)
    except Exception as e:
        logger.exception("scheduler: unexpected error during expenses sync — %s", e)
    finally:
        db.close()


def _job_daily_orders_sync() -> None:
    """
    Каждые 20 минут с 08:00 до 23:40 МСК.
    Всегда запускает run_full_sync за последние 2 дня — внутри sync-функции
    сами решают, какие записи вставлять (дедупликация по external_id / дате).
    Это гарантирует подгрузку недостающих данных при частичных сбоях WB API.
    """
    from app.core.database import SessionLocal
    from app.services.wb_api import WBApiError
    from app.services.wb_sync import run_full_sync

    db = SessionLocal()
    try:
        integration = _wb_integration(db)
        if not integration:
            logger.warning("daily_orders_sync: WB интеграция не найдена, пропуск")
            return

        logger.info("daily_orders_sync: запускаем синхронизацию WB за 2 дня")
        result = run_full_sync(db, integration, days_back=2)
        logger.info("daily_orders_sync: синхронизация завершена — %s", result)
    except WBApiError as e:
        logger.error("daily_orders_sync: WB API ошибка — %s", e)
    except Exception as e:
        logger.exception("daily_orders_sync: неожиданная ошибка — %s", e)
    finally:
        db.close()


def _job_sync_ads() -> None:
    """
    Синхронизация рекламной статистики WB — 3 раза в день: 10:00, 14:00, 20:00 МСК.
    Запускается независимо от наличия заказов, чтобы всегда иметь актуальные данные
    (WB fullstats API обновляется с задержкой 1-2 часа).
    """
    from app.core.database import SessionLocal
    from app.services.wb_api import WBClient, WBApiError
    from app.services.wb_sync import sync_ads

    db = SessionLocal()
    try:
        integration = _wb_integration(db)
        if not integration:
            logger.warning("ads_sync: WB интеграция не найдена, пропуск")
            return

        client = WBClient(integration.api_key, ads_api_key=integration.ads_api_key, prices_api_key=integration.prices_api_key)
        result = sync_ads(db, client, days_back=3)
        logger.info("ads_sync: завершено — %s", result)
    except WBApiError as e:
        logger.error("ads_sync: WB API ошибка — %s", e)
    except Exception as e:
        logger.exception("ads_sync: неожиданная ошибка — %s", e)
    finally:
        db.close()


def _job_sync_prices() -> None:
    """Ежедневная синхронизация цен продавца из WB Prices API (08:00 МСК)."""
    from app.core.database import SessionLocal
    from app.services.wb_api import WBClient, WBApiError
    from app.services.wb_sync import sync_prices

    db = SessionLocal()
    try:
        integration = _wb_integration(db)
        if not integration:
            logger.warning("scheduler: WB integration not found, skipping price sync")
            return

        client = WBClient(integration.api_key, ads_api_key=integration.ads_api_key, prices_api_key=integration.prices_api_key)
        result = sync_prices(db, client)
        logger.info("scheduler: price sync done — %s", result)
    except WBApiError as e:
        logger.error("scheduler: WB API error during price sync — %s", e)
    except Exception as e:
        logger.exception("scheduler: unexpected error during price sync — %s", e)
    finally:
        db.close()


def _job_sync_nm_report() -> None:
    """Ежедневная синхронизация воронки карточки и рейтингов из WB nm-report (09:30 МСК)."""
    from app.core.database import SessionLocal
    from app.services.wb_api import WBClient, WBApiError
    from app.services.wb_sync import sync_nm_report

    db = SessionLocal()
    try:
        integration = _wb_integration(db)
        if not integration:
            logger.warning("nm_report_sync: WB интеграция не найдена, пропуск")
            return

        client = WBClient(integration.api_key)
        result = sync_nm_report(db, client, days_back=14)
        logger.info("nm_report_sync: синхронизация завершена — %s", result)
    except WBApiError as e:
        logger.error("nm_report_sync: WB API ошибка — %s", e)
    except Exception as e:
        logger.exception("nm_report_sync: неожиданная ошибка — %s", e)
    finally:
        db.close()


def _job_lamoda_orders_sync() -> None:
    """
    Инкрементальный синк заказов Lamoda каждые 20 минут с 08:00 до 23:40 МСК.
    Всегда запускает sync — дедупликация внутри.
    """
    from app.core.database import SessionLocal
    from app.services.lamoda_api import LamodaClient, LamodaApiError
    from app.services.lamoda_sync import sync_lamoda_orders

    db = SessionLocal()
    try:
        integration = _lamoda_integration(db)
        if not integration:
            logger.debug("lamoda_orders_sync: Lamoda интеграция не найдена, пропуск")
            return

        client = LamodaClient(integration.client_id or "", integration.api_key)
        result = sync_lamoda_orders(db, client, days_back=2)
        logger.info("lamoda_orders_sync: завершено — %s", result)
    except LamodaApiError as e:
        logger.error("lamoda_orders_sync: API ошибка — %s", e)
    except Exception as e:
        logger.exception("lamoda_orders_sync: неожиданная ошибка — %s", e)
    finally:
        db.close()


def _job_lamoda_stock_sync() -> None:
    """Ежедневная выгрузка остатков Lamoda FBO в 00:10 МСК (INSERT-only)."""
    from app.core.database import SessionLocal
    from app.services.lamoda_api import LamodaClient, LamodaApiError
    from app.services.lamoda_sync import sync_lamoda_stock

    db = SessionLocal()
    try:
        integration = _lamoda_integration(db)
        if not integration:
            return

        client = LamodaClient(integration.client_id or "", integration.api_key)
        result = sync_lamoda_stock(db, client)
        logger.info("lamoda_stock_sync: завершено — %s", result)
    except LamodaApiError as e:
        logger.error("lamoda_stock_sync: API ошибка — %s", e)
    except Exception as e:
        logger.exception("lamoda_stock_sync: неожиданная ошибка — %s", e)
    finally:
        db.close()


def _job_lamoda_nomenclatures_sync() -> None:
    """Ежедневный синк номенклатур (цены + фото) Lamoda в 08:05 МСК."""
    from app.core.database import SessionLocal
    from app.services.lamoda_api import LamodaClient, LamodaApiError
    from app.services.lamoda_sync import sync_lamoda_nomenclatures

    db = SessionLocal()
    try:
        integration = _lamoda_integration(db)
        if not integration:
            return

        client = LamodaClient(integration.client_id or "", integration.api_key)
        result = sync_lamoda_nomenclatures(db, client)
        logger.info("lamoda_nomenclatures_sync: завершено — %s", result)
    except LamodaApiError as e:
        logger.error("lamoda_nomenclatures_sync: API ошибка — %s", e)
    except Exception as e:
        logger.exception("lamoda_nomenclatures_sync: неожиданная ошибка — %s", e)
    finally:
        db.close()


def _job_ozon_orders_sync() -> None:
    """Инкрементальный синк заказов+возвратов Ozon каждые 20 мин с 08:10 до 23:50 МСК."""
    from app.core.database import SessionLocal
    from app.services.ozon_api import OzonClient, OzonApiError
    from app.services.ozon_sync import sync_orders, sync_returns, sync_expenses

    db = SessionLocal()
    try:
        integration = _ozon_integration(db)
        if not integration:
            logger.debug("ozon_orders_sync: Ozon интеграция не найдена, пропуск")
            return

        client = OzonClient(integration.api_key, integration.client_id or "")
        result_orders  = sync_orders(db, client, days_back=2)
        result_returns = sync_returns(db, client, days_back=7)
        result_expenses = sync_expenses(db, client, days_back=30)
        logger.info("ozon_orders_sync: %s | returns: %s | expenses: %s", result_orders, result_returns, result_expenses)
    except OzonApiError as e:
        logger.error("ozon_orders_sync: API ошибка — %s", e)
    except Exception as e:
        logger.exception("ozon_orders_sync: неожиданная ошибка — %s", e)
    finally:
        db.close()


def _job_ozon_ads_sync() -> None:
    """Синхронизация рекламы Ozon Performance — 3 раза в день: 10:15, 14:15, 20:15 МСК."""
    from app.core.database import SessionLocal
    from app.services.ozon_api import OzonPerformanceClient, OzonPerformanceError
    from app.services.ozon_sync import sync_ads

    db = SessionLocal()
    try:
        integration = _ozon_integration(db)
        if not integration:
            return
        perf_client_id = getattr(integration, "perf_client_id", None)
        perf_secret    = integration.ads_api_key
        if not perf_client_id or not perf_secret:
            logger.debug("ozon_ads_sync: Performance API credentials не заданы, пропуск")
            return
        perf = OzonPerformanceClient(perf_client_id, perf_secret)
        result = sync_ads(db, perf, days_back=1)
        logger.info("ozon_ads_sync: завершено — %s", result)
    except OzonPerformanceError as e:
        logger.error("ozon_ads_sync: Performance API ошибка — %s", e)
    except Exception as e:
        logger.exception("ozon_ads_sync: неожиданная ошибка — %s", e)
    finally:
        db.close()


def _job_ozon_stock_sync() -> None:
    """Ежедневная выгрузка остатков Ozon в 00:15 МСК (INSERT-only)."""
    from app.core.database import SessionLocal
    from app.services.ozon_api import OzonClient, OzonApiError
    from app.services.ozon_sync import sync_stocks

    db = SessionLocal()
    try:
        integration = _ozon_integration(db)
        if not integration:
            return

        client = OzonClient(integration.api_key, integration.client_id or "")
        result = sync_stocks(db, client)
        logger.info("ozon_stock_sync: завершено — %s", result)
    except OzonApiError as e:
        logger.error("ozon_stock_sync: API ошибка — %s", e)
    except Exception as e:
        logger.exception("ozon_stock_sync: неожиданная ошибка — %s", e)
    finally:
        db.close()


def _job_ozon_prices_sync() -> None:
    """Ежедневная выгрузка цен Ozon в 08:10 МСК."""
    from app.core.database import SessionLocal
    from app.services.ozon_api import OzonClient, OzonApiError
    from app.services.ozon_sync import sync_prices

    db = SessionLocal()
    try:
        integration = _ozon_integration(db)
        if not integration:
            return

        client = OzonClient(integration.api_key, integration.client_id or "")
        result = sync_prices(db, client)
        logger.info("ozon_prices_sync: завершено — %s", result)
    except OzonApiError as e:
        logger.error("ozon_prices_sync: API ошибка — %s", e)
    except Exception as e:
        logger.exception("ozon_prices_sync: неожиданная ошибка — %s", e)
    finally:
        db.close()


def _startup_catchup() -> None:
    """
    Запускается однократно через 10 сек после старта.
    Догоняет всё, что могло быть пропущено:
      1. Остатки за вчера (WB, Ozon, Lamoda) — INSERT-only, не перезаписываем.
      2. nm-report (переходы) за вчера.
      3. Полный WB sync (заказы, продажи, цены и т.д.) за 2 дня.
      4. Ozon заказы/возвраты за 2 дня.
      5. Lamoda заказы за 2 дня.
    """
    from app.core.database import SessionLocal
    from app.models.catalog import ChannelType
    from app.models.sales import CardStats
    from app.services.wb_api import WBClient, WBApiError
    from app.services.wb_sync import sync_stocks as wb_sync_stocks, sync_nm_report, run_full_sync as wb_full_sync
    from app.services.ozon_api import OzonClient, OzonApiError, OzonPerformanceClient, OzonPerformanceError
    from app.services.ozon_sync import sync_stocks as ozon_sync_stocks, sync_orders as ozon_sync_orders, sync_returns as ozon_sync_returns, sync_expenses as ozon_sync_expenses, sync_ads as ozon_sync_ads, sync_photos as ozon_sync_photos
    from app.services.lamoda_api import LamodaClient, LamodaApiError
    from app.services.lamoda_sync import sync_lamoda_stock, sync_lamoda_orders

    yesterday = date.today() - timedelta(days=1)
    db = SessionLocal()

    try:
        # ── WB ──────────────────────────────────────────────────────────────
        wb = _wb_integration(db)
        if wb:
            # 1) Остатки (INSERT-only — sync_stocks сам проверяет дублирование)
            if not _stocks_exist(db, ChannelType.WB, yesterday):
                logger.info("startup catchup: WB остатки за %s отсутствуют, загружаем", yesterday)
                try:
                    client = WBClient(wb.api_key)
                    wb_sync_stocks(db, client, target_date=yesterday)
                except WBApiError as e:
                    logger.error("startup catchup: WB stocks error — %s", e)

            # 2) nm-report
            if not db.query(CardStats).filter(CardStats.date == yesterday).first():
                logger.info("startup catchup: nm-report за %s отсутствует, загружаем", yesterday)
                try:
                    client = WBClient(wb.api_key)
                    sync_nm_report(db, client, days_back=14)
                except WBApiError as e:
                    logger.error("startup catchup: nm-report error — %s", e)

            # 3) Полный WB sync (заказы, продажи, цены, реклама) за 2 дня
            logger.info("startup catchup: запуск WB full sync за 2 дня")
            try:
                wb_full_sync(db, wb, days_back=2)
            except Exception as e:
                logger.error("startup catchup: WB full sync error — %s", e)

            # 4) WB expenses из финотчёта
            logger.info("startup catchup: WB expenses sync")
            try:
                from app.services.wb_sync import sync_wb_expenses
                client = WBClient(wb.api_key)
                sync_wb_expenses(db, client, days_back=14)
            except Exception as e:
                logger.error("startup catchup: WB expenses error — %s", e)

        # ── Ozon ────────────────────────────────────────────────────────────
        ozon = _ozon_integration(db)
        if ozon:
            client = OzonClient(ozon.api_key, ozon.client_id or "")

            if not _stocks_exist(db, ChannelType.OZON, yesterday):
                logger.info("startup catchup: Ozon остатки за %s отсутствуют, загружаем", yesterday)
                try:
                    ozon_sync_stocks(db, client)
                except OzonApiError as e:
                    logger.error("startup catchup: Ozon stocks error — %s", e)

            logger.info("startup catchup: Ozon orders+returns+expenses+photos")
            try:
                ozon_sync_orders(db, client, days_back=2)
                ozon_sync_returns(db, client, days_back=7)
                ozon_sync_expenses(db, client, days_back=30)
                ozon_sync_photos(db, client)
            except OzonApiError as e:
                logger.error("startup catchup: Ozon sync error — %s", e)

            perf_client_id = getattr(ozon, "perf_client_id", None)
            perf_secret    = ozon.ads_api_key
            if perf_client_id and perf_secret:
                logger.info("startup catchup: Ozon ads")
                try:
                    perf = OzonPerformanceClient(perf_client_id, perf_secret)
                    ozon_sync_ads(db, perf, days_back=3)
                except OzonPerformanceError as e:
                    logger.error("startup catchup: Ozon ads error — %s", e)

        # ── Lamoda ──────────────────────────────────────────────────────────
        lamoda = _lamoda_integration(db)
        if lamoda:
            client = LamodaClient(lamoda.client_id or "", lamoda.api_key)

            if not _stocks_exist(db, ChannelType.LAMODA, yesterday):
                logger.info("startup catchup: Lamoda остатки за %s отсутствуют, загружаем", yesterday)
                try:
                    sync_lamoda_stock(db, client)
                except LamodaApiError as e:
                    logger.error("startup catchup: Lamoda stocks error — %s", e)

            logger.info("startup catchup: Lamoda orders за 2 дня")
            try:
                sync_lamoda_orders(db, client, days_back=2)
            except LamodaApiError as e:
                logger.error("startup catchup: Lamoda orders error — %s", e)

        logger.info("startup catchup: завершено")
    except Exception as e:
        logger.exception("startup catchup: неожиданная ошибка — %s", e)
    finally:
        db.close()


def _job_data_completeness_check() -> None:
    """
    Мониторинг полноты данных — запускается каждые 20 мин с 08:20 до 23:40.
    Проверяет данные за вчера и позавчера.
    Если есть пропуски — автоматически дозагружает.
    """
    from app.core.database import SessionLocal
    from app.models.catalog import ChannelType
    from app.services.data_completeness import check_and_fix_gaps

    db = SessionLocal()
    try:
        # Проверяем WB за последние 3 дня
        results = check_and_fix_gaps(db, days_back=3, channel_type=ChannelType.WB)
        for r in results:
            if not r["ok"]:
                logger.warning(
                    "data_completeness: %s %s — пропуски: %s, исправление: %s",
                    r["date"], r["channel"], r["missing"], r.get("fix_result", "")
                )
            else:
                logger.debug("data_completeness: %s %s — ✅ полные данные", r["date"], r["channel"])

        # Проверяем Ozon
        results_ozon = check_and_fix_gaps(db, days_back=3, channel_type=ChannelType.OZON)
        for r in results_ozon:
            if not r["ok"]:
                logger.warning(
                    "data_completeness: %s %s — пропуски: %s, исправление: %s",
                    r["date"], r["channel"], r["missing"], r.get("fix_result", "")
                )
    except Exception as e:
        logger.exception("data_completeness: ошибка проверки — %s", e)
    finally:
        db.close()


def start_scheduler() -> None:
    # Немедленная проверка при старте — догоняем все пропущенные данные
    scheduler.add_job(
        _startup_catchup,
        trigger=DateTrigger(run_date=datetime.now(tz=MSK) + timedelta(seconds=10)),
        id="startup_catchup",
        replace_existing=True,
    )
    scheduler.add_job(
        _job_sync_stocks,
        trigger=CronTrigger(hour=0, minute=5, timezone=MSK),
        id="daily_stock_sync",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        _job_sync_logistics,
        trigger=CronTrigger(day_of_week="mon", hour=13, minute=0, timezone=MSK),
        id="weekly_logistics_sync",
        replace_existing=True,
        misfire_grace_time=7200,
    )
    scheduler.add_job(
        _job_sync_commission,
        trigger=CronTrigger(day_of_week="mon", hour=13, minute=30, timezone=MSK),
        id="weekly_commission_sync",
        replace_existing=True,
        misfire_grace_time=7200,
    )
    # WB expenses: ежедневно 08:30 + понедельник 14:00 (еженедельный отчёт)
    scheduler.add_job(
        _job_wb_expenses_sync,
        trigger=CronTrigger(hour=8, minute=30, timezone=MSK),
        id="wb_expenses_daily_sync",
        replace_existing=True,
        misfire_grace_time=7200,
    )
    scheduler.add_job(
        _job_wb_expenses_sync,
        trigger=CronTrigger(day_of_week="mon", hour=14, minute=0, timezone=MSK),
        id="wb_expenses_weekly_sync",
        replace_existing=True,
        misfire_grace_time=7200,
    )
    scheduler.add_job(
        _job_daily_orders_sync,
        trigger=CronTrigger(hour="8-23", minute="0,20,40", timezone=MSK),
        id="daily_orders_sync",
        replace_existing=True,
        misfire_grace_time=600,
    )
    scheduler.add_job(
        _job_sync_ads,
        trigger=CronTrigger(hour="10,14,20", minute=0, timezone=MSK),
        id="daily_ads_sync",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        _job_sync_prices,
        trigger=CronTrigger(hour=8, minute=0, timezone=MSK),
        id="daily_price_sync",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        _job_sync_nm_report,
        trigger=CronTrigger(hour=9, minute=30, timezone=MSK),
        id="daily_nm_report_sync",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        _job_lamoda_orders_sync,
        trigger=CronTrigger(hour="8-23", minute="0,20,40", timezone=MSK),
        id="lamoda_orders_sync",
        replace_existing=True,
        misfire_grace_time=600,
    )
    scheduler.add_job(
        _job_lamoda_stock_sync,
        trigger=CronTrigger(hour=0, minute=10, timezone=MSK),
        id="lamoda_stock_sync",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        _job_lamoda_nomenclatures_sync,
        trigger=CronTrigger(hour=8, minute=5, timezone=MSK),
        id="lamoda_nomenclatures_sync",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    # ── Ozon ─────────────────────────────────────────────────────────────────
    scheduler.add_job(
        _job_ozon_orders_sync,
        trigger=CronTrigger(hour="8-23", minute="10,30,50", timezone=MSK),
        id="ozon_orders_sync",
        replace_existing=True,
        misfire_grace_time=600,
    )
    scheduler.add_job(
        _job_ozon_stock_sync,
        trigger=CronTrigger(hour=0, minute=15, timezone=MSK),
        id="ozon_stock_sync",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        _job_ozon_prices_sync,
        trigger=CronTrigger(hour=8, minute=10, timezone=MSK),
        id="ozon_prices_sync",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        _job_ozon_ads_sync,
        trigger=CronTrigger(hour="10,14,20", minute=15, timezone=MSK),
        id="ozon_ads_sync",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    # ── Мониторинг полноты данных ─────────────────────────────────────────
    # Каждые 20 мин с 08:20 до 23:40 — проверяет пропуски за 3 дня и дозагружает
    scheduler.add_job(
        _job_data_completeness_check,
        trigger=CronTrigger(hour="8-23", minute="15,35,55", timezone=MSK),
        id="data_completeness_check",
        replace_existing=True,
        misfire_grace_time=600,
    )
    scheduler.start()
    logger.info(
        "scheduler: started — WB: stocks 00:05, logistics mon 13:00, commission mon 13:30, "
        "orders every 20min 08:00-23:40, prices 08:00, nm-report 09:30 MSK | "
        "Lamoda: orders every 20min, stock 00:10, nomenclatures 08:05 MSK | "
        "Ozon: orders every 20min 08:10-23:50, stocks 00:15, prices 08:10, ads 10:15/14:15/20:15 MSK | "
        "Data completeness check: every 20min 08:15-23:55 MSK"
    )


def stop_scheduler() -> None:
    scheduler.shutdown(wait=False)
    logger.info("scheduler: stopped")
