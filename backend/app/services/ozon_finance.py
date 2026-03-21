"""
Утилиты для получения финансовых данных Ozon из API.
Используются в оцифровке и РнП.
"""
from sqlalchemy.orm import Session


# Кеш коэффициента цены покупателя Ozon (customer_price / seller_price)
_ozon_customer_price_ratio_cache: dict[str, float] = {}


def get_ozon_customer_price_ratio(db: Session) -> float:
    """Коэффициент (цена покупателя / цена продавца) из отчёта реализации Ozon.
    Отчёт за прошлый месяц. Кешируется на сессию."""
    cache_key = "ozon_cpr"
    if cache_key in _ozon_customer_price_ratio_cache:
        return _ozon_customer_price_ratio_cache[cache_key]

    try:
        from app.models.integration import Integration, IntegrationType
        import httpx
        from datetime import date

        integ = db.query(Integration).filter(
            Integration.type == IntegrationType.OZON,
            Integration.is_active == True,
        ).first()
        if not integ:
            return 0.489

        headers = {
            "Client-Id": str(integ.client_id),
            "Api-Key": str(integ.api_key),
            "Content-Type": "application/json",
        }

        today = date.today()
        if today.month == 1:
            m, y = 12, today.year - 1
        else:
            m, y = today.month - 1, today.year

        r = httpx.post(
            "https://api-seller.ozon.ru/v2/finance/realization",
            headers=headers,
            json={"month": m, "year": y},
            timeout=30,
        )
        if r.status_code != 200:
            _ozon_customer_price_ratio_cache[cache_key] = 0.489
            return 0.489

        rows = r.json().get("result", {}).get("rows", [])
        total_seller = 0.0
        total_customer = 0.0
        for row in rows:
            dc = row.get("delivery_commission") or {}
            qty = dc.get("quantity") or 0
            if qty > 0:
                total_seller += (row.get("seller_price_per_instance") or 0) * qty
                total_customer += dc.get("amount") or 0

        ratio = total_customer / total_seller if total_seller > 0 else 0.489
        _ozon_customer_price_ratio_cache[cache_key] = ratio
        return ratio
    except Exception:
        _ozon_customer_price_ratio_cache[cache_key] = 0.489
        return 0.489


# Кеш bonus_ratio (доля соинвеста Ozon в реализации)
_ozon_bonus_ratio_cache: dict[str, float] = {}


def get_ozon_bonus_ratio(db: Session) -> float:
    """Доля соинвеста Ozon (bonus balls) в общей реализации.
    bonus_ratio = total_bonus / total_accruals_for_sale.
    Из отчёта реализации (/v2/finance/realization) за прошлый месяц.
    Продажи (цена покупателя) = Реализация × (1 - bonus_ratio)."""
    cache_key = "ozon_br"
    if cache_key in _ozon_bonus_ratio_cache:
        return _ozon_bonus_ratio_cache[cache_key]

    fallback = 0.474

    try:
        from app.models.integration import Integration, IntegrationType
        import httpx
        from datetime import date

        integ = db.query(Integration).filter(
            Integration.type == IntegrationType.OZON,
            Integration.is_active == True,
        ).first()
        if not integ:
            _ozon_bonus_ratio_cache[cache_key] = fallback
            return fallback

        headers = {
            "Client-Id": str(integ.client_id),
            "Api-Key": str(integ.api_key),
            "Content-Type": "application/json",
        }

        today = date.today()
        if today.month == 1:
            m, y = 12, today.year - 1
        else:
            m, y = today.month - 1, today.year

        r = httpx.post(
            "https://api-seller.ozon.ru/v2/finance/realization",
            headers=headers,
            json={"month": m, "year": y},
            timeout=30,
        )
        if r.status_code != 200:
            _ozon_bonus_ratio_cache[cache_key] = fallback
            return fallback

        rows = r.json().get("result", {}).get("rows", [])
        total_bonus = 0.0
        total_accruals = 0.0
        for row in rows:
            dc = row.get("delivery_commission") or {}
            rc = row.get("return_commission") or {}
            qty_d = dc.get("quantity") or 0
            qty_r = rc.get("quantity") or 0
            if qty_d > 0:
                # accruals = seller_price × qty (what Ozon credits to seller)
                total_accruals += (row.get("seller_price_per_instance") or 0) * qty_d
                # bonus = Ozon co-investment (bonus balls, stars, bank coinvestment)
                total_bonus += abs(dc.get("bonus") or 0)
                total_bonus += abs(dc.get("bank_coinvestment") or 0)
                total_bonus += abs(dc.get("stars") or 0)
                total_bonus += abs(dc.get("pick_up_point_coinvestment") or 0)

        ratio = total_bonus / total_accruals if total_accruals > 0 else fallback
        _ozon_bonus_ratio_cache[cache_key] = round(ratio, 4)
        return round(ratio, 4)
    except Exception:
        _ozon_bonus_ratio_cache[cache_key] = fallback
        return fallback


# Кеш фактических % комиссии и эквайринга Ozon из финансового отчёта
_ozon_fin_ratios_cache: dict[str, dict] = {}


def get_ozon_fin_ratios(db: Session) -> dict:
    """Фактические % комиссии и эквайринга из /v1/finance/cash-flow-statement/list.
    Возвращает {'commission_pct': float, 'acquiring_pct': float, 'total_pct': float}.
    Данные за последние 30 дней. Кешируется на сессию."""
    cache_key = "ozon_fin"
    if cache_key in _ozon_fin_ratios_cache:
        return _ozon_fin_ratios_cache[cache_key]

    fallback = {"commission_pct": 39.2, "acquiring_pct": 0.8, "total_pct": 40.0}

    try:
        from app.models.integration import Integration, IntegrationType
        import httpx
        from datetime import date, timedelta

        integ = db.query(Integration).filter(
            Integration.type == IntegrationType.OZON,
            Integration.is_active == True,
        ).first()
        if not integ:
            _ozon_fin_ratios_cache[cache_key] = fallback
            return fallback

        headers = {
            "Client-Id": str(integ.client_id),
            "Api-Key": str(integ.api_key),
            "Content-Type": "application/json",
        }

        today = date.today()
        r = httpx.post(
            "https://api-seller.ozon.ru/v1/finance/cash-flow-statement/list",
            headers=headers,
            json={
                "date": {
                    "from": f"{(today - timedelta(days=30)).isoformat()}T00:00:00.000Z",
                    "to": f"{today.isoformat()}T23:59:59.000Z",
                },
                "with_details": True,
                "page": 1,
                "page_size": 10,
            },
            timeout=30,
        )
        if r.status_code != 200:
            _ozon_fin_ratios_cache[cache_key] = fallback
            return fallback

        result = r.json().get("result", {})
        cash_flows = result.get("cash_flows", [])
        details_list = result.get("details", [])
        if not isinstance(details_list, list):
            details_list = [details_list] if details_list else []

        total_orders = sum(abs(cf.get("orders_amount", 0)) for cf in cash_flows)
        total_commission = sum(abs(cf.get("commission_amount", 0)) for cf in cash_flows)

        total_acq = 0.0
        for det in details_list:
            if not det:
                continue
            for item in (det.get("others") or {}).get("items", []):
                if "acquiring" in (item.get("name") or "").lower():
                    total_acq += abs(item.get("price", 0))

        if total_orders > 0:
            comm_pct = round(total_commission / total_orders * 100, 2)
            acq_pct = round(total_acq / total_orders * 100, 2)
            ratios = {
                "commission_pct": comm_pct,
                "acquiring_pct": acq_pct,
                "total_pct": round(comm_pct + acq_pct, 2),
            }
        else:
            ratios = fallback

        _ozon_fin_ratios_cache[cache_key] = ratios
        return ratios
    except Exception:
        _ozon_fin_ratios_cache[cache_key] = fallback
        return fallback
