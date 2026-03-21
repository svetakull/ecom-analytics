"""
Wildberries API integration.
Docs: https://openapi.wildberries.ru/
"""
from datetime import date, datetime, timedelta
from typing import Optional
import time
import httpx

WB_BASE = "https://statistics-api.wildberries.ru/api/v1"
WB_BASE_V5 = "https://statistics-api.wildberries.ru/api/v5"
WB_ANALYTICS = "https://seller-analytics-api.wildberries.ru/api/v1"
WB_ANALYTICS_V2 = "https://seller-analytics-api.wildberries.ru/api/v2"
WB_ANALYTICS_V3 = "https://seller-analytics-api.wildberries.ru/api/analytics/v3"
WB_CONTENT = "https://suppliers-api.wildberries.ru"
WB_ADS = "https://advert-api.wildberries.ru"
WB_PRICES = "https://discounts-prices-api.wildberries.ru"
WB_STOCKS = "https://statistics-api.wildberries.ru/api/v1"


class WBApiError(Exception):
    pass


class WBClient:
    def __init__(self, api_key: str, ads_api_key: str = None, prices_api_key: str = None):
        self.api_key = api_key
        self.headers = {"Authorization": api_key}
        self.ads_headers = {"Authorization": ads_api_key or api_key}
        self.prices_headers = {"Authorization": prices_api_key or ads_api_key or api_key}

    def _get(self, url: str, params: dict = None, use_ads_token: bool = False, use_prices_token: bool = False) -> dict | list:
        if use_prices_token:
            headers = self.prices_headers
        elif use_ads_token:
            headers = self.ads_headers
        else:
            headers = self.headers
        with httpx.Client(timeout=300) as client:
            r = client.get(url, headers=headers, params=params)
            if r.status_code == 401:
                raise WBApiError("Неверный API-ключ WB")
            if r.status_code == 429:
                raise WBApiError("Превышен лимит запросов WB API")
            if not r.is_success:
                raise WBApiError(f"WB API error {r.status_code}: {r.text[:200]}")
            if not r.text.strip():
                return []
            return r.json()

    def test_connection(self) -> bool:
        """Проверить что ключ рабочий."""
        try:
            self._get(
                f"{WB_STOCKS}/supplier/stocks",
                {"dateFrom": date.today().isoformat()}
            )
            return True
        except WBApiError:
            return False

    def get_orders(self, date_from: date, flag: int = 0) -> list[dict]:
        """
        Заказы.
        flag=0 — только новые с dateFrom
        flag=1 — все заказы с dateFrom
        """
        data = self._get(
            f"{WB_BASE}/supplier/orders",
            {"dateFrom": date_from.isoformat(), "flag": flag}
        )
        return data if isinstance(data, list) else []

    def get_sales(self, date_from: date, flag: int = 0) -> list[dict]:
        """Продажи/возвраты (saleID начинается на S — продажа, R — возврат)."""
        data = self._get(
            f"{WB_BASE}/supplier/sales",
            {"dateFrom": date_from.isoformat(), "flag": flag}
        )
        return data if isinstance(data, list) else []

    def get_stocks(self, date_from: date) -> list[dict]:
        """Остатки на складах."""
        data = self._get(
            f"{WB_STOCKS}/supplier/stocks",
            {"dateFrom": date_from.isoformat()}
        )
        return data if isinstance(data, list) else []

    def get_income(self, date_from: date) -> list[dict]:
        """Поставки (приёмки)."""
        data = self._get(
            f"{WB_BASE}/supplier/incomes",
            {"dateFrom": date_from.isoformat()}
        )
        return data if isinstance(data, list) else []

    def get_report_detail(self, date_from: date, date_to: date, rrd_id: int = 0,
                          period: str = "weekly") -> list[dict]:
        """
        Детализация финансового отчёта WB (v5).
        period: "weekly" (еженедельный) или "daily" (ежедневный).
        Ключевые поля: sa_name, delivery_rub, storage_fee, supplier_oper_name,
        office_name, sale_dt, ppvz_for_pay, ppvz_sales_commission.
        """
        data = self._get(
            f"{WB_BASE_V5}/supplier/reportDetailByPeriod",
            {
                "dateFrom": date_from.isoformat(),
                "dateTo": date_to.isoformat(),
                "rrdid": rrd_id,
                "limit": 100000,
                "period": period,
            }
        )
        return data if isinstance(data, list) else []

    def get_report_detail_logistics(self, date_from: date, date_to: date) -> list[dict]:
        """Финансовый отчёт — берём delivery_rub и storage_fee на единицу продажи."""
        return self.get_report_detail(date_from, date_to)

    def get_paid_storage(self, date_from: date, date_to: date) -> list[dict]:
        """
        Отчёт по платному хранению WB (task-based API, макс 8 дней за запрос).
        Возвращает ежедневные расходы по каждому артикулу × склад.
        Ключевые поля: date, vendorCode, warehouse, warehousePrice, barcodesCount.
        Автоматически делит period на 8-дневные chunks.
        """
        all_rows: list[dict] = []
        chunk_days = 7  # макс 8 дней, берём 7 для надёжности

        current = date_from
        while current <= date_to:
            chunk_end = min(current + timedelta(days=chunk_days - 1), date_to)
            rows = self._get_paid_storage_chunk(current, chunk_end)
            all_rows.extend(rows)
            current = chunk_end + timedelta(days=1)

        return all_rows

    def _get_paid_storage_chunk(self, date_from: date, date_to: date) -> list[dict]:
        """Получить платное хранение за один chunk (до 8 дней) через task API."""
        with httpx.Client(timeout=30) as client:
            # Step 1: create task
            r = client.get(
                f"{WB_ANALYTICS}/paid_storage",
                headers=self.headers,
                params={"dateFrom": date_from.isoformat(), "dateTo": date_to.isoformat()},
            )
            if r.status_code == 401:
                raise WBApiError("Неверный API-ключ WB")
            if not r.is_success:
                raise WBApiError(f"WB paid_storage create error {r.status_code}: {r.text[:200]}")

            task_id = r.json().get("data", {}).get("taskId")
            if not task_id:
                return []

            # Step 2: poll status (max 30 attempts × 5s = 150s)
            for _ in range(30):
                time.sleep(5)
                rs = client.get(
                    f"{WB_ANALYTICS}/paid_storage/tasks/{task_id}/status",
                    headers=self.headers,
                    timeout=15,
                )
                if rs.status_code == 429:
                    time.sleep(30)
                    continue
                if rs.is_success:
                    status = rs.json().get("data", {}).get("status", "")
                    if status == "done":
                        break
                    if status == "error":
                        raise WBApiError(f"WB paid_storage task failed: {rs.text[:200]}")

            # Step 3: download result (retry on 429)
            for attempt in range(5):
                rd = client.get(
                    f"{WB_ANALYTICS}/paid_storage/tasks/{task_id}/download",
                    headers=self.headers,
                    timeout=60,
                )
                if rd.status_code == 429:
                    time.sleep(30 * (attempt + 1))
                    continue
                if not rd.is_success:
                    raise WBApiError(f"WB paid_storage download error {rd.status_code}: {rd.text[:200]}")
                break

            data = rd.json()
            return data if isinstance(data, list) else []

    def _post(self, url: str, payload: any, params: dict = None) -> dict | list:
        with httpx.Client(timeout=60) as client:
            r = client.post(url, headers=self.headers, json=payload, params=params)
            if r.status_code == 401:
                raise WBApiError("Неверный API-ключ WB")
            if r.status_code == 429:
                raise WBApiError("Превышен лимит запросов WB API")
            if not r.is_success:
                raise WBApiError(f"WB API error {r.status_code}: {r.text[:200]}")
            if not r.text.strip():
                return []
            return r.json()

    def get_ad_campaigns(self, statuses: list[int] = None) -> list[dict]:
        """
        Список рекламных кампаний через /adv/v1/promotion/count.
        Возвращает [{advertId, type, status}] для всех кампаний указанных статусов.
        statuses: 7=работает, 9=готов к запуску, 11=пауза, 4=архив, -1=удалён
        """
        if statuses is None:
            statuses = [7, 9, 11]
        statuses_set = set(statuses)

        try:
            data = self._get(f"{WB_ADS}/adv/v1/promotion/count", use_ads_token=True)
        except WBApiError:
            return []

        # Ответ: {"adverts": [{"type": 9, "status": 7, "count": N, "advert_list": [{"advertId": 123, ...}]}]}
        all_campaigns = []
        for group in (data.get("adverts") or []):
            if int(group.get("status") or 0) not in statuses_set:
                continue
            wb_type = int(group.get("type") or 0)
            wb_status = int(group.get("status") or 0)
            for item in (group.get("advert_list") or []):
                advert_id = item.get("advertId")
                if advert_id:
                    all_campaigns.append({
                        "advertId": advert_id,
                        "type": wb_type,
                        "status": wb_status,
                        "name": f"WB_{advert_id}",
                    })
        return all_campaigns

    def get_ad_fullstats(self, campaign_ids: list[int], date_from: date, date_to: date) -> list[dict]:
        """
        Детальная статистика по кампаниям за период с разбивкой по дням.
        GET /adv/v3/fullstats (v2/fullstats deprecated 2026-03-05, release-notes?id=388).
        Параметры: ids=1,2,3  beginDate=YYYY-MM-DD  endDate=YYYY-MM-DD
        Ограничение: до 31 дня за запрос, не более 300 кампаний за раз.
        Возвращает [{advertId, days: [{date, views, clicks, ctr, cpc, sum, atbs, orders, cr, shks, sum_price, canceled}]}]
        """
        if not campaign_ids:
            return []

        begin_str = date_from.strftime("%Y-%m-%d")
        end_str = date_to.strftime("%Y-%m-%d")

        all_stats = []
        # Максимум 50 кампаний за запрос (ограничение WB /adv/v3/fullstats)
        for i in range(0, len(campaign_ids), 50):
            chunk = campaign_ids[i:i + 50]
            params = {
                "ids": ",".join(str(c) for c in chunk),
                "beginDate": begin_str,
                "endDate": end_str,
            }
            for attempt in range(4):
                try:
                    data = self._get(f"{WB_ADS}/adv/v3/fullstats", params, use_ads_token=True)
                    if isinstance(data, list):
                        all_stats.extend(data)
                    time.sleep(5.0)  # пауза между чанками: WB ограничивает burst
                    break
                except WBApiError as e:
                    if "429" in str(e) or "лимит" in str(e).lower():
                        wait = 30 * (attempt + 1)  # 30s, 60s, 90s
                        time.sleep(wait)
                    else:
                        break  # пропускаем чанк при других ошибках
        return all_stats

    def get_nm_report(self, nm_ids: list[int], date_from: date = None, date_to: date = None, period_days: int = 7) -> list[dict]:
        """
        Воронка карточек WB по дням.
        POST /api/analytics/v3/sales-funnel/products/history
        Ограничения WB: max 20 nmIds за запрос, max 7 дней за запрос.
        Возвращает: [{"product": {"nmId": N, "vendorCode": "...", "title": "..."},
                      "history": [{"date": "YYYY-MM-DD", "openCount": N,
                                   "cartCount": N, "orderCount": N, "orderSum": N, ...}],
                      "currency": "RUB"}]
        """
        if date_to is None:
            date_to = date.today() - timedelta(days=1)
        if date_from is None:
            date_from = date_to - timedelta(days=period_days - 1)

        # WB ограничение: период не более 7 дней
        if (date_to - date_from).days > 6:
            date_from = date_to - timedelta(days=6)

        result = self._post(
            f"{WB_ANALYTICS_V3}/sales-funnel/products/history",
            {
                "selectedPeriod": {
                    "start": date_from.strftime("%Y-%m-%d"),
                    "end": date_to.strftime("%Y-%m-%d"),
                },
                "nmIds": nm_ids,
                "aggregationLevel": "day",
            }
        )
        return result if isinstance(result, list) else []

    def get_prices(self, limit: int = 1000) -> list[dict]:
        """
        Список товаров с актуальными ценами продавца.
        GET /api/v2/list/goods/filter
        Ключевые поля: nmID, vendorCode, sizes[].price, sizes[].discountedPrice
        discountedPrice = Цена со скидкой продавца = "Цена до СПП" в WB Аналитике.
        """
        all_goods: list[dict] = []
        cursor_updated_at: str | None = None
        cursor_nm_id: int | None = None

        for _ in range(100):  # max 100 pages
            params: dict = {"limit": limit}
            if cursor_updated_at:
                params["updatedAt"] = cursor_updated_at
            if cursor_nm_id:
                params["nmID"] = cursor_nm_id

            data = self._get(f"{WB_PRICES}/api/v2/list/goods/filter", params, use_prices_token=True)
            if not isinstance(data, dict):
                break
            goods = data.get("data", {}).get("listGoods", [])
            if not goods:
                break
            all_goods.extend(goods)

            cursor = data.get("data", {}).get("cursor", {})
            total = cursor.get("total", 0)
            if len(all_goods) >= total:
                break
            cursor_updated_at = cursor.get("updatedAt")
            cursor_nm_id = cursor.get("nmID")
            if not cursor_updated_at:
                break

        return all_goods

    def get_nm_report_grouped(self, nm_ids: list[int], date_from: date = None, date_to: date = None) -> list[dict]:
        """
        Сводная статистика воронки WB + рейтинги товаров.
        POST /api/analytics/v3/sales-funnel/products
        Поддерживает до 365 дней. Возвращает агрегированные данные (не по дням).
        Ключевые поля: product.nmId, product.productRating, product.feedbackRating.
        Возвращает: список products из data.products.
        """
        if date_to is None:
            date_to = date.today() - timedelta(days=1)
        if date_from is None:
            date_from = date_to - timedelta(days=7)

        all_products: list[dict] = []
        offset = 0
        limit = 100

        for _ in range(50):  # max 50 страниц
            payload: dict = {
                "selectedPeriod": {
                    "start": date_from.strftime("%Y-%m-%d"),
                    "end": date_to.strftime("%Y-%m-%d"),
                },
                "limit": limit,
                "offset": offset,
            }
            if nm_ids:
                payload["nmIds"] = nm_ids

            result = self._post(f"{WB_ANALYTICS_V3}/sales-funnel/products", payload)
            if not isinstance(result, dict):
                break
            products = (result.get("data") or {}).get("products") or []
            if not products:
                break
            all_products.extend(products)
            if len(products) < limit:
                break
            offset += limit

        return all_products

    # ─── WB Supplies API (поставки) ───────────────────────────────

    def get_supplies(self, date_from: date, date_to: date) -> list[dict]:
        """
        Список поставок за период.
        POST https://supplies-api.wildberries.ru/api/v1/supplies
        """
        url = "https://supplies-api.wildberries.ru/api/v1/supplies"
        body = {
            "dates": [{"from": date_from.isoformat(), "till": date_to.isoformat(), "type": "createDate"}],
            "statusIDs": [1, 2, 3, 4, 5, 6],
            "limit": 1000,
            "offset": 0,
        }
        with httpx.Client(timeout=60) as client:
            r = client.post(url, headers=self.headers, json=body)
            if r.status_code == 401:
                raise WBApiError("Неверный API-ключ WB (Supplies)")
            if not r.is_success:
                raise WBApiError(f"WB Supplies error {r.status_code}: {r.text[:200]}")
            data = r.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("supplies") or data.get("data") or data.get("result") or []
        return []

    def get_supply_details(self, supply_id: str) -> dict:
        """Детали поставки: склад, даты."""
        url = f"https://supplies-api.wildberries.ru/api/v1/supplies/{supply_id}"
        with httpx.Client(timeout=30) as client:
            r = client.get(url, headers=self.headers)
            if not r.is_success:
                return {}
            return r.json() or {}

    def get_supply_goods(self, supply_id: str) -> list[dict]:
        """Товары поставки: vendorCode, quantity, barcode."""
        url = f"https://supplies-api.wildberries.ru/api/v1/supplies/{supply_id}/goods"
        with httpx.Client(timeout=30) as client:
            r = client.get(url, headers=self.headers)
            if not r.is_success:
                return []
            data = r.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return []
