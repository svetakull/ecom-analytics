"""
Ozon Seller API integration.
Docs: https://docs.ozon.ru/api/seller/
"""
from datetime import date, datetime
import httpx

OZON_BASE = "https://api-seller.ozon.ru"


class OzonApiError(Exception):
    pass


class OzonClient:
    def __init__(self, api_key: str, client_id: str):
        self.api_key = api_key
        self.client_id = client_id
        self.headers = {
            "Client-Id": str(client_id),
            "Api-Key": api_key,
            "Content-Type": "application/json",
        }

    def _post(self, path: str, body: dict) -> dict:
        with httpx.Client(timeout=60) as client:
            r = client.post(f"{OZON_BASE}{path}", headers=self.headers, json=body)
            if r.status_code == 401:
                raise OzonApiError("Неверный API-ключ Ozon или Client-Id")
            if r.status_code == 429:
                raise OzonApiError("Превышен лимит запросов Ozon API")
            if not r.is_success:
                raise OzonApiError(f"Ozon API error {r.status_code}: {r.text[:300]}")
            return r.json()

    def test_connection(self) -> bool:
        try:
            self._post("/v1/seller/info", {})
            return True
        except OzonApiError:
            return False

    def get_fbo_postings(self, date_from: date, date_to: date, offset: int = 0, limit: int = 1000) -> list[dict]:
        """FBO заказы (Ozon хранит и доставляет). v2 — result это список."""
        body = {
            "dir": "asc",
            "filter": {
                "since": f"{date_from.isoformat()}T00:00:00Z",
                "to": f"{date_to.isoformat()}T23:59:59Z",
                "status": "",
            },
            "limit": limit,
            "offset": offset,
            "with": {
                "analytics_data": True,
                "financial_data": True,
            },
        }
        data = self._post("/v2/posting/fbo/list", body)
        result = data.get("result", [])
        # v2 returns result as a list directly
        return result if isinstance(result, list) else result.get("postings", [])

    def get_fbs_postings(self, date_from: date, date_to: date, offset: int = 0, limit: int = 1000) -> list[dict]:
        """FBS заказы (продавец хранит и доставляет)."""
        body = {
            "dir": "asc",
            "filter": {
                "since": f"{date_from.isoformat()}T00:00:00Z",
                "to": f"{date_to.isoformat()}T23:59:59Z",
                "status": "",
            },
            "limit": limit,
            "offset": offset,
            "with": {
                "analytics_data": True,
                "financial_data": True,
            },
        }
        data = self._post("/v3/posting/fbs/list", body)
        result = data.get("result")
        if isinstance(result, dict):
            return result.get("postings", [])
        return []

    def get_returns(self, offset: int = 0, limit: int = 500) -> tuple[list[dict], bool]:
        """Все возвраты (FBO + FBS) через /v1/returns/list.
        Возвращает (items, has_next)."""
        body = {
            "filter": {},
            "limit": limit,
            "offset": offset,
        }
        data = self._post("/v1/returns/list", body)
        return data.get("returns", []), bool(data.get("has_next", False))

    def get_stocks(self, limit: int = 1000, offset: int = 0) -> list[dict]:
        """Остатки на складах Ozon."""
        body = {
            "limit": limit,
            "offset": offset,
            "warehouse_type": "ALL",
        }
        data = self._post("/v2/analytics/stock_on_warehouses", body)
        return data.get("result", {}).get("rows", [])

    def get_prices(self, offer_ids: list[str] = None, limit: int = 1000, last_id: str = "") -> dict:
        """Цены товаров. v5 возвращает items + cursor."""
        body = {
            "filter": {
                "offer_id": offer_ids or [],
                "visibility": "ALL",
            },
            "last_id": last_id,
            "limit": limit,
        }
        return self._post("/v5/product/info/prices", body)

    def get_product_attributes(self, offer_ids: list[str], limit: int = 100, last_id: str = "") -> dict:
        """Атрибуты товаров (включая primary_image, images) по offer_id."""
        body = {
            "filter": {"offer_id": offer_ids},
            "limit": limit,
            "last_id": last_id,
        }
        return self._post("/v4/product/info/attributes", body)

    def get_transactions(self, date_from: date, date_to: date, page: int = 1, page_size: int = 1000) -> dict:
        """Финансовые транзакции — для получения реальных комиссий и логистики."""
        body = {
            "filter": {
                "date": {
                    "from": f"{date_from.isoformat()}T00:00:00Z",
                    "to": f"{date_to.isoformat()}T23:59:59Z",
                },
                "operation_type": [],
                "posting_number": "",
                "transaction_type": "all",
            },
            "page": page,
            "page_size": page_size,
        }
        return self._post("/v3/finance/transaction/list", body)

    # ─── Supply Orders (поставки на склады Ozon) ─────────────────

    def get_supply_order_ids(self) -> list[int]:
        """POST /v3/supply-order/list — список ID заказов на поставку."""
        import time as _time
        all_ids = []
        last_id = ""
        states = [
            "DATA_FILLING", "READY_TO_SUPPLY",
            "ACCEPTED_AT_SUPPLY_WAREHOUSE", "IN_TRANSIT",
            "ACCEPTANCE_AT_STORAGE_WAREHOUSE", "REPORTS_CONFIRMATION_AWAITING",
            "REPORT_REJECTED", "COMPLETED", "REJECTED_AT_SUPPLY_WAREHOUSE",
            "CANCELLED", "OVERDUE",
        ]
        for _ in range(20):
            payload = {"filter": {"states": states}, "limit": 100, "sort_by": "ORDER_CREATION", "sort_dir": "DESC"}
            if last_id:
                payload["last_id"] = last_id
            resp = self._post("/v3/supply-order/list", payload)
            ids = resp.get("order_ids") or []
            all_ids.extend(ids)
            last_id = resp.get("last_id", "")
            if not last_id or not ids:
                break
            _time.sleep(0.35)
        return all_ids

    def get_supply_order_details(self, order_ids: list[int]) -> list[dict]:
        """POST /v3/supply-order/get — детали заказов на поставку (батчами по 50)."""
        import time as _time
        all_orders = []
        for i in range(0, len(order_ids), 50):
            batch = order_ids[i : i + 50]
            resp = self._post("/v3/supply-order/get", {"order_ids": batch})
            all_orders.extend(resp.get("orders") or [])
            _time.sleep(0.35)
        return all_orders

    def get_supply_bundle_items(self, bundle_id: int, dropoff_warehouse_id: str = "", storage_warehouse_id: str = "") -> list[dict]:
        """POST /v1/supply-order/bundle — товары в бандле поставки."""
        import time as _time
        all_items = []
        last_id = ""
        while True:
            payload = {
                "bundle_ids": [bundle_id],
                "item_tags_calculation": {
                    "dropoff_warehouse_id": str(dropoff_warehouse_id),
                    "storage_warehouse_ids": [str(storage_warehouse_id)] if storage_warehouse_id else [],
                },
                "limit": 100, "sort_field": "SKU",
            }
            if last_id:
                payload["last_id"] = last_id
            resp = self._post("/v1/supply-order/bundle", payload)
            all_items.extend(resp.get("items") or [])
            if resp.get("has_next") and resp.get("last_id"):
                last_id = resp["last_id"]
                _time.sleep(0.35)
            else:
                break
        return all_items

    def get_product_offer_id(self, product_id: int) -> str:
        """POST /v1/product/info/description — получить offer_id по product_id."""
        try:
            resp = self._post("/v1/product/info/description", {"product_id": product_id})
            return (resp.get("result") or {}).get("offer_id", "")
        except OzonApiError:
            return ""


# ──────────────────────────────────────────────
# Ozon Performance API (рекламные кампании)
# ──────────────────────────────────────────────

PERF_BASE = "https://api-performance.ozon.ru"


class OzonPerformanceError(Exception):
    pass


class OzonPerformanceClient:
    """Клиент для Ozon Performance API (реклама).
    Авторизация через OAuth2 client_credentials.
    Docs: https://docs.ozon.ru/api/performance/
    """

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: str | None = None

    def _refresh_token(self) -> None:
        with httpx.Client(timeout=30) as client:
            r = client.post(
                f"{PERF_BASE}/api/client/token",
                json={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                },
            )
            if not r.is_success:
                raise OzonPerformanceError(
                    f"Performance API auth {r.status_code}: {r.text[:200]}"
                )
            self._token = r.json().get("access_token") or ""

    def _headers(self) -> dict:
        if not self._token:
            self._refresh_token()
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def _get(self, path: str, params: dict = None) -> any:
        with httpx.Client(timeout=60) as client:
            r = client.get(f"{PERF_BASE}{path}", headers=self._headers(), params=params)
            if r.status_code == 401:
                self._token = None
                self._refresh_token()
                r = client.get(f"{PERF_BASE}{path}", headers=self._headers(), params=params)
            if not r.is_success:
                raise OzonPerformanceError(
                    f"Performance API {r.status_code}: {r.text[:300]}"
                )
            return r.json()

    def get_campaigns(self, page: int = 1, page_size: int = 100) -> dict:
        """Список рекламных кампаний."""
        return self._get(
            "/api/client/campaign",
            params={"page": page, "pageSize": page_size},
        )

    def get_campaign_products(self, campaign_id: str, page: int = 1, page_size: int = 100) -> dict:
        """Товары в кампании (SKU IDs для привязки к нашим SKU)."""
        return self._get(
            f"/api/client/campaign/{campaign_id}/v2/products",
            params={"page": page, "pageSize": page_size},
        )

    def get_daily_stats(
        self,
        date_from: date,
        date_to: date,
        campaign_ids: list[str] | None = None,
    ) -> list[dict]:
        """Дневная статистика по всем кампаниям (JSON).
        Возвращает список строк: campaign_id, date, views, clicks, moneySpent, orders, ordersMoney.
        """
        params: dict = {
            "dateFrom": date_from.isoformat(),
            "dateTo": date_to.isoformat(),
        }
        if campaign_ids:
            params["campaignIds"] = campaign_ids
        data = self._get("/api/client/statistics/daily/json", params=params)
        if isinstance(data, list):
            return data
        # Могут быть разные обёртки: {"rows": [...]} или {"list": [...]}
        for key in ("rows", "list", "data", "items"):
            if isinstance(data, dict) and key in data:
                return data[key] or []
        return []
