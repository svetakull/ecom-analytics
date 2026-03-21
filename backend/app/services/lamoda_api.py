"""
Lamoda API client.

Два эндпоинта:
  B2B REST API  — https://api-b2b.lamoda.ru          (заказы, остатки FBO, возвраты)
  Seller API    — https://lk.lamoda.ru/jsonrpc        (номенклатуры с ценами и фото)

Авторизация: OAuth2 client_credentials, Bearer токен TTL ~15 мин.
Клиент автоматически обновляет токен при истечении.
"""
import time
import threading
import logging
from datetime import date, datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

LAMODA_AUTH_URL = "https://api-b2b.lamoda.ru/auth/token"
LAMODA_B2B_BASE = "https://api-b2b.lamoda.ru"
LAMODA_SELLER_BASE = "https://seller.lamoda.ru"
TOKEN_TTL_MARGIN = 60  # обновляем за 60 сек до истечения


class LamodaApiError(Exception):
    pass


class LamodaClient:
    """
    Клиент Lamoda API с автоматическим обновлением Bearer-токена.
    Потокобезопасен — использует threading.Lock для обновления токена.
    """

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._lock = threading.Lock()

    # ─── Auth ──────────────────────────────────────────────────────────────

    def _refresh_token(self) -> None:
        """Получить новый Bearer-токен через client_credentials."""
        with httpx.Client(timeout=15) as client:
            r = client.post(
                LAMODA_AUTH_URL,
                json={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                },
            )
            if r.status_code == 401:
                raise LamodaApiError("Lamoda: неверный client_id/client_secret")
            if not r.is_success:
                raise LamodaApiError(f"Lamoda auth error {r.status_code}: {r.text[:200]}")

            data = r.json()
            self._token = data.get("access_token") or data.get("token")
            if not self._token:
                raise LamodaApiError(f"Lamoda auth: нет access_token в ответе: {data}")

            # TTL из ответа или 15 минут по умолчанию
            expires_in = data.get("expires_in", 900)
            self._token_expires_at = time.time() + expires_in - TOKEN_TTL_MARGIN
            logger.debug("Lamoda token refreshed, expires in %ss", expires_in)

    def _get_token(self) -> str:
        """Вернуть действующий токен, обновить при необходимости."""
        with self._lock:
            if not self._token or time.time() >= self._token_expires_at:
                self._refresh_token()
            return self._token  # type: ignore[return-value]

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._get_token()}"}

    # ─── B2B REST API helpers ──────────────────────────────────────────────

    def _b2b_get(self, path: str, params: dict = None) -> dict:
        """GET запрос к B2B REST API с пагинацией на одну страницу."""
        with httpx.Client(timeout=30) as client:
            r = client.get(
                f"{LAMODA_B2B_BASE}{path}",
                headers=self._headers(),
                params=params or {},
            )
            if r.status_code == 401:
                # Принудительное обновление токена и повтор
                with self._lock:
                    self._token = None
                r2 = client.get(
                    f"{LAMODA_B2B_BASE}{path}",
                    headers=self._headers(),
                    params=params or {},
                )
                if not r2.is_success:
                    raise LamodaApiError(f"Lamoda B2B {path} {r2.status_code}: {r2.text[:200]}")
                return r2.json()
            if not r.is_success:
                raise LamodaApiError(f"Lamoda B2B {path} {r.status_code}: {r.text[:200]}")
            return r.json()

    def _b2b_get_all(self, path: str, embedded_key: str, params: dict = None) -> list[dict]:
        """Пройти все страницы B2B REST API и вернуть объединённый список."""
        params = dict(params or {})
        params.setdefault("limit", 100)
        page = 1
        all_items = []
        while True:
            params["page"] = page
            data = self._b2b_get(path, params)
            items = data.get("_embedded", {}).get(embedded_key, [])
            all_items.extend(items)
            if page >= data.get("pages", 1):
                break
            page += 1
        return all_items

    # ─── Seller JSON-RPC helper ────────────────────────────────────────────

    def _jsonrpc(self, method: str, params: dict) -> dict:
        """Вызов Lamoda Seller Partner API (JSON-RPC 2.0)."""
        payload = {
            "jsonrpc": "2.0",
            "id": f"rnp-{method}",
            "method": method,
            "params": params,
        }
        with httpx.Client(timeout=30) as client:
            r = client.post(
                f"{LAMODA_SELLER_BASE}/jsonrpc/",
                headers={**self._headers(), "Content-Type": "application/json"},
                json=payload,
            )
            if not r.is_success:
                raise LamodaApiError(f"Lamoda jsonrpc {method} {r.status_code}: {r.text[:200]}")
            data = r.json()
            if "error" in data:
                raise LamodaApiError(f"Lamoda jsonrpc {method} error: {data['error']}")
            return data.get("result", {})

    def _jsonrpc_all(self, method: str, params: dict) -> list[dict]:
        """Пройти все страницы JSON-RPC и вернуть объединённый список номенклатур."""
        params = dict(params)
        params.setdefault("limit", 100)
        page = 1
        all_items = []
        while True:
            params["page"] = page
            result = self._jsonrpc(method, params)
            items = result.get("nomenclatures", [])
            all_items.extend(items)
            total_pages = result.get("pages", 1)
            if page >= total_pages:
                break
            page += 1
        return all_items

    # ─── Public methods ────────────────────────────────────────────────────

    def get_orders(self, updated_from: Optional[datetime] = None,
                   days_back: int = 30) -> list[dict]:
        """
        Список заказов с детализацией.
        updated_from: инкрементальный синк по updatedAt
        days_back: полный синк за N дней
        """
        params: dict = {"limit": 100}
        if updated_from:
            ts = updated_from.strftime("%Y%m%d%H%M%S")
            params["filter"] = f"updatedAt>={ts}"
        else:
            ts = datetime.now().replace(
                year=datetime.now().year
            ).strftime("%Y%m%d%H%M%S")
            from_dt = date.today().__class__.fromordinal(
                date.today().toordinal() - days_back
            )
            from_ts = datetime(from_dt.year, from_dt.month, from_dt.day).strftime("%Y%m%d%H%M%S")
            now_ts = datetime.now().strftime("%Y%m%d%H%M%S")
            params["filter"] = f"updatedAt>=<{from_ts},{now_ts}"
        return self._b2b_get_all("/api/v1/orders", "orders", params)

    def get_order_detail(self, order_nr: str) -> dict:
        """Детальная информация о заказе с позициями (items)."""
        return self._b2b_get(f"/api/v1/orders/{order_nr}")

    def get_stock(self) -> list[dict]:
        """Полный сток FBO: возвращает [{sku, quantity}, ...]."""
        return self._b2b_get_all(
            "/api/v1/stock/goods", "stockStates",
            {"withZeroQuantity": 1, "limit": 100},
        )

    def get_stock_delta(self, updated_from: datetime) -> list[dict]:
        """Дельта стока с момента updated_from."""
        ts = updated_from.strftime("%Y-%m-%d+%H:%M:%S")
        return self._b2b_get_all(
            "/api/v1/stock/goods", "stockStates",
            {"updatedAt": ts, "withZeroQuantity": 1, "limit": 100},
        )

    def get_nomenclatures(self) -> list[dict]:
        """
        Список номенклатур с ценами и фото через Seller Partner API.
        Возвращает list[{nomenclature: {...}}].
        """
        return self._jsonrpc_all(
            "v1.nomenclatures.list",
            {"country": "RU", "limit": 100},
        )
