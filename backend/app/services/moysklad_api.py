"""
API клиент для МойСклад (api.moysklad.ru).
Используется для сверки поставок с маркетплейсами.
"""
import logging
import time
from datetime import date
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
LIMIT = 100  # МойСклад не разворачивает expand при limit>100
REQUEST_DELAY = 0.25  # 250ms между запросами


class MoySkladApiError(Exception):
    pass


class MoySkladClient:
    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json",
        }

    def _get(self, url: str, params: dict = None) -> dict:
        with httpx.Client(timeout=60) as client:
            r = client.get(url, headers=self.headers, params=params)
            if r.status_code == 401:
                raise MoySkladApiError("Неверный токен МойСклад")
            if r.status_code == 429:
                raise MoySkladApiError("Превышен лимит запросов МойСклад API")
            if not r.is_success:
                raise MoySkladApiError(f"МойСклад API error {r.status_code}: {r.text[:300]}")
            return r.json()

    def test_connection(self) -> bool:
        """Проверить соединение — запросить список организаций."""
        try:
            result = self._get(f"{BASE_URL}/entity/organization", {"limit": 1})
            return "rows" in result
        except Exception:
            return False

    def get_customer_orders(
        self,
        date_from: date,
        date_to: date,
        agent_name: Optional[str] = None,
        organization_name: Optional[str] = None,
    ) -> list[dict]:
        """
        Получить заказы покупателей за период с фильтром по контрагенту и организации.
        Возвращает список заказов с развёрнутыми agent, organization, store, state.
        """
        filter_str = (
            f"moment>={date_from.isoformat()} 00:00:00;"
            f"moment<={date_to.isoformat()} 23:59:59"
        )
        all_orders = []
        offset = 0

        while True:
            result = self._get(
                f"{BASE_URL}/entity/customerorder",
                {
                    "filter": filter_str,
                    "limit": LIMIT,
                    "offset": offset,
                    "expand": "agent,organization,store,state",
                },
            )
            rows = result.get("rows", [])
            if not rows:
                break

            for order in rows:
                # Фильтр по контрагенту (мягкий — содержит подстроку)
                if agent_name:
                    order_agent = (order.get("agent") or {}).get("name", "")
                    if agent_name.lower() not in order_agent.lower():
                        continue
                # Фильтр по организации
                if organization_name:
                    order_org = (order.get("organization") or {}).get("name", "")
                    if organization_name.lower() not in order_org.lower():
                        continue
                all_orders.append(order)

            if len(rows) < LIMIT:
                break
            offset += LIMIT
            time.sleep(REQUEST_DELAY)

        logger.info("MoySklad: got %d orders (agent=%s, org=%s)", len(all_orders), agent_name, organization_name)
        return all_orders

    def get_order_details(self, order_id: str) -> dict:
        """Получить детали заказа с развёрнутыми полями."""
        return self._get(
            f"{BASE_URL}/entity/customerorder/{order_id}",
            {"expand": "agent,organization,store,state"},
        )

    def get_order_positions(self, order_id: str) -> list[dict]:
        """
        Получить позиции (товары) заказа.
        Возвращает список позиций с развёрнутым assortment.
        """
        all_positions = []
        offset = 0

        while True:
            result = self._get(
                f"{BASE_URL}/entity/customerorder/{order_id}/positions",
                {"limit": LIMIT, "offset": offset, "expand": "assortment"},
            )
            rows = result.get("rows", [])
            all_positions.extend(rows)
            if len(rows) < LIMIT:
                break
            offset += LIMIT
            time.sleep(REQUEST_DELAY)

        return all_positions

    @staticmethod
    def extract_position_data(position: dict) -> dict:
        """Извлечь данные позиции в удобный формат."""
        assortment = position.get("assortment") or {}
        # article = name товара (seller article), не код МП (nmId)
        # Apps Script: getProductName -> assortment.name
        return {
            "article": assortment.get("name", "") or assortment.get("article", "Неизвестный товар"),
            "name": assortment.get("name", "Неизвестный товар"),
            "quantity": position.get("quantity", 0),
            "price": (position.get("price") or 0) / 100,  # МойСклад хранит в копейках
            "uom": ((assortment.get("uom") or {}).get("name", "шт")),
        }

    @staticmethod
    def extract_order_meta(order: dict) -> dict:
        """Извлечь мета-данные заказа."""
        agent = order.get("agent") or {}
        org = order.get("organization") or {}
        state = order.get("state") or {}
        store = order.get("store") or {}

        return {
            "order_number": order.get("name", ""),
            "date": order.get("moment", ""),
            "agent": agent.get("name", ""),
            "organization": org.get("name", ""),
            "status": state.get("name", "Проведен" if order.get("applicable") else "Черновик"),
            "store": store.get("name", ""),
        }
