"""
Парсинг банковских выписок (xlsx/csv) и авто-классификация операций.
Поддерживает: 1С выписки, Сбербанк, Тинькофф, Точка.
"""
import csv
import io
import re
from datetime import date, datetime
from typing import Optional

# --- Правила авто-классификации ---

CLASSIFICATION_RULES = [
    # (regex pattern на назначение платежа, category, confidence)
    (r"(?i)зарплат[аы]|з/?п\b|фот\b|оплата труда", "salary", 0.9),
    (r"(?i)аренд[аы]|арендная плата|склад.*аренд", "warehouse", 0.85),
    (r"(?i)усн\b|упрощ[её]н|налог(?!.*ндфл)|авансов.*плат.*налог", "usn", 0.85),
    (r"(?i)страхов.*взнос|пфр|фсс|фомс", "insurance", 0.85),
    (r"(?i)ндфл", "ndfl", 0.85),
    (r"(?i)wildberries|вайлдберриз|\bвб\b", "mp_payment", 0.9),
    (r"(?i)ozon|озон", "mp_payment", 0.9),
    (r"(?i)закупк[аи]|поставщик|китай|china|supplier|purchase", "purchase_china", 0.8),
    (r"(?i)доставк[аи]|логистик|сдэк|cdek|почта\s*росси|dpd|boxberry", "delivery_rf", 0.8),
    (r"(?i)реклам[аы]|продвижени|яндекс.*директ|google.*ads|vk.*ads|таргет", "external_ads", 0.8),
    (r"(?i)кредит|займ|заём|процент.*кредит", "bank_credit", 0.75),
    (r"(?i)процент.*по.*кредит|процент.*займ", "credit_interest", 0.8),
    (r"(?i)комисси.*банк|расчётно.*кассов|рко|обслуживани.*счёт", "bank_fees", 0.85),
    (r"(?i)оборудовани|техник|компьютер|принтер", "equipment", 0.7),
    (r"(?i)обучени|курс|тренинг|семинар", "education", 0.7),
    (r"(?i)подписк[аи]|сервис|saas|crm|erp", "subscriptions", 0.7),
    (r"(?i)командировк|перелёт|гостиниц|отель", "travel", 0.75),
    (r"(?i)курьер|экспресс.*доставк", "courier", 0.75),
    (r"(?i)дивиденд|выплат.*учредител", "dividend_investor", 0.8),
    (r"(?i)фулфилмент|\bфф\b|fulfillment", "ff", 0.8),
    (r"(?i)контент|фото.*съёмк|видео.*съёмк|дизайн", "content", 0.7),
    (r"(?i)выкуп|самовыкуп", "buyout_services", 0.75),
    (r"(?i)офис|канцеляр|хозтовар", "office", 0.7),
]


def parse_statement(file_bytes: bytes, filename: str) -> list[dict]:
    """
    Парсит xlsx или csv банковскую выписку.
    Возвращает список строк с полями: date, amount, counterparty, description.
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext == "xlsx":
        return _parse_xlsx(file_bytes)
    elif ext == "csv":
        return _parse_csv(file_bytes)
    else:
        raise ValueError(f"Unsupported file format: {ext}. Expected xlsx or csv.")


def classify_entries(rows: list[dict]) -> list[dict]:
    """
    Авто-классифицирует строки выписки по категориям.
    Добавляет поля: auto_category, confidence.
    """
    result = []
    for row in rows:
        description = (row.get("description") or "") + " " + (row.get("counterparty") or "")
        category, confidence = _classify_description(description)

        # Определяем тип: доход или расход
        amount = row.get("amount", 0)
        entry_type = "income" if amount > 0 else "expense"

        result.append({
            "date": row.get("date"),
            "amount": abs(amount),
            "counterparty": row.get("counterparty", ""),
            "description": row.get("description", ""),
            "auto_category": category,
            "confidence": confidence,
            "entry_type": entry_type,
        })
    return result


def _classify_description(text: str) -> tuple[str, float]:
    """Классифицирует по назначению платежа с помощью regex-правил."""
    if not text:
        return "other", 0.0

    for pattern, category, confidence in CLASSIFICATION_RULES:
        if re.search(pattern, text):
            return category, confidence

    return "other", 0.0


def _parse_xlsx(file_bytes: bytes) -> list[dict]:
    """Парсит xlsx файл банковской выписки."""
    try:
        import openpyxl
    except ImportError:
        raise ImportError("openpyxl is required for xlsx parsing. pip install openpyxl")

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)

    all_results = []
    for ws in wb.worksheets:
        rows_data = list(ws.iter_rows(values_only=True))
        if not rows_data:
            continue

        # Определяем валюту листа (пропускаем юаневые счета)
        sheet_text = " ".join(str(c) for row in rows_data[:6] for c in row if c)
        if "Китайский юань" in sheet_text or "CNY" in sheet_text:
            continue  # юаневые операции пока пропускаем

        # Пропускаем пустые листы (только ИТОГО)
        data_rows = [r for r in rows_data[7:] if r and any(c for c in r)]
        if len(data_rows) <= 1:
            continue

        # Сначала проверим формат СберБизнес
        sber = _try_parse_sber(rows_data)
        if sber is not None:
            all_results.extend(sber)
            continue

        # Стандартный парсинг (ВТБ, Тинькофф и др.)
        header_row = _find_header_row(rows_data)
        if header_row is None:
            continue

        headers = rows_data[header_row]
        col_map = _map_columns(headers)

        for row in rows_data[header_row + 1:]:
            parsed = _parse_row(row, col_map)
            if parsed and parsed.get("amount"):
                all_results.append(parsed)

    wb.close()
    return all_results


def _try_parse_sber(rows_data: list) -> Optional[list[dict]]:
    """
    Парсит формат СберБизнес.
    Шапка: Дата | ВО | Номер | ... | БИК | Корр.счёт | Сумма по дебету | Сумма по кредиту
    Без назначения платежа и контрагента.
    """
    # Ищем строку с "Сумма по дебету"
    header_idx = None
    for i, row in enumerate(rows_data[:25]):
        if not row:
            continue
        row_text = " ".join(str(c).lower() for c in row if c)
        if "сумма по дебету" in row_text and "сумма по кредиту" in row_text:
            header_idx = i
            break

    if header_idx is None:
        return None  # не формат Сбер

    headers = rows_data[header_idx]

    # Маппим столбцы
    date_col = debit_col = credit_col = corr_col = None
    for j, h in enumerate(headers):
        if h is None:
            continue
        hl = str(h).lower().strip()
        if "дата" in hl and date_col is None:
            date_col = j
        elif "дебету" in hl:
            debit_col = j
        elif "кредиту" in hl:
            credit_col = j
        elif "корреспондирующий" in hl:
            corr_col = j

    if date_col is None or (debit_col is None and credit_col is None):
        return None

    # Ищем входящий остаток
    opening_balance = 0.0
    for i, row in enumerate(rows_data[:header_idx]):
        row_text = " ".join(str(c).lower() for c in row if c)
        if "входящий остаток" in row_text:
            for c in row:
                if isinstance(c, (int, float)) and c > 0:
                    opening_balance = float(c)
                    break

    result = []
    for row in rows_data[header_idx + 1:]:
        if not row:
            continue

        # Дата
        raw_date = row[date_col] if date_col < len(row) else None
        parsed_date = _parse_date(raw_date)
        if not parsed_date:
            continue

        # Дебет / Кредит
        debit = _parse_amount(row[debit_col] if debit_col is not None and debit_col < len(row) else None)
        credit = _parse_amount(row[credit_col] if credit_col is not None and credit_col < len(row) else None)

        if not debit and not credit:
            continue

        # Дебет = расход (списание), Кредит = доход (поступление)
        if credit > 0:
            amount = credit  # доход
        else:
            amount = -debit  # расход

        # Корр.счёт для классификации
        corr_account = ""
        if corr_col is not None and corr_col < len(row) and row[corr_col]:
            corr_account = str(row[corr_col])

        # Пытаемся определить контрагента по корр.счёту
        counterparty = ""
        description = ""
        if corr_account.startswith("407"):
            description = "Перевод между счетами / контрагенту"
        elif corr_account.startswith("408"):
            description = "Перевод ИП/ООО"
        elif corr_account.startswith("302") or corr_account.startswith("303"):
            description = "Банковская операция (МЦС/переводы)"
        elif corr_account.startswith("454") or corr_account.startswith("455"):
            description = "Кредит/займ"
        elif corr_account.startswith("615"):
            description = "Прочие доходы"
        elif corr_account.startswith("706"):
            description = "Банковская комиссия"
        elif corr_account.startswith("474"):
            description = "Расчёты с покупателями/поставщиками"

        result.append({
            "date": parsed_date.isoformat() if isinstance(parsed_date, date) else parsed_date,
            "amount": amount,
            "counterparty": counterparty,
            "description": description or f"Корр.счёт {corr_account}",
        })

    return result


def _parse_csv(file_bytes: bytes) -> list[dict]:
    """Парсит csv файл банковской выписки."""
    # Пробуем разные кодировки
    text = None
    for encoding in ["utf-8", "cp1251", "windows-1251", "latin-1"]:
        try:
            text = file_bytes.decode(encoding)
            break
        except (UnicodeDecodeError, LookupError):
            continue

    if not text:
        raise ValueError("Cannot decode file. Tried utf-8, cp1251, windows-1251, latin-1.")

    # Определяем разделитель
    delimiter = ";"
    if text.count(",") > text.count(";"):
        delimiter = ","

    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    rows_data = list(reader)

    if not rows_data:
        return []

    header_row = _find_header_row_csv(rows_data)
    if header_row is None:
        return []

    headers = rows_data[header_row]
    col_map = _map_columns(headers)

    result = []
    for row in rows_data[header_row + 1:]:
        parsed = _parse_row(row, col_map)
        if parsed and parsed.get("amount"):
            result.append(parsed)

    return result


def _find_header_row(rows: list) -> Optional[int]:
    """Находит строку с заголовками в xlsx."""
    keywords = ["дата", "сумма", "назначение", "контрагент", "описание", "дебет", "кредит", "получатель"]
    for i, row in enumerate(rows[:20]):  # проверяем первые 20 строк
        if not row:
            continue
        row_text = " ".join(str(c).lower() for c in row if c)
        matches = sum(1 for kw in keywords if kw in row_text)
        if matches >= 2:
            return i
    return 0  # fallback: первая строка


def _find_header_row_csv(rows: list) -> Optional[int]:
    """Находит строку с заголовками в csv."""
    keywords = ["дата", "сумма", "назначение", "контрагент", "описание", "дебет", "кредит", "получатель"]
    for i, row in enumerate(rows[:20]):
        if not row:
            continue
        row_text = " ".join(str(c).lower() for c in row if c)
        matches = sum(1 for kw in keywords if kw in row_text)
        if matches >= 2:
            return i
    return 0


def _map_columns(headers) -> dict:
    """Маппинг заголовков к полям."""
    col_map = {}
    if not headers:
        return col_map

    for i, h in enumerate(headers):
        if h is None:
            continue
        hl = str(h).lower().strip()

        if any(kw in hl for kw in ["дата", "date"]):
            if "date" not in col_map:  # первая дата
                col_map["date"] = i
        elif any(kw in hl for kw in ["сумма", "amount", "итого"]):
            if "amount" not in col_map:
                col_map["amount"] = i
        elif any(kw in hl for kw in ["дебет", "debit", "приход", "списание"]):
            if "debit" not in col_map:
                col_map["debit"] = i
        elif any(kw in hl for kw in ["кредит", "credit", "расход", "зачисление"]):
            if "credit" not in col_map:
                col_map["credit"] = i
        elif any(kw in hl for kw in ["контрагент", "получатель", "плательщик", "counterparty", "наименование"]):
            if "counterparty" not in col_map:
                col_map["counterparty"] = i
        elif any(kw in hl for kw in ["назначение", "описание", "description", "основание", "комментар"]):
            if "description" not in col_map:
                col_map["description"] = i

    return col_map


def _parse_row(row, col_map: dict) -> Optional[dict]:
    """Парсит одну строку выписки."""
    if not row or not col_map:
        return None

    def safe_get(idx):
        if idx is not None and idx < len(row):
            return row[idx]
        return None

    # Пропускаем итоговые строки
    first_cell = str(row[0] if row else "").strip().lower()
    if first_cell in ("итого:", "итого", "всего", ""):
        if not any(isinstance(c, (int, float)) and c != 0 for c in row[:2]):
            pass  # может быть итого с суммами — пропустим ниже по дате

    # Date
    raw_date = safe_get(col_map.get("date"))
    parsed_date = _parse_date(raw_date)
    if not parsed_date:
        return None

    # Amount
    amount = 0.0
    if "amount" in col_map:
        amount = _parse_amount(safe_get(col_map["amount"]))
    elif "debit" in col_map and "credit" in col_map:
        debit = _parse_amount(safe_get(col_map["debit"]))
        credit = _parse_amount(safe_get(col_map["credit"]))
        # Дебет = расход (списание), Кредит = доход (поступление)
        if credit > 0:
            amount = credit  # доход — положительная сумма
        elif debit > 0:
            amount = -debit  # расход — отрицательная сумма
        else:
            amount = 0

    counterparty = str(safe_get(col_map.get("counterparty")) or "").strip()
    description = str(safe_get(col_map.get("description")) or "").strip()

    return {
        "date": parsed_date.isoformat() if isinstance(parsed_date, date) else parsed_date,
        "amount": amount,
        "counterparty": counterparty,
        "description": description,
    }


def _parse_date(value) -> Optional[date]:
    """Парсит дату из различных форматов."""
    if isinstance(value, (date, datetime)):
        return value if isinstance(value, date) else value.date()
    if not value:
        return None

    s = str(value).strip()
    for fmt in ["%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y.%m.%d"]:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_amount(value) -> float:
    """Парсит сумму из строки."""
    if isinstance(value, (int, float)):
        return float(value)
    if not value:
        return 0.0
    s = str(value).strip()
    # Убираем пробелы и заменяем запятую на точку
    s = s.replace(" ", "").replace("\u00a0", "").replace(",", ".")
    # Убираем символы валюты
    s = re.sub(r"[₽$€руб\.р\.]", "", s).strip()
    try:
        return float(s)
    except ValueError:
        return 0.0
