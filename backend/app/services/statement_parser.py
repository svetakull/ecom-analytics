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
    # (regex pattern на назначение+контрагент, category, confidence)

    # === Переводы (не доход и не расход) ===
    (r"(?i)перераспределени[ея].*собствен|перевод.*между.*счет|со счёта на счёт", "_transfer", 0.95),
    (r"(?i)перевод.*денежных.*средств.*на нужды", "_transfer", 0.9),
    (r"(?i)размещение.*средств.*депозит", "_transfer_out", 0.95),  # минус — уход на депозит
    (r"(?i)возврат.*депозит", "_transfer_in", 0.95),  # плюс — возврат с депозита

    # === Услуги ПВЗ (приоритет над контрагентами — РВБ может платить за ПВЗ) ===
    (r"(?i)оплата.*за.*оказани.*услуг.*пвз|услуги.*пвз|вознаграждени.*пвз|за.*пвз\b", "income_pvz", 0.95),  # доход от ПВЗ

    # === Фулфилмент (высокий приоритет: перехватывает по ключевому слову) ===
    (r"(?i)фулфилмент|fulfilment|fulfillment|фулфилм", "ff", 0.95),  # услуги ФФ
    (r"(?i)цуркан.*владимир|ИП\s*Цуркан", "ff", 0.95),  # ИП Цуркан — ФФ
    (r"(?i)памико|ООО\s*[\"«]?памико", "ff", 0.95),  # ООО ПАМИКО — ФФ

    # === Контрагенты (точные совпадения, высокий приоритет) ===
    (r"(?i)\bРВБ\b|ООО.*РВБ", "income_wb", 0.95),  # поступление от ВБ
    (r"(?i)купишуз|kupishuz", "income_lamoda", 0.95),  # поступление от Lamoda
    (r"(?i)интернет.?технологии|ООО.*интернет.*технолог|интернет.?решени[ея]|ООО.*интернет.*решен", "income_ozon", 0.95),  # поступление от Ozon
    (r"(?i)АО.*ТБанк|тинькофф|tinkoff|финансов.*и.*платёжн.*технолог|финансовые и платежные технологии", "income_site", 0.9),  # сайт
    (r"(?i)ГУРИНА\s+ЯНА|гурина.*олеговна", "income_opt", 0.9),  # оптовые продажи
    (r"(?i)эквайринг.*мерчант|зачисление.*эквайринг", "income_site", 0.9),  # эквайринг сайт
    (r"(?i)улюмжуев|ИП.*Улюмжуев", "rent_pvz", 0.9),  # аренда ПВЗ
    (r"(?i)фальковский|ИП.*ФАЛЬКОВСКИЙ", "warehouse", 0.9),  # склад Калмыкия
    (r"(?i)NOVYE\s*GORIZON|новые горизонт|RK\*OOONOVYE", "subscriptions", 0.9),  # подписка на сервисы
    (r"(?i)КРАВЧЕНКО.*ОКСАНА|ИП.*КРАВЧЕНКО", "salary_employee", 0.9),  # менеджер МП
    (r"(?i)GREBENYUK|гребенюк", "education", 0.9),  # обучение
    (r"(?i)YANDEX.*GO|яндекс.*го|yandex.*taxi", "travel", 0.85),  # такси → командировка
    (r"(?i)MKEEPER|мкипер", "subscriptions", 0.9),  # сервисы
    (r"(?i)ROSTELECOM|ростелеком", "pvz", 0.85),  # видеонаблюдение ПВЗ
    (r"(?i)PURCHASE_CB|покупка.*purchase", "other", 0.5),  # покупки по карте (не закупка Китай!)

    # === МП по названиям ===
    (r"(?i)wildberries|вайлдберриз|\bвб\b", "mp_payment", 0.9),
    (r"(?i)ozon|озон", "mp_payment", 0.9),
    (r"(?i)lamoda|ламода", "mp_payment", 0.9),

    # === Назначение платежа ===
    (r"(?i)дневн.*уход.*за.*ребёнк|дневного ухода за ребенком", "salary_manager", 0.9),
    (r"(?i)аванс.*по.*заработной|аванс.*зарплат", "salary_pvz", 0.9),  # зарплата ПВЗ
    (r"(?i)выплат.*самозанят", "outsource_accountant", 0.85),  # бухгалтер или СММ
    (r"(?i)бухгалтерск.*услуг|бухгалтер", "outsource_accountant", 0.85),
    (r"(?i)зарплат[аы]|з/?п\b|фот\b|оплата труда", "salary", 0.9),
    (r"(?i)аренд[аы]|арендная плата", "warehouse", 0.85),
    (r"(?i)усн\b|упрощ[её]н|налог(?!.*ндфл)(?!.*тамож)|авансов.*плат.*налог", "usn", 0.85),
    (r"(?i)страхов.*взнос|пфр|фсс|фомс", "insurance", 0.85),
    (r"(?i)ндфл", "ndfl", 0.85),
    (r"(?i)таможен|таможня|customs", "customs", 0.85),  # таможенные платежи
    (r"(?i)автомобильн.*фрахт|авиафрахт|карго|cargo", "delivery_china", 0.85),  # доставка из Китая
    (r"(?i)закупк[аи]|поставщик|китай|china|supplier|purchase", "purchase_china", 0.8),
    (r"(?i)доставк[аи]|логистик|сдэк|cdek|почта\s*росси|dpd|boxberry", "delivery_rf", 0.8),
    (r"(?i)фулфилмент|\bфф\b|fulfillment|услуги фулфилмента", "ff", 0.85),
    (r"(?i)монтаж.*видео|съёмк[аи]|фотосъёмк|фотосъемк|видеосъёмк|видеосъемк", "content", 0.85),
    (r"(?i)контент|дизайн", "content", 0.7),
    (r"(?i)смм|smm|контент.?стратег|продвижени.*внешн|создание.*смм|создание.*стратег", "external_ads", 0.85),
    (r"(?i)YANDEX.*DIRECT|яндекс.*директ", "external_ads_site", 0.9),  # Яндекс Директ — реклама сайта
    (r"(?i)реклам[аы]|google.*ads|vk.*ads|таргет", "external_ads", 0.8),
    (r"(?i)процент.*по.*депозит|уплата.*процент.*депозит", "_income_deposit", 0.9),  # доход от депозита
    (r"(?i)ВОЛГО.*ВЯТСКИЙ.*БАНК.*СБЕРБАНК|кредит.*сбербизнес|сбербизнес.*кредит", "bank_credit", 0.9),  # кредит СберБизнес
    (r"(?i)кредит|займ|заём", "bank_credit", 0.75),
    (r"(?i)процент.*по.*кредит|процент.*займ", "credit_interest", 0.8),
    (r"(?i)комисси.*банк|расчётно.*кассов|рко|обслуживани.*счёт|тариф", "bank_fees", 0.85),
    (r"(?i)обращени.*с.*твёрд.*коммунальн|тко|коммунальн.*отход", "pvz", 0.8),  # расходы ПВЗ
    (r"(?i)оборудовани|техник|компьютер|принтер", "equipment", 0.7),
    (r"(?i)обучени|курс|тренинг|семинар", "education", 0.7),
    (r"(?i)подписк[аи]|сервис|saas|crm|erp", "subscriptions", 0.7),
    (r"(?i)командировк|перелёт|гостиниц|отель", "travel", 0.75),
    (r"(?i)курьер|экспресс.*доставк", "courier", 0.75),
    (r"(?i)дивиденд|выплат.*учредител", "dividend_investor", 0.8),
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


def detect_bank(filename: str, rows: list[dict]) -> str:
    """Определяет название банка из имени файла или содержимого выписки."""
    haystack = filename.lower()
    # Объединяем первые несколько строк описания в строку поиска
    haystack += " " + " ".join(
        (r.get("counterparty", "") + " " + r.get("description", ""))
        for r in rows[:10]
    ).lower()

    banks = [
        # порядок важен: более специфичные паттерны сначала
        (("сбербизнес", "сбер бизнес", "sberbusiness"), "Сбербизнес"),
        (("сбер", "sber"), "Сбербанк"),
        (("тинькофф", "тбанк", "tinkoff", "tbank"), "Тинькофф"),
        (("точка", "tochka"), "Точка"),
        (("альфа", "alfa", "alpha"), "Альфа-Банк"),
        (("втб", "vtb"), "ВТБ"),
        (("райффайзен", "raiffeisen"), "Райффайзен"),
        (("модуль", "modulbank"), "Модульбанк"),
        (("открытие",), "Открытие"),
        (("газпромбанк", "gpb"), "Газпромбанк"),
        (("росбанк",), "Росбанк"),
        (("пси", "psb", "промсвязь"), "Промсвязьбанк"),
        (("юникредит", "unicredit"), "Юникредит"),
    ]
    for patterns, name in banks:
        if any(p in haystack for p in patterns):
            return name
    return "Банковская выписка"


def classify_entries(rows: list[dict]) -> list[dict]:
    """
    Авто-классифицирует строки выписки по категориям.
    Добавляет поля: auto_category, confidence.
    """
    result = []
    for row in rows:
        description = (row.get("description") or "") + " " + (row.get("counterparty") or "")
        category, confidence = _classify_description(description)

        # Определяем тип: доход, расход или перевод
        amount = row.get("amount", 0)
        if category in ("_transfer", "_transfer_in", "_transfer_out"):
            entry_type = "transfer"
            category = "other"
        elif category == "_income_deposit":
            entry_type = "income"
            category = "other"
        elif category.startswith("income_"):
            entry_type = "income"  # все income_ категории = доход
        else:
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

    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)

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
    date_col = debit_col = credit_col = corr_col = desc_col = None
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
        elif "назначение" in hl:
            desc_col = j
        elif "счет" in hl and "дебет" not in hl and "кредит" not in hl and corr_col is None:
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

        # Назначение платежа (столбец 21 в расширенном Сбер формате)
        description = ""
        if desc_col is not None and desc_col < len(row) and row[desc_col]:
            description = str(row[desc_col]).replace("\n", " ").strip()

        # Контрагент — из столбца Счёт (Дебет/Кредит)
        # В Сбер формате контрагент в многострочной ячейке: Счёт\nИНН\nНазвание
        counterparty = ""
        # Для расхода (дебет) контрагент в столбце Кредит (куда ушли деньги)
        # Для дохода (кредит) контрагент в столбце Дебет (откуда пришли)
        src_col = corr_col  # fallback
        if credit > 0 and corr_col is not None:
            # Доход — контрагент в столбце "Дебет" (col 5 в Сбер)
            for check_col in range(4, min(8, len(row))):
                cell = row[check_col] if check_col < len(row) else None
                if cell and isinstance(cell, str) and "\n" in cell:
                    lines = cell.split("\n")
                    if len(lines) >= 3:
                        counterparty = lines[2].strip()[:100]
                    break
        elif debit > 0 and corr_col is not None:
            # Расход — контрагент в столбце "Кредит" (col 9 в Сбер)
            for check_col in range(8, min(14, len(row))):
                cell = row[check_col] if check_col < len(row) else None
                if cell and isinstance(cell, str) and "\n" in cell:
                    lines = cell.split("\n")
                    if len(lines) >= 3:
                        counterparty = lines[2].strip()[:100]
                    break

        # Корр.счёт для классификации если нет назначения
        corr_account = ""
        if corr_col is not None and corr_col < len(row) and row[corr_col]:
            raw_corr = str(row[corr_col])
            corr_account = raw_corr.split("\n")[0].strip() if "\n" in raw_corr else raw_corr

        if not description:
            if corr_account.startswith("407"):
                description = "Перевод контрагенту"
            elif corr_account.startswith("408"):
                description = "Перевод ИП/ООО"
            elif corr_account.startswith("302") or corr_account.startswith("303"):
                description = "Банковская операция"
            elif corr_account.startswith("454") or corr_account.startswith("455"):
                description = "Кредит/займ"
            elif corr_account.startswith("615"):
                description = "Прочие доходы"
            elif corr_account.startswith("706"):
                description = "Банковская комиссия"
            elif corr_account.startswith("474"):
                description = "Расчёты с покупателями"
            else:
                description = f"Корр.счёт {corr_account}"

        result.append({
            "date": parsed_date.isoformat() if isinstance(parsed_date, date) else parsed_date,
            "amount": amount,
            "counterparty": counterparty,
            "description": description,
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
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None

    s = str(value).strip()
    # Обрезаем время если есть (2026-01-04 12:54:31 → 2026-01-04)
    if " " in s:
        s = s.split(" ")[0]
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
