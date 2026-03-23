"""
OCR распознавание чеков из скриншотов.
Поддержка: Сбербанк, ВТБ, Тинькофф, общий формат.
"""
import io
import re
from datetime import date, datetime
from typing import Optional

from PIL import Image

try:
    import pytesseract
except ImportError:
    pytesseract = None


def parse_receipt_image(image_bytes: bytes) -> dict:
    """
    Распознаёт чек из скриншота.
    Returns: {date, amount, counterparty, description, entry_type, bank}
    """
    if pytesseract is None:
        raise ImportError("pytesseract не установлен")

    img = Image.open(io.BytesIO(image_bytes))

    # Предобработка: конвертируем в grayscale для лучшего распознавания
    img = img.convert("L")

    # OCR с русским языком
    text = pytesseract.image_to_string(img, lang="rus+eng", config="--psm 6")

    return _parse_receipt_text(text)


def parse_multiple_receipts(images: list[bytes]) -> list[dict]:
    """Распознаёт несколько чеков."""
    results = []
    for i, img_bytes in enumerate(images):
        try:
            result = parse_receipt_image(img_bytes)
            result["image_index"] = i
            results.append(result)
        except Exception as e:
            results.append({
                "image_index": i,
                "error": str(e),
                "date": None,
                "amount": 0,
                "counterparty": "",
                "description": "",
                "entry_type": "expense",
                "bank": "unknown",
            })
    return results


def _parse_receipt_text(text: str) -> dict:
    """Парсит распознанный текст чека."""
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    full_text = " ".join(lines)

    # Определяем банк
    bank = _detect_bank(full_text)

    # Извлекаем данные
    receipt_date = _extract_date(full_text, lines)
    amount = _extract_amount(full_text, lines)
    counterparty = _extract_counterparty(lines, bank)
    description = _extract_description(lines, bank)
    entry_type = _determine_type(full_text, lines)

    return {
        "date": receipt_date,
        "amount": amount,
        "counterparty": counterparty,
        "description": description,
        "entry_type": entry_type,
        "bank": bank,
        "raw_text": text,
    }


def _detect_bank(text: str) -> str:
    text_lower = text.lower()
    if "сбер" in text_lower or "sber" in text_lower:
        return "sber"
    if "втб" in text_lower or "vtb" in text_lower:
        return "vtb"
    if "тинькофф" in text_lower or "тбанк" in text_lower or "tinkoff" in text_lower:
        return "tinkoff"
    if "альфа" in text_lower or "alfa" in text_lower:
        return "alfa"
    return "unknown"


def _extract_date(text: str, lines: list[str]) -> Optional[str]:
    """Извлекает дату из чека."""
    # Формат: 10 февраля 2026 12:10:15
    months_ru = {
        "января": "01", "февраля": "02", "марта": "03", "апреля": "04",
        "мая": "05", "июня": "06", "июля": "07", "августа": "08",
        "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12",
    }
    for month_name, month_num in months_ru.items():
        m = re.search(r"(\d{1,2})\s+" + month_name + r"\s+(\d{4})", text)
        if m:
            day = int(m.group(1))
            year = int(m.group(2))
            return f"{year}-{month_num}-{day:02d}"

    # Формат: DD.MM.YYYY
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # Формат: YYYY-MM-DD
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return m.group(0)

    return date.today().isoformat()


def _extract_amount(text: str, lines: list[str]) -> float:
    """Извлекает сумму из чека."""
    # Ищем "Сумма перевода" или "Сумма" с числом
    for line in lines:
        if re.search(r"(?i)сумма\s*(перевод|платеж|операци)?", line):
            continue  # Это заголовок, число на следующей строке

    # Ищем сумму после "Сумма перевода" / "Сумма"
    for i, line in enumerate(lines):
        if re.search(r"(?i)сумма", line):
            # Число может быть на той же строке или следующей
            amount = _find_amount_in_text(line)
            if amount and amount > 0:
                return amount
            if i + 1 < len(lines):
                amount = _find_amount_in_text(lines[i + 1])
                if amount and amount > 0:
                    return amount

    # Ищем любое число с ₽ или руб
    m = re.search(r"(\d[\d\s]*[\d,]\d{2})\s*[₽руб]", text)
    if m:
        return _parse_number(m.group(1))

    # Ищем крупное число (вероятно сумма)
    amounts = []
    for m in re.finditer(r"(\d[\d\s]*\d),(\d{2})", text):
        val = _parse_number(m.group(0))
        if val > 0:
            amounts.append(val)
    if amounts:
        return max(amounts)

    return 0.0


def _find_amount_in_text(text: str) -> Optional[float]:
    """Находит сумму в строке."""
    # 1 000,00 ₽ или 1000.00 или 1 000,00
    m = re.search(r"(\d[\d\s]*[\d,\.]\d{2})", text)
    if m:
        return _parse_number(m.group(1))
    return None


def _parse_number(s: str) -> float:
    """Парсит число: 1 000,00 → 1000.0"""
    s = s.strip()
    s = re.sub(r"\s+", "", s)  # убираем пробелы
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _extract_counterparty(lines: list[str], bank: str) -> str:
    """Извлекает получателя/контрагента."""
    # Сбер: после "ФИО получателя"
    for i, line in enumerate(lines):
        if re.search(r"(?i)фио получател|получатель", line):
            if i + 1 < len(lines):
                name = lines[i + 1].strip()
                if name and not re.match(r"(?i)номер|карт|счёт|счет", name):
                    return name

    # После "Наименование" / "Контрагент"
    for i, line in enumerate(lines):
        if re.search(r"(?i)наименовани|контрагент", line):
            if i + 1 < len(lines):
                return lines[i + 1].strip()

    # Ищем ФИО (Фамилия Имя О.)
    for line in lines:
        m = re.match(r"^([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.?)$", line)
        if m:
            return m.group(1)

    return ""


def _extract_description(lines: list[str], bank: str) -> str:
    """Извлекает назначение/описание операции."""
    # После "Операция" / "Назначение"
    for i, line in enumerate(lines):
        if re.search(r"(?i)^операция$|^назначение", line):
            if i + 1 < len(lines):
                return lines[i + 1].strip()

    # Ищем "Перевод клиенту" и подобное
    for line in lines:
        if re.search(r"(?i)перевод|оплата|платёж|покупка|списание", line):
            return line.strip()

    return ""


def _determine_type(text: str, lines: list[str]) -> str:
    """Определяет тип: расход или доход."""
    text_lower = text.lower()
    if any(w in text_lower for w in ["зачислен", "поступлен", "возврат", "кэшбэк"]):
        return "income"
    # По умолчанию чеки — расходы (платишь с карты)
    return "expense"
