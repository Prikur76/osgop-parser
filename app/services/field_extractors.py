"""Извлечение полей: даты, ИНН, госномера, страховая премия."""

import re
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from app.services.plate_normalizer import to_cyr_full, normalize_plate

log = logging.getLogger(__name__)


def parse_polis_header(text: str) -> Dict[str, Any]:
    """Парсинг заголовка полиса."""
    result = {}

    log.info("=== НАЧАЛО ПАРСИНГА ПОЛИСА ===")

    # 1. НОМЕР ПОЛИСА
    rosx_match = re.search(r'ROSX\d{8,20}', text, re.IGNORECASE)
    result["contract_number"] = rosx_match.group(0).upper() if rosx_match else None
    log.info(f"Номер полиса: {result['contract_number']}")

    # 2. ДАТЫ
    result.update(extract_dates_from_polis(text))

    # 3. СТРАХОВЩИК — более гибкий поиск
    insurer_section_search = re.search(
        r'Страховщик[:\s]*(.*?)(?=\s*(?:Страхователь|Итого|Премия|Срок|ИНН|$))',
        text, re.IGNORECASE | re.DOTALL
    )

    if insurer_section_search:
        insurer_text = insurer_section_search.group(1).strip()
        log.debug(f"Текст страховщика (сырой): {insurer_text[:200]}")

        # Очистка текста
        insurer_text = re.sub(r'\([^)]*\)', '', insurer_text)
        insurer_text = re.split(r'Лицензия|ЛИЦЕНЗИЯ', insurer_text, flags=re.IGNORECASE)[0]
        insurer_text = insurer_text.replace('"', '').replace('«', '').replace('»', '').replace("'", '')
        insurer_text = re.sub(r'[\s,:;.-]+$', '', insurer_text)
        insurer_text = re.sub(r'^\s*[,:;.-]+', '', insurer_text)
        insurer_text = re.sub(r'\s+', ' ', insurer_text).strip()

        if insurer_text and len(insurer_text) > 2:
            result["insurer"] = insurer_text
            log.info(f"Страховщик: {insurer_text}")

    # ИНН страховщика (DOTALL — ИНН может быть на следующей строке)
    insurer_inn_match = re.search(r'Страховщик.*?ИНН[:\s/]*(\d{10,12})', text, re.IGNORECASE | re.DOTALL)
    if insurer_inn_match:
        result["insurer_inn"] = insurer_inn_match.group(1)
        log.info(f"ИНН страховщика: {result['insurer_inn']}")
    else:
        if insurer_section_search:
            inn_in_text = re.search(r'ИНН[:\s/]*(\d{10,12})', insurer_section_search.group(0), re.IGNORECASE)
            if inn_in_text:
                result["insurer_inn"] = inn_in_text.group(1)

    # 4. СТРАХОВАТЕЛЬ
    insured_section_search = re.search(
        r'Страхователь[:\s]*(.*?)(?=\s*(?:Итого|Премия|Срок|Страховая|ИНН|$))',
        text, re.IGNORECASE | re.DOTALL
    )

    if insured_section_search:
        insured_text = insured_section_search.group(1).strip()
        log.debug(f"Текст страхователя (сырой): {insured_text[:200]}")

        insured_text = re.sub(r'\([^)]*\)', '', insured_text)
        insured_text = re.split(r'ИНН[:\s/]*КПП|ИНН', insured_text, flags=re.IGNORECASE)[0]
        insured_text = insured_text.replace('"', '').replace('«', '').replace('»', '').replace("'", '')
        insured_text = re.sub(r'[\s,:;.-]+$', '', insured_text)
        insured_text = re.sub(r'^\s*[,:;.-]+', '', insured_text)
        insured_text = re.sub(r'\s+', ' ', insured_text).strip()

        if insured_text and len(insured_text) > 2:
            result["insured"] = insured_text
            log.info(f"Страхователь: {insured_text}")

    # ИНН страхователя
    insured_inn_match = re.search(r'Страхователь.*?ИНН[:\s/]*(\d{10,12})', text, re.IGNORECASE | re.DOTALL)
    if insured_inn_match:
        result["insured_inn"] = insured_inn_match.group(1)
        log.info(f"ИНН страхователя: {result['insured_inn']}")
    else:
        if insured_section_search:
            inn_in_text = re.search(r'ИНН[:\s/]*(\d{10,12})', insured_section_search.group(0), re.IGNORECASE)
            if inn_in_text:
                result["insured_inn"] = inn_in_text.group(1)

    # Альтернативный поиск ИНН
    if not result.get("insurer_inn"):
        insurer_inn_patterns = [
            r'ИНН\s*[:\/]\s*(\d{10,12})',
            r'ИНН[^\d]*(\d{10,12})',
            r'\b(\d{10})\b.*?Страховщик',
            r'Страховщик.*?\b(\d{10})\b'
        ]
        for pattern in insurer_inn_patterns:
            inn_match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if inn_match:
                result["insurer_inn"] = inn_match.group(1)
                break

    if not result.get("insured_inn"):
        insured_inn_patterns = [
            r'Страхователь.*?ИНН\s*[:\/]\s*(\d{10,12})',
            r'Страхователь.*?\b(\d{10})\b',
            r'\b(\d{10})\b.*?Страхователь'
        ]
        for pattern in insured_inn_patterns:
            inn_match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if inn_match:
                result["insured_inn"] = inn_match.group(1)
                break

    # 5. СТРАХОВАЯ ПРЕМИЯ
    premium_patterns = [
        r'Итого\s+страховая\s+премия[:\s]*([\d\s,]+(?:\.\d{2})?)',
        r'Страховая\s+премия[:\s]*([\d\s,]+(?:\.\d{2})?)',
        r'Премия[:\s]*([\d\s,]+(?:\.\d{2})?)',
        r'([\d\s,]+(?:\.\d{2})?)\s*руб',
    ]

    for pattern in premium_patterns:
        premium_match = re.search(pattern, text, re.IGNORECASE)
        if premium_match:
            try:
                amount_str = premium_match.group(1).replace(' ', '').replace(',', '.')
                amount_str = re.sub(r'[^\d.]', '', amount_str)
                if amount_str:
                    result["bonus"] = float(amount_str)
                    log.info(f"Найдена страховая премия: {result['bonus']}")
                    break
            except (ValueError, TypeError) as e:
                log.warning(f"Ошибка парсинга премии: {e}")
                continue

    log.info("=== КОНЕЦ ПАРСИНГА ПОЛИСА ===")
    log.info(f"Результат: {result}")
    return result


def extract_dates_from_polis(text: str) -> Dict[str, Optional[str]]:
    """Извлечение всех дат из текста полиса."""
    result = {
        "contract_date": None,
        "period_from": None,
        "period_to": None
    }

    log.info(f"Поиск дат в тексте (первые 1500 символов): {text[:1500]}")

    # Нормализуем текст
    normalized_text = text.replace('«', '').replace('»', '').replace('"', '').replace('\xa0', ' ')
    normalized_text = re.sub(r'\s+', ' ', normalized_text)

    # 1. Поиск периода страхования
    period_patterns = [
        r'(?:Срок|Период)[\s-]*(?:страхования|действия)[\s:]*с\s*(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s*г?\s+по\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s*г?',
        r'с\s*(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s*г?\s+по\s*(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s*г?',
        r'Срок[\s-]*страхования[\s:]*с\s*(\d{1,2})\s+([а-яё]+)\s+(\d{4}).*?по\s*(\d{1,2})\s+([а-яё]+)\s+(\d{4})',
    ]

    for pattern in period_patterns:
        period_match = re.search(pattern, normalized_text, re.IGNORECASE)
        if period_match:
            from_day, from_month, from_year = period_match.group(1), period_match.group(2), period_match.group(3)
            to_day, to_month, to_year = period_match.group(4), period_match.group(5), period_match.group(6)

            result["period_from"] = normalize_date(from_day, from_month, from_year)
            result["period_to"] = normalize_date(to_day, to_month, to_year)

            if result["period_from"] and result["period_to"]:
                log.info(f"Найден период по паттерну: {result['period_from']} - {result['period_to']}")
                break

    # 2. Дата заключения договора
    contract_patterns = [
        r'Дата[\s-]*заключения[\s-]*(?:договора|полиса)[\s:]*(\d{1,2})\s+([а-яё]+)\s+(\d{4})',
        r'Заключен[\s:]*(\d{1,2})\s+([а-яё]+)\s+(\d{4})',
        r'Договор[\s-]*заключен[\s:]*(\d{1,2})\s+([а-яё]+)\s+(\d{4})',
    ]

    for pattern in contract_patterns:
        contract_match = re.search(pattern, normalized_text, re.IGNORECASE)
        if contract_match:
            day, month, year = contract_match.group(1), contract_match.group(2), contract_match.group(3)
            date_str = normalize_date(day, month, year)
            if date_str:
                result["contract_date"] = date_str
                log.info(f"Найдена дата договора: {result['contract_date']}")
                break

    # 3. Если не нашли — ищем все даты
    if not all([result["contract_date"], result["period_from"], result["period_to"]]):
        all_dates = re.findall(r'(\d{1,2})\s+([а-яё]+)\s+(\d{4})', normalized_text, re.IGNORECASE)
        dates_normalized = [normalize_date(d, m, y) for d, m, y in all_dates]
        dates_normalized = [d for d in dates_normalized if d]

        log.info(f"Все найденные даты: {dates_normalized}")

        if dates_normalized:
            if not result["contract_date"] and dates_normalized:
                result["contract_date"] = dates_normalized[0]

            if not result["period_from"] and len(dates_normalized) >= 2:
                result["period_from"] = dates_normalized[0]
                result["period_to"] = dates_normalized[1] if len(dates_normalized) > 1 else None

    log.info(f"Итоговые даты: договор={result['contract_date']}, период={result['period_from']}-{result['period_to']}")
    return result


def parse_svedeniya(text: str) -> Optional[Dict[str, Any]]:
    """Парсинг раздела СВЕДЕНИЙ для одного ТС."""
    result = {}

    # УБРАТЬ ВСЕ ПРОБЕЛЫ для поиска слипшихся номеров
    text_no_spaces = re.sub(r'\s+', '', text.upper())

    # Паттерны для госномеров БЕЗ ПРОБЕЛОВ
    plate_patterns = [
        r'[АВЕКМНОРСТУХ]\d{3}[АВЕКМНОРСТУХ]{2}\d{2,3}',   # А000АА000
        r'[АВЕКМНОРСТУХ]{2}\d{5}',                        # АА00000
    ]

    plate_match = None
    for pattern in plate_patterns:
        match = re.search(pattern, text_no_spaces)
        if match:
            plate_match = match.group(0)
            log.debug(f"Найден госномер (без пробелов): {plate_match}")
            break

    if not plate_match:
        # Попробуем найти в оригинальном тексте с пробелами
        log.debug(f"Поиск госномера в тексте: {text[:200]}")
        for pattern in [
            r'[АВЕКМНОРСТУХ]\s*\d{3}\s*[АВЕКМНОРСТУХ]{2}\s*\d{2,3}',
            r'[АВЕКМНОРСТУХ]{2}\s*\d{5}'
        ]:
            match = re.search(pattern, text.upper())
            if match:
                plate_match = re.sub(r'\s+', '', match.group(0))
                log.debug(f"Найден госномер (с пробелами): {plate_match}")
                break

    if not plate_match:
        log.warning(f"Госномер не найден в тексте. Первые 300 символов: {text[:300]}")
        return None

    # Нормализация госномера
    try:
        plate_cyr = to_cyr_full(plate_match)
        plate_lat = normalize_plate(plate_cyr)

        if not plate_cyr or not plate_lat:
            log.error(f"Ошибка нормализации номера: {plate_match}")
            return None

        result["plate_cyr"] = plate_cyr
        result["plate_lat"] = plate_lat

        # Извлечение даты из этого сведения
        date_match = re.search(
            r'Дата\s+заключения\s+договора[:\s]*(\d{1,2})\s+([а-яё]+)\s+(\d{4})',
            text, re.IGNORECASE
        )
        if date_match:
            day, month, year = date_match.group(1), date_match.group(2), date_match.group(3)
            result["contract_date"] = normalize_date(day, month, year)

        log.debug(f"Успешно извлечен ТС: {plate_cyr} -> {plate_lat}")
        return result

    except Exception as e:
        log.error(f"Ошибка при обработке госномера {plate_match}: {e}")
        return None


def extract_contract_date_from_svedeniya(text: str) -> Optional[str]:
    """Извлечение даты заключения договора из первого СВЕДЕНИЙ."""
    date_match = re.search(
        r'Дата\s+заключения\s+договора[:\s]*(\d{1,2})\s+([а-яё]+)\s+(\d{4})',
        text, re.IGNORECASE
    )
    if date_match:
        day, month, year = date_match.group(1), date_match.group(2), date_match.group(3)
        return normalize_date(day, month, year)
    return None


def normalize_date(day: str, month: str, year: str) -> Optional[str]:
    """Нормализация даты в формат YYYY-MM-DD."""
    months_full = {
        'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
        'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
        'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12',
    }

    months_short = {
        'янв': '01', 'фев': '02', 'мар': '03', 'апр': '04',
        'май': '05', 'июн': '06', 'июл': '07', 'авг': '08',
        'сен': '09', 'окт': '10', 'ноя': '11', 'дек': '12',
    }

    month_lower = month.lower().strip()

    # Ищем полное название
    month_num = months_full.get(month_lower)

    # Если не нашли, ищем по первым 3 буквам
    if not month_num and len(month_lower) >= 3:
        month_key = month_lower[:3]
        month_num = months_short.get(month_key)

        # Проверяем специальные случаи
        if month_key == 'мая' and month_lower.startswith('мая'):
            month_num = '05'
        elif month_key == 'июн' and month_lower.startswith('июня'):
            month_num = '06'
        elif month_key == 'июл' and month_lower.startswith('июля'):
            month_num = '07'

    if not month_num:
        log.warning(f"Неизвестный месяц: '{month}' (оригинал: '{month}')")
        return None

    try:
        day_int = int(day.strip())
        year_int = int(year.strip())

        # Проверяем корректность
        if day_int < 1 or day_int > 31:
            log.warning(f"Некорректный день: {day}")
            return None

        if year_int < 2000 or year_int > 2100:
            log.warning(f"Некорректный год: {year}")
            return None

        # Проверяем существование даты
        try:
            datetime(year_int, int(month_num), day_int)
        except ValueError as e:
            log.warning(f"Некорректная дата: {day_int}.{month_num}.{year_int}: {e}")
            return None

        return f"{year_int:04d}-{month_num}-{day_int:02d}"

    except ValueError as e:
        log.warning(f"Ошибка преобразования даты: день='{day}', месяц='{month}', год='{year}': {e}")
        return None
