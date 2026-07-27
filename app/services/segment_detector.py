"""Детекция сегментов документа (POLIS / SVEDENIYA) и нормализация текста."""

import re
import logging
from typing import List, Tuple

log = logging.getLogger(__name__)

_POLIS_START = r'ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ\s+ГРАЖДАНСКОЙ\s+ОТВЕТСТВЕННОСТИ\s+ПЕРЕВОЗЧИКА'
_SVED_START = r'СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ\s+ГРАЖДАНСКОЙ\s+ОТВЕТСТВЕННОСТИ\s+ПЕРЕВОЗЧИКА'
_END_PATTERNS = [r'СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ', r'ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ']


def _find_segment_end(pages: List[str], start: int, total: int) -> int:
    """Находит конец сегмента: первая страница с началом другого сегмента или конец документа."""
    i = start + 1
    while i < total:
        page = pages[i].upper()
        if any(re.search(p, page) for p in _END_PATTERNS):
            break
        i += 1
    return i


def detect_segments(pages: List[str]) -> List[Tuple[int, int, str]]:
    """Обнаружение сегментов документа: полис и сведения."""
    segments = []
    i = 0
    total_pages = len(pages)

    while i < total_pages:
        page_text = pages[i].upper()

        if re.search(_POLIS_START, page_text):
            end = _find_segment_end(pages, i, total_pages)
            segments.append((i, end, "POLIS"))
            i = end
            continue

        if re.search(_SVED_START, page_text):
            end = _find_segment_end(pages, i, total_pages)
            segments.append((i, end, "SVEDENIYA"))
            i = end
            continue

        i += 1

    return segments


def normalize_page_text(text: str) -> str:
    """Нормализация текста страницы."""
    if not text:
        return ""

    text = restore_spaces(text)
    text = text.replace("\xa0", " ").replace("\t", " ")
    text = re.sub(r'\s+', ' ', text)

    return text.strip()


def restore_spaces(text: str) -> str:
    """Восстановление пробелов в слипшемся русском тексте."""
    if not text:
        return text

    patterns = [
        (r'ПОЛИСОБЯЗАТЕЛЬНОГО', 'ПОЛИС ОБЯЗАТЕЛЬНОГО'),
        (r'ОБЯЗАТЕЛЬНОГОСТРАХОВАНИЯ', 'ОБЯЗАТЕЛЬНОГО СТРАХОВАНИЯ'),
        (r'СТРАХОВАНИЯГРАЖДАНСКОЙ', 'СТРАХОВАНИЯ ГРАЖДАНСКОЙ'),
        (r'ГРАЖДАНСКОЙОТВЕТСТВЕННОСТИ', 'ГРАЖДАНСКОЙ ОТВЕТСТВЕННОСТИ'),
        (r'ОТВЕТСТВЕННОСТИПЕРЕВОЗЧИКА', 'ОТВЕТСТВЕННОСТИ ПЕРЕВОЗЧИКА'),
        (r'СВЕДЕНИЯОДОГОВОРЕ', 'СВЕДЕНИЯ О ДОГОВОРЕ'),
        (r'ДОГОВОРЕОБЯЗАТЕЛЬНОГО', 'ДОГОВОРЕ ОБЯЗАТЕЛЬНОГО'),
        (r'Срокстрахования', 'Срок страхования'),
        (r'Датазаключения', 'Дата заключения'),
        (r'Страховщик:', 'Страховщик: '),
        (r'Страхователь:', 'Страхователь: '),
        (r'ИНН/КПП:', 'ИНН/КПП: '),
    ]

    for pattern, replacement in patterns:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    # Разделяем слова: строчная + заглавная
    text = re.sub(r'([а-яё])([А-ЯЁ])', r'\1 \2', text)

    # Разделяем цифра + буква или буква + цифра (но не в госномерах)
    text = re.sub(r'(?<!\d)(\d{3,})([А-ЯЁ])', r'\1 \2', text)
    text = re.sub(r'([А-ЯЁ])(\d{3,})(?!\d)', r'\1 \2', text)

    return text
