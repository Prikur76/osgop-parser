"""Детекция сегментов документа (POLIS / SVEDENIYA) и нормализация текста."""

import re
import logging
from typing import List, Tuple

log = logging.getLogger(__name__)


def detect_segments(pages: List[str]) -> List[Tuple[int, int, str]]:
    """Обнаружение сегментов документа: полис и сведения."""
    segments = []
    i = 0
    total_pages = len(pages)

    while i < total_pages:
        page_text = pages[i].upper()

        # Поиск начала ПОЛИСА
        if re.search(r'ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ\s+ГРАЖДАНСКОЙ\s+ОТВЕТСТВЕННОСТИ\s+ПЕРЕВОЗЧИКА', page_text):
            start = i
            i += 1
            while i < total_pages:
                next_page = pages[i].upper()
                if re.search(r'СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ', next_page) or \
                   re.search(r'ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ', next_page):
                    break
                i += 1
            segments.append((start, i, "POLIS"))
            continue

        # Поиск "СВЕДЕНИЯ О ДОГОВОРЕ"
        if re.search(r'СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ\s+ГРАЖДАНСКОЙ\s+ОТВЕТСТВЕННОСТИ\s+ПЕРЕВОЗЧИКА', page_text):
            start = i
            i += 1
            while i < total_pages:
                next_page = pages[i].upper()
                if re.search(r'СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ', next_page) or \
                   re.search(r'ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ', next_page):
                    break
                i += 1
            segments.append((start, i, "SVEDENIYA"))
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
