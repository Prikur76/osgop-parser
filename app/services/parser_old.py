import logging
import re
from typing import List, Tuple

from app.services.pdf_reader import extract_text_safe
from app.services.plate_normalizer import to_cyr, to_lat
from app.models.contract import Contract
from app.utils.regex import (
    RE_INSURER,
    RE_ROSX,
    RE_PLATE,
    RE_PERIOD_EXPLICIT,
    RE_DATE_ANY,
    RE_INN,
    RE_PREMIUM,
)

log = logging.getLogger(__name__)


class OSGOPParser:

    # ======================================================================
    # ПУБЛИЧНЫЕ МЕТОДЫ
    # ======================================================================

    def parse_with_segments(self, pdf_bytes: bytes) -> Tuple[List[Contract], List[Tuple[int, int]]]:
        """
        Возвращает:
        - список Contract
        - список сегментов [(start_page, end_page), ...]
        """
        pages = extract_text_safe(pdf_bytes)
        segments = self._detect_contract_segments(pages)

        log.info(f"Обнаружено договоров: {len(segments)}")

        contracts: List[Contract] = []

        for idx, (start, end) in enumerate(segments):
            block_text = "\n".join(pages[start:end])
            contract = self._parse_block(block_text)

            log.info(
                f"[Договор {idx+1}] "
                f"Страницы {start}-{end-1}, "
                f"Номер: {contract.contract_number}, "
                f"Госномер: {contract.vehicle_plate}"
            )

            contracts.append(contract)

        return contracts, segments

    def parse(self, pdf_bytes: bytes) -> List[Contract]:
        """
        Возвращает список договоров.
        """
        contracts, _ = self.parse_with_segments(pdf_bytes)
        return contracts

    # ======================================================================
    # ПОИСК ГРАНИЦ ДОГОВОРОВ
    # ======================================================================

    def _detect_contract_segments(self, pages: List[str]) -> List[Tuple[int, int]]:
        """
        Лучшая возможная версия поиска начала договора:
        1) Точное совпадение: "СВЕДЕНИЯ О ДОГОВОРЕ"
        2) Разорванные строки: "СВЕДЕНИ*" + "ДОГОВОР"
        """

        starts = []

        for i, t in enumerate(pages):
            page = t.upper().replace("\n", " ")

            # 1) Точное совпадение (стабильная логика)
            if re.search(r"СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ", page, re.I):
                starts.append(i)
                continue

            # 2) Нахождение ключевых слов на странице (переносы, OCR)
            if "СВЕДЕНИ" in page and "ДОГОВОР" in page:
                starts.append(i)
                continue
           
        starts = sorted(set(starts))

        if not starts:
            log.warning("‼ Не найдено начало договора — весь PDF считается одним блоком.")
            return [(0, len(pages))]

        # Формирование сегментов
        segments = []
        
        # Если первое начало не на странице 0, добавляем преамбулу как отдельный сегмент
        if starts[0] > 0:
            # Преамбула (листы "Полис ..." до первого "Сведения о договоре")
            segments.append((0, starts[0]))
            
        for idx in range(len(starts)):
            start_page = starts[idx]
            end_page = starts[idx + 1] if idx < len(starts) - 1 else len(pages)
            segments.append((start_page, end_page))

        return segments

    # ======================================================================
    # ПАРСИНГ ОДНОГО БЛОКА
    # ======================================================================

    def _parse_block(self, block: str) -> Contract:
        """
        Разбор текста одного договора.
        """

        # -----------------------------
        # Номер договора ROSX*******
        # -----------------------------
        m = RE_ROSX.search(block)
        contract_number = m.group(1) if m else None

        # -----------------------------
        # Даты
        # -----------------------------
        contract_date, period_from, period_to = self._extract_dates(block)

        # -----------------------------
        # Госномер
        # -----------------------------
        plate = None
        mp = RE_PLATE.findall(block)

        if mp:
            raw_plate = mp[0]

            try:
                plate_cyr = to_cyr(raw_plate)
                plate = to_lat(plate_cyr)
            except Exception:
                plate = raw_plate

        # -----------------------------
        # Страховщик
        # -----------------------------
        insurer = self._extract_insurer(block)
        # insurer = self._extract_label(block, ["Страховщик"])

        # -----------------------------
        # ИНН (страховщик / страхователь)
        # -----------------------------
        inns = RE_INN.findall(block)
        
        # Определяем ИНН страховщика - берем первый ИНН после слова "Страховщик"
        insurer_inn = inns[0] if len(inns) > 0 else None
        insured_inn = inns[1] if len(inns) > 1 else None
        
        if inns:
            # Находим позицию слова "Страховщик"
            insurer_pos = block.lower().find("страховщик")
            
            if insurer_pos != -1:
                # Ищем ИНН после "Страховщик" до "Страхователь"
                insurer_section = block[insurer_pos:]
                # Берем первый ИНН в этом разделе
                insurer_inn_match = RE_INN.search(insurer_section)
                if insurer_inn_match:
                    insurer_inn = insurer_inn_match.group(1)
            
            # ИНН страхователя - последний ИНН в документе или второй найденный
            if len(inns) > 0:
                # Если нашли только один ИНН и это не ИНН страховщика
                if insurer_inn is None:
                    insurer_inn = inns[0]
                elif len(inns) > 1:
                    # Берем второй ИНН как ИНН страхователя
                    insured_inn = inns[1] if insurer_inn == inns[0] else inns[0]

        # -----------------------------
        # Страхователь / Перевозчик
        # -----------------------------
        insured = self._extract_label(block, ["Страхователь", "Перевозчик"])

        # Если не нашли через стандартный метод, используем улучшенный
        if not insured:
            insured = self._extract_insured(block)
        
        # -----------------------------
        # Страховая премия
        # -----------------------------
        premium = self._extract_premium(block)
        # pm = RE_PREMIUM.search(block)
        # premium = None

        # if pm:
        #     try:
        #         premium = float(pm.group(2).replace(" ", "").replace(",", "."))
        #     except Exception:
        #         premium = None

        # -----------------------------
        # Формируем модель
        # -----------------------------
        return Contract(
            contract_number=contract_number,
            contract_date=contract_date,
            period_from=period_from,
            period_to=period_to,
            vehicle_plate=plate,
            insurer=insurer,
            insurer_inn=insurer_inn,
            insured=insured,
            insured_inn=insured_inn,
            premium=premium,
        )

    # ======================================================================
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # ======================================================================

    def _extract_dates(self, block: str):

        def norm_date(d, m, y):
            months = {
                'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
                'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
                'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
            }
            mm = months.get(m.lower())
            if not mm:
                return None
            return f"{y}-{mm}-{int(d):02d}"

        # 1) Явный период "с ... по ..."
        explicit = RE_PERIOD_EXPLICIT.search(block)
        period_from = None
        period_to = None

        if explicit:
            period_from = norm_date(explicit.group(1), explicit.group(2), explicit.group(3))
            period_to = norm_date(explicit.group(4), explicit.group(5), explicit.group(6))

        # 2) Все даты подряд
        raw_dates = RE_DATE_ANY.findall(block)
        all_dates = [norm_date(d, m, y) for (d, m, y) in raw_dates if norm_date(d, m, y)]

        # 3) Дата заключения — первая дата
        contract_date = all_dates[0] if len(all_dates) > 0 else None

        # Мягкий fallback
        if not period_from and len(all_dates) > 1:
            period_from = all_dates[1]
        if not period_to and len(all_dates) > 2:
            period_to = all_dates[2]

        return contract_date, period_from, period_to

    def _extract_label(self, block: str, keys: list[str]):
        """
        Ищет строку после ключа ("Страховщик", "Страхователь", "Перевозчик")
        """
        for key in keys:
            idx = block.find(key)
            if idx != -1:
                tail = block[idx + len(key):]
                line = tail.split("\n")[0].strip().strip(":").strip()
                if line:
                    return line
        return None

    def _extract_insurer(self, block: str) -> str | None:
        """
        Извлечение информации о страховщике с учетом сложного форматирования.
        """
        # Сначала пытаемся найти по улучшенному шаблону
        insurer_match = RE_INSURER.search(block)
        
        if insurer_match:
            insurer_text = insurer_match.group(1).strip()
            # Очищаем текст от лишних символов
            insurer_text = re.sub(r'Лицензия.*$', '', insurer_text, flags=re.I)
            insurer_text = re.sub(r'\([^)]*\)', '', insurer_text)  # Удаляем скобки с содержимым
            insurer_text = insurer_text.replace('"', '').replace('«', '').replace('»', '')
            insurer_text = insurer_text.strip(' ,.;:')
            
            if insurer_text:
                return insurer_text
        
        # Fallback: используем старый метод
        return self._extract_label(block, ["Страховщик"])

    def _extract_insured(self, block: str) -> str | None:
        """
        Извлечение информации о страхователе.
        """
        # Ищем "Страхователь:" или "Перевозчик:"
        for key in ["Страхователь", "Перевозчик"]:
            idx = block.find(key)
            if idx != -1:
                tail = block[idx + len(key):]
                # Берем первую строку, очищаем
                line = tail.split("\n")[0].strip().strip(":").strip()
                # Удаляем ИНН/КПП если есть
                line = re.sub(r'ИНН\s*/?\s*КПП.*$', '', line, flags=re.I)
                if line:
                    return line
        return None

    def _extract_premium(self, block: str) -> float | None:
        """
        Извлечение страховой премии с учетом разных форматов.
        """
        pm = RE_PREMIUM.search(block)
        
        if pm:
            try:
                # Извлекаем числовую часть
                amount_str = pm.group(2) if pm.lastindex == 2 else pm.group(1)
                # Очищаем от пробелов и заменяем запятую на точку
                amount_str = amount_str.replace(" ", "").replace(",", ".")
                # Удаляем все нецифровые символы кроме точки
                amount_str = re.sub(r'[^\d.]', '', amount_str)
                premium = float(amount_str)
                return premium
            except Exception:
                return None
        
        return None

    def _get_pages_slice(self, pdf_bytes: bytes, start: int, end: int) -> List[str]:
        """
        Извлекает текст срез страниц для анализа.
        """
        from app.services.pdf_reader import extract_text_safe
        pages = extract_text_safe(pdf_bytes)
        return pages[start:end]
