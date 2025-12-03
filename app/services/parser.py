import logging
import re
import asyncio
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple

from app.models.contract import OSGOPContract, VehicleInfo
from app.services.pdf_reader import extract_text_safe
from app.services.plate_normalizer import to_cyr_full, normalize_plate

log = logging.getLogger(__name__)


class OSGOPParser:
    def __init__(self, element_api_client=None):
        """
        Инициализация парсера ОСГОП.
        
        Args:
            element_api_client: Асинхронный клиент для Element API (опционально)
        """
        self.element_api_client = element_api_client
        self.contract_date_from_svedeniya = None

    # ====================== ПУБЛИЧНЫЕ МЕТОДЫ ============================

    async def parse_with_segments(self, pdf_bytes: bytes) -> Tuple[List[OSGOPContract], List[Tuple[int, int]]]:
        """Асинхронный парсинг PDF и разделение на сегменты по алгоритму"""
        try:
            # Асинхронное извлечение текста со страниц
            pages = await self._extract_text_async(pdf_bytes)
            log.info(f"Загружено {len(pages)} страниц")
            
            # ДЕБАГ: выводим первые 2 страницы
            for i, page in enumerate(pages[:2]):
                clean_page = page.replace('\n', ' ').replace('\r', ' ').replace('\xa0', ' ')
                clean_page = re.sub(r'\s+', ' ', clean_page)
                log.info(f"=== СТРАНИЦА {i} (первые 1000 символов) ===")
                log.info(clean_page[:1000])
            
            # Нормализуем текст каждой страницы
            normalized_pages = [self._normalize_page_text(page) for page in pages]
            
            # 1. Находим все сегменты документа
            segments = self._detect_segments(normalized_pages)
            
            if not segments:
                log.error("Не найдены сегменты в документе")
                return [], []
            
            # 2. Определяем, где находится полис и сведения
            polis_segment = None
            svedeniya_segments = []
            
            for start, end, segment_type in segments:
                if segment_type == "POLIS":
                    polis_segment = (start, end)
                elif segment_type == "SVEDENIYA":
                    svedeniya_segments.append((start, end))
            
            if not polis_segment:
                log.error("Не найден полис в документе")
                return [], []
            
            # 3. Парсим полис
            polis_text = "\n".join(normalized_pages[polis_segment[0]:polis_segment[1]])
            header_data = self._parse_polis_header(polis_text)
            
            # 4. Парсим сведения и извлекаем госномера
            vehicles_data = []
            
            # Для первого сведения извлекаем дату заключения договора
            if svedeniya_segments:
                first_sved_text = "\n".join(normalized_pages[svedeniya_segments[0][0]:svedeniya_segments[0][1]])
                sved_date = self._extract_contract_date_from_svedeniya(first_sved_text)
                if sved_date:
                    self.contract_date_from_svedeniya = sved_date
                    log.info(f"Дата из сведений: {self.contract_date_from_svedeniya}")
            
            # Парсим все сведения
            for i, (start, end) in enumerate(svedeniya_segments):
                sved_text = "\n".join(normalized_pages[start:end])
                vehicle_data = self._parse_svedeniya(sved_text)
                if vehicle_data:
                    vehicles_data.append(vehicle_data)
            
            if not vehicles_data:
                log.warning("Не найдены данные о транспортных средствах в сведениях")
            
            # 5. Получаем информацию о ТС из Element API асинхронно
            vehicles = await self._get_vehicles_info_from_element(vehicles_data)
            
            # 6. Используем дату из сведений, если в полисе нет
            if not header_data.get("contract_date") and self.contract_date_from_svedeniya:
                header_data["contract_date"] = self.contract_date_from_svedeniya
                log.info(f"Использована дата договора из сведений: {self.contract_date_from_svedeniya}")
            
            # 7. Создаем контракт
            contract = OSGOPContract(
                contract_number=header_data.get("contract_number"),
                contract_date=header_data.get("contract_date"),
                period_from=header_data.get("period_from"),
                period_to=header_data.get("period_to"),
                insurer=header_data.get("insurer"),
                insurer_inn=header_data.get("insurer_inn"),
                insured=header_data.get("insured"),
                insured_inn=header_data.get("insured_inn"),
                bonus=header_data.get("bonus"),
                vehicles=vehicles
            )
            
            # 8. Формируем сегменты для сохранения (start, end)
            all_segments = []
            all_segments.append((polis_segment[0], polis_segment[1]))
            for start, end in svedeniya_segments:
                all_segments.append((start, end))
            
            log.info(f"Успешно распарсен договор {contract.contract_number} с {len(vehicles)} ТС")
            return [contract], all_segments
            
        except Exception as e:
            log.error(f"Ошибка парсинга: {str(e)}", exc_info=True)
            return [], []

    async def _extract_text_async(self, pdf_bytes: bytes) -> List[str]:
        """Асинхронное извлечение текста из PDF"""
        # Выполняем CPU-bound операцию в отдельном потоке
        return await asyncio.to_thread(extract_text_safe, pdf_bytes)

    # ====================== ОБНАРУЖЕНИЕ СЕГМЕНТОВ =======================

    def _detect_segments(self, pages: List[str]) -> List[Tuple[int, int, str]]:
        """Обнаружение сегментов документа: полис и сведения"""
        segments = []
        i = 0
        total_pages = len(pages)
        
        while i < total_pages:
            page_text = pages[i].upper()
            
            # Поиск начала ПОЛИСА
            if re.search(r'ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ\s+ГРАЖДАНСКОЙ\s+ОТВЕТСТВЕННОСТИ\s+ПЕРЕВОЗЧИКА', page_text):
                start = i
                # Ищем конец полиса (начало следующего сегмента или конец документа)
                i += 1
                while i < total_pages:
                    next_page = pages[i].upper()
                    # Конец полиса - когда находим "СВЕДЕНИЯ" или другой полис
                    if re.search(r'СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ', next_page) or \
                       re.search(r'ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ', next_page):
                        break
                    i += 1
                segments.append((start, i, "POLIS"))
                continue
            
            # Поиск "СВЕДЕНИЯ О ДОГОВОРЕ"
            if re.search(r'СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ\s+ГРАЖДАНСКОЙ\s+ОТВЕТСТВЕННОСТИ\s+ПЕРЕВОЗЧИКА', page_text):
                start = i
                # Ищем конец сведений (начало следующего сегмента или конец документа)
                i += 1
                while i < total_pages:
                    next_page = pages[i].upper()
                    # Конец сведений - когда находим "СВЕДЕНИЯ" или "ПОЛИС"
                    if re.search(r'СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ', next_page) or \
                       re.search(r'ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ', next_page):
                        break
                    i += 1
                segments.append((start, i, "SVEDENIYA"))
                continue
            
            i += 1
        
        return segments

    # ====================== НОРМАЛИЗАЦИЯ ТЕКСТА =========================

    def _normalize_page_text(self, text: str) -> str:
        """Нормализация текста страницы"""
        if not text:
            return ""
        
        # Восстанавливаем пробелы в слипшемся тексте
        text = self._restore_spaces(text)
        
        # Заменяем специальные символы
        text = text.replace("\xa0", " ").replace("\t", " ")
        
        # Убираем лишние пробелы
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()

    def _restore_spaces(self, text: str) -> str:
        """Восстановление пробелов в слипшемся русском тексте"""
        if not text:
            return text
        
        # Восстанавливаем пробелы в ключевых фразах
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

    # ====================== ПАРСИНГ ПОЛИСА ==============================

    def _parse_polis_header(self, text: str) -> Dict[str, Any]:
        """Парсинг заголовка полиса"""
        result = {}
        
        log.info("=== НАЧАЛО ПАРСИНГА ПОЛИСА ===")
        
        # 1. НОМЕР ПОЛИСА
        rosx_match = re.search(r'ROSX\d{8,20}', text, re.IGNORECASE)
        result["contract_number"] = rosx_match.group(0).upper() if rosx_match else None
        log.info(f"Номер полиса: {result['contract_number']}")
        
        # 2. ДАТЫ
        result.update(self._extract_dates_from_polis(text))
        
        # 3. СТРАХОВЩИК - более гибкий поиск
        insurer_section_search = re.search(r'Страховщик[:\s]*(.*?)(?=\s*(?:Страхователь|Итого|Премия|Срок|ИНН|$))', 
                                          text, re.IGNORECASE | re.DOTALL)
        
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
        
        # ИНН страховщика
        insurer_inn_match = re.search(r'Страховщик[^ИНН]*ИНН[:\s/]*(\d{10,12})', text, re.IGNORECASE)
        if insurer_inn_match:
            result["insurer_inn"] = insurer_inn_match.group(1)
            log.info(f"ИНН страховщика: {result['insurer_inn']}")
        else:
            if insurer_section_search:
                inn_in_text = re.search(r'ИНН[:\s/]*(\d{10,12})', insurer_section_search.group(0), re.IGNORECASE)
                if inn_in_text:
                    result["insurer_inn"] = inn_in_text.group(1)
        
        # 4. СТРАХОВАТЕЛЬ
        insured_section_search = re.search(r'Страхователь[:\s]*(.*?)(?=\s*(?:Итого|Премия|Срок|Страховая|ИНН|$))', 
                                          text, re.IGNORECASE | re.DOTALL)
        
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
        insured_inn_match = re.search(r'Страхователь[^ИНН]*ИНН[:\s/]*(\d{10,12})', text, re.IGNORECASE)
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
    
    def _extract_dates_from_polis(self, text: str) -> Dict[str, Optional[str]]:
        """Извлечение всех дат из текста полиса"""
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
                
                result["period_from"] = self._normalize_date(from_day, from_month, from_year)
                result["period_to"] = self._normalize_date(to_day, to_month, to_year)
                
                if result["period_from"] and result["period_to"]:
                    log.info(f"Найден период по паттерну '{pattern[:50]}...': {result['period_from']} - {result['period_to']}")
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
                date_str = self._normalize_date(day, month, year)
                if date_str:
                    result["contract_date"] = date_str
                    log.info(f"Найдена дата договора по паттерну: {result['contract_date']}")
                    break
        
        # 3. Если не нашли - ищем все даты
        if not all([result["contract_date"], result["period_from"], result["period_to"]]):
            all_dates = re.findall(r'(\d{1,2})\s+([а-яё]+)\s+(\d{4})', normalized_text, re.IGNORECASE)
            dates_normalized = [self._normalize_date(d, m, y) for d, m, y in all_dates]
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

    # ====================== ПАРСИНГ СВЕДЕНИЙ ===========================

    def _extract_contract_date_from_svedeniya(self, text: str) -> Optional[str]:
        """Извлечение даты заключения договора из первого СВЕДЕНИЙ"""
        date_match = re.search(r'Дата\s+заключения\s+договора[:\s]*(\d{1,2})\s+([а-яё]+)\s+(\d{4})', text, re.IGNORECASE)
        if date_match:
            day, month, year = date_match.group(1), date_match.group(2), date_match.group(3)
            return self._normalize_date(day, month, year)
        return None

    def _parse_svedeniya(self, text: str) -> Optional[Dict[str, Any]]:
        """Парсинг раздела СВЕДЕНИЙ для одного ТС"""
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
            date_match = re.search(r'Дата\s+заключения\s+договора[:\s]*(\d{1,2})\s+([а-яё]+)\s+(\d{4})', text, re.IGNORECASE)
            if date_match:
                day, month, year = date_match.group(1), date_match.group(2), date_match.group(3)
                result["contract_date"] = self._normalize_date(day, month, year)
            
            log.debug(f"Успешно извлечен ТС: {plate_cyr} -> {plate_lat}")
            return result
            
        except Exception as e:
            log.error(f"Ошибка при обработке госномера {plate_match}: {e}")
            return None

    # ====================== РАБОТА С ELEMENT API =========================

    async def _get_vehicles_info_from_element(self, vehicles_data: List[Dict]) -> List[VehicleInfo]:
        """
        Асинхронное получение информации о ТС из Element API.
        
        Использует ElementApiClientAsync для поиска машин по госномерам.
        """
        vehicles = []
        
        if not vehicles_data:
            log.warning("Нет данных о ТС для запроса к Element API")
            return vehicles
        
        if not self.element_api_client:
            log.warning("Element API клиент не инициализирован, создаем VehicleInfo без данных из API")
            for data in vehicles_data:
                vehicle = VehicleInfo(
                    vehicle_plate_cyr=data.get("plate_cyr"),
                    vehicle_plate_lat=data.get("plate_lat"),
                    vin=None,
                    car_info=None
                )
                vehicles.append(vehicle)
                log.info(f"Добавлен ТС (без данных из Element): {data.get('plate_cyr')}")
            return vehicles
        
        async def process_vehicle(data: Dict) -> Optional[VehicleInfo]:
            plate_lat = data.get("plate_lat")
            plate_cyr = data.get("plate_cyr")
            
            if not plate_lat or not plate_cyr:
                log.warning(f"Пропуск ТС: нет plate_lat или plate_cyr в данных {data}")
                return None
            
            try:
                # Ищем ТС в Element по русскому номеру
                car_data = await self.element_api_client.get_car_by_plate(plate_cyr)
                
                # Извлекаем информацию из Element
                vin = None
                car_info = None
                
                if car_data:
                    # Извлекаем VIN (может называться по-разному в Element API)
                    vin = car_data.get("VIN") or car_data.get("vin")
                    if vin and isinstance(vin, str):
                        vin = vin.strip()
                        if vin in ("", "0", "Нет данных"):
                            vin = None
                    
                    # Собираем информацию об автомобиле
                    car_info = {
                        "model": car_data.get("Model") or car_data.get("model") or "",
                        "year": car_data.get("YearCar") or car_data.get("year") or "",
                        "code": car_data.get("Code") or car_data.get("code") or "",
                    }
                
                # Создаем объект VehicleInfo
                vehicle = VehicleInfo(
                    vehicle_plate_cyr=plate_cyr,
                    vehicle_plate_lat=plate_lat,
                    vin=vin,
                    car_info=car_info
                )
                
                log.info(f"Добавлен ТС: {plate_cyr} -> {plate_lat}, VIN: {vin or 'не найден'}")
                return vehicle
                
            except Exception as e:
                log.error(f"Ошибка при обработке ТС {plate_lat} в Element: {e}")
                # Создаем VehicleInfo без данных из API
                vehicle = VehicleInfo(
                    vehicle_plate_cyr=plate_cyr,
                    vehicle_plate_lat=plate_lat,
                    vin=None,
                    car_info=None
                )
                log.info(f"Добавлен ТС (без данных из Element): {plate_cyr}")
                return vehicle
        
        # Создаем задачи для всех ТС
        tasks = [process_vehicle(data) for data in vehicles_data]
        
        # Запускаем параллельно
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        for result in results:
            if isinstance(result, Exception):
                log.error(f"Исключение при обработке ТС в Element: {result}")
                continue
            if result:
                vehicles.append(result)
        
        log.info(f"Всего обработано ТС через Element API: {len(vehicles)}")
        return vehicles

    # ====================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ =====================

    def _normalize_date(self, day: str, month: str, year: str) -> Optional[str]:
        """Нормализация даты в формат YYYY-MM-DD"""
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
