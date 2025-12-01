import logging
import re
from typing import List, Optional, Tuple, Dict, Any

from app.services.pdf_reader import extract_text_safe
from app.services.plate_normalizer import to_cyr, to_lat
from app.services.car_api_client import get_car_api_client
from app.models.contract import OSGOPContract, VehicleInfo
from app.utils.regex import (
    RE_ROSX,
    RE_PLATE,
    RE_POLIS_START,
    RE_SVEDENIYA_START,
    RE_PERIOD_EXPLICIT,
    RE_DATE_ANY,
    RE_INN,
    RE_INN_KPP,
    RE_PREMIUM,
    RE_INSURER,
    RE_INSURED,
)

log = logging.getLogger(__name__)


class OSGOPParser:
    
    def __init__(self):
        """Инициализация парсера с API клиентом"""
        self.car_api_client = get_car_api_client()
    
    def parse_with_segments(self, pdf_bytes: bytes) -> Tuple[List[OSGOPContract], List[Tuple[int, int]]]:
        """
        Возвращает список полисов ОСГОП и сегменты страниц.
        """
        pages = extract_text_safe(pdf_bytes)
        
        # Находим начало основного полиса
        polis_start = self._find_polis_start(pages)
        
        if polis_start is None:
            log.error("Не найден основной полис ОСГОП")
            return [], []
        
        # Находим конец полиса и начало приложений
        svedeniya_starts = self._find_svedeniya_starts(pages, polis_start)
        
        if not svedeniya_starts:
            # Если нет приложений, обрабатываем как единый документ
            segments = [(polis_start, len(pages))]
            contracts = [self._parse_complete_polis("\n".join(pages[polis_start:]))]
        else:
            # Основной полис (от начала до первого приложения)
            polis_end = svedeniya_starts[0]
            
            # Сегменты для каждого приложения
            segments = []
            
            for i, start in enumerate(svedeniya_starts):
                end = svedeniya_starts[i + 1] if i < len(svedeniya_starts) - 1 else len(pages)
                segments.append((start, end))
            
            # Парсим основной полис
            polis_text = "\n".join(pages[polis_start:polis_end])
            polis_data = self._parse_polis_header(polis_text)
            
            # Получаем ИНН страхователя для фильтрации в API
            insured_inn = polis_data.get('insured_inn')
            
            # Собираем все госномера из приложений
            all_plates = []
            for start, end in segments:
                appendix_text = "\n".join(pages[start:end])
                plate = self._extract_plate_from_appendix(appendix_text)
                if plate:
                    all_plates.append(plate)
            
            # Обогащаем данные из API 1С с использованием ИНН для фильтрации
            vehicles_info = self._get_vehicles_info(all_plates)
            
            # Создаем полный полис
            contract = OSGOPContract(
                contract_number=polis_data.get('contract_number'),
                contract_date=polis_data.get('contract_date'),
                period_from=polis_data.get('period_from'),
                period_to=polis_data.get('period_to'),
                insurer=polis_data.get('insurer'),
                insurer_inn=polis_data.get('insurer_inn'),
                insured=polis_data.get('insured'),
                insured_inn=insured_inn,
                premium=polis_data.get('premium'),
                vehicles=vehicles_info
            )
            
            contracts = [contract]
            # Добавляем сегмент полиса в начало
            segments = [(polis_start, polis_end)] + segments
        
        return contracts, segments
    
    def _extract_plate_from_appendix(self, text: str) -> Optional[str]:
        """
        Извлекает госномер из приложения.
        """
        plates = RE_PLATE.findall(text)
        if not plates:
            return None
        
        plate = plates[0]
        try:
            plate_cyr = to_cyr(plate)
            return to_lat(plate_cyr)
        except Exception:
            return plate
    
    def _get_vehicles_info(self, plates: List[str]) -> List[VehicleInfo]:
        """
        Получает информацию о транспортных средствах из API 1С.
        """
        if not plates:
            return []
        
        log.info(f"Ищем информацию для {len(plates)} ТС")
        
        vehicles = []
        
        for plate in plates:
            try:
                # Логируем исходный номер
                log.debug(f"Обрабатываем номер: {plate}")
                
                # Получаем расширенную информацию из API
                car_info = self.car_api_client.get_car_extended_info(plate=plate)
                
                # Получаем VIN
                vin = car_info.get('vin')
                
                # Создаем информацию о ТС
                vehicle = VehicleInfo(
                    vehicle_plate=plate,
                    vin=vin,
                    car_info=car_info if car_info else None
                )
                
                if vin:
                    log.info(f"✓ Найден VIN {vin} для номера {plate}")
                else:
                    # Проверяем, есть ли вообще информация о машине
                    if car_info:
                        log.warning(f"✗ Машина найдена для {plate}, но VIN отсутствует")
                    else:
                        log.warning(f"✗ Машина не найдена в API для номера {plate}")
                        
            except Exception as e:
                log.error(f"⚠ Ошибка при получении данных для {plate}: {str(e)}")
                # Создаем базовую запись без данных из API
                vehicle = VehicleInfo(vehicle_plate=plate)
            
            vehicles.append(vehicle)
        
        # Детальная статистика
        with_vin_count = sum(1 for v in vehicles if v.vin)
        with_car_info = sum(1 for v in vehicles if v.car_info)
        
        log.info(f"Статистика по ТС:")
        log.info(f"  Всего ТС: {len(vehicles)}")
        log.info(f"  С VIN: {with_vin_count}")
        log.info(f"  С информацией из API: {with_car_info}")
        log.info(f"  Без данных из API: {len(vehicles) - with_car_info}")
        
        return vehicles
    
    def _parse_complete_polis(self, text: str) -> OSGOPContract:
        """
        Парсит полный полис (если нет отдельных приложений).
        """
        # Извлекаем все данные
        data = self._parse_polis_header(text)
        
        # Извлекаем госномера из текста полиса
        plates = RE_PLATE.findall(text)
        unique_plates = list(set(plates))
        
        # Нормализуем номера
        normalized_plates = []
        for plate in unique_plates:
            try:
                plate_cyr = to_cyr(plate)
                normalized_plate = to_lat(plate_cyr)
                normalized_plates.append(normalized_plate)
            except Exception:
                normalized_plates.append(plate)
        
        # Получаем информацию о ТС из API
        insured_inn = data.get("insured_inn")
        vehicles_info = self._get_vehicles_info(normalized_plates)
        
        return OSGOPContract(
            contract_number=data.get("contract_number"),
            contract_date=data.get("contract_date"),
            period_from=data.get("period_from"),
            period_to=data.get("period_to"),
            insurer=data.get("insurer"),
            insurer_inn=data.get("insurer_inn"),
            insured=data.get("insured"),
            insured_inn=insured_inn,
            premium=data.get("premium"),
            vehicles=vehicles_info
        )
    
    def _find_polis_start(self, pages: List[str]) -> int | None:
        """Находит начало полиса ОСГОП"""
        for i, page in enumerate(pages):
            if RE_POLIS_START.search(page.upper()):
                return i
        return None
    
    def _find_svedeniya_starts(self, pages: List[str], start_from: int = 0) -> List[int]:
        """Находит начала приложений 'Сведения о договоре'"""
        starts = []
        for i in range(start_from, len(pages)):
            if RE_SVEDENIYA_START.search(pages[i].upper()):
                starts.append(i)
        return starts
    
    def _parse_polis_header(self, text: str) -> Dict[str, Any]:
        """Парсит заголовок полиса (общую информацию)"""
        result = {}
        
        # Номер договора
        m = RE_ROSX.search(text)
        if m:
            result["contract_number"] = m.group(1)
        
        # Даты периода действия
        dates = self._extract_dates(text)
        if dates:
            result.update(dates)
        
        # Страховщик
        insurer_match = RE_INSURER.search(text)
        if insurer_match:
            insurer = insurer_match.group(1).strip()
            # Очистка текста
            insurer = re.sub(r"Лицензия.*$", "", insurer, flags=re.I)
            insurer = re.sub(r"\([^)]*\)", "", insurer)
            insurer = insurer.replace('"', "").replace('«', '').replace('»', '')
            insurer = " ".join(insurer.split())  # Удаляем лишние пробелы
            result["insurer"] = insurer
        
        # ИНН страховщика
        inns = self._extract_inns(text)
        if inns:
            result["insurer_inn"] = inns[0] if len(inns) > 0 else None
        
        # Страхователь
        insured_match = RE_INSURED.search(text)
        if insured_match:
            insured = insured_match.group(1).strip()
            insured = re.sub(r'ИНН\s*/?\s*КПП.*$', '', insured, flags=re.I)
            insured = ' '.join(insured.split())
            result['insured'] = insured
        
        # ИНН страхователя
        if inns and len(inns) > 1:
            result['insured_inn'] = inns[1]
        
        # Страховая премия
        premium = self._extract_premium(text)
        if premium is not None:
            result['premium'] = premium
        
        # Все госномера из полиса
        plates = RE_PLATE.findall(text)
        result['all_plates'] = plates
        
        return result    
    
    def _extract_dates(self, text: str) -> Dict[str, str]:
        """Извлекает даты из текста"""
        result = {}
        
        # Явный период
        explicit = RE_PERIOD_EXPLICIT.search(text)
        if explicit:
            period_from = self._normalize_date(
                explicit.group(1), explicit.group(2), explicit.group(3)
            )
            period_to = self._normalize_date(
                explicit.group(4), explicit.group(5), explicit.group(6)
            )
            result['period_from'] = period_from
            result['period_to'] = period_to
        
        # Все даты подряд
        raw_dates = RE_DATE_ANY.findall(text)
        all_dates = []
        for d, m, y in raw_dates:
            date = self._normalize_date(d, m, y)
            if date:
                all_dates.append(date)
        
        if all_dates:
            result['contract_date'] = all_dates[0]
            if not result.get('period_from') and len(all_dates) > 1:
                result['period_from'] = all_dates[1]
            if not result.get('period_to') and len(all_dates) > 2:
                result['period_to'] = all_dates[2]
        
        return result
    
    def _normalize_date(self, day: str, month_ru: str, year: str) -> str | None:
        """Нормализует дату в формат YYYY-MM-DD"""
        months = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        
        month = months.get(month_ru.lower())
        if not month:
            return None
        
        return f"{year}-{month}-{int(day):02d}"
    
    def _extract_inns(self, text: str) -> List[str]:
        """Извлекает все ИНН из текста"""
        inns = []
        
        # Пробуем найти через ИНН/КПП
        inn_kpp_matches = RE_INN_KPP.findall(text)
        for match in inn_kpp_matches:
            if match[0]:  # ИНН
                inns.append(match[0])
        
        # Если не нашли через ИНН/КПП, ищем просто ИНН
        if not inns:
            inns = RE_INN.findall(text)
        
        # Оставляем только уникальные
        unique_inns = []
        for inn in inns:
            if inn not in unique_inns:
                unique_inns.append(inn)
        
        return unique_inns
    
    def _extract_premium(self, text: str) -> float | None:
        """Извлекает страховую премию"""
        pm = RE_PREMIUM.search(text)
        if pm:
            try:
                amount_str = pm.group(1).replace(" ", "").replace(",", ".")
                # Удаляем все нецифровые символы кроме точки
                amount_str = re.sub(r'[^\d.]', '', amount_str)
                if amount_str:
                    return float(amount_str)
            except Exception as e:
                log.warning(f"Ошибка при извлечении премии: {e}")
        return None
