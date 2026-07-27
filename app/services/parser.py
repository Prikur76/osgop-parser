import logging
import re
import asyncio
from typing import List, Optional, Tuple

from app.models.contract import OSGOPContract, VehicleInfo
from app.services.pdf_reader import extract_text_safe
from app.services.segment_detector import detect_segments, normalize_page_text
from app.services.field_extractors import (
    parse_polis_header,
    parse_svedeniya,
    extract_contract_date_from_svedeniya,
)

log = logging.getLogger(__name__)


class OSGOPParser:
    def __init__(self, element_api_client=None, plate_cache=None):
        """
        Инициализация парсера ОСГОП.

        Args:
            element_api_client: Асинхронный клиент для Element API (опционально)
            plate_cache: Кэш госномеров PlateCache (опционально)
        """
        self.element_api_client = element_api_client
        self.plate_cache = plate_cache
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
            normalized_pages = [normalize_page_text(page) for page in pages]

            # 1. Находим все сегменты документа
            segments = detect_segments(normalized_pages)

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
                # Fallback: нет POLIS-секции — пробуем извлечь заголовок из сведений
                if svedeniya_segments:
                    log.warning("Не найден полис в документе, извлекаю заголовок из сведений")
                    first_sved_text = "\n".join(normalized_pages[svedeniya_segments[0][0]:svedeniya_segments[0][1]])
                    header_data = parse_polis_header(first_sved_text)
                else:
                    log.error("Не найдены ни полис, ни сведения в документе")
                    return [], []

            # 3. Парсим полис
            if polis_segment:
                polis_text = "\n".join(normalized_pages[polis_segment[0]:polis_segment[1]])
                header_data = parse_polis_header(polis_text)

            # 4. Парсим сведения и извлекаем госномера
            vehicles_data = []

            # Для первого сведения извлекаем дату заключения договора
            if svedeniya_segments:
                first_sved_text = "\n".join(normalized_pages[svedeniya_segments[0][0]:svedeniya_segments[0][1]])
                sved_date = extract_contract_date_from_svedeniya(first_sved_text)
                if sved_date:
                    self.contract_date_from_svedeniya = sved_date
                    log.info(f"Дата из сведений: {self.contract_date_from_svedeniya}")

            # Парсим все сведения
            for i, (start, end) in enumerate(svedeniya_segments):
                sved_text = "\n".join(normalized_pages[start:end])
                vehicle_data = parse_svedeniya(sved_text)
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
            if polis_segment:
                all_segments.append((polis_segment[0], polis_segment[1]))
            else:
                all_segments.append((0, 0))  # нет полиса — пустой сегмент
            for start, end in svedeniya_segments:
                all_segments.append((start, end))

            log.info(f"Успешно распарсен договор {contract.contract_number} с {len(vehicles)} ТС")
            return [contract], all_segments

        except Exception as e:
            log.error(f"Ошибка парсинга: {str(e)}", exc_info=True)
            return [], []

    async def parse_split_files(
        self,
        polis_pdf_bytes: bytes,
        svedeniya_pdf_bytes: bytes
    ) -> Tuple[List[OSGOPContract], List[Tuple[int, int]]]:
        """Раздельный парсинг: полис и сведения как два отдельных PDF."""
        try:
            # --- Полис ---
            polis_pages = await self._extract_text_async(polis_pdf_bytes)
            log.info(f"Полис: загружено {len(polis_pages)} страниц")
            polis_normalized = [normalize_page_text(p) for p in polis_pages]
            header_data = parse_polis_header("\n".join(polis_normalized))

            # --- Сведения ---
            sved_pages = await self._extract_text_async(svedeniya_pdf_bytes)
            log.info(f"Сведения: загружено {len(sved_pages)} страниц")
            sved_normalized = [normalize_page_text(p) for p in sved_pages]

            # Ищем SVEDENIYA-сегменты внутри сведенческого PDF
            sved_raw = detect_segments(sved_normalized)

            # Если detect_segments ничего не нашёл — fallback: весь текст как один блок
            if not sved_raw:
                log.warning("detect_segments не нашёл сегментов в сведениях, парсим весь текст как один блок")
                sved_text = "\n".join(sved_normalized)
                vehicle_data = parse_svedeniya(sved_text)
                vehicles_data = [vehicle_data] if vehicle_data else []
                sved_segments = [(0, len(sved_pages))]
            else:
                vehicles_data = []
                sved_segments = []
                for start, end, seg_type in sved_raw:
                    if seg_type != "SVEDENIYA":
                        continue
                    sved_segments.append((start, end))
                    sved_text = "\n".join(sved_normalized[start:end])
                    vehicle_data = parse_svedeniya(sved_text)
                    if vehicle_data:
                        vehicles_data.append(vehicle_data)

                if not vehicles_data:
                    log.warning("Не найдены данные о ТС в сведениях")

            # Дата из первого сведения
            first_sved_text = "\n".join(sved_normalized[sved_segments[0][0]:sved_segments[0][1]])
            sved_date = extract_contract_date_from_svedeniya(first_sved_text)
            if sved_date:
                self.contract_date_from_svedeniya = sved_date

            # --- Обогащение через Element API ---
            vehicles = await self._get_vehicles_info_from_element(vehicles_data)

            # --- Сборка контракта ---
            if not header_data.get("contract_date") and self.contract_date_from_svedeniya:
                header_data["contract_date"] = self.contract_date_from_svedeniya

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

            # Сегменты для FileSaver: полис (весь), сведения (блоки внутри svedeniya PDF)
            all_segments = [(0, len(polis_pages))]
            all_segments.extend(sved_segments)

            log.info(f"Успешно распарсен договор {contract.contract_number} с {len(vehicles)} ТС (split mode)")
            return [contract], all_segments

        except Exception as e:
            log.error(f"Ошибка раздельного парсинга: {str(e)}", exc_info=True)
            return [], []

    async def _extract_text_async(self, pdf_bytes: bytes) -> List[str]:
        """Асинхронное извлечение текста из PDF"""
        # Выполняем CPU-bound операцию в отдельном потоке
        return await asyncio.to_thread(extract_text_safe, pdf_bytes)

    # ====================== РАБОТА С ELEMENT API =========================

    async def _get_vehicles_info_from_element(self, vehicles_data: list) -> List[VehicleInfo]:
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
                    sts_series=None,
                    sts_number=None,
                    car_info=None
                )
                vehicles.append(vehicle)
                log.info(f"Добавлен ТС (без данных из Element): {data.get('plate_cyr')}")
            return vehicles

        async def process_vehicle(data: dict) -> Optional[VehicleInfo]:
            plate_lat = data.get("plate_lat")
            plate_cyr = data.get("plate_cyr")

            if not plate_lat or not plate_cyr:
                log.warning(f"Пропуск ТС: нет plate_lat или plate_cyr в данных {data}")
                return None

            try:
                vin = None
                sts_series = None
                sts_number = None
                car_info = None

                # 1. Сначала проверяем локальный кэш
                if self.plate_cache:
                    cached = await self.plate_cache.get(plate_cyr)
                    if cached:
                        vin = cached.get("vin")
                        sts_series = cached.get("sts_series")
                        sts_number = cached.get("sts_number")
                        car_info = {
                            "model": cached.get("model", ""),
                            "year": cached.get("year", ""),
                            "code": cached.get("code", ""),
                        }
                        log.info(f"Взято из кэша: {plate_cyr} -> VIN: {vin or 'не найден'}")
                        return VehicleInfo(
                            vehicle_plate_cyr=plate_cyr,
                            vehicle_plate_lat=plate_lat,
                            vin=vin,
                            sts_series=sts_series,
                            sts_number=sts_number,
                            car_info=car_info
                        )

                # 2. В кэше нет — идём в Element API
                car_data = await self.element_api_client.get_car_by_plate(plate_cyr)

                if car_data:
                    # Извлекаем VIN
                    vin = car_data.get("VIN") or car_data.get("vin")
                    if vin and isinstance(vin, str):
                        vin = vin.strip()
                        if vin in ("", "0", "Нет данных"):
                            vin = None

                    # Извлекаем СТС
                    sts_series = car_data.get("STSSeries") or ""
                    sts_number = car_data.get("STSNumber") or ""
                    if sts_series:
                        sts_series = str(sts_series).strip()
                    if sts_number:
                        sts_number = str(sts_number).strip()

                    # Собираем информацию об автомобиле
                    car_info = {
                        "model": car_data.get("Model") or car_data.get("model") or "",
                        "year": car_data.get("YearCar") or car_data.get("year") or "",
                        "code": car_data.get("Code") or car_data.get("code") or "",
                    }

                    # 3. Сохраняем в кэш для будущих запросов
                    if self.plate_cache:
                        await self.plate_cache.put(plate_cyr, car_data)

                vehicle = VehicleInfo(
                    vehicle_plate_cyr=plate_cyr,
                    vehicle_plate_lat=plate_lat,
                    vin=vin,
                    sts_series=sts_series or None,
                    sts_number=sts_number or None,
                    car_info=car_info
                )

                log.info(f"Добавлен ТС: {plate_cyr} -> {plate_lat}, VIN: {vin or 'не найден'}")
                return vehicle

            except Exception as e:
                log.error(f"Ошибка при обработке ТС {plate_lat} в Element: {e}")
                vehicle = VehicleInfo(
                    vehicle_plate_cyr=plate_cyr,
                    vehicle_plate_lat=plate_lat,
                    vin=None,
                    sts_series=None,
                    sts_number=None,
                    car_info=None
                )
                log.info(f"Добавлен ТС (без данных из Element): {plate_cyr}")
                return vehicle

        # Создаем задачи для всех ТС с ограничением конкурентности (не больше 3 одновременных запросов к API)
        semaphore = asyncio.Semaphore(3)

        async def process_with_limit(data: dict) -> Optional[VehicleInfo]:
            async with semaphore:
                return await process_vehicle(data)

        tasks = [process_with_limit(data) for data in vehicles_data]
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
