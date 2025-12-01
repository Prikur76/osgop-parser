import logging
import re
from typing import List, Optional, Dict, Any

from app.services.pdf_reader import extract_text_safe
from app.services.plate_normalizer import to_cyr_full, to_lat_full
from app.services.car_api_client import get_car_api_client
from app.models.contract import OSGOPContract, VehicleInfo
from app.utils.regex import (
    RE_ROSX,
    RE_PLATE,
    RE_POLIS_START,
    RE_SVEDENIYA_START,
    RE_PERIOD_EXPLICIT,
    RE_DATE_ANY,
    RE_CONTRACT_DATE,
    RE_INN,
    RE_PREMIUM,
    RE_INSURER,
    RE_INSURED,
)

log = logging.getLogger(__name__)


class OSGOPParser:
    def __init__(self):
        self.car_api_client = get_car_api_client()

    # ====================== ПУБЛИЧНЫЕ МЕТОДЫ ============================

    def parse_with_segments(self, pdf_bytes: bytes):
        pages = extract_text_safe(pdf_bytes)
        pages = [self._normalize_text(p) for p in pages]

        polis_start = self._find_polis_start(pages)
        if polis_start is None:
            log.error("Не найден основной полис ОСГОП")
            return [], []

        sved_starts = self._find_svedeniya_starts(pages, polis_start)

        if not sved_starts:
            text = "\n".join(pages)
            contract = self._parse_complete_polis(text)
            return [contract], [(0, len(pages))]

        polis_end = sved_starts[0]
        segments = [(polis_start, polis_end)]

        for i, st in enumerate(sved_starts):
            end = sved_starts[i + 1] if i + 1 < len(sved_starts) else len(pages)
            segments.append((st, end))

        polis_text = "\n".join(pages[polis_start:polis_end])
        header = self._parse_polis_header(polis_text)

        plates = []
        for st, en in segments[1:]:
            appendix = "\n".join(pages[st:en])
            pl = self._extract_plate_from_appendix(appendix)
            if pl:
                plates.append(pl)

        vehicles = self._get_vehicles_info(plates)

        contract = OSGOPContract(
            contract_number=header["contract_number"],
            contract_date=header["contract_date"],
            period_from=header["period_from"],
            period_to=header["period_to"],
            insurer=header["insurer"],
            insurer_inn=header["insurer_inn"],
            insured=header["insured"],
            insured_inn=header["insured_inn"],
            premium=header["premium"],
            vehicles=vehicles
        )

        return [contract], segments

    # ====================== НОРМАЛИЗАЦИЯ ТЕКСТА =========================

    def _normalize_text(self, text: str) -> str:
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\s*\n\s*", "\n", text)
        text = re.sub(r"([0-9])\s+([А-Яа-яёA-Za-z])", r"\1 \2", text)
        text = re.sub(r"([А-Яа-яёA-Za-z])\s+([0-9])", r"\1 \2", text)
        return text.strip()

    # ====================== СЕГМЕНТЫ =========================

    def _find_polis_start(self, pages):
        for i, p in enumerate(pages):
            if RE_POLIS_START.search(p.upper()):
                return i
        return None

    def _find_svedeniya_starts(self, pages, offset=0):
        res = []
        for i in range(offset, len(pages)):
            if RE_SVEDENIYA_START.search(pages[i].upper()):
                res.append(i)
        return res

    # ====================== ПАРСИНГ ПРИЛОЖЕНИЯ =========================

    def _extract_plate_from_appendix(self, text: str) -> Optional[str]:
        m = RE_PLATE.search(text)
        if not m:
            return None
        raw = m.group(0)
        plate_cyr = to_cyr_full(raw)
        plate_lat = to_lat_full(plate_cyr).upper().replace(" ", "")
        return plate_lat

    # ====================== ПАРСИНГ ТС =========================

    def _get_vehicles_info(self, plates: List[str]) -> List[VehicleInfo]:
        vehicles = []

        for plate in plates:
            try:
                car = self.car_api_client.get_car_extended_info(plate)
                vin = car.get("vin") if car else None
                vehicles.append(VehicleInfo(
                    vehicle_plate=plate,
                    vin=vin,
                    car_info=car or None
                ))
            except Exception as e:
                log.error(f"Ошибка API по {plate}: {e}")
                vehicles.append(VehicleInfo(vehicle_plate=plate))

        return vehicles

    # ====================== ПАРСИНГ ПОЛИСА =========================

    def _parse_complete_polis(self, text: str) -> OSGOPContract:
        data = self._parse_polis_header(text)

        plates = RE_PLATE.findall(text)
        plates_norm = [to_lat_full(to_cyr_full(p)).upper().replace(" ", "") for p in plates]

        vehicles = self._get_vehicles_info(list(set(plates_norm)))

        return OSGOPContract(
            contract_number=data["contract_number"],
            contract_date=data["contract_date"],
            period_from=data["period_from"],
            period_to=data["period_to"],
            insurer=data["insurer"],
            insurer_inn=data["insurer_inn"],
            insured=data["insured"],
            insured_inn=data["insured_inn"],
            premium=data["premium"],
            vehicles=vehicles
        )

    # ====================== ПАРСИНГ ХЕДЕРА =========================

    def _parse_polis_header(self, text: str) -> Dict[str, Any]:
        result = {}

        # --- НОМЕР ПОЛИСА ---
        m = RE_ROSX.search(text)
        result["contract_number"] = m.group(1) if m else None

        # --- ДАТЫ ---
        result.update(self._extract_dates(text))

        # --- СТРАХОВЩИК ---
        im = RE_INSURER.search(text)
        if im:
            insurer = self._clean_name(im.group(1))
            result["insurer"] = insurer

        # --- СТРАХОВАТЕЛЬ ---
        sm = RE_INSURED.search(text)
        if sm:
            insured = self._clean_name(sm.group(1))
            result["insured"] = insured

        # --- ИНН ---
        result["insurer_inn"], result["insured_inn"] = self._extract_inns(text)

        # --- ПРЕМИЯ ---
        pm = RE_PREMIUM.search(text)
        if pm:
            val = pm.group(2).replace(" ", "").replace(",", ".")
            val = re.sub(r"[^\d.]", "", val)
            result["premium"] = float(val) if val else None
        else:
            result["premium"] = None

        return result

    # ====================== ПАРСИНГ ДАТ =========================

    def _extract_dates(self, text: str) -> Dict[str, Optional[str]]:
        res = {
            "contract_date": None,
            "period_from": None,
            "period_to": None
        }

        # 1. Явная дата заключения договора
        cm = RE_CONTRACT_DATE.search(text)
        if cm:
            res["contract_date"] = self._normalize_date(cm.group(1), cm.group(2), cm.group(3))

        # 2. Период действия с ... по ...
        pm = RE_PERIOD_EXPLICIT.search(text)
        if pm:
            res["period_from"] = self._normalize_date(pm.group(1), pm.group(2), pm.group(3))
            res["period_to"] = self._normalize_date(pm.group(4), pm.group(5), pm.group(6))

        # 3. Все даты
        all_dates = [
            self._normalize_date(d, m, y)
            for d, m, y in RE_DATE_ANY.findall(text)
        ]

        # Удаляем даты года выпуска, формата "2023"
        all_dates = [d for d in all_dates if d and int(d[:4]) > 1900]

        if not res["contract_date"] and all_dates:
            res["contract_date"] = all_dates[0]

        if not res["period_from"] and len(all_dates) > 1:
            res["period_from"] = all_dates[1]

        if not res["period_to"] and len(all_dates) > 2:
            res["period_to"] = all_dates[2]

        return res

    def _normalize_date(self, day: str, month: str, year: str) -> Optional[str]:
        months = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        m = months.get(month.lower())
        if not m:
            return None
        return f"{year}-{m}-{int(day):02d}"

    # ====================== ИНН =========================

    def _extract_inns(self, text: str):
        inns = RE_INN.findall(text)

        insurer_block = text.split("Страхователь")[0]
        insured_block = text.split("Страхователь")[1] if "Страхователь" in text else ""

        insurer_inn = None
        insured_inn = None

        im = RE_INN.search(insurer_block)
        if im:
            insurer_inn = im.group(1)

        sm = RE_INN.search(insured_block)
        if sm:
            insured_inn = sm.group(1)

        if not insurer_inn and inns:
            insurer_inn = inns[0]
        if not insured_inn and len(inns) > 1:
            insured_inn = inns[1]

        return insurer_inn, insured_inn

    # ====================== ОЧИСТКА НАЗВАНИЙ =========================

    def _clean_name(self, text: str) -> str:
        text = re.sub(r"Лицензия.*$", "", text)
        text = re.sub(r"\([^)]*\)", "", text)
        text = re.sub(r"[,.;:]+$", "", text)
        text = " ".join(text.split())
        return text.strip()
