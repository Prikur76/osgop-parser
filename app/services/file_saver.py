import csv
import json
import logging
import re
import asyncio

from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

from app.models.contract import OSGOPContract, VehicleInfo
from app.services.pdf_reader import extract_pages_as_pdf

log = logging.getLogger(__name__)


class FileSaver:
    def __init__(self, base_dir: str = "output"):
        self.base_dir = Path(base_dir)
        self.pdf_dir = self.base_dir / "pdf"
        self.json_dir = self.base_dir / "json"
        self.csv_dir = self.base_dir / "csv"
        
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Создание необходимых директорий"""
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.json_dir.mkdir(parents=True, exist_ok=True)
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        log.info(f"Директории созданы: {self.base_dir}")
    
    async def save_all(self, pdf_bytes: bytes,
                       contracts: List[OSGOPContract],
                       segments: List[Tuple[int, int]]) -> Dict[str, Any]:
        """
        Сохранение всех файлов по алгоритму (БЕЗ загрузки в Element).
        
        Возвращает словарь с информацией о сохраненных файлах.
        """
        if not contracts:
            log.warning("Нет контрактов для сохранения")
            return {}
        
        contract = contracts[0]
        
        result = {
            "contract": contract.model_dump(),
            "saved_files": {
                "pdf": [],
                "json": None,
                "csv": None,
                "csv_detailed": None
            },
            "statistics": {
                "total_vehicles": len(contract.vehicles),
                "vehicles_with_vin": sum(1 for v in contract.vehicles if v.vin),
                "vehicles_with_car_info": sum(1 for v in contract.vehicles if v.car_info),
                "parsing_date": datetime.now().isoformat()
            }
        }
        
        # Создаем список задач для сохранения PDF файлов
        pdf_save_tasks = []
        
        # 1. Сохраняем полис
        if len(segments) > 0:
            pdf_save_tasks.append(self._save_polis(pdf_bytes, contract, segments[0]))
        
        # 2. Сохраняем сведения для каждого ТС
        for i, vehicle in enumerate(contract.vehicles):
            if i + 1 < len(segments):
                pdf_save_tasks.append(
                    self._save_svedeniya(pdf_bytes, contract, vehicle, segments[i + 1])
                )
            else:
                log.warning(f"Нет сегмента для ТС {i+1} (всего сегментов: {len(segments)})")
        
        # Запускаем все задачи сохранения PDF параллельно
        if pdf_save_tasks:
            pdf_results = await asyncio.gather(*pdf_save_tasks, return_exceptions=True)
            for pdf_result in pdf_results:
                if isinstance(pdf_result, Exception):
                    log.error(f"Ошибка сохранения PDF: {pdf_result}")
                elif pdf_result:
                    result["saved_files"]["pdf"].append(pdf_result)
        
        # 3. Сохраняем JSON асинхронно
        json_filename = await self._save_json(contract)
        result["saved_files"]["json"] = json_filename
        
        # 4. Сохраняем CSV асинхронно
        csv_simple_filename = await self._save_csv(contract, include_car_info=False)
        result["saved_files"]["csv"] = csv_simple_filename
        
        # 5. Сохраняем CSV с детальной информацией асинхронно
        if contract.vehicles and any(v.car_info for v in contract.vehicles):
            csv_detailed_filename = await self._save_csv(contract, include_car_info=True)
            result["saved_files"]["csv_detailed"] = csv_detailed_filename
        
        log.info(f"Сохранено файлов: {len(result['saved_files']['pdf'])} PDF, 1 JSON, 1-2 CSV")
        
        return result
    
    async def _save_csv(self, contract: OSGOPContract, include_car_info: bool = False) -> str:
        """
        Асинхронное сохранение данных в CSV.
        """
        try:
            if not contract.vehicles:
                log.warning(f"Контракт {contract.contract_number} не содержит данных о ТС")
            
            suffix = "_detailed" if include_car_info else ""
            filename = f"OSGOP_{contract.contract_number}{suffix}.csv"
            filepath = self.csv_dir / filename
            
            headers = [
                "Номер договора",
                "Дата заключения",
                "Дата начала",
                "Дата окончания",
                "Страховщик",
                "ИНН страховщика",
                "Страхователь",
                "ИНН страхователя",
                "Страховая премия",
                "Госномер (рус)",
                "Госномер (англ)",
                "VIN код",
            ]
            
            if include_car_info:
                headers.extend([
                    "Модель автомобиля",
                    "Год выпуска",
                ])
            
            rows = []
            for vehicle in contract.vehicles:
                row = {
                    "Номер договора": contract.contract_number,
                    "Дата заключения": contract.contract_date,
                    "Дата начала": contract.period_from,
                    "Дата окончания": contract.period_to,
                    "Страховщик": contract.insurer,
                    "ИНН страховщика": contract.insurer_inn,
                    "Страхователь": contract.insured,
                    "ИНН страхователя": contract.insured_inn,
                    "Страховая премия": contract.bonus,
                    "Госномер (рус)": vehicle.vehicle_plate_cyr,
                    "Госномер (англ)": vehicle.vehicle_plate_lat,
                    "VIN код": vehicle.vin,
                }
                
                if include_car_info and vehicle.car_info:
                    car_info = vehicle.car_info
                    row.update({
                        "Модель автомобиля": car_info.get("model"),
                        "Год выпуска": car_info.get("year"),
                    })
                
                rows.append(row)
            
            # Используем asyncio.to_thread для синхронной операции ввода-вывода
            def write_csv():
                with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=headers, delimiter=';')
                    writer.writeheader()
                    for row in rows:
                        writer.writerow(row)
            
            await asyncio.to_thread(write_csv)
            
            log.info(f"Сохранен CSV: {filename} ({len(rows)} записей)")
            return str(filepath)
            
        except Exception as e:
            log.error(f"Ошибка сохранения CSV: {e}", exc_info=True)
            raise
    
    async def _save_polis(self, pdf_bytes: bytes, contract: OSGOPContract,
                         segment: Tuple[int, int]) -> Optional[str]:
        """
        Асинхронное сохранение полиса.
        """
        try:
            date_str = self._format_date_for_filename(contract.contract_date)
            filename = f"OSGOP_{contract.contract_number}_{date_str}.pdf"
            filepath = self.pdf_dir / filename
            
            start, end = segment
            pages = list(range(start, end))
            
            if not pages:
                log.warning(f"Пустой сегмент для полиса: {segment}")
                return None
            
            # Используем asyncio.to_thread для синхронной операции
            def extract_and_save():
                pages_pdf = extract_pages_as_pdf(pdf_bytes, pages)
                with open(filepath, "wb") as f:
                    f.write(pages_pdf)
            
            await asyncio.to_thread(extract_and_save)
            
            log.info(f"Сохранен полис: {filename}")
            return str(filepath)
            
        except Exception as e:
            log.error(f"Ошибка сохранения полиса: {e}")
            return None
    
    async def _save_svedeniya(self, pdf_bytes: bytes, contract: OSGOPContract,
                             vehicle: VehicleInfo, segment: Tuple[int, int]) -> Optional[str]:
        """
        Асинхронное сохранение сведений для одного ТС.
        """
        try:
            if vehicle.vin:
                identifier = self._sanitize_filename(vehicle.vin)
            else:
                identifier = self._sanitize_filename(vehicle.vehicle_plate_lat)
            
            date_to_use = contract.period_from if contract.period_from else contract.contract_date
            date_str = self._format_date_for_filename(date_to_use)
            
            filename = f"{identifier}_OSGOP_{contract.contract_number}_{date_str}.pdf"
            filepath = self.pdf_dir / filename
            
            start, end = segment
            pages = list(range(start, end))
            
            if not pages:
                log.warning(f"Пустой сегмент для сведений: {segment}")
                return None
            
            def extract_and_save():
                pages_pdf = extract_pages_as_pdf(pdf_bytes, pages)
                with open(filepath, "wb") as f:
                    f.write(pages_pdf)
            
            await asyncio.to_thread(extract_and_save)
            
            log.info(f"Сохранены сведения для {vehicle.vehicle_plate_cyr}: {filename}")
            return str(filepath)
            
        except Exception as e:
            log.error(f"Ошибка сохранения сведений для {getattr(vehicle, 'vehicle_plate_cyr', 'unknown')}: {e}")
            return None
    
    async def _save_json(self, contract: OSGOPContract) -> str:
        """
        Асинхронное сохранение данных в JSON.
        """
        try:
            def write_json():
                data = {
                    "contract": contract.model_dump(),
                    "parsing_info": {
                        "parsed_at": datetime.now().isoformat(),
                        "version": "1.0"
                    }
                }
                
                filename = f"OSGOP_{contract.contract_number}.json"
                filepath = self.json_dir / filename
                
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2, default=str)
                
                return str(filepath)
            
            filepath = await asyncio.to_thread(write_json)
            
            log.info(f"Сохранен JSON: {Path(filepath).name}")
            return filepath
            
        except Exception as e:
            log.error(f"Ошибка сохранения JSON: {e}")
            raise
    
    def _format_date_for_filename(self, date_str: Optional[str]) -> str:
        """
        Форматирование даты для имени файла: YYmmdd.
        Возвращает "000000" если дата не может быть распарсена.
        """
        if not date_str:
            return "000000"
        
        try:
            date_str = date_str.split('T')[0]
            
            formats = [
                "%Y-%m-%d",
                "%d.%m.%Y",
                "%d/%m/%Y",
                "%Y.%m.%d",
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime("%y%m%d")
                except ValueError:
                    continue
            
            return "000000"
            
        except Exception as e:
            log.warning(f"Ошибка форматирования даты '{date_str}': {e}")
            return "000000"
    
    def _sanitize_filename(self, filename: str) -> str:
        """
        Очистка строки для использования в имени файла.
        Удаляет недопустимые символы.
        """
        if not filename:
            return "unknown"
        
        invalid_chars = r'[<>:"/\\|?*]'
        sanitized = re.sub(invalid_chars, '_', filename)
        sanitized = sanitized.strip(' .')
        
        if len(sanitized) > 100:
            sanitized = sanitized[:100]
        
        return sanitized
