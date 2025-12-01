import json
import re
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse, Response

from app.models.contract import VehicleInfo
from app.services.parser import OSGOPParser
from app.services.pdf_splitter import save_pdf_pages
from app.services.storage import PDF_DIR, ensure_dirs


router = APIRouter()


@router.post("/parse/json-download")
async def parse_json_download(file: UploadFile = File(...), 
                              use_vin_in_filenames: bool = True,
                              use_inn_filter: bool = False):
    """Возвращает JSON файл для скачивания"""
    ensure_dirs()

    pdf_bytes = await file.read()
    parser = OSGOPParser()

    # Получаем полисы и сегменты
    contracts, segments = parser.parse_with_segments(pdf_bytes)
    
    if not contracts:
        return JSONResponse(
            content={"error": "Не удалось распарсить документ"},
            status_code=400
        )
    
    contract = contracts[0]
    saved_files = []
    
    # Сохраняем каждый сегмент
    for idx, (start, end) in enumerate(segments):
        if idx == 0:
            # Основной полис
            date_short = contract.contract_date.replace("-", "")[2:8] if contract.contract_date else datetime.now().strftime("%y%m%d")
            filename = f"OSGOP_{contract.contract_number}_{date_short}.pdf"
        else:
            # Приложение по ТС
            vehicle_idx = idx - 1
            if vehicle_idx < len(contract.vehicles):
                vehicle = contract.vehicles[vehicle_idx]
                
                # Определяем идентификатор для имени файла
                identifier = _get_vehicle_identifier(vehicle, use_vin_in_filenames)
                
                # Формируем дату из даты договора
                date_short = contract.contract_date.replace("-", "")[2:8] if contract.contract_date else "000000"
                
                filename = f"{identifier}_OSGOP_{contract.contract_number}_{date_short}.pdf"
            else:
                filename = f"APPENDIX_{idx}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        
        saved_path = save_pdf_pages(
            pdf_bytes=pdf_bytes,
            pages=list(range(start, end)),
            filename=filename,
            outdir=PDF_DIR
        )
        
        saved_files.append(str(saved_path))
    
    # Формируем JSON
    response_data = {
        "contract": contract.model_dump(),
        "saved_pdf": saved_files,
        "use_vin_in_filenames": use_vin_in_filenames,
        "use_inn_filter": use_inn_filter,
        "statistics": {
            "total_vehicles": contract.vehicles_count,
            "vehicles_with_vin": contract.vehicles_with_vin_count,
            "parsing_date": datetime.now().isoformat()
        }
    }
    
    # Создаем JSON файл для скачивания
    json_str = json.dumps(response_data, ensure_ascii=False, indent=2)
    
    # Создаем имя файла
    filename = f"OSGOP_{contract.contract_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    return Response(
        content=json_str,
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


def _get_vehicle_identifier(vehicle: VehicleInfo, use_vin: bool) -> str:
    """
    Определяет идентификатор для имени файла.
    
    :param vehicle: Информация о транспортном средстве
    :param use_vin: Использовать ли VIN
    :return: Идентификатор (VIN или госномер)
    """
    if use_vin and vehicle.vin:
        # Используем VIN, удаляем недопустимые символы для имени файла
        vin_clean = re.sub(r'[<>:"/\\|?*]', '_', vehicle.vin)
        # Ограничиваем длину и удаляем пробелы
        return vin_clean.strip()[:50].replace(" ", "_")
    else:
        # Используем госномер
        return vehicle.vehicle_plate


@router.post("/parse/csv")
async def parse_csv(file: UploadFile = File(...), 
                    include_car_info: bool = False,
                    use_inn_filter: bool = False):
    """Возвращает CSV с детализацией по ТС"""
    pdf_bytes = await file.read()
    parser = OSGOPParser()

    try:
        contracts = parser.parse_with_segments(pdf_bytes)
        
        if not contracts:
            raise HTTPException(status_code=400, detail="Не удалось распарсить документ")
        
        contract = contracts[0]
        
        # Создаем таблицу с данными
        import pandas as pd
        
        data = []
        for vehicle in contract.vehicles:
            row = {
                "contract_number": contract.contract_number,
                "contract_date": contract.contract_date,
                "period_from": contract.period_from,
                "period_to": contract.period_to,
                "insurer": contract.insurer,
                "insurer_inn": contract.insurer_inn,
                "insured": contract.insured,
                "insured_inn": contract.insured_inn,
                "premium": contract.premium,
                "vehicle_plate": vehicle.vehicle_plate,
                "vin": vehicle.vin,
            }
            
            # Добавляем информацию из API если нужно
            if include_car_info and vehicle.car_info:
                car_info = vehicle.car_info
                row.update({
                    "car_model": car_info.get('model'),
                    "car_brand": car_info.get('brand'),
                    "car_year": car_info.get('year'),
                    "car_status": car_info.get('status'),
                    "car_activity": car_info.get('activity'),
                    "sts_series": car_info.get('sts_series'),
                    "sts_number": car_info.get('sts_number'),
                })
            
            data.append(row)
        
        df = pd.DataFrame(data)
        csv_data = df.to_csv(index=False, encoding='utf-8-sig')
        
        return StreamingResponse(
            content=csv_data,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=osgop_{contract.contract_number}.csv"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/debug/test-api")
async def test_api_connection(plate: str, inn: Optional[str] = None):
    """
    Тестовый endpoint для проверки работы API 1С.
    """
    from app.services.car_api_client import get_car_api_client
    from app.services.plate_normalizer import normalize_plate_for_api, normalize_plate_for_storage
    
    api_client = get_car_api_client()
    
    if not api_client.enabled:
        return {"error": "API 1С отключен в конфигурации"}
    
    results = {
        "input_plate": plate,
        "normalized_for_api": normalize_plate_for_api(plate),
        "normalized_for_storage": normalize_plate_for_storage(plate),
        "inn": inn,
        "tests": []
    }
    
    # Тест 1: Поиск с ИНН
    if inn:
        cars_with_inn = api_client.get_cars_with_filters(num=plate, inn=inn)
        results["tests"].append({
            "name": "Поиск с ИНН",
            "params": {"num": plate, "inn": inn},
            "found": len(cars_with_inn),
            "cars": cars_with_inn[:5]  # Ограничиваем вывод
        })
    
    # Тест 2: Поиск без ИНН
    cars_without_inn = api_client.get_cars_with_filters(num=plate)
    results["tests"].append({
        "name": "Поиск без ИНН",
        "params": {"num": plate},
        "found": len(cars_without_inn),
        "cars": cars_without_inn[:5]
    })
    
    # Тест 3: Получение VIN
    vin = api_client.get_vin_by_plate(plate)
    results["vin_result"] = vin
    
    return results