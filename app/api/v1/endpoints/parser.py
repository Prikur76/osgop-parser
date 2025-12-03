# app/api/v1/endpoints/parser.py
import json
import tempfile
import zipfile
import io
import asyncio

from datetime import datetime
from pathlib import Path
from typing import List, Optional
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse, Response, FileResponse

from app.services.parser_factory import get_osgop_parser, close_osgop_parser_resources
from app.services.file_saver import FileSaver
from app.services.element_api_client_async import ElementApiClientAsync
from app.core.config import config

router = APIRouter()


async def get_element_client() -> Optional[ElementApiClientAsync]:
    """Создает и возвращает клиент Element API если включен"""
    if config.ELEMENT_ENABLED:
        try:
            client = await ElementApiClientAsync(
                base_url=config.ELEMENT_BASE_URL,
                username=config.ELEMENT_USERNAME,
                password=config.ELEMENT_PASSWORD
            ).init()
            return client
        except Exception as e:
            print(f"Ошибка создания Element API клиента: {e}")
    return None


@router.post("/parse/json")
async def parse_json_download(file: UploadFile = File(...), 
                              use_vin_in_filenames: bool = True,
                              use_element_api: bool = True):
    """Возвращает JSON файл для скачивания"""
    element_client = None
    parser = None
    
    try:
        # Читаем PDF
        pdf_bytes = await file.read()
        
        # Создаем клиент Element API если нужно
        element_client = await get_element_client() if use_element_api else None
        
        # Получаем парсер
        parser = await get_osgop_parser(element_client)
        
        # Парсим документ асинхронно
        contracts, segments = await parser.parse_with_segments(pdf_bytes)
        
        if not contracts:
            return JSONResponse(
                content={"error": "Не удалось распарсить документ"},
                status_code=400
            )
        
        contract = contracts[0]
        
        # Сохраняем файлы асинхронно
        saver = FileSaver()
       
        save_result = await saver.save_all(pdf_bytes, contracts, segments)
        
        # Формируем ответ
        response_data = {
            "contract": contract.model_dump(),
            "saved_files": save_result["saved_files"],
            "use_vin_in_filenames": use_vin_in_filenames,
            "statistics": save_result["statistics"],
            "element_upload": save_result.get("element_upload", {})
        }
        
        # Создаем JSON файл для скачивания
        json_str = json.dumps(response_data, ensure_ascii=False, indent=2, default=str)
        
        # Имя файла для скачивания
        filename = f"OSGOP_{contract.contract_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        return Response(
            content=json_str,
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка обработки: {str(e)}")
    
    finally:
        # Закрываем ресурсы
        if parser:
            await close_osgop_parser_resources(parser)
        if element_client and not use_element_api:
            await element_client.close()


@router.post("/parse/csv")
async def parse_csv(file: UploadFile = File(...), 
                    include_car_info: bool = False,
                    use_element_api: bool = True):
    """Возвращает CSV с детализацией по ТС"""
    element_client = None
    parser = None
    
    try:
        pdf_bytes = await file.read()
        
        # Создаем клиент Element API если нужно
        element_client = await get_element_client() if use_element_api else None
        
        # Получаем парсер
        parser = await get_osgop_parser(element_client)
        
        # Парсим документ асинхронно
        contracts, _ = await parser.parse_with_segments(pdf_bytes)
        
        if not contracts:
            raise HTTPException(status_code=400, detail="Не удалось распарсить документ")
        
        contract = contracts[0]
        
        # Создаем CSV
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
                "bonus": contract.bonus,
                "vehicle_plate_cyr": vehicle.vehicle_plate_cyr,      # Кириллица
                "vehicle_plate_lat": vehicle.vehicle_plate_lat,      # Латинница
                "vin": vehicle.vin,
            }
            
            # Добавляем информацию из API если нужно
            if include_car_info and vehicle.car_info:
                car_info = vehicle.car_info
                row.update({                    
                    "car_model": car_info.get("model"),
                    "car_year": car_info.get("year"),
                    "car_code": car_info.get("code"),
                })
            
            data.append(row)
        
        df = pd.DataFrame(data)
        
        # Определяем разделитель в зависимости от локали
        from io import StringIO
        
        output = StringIO()
        
        if include_car_info:
            # Для русских данных используем разделитель ';'
            df.to_csv(output, index=False, sep=';', encoding='utf-8-sig')
        else:
            # Для простых данных используем стандартный разделитель ','
            df.to_csv(output, index=False, encoding='utf-8')
        
        csv_data = output.getvalue()
        
        suffix = "_detailed" if include_car_info else ""
        return StreamingResponse(
            content=csv_data,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=osgop_{contract.contract_number}{suffix}.csv"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        # Закрываем ресурсы
        if parser:
            await close_osgop_parser_resources(parser)
        if element_client and not use_element_api:
            await element_client.close()


@router.post("/parse/csv/save")
async def parse_and_save_csv(file: UploadFile = File(...), 
                             include_car_info: bool = True,
                             use_element_api: bool = True):
    """
    Парсит документ и сохраняет CSV используя FileSaver.
    Возвращает CSV файл для скачивания.
    """
    element_client = None
    parser = None
    
    try:
        pdf_bytes = await file.read()
        
        # Создаем клиент Element API если нужно
        element_client = await get_element_client() if use_element_api else None
        
        # Получаем парсер
        parser = await get_osgop_parser(element_client)
        
        # Парсим документ асинхронно
        contracts, _ = await parser.parse_with_segments(pdf_bytes)
        
        if not contracts:
            raise HTTPException(status_code=400, detail="Не удалось распарсить документ")
        
        contract = contracts[0]
        
        # Используем FileSaver для сохранения CSV
        saver = FileSaver()
        
        # Сохраняем CSV асинхронно
        csv_path = await saver._save_csv_async(contract, include_car_info=include_car_info)
        
        # Возвращаем файл для скачивания
        filename = Path(csv_path).name
        
        return FileResponse(
            path=csv_path,
            media_type="text/csv",
            filename=filename,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка сохранения CSV: {str(e)}")
    
    finally:
        # Закрываем ресурсы
        if parser:
            await close_osgop_parser_resources(parser)
        if element_client and not use_element_api:
            await element_client.close()


@router.post("/parse/all-formats")
async def parse_all_formats(file: UploadFile = File(...),
                            include_car_info: bool = True,
                            upload_to_element: bool = False):
    """
    Парсит документ и возвращает ZIP архив со всеми форматами.
    Включает: JSON, PDF полис, PDF сведения, CSV.
    """
    element_client = None
    parser = None
    
    try:
        pdf_bytes = await file.read()
        
        # Создаем клиент Element API
        element_client = await get_element_client()
        
        # Получаем парсер
        parser = await get_osgop_parser(element_client)
        
        # Парсим документ асинхронно
        contracts, segments = await parser.parse_with_segments(pdf_bytes)
        
        if not contracts:
            raise HTTPException(status_code=400, detail="Не удалось распарсить документ")
        
        contract = contracts[0]
        
        # Создаем временную директорию
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Создаем FileSaver с временной директорией
            saver = FileSaver(base_dir=str(temp_path))
            
            # Сохраняем все файлы асинхронно
            save_result = await saver.save_all(pdf_bytes, contracts, segments)
            
            # Создаем ZIP архив
            zip_filename = f"OSGOP_{contract.contract_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            zip_buffer = io.BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Добавляем все сохраненные файлы
                saved_files = save_result["saved_files"]
                
                # JSON
                if saved_files["json"]:
                    json_path = Path(saved_files["json"])
                    if json_path.exists():
                        zip_file.write(str(json_path), json_path.name)
                
                # CSV
                if saved_files["csv"]:
                    csv_path = Path(saved_files["csv"])
                    if csv_path.exists():
                        zip_file.write(str(csv_path), csv_path.name)
                
                # CSV detailed (если есть)
                if saved_files.get("csv_detailed"):
                    csv_detailed_path = Path(saved_files["csv_detailed"])
                    if csv_detailed_path.exists():
                        zip_file.write(str(csv_detailed_path), csv_detailed_path.name)
                
                # PDF файлы
                for pdf_file in saved_files["pdf"]:
                    pdf_path = Path(pdf_file)
                    if pdf_path.exists():
                        zip_file.write(str(pdf_path), pdf_path.name)
            
            zip_buffer.seek(0)
            
            return StreamingResponse(
                content=zip_buffer,
                media_type="application/zip",
                headers={"Content-Disposition": f"attachment; filename={zip_filename}"}
            )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка создания архива: {str(e)}")
    
    finally:
        # Закрываем ресурсы
        if parser:
            await close_osgop_parser_resources(parser)
        if element_client and not upload_to_element:
            await element_client.close()


@router.post("/parse/csv-only")
async def parse_csv_only(file: UploadFile = File(...), 
                        simple: bool = False,
                        detailed: bool = True,
                        use_element_api: bool = True):
    """
    Парсит документ и возвращает CSV файл.
    Опции:
    - simple: возвращает простой CSV (только базовые данные)
    - detailed: возвращает детальный CSV (с информацией об автомобилях)
    """
    element_client = None
    parser = None
    
    try:
        pdf_bytes = await file.read()
        
        # Создаем клиент Element API если нужно
        element_client = await get_element_client() if use_element_api else None
        
        # Получаем парсер
        parser = await get_osgop_parser(element_client)
        
        # Парсим документ асинхронно
        contracts, _ = await parser.parse_with_segments(pdf_bytes)
        
        if not contracts:
            raise HTTPException(status_code=400, detail="Не удалось распарсить документ")
        
        contract = contracts[0]
        
        # Используем FileSaver
        saver = FileSaver()
        
        # Определяем какие CSV возвращать
        csv_files = []
        
        if simple:
            csv_simple = await saver._save_csv_async(contract, include_car_info=False)
            csv_files.append(("simple", csv_simple))
        
        if detailed:
            csv_detailed = await saver._save_csv_async(contract, include_car_info=True)
            csv_files.append(("detailed", csv_detailed))
        
        # Если запрошен только один файл - возвращаем его напрямую
        if len(csv_files) == 1:
            name, path = csv_files[0]
            filename = Path(path).name
            
            return FileResponse(
                path=path,
                media_type="text/csv",
                filename=filename,
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
        # Если запрошено несколько файлов - возвращаем ZIP архив
        elif csv_files:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Создаем ZIP архив
                zip_filename = f"OSGOP_{contract.contract_number}_CSV_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
                zip_buffer = io.BytesIO()
                
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    for name, csv_path in csv_files:
                        path_obj = Path(csv_path)
                        if path_obj.exists():
                            zip_file.write(str(path_obj), f"{contract.contract_number}_{name}.csv")
                
                zip_buffer.seek(0)
                
                return StreamingResponse(
                    content=zip_buffer,
                    media_type="application/zip",
                    headers={"Content-Disposition": f"attachment; filename={zip_filename}"}
                )
        
        else:
            raise HTTPException(status_code=400, detail="Не выбран ни один формат CSV")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка создания CSV: {str(e)}")
    
    finally:
        # Закрываем ресурсы
        if parser:
            await close_osgop_parser_resources(parser)
        if element_client and not use_element_api:
            await element_client.close()


@router.post("/parse/test")
async def parse_test(file: UploadFile = File(...),
                    use_element_api: bool = False):
    """Тестовый эндпоинт для проверки парсинга"""
    element_client = None
    parser = None
    
    try:
        pdf_bytes = await file.read()
        
        # Создаем клиент Element API если нужно
        element_client = await get_element_client() if use_element_api else None
        
        # Получаем парсер
        parser = await get_osgop_parser(element_client)
        
        # Парсим документ асинхронно
        contracts, segments = await parser.parse_with_segments(pdf_bytes)
        
        if not contracts:
            return {"error": "Не удалось распарсить документ", "success": False}
        
        contract = contracts[0]
        
        # Тест сохранения CSV
        saver = FileSaver()
        csv_path_simple = None
        csv_path_detailed = None
        
        try:
            csv_path_simple = await saver._save_csv_async(contract, include_car_info=False)
        except Exception as csv_err:
            csv_path_simple = f"Ошибка: {csv_err}"
        
        try:
            csv_path_detailed = await saver._save_csv_async(contract, include_car_info=True)
        except Exception as csv_err:
            csv_path_detailed = f"Ошибка: {csv_err}"
        
        return {
            "success": True,
            "contract": {
                "number": contract.contract_number,
                "date": contract.contract_date,
                "period": f"{contract.period_from} - {contract.period_to}",
                "insurer": contract.insurer,
                "insured": contract.insured,
                "vehicles_count": len(contract.vehicles),
            },
            "vehicles": [
                {
                    "plate_cyr": v.vehicle_plate_cyr,
                    "plate_lat": v.vehicle_plate_lat,
                    "vin": v.vin,
                    "has_car_info": v.car_info is not None,
                    "car_info": v.car_info if v.car_info else None
                }
                for v in contract.vehicles[:5]  # Первые 5 для примера
            ],
            "segments": len(segments),
            "csv_test": {
                "simple_created": csv_path_simple is not None and "Ошибка" not in str(csv_path_simple),
                "detailed_created": csv_path_detailed is not None and "Ошибка" not in str(csv_path_detailed),
                "simple_path": str(csv_path_simple) if csv_path_simple else None,
                "detailed_path": str(csv_path_detailed) if csv_path_detailed else None
            },
            "element_api_used": element_client is not None
        }
        
    except Exception as e:
        return {"error": str(e), "success": False}
    
    finally:
        # Закрываем ресурсы
        if parser:
            await close_osgop_parser_resources(parser)
        if element_client and not use_element_api:
            await element_client.close()


@router.post("/parse/batch-csv")
async def parse_batch_csv(files: List[UploadFile] = File(...),
                          include_car_info: bool = True,
                          use_element_api: bool = True):
    """
    Парсит несколько документов и возвращает ZIP архив с CSV файлами.
    """
    element_client = None
    parser = None
    
    try:
        if not files:
            raise HTTPException(status_code=400, detail="Нет файлов для обработки")
        
        # Создаем клиент Element API если нужно (один для всех файлов)
        element_client = await get_element_client() if use_element_api else None
        
        # Получаем парсер
        parser = await get_osgop_parser(element_client)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Создаем ZIP архив
            zip_filename = f"OSGOP_Batch_CSV_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            zip_buffer = io.BytesIO()
            
            saver = FileSaver(base_dir=str(temp_path))
            
            # Создаем задачи для параллельной обработки файлов
            async def process_single_file(file: UploadFile, index: int):
                try:
                    pdf_bytes = await file.read()
                    contracts, _ = await parser.parse_with_segments(pdf_bytes)
                    
                    if contracts:
                        contract = contracts[0]
                        
                        # Сохраняем CSV
                        csv_path = await saver._save_csv_async(
                            contract, 
                            include_car_info=include_car_info
                        )
                        
                        return {
                            "success": True,
                            "index": index,
                            "contract": contract,
                            "csv_path": csv_path,
                            "filename": file.filename
                        }
                    else:
                        return {
                            "success": False,
                            "index": index,
                            "error": "Не удалось распарсить документ",
                            "filename": file.filename
                        }
                        
                except Exception as file_err:
                    return {
                        "success": False,
                        "index": index,
                        "error": str(file_err),
                        "filename": file.filename
                    }
            
            # Запускаем все задачи параллельно
            tasks = [process_single_file(file, i) for i, file in enumerate(files, 1)]
            results = await asyncio.gather(*tasks)
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for result in results:
                    if result["success"]:
                        # Добавляем успешно обработанный CSV
                        csv_path = Path(result["csv_path"])
                        if csv_path.exists():
                            original_name = Path(result["filename"]).stem
                            zip_name = f"{result['index']:03d}_{result['contract'].contract_number}_{original_name}.csv"
                            zip_file.write(str(csv_path), zip_name)
                    else:
                        # Создаем файл с ошибкой
                        error_content = f"Ошибка обработки файла {result['filename']}: {result.get('error', 'Неизвестная ошибка')}"
                        error_filename = f"{result['index']:03d}_ERROR_{Path(result['filename']).stem}.txt"
                        zip_file.writestr(error_filename, error_content)
            
            zip_buffer.seek(0)
            
            return StreamingResponse(
                content=zip_buffer,
                media_type="application/zip",
                headers={"Content-Disposition": f"attachment; filename={zip_filename}"}
            )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка пакетной обработки: {str(e)}")
    
    finally:
        # Закрываем ресурсы
        if parser:
            await close_osgop_parser_resources(parser)
        if element_client and not use_element_api:
            await element_client.close()


@router.post("/parse/async-batch")
async def parse_async_batch(files: List[UploadFile] = File(...),
                           background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Запускает асинхронную пакетную обработку в фоне.
    Возвращает ID задачи для отслеживания статуса.
    """
    from uuid import uuid4
    
    task_id = str(uuid4())
    
    # Хранилище для статусов задач (в реальном приложении используйте Redis или БД)
    task_statuses = {}
    
    async def process_batch_task(task_id: str, files: List[UploadFile]):
        """Фоновая задача для пакетной обработки"""
        task_statuses[task_id] = {
            "status": "processing",
            "total": len(files),
            "processed": 0,
            "successful": 0,
            "failed": 0,
            "results": []
        }
        
        element_client = None
        parser = None
        
        try:
            # Создаем клиент Element API
            element_client = await get_element_client()
            
            # Получаем парсер
            parser = await get_osgop_parser(element_client)
            
            for i, file in enumerate(files, 1):
                try:
                    pdf_bytes = await file.read()
                    contracts, _ = await parser.parse_with_segments(pdf_bytes)
                    
                    if contracts:
                        task_statuses[task_id]["successful"] += 1
                        task_statuses[task_id]["results"].append({
                            "filename": file.filename,
                            "status": "success",
                            "contract_number": contracts[0].contract_number,
                            "vehicles_count": len(contracts[0].vehicles)
                        })
                    else:
                        task_statuses[task_id]["failed"] += 1
                        task_statuses[task_id]["results"].append({
                            "filename": file.filename,
                            "status": "failed",
                            "error": "Не удалось распарсить документ"
                        })
                    
                except Exception as e:
                    task_statuses[task_id]["failed"] += 1
                    task_statuses[task_id]["results"].append({
                        "filename": file.filename,
                        "status": "error",
                        "error": str(e)
                    })
                
                task_statuses[task_id]["processed"] = i
                
            task_statuses[task_id]["status"] = "completed"
            
        except Exception as e:
            task_statuses[task_id]["status"] = "error"
            task_statuses[task_id]["error"] = str(e)
            
        finally:
            # Закрываем ресурсы
            if parser:
                await close_osgop_parser_resources(parser)
            if element_client:
                await element_client.close()
    
    # Запускаем задачу в фоне
    background_tasks.add_task(process_batch_task, task_id, files)
    
    return {
        "task_id": task_id,
        "status": "started",
        "message": f"Обработка {len(files)} файлов запущена в фоне",
        "check_status_endpoint": f"/api/v1/parser/tasks/{task_id}"
    }


@router.get("/parser/tasks/{task_id}")
async def get_task_status(task_id: str):
    """Получить статус фоновой задачи"""
    # В реальном приложении получайте из Redis/БД
    task_statuses = {}  # Это временное хранилище
    
    if task_id not in task_statuses:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    
    return task_statuses[task_id]
