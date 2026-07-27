# CLAUDE.md — osgop-parser

## Что это за проект

FastAPI-микросервис для парсинга PDF-документов ОСГОП (страхование гражданской ответственности перевозчика). Извлекает: номер полиса, даты, страховщика/страхователя, ИНН, страховую премию, госномера ТС. Обогащает данные через Element API (бывш. 1С Элемент): VIN, СТС, марка, модель, год. Результаты кэшируются в SQLite.

## Карта файлов

| Файл | Назначение |
|---|---|
| `app/main.py` | Точка входа: `create_app()`, health-эндпоинт, exception handler |
| `app/core/config.py` | Конфигурация: PDF-лимиты, Element API, путь к кэшу (`PLATE_CACHE_PATH`) |
| `app/core/exceptions.py` | Кастомные исключения (PDFParseError, NoSegmentsFoundError, ElementAPIError) |
| `app/core/logging.py` | Базовая настройка логирования |
| `app/models/contract.py` | Pydantic-модели: `OSGOPContract`, `VehicleInfo` (vin, sts_series, sts_number, car_info) |
| `app/services/parser.py` | Оркестрация: `parse_with_segments()`, `parse_split_files()`. Семафор на 3 API-запроса |
| `app/services/parser_factory.py` | `get_osgop_parser()` — создаёт парсер с Element-клиентом и PlateCache |
| `app/services/segment_detector.py` | Детекция POLIS/SVEDENIYA сегментов, нормализация текста |
| `app/services/field_extractors.py` | Module-level функции: даты, ИНН, госномера, страховая премия |
| `app/services/pdf_reader.py` | Чтение PDF: PyMuPDF → pdfminer.six → pypdf (каскадный fallback) |
| `app/services/plate_normalizer.py` | Кириллица ↔ латиница, `latin_to_cyrillic_text()` для OCR с латиницей |
| `app/services/plate_cache.py` | SQLite-кэш: VIN, СТС, модель по госномеру (connection-per-call) |
| `app/services/file_saver.py` | `FileSaver`: `save_all()` и `save_split()`, CSV с колонкой СТС |
| `app/services/element_api_client_async.py` | `ElementApiClientAsync`: поиск ТС, выбор по `STSIssueDate`, retry |
| `app/api/router.py` | Корневой роутер API |
| `app/api/v1/endpoints/parser.py` | Комбинированные (`/parse/*`) и раздельные (`/parse/split/*`) эндпоинты |

## Соглашения

### Async/await
- Все эндпоинты и парсер — асинхронные
- CPU-bound операции — через `asyncio.to_thread()`
- HTTP-клиент — `httpx.AsyncClient`
- Запросы к Element API: семафор `asyncio.Semaphore(3)` — не более 3 одновременно

### Модели
- Pydantic v2 с `model_dump()` (не `.dict()`)
- `VehicleInfo`: `vehicle_plate_cyr`, `vehicle_plate_lat`, `vin`, `sts_series`, `sts_number`, `car_info`

### Логирование
- `log = logging.getLogger(__name__)` в каждом модуле

### Обработка ошибок
- Кастомные исключения в `app.core/exceptions.py`, каждое несёт `status_code`
- Глобальный handler в `main.py`
- `_bool()` в config.py: защита от невалидных/пустых значений в `.env`

## Ключевые паттерны

### Три формата входных данных
1. **Комбинированный** — один PDF с POLIS + SVEDENIYA (`parse_with_segments`)
2. **Раздельный** — два PDF: полис + сведения (`parse_split_files`)
3. **Сведенческий** — только SVEDENIYA без POLIS (fallback в `parse_with_segments`)

### Каскадный fallback (pdf_reader.py)
PyMuPDF → pdfminer.six → pypdf. Все падают → `PDFReadError`.

### Нормализация госномеров (plate_normalizer.py)
`latin_to_cyrillic_text()` → `to_cyr_full()` → `normalize_plate()`. Обрабатывает латинские lookalike-символы (T→Т, H→Н, C→С и т.д.) — критично для OCR где кириллица выдаётся латиницей.

### Кэш госномеров (plate_cache.py)
SQLite, WAL-режим, отдельное соединение на каждый вызов. Ключ — кириллический госномер. Таблица: plate_cyr, vin, sts_series, sts_number, model, year, code. При изменении схемы — ALTER TABLE миграция.

### Element API (element_api_client_async.py)
- Выбор ТС: по `STSIssueDate` (самое свежее СТС), fallback по `Code`
- Retry: до 2 повторов с задержкой 1с/2с
- Поля: VIN, STSSeries, STSNumber, Model, YearCar, Code, STSIssueDate

### Именование файлов (file_saver.py)
- Полис: `OSGOP_{contract_number}_{YYYYmmdd}.pdf`
- Сведения: `{VIN}_OSGOP_{contract_number}_{YYYYmmdd}.pdf` (если VIN нет → госномер)
- Дата из `period_from` (предпочтительно) или `contract_date`

## Известные ограничения

- **Фоновая обработка**: статусы в in-memory словаре, для продакшна нужен Redis
- **Element upload**: `add_file()` реализован, но не вызывается из эндпоинтов
- **Кэш**: при изменении схемы БД старый файл `plate_cache.db*` нужно удалить

## Запуск тестов

```bash
uv run pytest tests/ -v
```
