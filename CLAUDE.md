# CLAUDE.md — osgop-parser

## Что это за проект

FastAPI-микросервис для парсинга PDF-документов ОСГОП (страхование гражданской ответственности перевозчика). Извлекает: номер полиса, даты, страховщика/страхователя, ИНН, страховую премию, госномера ТС. Опционально обогащает данные через Element API (бывш. 1С Элемент).

## Карта файлов

| Файл | Назначение |
|---|---|
| `app/main.py` | Точка входа: `create_app()`, health-эндпоинт, exception handler |
| `app/core/config.py` | Конфигурация через environs: PDF-лимиты, Element API |
| `app/core/exceptions.py` | Кастомные исключения (PDFParseError, NoSegmentsFoundError, ElementAPIError) |
| `app/core/logging.py` | Базовая настройка логирования |
| `app/models/contract.py` | Pydantic-модели: `OSGOPContract`, `VehicleInfo` |
| `app/services/parser.py` | Оркестрация: `OSGOPParser.parse_with_segments()` |
| `app/services/segment_detector.py` | Детекция POLIS/SVEDENIYA сегментов, нормализация текста |
| `app/services/field_extractors.py` | Module-level функции: даты, ИНН, госномера, страховая премия |
| `app/services/pdf_reader.py` | Чтение PDF: PyMuPDF → pdfminer → pypdf (каскадный fallback) |
| `app/services/plate_normalizer.py` | Конвертация госномеров кириллица ↔ латиница |
| `app/services/file_saver.py` | `FileSaver`: сохранение JSON/CSV/PDF в `output/` |
| `app/services/element_api_client_async.py` | `ElementApiClientAsync`: поиск ТС по госномеру, загрузка файлов |
| `app/services/parser_factory.py` | `get_osgop_parser()` / `close_osgop_parser_resources()` |
| `app/api/router.py` | Корневой роутер API |
| `app/api/v1/endpoints/parser.py` | 8 эндпоинтов парсинга |
| `references/` | Эталонные PDF для тестирования и отладки |

## Соглашения

### Async/await
- Все эндпоинты и парсер — асинхронные
- CPU-bound операции (чтение PDF, запись CSV) — через `asyncio.to_thread()`
- HTTP-клиент — `httpx.AsyncClient`

### Модели
- Pydantic v2 с `model_dump()` (не `.dict()`)
- `ConfigDict` вместо class-based `Config`

### Логирование
- `log = logging.getLogger(__name__)` в каждом модуле
- Уровень: INFO в продакшне, DEBUG для отладки

### Обработка ошибок
- Кастомные исключения в `app.core.exceptions.py`
- Каждое несёт `status_code` (422 для проблем с PDF, 502 для API)
- Глобальный handler в `main.py` маппит `AppError` → HTTP

## Ключевые паттерны

### Каскадный fallback (pdf_reader.py)
Три движка, пробуются по очереди: PyMuPDF → pdfminer.six → pypdf. Если все падают — `PDFReadError`.

### Сегментная детекция (segment_detector.py)
Многостраничный PDF содержит перемежающиеся секции POLIS и SVEDENIYA. Детектор идёт по страницам и находит границы каждого сегмента по ключевым фразам.

### Фабрика парсера (parser_factory.py)
`get_osgop_parser()` создаёт `OSGOPParser` с опциональным Element-клиентом. Если `ELEMENT_ENABLED` и клиент создан — обогащает ТС данными из API.

### Нормализация госномеров (plate_normalizer.py)
Двухшаговая: `to_cyr_full()` (латиница → кириллица), затем `normalize_plate()` (кириллица → латиница). Поддерживает форматы А000АА000 и АА00000.

## Известные ограничения

- **Фоновая обработка** (`/parse/async-batch`): статусы задач хранятся в in-memory словаре, теряются при перезапуске. Для продакшна нужен Redis.
- **Element upload**: `ElementApiClientAsync.add_file()` реализован, но не вызывается из эндпоинтов. Параметр `upload_to_element` в `/parse/all-formats` не используется.
- **Тестовое покрытие**: unit-тесты есть для парсера и нормализатора, но нет интеграционных тестов API и тестов FileSaver/pdf_reader.

## Запуск тестов

```bash
uv run pytest tests/ -v
```
