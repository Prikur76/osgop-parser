# Подготовка osgop-parser к доработке — Design Spec

**Дата:** 2026-07-27
**Статус:** утверждён
**Вариант:** 2 — Твёрдый фундамент

---

## Цель

Привести проект osgop-parser в состояние, удобное для входа новых разработчиков и безопасной доработки:
полная документация, надёжный технический baseline, чистый код.

---

## 1. Документация

### 1.1 README.md

Полноценный README со следующими разделами:

- **Что такое ОСГОП и что делает сервис** — одно предложение
- **Быстрый старт** — клонирование, установка (`pip install .`), `.env`, запуск (`uvicorn app.main:app`), Docker
- **API-эндпоинты** — таблица всех 8 эндпоинтов с примерами cURL и кратким описанием
- **Архитектура** — слои (api → services → models), поток обработки PDF (диаграмма текстом)
- **Зависимости** — Python 3.12, ключевые библиотеки (FastAPI, PyMuPDF, pdfminer, httpx, pandas)
- **Ссылки** — эталонные PDF в `references/`

### 1.2 CLAUDE.md

Карта кодовой базы для AI-ассистентов:

- Карта файлов (назначение каждого модуля)
- Соглашения: async/await, `asyncio.to_thread` для CPU-bound, Pydantic-модели, логирование
- Ключевые паттерны: каскадный fallback в pdf_reader, сегментная детекция, фабрика parser_factory
- Известные ограничения: in-memory task_statuses, неинтегрированный Element upload

---

## 2. Docker

### 2.1 Multi-stage Dockerfile

```dockerfile
# Build stage — установка зависимостей
FROM python:3.12-slim AS build
WORKDIR /app
COPY pyproject.toml ./
RUN pip install --no-cache-dir --user .

# Final stage
FROM python:3.12-slim
WORKDIR /app
COPY --from=build /root/.local /root/.local
COPY app ./app
ENV PATH=/root/.local/bin:$PATH
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "from urllib.request import urlopen; urlopen('http://localhost:8080/health')"
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

### 2.2 .dockerignore

Исключить: `__pycache__`, `.venv`, `.git`, `tests`, `output`, `docs`, `*.pyc`, `.env`, `.pytest_cache`.

---

## 3. Валидация конфига

В [app/core/config.py](app/core/config.py) добавить метод `validate()`:

- Если `ELEMENT_ENABLED=True`, но `ELEMENT_BASE_URL`/`ELEMENT_USERNAME`/`ELEMENT_PASSWORD` пусты → `ValueError`
- Вызывать `validate()` в `create_app()` перед инициализацией логгера

---

## 4. Structured Error Handling

### 4.1 Новый файл `app/core/exceptions.py`

```python
class AppError(Exception):
    """Базовое исключение приложения"""
    status_code: int = 500

class PDFParseError(AppError):
    status_code = 422

class NoSegmentsFoundError(AppError):
    status_code = 422

class ElementAPIError(AppError):
    status_code = 502
```

### 4.2 Глобальный handler в `app/main.py`

Регистрация exception handler'ов в `create_app()`:

```python
from app.core.exceptions import AppError

@app.exception_handler(AppError)
async def app_error_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": str(exc), "type": type(exc).__name__}
    )
```

---

## 5. Расслоение parser.py

[app/services/parser.py](app/services/parser.py) (659 строк) разбивается на три файла:

### 5.1 `parser.py` (~150 строк)
`OSGOPParser` — оркестрация:
- `parse_with_segments()`
- `_extract_text_async()`
- `_get_vehicles_info_from_element()`
- `process_vehicle()`

### 5.2 `segment_detector.py` (~80 строк)
- `_detect_segments()`
- `_normalize_page_text()`
- `_restore_spaces()`

### 5.3 `field_extractors.py` (~350 строк)
- `_parse_polis_header()`
- `_extract_dates_from_polis()`
- `_parse_svedeniya()`
- `_extract_contract_date_from_svedeniya()`
- `_normalize_date()`

Методы становятся статическими или module-level функциями, чтобы уменьшить связанность.
`OSGOPParser` импортирует и делегирует им.

Обновляются импорты в тестах:
- `test_parser_dates.py` → `from app.services.field_extractors import _extract_dates_from_polis`
- `test_parser_plate.py` → `from app.services.field_extractors import _parse_svedeniya`
- `test_parser_inn.py` → `from app.services.field_extractors import _parse_polis_header`
- `test_parser_regression.py` → аналогично

---

## 6. Тесты

### 6.1 Тест API — новый файл `tests/test_parser_api.py`

Использует `TestClient` из FastAPI:

| Тест | Что проверяет |
|---|---|
| `test_health_endpoint` | `/health` → 200, `{"status": "ok"}` |
| `test_parse_test_endpoint` | `/parse/test` с эталонным PDF из `references/` → 200, поле `success=True` |
| `test_parse_json_endpoint` | `/parse/json` → 200, Content-Disposition, корректный JSON |
| `test_parse_invalid_file` | Загрузка не-PDF файла → 400 или 422 |

### 6.2 Тест PDF reader — новый файл `tests/test_pdf_reader.py`

| Тест | Что проверяет |
|---|---|
| `test_extract_text_from_reference_polis` | `extract_text_safe` с эталонным «Полис» PDF — возвращает непустой список страниц |
| `test_extract_text_from_reference_svedeniya` | Аналогично для «Сведения» PDF |
| `test_extract_pages_as_pdf` | Вырезание подмножества страниц → валидный PDF |
| `test_corrupted_pdf_raises` | Битый PDF → `PDFReadError` |

### 6.3 Тест FileSaver — новый файл `tests/test_file_saver.py`

| Тест | Что проверяет |
|---|---|
| `test_save_all_creates_files` | `save_all` с эталонным PDF → JSON/CSV/PDF созданы |
| `test_csv_structure` | `_save_csv_async` → CSV парсится, заголовки корректны |
| `test_sanitize_filename` | `_sanitize_filename` — спецсимволы, пустая строка, длина > 100 |
| `test_format_date` | `_format_date_for_filename` — нормальная дата, None, невалидная строка |

### 6.4 Расширение regression-тестов

В [tests/test_parser_regression.py](tests/test_parser_regression.py) добавить:
- `test_empty_plate` — пустая строка → `""`
- `test_plate_with_special_chars` — номер с пробелами/тире
- `test_plate_mixed_cyr_lat` — смешанный ввод

---

## 7. Порядок выполнения

| Шаг | Что | Зависимости |
|---|---|---|
| 1 | README.md + CLAUDE.md | нет |
| 2 | Docker: healthcheck + .dockerignore | нет |
| 3 | Валидация конфига | нет |
| 4 | Structured errors (`exceptions.py` + handler) | нет |
| 5 | Расслоение parser.py | нет |
| 6 | Тесты (API, pdf_reader, file_saver, regression) | 5 |
| **7** | **Прогон всех тестов — проверка** | всё |

Шаги 1-5 можно делать параллельно. Шаг 6 зависит от 5 (импорты изменятся). Шаг 7 — финальный.
