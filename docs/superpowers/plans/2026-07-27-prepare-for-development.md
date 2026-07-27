# Подготовка osgop-parser к доработке — Implementation Plan

> **Для agentic workers:** Использовать superpowers:subagent-driven-development или inline execution task-by-task.

**Цель:** Привести проект osgop-parser в состояние, удобное для входа новых разработчиков: документация, чистый код, надёжный технический baseline.

**Архитектура:** FastAPI-монолит, слои api → services → models. После рефакторинга parser.py разбивается на parser.py (оркестрация), segment_detector.py, field_extractors.py.

**Технологии:** Python 3.12, FastAPI, PyMuPDF, pdfminer.six, pypdf, httpx, pandas, Pydantic

## Глобальные ограничения

- Python >= 3.12
- Все I/O операции через asyncio
- CPU-bound операции через asyncio.to_thread
- Тесты — pytest

---

## Карта файлов

### Создать
- `CLAUDE.md` — карта кодовой базы для AI-ассистентов
- `.dockerignore` — исключения для Docker
- `app/core/exceptions.py` — кастомные исключения
- `app/services/segment_detector.py` — детекция сегментов + нормализация
- `app/services/field_extractors.py` — извлечение полей из полиса/сведений
- `tests/test_parser_api.py` — тесты API-эндпоинтов
- `tests/test_pdf_reader.py` — тесты чтения PDF
- `tests/test_file_saver.py` — тесты FileSaver

### Изменить
- `README.md` — полная документация
- `Dockerfile` — multi-stage + healthcheck
- `app/core/config.py` — метод validate()
- `app/main.py` — exception handler
- `app/services/parser.py` — оставить только оркестрацию
- `tests/test_parser_dates.py` — обновить импорты
- `tests/test_parser_plate.py` — обновить импорты
- `tests/test_parser_inn.py` — обновить импорты
- `tests/test_parser_regression.py` — добавить edge-кейсы

---

### Task 1: README.md

**Файлы:**
- Modify: `README.md`

- [ ] **Шаг 1: Написать README.md**

```markdown
# OSGOP Parser

FastAPI-микросервис для автоматического извлечения структурированных данных из PDF-документов ОСГОП (Обязательное Страхование Гражданской Ответственности Перевозчика).

На вход — многостраничный PDF (полис + сведения о договоре), на выход — структурированные данные в JSON/CSV и нарезанные PDF-сегменты.

## Быстрый старт

### Локально

```bash
git clone <repo-url> osgop-parser
cd osgop-parser
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install .
uvicorn app.main:app --reload --port 8080
```

### Переменные окружения (.env)

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DEBUG` | `false` | Режим отладки |
| `ELEMENT_ENABLED` | `false` | Включить интеграцию с Element API |
| `ELEMENT_BASE_URL` | — | Базовый URL Element API |
| `ELEMENT_USERNAME` | — | Логин Element API |
| `ELEMENT_PASSWORD` | — | Пароль Element API |
| `ELEMENT_TIMEOUT` | `30.0` | Таймаут запросов (сек) |
| `ELEMENT_VERIFY_SSL` | `true` | Проверять SSL |

### Docker

```bash
docker build -t osgop-parser .
docker run -p 8080:8080 --env-file .env osgop-parser
```

## API-эндпоинты

Все эндпоинты принимают PDF-файл через `multipart/form-data` (поле `file`).

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/health` | Проверка работоспособности |
| `POST` | `/api/v1/parse/json` | Парсинг → JSON-файл для скачивания |
| `POST` | `/api/v1/parse/csv` | Парсинг → CSV (один ТС = одна строка) |
| `POST` | `/api/v1/parse/csv/save` | Парсинг → CSV-файл (через FileSaver) |
| `POST` | `/api/v1/parse/csv-only` | Парсинг → CSV (simple/detailed/both) |
| `POST` | `/api/v1/parse/all-formats` | Парсинг → ZIP (JSON + CSV + PDF) |
| `POST` | `/api/v1/parse/batch-csv` | Пакетный парсинг → ZIP с CSV |
| `POST` | `/api/v1/parse/async-batch` | Фоновый пакетный парсинг |
| `POST` | `/api/v1/parse/test` | Тестовый парсинг (отладка) |

### Примеры

#### Проверка здоровья

```bash
curl http://localhost:8080/health
# {"status": "ok"}
```

#### Парсинг в JSON

```bash
curl -X POST http://localhost:8080/api/v1/parse/json \
  -F "file=@references/24072026202858192_Полис ОСГОП Извозчик.pdf" \
  -o result.json
```

#### Парсинг в CSV

```bash
curl -X POST "http://localhost:8080/api/v1/parse/csv?include_car_info=true" \
  -F "file=@references/24072026202858192_Полис ОСГОП Извозчик.pdf" \
  -o result.csv
```

#### Все форматы (ZIP)

```bash
curl -X POST http://localhost:8080/api/v1/parse/all-formats \
  -F "file=@references/24072026202858192_Полис ОСГОП Извозчик.pdf" \
  -o result.zip
```

#### Тестовый парсинг

```bash
curl -X POST http://localhost:8080/api/v1/parse/test \
  -F "file=@references/24072026202858192_Полис ОСГОП Извозчик.pdf"
```

## Архитектура

```
app/
├── api/
│   ├── router.py              # Корневой роутер API
│   └── v1/endpoints/parser.py # 8 эндпоинтов парсинга
├── core/
│   ├── config.py              # Конфигурация (environs)
│   ├── exceptions.py           # Кастомные исключения
│   └── logging.py             # Настройка логирования
├── models/
│   └── contract.py            # Pydantic: OSGOPContract, VehicleInfo
├── services/
│   ├── parser.py              # Оркестрация парсинга
│   ├── segment_detector.py    # Детекция сегментов документа
│   ├── field_extractors.py    # Извлечение дат, ИНН, госномеров
│   ├── pdf_reader.py          # Чтение PDF (PyMuPDF/pdfminer/pypdf)
│   ├── plate_normalizer.py    # Нормализация госномеров
│   ├── file_saver.py          # Сохранение JSON/CSV/PDF
│   ├── element_api_client_async.py  # Клиент Element API
│   └── parser_factory.py      # Фабрика сборки парсера
└── main.py                    # Точка входа, create_app()
```

### Поток обработки PDF

```
PDF bytes
  → pdf_reader.extract_text_safe() (PyMuPDF → pdfminer → pypdf)
  → segment_detector: детекция POLIS / SVEDENIYA
  → field_extractors: извлечение номера, дат, страховщика, ИНН, премии, госномеров
  → element_api_client_async: обогащение VIN/маркой/моделью (опционально)
  → file_saver: запись JSON + CSV + нарезанных PDF
```

## Зависимости

| Библиотека | Зачем |
|---|---|
| **fastapi[standard]** | Веб-фреймворк |
| **pymupdf** | Основной движок чтения PDF (лучше сохраняет пробелы) |
| **pdfminer-six** | Fallback-движок PDF |
| **pypdf** | Последний fallback + нарезка страниц |
| **httpx** | Асинхронный HTTP-клиент для Element API |
| **pandas** | Генерация CSV |
| **environs** | Чтение .env |
| **pydantic** | Модели данных (встроен в FastAPI) |
| **pytest** | Тесты |

## Эталонные PDF

В `references/` лежат два реальных документа ОСГОП для тестирования и отладки:

- `24072026202858192_Полис ОСГОП Извозчик.pdf` — полный полис
- `24072026202749658_Сведения ОСГОП Извозчик.pdf` — сведения о договоре
```

- [ ] **Шаг 2: Commit**

```bash
git add README.md
git commit -m "docs: add comprehensive README"
```

---

### Task 2: CLAUDE.md

**Файлы:**
- Create: `CLAUDE.md`

- [ ] **Шаг 1: Написать CLAUDE.md**

```markdown
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
| `app/services/field_extractors.py` | Static-методы: даты, ИНН, госномера, страховая премия |
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
pytest tests/ -v
```
```

- [ ] **Шаг 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add CLAUDE.md codebase map"
```

---

### Task 3: Docker

**Файлы:**
- Modify: `Dockerfile`
- Create: `.dockerignore`

- [ ] **Шаг 1: Обновить Dockerfile**

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

- [ ] **Шаг 2: Создать .dockerignore**

```
__pycache__
.venv
.git
tests
output
docs
*.pyc
.env
.pytest_cache
references
```

- [ ] **Шаг 3: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "feat: add multi-stage Dockerfile with healthcheck and .dockerignore"
```

---

### Task 4: Валидация конфига

**Файлы:**
- Modify: `app/core/config.py`

- [ ] **Шаг 1: Добавить метод validate() в класс Config**

```python
# Добавить в конец класса Config:
    def validate(self) -> None:
        """Проверяет, что при включённом Element заданы все креды."""
        if not self.ELEMENT_ENABLED:
            return
        missing = []
        if not self.ELEMENT_BASE_URL:
            missing.append("ELEMENT_BASE_URL")
        if not self.ELEMENT_USERNAME:
            missing.append("ELEMENT_USERNAME")
        if not self.ELEMENT_PASSWORD:
            missing.append("ELEMENT_PASSWORD")
        if missing:
            raise ValueError(
                f"ELEMENT_ENABLED=True, но не заданы переменные: {', '.join(missing)}"
            )
```

- [ ] **Шаг 2: Вызвать validate() в create_app()**

В `app/main.py`, добавить вызов перед `setup_logging()`:

```python
def create_app() -> FastAPI:
    app = FastAPI(
        title="OSGOP Document Parser",
        version="1.0.0",
        description="Service for extracting structured data from OSGOP PDF documents"
    )

    config.validate()   # <-- добавить
    setup_logging()
    # ... остальное без изменений
```

Не забыть импорт: `from app.core.config import config` уже есть? Проверим — нет, нужно добавить:

```python
from app.core.config import config
```

- [ ] **Шаг 3: Commit**

```bash
git add app/core/config.py app/main.py
git commit -m "feat: add config validation for Element API credentials"
```

---

### Task 5: Structured Error Handling

**Файлы:**
- Create: `app/core/exceptions.py`
- Modify: `app/main.py`

- [ ] **Шаг 1: Создать app/core/exceptions.py**

```python
"""Кастомные исключения приложения."""


class AppError(Exception):
    """Базовое исключение приложения. Все наследники несут HTTP status_code."""
    status_code: int = 500


class PDFParseError(AppError):
    """Ошибка разбора PDF — документ повреждён или нечитаем."""
    status_code = 422


class NoSegmentsFoundError(AppError):
    """В документе не найдены сегменты (ни POLIS, ни SVEDENIYA)."""
    status_code = 422


class ElementAPIError(AppError):
    """Ошибка взаимодействия с Element API."""
    status_code = 502
```

- [ ] **Шаг 2: Добавить exception handler в app/main.py**

В функцию `create_app()`, после создания `app`, добавить:

```python
from fastapi.responses import JSONResponse
from app.core.exceptions import AppError

# ... внутри create_app(), после app = FastAPI(...):

    @app.exception_handler(AppError)
    async def app_error_handler(request, exc: AppError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": str(exc), "type": type(exc).__name__}
        )
```

- [ ] **Шаг 3: Commit**

```bash
git add app/core/exceptions.py app/main.py
git commit -m "feat: add structured error handling with custom exceptions"
```

---

### Task 6: Расслоение parser.py

**Файлы:**
- Create: `app/services/segment_detector.py`
- Create: `app/services/field_extractors.py`
- Modify: `app/services/parser.py`
- Modify: `tests/test_parser_dates.py`
- Modify: `tests/test_parser_plate.py`
- Modify: `tests/test_parser_inn.py`
- Modify: `tests/test_parser_regression.py`

**Интерфейсы:**
- `segment_detector.py` производит: `detect_segments(pages: List[str]) -> List[Tuple[int, int, str]]`, `normalize_page_text(text: str) -> str`, `restore_spaces(text: str) -> str`
- `field_extractors.py` производит: `parse_polis_header(text: str) -> Dict[str, Any]`, `extract_dates_from_polis(text: str) -> Dict[str, Optional[str]]`, `parse_svedeniya(text: str) -> Optional[Dict[str, Any]]`, `extract_contract_date_from_svedeniya(text: str) -> Optional[str]`, `normalize_date(day: str, month: str, year: str) -> Optional[str]`
- `parser.py` потребляет segment_detector и field_extractors, производит `OSGOPParser` с методом `parse_with_segments(pdf_bytes: bytes) -> Tuple[List[OSGOPContract], List[Tuple[int, int]]]`

- [ ] **Шаг 1: Создать segment_detector.py**

```python
"""Детекция сегментов документа (POLIS / SVEDENIYA) и нормализация текста."""

import re
import logging
from typing import List, Tuple

log = logging.getLogger(__name__)


def detect_segments(pages: List[str]) -> List[Tuple[int, int, str]]:
    """Обнаружение сегментов документа: полис и сведения."""
    segments = []
    i = 0
    total_pages = len(pages)

    while i < total_pages:
        page_text = pages[i].upper()

        # Поиск начала ПОЛИСА
        if re.search(r'ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ\s+ГРАЖДАНСКОЙ\s+ОТВЕТСТВЕННОСТИ\s+ПЕРЕВОЗЧИКА', page_text):
            start = i
            i += 1
            while i < total_pages:
                next_page = pages[i].upper()
                if re.search(r'СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ', next_page) or \
                   re.search(r'ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ', next_page):
                    break
                i += 1
            segments.append((start, i, "POLIS"))
            continue

        # Поиск "СВЕДЕНИЯ О ДОГОВОРЕ"
        if re.search(r'СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ\s+ГРАЖДАНСКОЙ\s+ОТВЕТСТВЕННОСТИ\s+ПЕРЕВОЗЧИКА', page_text):
            start = i
            i += 1
            while i < total_pages:
                next_page = pages[i].upper()
                if re.search(r'СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ', next_page) or \
                   re.search(r'ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ', next_page):
                    break
                i += 1
            segments.append((start, i, "SVEDENIYA"))
            continue

        i += 1

    return segments


def normalize_page_text(text: str) -> str:
    """Нормализация текста страницы."""
    if not text:
        return ""

    text = restore_spaces(text)
    text = text.replace("\xa0", " ").replace("\t", " ")
    text = re.sub(r'\s+', ' ', text)

    return text.strip()


def restore_spaces(text: str) -> str:
    """Восстановление пробелов в слипшемся русском тексте."""
    if not text:
        return text

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

    text = re.sub(r'([а-яё])([А-ЯЁ])', r'\1 \2', text)
    text = re.sub(r'(?<!\d)(\d{3,})([А-ЯЁ])', r'\1 \2', text)
    text = re.sub(r'([А-ЯЁ])(\d{3,})(?!\d)', r'\1 \2', text)

    return text
```

- [ ] **Шаг 2: Создать field_extractors.py**

```python
"""Извлечение полей: даты, ИНН, госномера, страховая премия."""

import re
import logging
from datetime import datetime
from typing import Optional, Dict, Any

from app.services.plate_normalizer import to_cyr_full, normalize_plate

log = logging.getLogger(__name__)


def parse_polis_header(text: str) -> Dict[str, Any]:
    """Парсинг заголовка полиса."""
    result = {}

    log.info("=== НАЧАЛО ПАРСИНГА ПОЛИСА ===")

    # 1. НОМЕР ПОЛИСА
    rosx_match = re.search(r'ROSX\d{8,20}', text, re.IGNORECASE)
    result["contract_number"] = rosx_match.group(0).upper() if rosx_match else None
    log.info(f"Номер полиса: {result['contract_number']}")

    # 2. ДАТЫ
    result.update(extract_dates_from_polis(text))

    # 3. СТРАХОВЩИК
    insurer_section_search = re.search(
        r'Страховщик[:\s]*(.*?)(?=\s*(?:Страхователь|Итого|Премия|Срок|ИНН|$))',
        text, re.IGNORECASE | re.DOTALL
    )

    if insurer_section_search:
        insurer_text = insurer_section_search.group(1).strip()
        log.debug(f"Текст страховщика (сырой): {insurer_text[:200]}")

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
    insurer_inn_match = re.search(r'Страховщик.*?ИНН[:\s/]*(\d{10,12})', text, re.IGNORECASE | re.DOTALL)
    if insurer_inn_match:
        result["insurer_inn"] = insurer_inn_match.group(1)
        log.info(f"ИНН страховщика: {result['insurer_inn']}")
    else:
        if insurer_section_search:
            inn_in_text = re.search(r'ИНН[:\s/]*(\d{10,12})', insurer_section_search.group(0), re.IGNORECASE)
            if inn_in_text:
                result["insurer_inn"] = inn_in_text.group(1)

    # 4. СТРАХОВАТЕЛЬ
    insured_section_search = re.search(
        r'Страхователь[:\s]*(.*?)(?=\s*(?:Итого|Премия|Срок|Страховая|ИНН|$))',
        text, re.IGNORECASE | re.DOTALL
    )

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
    insured_inn_match = re.search(r'Страхователь.*?ИНН[:\s/]*(\d{10,12})', text, re.IGNORECASE | re.DOTALL)
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


def extract_dates_from_polis(text: str) -> Dict[str, Optional[str]]:
    """Извлечение всех дат из текста полиса."""
    result = {
        "contract_date": None,
        "period_from": None,
        "period_to": None
    }

    log.info(f"Поиск дат в тексте (первые 1500 символов): {text[:1500]}")

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

            result["period_from"] = normalize_date(from_day, from_month, from_year)
            result["period_to"] = normalize_date(to_day, to_month, to_year)

            if result["period_from"] and result["period_to"]:
                log.info(f"Найден период: {result['period_from']} - {result['period_to']}")
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
            date_str = normalize_date(day, month, year)
            if date_str:
                result["contract_date"] = date_str
                log.info(f"Найдена дата договора: {result['contract_date']}")
                break

    # 3. Если не нашли — ищем все даты
    if not all([result["contract_date"], result["period_from"], result["period_to"]]):
        all_dates = re.findall(r'(\d{1,2})\s+([а-яё]+)\s+(\d{4})', normalized_text, re.IGNORECASE)
        dates_normalized = [normalize_date(d, m, y) for d, m, y in all_dates]
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


def parse_svedeniya(text: str) -> Optional[Dict[str, Any]]:
    """Парсинг раздела СВЕДЕНИЙ для одного ТС."""
    result = {}

    text_no_spaces = re.sub(r'\s+', '', text.upper())

    plate_patterns = [
        r'[АВЕКМНОРСТУХ]\d{3}[АВЕКМНОРСТУХ]{2}\d{2,3}',
        r'[АВЕКМНОРСТУХ]{2}\d{5}',
    ]

    plate_match = None
    for pattern in plate_patterns:
        match = re.search(pattern, text_no_spaces)
        if match:
            plate_match = match.group(0)
            log.debug(f"Найден госномер (без пробелов): {plate_match}")
            break

    if not plate_match:
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

    try:
        plate_cyr = to_cyr_full(plate_match)
        plate_lat = normalize_plate(plate_cyr)

        if not plate_cyr or not plate_lat:
            log.error(f"Ошибка нормализации номера: {plate_match}")
            return None

        result["plate_cyr"] = plate_cyr
        result["plate_lat"] = plate_lat

        date_match = re.search(
            r'Дата\s+заключения\s+договора[:\s]*(\d{1,2})\s+([а-яё]+)\s+(\d{4})',
            text, re.IGNORECASE
        )
        if date_match:
            day, month, year = date_match.group(1), date_match.group(2), date_match.group(3)
            result["contract_date"] = normalize_date(day, month, year)

        log.debug(f"Успешно извлечен ТС: {plate_cyr} -> {plate_lat}")
        return result

    except Exception as e:
        log.error(f"Ошибка при обработке госномера {plate_match}: {e}")
        return None


def extract_contract_date_from_svedeniya(text: str) -> Optional[str]:
    """Извлечение даты заключения договора из первого СВЕДЕНИЙ."""
    date_match = re.search(
        r'Дата\s+заключения\s+договора[:\s]*(\d{1,2})\s+([а-яё]+)\s+(\d{4})',
        text, re.IGNORECASE
    )
    if date_match:
        day, month, year = date_match.group(1), date_match.group(2), date_match.group(3)
        return normalize_date(day, month, year)
    return None


def normalize_date(day: str, month: str, year: str) -> Optional[str]:
    """Нормализация даты в формат YYYY-MM-DD."""
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
    month_num = months_full.get(month_lower)

    if not month_num and len(month_lower) >= 3:
        month_key = month_lower[:3]
        month_num = months_short.get(month_key)

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

        if day_int < 1 or day_int > 31:
            log.warning(f"Некорректный день: {day}")
            return None

        if year_int < 2000 or year_int > 2100:
            log.warning(f"Некорректный год: {year}")
            return None

        try:
            datetime(year_int, int(month_num), day_int)
        except ValueError as e:
            log.warning(f"Некорректная дата: {day_int}.{month_num}.{year_int}: {e}")
            return None

        return f"{year_int:04d}-{month_num}-{day_int:02d}"

    except ValueError as e:
        log.warning(f"Ошибка преобразования даты: день='{day}', месяц='{month}', год='{year}': {e}")
        return None
```

- [ ] **Шаг 3: Обновить parser.py — оставить только оркестрацию**

```python
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
                log.error("Не найден полис в документе")
                return [], []

            # 3. Парсим полис
            polis_text = "\n".join(normalized_pages[polis_segment[0]:polis_segment[1]])
            header_data = parse_polis_header(polis_text)

            # 4. Парсим сведения и извлекаем госномера
            vehicles_data = []

            if svedeniya_segments:
                first_sved_text = "\n".join(normalized_pages[svedeniya_segments[0][0]:svedeniya_segments[0][1]])
                sved_date = extract_contract_date_from_svedeniya(first_sved_text)
                if sved_date:
                    self.contract_date_from_svedeniya = sved_date
                    log.info(f"Дата из сведений: {self.contract_date_from_svedeniya}")

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

            # 8. Формируем сегменты для сохранения
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
        return await asyncio.to_thread(extract_text_safe, pdf_bytes)

    # ====================== РАБОТА С ELEMENT API =========================

    async def _get_vehicles_info_from_element(self, vehicles_data: List[dict]) -> List[VehicleInfo]:
        """Асинхронное получение информации о ТС из Element API."""
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

        async def process_vehicle(data: dict) -> Optional[VehicleInfo]:
            plate_lat = data.get("plate_lat")
            plate_cyr = data.get("plate_cyr")

            if not plate_lat or not plate_cyr:
                log.warning(f"Пропуск ТС: нет plate_lat или plate_cyr в данных {data}")
                return None

            try:
                car_data = await self.element_api_client.get_car_by_plate(plate_cyr)

                vin = None
                car_info = None

                if car_data:
                    vin = car_data.get("VIN") or car_data.get("vin")
                    if vin and isinstance(vin, str):
                        vin = vin.strip()
                        if vin in ("", "0", "Нет данных"):
                            vin = None

                    car_info = {
                        "model": car_data.get("Model") or car_data.get("model") or "",
                        "year": car_data.get("YearCar") or car_data.get("year") or "",
                        "code": car_data.get("Code") or car_data.get("code") or "",
                    }

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
                vehicle = VehicleInfo(
                    vehicle_plate_cyr=plate_cyr,
                    vehicle_plate_lat=plate_lat,
                    vin=None,
                    car_info=None
                )
                log.info(f"Добавлен ТС (без данных из Element): {plate_cyr}")
                return vehicle

        tasks = [process_vehicle(data) for data in vehicles_data]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                log.error(f"Исключение при обработке ТС в Element: {result}")
                continue
            if result:
                vehicles.append(result)

        log.info(f"Всего обработано ТС через Element API: {len(vehicles)}")
        return vehicles
```

- [ ] **Шаг 4: Обновить импорты в тестах**

`tests/test_parser_dates.py`:
```python
from app.services.field_extractors import extract_dates_from_polis


TEXT = """
Срок страхования: с «31» октября 2025 г. по «30» октября 2026 г.

Дата заключения договора: 28 октября 2025 г.
Срок действия договора: с 31 октября 2025 г. по 30 октября 2026 г.
"""

def test_date_extraction():
    data = extract_dates_from_polis(TEXT)

    assert data["contract_date"] == "2025-10-28"
    assert data["period_from"] == "2025-10-31"
    assert data["period_to"] == "2026-10-30"
```

`tests/test_parser_plate.py`:
```python
from app.services.field_extractors import parse_svedeniya


TEXT = """
ТС:
Гос. номер: А 225  РН  797
"""


def test_plate_extraction():
    res = parse_svedeniya(TEXT)

    assert res is not None
    assert res["plate_cyr"] == "А225РН797"
    assert res["plate_lat"] == "A225PH797"
```

`tests/test_parser_inn.py`:
```python
from app.services.field_extractors import parse_polis_header


TEXT = """
Страховщик:
АО "Зетта Страхование"
ИНН 7702073683 КПП 770201001

Страхователь:
ООО "КАРТЕЛЬ"
ИНН 7721751172 КПП 772101001
"""


def test_inn_extraction():
    data = parse_polis_header(TEXT)

    assert data["insurer_inn"] == "7702073683"
    assert data["insured_inn"] == "7721751172"
```

`tests/test_parser_regression.py` — заменить импорт `OSGOPParser` на `parse_polis_header`:
```python
"""Regression-тесты: нормализация госномера и извлечение ИНН."""
from app.services.field_extractors import parse_polis_header
from app.services.plate_normalizer import to_cyr_full, normalize_plate


# --------------------------- Госномера ---------------------------

def test_plate_normalization_latin_to_cyrillic():
    assert to_cyr_full("A225PH797") == "А225РН797"


def test_plate_normalization_cyrillic_to_latin():
    assert normalize_plate("А225РН797") == "A225PH797"


def test_plate_normalization_real_plate():
    assert to_cyr_full("Е866НР977") == "Е866НР977"
    assert normalize_plate("Е866НР977") == "E866HP977"


def test_plate_round_trip():
    cyr = "А225РН797"
    assert to_cyr_full(normalize_plate(cyr)) == cyr


def test_empty_plate():
    assert to_cyr_full("") == ""
    assert normalize_plate("") == ""


def test_plate_with_special_chars():
    assert to_cyr_full("А 225-РН 797") == "А225РН797"


# --------------------------- ИНН ---------------------------

INN_TEXT = """
Страховщик:
АО «Страховая компания ИнноГарант»
ИНН 7710001234 КПП 771001001

Страхователь:
ООО «ТрансАвто-Инвест»
ИНН 9718074750 КПП 771801001
"""


def test_inn_extraction_with_inn_letters_in_name():
    """Имя страховщика содержит буквы И/Н — первичный regex не должен ломаться."""
    data = parse_polis_header(INN_TEXT)

    assert data["insurer_inn"] == "7710001234"
    assert data["insured_inn"] == "9718074750"
```

- [ ] **Шаг 5: Запустить существующие тесты — убедиться что не сломались**

```bash
pytest tests/test_parser_dates.py tests/test_parser_plate.py tests/test_parser_inn.py tests/test_parser_regression.py -v
```
Ожидаемый результат: все тесты PASS.

- [ ] **Шаг 6: Commit**

```bash
git add app/services/segment_detector.py app/services/field_extractors.py app/services/parser.py
git add tests/test_parser_dates.py tests/test_parser_plate.py tests/test_parser_inn.py tests/test_parser_regression.py
git commit -m "refactor: split parser.py into parser, segment_detector, field_extractors"
```

---

### Task 7: Тесты — pdf_reader

**Файлы:**
- Create: `tests/test_pdf_reader.py`

- [ ] **Шаг 1: Написать тесты**

```python
"""Тесты чтения PDF."""
import pytest
from pathlib import Path

from app.services.pdf_reader import extract_text_safe, extract_pages_as_pdf, PDFReadError


REFERENCES_DIR = Path(__file__).parent.parent / "references"
POLIS_PDF = REFERENCES_DIR / "24072026202858192_Полис ОСГОП Извозчик.pdf"
SVED_PDF = REFERENCES_DIR / "24072026202749658_Сведения ОСГОП Извозчик.pdf"


def _read_pdf(path: Path) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def test_extract_text_from_polis():
    """extract_text_safe должен вернуть непустой список страниц для полиса."""
    pdf_bytes = _read_pdf(POLIS_PDF)
    pages = extract_text_safe(pdf_bytes)
    assert isinstance(pages, list)
    assert len(pages) > 0
    assert any("ПОЛИС" in page.upper() for page in pages)


def test_extract_text_from_svedeniya():
    """extract_text_safe должен вернуть непустой список страниц для сведений."""
    pdf_bytes = _read_pdf(SVED_PDF)
    pages = extract_text_safe(pdf_bytes)
    assert isinstance(pages, list)
    assert len(pages) > 0
    assert any("СВЕДЕНИЯ" in page.upper() for page in pages)


def test_extract_pages_as_pdf():
    """Вырезание подмножества страниц должно вернуть валидный PDF."""
    pdf_bytes = _read_pdf(POLIS_PDF)
    result = extract_pages_as_pdf(pdf_bytes, [0])
    assert isinstance(result, bytes)
    assert len(result) > 0
    assert result[:5] == b"%PDF-"


def test_corrupted_pdf_raises():
    """Битый PDF должен вызывать PDFReadError."""
    with pytest.raises(PDFReadError):
        extract_text_safe(b"not a pdf at all")
```

- [ ] **Шаг 2: Запустить тесты**

```bash
pytest tests/test_pdf_reader.py -v
```
Ожидаемый результат: 4 теста PASS.

- [ ] **Шаг 3: Commit**

```bash
git add tests/test_pdf_reader.py
git commit -m "test: add PDF reader tests"
```

---

### Task 8: Тесты — file_saver

**Файлы:**
- Create: `tests/test_file_saver.py`

- [ ] **Шаг 1: Написать тесты**

```python
"""Тесты FileSaver."""
import tempfile
import json
from pathlib import Path

import pytest

from app.models.contract import OSGOPContract, VehicleInfo
from app.services.file_saver import FileSaver


@pytest.fixture
def sample_contract():
    return OSGOPContract(
        contract_number="ROSX1234567890",
        contract_date="2025-10-28",
        period_from="2025-10-31",
        period_to="2026-10-30",
        insurer="АО «Тест-Страхование»",
        insurer_inn="7702073683",
        insured="ООО «Тест-Перевозчик»",
        insured_inn="7721751172",
        bonus=15000.50,
        vehicles=[
            VehicleInfo(
                vehicle_plate_cyr="А225РН797",
                vehicle_plate_lat="A225PH797",
                vin="XTA1234567890",
                car_info={"model": "ГАЗель NEXT", "year": "2023"}
            )
        ]
    )


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmp:
        yield tmp


def test_save_csv_structure(temp_dir, sample_contract):
    """_save_csv_async должен создать CSV с правильными заголовками."""
    import asyncio
    saver = FileSaver(base_dir=temp_dir)

    async def run():
        return await saver._save_csv_async(sample_contract, include_car_info=False)

    csv_path = asyncio.run(run())
    assert Path(csv_path).exists()

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        content = f.read()
    assert "Номер договора" in content
    assert "ROSX1234567890" in content
    assert "А225РН797" in content


def test_save_json(temp_dir, sample_contract):
    """_save_json должен создать JSON-файл с корректной структурой."""
    import asyncio
    saver = FileSaver(base_dir=temp_dir)

    async def run():
        return await saver._save_json(sample_contract)

    json_path = asyncio.run(run())
    assert Path(json_path).exists()

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["contract"]["contract_number"] == "ROSX1234567890"


def test_sanitize_filename():
    """_sanitize_filename должен очищать спецсимволы."""
    saver = FileSaver()
    assert saver._sanitize_filename('test:file<name>.pdf') == 'test_file_name_.pdf'
    assert saver._sanitize_filename('') == 'unknown'
    assert saver._sanitize_filename('a' * 200) == 'a' * 100


def test_format_date_for_filename():
    """_format_date_for_filename должен форматировать даты."""
    saver = FileSaver()
    assert saver._format_date_for_filename("2025-10-28") == "251028"
    assert saver._format_date_for_filename(None) == "000000"
    assert saver._format_date_for_filename("not-a-date") == "000000"
```

- [ ] **Шаг 2: Запустить тесты**

```bash
pytest tests/test_file_saver.py -v
```

**Примечание:** если `save_all` асинхронный и не может быть вызван синхронно из `run()`, тесты нужно адаптировать под `pytest-asyncio`. На этом этапе пишем тесты, запускаем, чиним если нужно.

- [ ] **Шаг 3: Commit**

```bash
git add tests/test_file_saver.py
git commit -m "test: add FileSaver tests"
```

---

### Task 9: Тесты — API

**Файлы:**
- Create: `tests/test_parser_api.py`

- [ ] **Шаг 1: Написать тесты**

```python
"""Тесты API-эндпоинтов."""
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)
REFERENCES_DIR = Path(__file__).parent.parent / "references"
POLIS_PDF = REFERENCES_DIR / "24072026202858192_Полис ОСГОП Извозчик.pdf"


def _read_pdf(path: Path) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def test_health_endpoint():
    """GET /health должен вернуть 200 и status=ok."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_parse_test_endpoint():
    """POST /api/v1/parse/test с эталонным PDF должен вернуть success=True."""
    pdf_bytes = _read_pdf(POLIS_PDF)
    response = client.post(
        "/api/v1/parse/test",
        files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
    )
    assert response.status_code == 200
    data = response.json()
    assert data.get("success") is True
    assert "contract" in data


def test_parse_json_endpoint():
    """POST /api/v1/parse/json должен вернуть JSON с Content-Disposition."""
    pdf_bytes = _read_pdf(POLIS_PDF)
    response = client.post(
        "/api/v1/parse/json",
        files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
    )
    assert response.status_code == 200
    assert "application/json" in response.headers.get("content-type", "")
    assert "attachment" in response.headers.get("content-disposition", "")


def test_parse_invalid_file():
    """POST /api/v1/parse/json с не-PDF должен вернуть ошибку."""
    response = client.post(
        "/api/v1/parse/json",
        files={"file": ("test.txt", b"not a pdf", "text/plain")},
    )
    assert response.status_code in (400, 422)
```

- [ ] **Шаг 2: Запустить тесты**

```bash
pytest tests/test_parser_api.py -v
```
Ожидаемый результат: 4 теста PASS.

- [ ] **Шаг 3: Commit**

```bash
git add tests/test_parser_api.py
git commit -m "test: add API endpoint tests"
```

---

### Task 10: Финальная проверка

- [ ] **Шаг 1: Прогнать все тесты**

```bash
pytest tests/ -v
```

Ожидаемый результат: все тесты (существующие + новые) PASS.

- [ ] **Шаг 2: Проверить что приложение стартует**

```bash
python -c "from app.main import app; print('App created OK')"
```
Ожидаемый результат: `App created OK`, без ошибок.

- [ ] **Шаг 3: Commit (если были правки)**

```bash
git add -A
git commit -m "chore: final cleanup after preparation"
```
