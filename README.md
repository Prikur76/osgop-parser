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
