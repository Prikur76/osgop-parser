# OSGOP Parser

FastAPI-микросервис для извлечения структурированных данных из PDF-документов ОСГОП (Обязательное Страхование Гражданской Ответственности Перевозчика).

На вход — PDF (полис + сведения), на выход — JSON/CSV с VIN, СТС, маркой/моделью и нарезанные PDF-сегменты. Обогащение данными — через Element API (бывш. 1С Элемент) с локальным SQLite-кэшем.

## Быстрый старт

```bash
git clone <repo-url> osgop-parser
cd osgop-parser
uv sync
uv run uvicorn app.main:app --reload --port 8080
```

### Docker

```bash
docker build -t osgop-parser .
docker run --rm -p 8080:8080 --env-file .env osgop-parser
```

### Переменные окружения (.env)

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DEBUG` | `false` | Режим отладки |
| `ELEMENT_ENABLED` | `false` | Включить обогащение через Element API |
| `ELEMENT_BASE_URL` | — | Базовый URL Element API |
| `ELEMENT_USERNAME` | — | Логин Element API |
| `ELEMENT_PASSWORD` | — | Пароль Element API |
| `ELEMENT_TIMEOUT` | `30.0` | Таймаут запросов (сек) |
| `ELEMENT_VERIFY_SSL` | `true` | Проверять SSL |
| `PLATE_CACHE_PATH` | `plate_cache.db` | Путь к SQLite-кэшу госномеров |

### Разработка

```bash
uv sync                        # установка зависимостей
uv run pytest tests/ -v        # запуск тестов
```

## Форматы документов

Парсер поддерживает три варианта входных данных:

| Формат | Эндпоинты | Описание |
|---|---|---|
| **Комбинированный** | `/parse/*` | Один PDF — полис и сведения вперемешку |
| **Раздельный** | `/parse/split/*` | Два PDF: полис + сведения отдельно |
| **Сведенческий** | `/parse/*` | Только сведения (без полиса) — fallback |

## API-эндпоинты

### Комбинированные (один файл)

| Метод | Путь | Описание |
|---|---|---|
| `GET` | `/health` | Проверка работоспособности |
| `POST` | `/api/v1/parse/json` | Парсинг → JSON |
| `POST` | `/api/v1/parse/csv` | Парсинг → CSV |
| `POST` | `/api/v1/parse/all-formats` | Парсинг → ZIP (JSON + CSV + PDF) |
| `POST` | `/api/v1/parse/batch-csv` | Пакетный парсинг → ZIP |
| `POST` | `/api/v1/parse/test` | Тестовый парсинг (отладка) |

### Раздельные (два файла)

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/api/v1/parse/split/json` | Полис + сведения → JSON |
| `POST` | `/api/v1/parse/split/all-formats` | Полис + сведения → ZIP |

### Примеры

```bash
# Комбинированный — JSON
curl -X POST http://localhost:8080/api/v1/parse/json \
  -F "file=@Полис.pdf" -o result.json

# Комбинированный — ZIP (все форматы)
curl -X POST http://localhost:8080/api/v1/parse/all-formats \
  -F "file=@Полис.pdf" -o result.zip

# Раздельный — JSON
curl -X POST http://localhost:8080/api/v1/parse/split/json \
  -F "polis_file=@Полис.pdf" \
  -F "svedeniya_file=@Сведения.pdf" -o result.json

# Раздельный — ZIP
curl -X POST "http://localhost:8080/api/v1/parse/split/all-formats?include_car_info=true" \
  -F "polis_file=@Полис.pdf" \
  -F "svedeniya_file=@Сведения.pdf" -o result.zip

# Здоровье
curl http://localhost:8080/health
# {"status": "ok"}
```

## Выходные данные

### JSON

```json
{
  "contract": {
    "contract_number": "ROSX22654944996000",
    "contract_date": "2026-07-24",
    "period_from": "2026-07-24",
    "period_to": "2027-07-23",
    "insurer": "Акционерное общество Зетта Страхование",
    "insurer_inn": "7702073683",
    "insured": "ООО ИЗВОЗЧИК",
    "insured_inn": "7727359934",
    "bonus": 54279.84,
    "vehicles": [{
      "vehicle_plate_cyr": "Е866НР977",
      "vehicle_plate_lat": "E866HP977",
      "vin": "MX1J7AGGXPK019168",
      "sts_series": "7729",
      "sts_number": "523038",
      "car_info": {"model": "JAC J7", "year": "2023", "code": "4305"}
    }]
  }
}
```

### CSV

Колонки: Номер договора, Дата заключения, Дата начала, Дата окончания, Страховщик, ИНН страховщика, Страхователь, ИНН страхователя, Страховая премия, Госномер (рус), Госномер (англ), VIN код, СТС.

СТС = серия + номер слитно (например `7729523038`).

### Имена PDF-файлов

- Полис: `OSGOP_{contract_number}_{YYYYmmdd}.pdf`
- Сведения: `{VIN}_OSGOP_{contract_number}_{YYYYmmdd}.pdf`

Если VIN не найден — вместо него используется латинский госномер.

## Архитектура

```
app/
├── api/v1/endpoints/parser.py  # Эндпоинты
├── core/
│   ├── config.py               # Конфигурация (environs)
│   ├── exceptions.py           # Кастомные исключения
│   └── logging.py              # Логирование
├── models/
│   └── contract.py             # Pydantic: OSGOPContract, VehicleInfo
├── services/
│   ├── parser.py               # Оркестрация парсинга
│   ├── parser_factory.py       # Фабрика сборки парсера
│   ├── segment_detector.py     # Детекция POLIS / SVEDENIYA
│   ├── field_extractors.py     # Даты, ИНН, госномера, премия
│   ├── pdf_reader.py           # PyMuPDF → pdfminer.six → pypdf
│   ├── plate_normalizer.py     # Кириллица ↔ латиница
│   ├── plate_cache.py          # SQLite-кэш госномеров
│   ├── file_saver.py           # JSON / CSV / PDF
│   └── element_api_client_async.py  # Element API (1С)
└── main.py                     # FastAPI create_app()
```

### Поток обработки

```
PDF bytes
  → pdf_reader (PyMuPDF → pdfminer.six → pypdf)
  → segment_detector (POLIS / SVEDENIYA)
  → field_extractors (номер, даты, ИНН, госномера, премия)
  → plate_cache.get() → кэш?
    → да: VIN + СТС + модель
    → нет: Element API → plate_cache.put()
  → file_saver (JSON + CSV + нарезанные PDF)
```

### Кэш госномеров (PlateCache)

SQLite-база `plate_cache.db`. Ключ — кириллический госномер. При первом запросе ходит в Element API, при повторных — мгновенный ответ из кэша (ускорение ×6–9).

## Зависимости

| Библиотека | Зачем |
|---|---|
| **fastapi[standard]** | Веб-фреймворк |
| **pymupdf** | Основной движок чтения PDF |
| **pdfminer.six** | Fallback-движок PDF |
| **pypdf** | Последний fallback + нарезка страниц |
| **httpx** | Асинхронный HTTP-клиент (Element API) |
| **pandas** | Генерация CSV |
| **environs** | Чтение .env |
| **pytest** | Тесты |
