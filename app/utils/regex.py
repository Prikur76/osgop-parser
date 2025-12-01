import re

# ============ Основные шаблоны ============

# Номер полиса — поддержка № / No / N / ROSX без символа №
RE_ROSX = re.compile(
    r"(?:№|Nо?|No)?\s*?(ROSX\d{10,20})",
    re.I
)

# Заголовки
RE_POLIS_START = re.compile(r"ПОЛИС\s+ОБЯЗАТЕЛЬНОГО\s+СТРАХОВАНИЯ", re.I)
RE_SVEDENIYA_START = re.compile(r"СВЕДЕНИЯ\s+О\s+ДОГОВОРЕ", re.I)

# Госномер
RE_PLATE = re.compile(
    r"\b[А-ЯA-Z]{1}\s*\d{3}\s*[А-ЯA-Z]{2}\s*\d{2,3}\b",
    re.I
)

# Период с по
RE_PERIOD_EXPLICIT = re.compile(
    r"с\s+(\d{1,2})\s+([А-Яа-яё]+)\s+(\d{4}).{0,120}?по\s+(\d{1,2})\s+([А-Яа-яё]+)\s+(\d{4})",
    re.S | re.I
)

# Дата вида: 29 октября 2025
RE_DATE_ANY = re.compile(
    r"(\d{1,2})\s+([А-Яа-яё]+)\s+(\d{4})",
    re.I
)

# Отдельная дата заключения договора
RE_CONTRACT_DATE = re.compile(
    r"Дата\s+заключения\s+договора[:\s]*(\d{1,2})\s+([А-Яа-яё]+)\s+(\d{4})",
    re.I
)

# ИНН / ИНН-КПП
RE_INN = re.compile(r"ИНН[:\s/]*(\d{10,12})", re.I)

RE_INN_KPP = re.compile(
    r"ИНН\s*/?\s*КПП[:\s]*(\d{10,12}).*?(\d{9})?",
    re.I | re.S
)

# Страховая премия
RE_PREMIUM = re.compile(
    r"(?:страхов(ая|ой)\s+преми[яи]|Итого\s+страховая\s+премия)[\s:]*(\d[\d\s.,]+)",
    re.I
)

# Страховщик
RE_INSURER = re.compile(
    r"Страховщик[:\s]*\n?(.*?)(?=\bИНН\b|Страхователь|Лицензия|$)",
    re.I | re.S
)

# Страхователь
RE_INSURED = re.compile(
    r"Страхователь[:\s]*\n?(.*?)(?=\bИНН\b|КПП|Перевозчик|$)",
    re.I | re.S
)
