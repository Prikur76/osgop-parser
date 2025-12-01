# normalization maps for plates
LATIN_TO_CYR = str.maketrans({
    "A": "А", "B": "В", "E": "Е", "K": "К", "M": "М", "H": "Н", "O": "О",
    "P": "Р", "C": "С", "T": "Т", "Y": "У", "X": "Х"
})

CYR_TO_LAT = str.maketrans({
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O",
    "Р": "P", "С": "C", "Т": "T", "У": "Y", "Х": "X"
})

# Добавляем более полные таблицы для обратной конвертации
FULL_CYR_TO_LAT = {
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", 
    "О": "O", "Р": "P", "С": "C", "Т": "T", "У": "Y", "Х": "X",
    "а": "a", "в": "b", "е": "e", "к": "k", "м": "m", "н": "h",
    "о": "o", "р": "p", "с": "c", "т": "t", "у": "y", "х": "x"
}

FULL_LAT_TO_CYR = {
    "A": "А", "B": "В", "E": "Е", "K": "К", "M": "М", "H": "Н",
    "O": "О", "P": "Р", "C": "С", "T": "Т", "Y": "У", "X": "Х",
    "a": "а", "b": "в", "e": "е", "k": "к", "m": "м", "h": "н",
    "o": "о", "p": "р", "c": "с", "t": "т", "y": "у", "x": "х"
}


def to_cyr(plate: str) -> str:
    """Конвертирует латинские буквы в кириллические"""
    return plate.upper().translate(LATIN_TO_CYR)


def to_lat(plate: str) -> str:
    """Конвертирует кириллические буквы в латинские"""
    return plate.translate(CYR_TO_LAT)


def to_cyr_full(plate: str) -> str:
    """Полная конвертация латиницы в кириллицу"""
    result = []
    for char in plate:
        if char in FULL_LAT_TO_CYR:
            result.append(FULL_LAT_TO_CYR[char])
        else:
            result.append(char)
    return ''.join(result)


def to_lat_full(plate: str) -> str:
    """Полная конвертация кириллицы в латиницу"""
    result = []
    for char in plate:
        if char in FULL_CYR_TO_LAT:
            result.append(FULL_CYR_TO_LAT[char])
        else:
            result.append(char)
    return ''.join(result)


def normalize_plate_for_api(plate: str) -> str:
    """
    Нормализует госномер для запроса в API 1С.
    Конвертирует из латиницы в кириллицу и убирает пробелы.
    """
    # Конвертируем в кириллицу
    plate_cyr = to_cyr_full(plate)
    # Убираем пробелы и лишние символы
    plate_clean = plate_cyr.upper().replace(" ", "").replace("-", "")
    return plate_clean


def normalize_plate_for_storage(plate: str) -> str:
    """
    Нормализует госномер для хранения.
    Конвертирует в латиницу и приводит к единому формату.
    """
    # Конвертируем в латиницу
    plate_lat = to_lat_full(plate)
    # Приводим к верхнему регистру и убираем пробелы
    plate_clean = plate_lat.upper().replace(" ", "")
    return plate_clean
