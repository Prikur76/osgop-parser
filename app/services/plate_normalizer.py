import re
import logging


log = logging.getLogger(__name__)


# Таблицы конвертации
CYR_TO_LAT = {
    'А': 'A', 'В': 'B', 'Е': 'E', 'К': 'K', 'М': 'M', 'Н': 'H',
    'О': 'O', 'Р': 'P', 'С': 'C', 'Т': 'T', 'У': 'Y', 'Х': 'X',
    'а': 'a', 'в': 'b', 'е': 'e', 'к': 'k', 'м': 'm', 'н': 'h',
    'о': 'o', 'р': 'p', 'с': 'c', 'т': 't', 'у': 'y', 'х': 'x'
}

LAT_TO_CYR = {
    'A': 'А', 'B': 'В', 'E': 'Е', 'K': 'К', 'M': 'М', 'H': 'Н',
    'O': 'О', 'P': 'Р', 'C': 'С', 'T': 'Т', 'Y': 'У', 'X': 'Х',
    'a': 'а', 'b': 'в', 'e': 'е', 'k': 'к', 'm': 'м', 'h': 'н',
    'o': 'о', 'p': 'р', 'c': 'с', 't': 'т', 'y': 'у', 'x': 'х'
}


def to_cyr_full(plate: str) -> str:
    """Конвертация латиницы в кириллицу БЕЗ ПРОБЕЛОВ - с поддержкой всех форматов"""
    if not plate:
        return ""
    
    try:
        # Удаляем все неалфавитно-цифровые символы
        plate_clean = re.sub(r'[^\w]', '', plate.upper())
        
        if not plate_clean:
            return ""
        
        # Специальная обработка для формата "РР77777" (Р - особая буква)
        # Русская Р и латинская P выглядят одинаково в верхнем регистре
        plate_clean = plate_clean.replace('P', 'Р')  # Заменяем латинскую P на русскую Р
        
        # Конвертируем остальные символы
        result_chars = []
        for char in plate_clean:
            if char in LAT_TO_CYR:
                result_chars.append(LAT_TO_CYR[char])
            else:
                result_chars.append(char)
        
        plate_cyr = ''.join(result_chars)
        
        # Дополнительная проверка: если все символы уже кириллические
        if re.match(r'^[А-ЯЁ\d]+$', plate_cyr):
            return plate_cyr
        
        # Если есть смесь - оставляем как есть
        return plate_cyr
        
    except Exception as e:
        log.error(f"Ошибка конвертации номера {plate}: {e}")
        return plate.upper()  # Возвращаем оригинал в верхнем регистре


def to_lat_full(plate: str) -> str:
    """Конвертация кириллицы в латиницу"""
    result = []
    for char in plate:
        if char in CYR_TO_LAT:
            result.append(CYR_TO_LAT[char])
        else:
            result.append(char)
    return ''.join(result).upper()


def normalize_plate(plate_cyr: str) -> str:
    """
    Нормализация госномера: кириллица -> латиница без пробелов.
    Поддерживает форматы: А000АА000 и АА00000
    """
    # Удаляем пробелы и лишние символы
    plate_clean = re.sub(r'[^\w]', '', plate_cyr.upper())
    
    # Конвертируем в латиницу
    plate_lat = to_lat_full(plate_clean)
   
    return plate_lat


def normalize_plate_for_api(plate: str) -> str:
    """Нормализация для запроса в API 1С (кириллица без пробелов)"""
    # Сначала в кириллицу
    plate_cyr = to_cyr_full(plate)
    # Убираем пробелы
    plate_clean = plate_cyr.replace(' ', '')
    return plate_clean


def normalize_plate_for_storage(plate: str) -> str:
    """Нормализация для хранения (латиница без пробелов)"""
    plate_lat = normalize_plate(plate)
    return plate_lat.upper().replace(' ', '')
