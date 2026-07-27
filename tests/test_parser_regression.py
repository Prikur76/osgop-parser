"""Regression-тесты: нормализация госномера и извлечение ИНН."""
from app.services.field_extractors import parse_polis_header
from app.services.plate_normalizer import to_cyr_full, normalize_plate


# --------------------------- Госномера ---------------------------

def test_plate_normalization_latin_to_cyrillic():
    # Латинский ввод -> кириллический канон
    assert to_cyr_full("A225PH797") == "А225РН797"


def test_plate_normalization_cyrillic_to_latin():
    assert normalize_plate("А225РН797") == "A225PH797"


def test_plate_normalization_real_plate():
    # Реальный номер из эталонного PDF
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
