"""Тесты детекции сегментов."""
from app.services.segment_detector import detect_segments, normalize_page_text, restore_spaces


POLIS_PAGE = "ПОЛИС ОБЯЗАТЕЛЬНОГО СТРАХОВАНИЯ ГРАЖДАНСКОЙ ОТВЕТСТВЕННОСТИ ПЕРЕВОЗЧИКА №ROSX123"
SVED_PAGE = "СВЕДЕНИЯ О ДОГОВОРЕ ОБЯЗАТЕЛЬНОГО СТРАХОВАНИЯ ГРАЖДАНСКОЙ ОТВЕТСТВЕННОСТИ ПЕРЕВОЗЧИКА ТС: А123ВС777"
PLAIN_PAGE = "Обычная страница без маркеров"


def test_detect_polis_only():
    pages = [POLIS_PAGE, PLAIN_PAGE]
    segments = detect_segments(pages)
    assert len(segments) == 1
    assert segments[0] == (0, 2, "POLIS")


def test_detect_svedeniya_only():
    pages = [SVED_PAGE, PLAIN_PAGE]
    segments = detect_segments(pages)
    assert len(segments) == 1
    assert segments[0] == (0, 2, "SVEDENIYA")


def test_detect_both():
    pages = [POLIS_PAGE, PLAIN_PAGE, SVED_PAGE, PLAIN_PAGE]
    segments = detect_segments(pages)
    assert len(segments) == 2
    assert segments[0] == (0, 2, "POLIS")
    assert segments[1] == (2, 4, "SVEDENIYA")


def test_detect_multiple_svedeniya():
    pages = [SVED_PAGE, PLAIN_PAGE, SVED_PAGE, PLAIN_PAGE]
    segments = detect_segments(pages)
    assert len(segments) == 2
    assert all(s[2] == "SVEDENIYA" for s in segments)


def test_detect_empty():
    assert detect_segments([]) == []


def test_detect_no_markers():
    assert detect_segments(["Страница 1", "Страница 2"]) == []


def test_normalize_removes_nbsp():
    assert "\xa0" not in normalize_page_text("текст\xa0с\xa0nbsp")


def test_restore_spaces_glued():
    result = restore_spaces("ПОЛИСОБЯЗАТЕЛЬНОГОСТРАХОВАНИЯ")
    assert "ПОЛИС ОБЯЗАТЕЛЬНОГО" in result
    assert "СТРАХОВАНИЯ" in result
