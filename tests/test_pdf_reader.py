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
