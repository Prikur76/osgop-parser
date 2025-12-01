import logging
import PyPDF2

from io import BytesIO
from typing import List
from pdfminer.high_level import extract_text as pdfminer_extract_text

log = logging.getLogger(__name__)


class PDFReadError(Exception):
    """Ошибка чтения PDF"""


def extract_with_pypdf2(pdf_bytes: bytes) -> List[str]:
    """
    Попытка прочитать PDF с помощью PyPDF2 (быстро, но не всегда качественно).
    """
    try:
        reader = PyPDF2.PdfReader(BytesIO(pdf_bytes))
        pages = []

        for i, page in enumerate(reader.pages):
            try:
                text = page.extract_text() or ""
                pages.append(text)
            except Exception as e:
                log.warning(f"[PyPDF2] Ошибка чтения страницы {i}: {e}")
                pages.append("")

        if not pages:
            raise PDFReadError("PyPDF2 не смог извлечь текст.")

        log.info(f"[PyPDF2] Извлечено {len(pages)} страниц.")
        return pages

    except Exception as e:
        log.error(f"[PyPDF2] Ошибка загрузки PDF: {e}")
        raise PDFReadError(str(e))


def extract_with_pdfminer(pdf_bytes: bytes) -> List[str]:
    """
    Fallback: более глубокое и качественное извлечение через pdfminer.six.
    Медленнее, зато более устойчиво к нестандартным PDF.
    """
    try:
        text = pdfminer_extract_text(BytesIO(pdf_bytes)) or ""
        if not text.strip():
            raise PDFReadError("pdfminer не смог извлечь текст.")

        # Превращаем текст в псевдо-страницы.
        pages = text.split("\f")
        log.info(f"[pdfminer] Извлечено {len(pages)} логических страниц.")
        return pages

    except Exception as e:
        log.error(f"[pdfminer] Ошибка извлечения текста: {e}")
        raise PDFReadError(str(e))


def extract_text_safe(pdf_bytes: bytes) -> List[str]:
    """
    Универсальная безопасная функция:
    1. Сначала PyPDF2 (быстро)
    2. Если неудачно → fallback на pdfminer (надёжно)
    3. Если оба не работают → exception
    """
    log.info("Начинаю чтение PDF...")

    # 1) Быстрая попытка
    try:
        return extract_with_pypdf2(pdf_bytes)
    except PDFReadError:
        log.warning("PyPDF2 не справился, пробую pdfminer...")

    # 2) Надёжный fallback
    try:
        return extract_with_pdfminer(pdf_bytes)
    except PDFReadError:
        pass

    # 3) Полная неудача
    log.critical("Ни один из PDF движков не смог прочитать документ.")
    raise PDFReadError("PDF повреждён или не поддаётся разбору.")
