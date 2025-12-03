import logging
import pypdf
import fitz

from io import BytesIO
from typing import List
from pdfminer.high_level import extract_text as pdfminer_extract_text

log = logging.getLogger(__name__)


class PDFReadError(Exception):
    """Ошибка чтения PDF"""


def extract_text_safe(pdf_bytes: bytes) -> List[str]:
    """
    Универсальное извлечение текста из PDF с восстановлением структуры.
    Использует PyMuPDF как основной движок (лучше сохраняет пробелы).
    """
    log.info("Начинаю чтение PDF...")
    
    # 1. Попробовать PyMuPDF (fitz) - лучше сохраняет пробелы
    try:
        pages = extract_with_pymupdf(pdf_bytes)
        log.info(f"[PyMuPDF] Успешно извлечено {len(pages)} страниц")
        
        # Дебаг: выводим первые 300 символов каждой страницы
        for i, page in enumerate(pages[:3]):  # Только первые 3 страницы
            clean_page = page.replace('\n', ' ').replace('\r', ' ')
            log.debug(f"Страница {i} (300 символов): {clean_page[:300]}")
            
        return pages
        
    except Exception as e:
        log.warning(f"PyMuPDF не справился: {e}")
    
    # 2. Попробовать pdfminer как fallback
    try:
        return extract_with_pdfminer(pdf_bytes)
    except Exception as e:
        log.warning(f"pdfminer не справился: {e}")
    
    # 3. Последняя попытка - pypdf
    try:
        return extract_with_pypdf(pdf_bytes)
    except Exception as e:
        log.error(f"pypdf не справился: {e}")
    
    log.critical("Ни один из PDF движков не смог прочитать документ.")
    raise PDFReadError("PDF повреждён или не поддаётся разбору.")


def extract_with_pymupdf(pdf_bytes: bytes) -> List[str]:
    """
    Извлечение текста с помощью PyMuPDF (лучше сохраняет пробелы).
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = []
        
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text = page.get_text()
            pages.append(text)
        
        doc.close()
        
        if not pages or all(not page.strip() for page in pages):
            raise PDFReadError("PyMuPDF не смог извлечь текст")
        
        log.info(f"[PyMuPDF] Извлечено {len(pages)} страниц.")
        return pages
        
    except Exception as e:
        log.error(f"[PyMuPDF] Ошибка: {e}")
        raise PDFReadError(str(e))


def extract_with_pdfminer(pdf_bytes: bytes) -> List[str]:
    """
    Fallback: извлечение через pdfminer.six.
    """
    try:
        text = pdfminer_extract_text(BytesIO(pdf_bytes)) or ""
        if not text.strip():
            raise PDFReadError("pdfminer не смог извлечь текст")
        
        pages = text.split("\f")
        log.info(f"[pdfminer] Извлечено {len(pages)} логических страниц.")
        return pages
        
    except Exception as e:
        log.error(f"[pdfminer] Ошибка: {e}")
        raise PDFReadError(str(e))


def extract_with_pypdf(pdf_bytes: bytes) -> List[str]:
    """
    Извлечение текста с помощью pypdf.
    """
    try:
        reader = pypdf.PdfReader(BytesIO(pdf_bytes))
        pages = []
        
        for page in reader.pages:
            text = page.extract_text() or ""
            pages.append(text)
        
        if not pages:
            raise PDFReadError("pypdf не смог извлечь текст")
        
        log.info(f"[pypdf] Извлечено {len(pages)} страниц.")
        return pages
        
    except Exception as e:
        log.error(f"[pypdf] Ошибка: {e}")
        raise PDFReadError(str(e))


def extract_pages_as_pdf(pdf_bytes: bytes, pages: List[int]) -> bytes:
    """
    Извлечение указанных страниц как отдельного PDF.
    """
    try:
        input_pdf = pypdf.PdfReader(BytesIO(pdf_bytes))
        output_pdf = pypdf.PdfWriter()
        
        for page_num in pages:
            if 0 <= page_num < len(input_pdf.pages):
                output_pdf.add_page(input_pdf.pages[page_num])
        
        output_buffer = BytesIO()
        output_pdf.write(output_buffer)
        output_buffer.seek(0)
        
        return output_buffer.read()
        
    except Exception as e:
        log.error(f"Ошибка при извлечении страниц: {e}")
        raise
