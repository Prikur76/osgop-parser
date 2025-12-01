from io import BytesIO
from PyPDF2 import PdfReader, PdfWriter
from pathlib import Path
import logging


log = logging.getLogger(__name__)


def save_pdf_pages(pdf_bytes: bytes, pages: list[int], filename: str, outdir: Path):
    """
    Сохраняет указанные страницы PDF в новый файл.
    """
    outdir.mkdir(parents=True, exist_ok=True)
    full_path = outdir / filename
    
    reader = PdfReader(BytesIO(pdf_bytes))
    writer = PdfWriter()

    for p in pages:
        writer.add_page(reader.pages[p])

    with open(full_path, "wb") as f:
        writer.write(f)

    log.info(f"Saved PDF: {full_path}")
    return full_path
