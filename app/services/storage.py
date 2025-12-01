from pathlib import Path


BASE_OUTPUT = Path("output")
PDF_DIR     = BASE_OUTPUT / "pdf"
JSON_DIR    = BASE_OUTPUT / "json"
CSV_DIR     = BASE_OUTPUT / "csv"


def ensure_dirs():
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    CSV_DIR.mkdir(parents=True, exist_ok=True)
