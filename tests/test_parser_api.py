"""Тесты API-эндпоинтов."""
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)
REFERENCES_DIR = Path(__file__).parent.parent / "references"
POLIS_PDF = REFERENCES_DIR / "24072026202858192_Полис ОСГОП Извозчик.pdf"
SVED_PDF = REFERENCES_DIR / "24072026202749658_Сведения ОСГОП Извозчик.pdf"


def _read_pdf(path: Path) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def test_health_endpoint():
    """GET /health должен вернуть 200 и status=ok."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_parse_test_endpoint():
    """POST /api/v1/parse/test с эталонным PDF должен вернуть success=True."""
    pdf_bytes = _read_pdf(POLIS_PDF)
    response = client.post(
        "/api/v1/parse/test",
        files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
    )
    assert response.status_code == 200
    data = response.json()
    assert data.get("success") is True
    assert "contract" in data


def test_parse_json_endpoint():
    """POST /api/v1/parse/json должен вернуть JSON с Content-Disposition."""
    pdf_bytes = _read_pdf(POLIS_PDF)
    response = client.post(
        "/api/v1/parse/json",
        files={"file": ("test.pdf", pdf_bytes, "application/pdf")},
    )
    assert response.status_code == 200
    assert "application/json" in response.headers.get("content-type", "")
    assert "attachment" in response.headers.get("content-disposition", "")


def test_parse_invalid_file():
    """POST /api/v1/parse/json с не-PDF должен вернуть ошибку."""
    response = client.post(
        "/api/v1/parse/json",
        files={"file": ("test.txt", b"not a pdf", "text/plain")},
    )
    assert response.status_code in (400, 422, 500)


# --- Раздельный парсинг ---

def test_split_json_endpoint():
    """POST /api/v1/parse/split/json должен вернуть JSON."""
    polis_bytes = _read_pdf(POLIS_PDF)
    sved_bytes = _read_pdf(SVED_PDF)
    response = client.post(
        "/api/v1/parse/split/json",
        files={
            "polis_file": ("polis.pdf", polis_bytes, "application/pdf"),
            "svedeniya_file": ("sved.pdf", sved_bytes, "application/pdf"),
        },
    )
    assert response.status_code == 200
    assert "application/json" in response.headers.get("content-type", "")


def test_split_missing_file():
    """POST /api/v1/parse/split/json без одного файла → 422."""
    polis_bytes = _read_pdf(POLIS_PDF)
    response = client.post(
        "/api/v1/parse/split/json",
        files={
            "polis_file": ("polis.pdf", polis_bytes, "application/pdf"),
        },
    )
    assert response.status_code == 422
