from app.services.field_extractors import extract_dates_from_polis


TEXT = """
Срок страхования: с «31» октября 2025 г. по «30» октября 2026 г.

Дата заключения договора: 28 октября 2025 г.
Срок действия договора: с 31 октября 2025 г. по 30 октября 2026 г.
"""

def test_date_extraction():
    data = extract_dates_from_polis(TEXT)

    assert data["contract_date"] == "2025-10-28"
    assert data["period_from"] == "2025-10-31"
    assert data["period_to"] == "2026-10-30"
