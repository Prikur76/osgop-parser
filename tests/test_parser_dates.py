from app.services.parser import OSGOPParser


TEXT = """
Дата заключения договора: 29 октября 2025
Срок действия:
с 30 октября 2025 по 29 октября 2026

(Год выпуска 2023)
"""

def test_date_extraction():
    p = OSGOPParser()
    data = p._extract_dates(TEXT)

    assert data["contract_date"] == "2025-10-29"
    assert data["period_from"] == "2025-10-30"
    assert data["period_to"] == "2026-10-29"
