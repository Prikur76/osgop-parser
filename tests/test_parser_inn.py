from app.services.field_extractors import parse_polis_header


TEXT = """
Страховщик:
АО "Зетта Страхование"
ИНН 7702073683 КПП 770201001

Страхователь:
ООО "КАРТЕЛЬ"
ИНН 7721751172 КПП 772101001
"""


def test_inn_extraction():
    data = parse_polis_header(TEXT)

    assert data["insurer_inn"] == "7702073683"
    assert data["insured_inn"] == "7721751172"
