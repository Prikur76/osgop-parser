from app.services.parser import OSGOPParser


SAMPLE_TEXT = """
ПОЛИС ОБЯЗАТЕЛЬНОГО СТРАХОВАНИЯ

№ ROSX22576020266000

Страховщик:
Акционерное общество "Зетта Страхование"
ИНН 7702073683 КПП 770201001

Страхователь:
Общество с ограниченной ответственностью "КАРТЕЛЬ"
ИНН 7721751172 КПП 772101001
"""


def test_header_extraction():
    p = OSGOPParser()
    data = p._parse_polis_header(SAMPLE_TEXT)

    assert data["contract_number"] == "ROSX22576020266000"
    assert data["insurer"] == "Акционерное общество Зетта Страхование"
    assert data["insured"] == 'Общество с ограниченной ответственностью "КАРТЕЛЬ"'.replace('"', '')
    assert data["insurer_inn"] == "7702073683"
    assert data["insured_inn"] == "7721751172"
