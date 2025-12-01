from app.services.parser import OSGOPParser


TEXT = """
Страховщик:
АО "Зетта Страхование"
ИНН 7702073683 КПП 770201001

Страхователь:
ООО "КАРТЕЛЬ"
ИНН 7721751172 КПП 772101001
"""


def test_inn_extraction():
    p = OSGOPParser()
    insurer_inn, insured_inn = p._extract_inns(TEXT)

    assert insurer_inn == "7702073683"
    assert insured_inn == "7721751172"
