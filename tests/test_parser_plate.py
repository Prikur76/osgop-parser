from app.services.parser import OSGOPParser

TEXT = """
ТС:
Гос. номер: A 225  PH  797
"""


def test_plate_extraction():
    p = OSGOPParser()
    plate = p._extract_plate_from_appendix(TEXT)

    assert plate == "A225PH797"
