from app.services.field_extractors import parse_svedeniya


# В реальных PDF госномера — кириллические; парсер нормализует их в латиницу.
TEXT = """
ТС:
Гос. номер: А 225  РН  797
"""


def test_plate_extraction():
    res = parse_svedeniya(TEXT)

    assert res is not None
    assert res["plate_cyr"] == "А225РН797"
    assert res["plate_lat"] == "A225PH797"
