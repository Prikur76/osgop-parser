from app.services.parser import OSGOPParser


def test_parser_runs():
    parser = OSGOPParser()
    result = parser.parse(b"%PDF- FAKE DATA%")
    assert isinstance(result, list)
