import pytest
from unittest.mock import patch


@pytest.fixture(autouse=True)
def mock_car_api():
    """Подменяем API клиента, чтобы тесты были offline и стабильны."""

    class DummyAPI:
        def get_car_extended_info(self, plate):
            return {
                "vin": "TESTVIN1234567890",
                "model": "TESTMODEL",
                "brand": "TESTBRAND",
                "year": "2024-01-01",
                "status": "ACTIVE",
                "activity": True,
                "plate": plate,
                "sts_series": "1234",
                "sts_number": "567890",
            }

    with patch("app.services.parser.get_car_api_client", return_value=DummyAPI()):
        yield

