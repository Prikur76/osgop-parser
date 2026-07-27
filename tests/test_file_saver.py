"""Тесты FileSaver."""
import json
import tempfile
from pathlib import Path

import pytest

from app.models.contract import OSGOPContract, VehicleInfo
from app.services.file_saver import FileSaver


@pytest.fixture
def sample_contract():
    return OSGOPContract(
        contract_number="ROSX1234567890",
        contract_date="2025-10-28",
        period_from="2025-10-31",
        period_to="2026-10-30",
        insurer="АО «Тест-Страхование»",
        insurer_inn="7702073683",
        insured="ООО «Тест-Перевозчик»",
        insured_inn="7721751172",
        bonus=15000.50,
        vehicles=[
            VehicleInfo(
                vehicle_plate_cyr="А225РН797",
                vehicle_plate_lat="A225PH797",
                vin="XTA1234567890",
                car_info={"model": "ГАЗель NEXT", "year": "2023"}
            )
        ]
    )


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmp:
        yield tmp


def test_save_csv_structure(temp_dir, sample_contract):
    """_save_csv должен создать CSV с правильными заголовками."""
    import asyncio
    saver = FileSaver(base_dir=temp_dir)

    async def run():
        return await saver._save_csv(sample_contract, include_car_info=False)

    csv_path = asyncio.run(run())
    assert Path(csv_path).exists()

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        content = f.read()
    assert "Номер договора" in content
    assert "ROSX1234567890" in content
    assert "А225РН797" in content


def test_save_json(temp_dir, sample_contract):
    """_save_json должен создать JSON-файл с корректной структурой."""
    import asyncio
    saver = FileSaver(base_dir=temp_dir)

    async def run():
        return await saver._save_json(sample_contract)

    json_path = asyncio.run(run())
    assert Path(json_path).exists()

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["contract"]["contract_number"] == "ROSX1234567890"


def test_sanitize_filename():
    """_sanitize_filename должен очищать спецсимволы."""
    saver = FileSaver()
    assert saver._sanitize_filename('test:file<name>.pdf') == 'test_file_name_.pdf'
    assert saver._sanitize_filename('') == 'unknown'
    assert saver._sanitize_filename('a' * 200) == 'a' * 100


def test_format_date_for_filename():
    """_format_date_for_filename должен форматировать даты."""
    saver = FileSaver()
    assert saver._format_date_for_filename("2025-10-28") == "251028"
    assert saver._format_date_for_filename(None) == "000000"
    assert saver._format_date_for_filename("not-a-date") == "000000"
