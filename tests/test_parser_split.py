"""Тесты раздельного парсинга: полис + сведения как два отдельных PDF."""
import pytest
import asyncio
from pathlib import Path

from app.services.parser import OSGOPParser


REFERENCES_DIR = Path(__file__).parent.parent / "references"
POLIS_PDF = REFERENCES_DIR / "24072026202858192_Полис ОСГОП Извозчик.pdf"
SVED_PDF = REFERENCES_DIR / "24072026202749658_Сведения ОСГОП Извозчик.pdf"


def _read_pdf(path: Path) -> bytes:
    with open(path, "rb") as f:
        return f.read()


@pytest.fixture
def polis_bytes():
    return _read_pdf(POLIS_PDF)


@pytest.fixture
def sved_bytes():
    return _read_pdf(SVED_PDF)


def _parse(polis_bytes, sved_bytes):
    """Синхронная обёртка для вызова асинхронного parse_split_files."""
    async def run():
        parser = OSGOPParser()
        return await parser.parse_split_files(polis_bytes, sved_bytes)
    return asyncio.run(run())


def test_split_parse_contract_number(polis_bytes, sved_bytes):
    """Раздельный парсинг должен найти номер полиса ROSX..."""
    contracts, segments = _parse(polis_bytes, sved_bytes)
    assert len(contracts) == 1
    contract = contracts[0]
    assert contract.contract_number is not None
    assert contract.contract_number.startswith("ROSX")


def test_split_parse_vehicles(polis_bytes, sved_bytes):
    """Раздельный парсинг должен найти несколько ТС с госномерами."""
    contracts, _ = _parse(polis_bytes, sved_bytes)
    contract = contracts[0]
    assert len(contract.vehicles) > 0
    for v in contract.vehicles:
        assert v.vehicle_plate_cyr
        assert v.vehicle_plate_lat


def test_split_parse_header(polis_bytes, sved_bytes):
    """Раздельный парсинг должен извлечь страховщика, страхователя, ИНН."""
    contracts, _ = _parse(polis_bytes, sved_bytes)
    contract = contracts[0]
    assert contract.insurer is not None
    assert contract.insured is not None
    assert contract.insurer_inn is not None
    assert contract.insured_inn is not None


def test_split_parse_dates(polis_bytes, sved_bytes):
    """Раздельный парсинг должен извлечь дату договора и период."""
    contracts, _ = _parse(polis_bytes, sved_bytes)
    contract = contracts[0]
    assert contract.contract_date is not None
    assert contract.period_from is not None
    assert contract.period_to is not None


def test_split_parse_segments(polis_bytes, sved_bytes):
    """segments[0] — полис, segments[1:] — сведения для каждого ТС."""
    contracts, segments = _parse(polis_bytes, sved_bytes)
    contract = contracts[0]
    assert len(segments) >= 1 + len(contract.vehicles), \
        f"Ожидалось минимум {1 + len(contract.vehicles)} сегментов, получено {len(segments)}"
