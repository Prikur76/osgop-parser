"""Тесты SQLite-кэша госномеров."""
import tempfile
import asyncio
from pathlib import Path

import pytest

from app.services.plate_cache import PlateCache


@pytest.fixture
def temp_db():
    with tempfile.TemporaryDirectory() as tmp:
        yield str(Path(tmp) / "test_cache.db")


def test_put_and_get(temp_db):
    """Записали — прочитали."""
    async def run():
        cache = PlateCache(temp_db)
        await cache.put("А123ВС777", {"VIN": "XTA123456", "Model": "Lada", "YearCar": "2023"})
        result = await cache.get("А123ВС777")
        assert result is not None
        assert result["vin"] == "XTA123456"
        assert result["model"] == "Lada"
        assert result["year"] == "2023"

    asyncio.run(run())


def test_get_missing(temp_db):
    """Несуществующий номер → None."""
    async def run():
        cache = PlateCache(temp_db)
        result = await cache.get("НЕТТАКОГО")
        assert result is None

    asyncio.run(run())


def test_put_updates_existing(temp_db):
    """Повторный put обновляет запись."""
    async def run():
        cache = PlateCache(temp_db)
        await cache.put("А123ВС777", {"VIN": "OLD_VIN"})
        await cache.put("А123ВС777", {"VIN": "NEW_VIN"})
        result = await cache.get("А123ВС777")
        assert result["vin"] == "NEW_VIN"

    asyncio.run(run())


def test_sts_fields(temp_db):
    """STS поля сохраняются и читаются."""
    async def run():
        cache = PlateCache(temp_db)
        await cache.put("Е866НР977", {
            "VIN": "MX1J7AGGXPK019168",
            "STSSeries": "7729",
            "STSNumber": "523038",
            "Model": "JAC J7",
        })
        result = await cache.get("Е866НР977")
        assert result["vin"] == "MX1J7AGGXPK019168"
        assert result["sts_series"] == "7729"
        assert result["sts_number"] == "523038"

    asyncio.run(run())
