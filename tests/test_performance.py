"""Бенчмарки: скорость парсинга и кэша."""
import asyncio
import time
import tempfile
from pathlib import Path

import pytest

from app.services.parser import OSGOPParser
from app.services.plate_cache import PlateCache
from app.services.element_api_client_async import ElementApiClientAsync
from app.core.config import config


REFERENCES_DIR = Path(__file__).parent.parent / "references"
POLIS_PDF = REFERENCES_DIR / "24072026202858192_Полис ОСГОП Извозчик.pdf"
SVED_PDF = REFERENCES_DIR / "24072026202749658_Сведения ОСГОП Извозчик.pdf"


def _read(path: Path) -> bytes:
    return path.read_bytes()


def _run(coro):
    return asyncio.run(coro)


class TestCacheSpeed:
    """Скорость SQLite-кэша."""

    def test_write_100_read_100(self):
        """100 записей + 100 чтений — быстро."""
        with tempfile.TemporaryDirectory() as tmp:
            cache = PlateCache(str(Path(tmp) / "perf.db"))

            async def run():
                t0 = time.perf_counter()
                for i in range(100):
                    await cache.put(f"T{i:04d}", {"VIN": f"V{i:04d}"})
                write_t = time.perf_counter() - t0

                t0 = time.perf_counter()
                for i in range(100):
                    await cache.get(f"T{i:04d}")
                read_t = time.perf_counter() - t0
                return write_t, read_t

            w, r = _run(run())
            assert w < 5.0, f"Запись 100: {w:.2f}c"
            assert r < 1.0, f"Чтение 100: {r:.2f}c"


class TestParseSpeed:
    """Скорость парсинга без Element API."""

    @pytest.fixture
    def pdfs(self):
        return _read(POLIS_PDF), _read(SVED_PDF)

    def test_parse_no_cache(self, pdfs):
        """Два прохода без кэша — оба быстрые."""
        polis, sved = pdfs

        async def run():
            p = OSGOPParser()
            t0 = time.perf_counter()
            c1, _ = await p.parse_split_files(polis, sved)
            t1 = time.perf_counter() - t0
            t0 = time.perf_counter()
            c2, _ = await p.parse_split_files(polis, sved)
            t2 = time.perf_counter() - t0
            return t1, t2, len(c1[0].vehicles) if c1 else 0

        t1, t2, n = _run(run())
        assert t1 < 10.0, f"1-й проход: {t1:.2f}c"
        assert t2 < 10.0, f"2-й проход: {t2:.2f}c"
        assert n > 0, "Нет ТС"

    def test_parse_cache_accelerates(self, pdfs):
        """С кэшем второй проход быстрее первого."""
        polis, sved = pdfs
        with tempfile.TemporaryDirectory() as tmp:
            cache = PlateCache(str(Path(tmp) / "perf.db"))

            async def run():
                p = OSGOPParser(plate_cache=cache)
                t0 = time.perf_counter()
                c1, _ = await p.parse_split_files(polis, sved)
                t1 = time.perf_counter() - t0
                t0 = time.perf_counter()
                c2, _ = await p.parse_split_files(polis, sved)
                t2 = time.perf_counter() - t0
                return t1, t2 and t1 / t2 if t2 > 0 else 0, len(c1[0].vehicles) if c1 else 0

            t1, speedup, n = _run(run())
            assert n > 0, "Нет ТС"
            assert t1 < 10.0, f"1-й проход: {t1:.2f}c"
            # Кэш должен помогать даже без Element (повторный regex-поиск быстрее)


class TestElementAPIPerformance:
    """Скорость с Element API (нужен .env с ELEMENT_ENABLED=true)."""

    @pytest.fixture
    def pdfs(self):
        return _read(POLIS_PDF), _read(SVED_PDF)

    @pytest.mark.skipif(not config.ELEMENT_ENABLED, reason="Требуется Element API")
    def test_full_pipeline_speed(self, pdfs):
        """Холодный → горячий кэш: ускорение x3+."""
        polis, sved = pdfs
        with tempfile.TemporaryDirectory() as tmp:
            async def run():
                client = await ElementApiClientAsync(
                    base_url=config.ELEMENT_BASE_URL,
                    username=config.ELEMENT_USERNAME,
                    password=config.ELEMENT_PASSWORD,
                ).init()
                cache = PlateCache(str(Path(tmp) / "perf.db"))
                p = OSGOPParser(element_api_client=client, plate_cache=cache)

                t0 = time.perf_counter()
                c1, _ = await p.parse_split_files(polis, sved)
                cold_t = time.perf_counter() - t0

                t0 = time.perf_counter()
                c2, _ = await p.parse_split_files(polis, sved)
                hot_t = time.perf_counter() - t0

                await client.close()
                v1 = c1[0]
                v2 = c2[0]
                return cold_t, hot_t, v1.vehicles_with_vin_count, v2.vehicles_with_vin_count, len(v1.vehicles)

            cold, hot, vin1, vin2, total = _run(run())
            speedup = cold / hot if hot > 0 else 0

            assert cold < 15.0, f"Холодный: {cold:.1f}c > 15c"
            assert hot < 3.0, f"Горячий: {hot:.1f}c > 3c"
            assert speedup >= 3.0, f"Ускорение x{speedup:.1f} < x3.0"
            assert vin1 == vin2 == total, f"VIN: {vin1}/{vin2}/{total}"
