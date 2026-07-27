"""SQLite-кэш для результатов запросов к Element API по госномерам."""

import sqlite3
import asyncio
import logging
import threading
from contextlib import contextmanager
from queue import Queue, Empty
from datetime import datetime, timezone
from typing import Optional, Dict, Any

log = logging.getLogger(__name__)

POOL_SIZE = 3

_INIT_SQL = """CREATE TABLE IF NOT EXISTS plate_cache (
    plate_cyr TEXT PRIMARY KEY,
    vin TEXT,
    sts_series TEXT,
    sts_number TEXT,
    model TEXT,
    year TEXT,
    code TEXT,
    updated_at TEXT NOT NULL
)"""


class PlateCache:
    """Локальный кэш VIN, STS и данных ТС по кириллическому госномеру."""

    def __init__(self, db_path: str = "plate_cache.db"):
        self.db_path = db_path
        self._pool: Queue = Queue(maxsize=POOL_SIZE)
        self._write_lock = threading.Lock()

        # Инициализируем БД
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(_INIT_SQL)
        for col in ["sts_series", "sts_number"]:
            try:
                conn.execute(f"ALTER TABLE plate_cache ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError:
                pass
        conn.commit()
        conn.close()

    @contextmanager
    def _conn(self):
        """Взять соединение из пула, вернуть после использования."""
        try:
            conn = self._pool.get(block=False)
        except Empty:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
        finally:
            try:
                self._pool.put_nowait(conn)
            except Exception:
                conn.close()

    async def get(self, plate_cyr: str) -> Optional[Dict[str, Any]]:
        """Получить кэшированные данные по госномеру или None."""

        def _get():
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT vin, sts_series, sts_number, model, year, code "
                    "FROM plate_cache WHERE plate_cyr = ?",
                    (plate_cyr,)
                ).fetchone()
                if row is None:
                    return None
                return {
                    "vin": row[0], "sts_series": row[1], "sts_number": row[2],
                    "model": row[3], "year": row[4], "code": row[5]
                }

        return await asyncio.to_thread(_get)

    async def put(self, plate_cyr: str, car_data: Dict[str, Any]) -> None:
        """Сохранить данные ТС в кэш."""

        def _put():
            vin = car_data.get("VIN") or car_data.get("vin") or ""
            sts_series = car_data.get("STSSeries") or ""
            sts_number = car_data.get("STSNumber") or ""
            model = car_data.get("Model") or car_data.get("model") or ""
            year = car_data.get("YearCar") or car_data.get("year") or ""
            code = car_data.get("Code") or car_data.get("code") or ""
            now = datetime.now(timezone.utc).isoformat()

            with self._write_lock:
                with self._conn() as conn:
                    conn.execute(
                        """INSERT OR REPLACE INTO plate_cache
                           (plate_cyr, vin, sts_series, sts_number, model, year, code, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (plate_cyr, vin, str(sts_series), str(sts_number),
                         str(model), str(year), str(code), now)
                    )
                    conn.commit()

        await asyncio.to_thread(_put)
        log.debug(f"Кэш обновлён: {plate_cyr}")

    async def close(self) -> None:
        """Закрыть все соединения в пуле."""

        def _close():
            while True:
                try:
                    conn = self._pool.get(block=False)
                    conn.close()
                except Empty:
                    break

        await asyncio.to_thread(_close)
