from environs import Env
import os


env = Env()
env.read_env()


def _bool(key: str, default: bool = False) -> bool:
    """Читает булеву переменную окружения с защитой от невалидных значений."""
    raw = os.getenv(key)
    if raw is None or raw.strip() == "":
        return default
    try:
        return env.bool(key, default)
    except Exception:
        return default


class Config:
    PDF_MAX_SIZE_MB = 25
    DEBUG = _bool("DEBUG", False)

    # Element (бывш. 1С Элемент) — опционально. Дефолты позволяют
    # запускать приложение без .env; валидация кредов — только при ELEMENT_ENABLED.
    ELEMENT_BASE_URL = env.str("ELEMENT_BASE_URL", "")
    ELEMENT_USERNAME = env.str("ELEMENT_USERNAME", "")
    ELEMENT_PASSWORD = env.str("ELEMENT_PASSWORD", "")
    ELEMENT_ENABLED = _bool("ELEMENT_ENABLED", False)
    ELEMENT_TIMEOUT = env.float("ELEMENT_TIMEOUT", 30.0)
    ELEMENT_VERIFY_SSL = _bool("ELEMENT_VERIFY_SSL", True)

    # SQLite-кэш госномеров
    PLATE_CACHE_PATH = env.str("PLATE_CACHE_PATH", "plate_cache.db")

    def validate(self) -> None:
        """Проверяет, что при включённом Element заданы все креды."""
        if not self.ELEMENT_ENABLED:
            return
        missing = []
        if not self.ELEMENT_BASE_URL:
            missing.append("ELEMENT_BASE_URL")
        if not self.ELEMENT_USERNAME:
            missing.append("ELEMENT_USERNAME")
        if not self.ELEMENT_PASSWORD:
            missing.append("ELEMENT_PASSWORD")
        if missing:
            raise ValueError(
                f"ELEMENT_ENABLED=True, но не заданы переменные: {', '.join(missing)}"
            )


config = Config()
