from environs import Env


env = Env()
env.read_env()


class Config:
    PDF_MAX_SIZE_MB = 25
    DEBUG = env.bool("DEBUG", False)

    # Element (бывш. 1С Элемент) — опционально. Дефолты позволяют
    # запускать приложение без .env; валидация кредов — только при ELEMENT_ENABLED.
    ELEMENT_BASE_URL = env.str("ELEMENT_BASE_URL", "")
    ELEMENT_USERNAME = env.str("ELEMENT_USERNAME", "")
    ELEMENT_PASSWORD = env.str("ELEMENT_PASSWORD", "")
    ELEMENT_ENABLED = env.bool("ELEMENT_ENABLED", False)
    ELEMENT_TIMEOUT = env.float("ELEMENT_TIMEOUT", 30.0)
    ELEMENT_VERIFY_SSL = env.bool("ELEMENT_VERIFY_SSL", True)

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
