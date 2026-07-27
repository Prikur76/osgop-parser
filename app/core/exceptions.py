"""Кастомные исключения приложения."""


class AppError(Exception):
    """Базовое исключение приложения. Все наследники несут HTTP status_code."""
    status_code: int = 500


class PDFParseError(AppError):
    """Ошибка разбора PDF — документ повреждён или нечитаем."""
    status_code = 422


class NoSegmentsFoundError(AppError):
    """В документе не найдены сегменты (ни POLIS, ни SVEDENIYA)."""
    status_code = 422


class ElementAPIError(AppError):
    """Ошибка взаимодействия с Element API."""
    status_code = 502
