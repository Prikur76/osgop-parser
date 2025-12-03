import logging
from typing import Optional

from app.services.element_api_client_async import ElementApiClientAsync
from app.services.parser import OSGOPParser
from app.core.config import config

log = logging.getLogger(__name__)


async def get_osgop_parser(element_client: Optional[ElementApiClientAsync] = None) -> OSGOPParser:
    """
    Создает и возвращает асинхронный парсер ОСГОП.
    
    Args:
        element_client: Существующий клиент Element API (опционально)
    
    Returns:
        Экземпляр OSGOPParser
    """
    if element_client is None and config.ELEMENT_ENABLED:
        try:
            # Создаем новый клиент Element API
            element_client = await ElementApiClientAsync(
                base_url=config.ELEMENT_BASE_URL,
                username=config.ELEMENT_USERNAME,
                password=config.ELEMENT_PASSWORD,
                enabled=config.ELEMENT_ENABLED
            ).init()
            log.info("Создан новый Element API клиент для парсера")
        except Exception as e:
            log.error(f"Ошибка создания Element API клиента: {e}")
            element_client = None
    
    # Создаем парсер с клиентом Element API
    parser = OSGOPParser(element_api_client=element_client)
    return parser


async def close_osgop_parser_resources(parser: OSGOPParser) -> None:
    """
    Закрывает ресурсы, связанные с парсером.
    
    Args:
        parser: Экземпляр OSGOPParser
    """
    if parser.element_api_client:
        try:
            await parser.element_api_client.close()
            log.info("Element API клиент парсера закрыт")
        except Exception as e:
            log.error(f"Ошибка закрытия Element API клиента: {e}")
