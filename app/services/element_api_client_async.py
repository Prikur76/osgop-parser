import base64
import httpx
import logging

from typing import Optional, Dict, Any

from app.core.config import config


logger = logging.getLogger(__name__)


class ElementApiClientAsync:
    """
    Асинхронный клиент Element (быв. 1С Элемент).
    Не создаёт новые ТС, работает только с поиском и загрузкой файлов.
    """

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.auth = (username, password)
        self.client: Optional[httpx.AsyncClient] = None

    async def init(self):
        self.client = httpx.AsyncClient(
            auth=self.auth,
            timeout=config.ELEMENT_TIMEOUT,
            verify=config.ELEMENT_VERIFY_SSL
        )
        logger.info("Element Async API Client initialized")
        return self

    async def close(self):
        if self.client:
            await self.client.aclose()

    # ----------------------------------------------
    #   Поиск машины в Element по гос. номеру
    # ----------------------------------------------
    async def get_car_by_plate(self, plate: str) -> Optional[Dict[str, Any]]:
        try:
            url = f"{self.base_url}/hs/Car/v1/Get"
            r = await self.client.get(url, params={"num": plate})
            r.raise_for_status()

            cars = r.json()
            if not cars:
                return None

            cars_with_code = [c for c in cars if c.get("Code") not in (None, "", 0)]

            if cars_with_code:
                return max(cars_with_code, key=lambda c: int(c["Code"]))

            return cars[0]

        except Exception as e:
            logger.error(f"Element: get_car_by_plate error: {e}")
            return None

    # ----------------------------------------------
    #   Добавить файл в Element (base64 → файл)
    # ----------------------------------------------
    async def add_file(self, code: int, filename: str, file_bytes: bytes,
                       filetype: str = "", comment: str = "") -> Optional[str]:
        payload = {
            "Code": code,
            "FileName": filename,
            "FileBase64": base64.b64encode(file_bytes).decode("utf-8"),
            "FileType": filetype,
            "Comment": comment,
        }

        url = f"{self.base_url}/hs/Car/AddFile"
        try:
            r = await self.client.post(url, json=payload)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.error(f"Element: add_file error for Code={code}: {e}")
            return None
