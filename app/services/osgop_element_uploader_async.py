import asyncio
import logging

from pathlib import Path
from typing import Dict, Any

from app.services.element_api_client_async import ElementApiClientAsync

logger = logging.getLogger(__name__)


class OsgopElementUploaderAsync:
    """
    Асинхронный загрузчик PDF-файлов в Element.
    Полис → ко всем ТС
    Сведения → по соответствующему ТС
    Только если у ТС есть car_info.code
    """

    def __init__(self, api: ElementApiClientAsync):
        self.api = api

    async def upload_vehicle_pdfs(self, contract, saved_files: Dict[str, Any]):
        pdf_paths = saved_files.get("pdf", [])
        if not pdf_paths:
            return

        tasks = []

        # ---- Полис ----
        polis_bytes = Path(pdf_paths[0]).read_bytes()
        polis_name = Path(pdf_paths[0]).name

        for v in contract.vehicles:
            code = getattr(v, "code", None) or (v.car_info or {}).get("code")
            if not code:
                continue
            tasks.append(
                self.api.add_file(
                    code=code,
                    filename=polis_name,
                    file_bytes=polis_bytes,
                    filetype="polis"
                )
            )

        # ---- Сведения ----
        for idx, v in enumerate(contract.vehicles):
            if idx + 1 >= len(pdf_paths):
                break

            code = getattr(v, "code", None) or (v.car_info or {}).get("code")
            if not code:
                continue

            path = Path(pdf_paths[idx + 1])
            tasks.append(
                self.api.add_file(
                    code=code,
                    filename=path.name,
                    file_bytes=path.read_bytes(),
                    filetype="svedeniya"
                )
            )

        await asyncio.gather(*tasks, return_exceptions=True)