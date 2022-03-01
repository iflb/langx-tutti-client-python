from typing import Optional
import hashlib

class ScattController:
    def __init__(self, duct):
        self._duct = duct

    async def open(self, wsd_url: str):
        await self._duct.open(wsd_url)
