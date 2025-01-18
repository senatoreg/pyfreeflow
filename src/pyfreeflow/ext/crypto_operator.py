from .types import FreeFlowExt
from cryptography.fernet import Fernet
import aiofiles
import logging

__TYPENAME__ = "{}CryptoOperator"


class FernetCryptoOperator(FreeFlowExt):
    __typename__ = __TYPENAME__.format("Fernet")
    __version__ = "1.0"

    def __init__(self, name, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))
        self._action = {
            "decrypt": self._dec,
            "encrypt": self._enc,
        }

    async def _read_key(self, path):
        async with aiofiles.open(path, "rb") as f:
            key = await f.read()

        return Fernet(key)

    async def _enc(self, data, key):
        cipher = await self._read_key(key)
        d = cipher.encrypt(data.encode("utf-8"))
        return d.decode("utf-8"), 0

    async def _dec(self, data, key):
        cipher = await self._read_key(key)
        d = cipher.decrypt(data.encode("utf-8"))
        return d.decode("utf-8"), 0

    async def do(self, state, data):
        op = data.get("op", "decrypt")
        key = data.get("key")
        raw = data.get("data", "")
        rval = await self._action[op](raw, key)
        return state, rval
