from .types import FreeFlowExt
import aiosqlite
import asyncio
import logging
from ..utils import EnvVarParser

__TYPENAME__ = "SqLiteExecutor"


#
# Connection Pool
#
class ConnectionPool():
    CLIENT = {}
    POOL = {}
    LOCK = asyncio.Lock()
    LOGGER = logging.getLogger(".".join([__name__, "ConnectionPool"]))

    @classmethod
    def registered(cls, client_name):
        return client_name in cls.CLIENT.keys()

    @classmethod
    def register(cls, client_name, conninfo, pragma={}, extension=[],
                 max_size=4):
        if client_name not in cls.CLIENT.keys():
            cls.CLIENT[client_name] = {
                "conninfo": conninfo,
                "pragma": pragma,
                "extension": extension,
                "lock": asyncio.BoundedSemaphore(max_size)}

            if client_name not in cls.POOL.keys():
                cls.POOL[client_name] = asyncio.Queue()

    @classmethod
    async def unregister(cls, client_name):
        if client_name not in cls.CLIENT.keys():
            return

        lock = cls.CLIENT[client_name]["lock"]
        try:
            while not cls.POOL[client_name].empty():
                await lock.acquire()
                cls.LOGGER.debug("UNREGISTER {} Lock[{}/{}/{}] Queue[{}]".format(
                    client_name, len(lock._waiters) if lock._waiters else 0,
                    lock._value, lock._bound_value,
                    cls.POOL[client_name].qsize()))
                conn = await cls.POOL[client_name].get()
                await conn.close()
        except aiosqlite.Error as ex:
            # lock.release()
            raise ex

        # lock.release()
        del cls.CLIENT[client_name]
        del cls.POOL[client_name]

    @classmethod
    async def get(cls, client_name):
        if client_name not in cls.CLIENT.keys():
            return None

        lock = cls.CLIENT[client_name]["lock"]
        cls.LOGGER.debug("GET {} Lock[{}/{}/{}] Queue[{}]".format(
            client_name, len(lock._waiters) if lock._waiters else 0,
            lock._value, lock._bound_value,
            cls.POOL[client_name].qsize()))
        await lock.acquire()

        try:
            while not cls.POOL[client_name].empty():
                conn = await cls.POOL[client_name].get()
                if await cls.is_alive(conn):
                    return conn
        except aiosqlite.Error as ex:
            lock.release()
            raise ex

        conninfo = cls.CLIENT[client_name]["conninfo"]

        db = await aiosqlite.connect(**conninfo)
        db.text_factory = lambda x: x.decode(errors='ignore')

        # default check foreign keys
        await db.execute("PRAGMA foreign_keys = ON;")

        for pragma_name, pragma_value in cls.CLIENT[client_name]["pragma"].items():
            await db.execute("PRAGMA {n} = {v};".format(
                n=pragma_name, v=pragma_value))

        await db.enable_load_extension(True)
        for ext in cls.CLIENT[client_name]["extension"]:
            await db.load_extension(ext)
        return db

    @classmethod
    async def release(cls, client_name, conn):
        if client_name in cls.CLIENT.keys():
            lock = cls.CLIENT[client_name]["lock"]
            await cls.POOL[client_name].put(conn)
            # await conn.close()
            lock.release()
            cls.LOGGER.debug("RELEASE {} Lock[{}/{}/{}] Queue[{}]".format(
                client_name, len(lock._waiters) if lock._waiters else 0,
                lock._value, lock._bound_value,
                cls.POOL[client_name].qsize()))
        else:
            await conn.close()

    @staticmethod
    async def is_alive(conn):
        try:
            async with conn.cursor() as cur:
                cur = await cur.execute("SELECT 1;")
                d = await cur.fetchall()
                del d
                # await conn.commit()
            return True
        except aiosqlite.Error:
            return False


#
# SqLite Executor
#
class SqLiteExecutorV1_0(FreeFlowExt):
    __typename__ = __TYPENAME__
    __version__ = "1.0"

    def __init__(self, name, path, statement=None, param={}, pragma={},
                 extension=[], max_connections=4, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        self._conninfo = {"database": EnvVarParser.parse(path)}
        for k, v in param.items():
            self._conninfo[k] = EnvVarParser.parse(v)

        self._stm = statement
        assert (self._stm is not None)

        ConnectionPool.register(self._name, self._conninfo,
                                {k: EnvVarParser.parse(v) for k, v in pragma.items()},
                                [EnvVarParser.parse(x) for x in extension],
                                max_size=max_connections)

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await ConnectionPool.unregister(self._name)

    def __del__(self):
        if ConnectionPool.registered(self._name):
            self._logger.warning("object deleted before calling its fini()")

    async def fini(self):
        await ConnectionPool.unregister(self._name)

    async def do(self, state, data):
        if self._stm is None:
            return state, (data, 101)

        rs = {"resultset": []}
        rc = 0

        try:
            conn = await ConnectionPool.get(self._name)
        except aiosqlite.Error as ex:
            self._logger.error(ex)
            return state, (rs, 101)

        try:
            async with conn.cursor() as cur:
                value = data.get("value")
                placeholder = data.get("placeholder", {})

                stm = self._stm.format(**placeholder)
                self._logger.debug(f"executing statement: {stm}")

                if value is not None:
                    if value and isinstance(value, list) and len(value) > 0:
                        await cur.executemany(stm, value)
                    elif value and isinstance(value, dict) and len(value) > 0:
                        await cur.execute(stm, value)
                else:
                    await cur.execute(stm)

                if cur.description:
                    rs["resultset"] = await cur.fetchall()

                await conn.commit()
        except aiosqlite.Error as ex:
            rc = 102
            await conn.rollback()
            self._logger.error(ex)
        finally:
            await ConnectionPool.release(self._name, conn)

        return state, (rs, rc)
