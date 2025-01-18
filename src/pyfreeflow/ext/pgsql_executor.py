from .types import FreeFlowExt
import psycopg
import asyncio
from cryptography.fernet import Fernet
import logging

__TYPENAME__ = "PgSqlExecutor"


#
# Connection Pool
#
class ConnectionPool():
    CLIENT = {}
    POOL = {}
    LOCK = asyncio.Lock()
    LOGGER = logging.getLogger(".".join([__name__, "ConnectionPool"]))

    @classmethod
    def register(cls, client_name, conninfo, max_size=4):
        if client_name not in cls.CLIENT.keys():
            cls.CLIENT[client_name] = {
                "conninfo": conninfo,
                "lock": asyncio.BoundedSemaphore(max_size)}

            if client_name not in cls.POOL.keys():
                cls.POOL[client_name] = asyncio.Queue()

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
        except psycopg.errors.Error as ex:
            lock.release()
            raise ex

        conninfo = cls.CLIENT[client_name]["conninfo"]
        return await psycopg.AsyncConnection.connect(conninfo)

    @classmethod
    async def release(cls, client_name, conn):
        if client_name in cls.CLIENT.keys():
            lock = cls.CLIENT[client_name]["lock"]
            await cls.POOL[client_name].put(conn)
            lock.release()
            cls.LOGGER.debug("RELEASE {} Lock[{}/{}/{}] Queue[{}]".format(
                client_name, len(lock._waiters) if lock._waiters else 0,
                lock._value, lock._bound_value,
                cls.POOL[client_name].qsize()))
        else:
            conn.close()

    @staticmethod
    async def is_alive(conn):
        try:
            async with conn.cursor() as cur:
                cur = await cur.execute("SELECT 1;")
                d = await cur.fetchall()
                del d
            await conn.commit()
            return True
        except psycopg.errors.Error:
            return False


#
# PgSql Executor
#
class PgSqlExecutor(FreeFlowExt):
    __typename__ = __TYPENAME__
    __version__ = "1.0"

    CONNECTION_STRING = "postgresql://{userspec}{hostspec}{dbspec}{paramspec}"

    def __init__(self, name, username=None, password=None, secret=None,
                 host=[], dbname=None, param={}, statement=None,
                 max_connections=4, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        if secret is not None:
            with open(secret, "rb") as f:
                cipher = Fernet(f.read())
                password = cipher.decrypt(password.encode("utf-8")).decode("utf-8")

        userspec = self._conninfo_helper(username, password, sep=":")

        hostspec = ",".join(host)
        hostspec = "@" + hostspec if len(hostspec) > 0 else ""

        dbspec = self._conninfo_helper(None, dbname, sep="/")

        if "connect_timeout" not in param.keys():
            param["connect_timeout"] = 30

        paramspec = "?" + "&".join([k + "=" + str(v) for k, v in param.items()])

        self._conninfo = self.CONNECTION_STRING.format(
            userspec=userspec, hostspec=hostspec, dbspec=dbspec,
            paramspec=paramspec)
        self._stm = statement
        assert (self._stm is not None)

        ConnectionPool.register(self._name, self._conninfo,
                                max_size=max_connections)

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

    def _conninfo_helper(self, a, b, sep=":"):
        return "{a}{s}{b}".format(
            a=a if a else "",
            s=sep if b else "",
            b=b if b else ""
        )

    async def __aenter__(self):
        await self._ensure_connection()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._close()

    def __del__(self):
        pass

    async def do(self, state, data):
        if self._stm is None:
            return state, (data, 101)

        rs = {"resultset": []}
        rc = 0

        try:
            conn = await ConnectionPool.get(self._name)
        except psycopg.errors.Error as ex:
            self._logger.error(ex)
            return state, (rs, 101)

        try:
            async with conn.cursor() as cur:
                value = data.get("value")

                if value and len(value) > 1:
                    await cur.executemany(self._stm, value)
                else:
                    await cur.execute(self._stm, value)

                if cur.description:
                    rs["resultset"] = await cur.fetchall()

            await conn.commit()
        except psycopg.errors.Error as ex:
            rc = 102
            if not conn.closed:
                await conn.rollback()
            self._logger.error(ex)
        finally:
            await ConnectionPool.release(self._name, conn)

        return state, (rs, rc)
