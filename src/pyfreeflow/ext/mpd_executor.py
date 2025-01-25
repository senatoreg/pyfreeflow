from .types import FreeFlowExt
import asyncio
import aiofiles
import re
import logging
from ..utils import EnvVarParser, asyncio_run

__TYPENAME__ = "MpdExecutor"


#
# Connection Pool
#
class ConnectionPool():
    CLIENT = {}
    POOL = {}
    LOCK = asyncio.Lock()
    LOGGER = logging.getLogger(".".join([__name__, "ConnectionPool"]))

    @classmethod
    def register(cls, client_name, conninfo, extension=[], max_size=4):
        if client_name not in cls.CLIENT.keys():
            cls.CLIENT[client_name] = {
                "conninfo": conninfo,
                "lock": asyncio.BoundedSemaphore(max_size)}

            if client_name not in cls.POOL.keys():
                cls.POOL[client_name] = asyncio.Queue()

    @classmethod
    async def unregister(cls, client_name):
        if client_name not in cls.CLIENT.keys():
            return

        lock = cls.CLIENT[client_name]["lock"]
        await lock.acquire()
        try:
            while not cls.POOL[client_name].empty():
                conn = await cls.POOL[client_name].get()
                wr = conn.get("writer")

                wr.write("close\n".encode("utf-8"))
                await wr.drain()

                wr.close()
                await wr.wait_closed()
        except Exception as ex:
            lock.release()
            raise ex

        lock.release()
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
        except Exception as ex:
            lock.release()
            raise ex

        conninfo = cls.CLIENT[client_name]["conninfo"]

        if conninfo.get("path") is not None:
            sock = await aiofiles.open(conninfo.get("path"))
            return {"reader": sock, "writer": sock}
        else:
            rd, wr = await asyncio.open_connection(host=conninfo.get("host"),
                                                   port=conninfo.get("port"))
            res = await rd.read(1000)
            ok = re.search(r'^OK'.encode("utf-8"), res) is not None
            if ok:
                return {"reader": rd, "writer": wr}
            wr.close()
            await wr.wait_closed()
            raise Exception("cannot connect to mpd")

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
            wr = conn.get("writer")
            wr.write("close\n".encode("utf-8"))
            await wr.drain()
            wr.close()
            await wr.wait_closed()

    @staticmethod
    async def is_alive(conn):
        try:
            print("is alive A")
            wr = conn.get("writer")
            wr.write("currentsong\n".encode("utf-8"))
            print("is alive B")
            await wr.drain()
            print("is alive C")
            rd = conn.get("reader")
            print("is alive D")
            r = await rd.read(1000)
            print("is alive E")
            print(r)
            return re.search(r'\nOK$'.encode("utf-8"), r)
        except Exception:
            return False


#
# SqLite Executor
#
class MpdExecutorV1_0(FreeFlowExt):
    __typename__ = __TYPENAME__
    __version__ = "1.0"

    def __init__(self, name, path=None, host="localhost", port=6600, param={},
                 max_connections=4, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        assert (path is not None or (host is not None and port is not None))
        self._conninfo = {
            "path": EnvVarParser.parse(path),
            "host": EnvVarParser.parse(host),
            "port": EnvVarParser.parse(port),
        }

        for k, v in param.items():
            self._conninfo[k] = EnvVarParser.parse(v)

        ConnectionPool.register(self._name, self._conninfo,
                                max_size=max_connections)

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

        self._action = {
            "add": self._add,
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await ConnectionPool.unregister(self._name)

    def __del__(self):
        pass
        # asyncio_run(ConnectionPool.unregister(self._name), force=True)

    async def _add(self, data, conn):
        uri = data.get("uri")

        if uri is not None:
            req = "add \"" + uri + "\"" + "\n"
            wr = conn.get("writer")
            wr.write(req.encode("utf-8"))
            await wr.drain()
            res = await conn.get("reader").read(1000)
            return (re.search(r'^OK'.encode("utf-8"), res) is not None,
                    res.decode("utf-8"))

    async def do(self, state, data):
        rs = {"result": None}
        rc = 0

        try:
            conn = await ConnectionPool.get(self._name)
        except Exception as ex:
            self._logger.error(ex)
            return state, (rs, 101)

        try:
            op = data.get("op")
            if op is None:
                return state, (rs, rc)

            a, b = await self._action[op](data, conn)
            rs["result"] = b
            rc = 0 if a else 103
        except Exception as ex:
            rc = 102
            self._logger.error(ex)
        finally:
            await ConnectionPool.release(self._name, conn)

        return state, (rs, rc)
