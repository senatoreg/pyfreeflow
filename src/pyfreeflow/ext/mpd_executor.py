from .types import FreeFlowExt
import asyncio
import re
import logging
from ..utils import EnvVarParser

__TYPENAME__ = "MpdExecutor"


SOCKET = 0
FILE = 1


class MpdConnection():
    OK = re.compile(r'^OK MPD [0-9\.]+$'.encode("utf-8")).search

    @classmethod
    async def write(cls, conn, data, buffer_size):
        if conn.get("type") == SOCKET:
            wr = conn.get("writer")
            wr.write(data.encode("utf-8"))
            await wr.drain()
            return await conn.get("reader").read(buffer_size)

    @classmethod
    async def close(cls, conn):
        close_cmd = "close\n".encode("utf-8")
        wr = conn.get("writer")
        wr.write(close_cmd)
        await wr.drain()
        wr.close()
        await wr.wait_closed()

    @classmethod
    async def open(cls, conninfo):
        if conninfo.get("path") is not None:
            rd, wr = await asyncio.open_unix_connection(
                path=conninfo.get("path"))
        else:
            rd, wr = await asyncio.open_connection(
                host=conninfo.get("host"),
                port=conninfo.get("port"))
        res = await rd.read(10000)
        print(rd, wr, res)
        if cls.OK(res) is not None:
            return {"reader": rd, "writer": wr, "type": SOCKET}
        wr.close()
        await wr.wait_closed()
        raise Exception("cannot connect to mpd")


#
# Connection Pool
#
class ConnectionPool():
    OK = re.compile(r'OK$'.encode("utf-8")).search

    CLIENT = {}
    POOL = {}
    LOCK = asyncio.Lock()
    LOGGER = logging.getLogger(".".join([__name__, "ConnectionPool"]))

    @classmethod
    def registered(cls, client_name):
        return client_name in cls.CLIENT.keys()

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
                await MpdConnection.close(conn)
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
        return await MpdConnection.open(conninfo)

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
            await MpdConnection.close(conn)

    @staticmethod
    async def is_alive(conn):
        try:
            wr = conn.get("writer")
            wr.write("currentsong\n".encode("utf-8"))
            await wr.drain()
            rd = conn.get("reader")
            r = await rd.read(1000)
            return re.search(r'\nOK$'.encode("utf-8"), r)
        except Exception:
            return False


#
# SqLite Executor
#
class MpdExecutorV1_0(FreeFlowExt):
    __typename__ = __TYPENAME__
    __version__ = "1.0"

    OK = re.compile(r'OK$'.encode("utf-8")).search
    LINER = re.compile(r'\n').split
    FIELD_SEP = re.compile(r'^([\w\d]+): *(.*)').match
    UNQUOTE = re.compile(r'"')

    def __init__(self, name, path=None, host="localhost", port=6600, param={},
                 max_buffer=10*1024*1024, max_connections=4, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        assert (path is not None or (host is not None and port is not None))
        self._conninfo = {
            "path": EnvVarParser.parse(path),
            "host": EnvVarParser.parse(host),
            "port": EnvVarParser.parse(port),
        }

        self._max_buffer = max_buffer

        for k, v in param.items():
            self._conninfo[k] = EnvVarParser.parse(v)

        ConnectionPool.register(self._name, self._conninfo,
                                max_size=max_connections)

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

        self._action = {
            "add": self._add,
            "playlist": self._playlist,
            "playlistsearch": self._playlistsearch,
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await ConnectionPool.unregister(self._name)

    def __del__(self):
        if ConnectionPool.registered(self._name):
            self._logger.warning("object deleted before calling its fini()")

    async def fini(self):
        await ConnectionPool.unregister(self._name)

    async def _send(self, cmd, conn):
        try:
            wr = conn.get("writer")
            wr.write((cmd + "\n").encode("utf-8"))
            await wr.drain()
            res = await conn.get("reader").read(self._max_buffer)
            return (self.OK(res) is not None, res.decode("utf-8"))
        except Exception as ex:
            self._logger.error(ex)
            return (False, None)

    async def _add(self, data, conn):
        uri = data.get("uri")
        pos = data.get("pos", "")

        if uri is not None:
            req = "add \"" + uri + "\" " + pos
            return await self._send(req, conn)
        return (True, {})

    async def _playlistsearch(self, data, conn):
        f = data.get("filter")

        if f is not None:
            req = "playlistsearch \"" + self.UNQUOTE.sub(r"\\\"", f) + "\""
            completed, track = await self._send(req, conn)
            t = {}
            if completed and len(track) > 2:
                for e in self.LINER(track)[:-2]:
                    info = self.FIELD_SEP(e)
                    t[info.group(1)] = info.group(2)
            return (completed, t)
        return (True, {})

    async def _playlist(self, data, conn):
        completed, plist = await self._send("playlist", conn)
        li = []
        for e in self.LINER(plist)[:-2]:
            f = self.FIELD_SEP(e)
            li.append(f.group(2))
        return (completed, li)

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

            completed, res = await self._action[op](data, conn)
            rs["result"] = res
            rc = 0 if completed else 103
        except Exception as ex:
            rc = 102
            self._logger.error(ex)
        finally:
            await ConnectionPool.release(self._name, conn)

        return state, (rs, rc)
