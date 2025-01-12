from .types import FreeFlowExt
import aiohttp
import yarl
import multidict
import json
import ssl
import asyncio
import logging
from ..utils import asyncio_run

__TYPENAME__ = "RestApiRequester"


"""
run parameter:
{
  "state": { ... },
  "data": {
    "headers": {},
    "body": {}
  }
}
"""


class RestApiRequesterV1_0(FreeFlowExt):
    __typename__ = __TYPENAME__
    __version__ = "1.0"

    def __init__(self, name, url, method="GET", headers={}, timeout=300,
                 sslenabled=True, insecure=False, cafile=None, capath=None,
                 cadata=None, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        self._url = url
        self._timeout = timeout
        self._headers = headers
        self._method = method.upper()

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

        if sslenabled:
            self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            if insecure:
                self._ssl_context.check_hostname = False
                self._ssl_context.verify_mode = ssl.CERT_NONE
            else:
                self._ssl_context.check_hostname = True
                self._ssl_context.verify_mode = ssl.CERT_REQUIRED
            if cafile or capath or cadata:
                self._ssl_context.load_verify_locations(
                    cafile=cafile, capath=capath, cadata=cadata)
        else:
            self._ssl_context = None

        self._session = None

        self._method_op = {
            "GET": self._do_get,
            "POST": self._do_post,
        }

    def __str__(self):
        return "{typ}(name: {n}, version: {v}, url: {u}, headers: {h}, timeout: {tm_out})".format(
            typ=self.__typename__, n=self._name, v=self.__version__,
            u=self._url, h=self._headers, tm_out=self._timeout)

    def _validate_ssl_ca_config(self, config):
        keys = ["file", "path", "data"]
        return isinstance(config, dict) and len([k for k in config.keys() if k in keys]) > 0

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            t = aiohttp.ClientTimeout(total=self._timeout)
            self._session = aiohttp.ClientSession(timeout=t)

    async def _close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self):
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._close()

    def __del__(self):
        asyncio_run(self._close(), force=True)

    def _multidict_to_dict(self, x):
        if isinstance(x, yarl.URL):
            return str(x)
        elif isinstance(x, multidict._multidict.CIMultiDictProxy):
            return dict(x)
        return x

    def _prepare_request_p(self, x):
        return {k: str(v) for k, v in x.items()} if x is not None else x

    async def _do_request(self, method, url, headers=None, params=None,
                          data=None):
        try:
            await self._ensure_session()

            async with self._session.request(
                    method, url, headers=headers, params=params, data=data,
                    ssl=self._ssl_context, allow_redirects=True) as resp:

                if resp.status >= 400:
                    self._logger.error(f"'{url}' response code {resp.status}")
                    return (
                        {"req": {}, "headers": {}, "body": {}}, 102)

                raw = await resp.read()

                try:
                    body = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    body = raw.decode("utf-8")  # oppure tenere i bytes
            req_info = {k: self._multidict_to_dict(v)
                        for k, v in dict(resp._request_info._asdict()).items()}
            return (
                {"req": req_info, "headers": dict(resp.headers), "body": body}, 0)

        except aiohttp.ClientError as ex:
            self._logger.error("aiohttp request error %s", ex)
            return (
                {"req": {}, "headers": {}, "body": {}}, 101)
        except asyncio.exceptions.TimeoutError as ex:
            self._logger.error("aiohttp timeout on %s error %s", url, ex)
            return (
                {"req": {}, "headers": {}, "body": {}}, 104)

    async def _do_get(self, state, data):
        headers = self._headers | data.get("headers", {})

        url = self._url.format(**data.get("urlcomp", {}))
        query_params = data.get("body", {})

        return await self._do_request(
            "GET", url, headers=headers, params=query_params)

    async def _do_post(self, state, data):
        headers = self._headers | data.get("headers", {})

        url = self._url.format(**data.get("urlcomp", {}))
        body_bytes = json.dumps(data.get("body", {})).encode("utf-8")

        return await self._do_request("POST", url, headers=headers,
                                      data=body_bytes)

    async def do(self, state, data):
        if isinstance(data, dict):
            rval = await self._method_op[self._method](state, data)
            return state, rval
        self._logger.error("Bad request expected 'dict' got '{}'".format(type(data)))
        return (state, ({"headers": {}, "body": {}}, 103))
