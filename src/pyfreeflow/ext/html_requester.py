from .types import FreeFlowExt
import aiohttp
import yarl
import multidict
import json
import ssl
import asyncio
import logging
# import babel.dates
import re
import random
from ..utils import asyncio_run, MimeTypeParser, SecureXMLParser

__TYPENAME__ = "HtmlRequester"


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


class HtmlRequesterV1_0(FreeFlowExt):
    __typename__ = __TYPENAME__
    __version__ = "1.0"

    CONTENT_TYPE_PATTERN = re.compile(r';\s*')
    CONTENT_TYPE_PATTERN2 = re.compile(r'\s*=\s*')

    def __init__(self, name, url, method="GET", headers={}, timeout=300,
                 max_retries=5, max_retry_sleep=10, max_response_size=10485760,
                 sslenabled=True, insecure=False, cafile=None, capath=None,
                 cadata=None, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        self._url = url
        self._timeout = timeout
        self._max_retries = max_retries
        self._max_retry_sleep = max_retry_sleep
        self._headers = headers
        self._method = method.upper()
        self._max_resp_size = max_response_size

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
        return isinstance(config, dict) and len([k for k in config.keys()
                                                 if k in keys]) > 0

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

    def _split_mimetype(self, mimetype):
        t = self.CONTENT_TYPE_PATTERN.split(mimetype)
        m = {"type": t[0]}
        for x in t[1:]:
            kv = self.CONTENT_TYPE_PATTERN2.split(x)
            m[kv[0]] = kv[1]
        return m

    async def _try_request(self, method, url, headers, params, data):
        sleep = 1
        max_sleep = int(self._max_retry_sleep / self._max_retries)
        for i in range(1, self._max_retries + 1):
            try:
                resp = await self._session.request(
                        method, url, headers=headers, params=params, data=data,
                        ssl=self._ssl_context, allow_redirects=True)
                return resp
            except aiohttp.ClientError as ex:
                sleep = random.randint(sleep, i * max_sleep)
                self._logger.warning(
                    f"error connecting '{url}' " +
                    f"try {i}/{self._max_retries} retry in {sleep}s: {ex}")
                await asyncio.sleep(sleep)
        raise aiohttp.ClientError(f"cannot connect to {url}")

    async def _do_request(self, method, url, headers=None, params=None,
                          data=None, userdata=None):
        try:
            await self._ensure_session()

            resp = await self._try_request(method, url, headers, params, data)

            if resp.status >= 400:
                self._logger.error(f"'{url}' response code {resp.status}")
                return (
                    {"req": {}, "userdata": userdata, "headers": {},
                     "body": {}}, 102)

            content_length = int(resp.headers.get('Content-Length', 0))
            if content_length > self._max_resp_size:
                self._logger.error("response size %d exceeded max size %s",
                                   content_length, self._max_resp_size)
                resp.release()
                return (
                    {"req": {}, "userdata": userdata, "headers": {},
                     "body": {}}, 101)

            raw = await resp.read()
            if len(raw) > self._max_resp_size:
                self._logger.error(
                    "real response size %d exceeded max size %s",
                    content_length, self._max_resp_size)
                resp.release()
                return (
                    {"req": {}, "userdata": userdata, "headers": {},
                     "body": {}}, 101)

            req_info = {k: self._multidict_to_dict(v)
                        for k, v in dict(
                                resp._request_info._asdict()).items()}
            try:
                mimetype = self._split_mimetype(
                    resp.headers.get("Content-Type"))
                if MimeTypeParser.is_html(mimetype.get("type")):
                    body = SecureXMLParser.parse_string(raw.decode(
                        mimetype.get("charset", "utf-8")), html=True)
                else:
                    self._logger.warning(
                        "aiohttp request %s warning: response type '%s'",
                        url, mimetype)
                    body = {}
                resp.release()
            except Exception as ex:
                self._logger.error("feed load %s error: %s", url, ex)
                resp.release()
                return (
                    {"req": req_info, "userdata": userdata,
                     "headers": dict(resp.headers), "body": {}}, 106)

            return (
                {"req": req_info, "userdata": userdata,
                 "headers": dict(resp.headers), "body": body}, 0)

        except aiohttp.ClientError as ex:
            self._logger.error("aiohttp request %s error: %s", url, ex)
            return (
                {"req": {}, "userdata": userdata, "headers": {},
                 "body": {}}, 101)
        except asyncio.exceptions.TimeoutError as ex:
            self._logger.error("aiohttp timeout on %s error: %s", url, ex)
            return (
                {"req": {}, "userdata": userdata, "headers": {},
                 "body": {}}, 104)

    async def _do_get(self, state, data):
        headers = self._headers | data.get("headers", {})

        url = self._url.format(**data.get("urlcomp", {}))
        query_params = data.get("body", {})

        return await self._do_request(
            "GET", url, headers=headers, params=query_params,
            userdata=data.get("userdata"))

    async def _do_post(self, state, data):
        headers = self._headers | data.get("headers", {})

        url = self._url.format(**data.get("urlcomp", {}))
        body_bytes = json.dumps(data.get("body", {})).encode("utf-8")

        return await self._do_request("POST", url, headers=headers,
                                      data=body_bytes,
                                      userdata=data.get("userdata"))

    async def do(self, state, data):
        if isinstance(data, dict):
            rval = await self._method_op[self._method](state, data)
            return state, rval
        self._logger.error("Bad request expected 'dict' got '{}'".format(type(data)))
        return (state, ({"headers": {}, "body": {}}, 103))
