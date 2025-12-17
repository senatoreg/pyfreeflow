from .types import FreeFlowExt
import aiohttp
import yarl
import multidict
import json
import ssl
import asyncio
import logging
# import babel.dates
import urllib.parse
import re
import random
from ..utils import MimeTypeParser, SecureXMLParser, DateParser

__TYPENAME__ = "FeedRequester"


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


class FeedTagParser():
    @classmethod
    def parse_content(cls, a, b):
        content = SecureXMLParser.get_elem(a, b, "attrs")
        if isinstance(content, list):
            return [{"url": x.get("url"), "type": x.get("type")} for x in content]
        return [{"url": content.get("url"), "type": content.get("type")}]

    @classmethod
    def parse_enclosure(cls, a, b):
        return cls.parse_content(a, b)

    @classmethod
    def join_if(cls, a, sep=", "):
        return sep.join(a) if isinstance(a, list) else a

    @classmethod
    def tolist_if(cls, a):
        if not isinstance(a, (list, tuple)):
            a = [a]
        return [x for x in a if x is not None]

    @classmethod
    def get_attr(cls, a, b):
        if isinstance(a, list):
            return [x.get(b) for x in a]
        return [a.get(b)]

    @classmethod
    def get_atom_link(cls, a, b):
        if isinstance(a, list):
            return [{"href": x.get(b), "rel": x.get("rel", "alternate")} for x in a]
        return [{"href": a.get(b), "rel": a.get("rel", "alternate")}]


class FeedTagDefinition():
    RSS20_TAG = {
        "channel": lambda a: (None, SecureXMLParser.get_elem(
            a, ["channel"], "elem")),
        "item": lambda a: ("entry", SecureXMLParser.get_elem(
            a, ["item"])),
        "title": lambda a: ("title", SecureXMLParser.get_elem(
            a, ["title"], "text")),
        "link": lambda a: ("link", [{"href": SecureXMLParser.get_elem(
            a, ["link"], "text"), "rel": "alternate"}]),
        "description": lambda a: ("description", SecureXMLParser.get_elem(
            a, ["description"], "text")),
        "language": lambda a: ("language", SecureXMLParser.get_elem(
            a, ["language"], "text")),
        "copyright": lambda a: ("copyright", SecureXMLParser.get_elem(
            a, ["copyright"], "text")),
        "managingEditor": lambda a: ("copyright", SecureXMLParser.get_elem(
            a, ["managingEditor"], "text")),
        "webMaster": lambda a: ("webMaster", SecureXMLParser.get_elem(
            a, ["webMaster"], "text")),
        "pubDate": lambda a: ("published", DateParser.parse_date(
            SecureXMLParser.get_elem(a, ["pubDate"], "text"))),
        "pubdate": lambda a: ("published", DateParser.parse_date(
            SecureXMLParser.get_elem(a, ["pubdate"], "text"))),
        "lastBuildDate": lambda a: ("updated", DateParser.parse_date(
            SecureXMLParser.get_elem(a, ["lastBuildDate"], "text"))),
        "lastbuilddate": lambda a: ("updated", DateParser.parse_date(
            SecureXMLParser.get_elem(a, ["lastBuildDate"], "text"))),
        "category": lambda a: ("category", SecureXMLParser.get_elem(
            a, ["category"], "text")),
        "generator": lambda a: ("generator", SecureXMLParser.get_elem(
            a, ["generator"], "text")),
        "docs": lambda a: ("docs", SecureXMLParser.get_elem(
            a, ["docs"], "text")),
        "cloud": lambda a: ("cloud", SecureXMLParser.get_elem(
            a, ["cloud"], "text")),
        "ttl": lambda a: ("ttl", SecureXMLParser.get_elem(a, ["ttl"], "text")),
        "image": lambda a: ("image", SecureXMLParser.get_elem(
            a, ["image"], "text")),
        "enclosure": lambda a: ("media", FeedTagParser.parse_enclosure(
            a, ["enclosure"])),
        "rating": lambda a: ("rating", SecureXMLParser.get_elem(
            a, ["rating"], "text")),
        "textInput": lambda a: ("textInput", SecureXMLParser.get_elem(
            a, ["textInput"], "text")),
        "skipHours": lambda a: ("skipHours", SecureXMLParser.get_elem(
            a, ["skipHours"], "text")),
        "skipDays": lambda a: ("skipDays", SecureXMLParser.get_elem(
            a, ["skipDays"], "text")),
        "author": lambda a: ("author", FeedTagParser.tolist_if(
            SecureXMLParser.get_elem(a, ["author"], "text"))),
        "comments": lambda a: ("comments", SecureXMLParser.get_elem(
            a, ["comments"], "text")),
        "guid": lambda a: ("guid", SecureXMLParser.get_elem(
            a, ["guid"], "text")),
        "source": lambda a: ("source", SecureXMLParser.get_elem(
            a, ["source"], "text")),
    }

    ATOM_TAG = {
        "{http://www.w3.org/2005/Atom}link": lambda a: (
            "link", FeedTagParser.get_atom_link(
                SecureXMLParser.get_elem(
                    a, ["{http://www.w3.org/2005/Atom}link"], "attrs"),
                "href")),
        "{http://www.w3.org/2005/Atom}id": lambda a: (
            "id", SecureXMLParser.get_elem(
                a, ["{http://www.w3.org/2005/Atom}id"], "text")),
        "{http://www.w3.org/2005/Atom}title": lambda a: (
            "title", SecureXMLParser.get_elem(
                a, ["{http://www.w3.org/2005/Atom}title"], "text")),
        "{http://www.w3.org/2005/Atom}content": lambda a: (
            "content", SecureXMLParser.get_elem(
                a, ["{http://www.w3.org/2005/Atom}content"], "text")),
        "{http://www.w3.org/2005/Atom}author": lambda a:
        ("author", FeedTagParser.tolist_if(SecureXMLParser.get_elem(a, [
            "{http://www.w3.org/2005/Atom}author",
            "{http://www.w3.org/2005/Atom}name"], "text"))),
        "{http://www.w3.org/2005/Atom}published": lambda a: (
            "published", DateParser.parse_date(SecureXMLParser.get_elem(
                a, ["{http://www.w3.org/2005/Atom}published"], "text"))),
        "{http://www.w3.org/2005/Atom}updated": lambda a: (
            "updated", DateParser.parse_date(SecureXMLParser.get_elem(
                a, ["{http://www.w3.org/2005/Atom}updated"], "text"))),
        "{http://www.w3.org/2005/Atom}entry": lambda a: (
            "entry", SecureXMLParser.get_elem(
                a, ["{http://www.w3.org/2005/Atom}entry"])),
    }

    ITUNES_TAG = {
        "{http://www.itunes.com/dtds/podcast-1.0.dtd}author": lambda a: (
            "author", FeedTagParser.tolist_if(SecureXMLParser.get_elem(
                a, ["{http://www.itunes.com/dtds/podcast-1.0.dtd}author"],
                "text"))),
        "{http://www.itunes.com/dtds/podcast-1.0.dtd}summary": lambda a: (
            "description", SecureXMLParser.get_elem(
                a, ["{http://www.itunes.com/dtds/podcast-1.0.dtd}summary"],
                "text")),
        "{http://www.itunes.com/dtds/podcast-1.0.dtd}category": lambda a: (
            "category", SecureXMLParser.get_elem(
                a, ["{http://www.itunes.com/dtds/podcast-1.0.dtd}category"],
                "text")),
        "{http://www.itunes.com/dtds/podcast-1.0.dtd}title": lambda a: (
            "title", SecureXMLParser.get_elem(
                a, ["{http://www.itunes.com/dtds/podcast-1.0.dtd}title"],
                "text")),
    }

    MEDIA_RSS_TAG = {
        "{http://search.yahoo.com/mrss}group": lambda a: (
            "group", SecureXMLParser.get_elem(
                a, ["{http://search.yahoo.com/mrss}group"])),
        "{http://search.yahoo.com/mrss}credit": lambda a: (
            "author", FeedTagParser.tolist_if(SecureXMLParser.get_elem(
                a, ["{http://search.yahoo.com/mrss}credit"],
                "text"))),
        "{http://search.yahoo.com/mrss}description": lambda a: (
            "description", SecureXMLParser.get_elem(
                a, ["{http://search.yahoo.com/mrss}description"],
                "text")),
        "{http://search.yahoo.com/mrss}content": lambda a: (
            "media", FeedTagParser.parse_content(
                a, ["{http://search.yahoo.com/mrss}content"])),
        "{http://search.yahoo.com/mrss}category": lambda a: (
            "category", SecureXMLParser.get_elem(
                a, ["{http://search.yahoo.com/mrss}category"],
                "text")),
        "{http://search.yahoo.com/mrss}comments": lambda a: (
            "comments", SecureXMLParser.get_elem(
                a, ["{http://search.yahoo.com/mrss}comments"],
                "text")),
        "{http://search.yahoo.com/mrss}title": lambda a: (
            "title", SecureXMLParser.get_elem(
                a, ["{http://search.yahoo.com/mrss}title"],
                "text")),
    }

    RSS10_TAG = {
        "{http://purl.org/rss/1.0}channel": lambda a: (
            None, SecureXMLParser.get_elem(
                a, ["{http://purl.org/rss/1.0}channel"])),
        "{http://purl.org/rss/1.0}title": lambda a: (
            "title", SecureXMLParser.get_elem(
                a, ["{http://purl.org/rss/1.0}title"], "text")),
        "{http://purl.org/rss/1.0}description": lambda a: (
            "description", SecureXMLParser.get_elem(
                a, ["{http://purl.org/rss/1.0}description"], "text")),
        "{http://purl.org/rss/1.0}item": lambda a: (
            "entry", SecureXMLParser.get_elem(
                a, ["{http://purl.org/rss/1.0}item"])),
        "{http://purl.org/rss/1.0}link": lambda a: (
            "link", [{"href": SecureXMLParser.get_elem(
                a, ["{http://purl.org/rss/1.0}link"], "text"),
                      "rel": "alternate"}]),
    }

    RSS10_CONTENT_TAG = {
        "{http://purl.org/rss/1.0/modules/content}encoded": lambda a: (
            "content", SecureXMLParser.get_elem(
                a, ["{http://purl.org/rss/1.0/modules/content}encoded"],
                "text")),
    }

    DCMI_TAG = {
        "{http://purl.org/dc/elements/1.1}creator": lambda a: (
            "author", FeedTagParser.tolist_if(SecureXMLParser.get_elem(
                a, ["{http://purl.org/dc/elements/1.1}creator"],
                "text"))),
        "{http://purl.org/dc/elements/1.1}date": lambda a: (
            "published", DateParser.parse_date(SecureXMLParser.get_elem(
                a, ["{http://purl.org/dc/elements/1.1}date"], "text"))),
        "{http://purl.org/dc/elements/1.1}description": lambda a: (
            "description", SecureXMLParser.get_elem(
                a, ["{http://purl.org/dc/elements/1.1}description"], "text")),
        "{http://purl.org/dc/elements/1.1}type": lambda a: (
            "type", SecureXMLParser.get_elem(
                a, ["{http://purl.org/dc/elements/1.1}type"], "text")),
        "{http://purl.org/dc/elements/1.1}language": lambda a: (
            "language", SecureXMLParser.get_elem(
                a, ["{http://purl.org/dc/elements/1.1}language"], "text")),
        "{http://purl.org/dc/elements/1.1}publisher": lambda a: (
            "managingEditor", SecureXMLParser.get_elem(
                a, ["{http://purl.org/dc/elements/1.1}publisher"], "text")),
        "{http://purl.org/dc/elements/1.1}rights": lambda a: (
            "copyright", SecureXMLParser.get_elem(
                a, ["{http://purl.org/dc/elements/1.1}rights"], "text")),
        "{http://purl.org/dc/elements/1.1}source": lambda a: (
            "source", SecureXMLParser.get_elem(
                a, ["{http://purl.org/dc/elements/1.1}source"], "text")),
        "{http://purl.org/dc/elements/1.1}title": lambda a: (
            "title", SecureXMLParser.get_elem(
                a, ["{http://purl.org/dc/elements/1.1}title"], "text")),
        "{http://purl.org/dc/elements/1.1}subject": lambda a: (
            "category", SecureXMLParser.get_elem(
                a, ["{http://purl.org/dc/elements/1.1}subject"], "text")),
    }


class FeedRequesterV1_0(FreeFlowExt):
    __typename__ = __TYPENAME__
    __version__ = "1.0"

    TAG_MATCHING_RE = re.compile(r"{(https?://[-a-zA-Z0-9@%._\+\-]+:?[0-9]*/?"
                                 + r"[a-zA-Z0-9_\.\+\-@%#&?/]*)/}"
                                 + r"([a-zA-Z0-9_\.\+\-@%#&?]*)")
    RSS20_TAG = FeedTagDefinition.RSS20_TAG | FeedTagDefinition.ITUNES_TAG | \
        FeedTagDefinition.MEDIA_RSS_TAG | FeedTagDefinition.RSS10_CONTENT_TAG | \
        FeedTagDefinition.DCMI_TAG | FeedTagDefinition.ATOM_TAG

    ATOM_TAG = FeedTagDefinition.ATOM_TAG | FeedTagDefinition.MEDIA_RSS_TAG

    RDF_TAG = FeedTagDefinition.RSS10_TAG | FeedTagDefinition.RSS10_CONTENT_TAG \
        | FeedTagDefinition.DCMI_TAG

    CONTENT_TYPE_PATTERN = re.compile(r";\s*")
    CONTENT_TYPE_PATTERN2 = re.compile(r"\s*=\s*")

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

    async def _on_request_start(
            self, session, trace_config_ctx, params):
        trace_config_ctx.url = params.url
        trace_config_ctx.start = asyncio.get_event_loop().time()

    async def _on_request_end(self, session, trace_config_ctx, params):
        elapsed = asyncio.get_event_loop().time() - trace_config_ctx.start
        self._logger.debug("Request to {} took {} s".format(trace_config_ctx.url,
                                                          elapsed))

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            t = aiohttp.ClientTimeout(total=self._timeout)
            trace_config = aiohttp.TraceConfig()
            trace_config._on_request_start(self._on_request_start)
            trace_config._on_request_end(self._on_request_end)
            self._session = aiohttp.ClientSession(
                trace_configs=[trace_config], timeout=t)

    async def _close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self):
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._close()

    def __del__(self):
        if self._session and not self._session.closed:
            self._logger.warning("object deleted before calling its fini()")

    async def fini(self):
        await self._close()

    def _multidict_to_dict(self, x):
        if isinstance(x, yarl.URL):
            return urllib.parse.unquote(str(x))
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

    def _fix_cdata(self, x, encoding="utf-8"):
        CDATA_FIX = [
            lambda a: re.compile(
                br"(?s)(<title>\s*)(?!(?:\s*<!\[CDATA\[))(.*?)(\s*</title>)").sub(
                    br"\1<![CDATA[\2]]>\3", a),
            lambda a: re.compile(
                br"(?s)(<description>\s*)(?!(?:\s*<!\[CDATA\[))(.*?)(\s*</description>)").sub(
                    br"\1<![CDATA[\2]]>\3", a),
        ]

        for fix in CDATA_FIX:
            x = fix(x)
        return x

    def _sanitize_feed(self, data):
        if isinstance(data, dict):
            new_data = {}
            for k, v in data.items():
                fix = self.TAG_MATCHING_RE.match(k)
                k = "{" + fix.group(1) + "}" + fix.group(2) if fix else k
                new_data[k] = self._sanitize_feed(v)
            return new_data
        elif isinstance(data, list):
            return [self._sanitize_feed(x) for x in data]
        return data

    def _rss_parser2(self, data):
        rss = {}

        if not isinstance(data, dict) or "elem" not in data.keys():
            return data

        for tag in data.get("elem", {}).keys():
            if tag in self.RSS20_TAG.keys():
                tag_name, tag_value = self.RSS20_TAG[tag](data)
                if isinstance(tag_value, list):
                    tag_value = [self._rss_parser2(x) for x in tag_value]
                elif isinstance(tag_value, dict):
                    tag_value = self._rss_parser2(tag_value)
                if tag_value is not None:
                    rss[tag_name] = tag_value

        return rss

    def _atom_parser2(self, data):
        # print(json.dumps(data))
        atom = {}

        if not isinstance(data, dict) or "elem" not in data.keys():
            return data

        for tag in data.get("elem", {}).keys():
            if tag in self.ATOM_TAG.keys():
                tag_name, tag_value = self.ATOM_TAG[tag](data)
                if isinstance(tag_value, list):
                    tag_value = [self._atom_parser2(x) for x in tag_value]
                elif isinstance(tag_value, dict):
                    tag_value = self._atom_parser2(tag_value)
                if tag_value is not None:
                    atom[tag_name] = tag_value

        return atom

    def _rdf_parser2(self, data):
        # print(json.dumps({"DATA": data}))
        rdf = {}

        if not isinstance(data, dict) or "elem" not in data.keys():
            return data

        for tag in data.get("elem", {}).keys():
            if tag in self.RDF_TAG.keys():
                tag_name, tag_value = self.RDF_TAG[tag](data)
                if isinstance(tag_value, list):
                    tag_value = [self._rdf_parser2(x) for x in tag_value]
                elif isinstance(tag_value, dict):
                    tag_value = self._rdf_parser2(tag_value)
                if tag_value is not None:
                    if tag_name is None:
                        rdf = rdf | tag_value
                    else:
                        rdf[tag_name] = tag_value

        return rdf

    async def _try_request(self, method, url, headers, params, data):
        sleep = 0
        max_sleep = int(self._max_retry_sleep / self._max_retries)
        for i in range(1, self._max_retries + 1):
            try:
                resp = await self._session.request(
                        method, url, headers=headers, params=params, data=data,
                        ssl=self._ssl_context, allow_redirects=True)
                return resp
            except aiohttp.ClientError as ex:
                sleep = random.randint(sleep + 1, i * max_sleep)
                self._logger.warning(
                    f"error connecting '{url}' " +
                    f"try {i}/{self._max_retries} retry in {sleep}s: {ex}")
                await asyncio.sleep(sleep)
        raise aiohttp.ClientError(f"cannot connect to {url}")

    def _parse_resp(self, resp, raw, url):
        mimetype = self._split_mimetype(
            resp.headers.get("Content-Type"))

        encoding = mimetype.get("charset", "utf-8")

        VALUE_MALFORMED_RE = [
            (re.compile(br"<\?(=|php)?[\w\d\s_();]*\?>"), b""),
        ]

        # raw = re.sub(r"\s*\n\s*".encode(encoding), r"".encode(encoding), raw)
        #raw = self._fix_cdata(raw, encoding)

        for expr in VALUE_MALFORMED_RE:
            raw = expr[0].sub(expr[1], raw)

        if MimeTypeParser.is_xml(mimetype.get("type")) or (
                MimeTypeParser.is_html(mimetype.get("type")) and
                raw[:5] == b'<?xml'):
            try:
                body = SecureXMLParser.parse_bytes(raw)
            except:
                self._logger.warning(
                    "parsing error trying to fix cdata for %s",
                    url)
                raw = self._fix_cdata(raw, encoding)
                body = SecureXMLParser.parse_bytes(raw)
        else:
            self._logger.warning(
                "aiohttp request %s warning: response type '%s'",
                url, mimetype)
            body = {}
        return body

    async def _do_request(self, method, url, headers=None, params=None,
                          data=None):
        try:
            await self._ensure_session()
            resp = await self._try_request(method, url, headers, params, data)
            redirect = [urllib.parse.unquote(str(x.url)) for x in resp.history]

            if resp.status >= 400:
                self._logger.error(f"'{url}' response code {resp.status}")
                resp.release()
                return (
                    {"req": {}, "redirect": redirect, "headers": {},
                     "body": {}}, 102)

            content_length = int(resp.headers.get('Content-Length', 0))
            if content_length > self._max_resp_size:
                self._logger.error("response size %d exceeded max size %s",
                                   content_length, self._max_resp_size)
                resp.release()
                return (
                    {"req": {}, "redirect": redirect, "headers": {},
                     "body": {}}, 101)

            raw = await resp.read()
            if len(raw) > self._max_resp_size:
                self._logger.error(
                    "real response size %d exceeded max size %s",
                    len(raw), self._max_resp_size)
                resp.release()
                return (
                    {"req": {}, "redirect": redirect, "headers": {}, "body": {}},
                    101)

            req_info = {k: self._multidict_to_dict(v)
                        for k, v in dict(
                                resp._request_info._asdict()).items()}

            try:
                body = self._parse_resp(resp, raw, url)
                resp.release()
            except Exception as ex:
                self._logger.error("feed load %s error: %s", url, ex)
                resp.release()
                return (
                    {"req": req_info, "redirect": redirect,
                     "headers": dict(resp.headers), "body": {}}, 106)

            try:
                body = self._sanitize_feed(body)
                if "rss" in body.keys():
                    body = self._rss_parser2(
                        body.get("rss").get("elem").get("channel"))
                elif "{http://www.w3.org/2005/Atom}feed" in body.keys():
                    body = self._atom_parser2(
                        body.get("{http://www.w3.org/2005/Atom}feed"))
                elif "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF" in body.keys():
                    body = self._rdf_parser2(
                        body.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF"))

                return (
                    {"req": req_info, "redirect": redirect,
                     "headers": dict(resp.headers), "body": body}, 0)
            except Exception as ex:
                self._logger.error("feed parsing %s error: %s", url, ex)
                return (
                    {"req": req_info, "redirect": redirect,
                     "headers": dict(resp.headers), "body": {}}, 106)

        except aiohttp.ClientError as ex:
            self._logger.error("aiohttp request %s error: %s", url, ex)
            return (
                {"req": {}, "redirect": [], "headers": {}, "body": {}},
                101)
        except asyncio.exceptions.TimeoutError as ex:
            self._logger.error("aiohttp timeout on %s error: %s", url, ex)
            return (
                {"req": {}, "redirect": [], "headers": {}, "body": {}},
                104)
        except Exception as ex:
            self._logger.error("aiohttp request %s error: %s", url, ex)
            return (
                {"req": {}, "redirect": [],
                 "headers": dict(resp.headers), "body": {}}, 106)

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
