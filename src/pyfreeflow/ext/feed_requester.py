from .types import FreeFlowExt
import aiohttp
import yarl
import multidict
import json
import ssl
import asyncio
import logging
import dateutil
# import babel.dates
import datetime as dt
import locale
import re
from ..utils import asyncio_run, MimeTypeParser, SecureXMLParser

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


WHOIS_TIMEZONE_INFO = {
    "A": 1 * 3600,
    "ACDT": 10.5 * 3600,
    "ACST": 9.5 * 3600,
    "ACT": -5 * 3600,
    "ACWST": 8.75 * 3600,
    "ADT": 4 * 3600,
    "AEDT": 11 * 3600,
    "AEST": 10 * 3600,
    "AET": 10 * 3600,
    "AFT": 4.5 * 3600,
    "AKDT": -8 * 3600,
    "AKST": -9 * 3600,
    "ALMT": 6 * 3600,
    "AMST": -3 * 3600,
    "AMT": -4 * 3600,
    "ANAST": 12 * 3600,
    "ANAT": 12 * 3600,
    "AQTT": 5 * 3600,
    "ART": -3 * 3600,
    "AST": 3 * 3600,
    "AT": -4 * 3600,
    "AWDT": 9 * 3600,
    "AWST": 8 * 3600,
    "AZOST": 0 * 3600,
    "AZOT": -1 * 3600,
    "AZST": 5 * 3600,
    "AZT": 4 * 3600,
    "AoE": -12 * 3600,
    "B": 2 * 3600,
    "BNT": 8 * 3600,
    "BOT": -4 * 3600,
    "BRST": -2 * 3600,
    "BRT": -3 * 3600,
    "BST": 6 * 3600,
    "BTT": 6 * 3600,
    "C": 3 * 3600,
    "CAST": 8 * 3600,
    "CAT": 2 * 3600,
    "CCT": 6.5 * 3600,
    "CDT": -5 * 3600,
    "CEST": 2 * 3600,
    "CET": 1 * 3600,
    "CHADT": 13.75 * 3600,
    "CHAST": 12.75 * 3600,
    "CHOST": 9 * 3600,
    "CHOT": 8 * 3600,
    "CHUT": 10 * 3600,
    "CIDST": -4 * 3600,
    "CIST": -5 * 3600,
    "CKT": -10 * 3600,
    "CLST": -3 * 3600,
    "CLT": -4 * 3600,
    "COT": -5 * 3600,
    "CST": -6 * 3600,
    "CT": -6 * 3600,
    "CVT": -1 * 3600,
    "CXT": 7 * 3600,
    "ChST": 10 * 3600,
    "D": 4 * 3600,
    "DAVT": 7 * 3600,
    "DDUT": 10 * 3600,
    "E": 5 * 3600,
    "EASST": -5 * 3600,
    "EAST": -6 * 3600,
    "EAT": 3 * 3600,
    "ECT": -5 * 3600,
    "EDT": -4 * 3600,
    "EEST": 3 * 3600,
    "EET": 2 * 3600,
    "EGST": 0 * 3600,
    "EGT": -1 * 3600,
    "EST": -5 * 3600,
    "ET": -5 * 3600,
    "F": 6 * 3600,
    "FET": 3 * 3600,
    "FJST": 13 * 3600,
    "FJT": 12 * 3600,
    "FKST": -3 * 3600,
    "FKT": -4 * 3600,
    "FNT": -2 * 3600,
    "G": 7 * 3600,
    "GALT": -6 * 3600,
    "GAMT": -9 * 3600,
    "GET": 4 * 3600,
    "GFT": -3 * 3600,
    "GILT": 12 * 3600,
    "GMT": 0 * 3600,
    "GST": 4 * 3600,
    "GYT": -4 * 3600,
    "H": 8 * 3600,
    "HDT": -9 * 3600,
    "HKT": 8 * 3600,
    "HOVST": 8 * 3600,
    "HOVT": 7 * 3600,
    "HST": -10 * 3600,
    "I": 9 * 3600,
    "ICT": 7 * 3600,
    "IDT": 3 * 3600,
    "IOT": 6 * 3600,
    "IRDT": 4.5 * 3600,
    "IRKST": 9 * 3600,
    "IRKT": 8 * 3600,
    "IRST": 3.5 * 3600,
    "IST": 5.5 * 3600,
    "JST": 9 * 3600,
    "K": 10 * 3600,
    "KGT": 6 * 3600,
    "KOST": 11 * 3600,
    "KRAST": 8 * 3600,
    "KRAT": 7 * 3600,
    "KST": 9 * 3600,
    "KUYT": 4 * 3600,
    "L": 11 * 3600,
    "LHDT": 11 * 3600,
    "LHST": 10.5 * 3600,
    "LINT": 14 * 3600,
    "M": 12 * 3600,
    "MAGST": 12 * 3600,
    "MAGT": 11 * 3600,
    "MART": 9.5 * 3600,
    "MAWT": 5 * 3600,
    "MDT": -6 * 3600,
    "MHT": 12 * 3600,
    "MMT": 6.5 * 3600,
    "MSD": 4 * 3600,
    "MSK": 3 * 3600,
    "MST": -7 * 3600,
    "MT": -7 * 3600,
    "MUT": 4 * 3600,
    "MVT": 5 * 3600,
    "MYT": 8 * 3600,
    "N": -1 * 3600,
    "NCT": 11 * 3600,
    "NDT": 2.5 * 3600,
    "NFT": 11 * 3600,
    "NOVST": 7 * 3600,
    "NOVT": 7 * 3600,
    "NPT": 5.5 * 3600,
    "NRT": 12 * 3600,
    "NST": 3.5 * 3600,
    "NUT": -11 * 3600,
    "NZDT": 13 * 3600,
    "NZST": 12 * 3600,
    "O": -2 * 3600,
    "OMSST": 7 * 3600,
    "OMST": 6 * 3600,
    "ORAT": 5 * 3600,
    "P": -3 * 3600,
    "PDT": -7 * 3600,
    "PET": -5 * 3600,
    "PETST": 12 * 3600,
    "PETT": 12 * 3600,
    "PGT": 10 * 3600,
    "PHOT": 13 * 3600,
    "PHT": 8 * 3600,
    "PKT": 5 * 3600,
    "PMDT": -2 * 3600,
    "PMST": -3 * 3600,
    "PONT": 11 * 3600,
    "PST": -8 * 3600,
    "PT": -8 * 3600,
    "PWT": 9 * 3600,
    "PYST": -3 * 3600,
    "PYT": -4 * 3600,
    "Q": -4 * 3600,
    "QYZT": 6 * 3600,
    "R": -5 * 3600,
    "RET": 4 * 3600,
    "ROTT": -3 * 3600,
    "S": -6 * 3600,
    "SAKT": 11 * 3600,
    "SAMT": 4 * 3600,
    "SAST": 2 * 3600,
    "SBT": 11 * 3600,
    "SCT": 4 * 3600,
    "SGT": 8 * 3600,
    "SRET": 11 * 3600,
    "SRT": -3 * 3600,
    "SST": -11 * 3600,
    "SYOT": 3 * 3600,
    "T": -7 * 3600,
    "TAHT": -10 * 3600,
    "TFT": 5 * 3600,
    "TJT": 5 * 3600,
    "TKT": 13 * 3600,
    "TLT": 9 * 3600,
    "TMT": 5 * 3600,
    "TOST": 14 * 3600,
    "TOT": 13 * 3600,
    "TRT": 3 * 3600,
    "TVT": 12 * 3600,
    "U": -8 * 3600,
    "ULAST": 9 * 3600,
    "ULAT": 8 * 3600,
    "UTC": 0 * 3600,
    "UYST": -2 * 3600,
    "UYT": -3 * 3600,
    "UZT": 5 * 3600,
    "V": -9 * 3600,
    "VET": -4 * 3600,
    "VLAST": 11 * 3600,
    "VLAT": 10 * 3600,
    "VOST": 6 * 3600,
    "VUT": 11 * 3600,
    "W": -10 * 3600,
    "WAKT": 12 * 3600,
    "WARST": -3 * 3600,
    "WAST": 2 * 3600,
    "WAT": 1 * 3600,
    "WEST": 1 * 3600,
    "WET": 0 * 3600,
    "WFT": 12 * 3600,
    "WGST": -2 * 3600,
    "WGT": -3 * 3600,
    "WIB": 7 * 3600,
    "WIT": 9 * 3600,
    "WITA": 8 * 3600,
    "WST": 14 * 3600,
    "WT": 0 * 3600,
    "X": -11 * 3600,
    "Y": -12 * 3600,
    "YAKST": 10 * 3600,
    "YAKT": 9 * 3600,
    "YAPT": 10 * 3600,
    "YEKST": 6 * 3600,
    "YEKT": 5 * 3600,
    "Z": 0 * 3600,
}


COMMON_DATETIME_FORMAT = [
    "%a, %d %b %Y %H:%M:%S %z",
    "%Y-%m-%dT%H:%M:%SZ",
]


class FeedTagParser():
    @classmethod
    def parse_date(cls, a, fmt=COMMON_DATETIME_FORMAT):
        locales = ['en_US.UTF-8', 'it_IT.UTF-8']
        cur_loc = locale.getlocale()
        date = None

        if cur_loc not in locales:
            locales.append(cur_loc)

        try:
            return dateutil.parser.parse(
                a, tzinfos=WHOIS_TIMEZONE_INFO).timestamp()
        except Exception:
            pass

        for loc in locales:
            locale.setlocale(locale.LC_ALL, loc)
            for f in fmt:
                try:
                    date = dt.datetime.strptime(a, f).timestamp()
                    break
                except Exception:
                    pass
            break
        locale.setlocale(locale.LC_ALL, cur_loc)
        return date

    @classmethod
    def parse_content(cls, a, b):
        content = cls.get_child(a, b, "attrs")
        if isinstance(content, list):
            return [{"url": x.get("url"), "type": x.get("type")} for x in content]
        return [{"url": content.get("url"), "type": content.get("type")}]

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

    @classmethod
    def get_child(cls, a, b, c=None):
        if a is None:
            return a

        if len(b) > 0:
            return cls.get_child(a.get("elem", {}).get(b[0]), b[1:], c)

        if isinstance(a, list):
            return [x.get(c) if c is not None else x for x in a]

        return a.get(c) if c is not None else a


class FeedTagDefinition():
    RSS20_TAG = {
        "channel": lambda a: (None, FeedTagParser.get_child(
            a, ["channel"], "elem")),
        "item": lambda a: ("entry", FeedTagParser.get_child(
            a, ["item"])),
        "title": lambda a: ("title", FeedTagParser.get_child(
            a, ["title"], "text")),
        "link": lambda a: ("link", [{"href": FeedTagParser.get_child(
            a, ["link"], "text"), "rel": "alternate"}]),
        "description": lambda a: ("description", FeedTagParser.get_child(
            a, ["description"], "text")),
        "language": lambda a: ("language", FeedTagParser.get_child(
            a, ["language"], "text")),
        "copyright": lambda a: ("copyright", FeedTagParser.get_child(
            a, ["copyright"], "text")),
        "managingEditor": lambda a: ("copyright", FeedTagParser.get_child(
            a, ["managingEditor"], "text")),
        "webMaster": lambda a: ("webMaster", FeedTagParser.get_child(
            a, ["webMaster"], "text")),
        "pubDate": lambda a: ("published", FeedTagParser.parse_date(
            FeedTagParser.get_child(a, ["pubDate"], "text"))),
        "lastBuildDate": lambda a: ("updated", FeedTagParser.parse_date(
            FeedTagParser.get_child(a, ["lastBuildDate"], "text"))),
        "category": lambda a: ("category", FeedTagParser.get_child(
            a, ["category"], "text")),
        "generator": lambda a: ("generator", FeedTagParser.get_child(
            a, ["generator"], "text")),
        "docs": lambda a: ("docs", FeedTagParser.get_child(
            a, ["docs"], "text")),
        "cloud": lambda a: ("cloud", FeedTagParser.get_child(
            a, ["cloud"], "text")),
        "ttl": lambda a: ("ttl", FeedTagParser.get_child(a, ["ttl"], "text")),
        "image": lambda a: ("image", FeedTagParser.get_child(
            a, ["image"], "text")),
        "enclosure": lambda a: ("media", [{
            "url": FeedTagParser.get_child(
                a, ["enclosure"], "attrs").get("url"),
            "type": FeedTagParser.get_child(
                a, ["enclosure"], "attrs").get("type"),
        }]),
        "rating": lambda a: ("rating", FeedTagParser.get_child(
            a, ["rating"], "text")),
        "textInput": lambda a: ("textInput", FeedTagParser.get_child(
            a, ["textInput"], "text")),
        "skipHours": lambda a: ("skipHours", FeedTagParser.get_child(
            a, ["skipHours"], "text")),
        "skipDays": lambda a: ("skipDays", FeedTagParser.get_child(
            a, ["skipDays"], "text")),
        "author": lambda a: ("author", FeedTagParser.tolist_if(
            FeedTagParser.get_child(a, ["author"], "text"))),
        "comments": lambda a: ("comments", FeedTagParser.get_child(
            a, ["comments"], "text")),
        "guid": lambda a: ("guid", FeedTagParser.get_child(
            a, ["guid"], "text")),
        "source": lambda a: ("source", FeedTagParser.get_child(
            a, ["source"], "text")),
    }

    ATOM_TAG = {
        "{http://www.w3.org/2005/Atom}link": lambda a: (
            "link", FeedTagParser.get_atom_link(
                FeedTagParser.get_child(
                    a, ["{http://www.w3.org/2005/Atom}link"], "attrs"),
                "href")),
        "{http://www.w3.org/2005/Atom}id": lambda a: (
            "id", FeedTagParser.get_child(
                a, ["{http://www.w3.org/2005/Atom}id"], "text")),
        "{http://www.w3.org/2005/Atom}title": lambda a: (
            "title", FeedTagParser.get_child(
                a, ["{http://www.w3.org/2005/Atom}title"], "text")),
        "{http://www.w3.org/2005/Atom}content": lambda a: (
            "content", FeedTagParser.get_child(
                a, ["{http://www.w3.org/2005/Atom}content"], "text")),
        "{http://www.w3.org/2005/Atom}author": lambda a:
        ("author", FeedTagParser.tolist_if(FeedTagParser.get_child(a, [
            "{http://www.w3.org/2005/Atom}author",
            "{http://www.w3.org/2005/Atom}name"], "text"))),
        "{http://www.w3.org/2005/Atom}published": lambda a: (
            "published", FeedTagParser.parse_date(FeedTagParser.get_child(
                a, ["{http://www.w3.org/2005/Atom}published"], "text"))),
        "{http://www.w3.org/2005/Atom}updated": lambda a: (
            "updated", FeedTagParser.parse_date(FeedTagParser.get_child(
                a, ["{http://www.w3.org/2005/Atom}updated"], "text"))),
        "{http://www.w3.org/2005/Atom}entry": lambda a: (
            "entry", FeedTagParser.get_child(
                a, ["{http://www.w3.org/2005/Atom}entry"])),
    }

    ITUNES_TAG = {
        "{http://www.itunes.com/dtds/podcast-1.0.dtd}author": lambda a: (
            "author", FeedTagParser.tolist_if(FeedTagParser.get_child(
                a, ["{http://www.itunes.com/dtds/podcast-1.0.dtd}author"],
                "text"))),
        "{http://www.itunes.com/dtds/podcast-1.0.dtd}summary": lambda a: (
            "description", FeedTagParser.get_child(
                a, ["{http://www.itunes.com/dtds/podcast-1.0.dtd}summary"],
                "text")),
        "{http://www.itunes.com/dtds/podcast-1.0.dtd}category": lambda a: (
            "category", FeedTagParser.get_child(
                a, ["{http://www.itunes.com/dtds/podcast-1.0.dtd}category"],
                "text")),
        "{http://www.itunes.com/dtds/podcast-1.0.dtd}title": lambda a: (
            "title", FeedTagParser.get_child(
                a, ["{http://www.itunes.com/dtds/podcast-1.0.dtd}title"],
                "text")),
    }

    MEDIA_RSS_TAG = {
        "{http://search.yahoo.com/mrss}group": lambda a: (
            "group", FeedTagParser.get_child(
                a, ["{http://search.yahoo.com/mrss}group"])),
        "{http://search.yahoo.com/mrss}credit": lambda a: (
            "author", FeedTagParser.tolist_if(FeedTagParser.get_child(
                a, ["{http://search.yahoo.com/mrss}credit"],
                "text"))),
        "{http://search.yahoo.com/mrss}description": lambda a: (
            "description", FeedTagParser.get_child(
                a, ["{http://search.yahoo.com/mrss}description"],
                "text")),
        "{http://search.yahoo.com/mrss}content": lambda a: (
            "media", FeedTagParser.parse_content(
                a, ["{http://search.yahoo.com/mrss}content"])),
        "{http://search.yahoo.com/mrss}category": lambda a: (
            "category", FeedTagParser.get_child(
                a, ["{http://search.yahoo.com/mrss}category"],
                "text")),
        "{http://search.yahoo.com/mrss}comments": lambda a: (
            "comments", FeedTagParser.get_child(
                a, ["{http://search.yahoo.com/mrss}comments"],
                "text")),
        "{http://search.yahoo.com/mrss}title": lambda a: (
            "title", FeedTagParser.get_child(
                a, ["{http://search.yahoo.com/mrss}title"],
                "text")),
    }

    RSS10_TAG = {
        "{http://purl.org/rss/1.0}channel": lambda a: (
            None, FeedTagParser.get_child(
                a, ["{http://purl.org/rss/1.0}channel"])),
        "{http://purl.org/rss/1.0}title": lambda a: (
            "title", FeedTagParser.get_child(
                a, ["{http://purl.org/rss/1.0}title"], "text")),
        "{http://purl.org/rss/1.0}description": lambda a: (
            "description", FeedTagParser.get_child(
                a, ["{http://purl.org/rss/1.0}description"], "text")),
        "{http://purl.org/rss/1.0}item": lambda a: (
            "entry", FeedTagParser.get_child(
                a, ["{http://purl.org/rss/1.0}item"])),
        "{http://purl.org/rss/1.0}link": lambda a: (
            "link", [{"href": FeedTagParser.get_child(
                a, ["{http://purl.org/rss/1.0}link"], "text"),
                      "rel": "alternate"}]),
    }

    RSS10_CONTENT_TAG = {
        "{http://purl.org/rss/1.0/modules/content}encoded": lambda a: (
            "content", FeedTagParser.get_child(
                a, ["{http://purl.org/rss/1.0/modules/content}encoded"],
                "text")),
    }

    DCMI_TAG = {
        "{http://purl.org/dc/elements/1.1}creator": lambda a: (
            "author", FeedTagParser.tolist_if(FeedTagParser.get_child(
                a, ["{http://purl.org/dc/elements/1.1}creator"],
                "text"))),
        "{http://purl.org/dc/elements/1.1}date": lambda a: (
            "published", FeedTagParser.parse_date(FeedTagParser.get_child(
                a, ["{http://purl.org/dc/elements/1.1}date"], "text"))),
        "{http://purl.org/dc/elements/1.1}description": lambda a: (
            "description", FeedTagParser.get_child(
                a, ["{http://purl.org/dc/elements/1.1}description"], "text")),
        "{http://purl.org/dc/elements/1.1}type": lambda a: (
            "type", FeedTagParser.get_child(
                a, ["{http://purl.org/dc/elements/1.1}type"], "text")),
        "{http://purl.org/dc/elements/1.1}language": lambda a: (
            "language", FeedTagParser.get_child(
                a, ["{http://purl.org/dc/elements/1.1}language"], "text")),
        "{http://purl.org/dc/elements/1.1}publisher": lambda a: (
            "managingEditor", FeedTagParser.get_child(
                a, ["{http://purl.org/dc/elements/1.1}publisher"], "text")),
        "{http://purl.org/dc/elements/1.1}rights": lambda a: (
            "copyright", FeedTagParser.get_child(
                a, ["{http://purl.org/dc/elements/1.1}rights"], "text")),
        "{http://purl.org/dc/elements/1.1}source": lambda a: (
            "source", FeedTagParser.get_child(
                a, ["{http://purl.org/dc/elements/1.1}source"], "text")),
        "{http://purl.org/dc/elements/1.1}title": lambda a: (
            "title", FeedTagParser.get_child(
                a, ["{http://purl.org/dc/elements/1.1}title"], "text")),
        "{http://purl.org/dc/elements/1.1}subject": lambda a: (
            "category", FeedTagParser.get_child(
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
        FeedTagDefinition.DCMI_TAG

    ATOM_TAG = FeedTagDefinition.ATOM_TAG | FeedTagDefinition.MEDIA_RSS_TAG

    RDF_TAG = FeedTagDefinition.RSS10_TAG | FeedTagDefinition.RSS10_CONTENT_TAG \
        | FeedTagDefinition.DCMI_TAG

    def __init__(self, name, url, method="GET", headers={}, timeout=300,
                 max_response_size=10485760, sslenabled=True, insecure=False,
                 cafile=None, capath=None, cadata=None, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        self._url = url
        self._timeout = timeout
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

        for tag, value in data.get("elem", {}).items():
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

        for tag, value in data.get("elem", {}).items():
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

        for tag, value in data.get("elem", {}).items():
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

    async def _do_request(self, method, url, headers=None, params=None,
                          data=None, userdata=None):
        try:
            await self._ensure_session()

            async with self._session.request(
                    method, url, headers=headers, params=params, data=data,
                    ssl=self._ssl_context, allow_redirects=True) as resp:

                if resp.status >= 400:
                    self._logger.error(f"'{url}' response code {resp.status}")
                    return (
                        {"req": {}, "userdata": userdata, "headers": {},
                         "body": {}}, 102)

                content_length = int(resp.headers.get('Content-Length', 0))
                if content_length > self._max_resp_size:
                    self._logger.error("response size %d exceeded max size %s",
                                       content_length, self._max_resp_size)
                    return (
                        {"req": {}, "userdata": userdata, "headers": {},
                         "body": {}}, 101)

                raw = await resp.read()
                if len(raw) > self._max_resp_size:
                    self._logger.error(
                        "real response size %d exceeded max size %s",
                        content_length, self._max_resp_size)
                    return (
                        {"req": {}, "userdata": userdata, "headers": {},
                         "body": {}}, 101)

                req_info = {k: self._multidict_to_dict(v)
                            for k, v in dict(
                                    resp._request_info._asdict()).items()}
                try:
                    mimetype = resp.headers.get("Content-Type")
                    if MimeTypeParser.is_xml(mimetype):
                        parser = SecureXMLParser()
                        body = parser.parse_string(raw.decode("utf-8"))
                    else:
                        self._logger.warning(
                            "aiohttp request %s warning: response type '%s'",
                            url, mimetype)
                        body = {}
                except Exception as ex:
                    self._logger.error("feed load %s error: %s", url, ex)
                    return (
                        {"req": req_info, "userdata": userdata,
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
                    {"req": req_info, "userdata": userdata,
                     "headers": dict(resp.headers), "body": body}, 0)
            except Exception as ex:
                self._logger.error("feed parsing %s error: %s", url, ex)
                return (
                    {"req": req_info, "userdata": userdata,
                     "headers": dict(resp.headers), "body": {}}, 106)

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
