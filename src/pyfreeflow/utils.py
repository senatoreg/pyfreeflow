import os
import copy
import re
import asyncio
import pyparsing as pp
import datetime as dt
import locale
import dateutil
import lxml.etree


def deepupdate(base, other, keep=True):
    assert (isinstance(base, dict) and isinstance(other, dict))

    if not keep:
        bk = list(base.keys())
        for k in bk:
            if k not in other:
                del base[k]

    for k, v in other.items():
        if k not in base:
            base[k] = copy.deepcopy(v)
        else:
            if isinstance(v, dict):
                deepupdate(base[k], v, keep=keep)
            else:
                base[k] = copy.deepcopy(v)


def asyncio_run(fn, force=False):
    try:
        return asyncio.create_task(fn)
    except RuntimeError:
        if not force:
            raise RuntimeError("No loop!")
        else:
            return asyncio.run(fn)


class EnvVarParser():
    SIMPLE_RE = re.compile(r'(?<!\\)\$([a-zA-Z0-9_]+)')
    EXTENDED_RE = re.compile(r'(?<!\\)\$\{([a-zA-Z0-9_]+)((:?-)([^}]+))?\}')

    @classmethod
    def _repl_simple(cls, m, default=None):
        var = m.group(1)
        return os.environ.get(var, default)

    @classmethod
    def _repl_ext(cls, m):
        var = m.group(1)

        if m.group(2) is not None:
            default = m.group(4)
            spec = m.group(3)
            value = os.environ.get(var, default)
            if spec == ":-" and len(value) == 0:
                return default
            elif spec == "-" or value is not None:
                return value
            else:
                return None

        return os.environ.get(var, None)

    @classmethod
    def parse(cls, s):
        if s is None or not isinstance(s, str):
            return s
        a = cls.SIMPLE_RE.sub(cls._repl_simple, s)
        b = cls.EXTENDED_RE.sub(cls._repl_ext, a)
        return b


class DurationParser():
    INTEGER = pp.Word(pp.nums).setParseAction(lambda t: int(t[0]))

    SECONDS = pp.Opt(pp.Group(INTEGER + "s"))
    MINUTES = pp.Opt(pp.Group(INTEGER + "m"))
    HOURS = pp.Opt(pp.Group(INTEGER + "h"))
    DAYS = pp.Opt(pp.Group(INTEGER + "d"))
    WEEKS = pp.Opt(pp.Group(INTEGER + "w"))
    YEARS = pp.Opt(pp.Group(INTEGER + "y"))

    PARSER = YEARS + WEEKS + DAYS + HOURS + MINUTES + SECONDS

    OP = {
        "y": lambda x: ("days", x * 365),
        "w": lambda x: ("weeks", x),
        "d": lambda x: ("days", x),
        "h": lambda x: ("hours", x),
        "m": lambda x: ("minutes", x),
        "s": lambda x: ("seconds", x),
    }

    @classmethod
    def parse(cls, duration):

        parsed_duration = cls.PARSER.parseString(duration)
        delta = {}

        for val, unit in parsed_duration:
            k, v = cls.OP[unit](val)
            delta[k] = delta.get(k, 0) + v

        return dt.timedelta(**delta) / dt.timedelta(microseconds=1)


class MimeTypeParser():
    XML_MIME_PATTERN = re.compile(
        r'^(application|text)/([\w\.\-]+\+)?xml($|;)',
        re.IGNORECASE
    )

    JSON_MIME_PATTERN = re.compile(
        r'^(application|text)/([\w\.\-]+\+)?json($|;)',
        re.IGNORECASE
    )

    @classmethod
    def is_xml(cls, content_type: str) -> bool:
        """
        Verifica se il Content-Type è XML.

        Args:
        content_type: Header Content-Type

        Returns:
        True se è XML, False altrimenti
        """
        return bool(cls.XML_MIME_PATTERN.match(content_type.lower()))

    @classmethod
    def is_json(cls, content_type: str) -> bool:
        """
        Verifica se il Content-Type è XML.

        Args:
        content_type: Header Content-Type

        Returns:
        True se è XML, False altrimenti
        """
        return bool(cls.JSON_MIME_PATTERN.match(content_type.lower()))


class SecureXMLParser:
    """
    Parser XML sicuro che converte XML in dizionario con struttura personalizzata.
    Formato: {tag: {attrs: {}, text: "", children: {...}}}
    """

    def __init__(self,
                 max_size=10 * 1024 * 1024,  # 10MB default
                 max_depth=100,
                 resolve_entities=False,
                 huge_tree=False,
                 strip_whitespace=True):
        """
        Inizializza il parser con opzioni di sicurezza.

        Args:
            max_size: Dimensione massima del documento XML in bytes
            max_depth: Profondità massima dell'albero XML
            resolve_entities: Se risolvere le entità XML (False per sicurezza)
            huge_tree: Se permettere alberi molto grandi (False per sicurezza)
            strip_whitespace: Se rimuovere whitespace dai testi
        """
        self.max_size = max_size
        self.max_depth = max_depth
        self.strip_whitespace = strip_whitespace

        # Parser configurato per sicurezza
        self.parser = lxml.etree.XMLParser(
            resolve_entities=resolve_entities,
            huge_tree=huge_tree,
            recover=False,  # Non recuperare da errori
            no_network=True,  # Blocca accesso rete
            remove_blank_text=strip_whitespace,
            ns_clean=True,
        )

    def _validate_size(self, content):
        """Valida la dimensione del contenuto."""
        size = len(content.encode('utf-8') if isinstance(content, str) else content)
        if size > self.max_size:
            raise ValueError(f"XML troppo grande: {size} bytes (max: {self.max_size})")

    def _element_to_dict(self, element, depth=0):
        """
        Converte un elemento lxml in dizionario.

        Args:
            element: Elemento lxml da convertire
            depth: Profondità corrente (per prevenire DoS)

        Returns:
            Dizionario con struttura personalizzata
        """
        if depth > self.max_depth:
            raise ValueError(f"XML troppo profondo: superata profondità massima di {self.max_depth}")

        # result = {}

        # Attributi
        attrs = dict(element.attrib) if element.attrib else {}

        # Testo dell'elemento
        text = element.text or None
        if text is not None and self.strip_whitespace:
            text = text.strip()

        # Testo dalla coda (tail)
        tail = element.tail or None
        if tail is not None and self.strip_whitespace:
            tail = tail.strip()

        # Figli raggruppati per tag
        children = {}
        for child in element:
            # child_tag = child.tag
            # child_tag = lxml.etree.QName(child).localname
            child_tag = child.tag
            child_dict = self._element_to_dict(child, depth + 1)

            if child_tag in children:
                # Se già esiste, trasforma in lista
                if not isinstance(children[child_tag], list):
                    children[child_tag] = [children[child_tag]]
                children[child_tag].append(child_dict)
            else:
                children[child_tag] = child_dict

        # Costruisci il risultato
        element_data = {}
        element_data['attrs'] = attrs
        element_data['text'] = text
        element_data['tail'] = tail
        if children:
            # element_data.update(children)
            element_data['elem'] = children

        return element_data

    def parse_string(self, xml_string):
        """
        Parse an XML string.

        Args:
            xml_string: XML string

        Returns:
            XML dict
        """
        self._validate_size(xml_string)

        try:
            root = lxml.etree.fromstring(xml_string.encode('utf-8'),
                                         self.parser)
            for comment in root.xpath("//comment()"):
                parent = comment.getparent()
                if parent is not None:
                    comment.getparent().remove(comment)
            self._nsmap = root.nsmap
            self._mapns = {v: k for k, v in self._nsmap.items()}
            # return {lxml.etree.QName(root).localname: self._element_to_dict(root)}
            return {root.tag: self._element_to_dict(root)}
        except lxml.etree.XMLSyntaxError as e:
            raise ValueError(f"XML malformed: {e}")
        except Exception as e:
            raise RuntimeError(f"parsing error: {e}")

    def parse_bytes(self, xml_bytes):
        """
        Parse bytes XML.

        Args:
            xml_bytes: XML bytes

        Returns:
            XML dict
        """
        self._validate_size(xml_bytes)

        try:
            root = lxml.etree.fromstring(xml_bytes, self.parser)
            for comment in root.xpath("//comment()"):
                parent = comment.getparent()
                if parent is not None:
                    comment.getparent().remove(comment)
            self._nsmap = root.nsmap
            self._mapns = {v: k for k, v in self._nsmap.items()}
            # return {lxml.etree.QName(root).localname: self._element_to_dict(root)}
            return {root.tag: self._element_to_dict(root)}
        except lxml.etree.XMLSyntaxError as e:
            raise ValueError(f"XML malformato: {e}")
        except Exception as e:
            raise RuntimeError(f"Errore durante il parsing: {e}")


class DateParser():
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

    @classmethod
    def parse_date(cls, a, fmt=COMMON_DATETIME_FORMAT):
        locales = ['en_US.UTF-8', 'it_IT.UTF-8']
        cur_loc = locale.getlocale()
        date = None

        if cur_loc not in locales:
            locales.append(cur_loc)

        try:
            return dateutil.parser.parse(
                a, tzinfos=cls.WHOIS_TIMEZONE_INFO).timestamp()
        except Exception:
            pass

        found = False
        for loc in locales:
            locale.setlocale(locale.LC_ALL, loc)
            for f in fmt:
                try:
                    date = dt.datetime.strptime(a, f).timestamp()
                    found = True
                    break
                except Exception:
                    pass
            if found:
                break
        locale.setlocale(locale.LC_ALL, cur_loc)
        return date
