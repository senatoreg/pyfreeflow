import os
import copy
import re
import asyncio
import pyparsing as pp
import datetime as dt
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
            self._nsmap = root.nsmap
            self._mapns = {v: k for k, v in self._nsmap.items()}
            # return {lxml.etree.QName(root).localname: self._element_to_dict(root)}
            return {root.tag: self._element_to_dict(root)}
        except lxml.etree.XMLSyntaxError as e:
            raise ValueError(f"XML malformato: {e}")
        except Exception as e:
            raise RuntimeError(f"Errore durante il parsing: {e}")
