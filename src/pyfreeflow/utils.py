import copy
import asyncio
import pyparsing as pp
import datetime as dt


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

        return int(dt.timedelta(**delta).total_seconds())
