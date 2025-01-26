#!/usr/bin/python3
import sys
import argparse
import pyfreeflow
from pyfreeflow.utils import EnvVarParser
import json
import yaml
import asyncio
import logging
from platform import system

if system() == "Linux":
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

loglevel_defs = {
    "info": logging.INFO,
    "warning": logging.WARNING,
    "debug": logging.DEBUG,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
    "fatal": logging.FATAL
}


def to_loglevel(x):
    return loglevel_defs[x]


def json_formatter(x, path):
    if path:
        with open(path, "w") as f:
            json.dump(x, f)
    else:
        print(json.dumps(x))


def _str_presenter(dumper, data):
    if "\n" in data:
        block = "\n".join([line.rstrip() for line in data.splitlines()])
        if data.endswith("\n"):
            block += "\n"
        return dumper.represent_scalar("tag:yaml.org,2002:str", block, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


def yaml_formatter(x, path):
    yaml.add_representer(str, _str_presenter)
    yaml.representer.SafeRepresenter.add_representer(str, _str_presenter)
    if path:
        with open(path, "w") as f:
            yaml.dump(
                x, f, default_flow_style=False, allow_unicode=True)
    else:
        print(yaml.dump(
            x, default_flow_style=False, allow_unicode=True))


OUTPUT_FORMATTER = {
    "json": json_formatter,
    "yaml": yaml_formatter,
}


async def cli(argv):
    argparser = argparse.ArgumentParser("pyfreeflow-cli")

    argparser.add_argument("--config", "-c", dest="config", type=str,
                           action="store", default="pyfreeflow.yaml",
                           required=False, help="Pipeline configuration file")
    argparser.add_argument("--output", "-o", dest="output", type=str,
                           action="store",
                           required=False, help="Pipeline output file")
    argparser.add_argument("--format", "-f", dest="fmt", type=str,
                           action="store", choices=OUTPUT_FORMATTER.keys(),
                           default="json", required=False,
                           help="Pipeline output file format")
    argparser.add_argument("--loglevel", "-l", dest="loglevel", action="store",
                           default="warning", type=str,
                           choices=loglevel_defs.keys(), help="log level")
    argparser.add_argument("--logfile", "-g", dest="logfile", action="store",
                           required=False, type=str,
                           help="log file")

    args = argparser.parse_args(argv)
    pyfreeflow.set_loglevel(to_loglevel(args.loglevel))
    if args.logfile:
        handler = logging.FileHandler(args.logfile, mode="a")
        pyfreeflow.add_loghandler(handler)

    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    for ext in config.get("ext", []):
        pyfreeflow.load_extension(ext)

    assert ("pipeline" in config.keys())
    pipe = pyfreeflow.pipeline.Pipeline()
    await pipe.init(**config.get("pipeline"))

    params = {k: EnvVarParser.parse(v) for k, v in config.get("args", {}).items()}
    rc = 0

    try:
        output = await pipe.run(params)
        OUTPUT_FORMATTER[args.fmt](output[0], args.output)
        rc = 0 if output[1] == 0 else 1
    except Exception as ex:
        pyfreeflow.logger.error(ex)
        rc = 1
    finally:
        await pipe.fini()

    return rc


def main(argv):
    return asyncio.run(cli(argv))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
