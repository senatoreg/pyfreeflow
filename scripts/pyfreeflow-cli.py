#!/usr/bin/python3
import sys
import argparse
import pyfreeflow
import json
import yaml
import asyncio
from platform import system

if system() == "Linux":
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


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

    args = argparser.parse_args(argv)

    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    for ext in config.get("ext", []):
        pyfreeflow.load_extension(ext)

    assert ("pipeline" in config.keys())
    pipe = pyfreeflow.pipeline.Pipeline(**config.get("pipeline"))
    output = await pipe.run(config.get("args", {}))

    OUTPUT_FORMATTER[args.fmt](output[0], args.output)

    return output[1]


def main(argv):
    return asyncio.run(cli(argv))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
