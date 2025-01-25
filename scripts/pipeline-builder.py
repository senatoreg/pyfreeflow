#!/usr/bin/python3
import sys
import pyfreeflow
import inspect
import argparse
import json
import yaml
import ast


FORMAT = {
    "json": json.dumps,
    "yaml": yaml.dump,
}

QUERY_FORMAT = {
    type: type,
    dict: ast.literal_eval,
    str: str,
    bool: bool,
    int: int,
    list: ast.literal_eval,
    type(None): lambda x: None,
}


def query_config(mod):
    params = inspect.signature(mod).parameters
    config = {}

    for k, v in params.items():
        converter = QUERY_FORMAT[type(v.default)]
        config[k] = converter(input("{} [{}]: ".format(
            k,
            v.default)).strip() or str(v.default))

    print(config)


def multi_choice(choices):
    bvalue = len(choices)
    resp = bvalue

    while resp >= bvalue:
        for i, x in enumerate(choices):
            print(str(i) + ")", x)
        resp = int(input("choose number: ", ).strip() or 0)

    return choices[resp]


def main(argv):
    argparser = argparse.ArgumentParser()

    argparser.add_argument("--format", "-f", dest="fmt", type=str,
                           choices=FORMAT.keys(), default="json", required=False,
                           help="Output format")

    # args = argparser.parse_args(argv)

    registry = pyfreeflow.registry.ExtRegistry.REGISTRY

    mod_name = multi_choice(list(registry.keys()))
    mod_ver = multi_choice(list(registry[mod_name].keys()))
    query_config(registry[mod_name][mod_ver])


if __name__ == "__main__":
    main(sys.argv[1:])
