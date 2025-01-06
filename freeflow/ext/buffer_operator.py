from .types import FreeFlowExt
import json
import yaml
import logging
from sys import version_info

__JSON_TYPENAME__ = "JsonBufferOperator"
__YAML_TYPENAME__ = "YamlBufferOperator"
__TOML_TYPENAME__ = "TomlBufferOperator"


class JsonBufferOperator(FreeFlowExt):
    __typename__ = __JSON_TYPENAME__
    __version__ = "1.0"

    def __init__(self, name, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

        self._action = {
            "read": self._read,
            "write": self._write,
        }

    async def _read(self, raw):
        if not isinstance(raw, str):
            self._logger.error("Invalid input format '{}'".format(type(raw)))
            return None, 101

        try:
            j = json.loads(raw)
            return j, 0
        except Exception as ex:
            self._logger.error("Cannot load json data '{}' {}".format(raw, ex))
            return None, 102

    async def _write(self, raw):
        if not isinstance(raw, (dict, list, tuple)):
            self._logger.error("Invalid input format '{}'".format(type(raw)))
            return None, 101

        try:
            j = json.dumps(raw)
            return j, 0
        except Exception as ex:
            self._logger.error("Cannot write json data '{}' {}".format(
                raw, ex))
            return None, 103

    async def do(self, state, data):
        op = data.get("op", "read")
        raw = data.get("data", {} if op == "read" else "")
        rval = await self._action[op](raw)
        return state, rval


class YamlBufferOperator(FreeFlowExt):
    __typename__ = __YAML_TYPENAME__
    __version__ = "1.0"

    def __init__(self, name, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

        self._action = {
            "read": self._read,
            "write": self._write,
        }

    async def _read(self, raw):
        if not isinstance(raw, str):
            self._logger.error("Invalid input format '{}'".format(type(raw)))
            return None, 101

        try:
            j = yaml.safe_load(raw)
            return j, 0
        except Exception as ex:
            self._logger.error("Cannot write yaml data '{}' {}".format(
                raw, ex))
            return None, 102

    async def _write(self, raw):
        if not isinstance(raw, (dict, list, tuple)):
            self._logger.error("Invalid input format '{}'".format(type(raw)))
            return None, 101

        try:
            j = yaml.safe_dump(raw)
            return j, 0
        except Exception as ex:
            self._logger.error("Cannot write yaml data '{}' {}".format(
                raw, ex))
            return None, 103

    async def do(self, state, data):
        op = data.get("op", "read")
        raw = data.get("data", {} if op == "read" else "")
        rval = await self._action[op](raw)
        return state, rval


if version_info.major > 3 or (version_info.major == 3 and
                              version_info.minor > 10):
    import tomllib
    import tomli_w

    class TomlBufferOperator(FreeFlowExt):
        __typename__ = __TOML_TYPENAME__
        __version__ = "1.0"

        def __init__(self, name, max_tasks=4):
            super().__init__(name, max_tasks=max_tasks)

            self._logger = logging.getLogger(".".join(
                [__name__, self.__typename__, self._name]))

            self._action = {
                "read": self._read,
                "write": self._write,
            }

        async def _read(self, raw):
            if not isinstance(raw, str):
                self._logger.error("Invalid input format '{}'".format(
                    type(raw)))
                return None, 101

            try:
                j = tomllib.loads(raw)
                return j, 0
            except Exception as ex:
                self._logger.error("Invalid input format '{}' {}".format(
                    type(raw), ex))
                return None, 102

        async def _write(self, raw):
            if not isinstance(raw, (dict, list, tuple)):
                self._logger.error("Invalid input format '{}'".format(
                    type(raw)))
                return None, 101

            try:
                j = tomli_w.dumps(raw)
                return j, 0
            except Exception as ex:
                self._logger.error("Invalid input format '{}' {}".format(
                    type(raw), ex))
                return None, 102

        async def do(self, state, data):
            op = data.get("op", "read")
            raw = data.get("data", {} if op == "read" else "")
            rval = await self._action[op](raw)
            return state, rval
