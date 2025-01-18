from .types import FreeFlowExt
import aiofiles
import json
import yaml
import logging
from sys import version_info

__ANY_TYPENAME__ = "AnyFileOperator"
__JSON_TYPENAME__ = "JsonFileOperator"
__YAML_TYPENAME__ = "YamlFileOperator"
__TOML_TYPENAME__ = "TomlFileOperator"


class AnyFileOperator(FreeFlowExt):
    __typename__ = __ANY_TYPENAME__
    __version__ = "1.0"

    def __init__(self, name, max_tasks=4, binary=False):
        super().__init__(name, max_tasks=max_tasks)

        self._mode = "b" if binary else ""

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

        self._action = {
            "read": self._read,
            "write": self._write,
        }

    async def _read(self, path, raw):
        try:
            async with aiofiles.open(path, "r" + self._mode) as f:
                contents = await f.read()
                return contents, 0
        except Exception as ex:
            self._logger.error("Cannot load file '{}' {}".format(path, ex))
            return None, 102

    async def _write(self, path, raw):
        if self._mode == "b" and isinstance(raw, str):
            raw_ = raw.encode("utf-8")
        else:
            raw_ = raw
        try:
            async with aiofiles.open(path, "w" + self._mode) as f:
                await f.write(raw_)
                return raw, 0
        except Exception as ex:
            self._logger.error("Cannot write json file '{}' {}".format(
                path, ex))
            return raw, 103

    async def do(self, state, data):
        op = data.get("op", "read")
        raw = data.get("data", {})
        path = data.get("path")
        rval = await self._action[op](path, raw)
        return state, rval


class JsonFileOperator(FreeFlowExt):
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

    async def _read(self, path, raw):
        try:
            async with aiofiles.open(path, "r") as f:
                contents = await f.read()
                j = json.loads(contents)
                return j, 0
        except Exception as ex:
            self._logger.error("Cannot load json file '{}' {}".format(
                path, ex))
            return None, 102

    async def _write(self, path, raw):
        if not isinstance(raw, (dict, list, tuple)):
            self._logger.error("Invalid input format '{}'".format(type(raw)))
            return raw, 101

        try:
            async with aiofiles.open(path, "w") as f:
                await f.write(json.dumps(raw))
                return raw, 0
        except Exception as ex:
            self._logger.error("Cannot write json file '{}' {}".format(
                path, ex))
            return raw, 103

    async def do(self, state, data):
        op = data.get("op", "read")
        raw = data.get("data", {})
        path = data.get("path")
        rval = await self._action[op](path, raw)
        return state, rval


class YamlFileOperator(FreeFlowExt):
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

    async def _read(self, path, raw):
        try:
            async with aiofiles.open(path, "r") as f:
                contents = await f.read()
                j = yaml.safe_load(contents)
                return j, 0
        except Exception as ex:
            self._logger.error("Cannot load yaml file '{}' {}".format(
                path, ex))
            return None, 102

    async def _write(self, path, raw):
        if not isinstance(raw, (dict, list, tuple)):
            self._logger.error("Invalid input format '{}'".format(type(raw)))
            return raw, 101

        try:
            async with aiofiles.open(path, "w") as f:
                await f.write(yaml.safe_dump(raw))
                return raw, 0
        except Exception as ex:
            self._logger.error("Cannot write yaml file '{}' {}".format(
                path, ex))
            return raw, 103

    async def do(self, state, data):
        op = data.get("op", "read")
        raw = data.get("data", {})
        path = data.get("path")
        rval = await self._action[op](path, raw)
        return state, rval


if version_info.major > 3 or (version_info.major == 3 and
                              version_info.minor > 10):
    import tomllib
    import tomli_w

    class TomlFileOperator(FreeFlowExt):
        __typename__ = __TOML_TYPENAME__
        __version__ = "1.0"

        def __init__(self, name, max_tasks=4):
            super().__init__(name, max_tasks=max_tasks)

            self._logger = logging.getLogger(
                ".".join([__name__, self.__typename__, self._name]))
            self._action = {
                "read": self._read,
                "write": self._write,
            }

        async def _read(self, path, raw):
            try:
                async with aiofiles.open(path, "r") as f:
                    contents = await f.read()
                    j = tomllib.loads(contents)
                    return j, 0
            except Exception as ex:
                self._logger.error("Cannot read toml file '{}' {}".format(
                    path, ex))
                return None, 102

        async def _write(self, path, raw):
            if not isinstance(raw, (dict, list, tuple)):
                self._logger.error("Invalid input format '{}'".format(
                    type(raw)))
                return raw, 101

            try:
                async with aiofiles.open(path, "w") as f:
                    await f.write(tomli_w.dump(raw))
                    return raw, 0
            except Exception as ex:
                self._logger.error("Cannot write toml file '{}' {}".format(
                    path, ex))
                return raw, 103

        async def do(self, state, data):
            op = data.get("op", "read")
            raw = data.get("data", {})
            path = data.get("path")
            rval = await self._action[op](path, raw)
            return state, rval
