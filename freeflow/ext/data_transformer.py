from .types import FreeFlowExt
import logging
import datetime as dt
from ..utils import deepupdate

try:
    import lupa.luajit21 as lupa
except ImportError:
    try:
        import lupa.lua54 as lupa
    except ImportError:
        try:
            import lupa.lua53 as lupa
        except ImportError:
            import lupa.lua as lupa

__TYPENAME__ = "DataTransformer"


"""
run parameter:
{
  "state": { ... },
  "data": { ... }
}
"""


class DataTransformerV1_0(FreeFlowExt):
    __typename__ = __TYPENAME__
    __version__ = "1.0"

    def __init__(self, name, transformer="", lua_func=[],
                 force=False, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)
        self._force = force
        self._env = self._create_safe_lua_env()

        self._env.globals().safe_env["now"] = self._dt_now_ts
        self._env.globals().safe_env["timedelta"] = self._dt_delta_ts
        self._env.globals().safe_env["dict"] = dict
        self._env.globals().safe_env["list"] = list

        for _name, _func in lua_func:
            self._env.globals().safe_env[_name] = _func

        self._transformer = self._env.globals().eval_safe(
            "\n".join(["function f(state, data)",
                       transformer,
                       "return state, data",
                       "end"]))

        assert (self._transformer is not None)
        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

    def __str__(self):
        return "{typ}(name: {n}, version: {v})".format(
            typ=self.__typename__, n=self._name, v=self.__version__)

    def _create_safe_lua_env(self):
        lua = lupa.LuaRuntime(unpack_returned_tuples=True)

        lua.execute("""
            if not table.find then
              function table.find(t, value, start_index)
                local si = start_index or 1
                for i = si, #t do
                  if t[i] == value then
                    return i
                  end
                end
                return nil
              end
            end

            if not string.split then
              function string.split(s, pattern)
                local p = pattern or "%S+"
                local t = {}
                for m in string.gmatch(s, p) do
                  table.insert(t, m)
                end
                return t
              end
            end

            safe_env = {
              assert = assert,
              pairs = pairs,
              ipairs = ipairs,
              next = next,
              type = type,
              tostring = tostring,
              tonumber = tonumber,
              string = string,
              math = math,
              table = table,
              print = print,
            }

            function eval_safe(code)
              local f, e = load(code, "config", "t", safe_env)
              if not f then
                print(e)
                return nil
              end
              f()
              return safe_env.f
            end
        """)

        return lua

    def _dt_now_ts(self):
        return int(dt.datetime.now(dt.UTC).timestamp())

    def _dt_delta_ts(self, days=0, seconds=0, microseconds=0, milliseconds=0,
                     minutes=0, hours=0, weeks=0):
        return int(dt.timedelta(days=days, seconds=seconds,
                                microseconds=microseconds,
                                milliseconds=milliseconds,
                                minutes=minutes, hours=hours,
                                weeks=weeks).total_seconds())

    def _lua_to_py(self, a):
        if lupa.lua_type(a) == "table":
            if len(a) == 0:
                return {k: self._lua_to_py(v) for k, v in a.items()}
            elif all([isinstance(x, int) for x in a.keys()]):
                return [self._lua_to_py(v) for v in a.values()]
            else:
                return {}
        else:
            return a

    def _py_to_lua(self, a):
        if isinstance(a, dict):
            return self._env.table_from({k: self._py_to_lua(v) for k, v in a.items()})
        elif isinstance(a, (list, tuple)):
            return self._env.table_from([self._py_to_lua(v) for v in a])
        else:
            return a

    async def run(self, state, data=({}, 0)):
        if isinstance(data, list):
            _data = [x[0] for x in data if x[1] == 0]
            err = len(_data) == 0
        else:
            _data = data[0]
            err = data[1] != 0

        if err:
            return state, (None, 103)

        try:
            s, d = self._transformer(self._py_to_lua(state),
                                     self._py_to_lua(_data))

            s = self._lua_to_py(s)
            d = self._lua_to_py(d)

            deepupdate(state, s)
            if not self._force:
                return state, (d, 0)
            else:
                return state, d

        except Exception as ex:
            self._logger.error(ex)
            return state, (None, 101)
