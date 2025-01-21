from .types import FreeFlowExt
import logging
import datetime as dt
import xxhash
from decimal import Decimal
from cryptography.fernet import Fernet
from ..utils import deepupdate, DurationParser, EnvVarParser, DateParser

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

    def __init__(self, name, transformer="", secret=None, force=False,
                 max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)
        self._force = force
        self._env = self._create_safe_lua_env()
        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

        self._env.globals().safe_env["xxh3_64"] = xxhash.xxh3_64_hexdigest
        self._env.globals().safe_env["xxh3_128"] = xxhash.xxh3_128_hexdigest
        self._env.globals().safe_env["now"] = self._dt_now_ts
        self._env.globals().safe_env["timedelta"] = self._dt_delta_ts
        self._env.globals().safe_env["parsedatetime"] = self._dt_parsedt_ts

        if secret is not None:
            with open(EnvVarParser.parse(secret), "rb") as f:
                self._cipher = Fernet(f.read())
            self._env.globals().safe_env["encrypt"] = self._encrypt
            self._env.globals().safe_env["decrypt"] = self._decrypt

        try:
            self._transformer = self._env.globals().eval_safe(
                "\n".join(["function f(state, data)",
                           transformer,
                           "return state, data",
                           "end"]))
        except Exception as ex:
            self._logger.error(ex)

        assert (self._transformer is not None)

    def __str__(self):
        return "{typ}(name: {n}, version: {v})".format(
            typ=self.__typename__, n=self._name, v=self.__version__)

    def _create_safe_lua_env(self):
        lua = lupa.LuaRuntime(unpack_returned_tuples=True)

        lua.execute("""
          local dummy = function(...) end

          local regex = {
            escape = function (text)
              return text:gsub("([%%%^%$%(%)%.%[%]%*%+%-%?])", "%%%1")
            end
          }

          local null_mt = {
            __metatable = "null",
            __tostring = function() return "null" end,
            __newindex = function(t, key, value) end,
            __usedindex = function(t, key, value) end
          }

          array_mt = {
            __metatable = "array",
            __newindex = function(t, key, value)
              if type(key) ~= "number" then
                error("index value must be a number", 2)
              end
              if key < 1 then
                error("index value must greater than 0", 2)
              end
              rawset(t, key, value)
            end,
            __usedindex = function(t, key, value)
              if type(key) ~= "number" then
                error("index value must be a number", 2)
              end
              if key < 1 then
                error("index value must greater than 0", 2)
              end
              rawset(t, key, value)
            end
          }

          map_mt = {
            __metatable = "map",
          }

          local __table = {}
          __table.sort = table.sort
          __table.maxn = table.maxn
          __table.unpack = table.unpack
          __table.pack = table.pack
          __table.insert = function(t, ...)
            if getmetatable(t) ~= "array" then
              error("array expected", 3)
            end
            return table.insert(t, ...)
          end
          __table.remove = function(t, ...)
            if getmetatable(t) ~= "array" then
              error("array expected", 3)
            end
            return table.remove(t, ...)
          end
          __table.concat = function(t, ...)
            if getmetatable(t) ~= "array" then
              error("array expected", 3)
            end
            return table.concat(t, ...)
          end
          if table.move then
            __table.move = function(s, sp, tp, n, t)
              if getmetatable(t) ~= "array" then
                error("array expected", 3)
              end
              return table.move(s, sp, tp, n, t)
            end
          end
          local __rawset = function(t, ...)
            if getmetatable(t) ~= "null" then
              return rawset(t, ...)
            end
          end
          local __rawget = function(t, ...)
            if getmetatable(t) ~= "null" then
              return rawget(t, ...)
            end
          end

          local isnull = function(t)
            return getmetatable(t) == "null"
          end

          __table.ismap = function(t)
            return getmetatable(t) == "map"
          end

          __table.isarray = function(t)
            return getmetatable(t) == "array"
          end

          -- Aggiungi table.find se non esiste
          __table.find = function(t, value, start_index)
            if getmetatable(t) == "array" then
              local si = start_index or 1

              if type(si) ~= "number" or si < 1 then
                error("start_index must be a number greater than 0", 2)
              end

              for i = si, #t do
                if t[i] == value then
                  return i
                end
              end

            else
              for k, v in pairs(t) do
                if v == value then
                  return k
                end
              end
            end

            return nil
          end

          local null = setmetatable({}, null_mt)
          local array = function(t)
            if type(t) == "table" then
              local self = setmetatable({}, array_mt)
              for i, d in ipairs(t) do
                __table.insert(self, d)
              end
              return self
            end
            return setmetatable({}, array_mt)
          end
          local map = function(t)
            if type(t) == "table" then
              return setmetatable(t, map_mt)
            end
            return setmetatable({}, map_mt)
          end

          __table.map = function(t, f)
            local u = array()
            for i, d in ipairs(t) do
              u[i] = f(d)
            end
            return u
          end

          __table.tail = function(t, index)
            if getmetatable(t) ~= "array" then
              error("array expected", 3)
            end
            index = index or 2
            local u = array()
            for i = index, #t do
              table.insert(u, t[i])
            end
            return u
          end

          string.qsplit = function(s, pattern, keepqmark)
            pattern = pattern or "%s"
            keepqmark = keepqmark ~= nil and keepqmark or false
            local squote = false
            local dquote = false
            local t = array()
            local buf = ""
            local start = 1

            while start <= #s do
              local m, M = string.find(s, pattern, start)
              local subs = string.sub(s, start, (m and m - 1 or nil))
              local captured = string.sub(s, m or start, M or -1)

              local c, p
              for i = 1, #subs do
                p = c
                c = string.sub(subs, i, i)
                if c == "'" and not dquote and p ~= "\\\\\" then
                  squote = not squote
                  if keepqmark then buf = buf .. c end
                elseif c == "\\"" and not squote and p ~= "\\\\\" then
                  dquote = not dquote
                  if keepqmark then buf = buf .. c end
                elseif c ~= "\\\\" or p == "\\\\" then
                  buf = buf .. c
                end
              end

              if squote or dquote then
                buf = buf .. captured
              else
                __table.insert(t, buf)
                buf = ""
              end

              if not M then break end
              start = M + 1
            end
            return t
          end

          string.esplit = function(s, pattern)
            return string.qsplit(s, pattern, true)
          end

          local serialize
          serialize = function(t)
            local str = "{"
            for k, v in pairs(t) do
              str = str .. " "
              if (type(k) == "number") then
                str = str .. "[" .. k .. "] = "
              elseif (type(k) == "string") then
                str = str  .. k ..  "= "
              end
              if (type(v) == "number") then
                str = str .. v .. ", "
              elseif (type(v) == "string") then
                str = str .. "\\"" .. v .. "\\", "
              elseif (type(v) == "table") then
                if isnull(v) then
                  str = str .. tostring(v) .. ", "
                else
                  str = str .. serialize(v) .. ", "
                end
              else
                str = str .. "\\"" .. tostring(v) .. "\\", "
              end
            end
            str = str .. "}"
            return str
          end

          local securexmlparser = {}
          securexmlparser.getelem = function(a, b, c)
            if isnull(a) then return null end

            if #b > 0 then
              local e = a.elem and a.elem[b[1]] or map()
              return securexmlparser.getelem(e, __table.tail(b, 2), c)
            end

            if __table.isarray(a) then
              local r = array()
              for i, d in ipairs(a) do
                local val = c and d[c] or d
                __table.insert(r, val)
              end
              return r
            end

            return c and a[c] or a
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
            table = __table,
            serialize = serialize,
            print = print,
            regex = regex,
            null = null,
            isnull = isnull,
            map = map,
            array = array,
            securexmlparser = securexmlparser,
          }

          -- Funzione per valutare codice in ambiente sicuro
          eval_safe = function(code)
            local f, e = load(code, "safenv", "t", safe_env)
            if not f then
              print("Error loading code: " .. tostring(e))
              return nil
            end
            f()
            local success, result = pcall(f)
            if not success then
              print("Error executing code: " .. tostring(result))
              return nil
            end
            return safe_env.f
          end
        """)

        return lua

    def _dt_parsedt_ts(self, a, fmt=None):
        if fmt is None:
            d = DateParser.parse_date(a)
        d = DateParser.parse_date(a, fmt=[fmt])
        return d * 1000000 if d is not None else None

    def _dt_now_ts(self):
        return dt.datetime.now(dt.timezone.utc).timestamp() * 1000000

    def _dt_delta_ts(self, duration):
        return DurationParser.parse(duration)

    def _encrypt(self, value):
        return self._cipher.encrypt(value.encode("utf-8"))

    def _decrypt(self, value):
        return self._cipher.decrypt(value).decode("utf-8")

    def _lua_null_to_none(self, x):
        return str(x) == "null"

    def _lua_to_py(self, a):
        if lupa.lua_type(a) == "table":
            if self._env.globals().getmetatable(a) == "null":
                return None
            elif self._env.globals().getmetatable(a) == "array":
                return [self._lua_to_py(v) for v in a.values()]
            elif self._env.globals().getmetatable(a) == "map":
                return {k: self._lua_to_py(v) for k, v in a.items()}
            else:
                return {k: self._lua_to_py(v) for k, v in a.items()}
        else:
            return a

    def _py_to_lua(self, a):
        if isinstance(a, dict):
            t = self._env.table_from({k: self._py_to_lua(v) for k, v in a.items()})
            self._env.globals().setmetatable(
                t, self._env.globals()["map_mt"])
            return t
        elif isinstance(a, (list, tuple)):
            li = self._env.table_from([self._py_to_lua(v) for v in a])
            self._env.globals().setmetatable(
                li, self._env.globals()["array_mt"])
            return li
        elif isinstance(a, Decimal):
            return float(a)
        elif a is None:
            return self._env.globals().safe_env['null']
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
