from .registry import ExtRegistry
import networkx as nx
import io
import copy
import asyncio
import logging

"""
Example of configuratio file

last: "D"  # Optional
node:
- name: "A"
  type: "RestApiRequester"
  version: "1.0"
  config: {}
- name: "B"
  type: "DataTransformer"
  version: "1.0"
  config: {}
- name: "C"
  type: "RestApiRequester"
  version: "1.0"
  config: {}
- name: "D"
  type: "DataTransformer"
  version: "1.0"
  config: {}
digraph:
  - A -> B
  - B -> C
  - A -> D
"""


class Pipeline():
    def __init__(self, node, digraph, last=None, name="stream"):
        self._name = name
        self._registry = {}
        self._data = {}
        self._state = {}
        self._last = last
        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition()

        self._logger = logging.getLogger(".".join([__name__, "Pipeline",
                                                   self._name]))

        for cls in node:
            cls_name = cls.get("name")
            cls_config = cls.get("config", {})
            cls_type = cls.get("type")
            cls_version = cls.get("version")

            assert (cls_name not in self._registry.keys())

            self._registry[cls_name] = ExtRegistry.get_registered_class(
                cls_type, cls_version)(cls_name, **cls_config)

        dot = io.StringIO("digraph D {" + "\n".join(digraph) + "}")
        self._G = nx.nx_pydot.read_dot(dot)

        self._tree = list(nx.topological_sort(self._G))

    async def _cleanup(self):
        t = self._state
        self._state = {}
        del t

        t = self._data
        self._data = {}
        del t

    async def _task(self, n, _data):
        try:
            self._state, self._data[n] = await self._registry[n].run(
                self._state, _data)

        except Exception as ex:
            self._logger.error(ex)
        finally:
            async with self._cond:
                self._cond.notify()

    async def run(self, data={}):
        async with self._lock:
            degrees = {x[0]: x[1] for x in self._G.in_degree()}
            loop = asyncio.get_running_loop()

            pending = len(self._tree)
            task = {}

            while pending > 0:
                nodes = [k for k, v in degrees.items() if v == 0]
                for n in nodes:
                    _prev = list(self._G.predecessors(n))
                    if len(_prev) > 1:
                        _data = [self._data.get(x) for x in _prev]
                    elif len(_prev) == 1:
                        _data = self._data.get(_prev[0])
                    else:
                        _data = (data, 0)

                    task[n] = loop.create_task(self._task(n, _data),
                                               name=n)
                    degrees[n] -= 1

                async with self._cond:
                    await self._cond.wait()

                nodes.clear()
                for tname, t in {k: v for k, v in task.items() if v.done()}.items():
                    degrees[tname] -= 1
                    pending -= 1
                    del task[tname]
                    for succ in self._G.successors(tname):
                        degrees[succ] -= 1

            if self._last is not None:
                _data = self._data.get(self._last, {})
            else:
                _data = self._data.get(self._tree[-1], {})

            rep = (copy.deepcopy(_data[0]), _data[1])

            await self._cleanup()
        return rep
