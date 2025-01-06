from .types import FreeFlowExt
import os
import logging

__TYPENAME__ = "EnvOperator"


class EnvOperator(FreeFlowExt):
    __typename__ = __TYPENAME__
    __version__ = "1.0"

    def __init__(self, name, vars=[], max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))
        self._vars = vars

    async def do(self, state, data):
        rval = {}
        for v in self._vars:
            rval[v] = os.getenv(v)
        return state, (rval, 0)
