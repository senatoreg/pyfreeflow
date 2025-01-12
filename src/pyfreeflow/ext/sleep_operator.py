from .types import FreeFlowExt
import random
import logging
import asyncio

__TYPENAME__ = "SleepOperator"


"""
run parameter:
{
  "state": { ... },
  "data": {}
}
"""


class SleepOperatorV1_0(FreeFlowExt):
    __typename__ = __TYPENAME__
    __version__ = "1.0"

    def __init__(self, name, sleep=5, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)
        self._sleep = sleep

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

    async def run(self, state, data):
        self._logger.debug("%s sleeping for %d", self._name, self._sleep)
        await asyncio.sleep(self._sleep)

        return state, data


class RandomSleepOperatorV1_0(FreeFlowExt):
    __typename__ = "Random" + __TYPENAME__
    __version__ = "1.0"

    def __init__(self, name, sleep_min=5, sleep_max=10, max_tasks=4):
        super().__init__(name, max_tasks=max_tasks)
        self._sleep_min = sleep_min
        self._sleep_max = sleep_max

        self._logger = logging.getLogger(".".join([__name__, self.__typename__,
                                                   self._name]))

    async def run(self, state, data):
        t = random.randint(self._sleep_min, self._sleep_max)
        self._logger.debug("%s sleeping for %d", self._name, t)
        await asyncio.sleep(t)

        return state, data
