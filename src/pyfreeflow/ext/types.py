from ..registry import ExtRegister
import asyncio

"""
run parameter:
{
  "state": { ... },
  "param": {
    <CLASS SPECIFIC PARAMETERS>
  }
}
"""


class FreeFlowExt(metaclass=ExtRegister):
    def __init__(self, name, max_tasks=4):
        self._name = name
        self._max_tasks = max_tasks

    async def do(self, state, data):
        raise NotImplementedError

    async def unpack(self, state, data):
        if isinstance(data, list):
            loop = asyncio.get_running_loop()

            # [param0, param1, ...]
            cur = self._max_tasks
            _data = []
            aws = []

            for i, p in enumerate(data):
                if cur == 0:
                    await asyncio.wait(aws, loop=loop,
                                       return_when=asyncio.FIRST_COMPLETED)
                    cur += 1

                if p[1] == 0:
                    aws.append(loop.create_task(
                        self.do(state, p[0]),
                        name=self._name + "-unpack-" + str(i)))
                    cur -= 1

            await asyncio.wait(aws, loop=loop,
                               return_when=asyncio.ALL_COMPLETED)

            for task in aws:
                state, p = await task
                _data.append(p)
            return state, _data
        else:
            # param0 or param1 or ...
            if data[1] == 0:
                return await self.do(state, data[0])
            return state, data

    async def run(self, state, data={}):
        return await self.unpack(state, data)
