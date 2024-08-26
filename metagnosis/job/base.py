from abc import ABC, abstractmethod
from asyncio import sleep
from ..log import log

class Job(ABC):
    INTERVAL = 60

    @abstractmethod
    async def perform(self):
        pass

    async def start(self):
        while 1:
            log.info("Job {} tick".format(self.__class__.__name__))
            await self.perform()
            await sleep(self.INTERVAL)