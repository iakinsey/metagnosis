from abc import ABC, abstractmethod

class Strategy(ABC):
    @abstractmethod
    async def perform(self):
        pass

    async def start(self):
        pass