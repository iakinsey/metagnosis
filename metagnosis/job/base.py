from abc import ABC, abstractmethod
from datetime import datetime
from asyncio import sleep


class Job(ABC):
    INTERVAL = 60
    current: datetime
    last: datetime

    @abstractmethod
    async def perform(self):
        pass

    def set_run_times(self, last: datetime, current: datetime):
        self.last_run_time = last
        self.current_run_time = current
