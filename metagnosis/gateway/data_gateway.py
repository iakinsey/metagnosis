from abc import ABC, abstractmethod
from asyncio import Lock
from contextlib import asynccontextmanager
from os import makedirs
from aiosqlite import Connection
from ..config import get_config, Config


class StorageGateway(ABC):
    db: Connection
    storage_path: str
    process_lock: Lock
    config: Config

    def __init__(self, db: Connection, storage_path: str, process_lock: Lock):
        self.config = get_config()
        self.process_lock = process_lock
        self.db = db
        self.storage_path = storage_path

    @classmethod
    async def new(cls, db: Connection, storage_path: str, process_lock: Lock):
        makedirs(storage_path, exist_ok=True)

        for q in cls.SCHEMA.split(";"):
            if not q:
                continue

            await db.execute(q)
            await db.commit()

        return cls(db, storage_path, process_lock)

    @property
    @abstractmethod
    def SCHEMA(self):
        pass

    @asynccontextmanager
    async def transaction(self):
        async with self.process_lock:
            try:
                yield
                await self.db.commit()
            except Exception as e:
                await self.db.rollback()
                raise e
