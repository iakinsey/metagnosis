from abc import ABC, abstractmethod
from asyncio import get_event_loop
from contextlib import asynccontextmanager
from os import makedirs
from aiosqlite import Connection


class StorageGateway(ABC):
    db: Connection
    storage_path: str

    def __init__(self, db: Connection, storage_path: str):
        self.db = db
        self.storage_path = storage_path

    @classmethod
    async def new(cls, db: Connection, storage_path: str):
        makedirs(storage_path, exist_ok=True)

        for q in cls.SCHEMA.split(";"):
            if not q:
                continue

            await db.execute(q)
            await db.commit()

        return cls(db, storage_path)

    @property
    @abstractmethod
    def SCHEMA(self):
        pass

    @asynccontextmanager
    async def transaction(self):
        await self.db.execute("BEGIN TRANSACTION")

        try:
            yield
            await self.db.commit()
        except Exception as e:
            await self.db.rollback()
            raise e

