from asyncio import Semaphore
from contextlib import asynccontextmanager
from datetime import datetime
from os import makedirs
from os.path import join
from typing import AsyncGenerator, List
from uuid import uuid4
from aiohttp import ClientSession
from aiosqlite import Connection
from aiofiles import open
from ..models.pdf import PDF
from ..log import log


SCHEMA = '''
    CREATE TABLE IF NOT EXISTS PDF (
        id TEXT PRIMARY KEY,
        path TEXT NOT NULL,
        created DATETIME NOT NULL,
        updated DATETIME NOT NULL,
        processed BOOLEAN NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_pdf_processed ON PDF (processed);
    CREATE INDEX IF NOT EXISTS idx_pdf_created ON PDF (created);
'''


class QueueGateway:
    db: Connection
    storage_path: str

    def __init__(self, conn: Connection, storage_path: str):
        self.db = conn
        self.storage_path = storage_path

        makedirs(storage_path, exist_ok=True)

    @staticmethod
    async def setup(conn: Connection):
        for q in SCHEMA.split(";"):
            await conn.execute(q)
            await conn.commit()
 
    @asynccontextmanager
    async def transaction(self):
        await self.db.execute("BEGIN TRANSACTION")

        try:
            yield
            await self.db.commit()
        except Exception as e:
            await self.db.rollback()
            raise e

    @asynccontextmanager
    async def get_pdfs_for_processing(self, limit=None) -> AsyncGenerator[List[PDF], None]:
        async with self.transaction():
            log.info("Retrieving pdfs from the queue for processing")
            pdfs = await self._get_pdfs()

            yield pdfs

            log.info("Deleting pdfs from the queue")
            await self._delete_pdfs([i.id for i in pdfs])

    async def _delete_pdfs(self, ids):
        placeholders = ','.join(['?' for _ in ids])
        query = f'''
        UPDATE
            pdf
        SET processed = TRUE
        SET updated = DATETIME('now)
        WHERE id IN ({placeholders})
        '''

        await self.db.execute(query, ids)

    async def _get_pdfs(self, limit=None) -> List[PDF]:
        query = '''
            SELECT
                id, path, processed, created, updated
            FROM
                pdf
            WHERE
                processed = true
            ORDER BY created DESC 
        '''

        if limit:
            query += f"\n LIMIT {limit}"

        async with self.db.execute(query) as cursor:
            return [
                PDF(
                    id=row[0],
                    path=row[1],
                    processed=row[2],
                    created=row[3],
                    updated=row[4]
                )
                for row in cursor
            ]

    async def download_pdf(self, url: str, sem: Semaphore):
        log.info(f"Downloading PDF {url}")

        async with sem:
            pdf_id = str(uuid4())
            path = join(self.storage_path, pdf_id)
            now = datetime.now()

            async with ClientSession() as client:
                async with client.get(url) as response:
                    response.raise_for_status()
                    log.info(f"Saving {url} to {path}")

                    async with open(path, 'wb') as f:
                        async for chunk in response.content.iter_any():
                            await f.write(chunk)
            
            await self.add_pdf(
                PDF(
                    id=pdf_id,
                    path=path,
                    processed=False,
                    created=now,
                    updated=now
                )
            )

    async def add_pdf(self, pdf: PDF):
        query = '''
            INSERT INTO pdf
            (id, path, processed, created, updated) 
            VALUES
            (?, ?, ?, ?, ?)
        '''

        log.info(f"Saving PDF metadata {pdf.id}")
        await self.db.execute(
            query, (
                pdf.id,
                pdf.path,
                pdf.processed,
                pdf.created,
                pdf.updated,
            )
        )
        await self.db.commit()

    """
    @asynccontextmanager
    async def get_articles_for_processing(self) -> List[Article]:
        async with self.transaction():
            pass

    async def add_articles(self):
        pass
    """