from asyncio import Semaphore
from contextlib import asynccontextmanager
from datetime import datetime
from os.path import join
from typing import AsyncGenerator, List, Optional
from uuid import uuid4
from aiohttp import ClientSession
from aiofiles import open
from .data_gateway import StorageGateway
from ..models.pdf import PDF
from ..log import log


class QueueGateway(StorageGateway):
    SCHEMA = '''
        CREATE TABLE IF NOT EXISTS pdf (
            id TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            url TEXT NOT NULL,
            created DATETIME NOT NULL,
            updated DATETIME NOT NULL,
            processed BOOLEAN NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_pdf_processed ON PDF (processed);
        CREATE INDEX IF NOT EXISTS idx_pdf_created ON PDF (created);
        CREATE INDEX IF NOT EXISTS idx_pdf_url ON PDF (url);
    '''

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
                id, path, url, processed, created, updated
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
                    url=row[2],
                    processed=row[3],
                    created=row[4],
                    updated=row[5]
                )
                for row in cursor
            ]

    async def download_pdf(self, url: str, sem: Optional[Semaphore] = None):
        log.info(f"Downloading PDF {url}")

        if await self.pdf_exists(url):
            return

        if sem:
            async with sem:
                await self._download_pdf(url, sem)
        else:
            await self._download_pdf(url)

    async def _download_pdf(self, url: str):
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
                url=url,
                processed=False,
                created=now,
                updated=now
            )
            )

    async def pdf_exists(self, url) -> bool:
        query = '''
            SELECT 1 FROM pdf WHERE url = ?
        '''

        async with self.db.execute(query, (url,)) as cursor:
            result = await cursor.fetchone()

        return result is not None

    async def add_pdf(self, pdf: PDF):
        query = '''
            INSERT OR IGNORE INTO pdf
            (id, path, url, processed, created, updated) 
            VALUES
            (?, ?, ?, ?, ?, ?)
        '''

        log.info(f"Saving PDF metadata {pdf.id}")
        await self.db.execute(
            query, (
                pdf.id,
                pdf.path,
                pdf.url,
                pdf.processed,
                pdf.created,
                pdf.updated,
            )
        )
        await self.db.commit()