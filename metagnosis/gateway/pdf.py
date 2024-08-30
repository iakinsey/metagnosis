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


class PDFGateway(StorageGateway):
    SCHEMA = '''
        CREATE TABLE IF NOT EXISTS pdf (
            id TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            url TEXT NOT NULL,
            origin TEXT NOT NULL,
            title STR,
            score INT,
            error STR,
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
            pdfs = await self._get_pdfs(limit=limit)

            yield pdfs

            log.info("Deleting pdfs from the queue")
            await self._delete_pdfs([i.id for i in pdfs])

    async def _delete_pdfs(self, ids: list[str]):
        placeholders = ','.join(['?' for _ in ids])
        query = f'''
        UPDATE
            pdf
        SET processed = TRUE
        SET updated = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
        '''

        await self.db.execute(query, ids)

    async def _get_pdfs(self, limit=None) -> List[PDF]:
        query = '''
            SELECT
                id, path, url, title, score, error, created, updated, processed, origin
            FROM
                pdf
            WHERE
                processed = FALSE
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
                    title=row[3],
                    score=row[4],
                    error=row[5],
                    created=row[6],
                    updated=row[7],
                    processed=row[8],
                    origin=row[9]
                )
                for row in cursor
            ]

    async def download_pdf(self, url: str, origin: str, sem: Optional[Semaphore] = None, title: str = None, score: int = None):
        log.info(f"Downloading PDF {url}")

        if await self.pdf_exists(url):
            await self._update_pdf(url, title=title, score=score)
            return

        if sem:
            async with sem:
                await self._download_pdf(url, origin, title=title, score=score)
        else:
            await self._download_pdf(url, origin, title=title, score=score)

    async def _download_pdf(self, url: str, origin: str, title: str = None, score: int = None):
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
                origin=origin,
                title=title,
                score=score,
                created=now,
                updated=now,
                processed=False
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
            (id, path, url, title, score, error, created, updated, processed, origin)
            VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                pdf.origin
            )
        )
        await self.db.commit()

    async def update_pdf(self, url: str, title: str = None, score: int = None):
        query = '''
            UPDATE pdf
            SET title = ?,
            score = ?
            WHERE url = ?
        '''

        await self.db.execute(query, (title, score, url))

    async def upesrt_pdf(self, pdfs: list[PDF]):
        query = """
            INSERT INTO pdf
            (id, path, url, title, score, error, created, updated, processed, origin)
            VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                score = excluded.score,
                updated = CURRENT_TIMESTAMP
            WHERE Page.processed = 0
        """
        rows = [(
            pdf.id,
            pdf.path,
            pdf.url,
            pdf.title,
            pdf.score,
            pdf.error,
            pdf.created,
            pdf.updated,
            pdf.processed,
            pdf.origin
        ) for pdf in pdfs]

        await self.db.executemany(query, rows)
        await self.db.commit()

    async def get_processing_status(self, ids: list[str]) -> list[tuple[bool, bool]]:
        query = f'''
            SELECT
            id, processed
            FROM pdf
            WHERE id IN ({",".join("?" for _ in ids)})
            AND processed = TRUE
        '''

        cursor = await self.db.execute(query, ids)
        needs_update = set()
        processed = set()

        async for row in cursor:
            if row['processed']:
                needs_update.add(row['id'])
            else:
                needs_update.add(row['id'])
            
        return [(i in needs_update, i in processed) for i in ids] 
