from .data_gateway import StorageGateway
from ..models.page import Page


class PageGateway(StorageGateway):
    SCHEMA = '''
        CREATE TABLE IF NOT EXISTS Page (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            score INTEGER NOT NULL,
            path TEXT NOT NULL,
            processed BOOLEAN NOT NULL,
            created DATETIME NOT NULL,
            updated DATETIME NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_processed ON Page(processed);
        CREATE INDEX IF NOT EXISTS idx_id ON Page(id);
    '''

    async def get_page_processing_status(self, ids: list[str]) -> list[tuple[bool, bool]]:
        query = f'''
            SELECT
            id, processed
            FROM Page
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


    async def upsert_pages(self, pages: list[Page]):
        query = """
            INSERT INTO Page
            (id, title, url, score, path, processed, created, updated)
            VALUES
            (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                score = excluded.score,
                updated = CURRENT_TIMESTAMP
            WHERE Page.processed = 0
        """

        rows = [(
            p.id,
            p.title,
            p.url,
            p.score,
            p.path,
            False,
            p.created,
            p.updated
        ) for p in pages]

        await self.db.executemany(query, rows)
        await self.db.commit()