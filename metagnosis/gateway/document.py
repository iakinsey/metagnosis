from datetime import datetime
from json import dumps
from typing import List
from sqlite_vec import serialize_float32
from .data_gateway import StorageGateway
from ..log import log
from ..models.document import Document


class DocumentGateway(StorageGateway):
    SCHEMA = '''
        CREATE TABLE IF NOT EXISTS document USING vec0(
            id TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            origin TEXT NOT NULL,
            title TEXT NOT NULL,
            score INT NOT NULL,pa
            categories TEXT NOT NULL,
            vector FLOAT[1024] NOT NULL,
            processed BOOLEAN NOT NULL,
            created DATETIME NOT NULL,
            updated DATETIME NOT NULL
        );
    '''

    async def save_documents(self, documents: List[Document], commit=True):
        log.info(f"Saving {len(documents)} documents")

        stmt = """
            INSERT INTO document
            (id, path, origin, title, score, categories, vector, processed, created, updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = [(
            d.id,
            d.path,
            d.origin,
            d.path,
            d.title,
            d.score,
            dumps(d.categories),
            serialize_float32(d.vector),
            d.processed,
            d.created,
            d.updated
        ) for d in documents]

        await self.db.executemany(stmt, params)

        if commit:
            await self.db.commit()

    async def _get_docs(self, origin: str, after: datetime, rank_threshold: int, limit: int = None) -> list[Document]:
        query = '''
        SELECT
            id, path, origin, title, score, categories, vector, processed, created, updated
        FROM document
        WHERE processed = FALSE
        AND created > ?
        AND rank_threshold >= ?
        AND origin = ?
        ORDER BY score DESC
        '''

        if limit:
            query += f"\n LIMIT {limit}"

        async with self.db.execute(query, after, rank_threshold, origin) as cursor:
            return [
                Document(
                    id=row[0],
                    path=row[1],
                    origin=row[2],
                    title=row[3],
                    score=row[4],
                    categories=row[5],
                    vector=row[6],
                    processed=row[7],
                    created=row[8],
                    updated=row[9]
                ) for row in cursor
            ]

    async def _delete_docs(self, ids: list[str]):
        placeholders = ",".join(["?" for _ in ids])
        query = f'''
        UPDATE
            document
        SET processed = TRUE
        SET updated = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
        '''

        await self.db.execute(query, ids)

    async def get_documents_for_processing_multi(self, args_multi):
        to_process = []

        class GetDocumentsForProcessingMulti:
            async def __aenter__(s) -> List[List[Document]]:
                await self.transaction().__aenter__()
                log.info("Retrieving documents from the queue for processing")
                s.to_process = []

                for args in args_multi:
                    to_process.append(await self._get_docs(*args))

                return to_process

            async def __aexit__(s, exc_type, exc_val, exc_tb):
                if exc_type is None:
                    log.info("Document processing failed")
                else:
                    log.info("Document processing success")
                    log.info("Deleting docs from the queue")
                    await self._delete_docs([doc.id for docs in self.to_process for doc in docs])

                await self.transaction().__aexit__(exc_type, exc_val, exc_tb)
            
        return GetDocumentsForProcessingMulti()