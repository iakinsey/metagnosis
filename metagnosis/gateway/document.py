from datetime import datetime
from os import remove
from typing import List
from sqlite_vec import serialize_float32
from .data_gateway import StorageGateway
from ..log import log
from ..models.document import Document


class DocumentGateway(StorageGateway):
    SCHEMA = '''
        CREATE TABLE IF NOT EXISTS document (
            id TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            origin TEXT NOT NULL,
            score INT NOT NULL,
            vector FLOAT[1024] NOT NULL,
            processed BOOLEAN NOT NULL,
            created DATETIME NOT NULL,
            updated DATETIME NOT NULL
        );
    '''

    async def save_documents(self, documents: List[Document], commit=True):
        log.info(f"Saving {len(documents)} documents")

        stmt = """
            INSERT OR REPLACE INTO document
            (id, path, origin, score, vector, processed, created, updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = [(
            d.id,
            d.path,
            d.origin,
            d.score,
            serialize_float32(d.vector),
            d.processed,
            d.created,
            d.updated
        ) for d in documents]

        await self.db.executemany(stmt, params)

        if commit:
            await self.db.commit()

    async def _get_docs(self, origin: str, after: datetime, rank_threshold: int, limit: int) -> list[Document]:
        query = '''
        SELECT
            id, path, origin, score, vector, processed, created, updated
        FROM document
        WHERE processed = FALSE
        AND created > ?
        AND score >= ?
        AND origin = ?
        ORDER BY score DESC
        '''

        if limit:
            query += f"\n LIMIT {limit}"
        
        results = []

        async with self.db.execute(query, (after, rank_threshold, origin)) as cursor:
            async for row in cursor:
                results.append(
                    Document(
                        id=row[0],
                        path=row[1],
                        origin=row[2],
                        score=row[3],
                        vector=row[4],
                        processed=row[5],
                        created=row[6],
                        updated=row[7]
                    )
                )

        return results

    async def _delete_docs(self, ids: list[str]):
        placeholders = ",".join(["?" for _ in ids])
        query = f'''
        UPDATE
            document
        SET processed = TRUE,
        updated = CURRENT_TIMESTAMP
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
                    await self._delete_docs([doc.id for docs in s.to_process for doc in docs])

                    for doc in s.to_process:
                        try:
                            remove(doc.path)
                        except:
                            pass

                await self.transaction().__aexit__(exc_type, exc_val, exc_tb)
            
        return GetDocumentsForProcessingMulti()