from json import dumps
from typing import List
from sqlite_vec import serialize_float32
from .data_gateway import StorageGateway
from ..log import log
from ..models.document import Document


class DocumentGateway(StorageGateway):
    # TODO add data type (pdf/web) and a ranking:x

    SCHEMA = '''
        CREATE TABLE IF NOT EXISTS document USING vec0(
            id TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            data_type TEXT NOT NULL,
            path TEXT NOT NULL,
            title TEXT NOT NULL,
            score INT NOT NULL,
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
            (id, path, data_type, path, title, score, categories, vector, processed, created, updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = [(
            d.id,
            d.path,
            d.data_type,
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

    async def get_documents_for_processing(self, limit=None):
        # TODO
        raise NotImplemented