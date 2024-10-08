from asyncio import as_completed, gather, get_running_loop
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from os import remove
from typing import List, Tuple
from .base import Job
from ..gateway.encoder import EncoderGateway
from ..gateway.document import DocumentGateway
from ..gateway.pdf import PDFGateway
from ..log import log
from ..models.document import Document
from ..models.task import Task, TaskCategory


class DocumentProcessorJob(Job):
    INTERVAL = 1
    document: DocumentGateway
    encoder: EncoderGateway
    text_executor: ThreadPoolExecutor
    pdf: PDFGateway
    limit: int

    def __init__(
        self,
        document: DocumentGateway,
        pdf: PDFGateway,
        encoder: EncoderGateway,
    ):
        self.limit = cpu_count() * 2
        self.document = document
        self.encoder = encoder
        self.text_executor = ThreadPoolExecutor(max_workers=self.limit)
        self.pdf = pdf

    async def perform(self):
        async with self.pdf.get_pdfs_for_processing(limit=self.limit) as pdfs:
            loop = get_running_loop()

            await gather(
                *[
                    loop.run_in_executor(self.text_executor, p.hydrate_text)
                    for p in pdfs
                ]
            )

            await self.process_documents({p.id: Document.from_pdf(p) for p in pdfs})

    async def process_documents(self, documents: dict[str, Document]):
        encodings = await self.encoder.encode(
            [(d.id, d.text) for d in documents.values()]
        )

        for id, vector in encodings:
            documents[id].vector = vector

        await self.document.save_documents([d for d in documents.values()])
