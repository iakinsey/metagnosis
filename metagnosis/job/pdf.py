from asyncio import as_completed
from typing import List, Tuple
from .base import Job
from ..gateway.encoder import EncoderGateway
from ..gateway.document import DocumentGateway
from ..gateway.llm import LLMGateway
from ..gateway.queue import QueueGateway
from ..log import log
from ..models.document import Document
from ..models.metadata import Metadata
from ..models.task import Task, TaskCategory


class PDFProcessorJob(Job):
    document: DocumentGateway
    encoder: EncoderGateway
    queue: QueueGateway
    llm: LLMGateway
    limit: int

    def __init__(self, document: DocumentGateway, queue: QueueGateway, encoder: EncoderGateway, llm: LLMGateway, limit=None):
        self.document = document
        self.encoder = encoder
        self.llm = llm
        self.queue = queue
        self.limit = limit

    async def perform(self):
        async with await self.queue.get_pdfs_for_processing(limit=self.limit) as pdfs:
            documents = {
                p.id: Document.from_pdf(p) for p in pdfs
            }
            tasks = await as_completed(
                Task.wrap(
                    self.encoder.encode([(d.id, d.text) for d in documents.values()]),
                    TaskCategory.VECTORIZE_TEXT
                ),
                *[
                    Task.Wrap(
                        self.llm.extract_metadata(d.id, d.text),
                        TaskCategory.EXTRACT_METADATA,
                        id=d.id
                    ) for d in documents.values()
                ]
            )

            for coro in as_completed(tasks):
                task: Task = await coro 

                if task.category == TaskCategory.VECTORIZE_TEXT:
                    await self.process_vectorize(task, documents)
                elif task.category == TaskCategory.EXTRACT_METADATA:
                    await self.process_extract(task, documents[task.id])

            await self.document.save_documents(
                [d for d in documents if d.title is not None]
            )

    def process_extract(self, task: Task, doc: Document):
        meta: Metadata = task.result

        if task.error:
            log.exception(task.error)
        
        doc.categories = meta.tags
        doc.title = meta.title

    def process_vectorize(self, task: Task, docs: Document):
        if task.error:
            raise task.error

        results: List[Tuple[str, List[float]]] = task.result

        for id, vector in results:
            docs[id].vector = vector