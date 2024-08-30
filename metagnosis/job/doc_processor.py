from asyncio import as_completed, gather, get_running_loop
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple
from .base import Job
from ..gateway.encoder import EncoderGateway
from ..gateway.document import DocumentGateway
from ..gateway.llm import LLMGateway
from ..gateway.pdf import PDFGateway
from ..log import log
from ..models.document import Document
from ..models.metadata import Metadata
from ..models.task import Task, TaskCategory


class DocumentProcessorJob(Job):
    document: DocumentGateway
    encoder: EncoderGateway
    text_executor: ThreadPoolExecutor
    pdf: PDFGateway
    llm: LLMGateway
    limit: int

    def __init__(self, document: DocumentGateway, pdf: PDFGateway, encoder: EncoderGateway, llm: LLMGateway, limit=None):
        self.document = document
        self.encoder = encoder
        self.text_executor = ThreadPoolExecutor(max_workers=25)
        self.llm = llm
        self.pdf = pdf
        self.limit = limit

    async def perform(self):
        async with await self.pdf.get_pdfs_for_processing(limit=self.limit) as pdfs:
            loop = get_running_loop()

            await gather(*(loop.run_in_executor(p.hydrate_text) for p in pdfs))
            await self.process_documents({
                p.id: Document.from_pdf(p) for p in pdfs
            })

    async def process_documents(self, documents: dict[str, Document]):
        tasks = await as_completed(
            Task.wrap(
                self.encoder.encode([(d.id, d.text) for d in documents.values()]),
                TaskCategory.VECTORIZE_TEXT
            ),
            #*[
            #    Task.Wrap(
            #        self.llm.extract_metadata(d.id, d.text),
            #        TaskCategory.EXTRACT_METADATA,
            #        id=d.id
            #    ) for d in documents.values()
            #]
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