from .base import Job
from ..gateway.document import DocumentGateway


class PublisherJob(Job):
    document: DocumentGateway

    def __init__(self, document: DocumentGateway):
        self.document = document

    async def perform(self):
        async with self.document.get_documents_for_processing() as docs:
            # Rank HN posts and select the top ones
            # Break arxiv into clusters, determine centers of each one, determine most novel
            pass