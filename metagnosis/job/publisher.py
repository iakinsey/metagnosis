import numpy as np

from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
from pydantic import BaseModel
from .base import Job
from .arxiv import TOPICS
from ..gateway.document import DocumentGateway
from ..models.document import Document


class ArxivClusters(BaseModel):
    ids: list[str]
    vectors: np.array
    kmeans: KMeans
    clusters: int = len(TOPICS)


class PublisherJob(Job):
    document: DocumentGateway
    hn_limit: int = 25
    arxiv_limit: int = 25
    hn_rank_threshold: int = 100

    def __init__(self, document: DocumentGateway):
        self.document = document

    async def perform(self):
        args_multi = [
            ("Hacker News", self.last_run_time, self.hn_rank_threshold, self.hn_limit)
            ("arxiv", self.last_run_time, 0, None)
        ]

        async with self.document.get_documents_for_processing_multi(args_multi) as docs_multi:
            docs = self.get_relevant_docs(docs_multi)
            # Generate front page with image
            # Merge pdfs
            # Send to publishing api 
    
    def get_relevant_docs(self, docs_multi) -> list[Document]:
        hn_docs, arxiv_docs = docs_multi
        arxiv_docs = self.filter_arxiv_docs(arxiv_docs)

        return hn_docs + arxiv_docs

    def filter_arxiv_docs(self, docs: list[Document]):
        arxiv = self.get_arxiv_cluster(docs)
        ids = self.get_interesting_arxiv_papers(arxiv)

        return [d for d in docs if d.id in ids]

    def get_arxiv_cluster(self, docs: list[Document]) -> ArxivClusters:
        data = {d.id: d.vector for d in docs}
        ids = list(data.keys())
        vectors = np.array(list(data.values()))
        kmeans = KMeans(n_clusters=ArxivClusters.clusters, random_state=56)

        kmeans.fit(vectors)

        return ArxivClusters(
            ids=ids,
            vectors=vectors,
            kmeans=kmeans
        )
    
    def get_intersting_arxiv_papers(self, arxiv: ArxivClusters) -> set[str]:
        centers = arxiv.kmeans.cluster_centers_
        labels = arxiv.kmeans.labels_
        distances = cdist(arxiv.vectors, centers, 'euclidean')
        ids = set()

        for i in range(arxiv.clusters):
            # Novelty
            indices = np.where(labels == i)[0]
            vectors = vectors[indices]
            inner_distances = cdist(vectors, vectors, 'euclidean')
            avg_distances = np.mean(inner_distances, axis=1)
            novel_index = np.argmax(avg_distances)
            ids.add(arxiv.ids[indices[novel_index]])

            # Centrality
            index = np.argmin(distances[:, i]) 
            ids.add(arxiv.ids[index])

        return ids 