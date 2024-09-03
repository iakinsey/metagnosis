from datetime import datetime
from json import dumps
from os import remove
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from uuid import uuid4

import numpy as np

from aioboto3 import Session
from aiohttp import ClientResponseError, ClientSession
from fpdf import FPDF
from PIL import Image
from PyPDF2 import PdfMerger
from pydantic import BaseModel, ConfigDict
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
from .base import Job
from .arxiv import TOPICS
from ..config import get_config, PublishCredentials
from ..gateway.document import DocumentGateway
from ..gateway.image_gen import ImageGenerationGateway
from ..models.document import Document


class ArxivClusters(BaseModel):
    ids: list[str]
    vectors: Any
    kmeans: KMeans
    clusters: int
    model_config = ConfigDict(arbitrary_types_allowed=True)


class PublisherJob(Job):
    document: DocumentGateway
    hn_limit: int = 25
    arxiv_limit: int = 25
    hn_rank_threshold: int = 100
    image: ImageGenerationGateway
    aws_access_key_id: str
    aws_secret_access_key: str
    s3_bucket: str
    publish_creds: PublishCredentials

    def __init__(self, document: DocumentGateway):
        config = get_config()
        self.document = document
        self.image = ImageGenerationGateway()
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.s3_bucket = config.s3_bucket
        self.publish_creds = config.publish_creds
        self.lulu_auth = config.lulu_auth
 
    async def perform(self):
        args_multi = [
            ("Hacker News", self.last_run_time, self.hn_rank_threshold, self.hn_limit),
            ("arxiv", self.last_run_time, 0, None)
        ]

        async with self.document.get_documents_for_processing_multi(args_multi) as docs_multi:
            docs = self.get_relevant_docs(docs_multi)

            if not docs:
                return

            cover_page_path = self.get_cover_page()
            body = self.merge_pdfs(docs)
            cover_path = await self.upload_to_s3(cover_page_path)
            body_path = await self.upload_to_s3(body.name)
            
            await self.publish_book(cover_path, body_path)

    async def get_lulu_auth(self) -> str:
        url = "https://api.lulu.com/auth/realms/glasstree/protocol/openid-connect/token"
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': self.lulu_auth
        }
        data = {
            'grant_type': 'client_credentials'
        }

        async with ClientSession() as session:
            async with session.post(url, headers=headers, data=data) as response:
                return await response.json()['access_token']

    async def publish_book(self, cover_path: str, body_path: str) -> str:
        auth = await self.get_lulu_auth()
        url = "https://api.lulu.com/print-jobs/"
        headers = {
            'Authorization': f'Bearer {auth}',
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/json'
        }
        external_id = str(uuid4())
        payload = {
            "contact_email": self.publish_creds.email,
            "external_id": external_id,
            "line_items": [
                {
                    "external_id": external_id,
                    "printable_normalization": {
                        "cover": {
                            "source_url": cover_path
                        },
                        "interior": {
                            "source_url": body_path
                        },
                        "pod_package_id": self.publish_creds.package_id
                    },
                    "quantity": 1,
                    "title": f"Metagnosis {datetime.now().strftime('%Y-%m-%d')}"
                }
            ],
            "production_delay": 120,
            "shipping_address": {
                "city": self.publish_creds.city,
                "country_code": self.publish_creds.country_code,
                "name": self.publish_creds.name,
                "phone_number": self.publish_creds.phone_number,
                "postcode": self.publish_creds.postcode,
                "state_code": self.publish_creds.state_code,
                "street1": self.publish_creds.street1,
                "street2": self.publish_creds.street2
            },
            "shipping_level": self.publish_creds.shipping_level
        }

        print("publishing book")
        return

        async with ClientSession() as session:
            async with session.post(url, headers=headers, data=dumps(payload)) as response:
                try:
                    response.raise_for_status()
                except ClientResponseError as e:
                    error_response = await response.text()

                    raise RuntimeError(f"Lulu Error: {e.status}, Details: {error_response}")


    async def uplodad_to_s3(self, file_name: str):
        object_name = Path(file_name).name
        session = Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
        
        async with session.client('s3') as s3_client:
            await s3_client.upload_file(file_name, self.s3_bucket, object_name)
        
        await s3_client.put_object_acl(Bucket=self.s3_bucket, Key=object_name, ACL='public-read')

        presigned_url = await s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': self.s3_bucket, 'Key': object_name},
            ExpiresIn=604800
        )

        return presigned_url

    def merge_pdfs(self, docs: list[Document]) -> NamedTemporaryFile:
        temp_file = NamedTemporaryFile(delete=True, suffix='.pdf')
        merger = PdfMerger()

        for pdf_file in [d.path for d in docs]:
            merger.append(pdf_file)

        merger.write(temp_file)
        merger.close()
        temp_file.seek(0)

        return temp_file

    def get_cover_page(self) -> str:
        image_path = self.image.generate_random_image()

        try:
            start = self.last_run_time.strftime('%B %d').lstrip('0')
            end = self.current_run_time.strftime('%B %d').lstrip('0')
            header_text = "Metagnosis"
            subheader_text = f"{start} - {end}"
            pdf = FPDF()

            pdf.add_page()

            max_font_size = 48
            pdf.set_font("Arial", 'B', max_font_size)
            text_width = pdf.get_string_width(header_text)
            page_width = pdf.w - 20

            while text_width > page_width:
                max_font_size -= 1
                pdf.set_font("Arial", 'B', max_font_size)
                text_width = pdf.get_string_width(header_text)

            pdf.cell(0, max_font_size / 2, header_text, ln=True, align="L")
            pdf.set_font("Arial", 'I', 18)
            pdf.cell(0, 15, subheader_text, ln=True, align="L")
            pdf.ln(30)

            image = Image.open(image_path)
            img_width, img_height = image.size
            max_size = min(pdf.w, pdf.h)
            side_length = min(max_size, min(img_width, img_height))
            x = (pdf.w - side_length) / 2
            y = pdf.h - side_length

            pdf.image(image_path, x=x, y=y, w=side_length, h=side_length)

            temp_file = NamedTemporaryFile(delete=False, suffix=".pdf")
            pdf.output(temp_file.name)
        finally:
            remove(image_path)

        return temp_file.name

    def get_relevant_docs(self, docs_multi) -> list[Document]:
        hn_docs, arxiv_docs = docs_multi
        arxiv_docs = self.filter_arxiv_docs(arxiv_docs) if arxiv_docs else []

        return hn_docs + arxiv_docs

    def filter_arxiv_docs(self, docs: list[Document]):
        arxiv = self.get_arxiv_cluster(docs)
        ids = self.get_interesting_arxiv_papers(arxiv)

        return [d for d in docs if d.id in ids]

    def get_arxiv_cluster(self, docs: list[Document]) -> ArxivClusters:
        data = {d.id: d.vector for d in docs}
        ids = list(data.keys())
        vectors = np.array(list(data.values()))
        clusters = len(TOPICS)
        kmeans = KMeans(n_clusters=clusters, random_state=56)

        kmeans.fit(vectors)

        return ArxivClusters(
            ids=ids,
            vectors=vectors,
            kmeans=kmeans,
            clusters=clusters
        )
    
    def get_interesting_arxiv_papers(self, arxiv: ArxivClusters) -> set[str]:
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