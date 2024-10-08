from asyncio import create_subprocess_exec
from asyncio.subprocess import PIPE
from bisect import insort
from datetime import datetime
from json import dumps
from os import remove
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Optional
from uuid import uuid4

import numpy as np

from aioboto3 import Session
from aiohttp import ClientResponseError, ClientSession
from fpdf import FPDF
from pikepdf import open as pikepdf_open
from PIL import Image
from PyPDF2 import PdfMerger, PdfReader, PdfWriter
from PyPDF2.generic import RectangleObject
from pydantic import BaseModel, ConfigDict
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
from .base import Job
from .arxiv import TOPICS
from ..config import get_config, Config, PublishCredentials
from ..gateway.document import DocumentGateway
from ..gateway.image_gen import ImageGenerationGateway
from ..log import log
from ..models.document import Document

LULU_TEST = "https://api.sandbox.lulu.com"
LULU_PROD = "https://api.lulu.com"


class ArxivClusters(BaseModel):
    ids: list[str]
    vectors: Any
    kmeans: KMeans
    clusters: int
    model_config = ConfigDict(arbitrary_types_allowed=True)


class PublisherJob(Job):
    INTERVAL = 604800
    API_PREFIX = LULU_PROD
    document: DocumentGateway
    hn_limit: int = 25
    arxiv_limit: int = 25
    hn_rank_threshold: int = 100
    image: ImageGenerationGateway
    aws_access_key_id: str
    aws_secret_access_key: str
    s3_bucket: str
    publish_creds: PublishCredentials
    proxy: Optional[str]

    def __init__(self, document: DocumentGateway):
        config = get_config()
        self.document = document
        self.image = ImageGenerationGateway()
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.s3_bucket = config.s3_bucket
        self.publish_creds = config.publish_creds
        self.lulu_auth = config.lulu_auth
        self.proxy = config.proxy

    async def perform(self):
        args_multi = [
            # ("Hacker News", self.last_run_time, self.hn_rank_threshold, self.hn_limit),
            ("arxiv", self.last_run_time, 0, None),
        ]

        async with self.document.get_documents_for_processing_multi(
            args_multi
        ) as docs_multi:
            docs = self.get_relevant_docs(docs_multi)

            if not docs:
                return

            start, end = self.get_times(docs_multi)
            cover_page_path = await self.get_cover_page(start, end)
            body_file_path = await self.merge_pdfs(docs)

            try:
                cover_path = await self.upload_to_s3(cover_page_path)
                body_path = await self.upload_to_s3(body_file_path)

                await self.publish_book(cover_path, body_path)
            finally:
                remove(cover_page_path)
                remove(body_file_path)

    async def get_lulu_auth(self) -> str:
        url = self.API_PREFIX + "/auth/realms/glasstree/protocol/openid-connect/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": self.lulu_auth,
        }
        data = {"grant_type": "client_credentials"}

        async with ClientSession() as session:
            async with session.post(
                url, proxy=self.proxy, headers=headers, data=data
            ) as response:
                payload = await response.json()
                return payload["access_token"]

    async def publish_book(self, cover_path: str, body_path: str) -> str:
        auth = await self.get_lulu_auth()
        url = self.API_PREFIX + "/print-jobs/"
        headers = {
            "Authorization": f"Bearer {auth}",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
        }
        external_id = str(uuid4())
        payload = {
            "contact_email": self.publish_creds.email,
            "external_id": external_id,
            "line_items": [
                {
                    "external_id": external_id,
                    "printable_normalization": {
                        "cover": {"source_url": cover_path},
                        "interior": {"source_url": body_path},
                        "pod_package_id": self.publish_creds.package_id,
                    },
                    "quantity": 1,
                    "title": f"Metagnosis {datetime.now().strftime('%Y-%m-%d')}",
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
                "street2": self.publish_creds.street2,
            },
            "shipping_level": self.publish_creds.shipping_level,
        }

        async with ClientSession() as session:
            async with session.post(
                url, headers=headers, data=dumps(payload)
            ) as response:
                try:
                    response.raise_for_status()
                except ClientResponseError as e:
                    error_response = await response.text()

                    raise RuntimeError(
                        f"Lulu Error: {e.status}, Details: {error_response}"
                    )

    async def upload_to_s3(self, file_name: str):
        object_name = Path(file_name).name
        session = Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

        async with session.client("s3") as s3_client:
            await s3_client.upload_file(file_name, self.s3_bucket, object_name)

        presigned_url = await s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.s3_bucket, "Key": object_name},
            ExpiresIn=604800,
        )

        return presigned_url

    async def merge_pdfs(self, docs: list[Document]) -> NamedTemporaryFile:
        temp_file = NamedTemporaryFile(delete=True, suffix=".pdf")

        try:
            merger = PdfMerger()

            for pdf_file in [d.path for d in docs]:
                merger.append(pdf_file)

            merger.write(temp_file)
            merger.close()
            temp_file.seek(0)

            reader = PdfReader(temp_file)
            writer = PdfWriter()
            letter_size = RectangleObject([0, 0, 612, 792])

            for page in reader.pages:
                page.mediabox = letter_size
                page.trimbox = letter_size
                page.crobox = letter_size
                page.bleedbox = letter_size

                writer.add_page(page)

            resized_temp_file = NamedTemporaryFile(suffix=".pdf")

            with open(resized_temp_file.name, "wb") as f:
                writer.write(f)

            resized_temp_file.seek(0)

            return await self.fix_pdf(resized_temp_file.name)
        finally:
            remove(temp_file.name)

    async def fix_pdf(self, path: str) -> str:
        try:
            output_path = NamedTemporaryFile(suffix=".pdf").name

            with pikepdf_open(path) as pdf:
                pdf.save(
                    output_path,
                    fix_metadata_version=True,
                    compress_streams=True,
                    normalize_content=True,
                )

            return await self.ghostscript_fix(output_path)
        finally:
            remove(path)

    async def ghostscript_fix(self, input_path: str):
        output_path = NamedTemporaryFile(suffix=".pdf").name
        cmd = [
            "gs",
            "-dBATCH",
            "-dNOPAUSE",
            "-q",
            "-sDEVICE=pdfwrite",
            "-dPDFSETTINGS=/prepress",
            "-dEmbedAllFonts=true",
            "-dSubsetFonts=true",
            "-dCompressFonts=true",
            f"-sOutputFile={output_path}",
            f"{input_path}",
        ]

        try:
            process = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
            stdout, stderr = await process.communicate()

            log.info(stdout)

            if process.returncode != 0:
                err = stderr.decode()
                log.error(f"Ghostscript error: {err}")
                raise RuntimeError(err)
        finally:
            remove(input_path)

        return output_path

    async def get_cover_page(self, start: datetime, end: datetime) -> str:
        image_path = self.image.generate_random_image()

        try:
            start = start.strftime("%B %d").lstrip("0")
            end = end.strftime("%B %d").lstrip("0")
            header_text = "Metagnosis"
            subheader_text = f"{start} - {end}"

            document_width = 1302.408
            document_height = 810

            pdf = FPDF(unit="pt", format=(document_width, document_height))
            pdf.add_page()

            trim_width = 612
            trim_height = 792
            trim_x_offset = document_width - trim_width
            trim_y_offset = (document_height - trim_height) / 2

            max_font_size = 48
            pdf.set_font("Arial", "B", max_font_size)
            text_width = pdf.get_string_width(header_text)
            page_width = trim_width - 2 * 36

            while text_width > page_width:
                max_font_size -= 1
                pdf.set_font("Arial", "B", max_font_size)
                text_width = pdf.get_string_width(header_text)

            pdf.set_xy(trim_x_offset + 36, trim_y_offset + 50)
            pdf.cell(trim_width - 72, max_font_size, header_text, ln=True, align="C")

            pdf.set_font("Arial", "I", 18)
            pdf.set_xy(trim_x_offset + 36, trim_y_offset + 100)
            pdf.cell(trim_width - 72, 30, subheader_text, ln=True, align="C")

            image = Image.open(image_path)
            img_width, img_height = image.size
            img_aspect = img_width / img_height

            max_image_width = trim_width - 72
            max_image_height = trim_height - 200

            if img_width > max_image_width or img_height > max_image_height:
                if img_aspect > (max_image_width / max_image_height):
                    new_width = max_image_width
                    new_height = max_image_width / img_aspect
                else:
                    new_height = max_image_height
                    new_width = max_image_height * img_aspect
            else:
                new_width, new_height = img_width, img_height

            x = trim_x_offset + (trim_width - new_width) / 2
            y = trim_y_offset + 150

            pdf.image(image_path, x=x, y=y, w=new_width, h=new_height)

            temp_file = NamedTemporaryFile(delete=False, suffix=".pdf")
            pdf.output(temp_file.name)
        finally:
            remove(image_path)

        return await self.fix_pdf(temp_file.name)

    def get_times(self, docs_multi) -> tuple[datetime, datetime]:
        dates = []

        for doc_group in docs_multi:
            for document in doc_group:
                insort(dates, document.created)

        return dates[0], dates[-1]

    def get_relevant_docs(self, docs_multi) -> list[Document]:
        # hn_docs, arxiv_docs = docs_multi
        (arxiv_docs,) = docs_multi
        arxiv_docs = self.filter_arxiv_docs(arxiv_docs) if arxiv_docs else []

        # return hn_docs + arxiv_docs
        return arxiv_docs

    def filter_arxiv_docs(self, docs: list[Document]):
        arxiv = self.get_arxiv_cluster(docs)
        ids = self.get_interesting_arxiv_papers(arxiv)

        return [d for d in docs if d.id in ids]

    def get_arxiv_cluster(self, docs: list[Document]) -> ArxivClusters:
        data = {d.id: d.vector for d in docs}
        ids = list(data.keys())
        vectors = np.array(list(data.values()))
        clusters = len(TOPICS)
        kmeans = KMeans(n_clusters=clusters, random_state=56, n_init=10)

        kmeans.fit(vectors)

        return ArxivClusters(ids=ids, vectors=vectors, kmeans=kmeans, clusters=clusters)

    def get_interesting_arxiv_papers(self, arxiv: ArxivClusters) -> set[str]:
        centers = arxiv.kmeans.cluster_centers_
        labels = arxiv.kmeans.labels_
        centroids = arxiv.kmeans.cluster_centers_
        # distances = cdist(arxiv.vectors, centers, "euclidean")
        ids = set()

        for i in range(arxiv.clusters):
            # Novelty
            indices = np.where(labels == i)[0]
            vectors = arxiv.vectors[indices]

            distances_to_centroid = np.linalg.norm(vectors - centroids[i], axis=1)
            novel_index = np.argmax(distances_to_centroid)
            ids.add(arxiv.ids[indices[novel_index]])

            # Centrality
            # index = np.argmin(distances[:, i])
            # ids.add(arxiv.ids[index])

        return ids
