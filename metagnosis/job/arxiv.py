from asyncio import gather, Semaphore
from aiohttp import ClientSession
from feedparser import parse
from .base import Job
from ..log import log
from ..gateway.encoder import EncoderGateway
from ..gateway.queue import QueueGateway

URL_TEMPLATE = "https://rss.arxiv.org/rss/{}"
TOPICS = [
    "cs.AI", "cs.CL", "cs.CV", "cs.CY", "cs.CR", "cs.DS", "cs.DB", "cs.DL",
    "cs.DC", "cs.ET", "cs.HC", "cs.IR", "cs.IT", "cs.LG", "cs.MA", "cs.MM",
    "cs.NI", "cs.NE", "cs.OS", "cs.PF", "cs.PL", "cs.RO", "cs.SI", "cs.SE",
    "cs.SD"
]


class ArxivProcessorJob(Job):
    queue: QueueGateway
    download_limit: int = 10

    def __init__(self, queue: QueueGateway):
        self.queue = queue
 
    async def perform(self):
        await gather(*(
            self.process_rss(t)
            for t in TOPICS
        ))

    async def process_rss(self, topic: str):
        try:
            await self._process_rss(topic)
        except Exception as e:
            log.exception(e)

    async def _process_rss(self, topic: str):
        url = URL_TEMPLATE.format(topic)

        async with ClientSession() as client:
            async with client.get(url) as resp:
                text = await resp.text()
        
        urls = await self.extract_pdf_urls(text)
        sem = Semaphore(self.download_limit)

        await gather(*(
            self.queue.download_pdf(url, sem)
            for url in urls
        ))

    async def extract_pdf_urls(self, text: str) -> list[str]:
        feed = parse(text)
        urls: list[str] = []

        for entry in feed.entries:
            url = entry.link

            if url:
                pdf_url = url.replace("/abs/", "/pdf/")
                urls.append(pdf_url)
        
        return urls