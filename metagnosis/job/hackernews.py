from asyncio import gather
from datetime import datetime
from hashlib import sha256
from aiohttp import ClientSession
from playwright.async_api import async_playwright, Browser
from bs4 import BeautifulSoup
from .base import Job
from ..gateway.page import PageGateway
from ..gateway.queue import QueueGateway
from ..log import log
from ..models.page import Page
from ..models.pdf import PDF


class HackerNewsProcessorJob(Job):
    user_agent: str
    page: PageGateway
    queue: QueueGateway

    def __init__(self, user_agent: str, page: PageGateway, queue: QueueGateway):
        self.page = page
        self.queue = queue
        self.user_agent = user_agent
        self.hn_url = "https://news.ycombinator.com/"

    async def perform(self):
        async with ClientSession() as session:
            async with session.get(self.hn_url) as resp:
                html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")
        titles = [i.find("a").text for i in soup.find_all('span', class_="titleline")]
        urls = [i.find("a").get("href") for i in soup.find_all('span', class_="titleline")]
        ids = [sha256(bytes(u, encoding="utf-8")).hexdigest() for u in urls]
        comments = [int(i.find_all()[-1].text.split()[0] if i.find_all()[-1].text.split()[0].isdigit() else 0) for i in soup.find_all('td', class_="subtext")]

        if not len(titles) == len(urls) == len(comments):
            raise ValueError("Elements found are not equal")
        
        unprepared = await self.page.get_page_processing_status(ids)

        async with async_playwright() as p:
            browser = await p.chromium.launch()
            pages = await gather(*(
                self.process_entity(browser, id, title, url, comment, needs_update)
                for id, title, url, comment, (needs_update, processed)
                in zip(ids, titles, urls, comments, unprepared)
                if not processed 
            ))

        await self.page.upsert_pages([i for i in pages if i])

    async def process_entity(self, browser: Browser, id: str, title: str, url: str, comment: int, needs_update: bool) -> PDF:
        log.info(f"Processing entity {url}")

        if url.endswith(".pdf"):
            await self.queue.download_pdf(url)

            return

        if needs_update:
            return self.update_page(id, title, url, comment)

        return await self.new_page(browser, id, title, url, comment)

    def update_page(self, id: str, title: str, url: str, comment: int) -> Page:
        now = datetime.now()

        return Page(
            id=id,
            title=title,
            url=url,
            score=comment,
            text="",
            error=None,
            processed=False,
            created=now,
            updated=now
        )

    async def new_page(self, browser: Browser, id: str, title: str, url: str, comment: int) -> Page:
        page = await browser.new_page()
        err = None

        try:
            await page.goto(url)
        except Exception as e:
            err = str(e)

        text = await page.content()
        now = datetime.now()

        return Page(
            id=id,
            title=title,
            url=url,
            score=comment,
            text=text,
            error=err,
            processed=False,
            created=now,
            updated=now
        )