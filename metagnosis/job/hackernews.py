from asyncio import gather, wait_for, CancelledError, TimeoutError
from datetime import datetime
from hashlib import sha256
from os.path import join
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser
from playwright._impl._errors import TargetClosedError
from trafilatura import extract
from .base import Job
from ..config import get_config, Config
from ..gateway.pdf import PDFGateway
from ..log import log
from ..models.pdf import PDF


class HackerNewsProcessorJob(Job):
    INTERVAL = 60
    REQUEST_TIMEOUT = 120
    storage_path: str
    user_agent: str
    pdf: PDFGateway
    config: Config

    def __init__(self, storage_path: str, user_agent: str, pdf: PDFGateway):
        self.storage_path = storage_path
        self.pdf = pdf
        self.user_agent = user_agent
        self.hn_url = "https://news.ycombinator.com/"
        self.config = get_config()

    async def perform(self):
        headers = {"User-Agent": self.config.user_agent}

        async with ClientSession() as session:
            async with session.get(
                self.hn_url, headers=headers, proxy=self.config.proxy
            ) as resp:
                html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")
        titles = [i.find("a").text for i in soup.find_all("span", class_="titleline")]
        urls = [
            i.find("a").get("href") for i in soup.find_all("span", class_="titleline")
        ]
        ids = [sha256(bytes(u, encoding="utf-8")).hexdigest() for u in urls]
        comments = [
            int(
                i.find_all()[-1].text.split()[0]
                if i.find_all()[-1].text.split()[0].isdigit()
                else 0
            )
            for i in soup.find_all("td", class_="subtext")
        ]

        if not len(titles) == len(urls) == len(comments):
            raise ValueError("Elements found are not equal")

        unprepared = await self.pdf.get_processing_status(ids)

        async with async_playwright() as p:
            browser = await p.chromium.launch()
            pages = await gather(
                *(
                    self.process_entity(browser, id, title, url, comment, needs_update)
                    for id, title, url, comment, (needs_update, processed) in zip(
                        ids, titles, urls, comments, unprepared
                    )
                    if not processed
                )
            )

        log.info("Pages gathered, upserting")
        await self.pdf.upsert_pdfs([i for i in pages if i])

    async def process_entity(self, *args) -> PDF:
        try:
            return await wait_for(
                self._process_entity(*args), timeout=self.REQUEST_TIMEOUT
            )
        except TimeoutError:
            return None

    async def _process_entity(
        self,
        browser: Browser,
        id: str,
        title: str,
        url: str,
        comment: int,
        needs_update: bool,
    ) -> PDF:
        if url.startswith("item?id="):
            url = "https://news.ycombinator.com/" + url

        log.info(f"Processing entity {url}")

        if url.endswith(".pdf"):
            await self.pdf.download_pdf(url, "Hacker News", title=title, score=comment)

            return

        if needs_update:
            return self.update_page(id, title, url, comment)

        try:
            return await self.new_page(browser, id, title, url, comment)
        except (TargetClosedError, CancelledError):
            return None

    def update_page(self, id: str, title: str, url: str, comment: int) -> PDF:
        log.info(f"Updating page {url}")

        now = datetime.now()

        return PDF(
            id=id,
            path="",
            url=url,
            title=title,
            score=comment,
            error=None,
            created=now,
            updated=now,
            processed=False,
        )

    async def new_page(
        self, browser: Browser, id: str, title: str, url: str, comment: int
    ) -> PDF:
        log.info(f"Fetching page {url}")

        page = await browser.new_page()
        err = None

        try:
            await page.goto(url)
            await page.wait_for_load_state()
        except Exception as e:
            err = str(e)

        now = datetime.now()

        try:
            path = await self.screenshot_page(page, id)
        except Exception as e:
            log.error("Failed to screenshot page", exc_info=True)

            return None

        return PDF(
            id=id,
            path=path,
            url=url,
            origin="Hacker News",
            title=title,
            score=comment,
            error=err,
            created=now,
            updated=now,
            processed=False,
        )

    async def screenshot_page(self, page, id) -> str:
        log.info(f"Screenshotting page {id}")

        path = join(self.storage_path, id)
        old_html = await page.evaluate("document.body.innerHTML")
        try:
            new_html = extract(old_html, include_images=True, output_format="html")
        except:
            extracted = extract(old_html)
            new_html = "".join(f"<p>{line}</p>" for line in extracted.split("\n"))

        await page.evaluate(
            """
            (newBodyHtml) => {
                const newBody = new DOMParser().parseFromString(newBodyHtml, 'text/html').body;
                const scripts = document.body.querySelectorAll('script');
                document.body.innerHTML = '';
                document.body.append(...Array.from(newBody.childNodes));
                document.body.append(...scripts);

                const newHtmlContent = document.documentElement.outerHTML;
                const dataUrl = 'data:text/html;charset=utf-8,' + encodeURIComponent(newHtmlContent);
                window.location.href = dataUrl;
            }
        """,
            new_html,
        )
        await page.wait_for_load_state()
        await page.pdf(path=path)

        return path
