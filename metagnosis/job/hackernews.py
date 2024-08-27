from asyncio import gather
from datetime import datetime
from hashlib import sha256
from os.path import join
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser
from trafilatura import extract
from .base import Job
from ..gateway.pdf import PDFGateway
from ..log import log
from ..models.pdf import PDF


class HackerNewsProcessorJob(Job):
    storage_path: str
    user_agent: str
    pdf: PDFGateway

    def __init__(self, storage_path: str, user_agent: str, pdf: PDFGateway):
        self.storage_path = storage_path
        self.pdf = pdf
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
        
        unprepared = await self.pdf.get_processing_status(ids)

        async with async_playwright() as p:
            browser = await p.chromium.launch()
            pages = await gather(*(
                self.process_entity(browser, id, title, url, comment, needs_update)
                for id, title, url, comment, (needs_update, processed)
                in zip(ids, titles, urls, comments, unprepared)
                if not processed 
            ))

        await self.pdf.upsert_pages([i for i in pages if i])

    async def process_entity(self, browser: Browser, id: str, title: str, url: str, comment: int, needs_update: bool) -> PDF:
        log.info(f"Processing entity {url}")

        if url.endswith(".pdf"):
            await self.pdf.download_pdf(url, "Hacker News", title=title, score=comment)

            return

        if needs_update:
            return self.update_page(id, title, url, comment)

        return await self.new_page(browser, id, title, url, comment)

    def update_page(self, id: str, title: str, url: str, comment: int) -> PDF:
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
            processed=False
        )

    async def new_page(self, browser: Browser, id: str, title: str, url: str, comment: int) -> PDF:
        page = await browser.new_page()
        err = None

        try:
            await page.goto(url)
        except Exception as e:
            err = str(e)

        now = datetime.now()
        path = await self.screenshot_page()

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
            processed=False
        )
    
    async def screenshot_page(self, page, id) -> str:
        path = join(self.storage_path, id)
        old_html = await page.evaluate("document.body.innerHTML")
        new_html = extract(old_html, include_images=True, output_format='html')

        await page.evaluate("""
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
        """, new_html)

        await page.pdf(path=path)

        return path