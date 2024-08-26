from asyncio import gather
from aiosqlite import connect
from sqlite_vec import loadable_path
from .config import get_config
from .gateway.document import DocumentGateway
from .gateway.encoder import EncoderGateway
from .gateway.llm import LLMGateway
from .gateway.page import PageGateway
from .gateway.queue import QueueGateway
from .job.arxiv import ArxivProcessorJob
from .job.hackernews import HackerNewsProcessorJob
from .job.pdf import PDFProcessorJob


async def main():
    config = get_config()
    conn = await connect(config.queue_path)

    await conn.enable_load_extension(True)
    await conn.load_extension(loadable_path())
    await conn.enable_load_extension(False)

    queue = await QueueGateway.new(
        conn,
        config.storage_path,
    )
    document = await DocumentGateway.new(
        conn,
        config.storage_path
    )
    encoder = EncoderGateway()
    llm = LLMGateway()
    arxiv = ArxivProcessorJob(queue)
    pdf = PDFProcessorJob(document, queue, encoder, llm, 10)

    await gather(
        arxiv.perform(),
        pdf.perform()
    )


async def test_hn():
    config = get_config()
    conn = await connect(config.queue_path)

    await conn.enable_load_extension(True)
    await conn.load_extension(loadable_path())
    await conn.enable_load_extension(False)

    page = await PageGateway.new(
        conn,
        config.storage_path
    )
    queue = await QueueGateway.new(
        conn,
        config.storage_path
    )
    hn = HackerNewsProcessorJob(config.user_agent, page, queue)

    await hn.perform()

from asyncio import new_event_loop
loop = new_event_loop()
loop.run_until_complete(test_hn())