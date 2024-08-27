from asyncio import gather
from aiosqlite import connect
from sqlite_vec import loadable_path
from .config import get_config
from .gateway.document import DocumentGateway
from .gateway.encoder import EncoderGateway
from .gateway.llm import LLMGateway
from .gateway.page import PageGateway
from .gateway.pdf import PDFGateway
from .job.arxiv import ArxivProcessorJob
from .job.hackernews import HackerNewsProcessorJob
from .job.doc_processor import DocumentProcessorJob


async def main():
    config = get_config()
    conn = await connect(config.db_path)

    await conn.enable_load_extension(True)
    await conn.load_extension(loadable_path())
    await conn.enable_load_extension(False)

    pdfg = await PDFGateway.new(
        conn,
        config.storage_path,
    )
    document = await DocumentGateway.new(
        conn,
        config.storage_path
    )
    page = await PageGateway.new(
        conn,
        config.storage_path
    )
    encoder = EncoderGateway()
    llm = LLMGateway()
    arxiv = ArxivProcessorJob(pdfg)
    pdf = DocumentProcessorJob(document, pdfg, encoder, llm, 10)
    hn = HackerNewsProcessorJob(config.storage_path, config.user_agent, page, pdfg)

    await gather(
        arxiv.start(),
        pdf.start(),
        hn.start()
    )
