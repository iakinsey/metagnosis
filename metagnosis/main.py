from asyncio import gather, Lock
from aiosqlite import connect
from sqlite_vec import loadable_path
from .config import get_config
from .gateway.document import DocumentGateway
from .gateway.encoder import EncoderGateway
from .gateway.llm import LLMGateway
from .gateway.pdf import PDFGateway
from .job.arxiv import ArxivProcessorJob
from .job.hackernews import HackerNewsProcessorJob
from .job.doc_processor import DocumentProcessorJob
from .job.publisher import PublisherJob
from .util.job_server import JobServer

async def main():
    config = get_config()
    conn = await connect(config.db_path)
    job_conn = await connect(config.job_path)
    process_lock = Lock()

    await conn.enable_load_extension(True)
    await conn.load_extension(loadable_path())
    await conn.enable_load_extension(False)

    pdf = await PDFGateway.new(
        conn,
        config.storage_path,
        process_lock
    )
    document = await DocumentGateway.new(
        conn,
        config.storage_path,
        process_lock
    )
    encoder = EncoderGateway()
    llm = LLMGateway()
    jobs = [
        ArxivProcessorJob(pdf),
        DocumentProcessorJob(document, pdf, encoder, llm, 10),
        HackerNewsProcessorJob(config.storage_path, config.user_agent, pdf),
        PublisherJob(document)
    ]

    server = JobServer(job_conn, jobs)

    await server.start()