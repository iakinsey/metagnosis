from asyncio import gather, new_event_loop, Lock
from os import _exit
from signal import signal, SIGTERM, SIGINT
from time import sleep

from aiosqlite import connect
from sqlite_vec import loadable_path

from .config import get_config
from .gateway.document import DocumentGateway
from .gateway.encoder import EncoderGateway
#from .gateway.llm import LLMGateway
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
    await conn.execute('PRAGMA journal_mode=WAL;')

    document = await DocumentGateway.new(
        conn,
        config.storage_path,
        process_lock
    )
    pdf = await PDFGateway.new(
        conn,
        config.storage_path,
        process_lock
    )
    encoder = EncoderGateway()
    #llm = LLMGateway()
    jobs = [
        #ArxivProcessorJob(pdf),
        #HackerNewsProcessorJob(config.storage_path, config.user_agent, pdf),
        #DocumentProcessorJob(document, pdf, encoder, 10),
        PublisherJob(document)
    ]

    server = JobServer(job_conn, jobs)

    await server.start()

def signal_handler(signum, frame):
    _exit(1)

if __name__ == '__main__':
    signal(SIGTERM, signal_handler)
    signal(SIGINT, signal_handler)

    loop = new_event_loop()
    loop.run_until_complete(main())