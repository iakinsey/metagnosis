from aiosqlite import connect
from .config import get_config
from .gateway.queue import QueueGateway
from .strategy.arxiv import ArxivStrategy


async def main():
    config = get_config()
    conn = await connect(config.queue_path)

    queue = QueueGateway(
        conn,
        config.storage_path,
    )

    await queue.setup(conn)

    s = ArxivStrategy(queue)

    await s.perform()

if __name__ == "__main__":
    from asyncio import new_event_loop
    loop = new_event_loop()
    loop.run_until_complete(main())
