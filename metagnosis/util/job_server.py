from asyncio import gather, sleep
from datetime import datetime, timedelta
from aiosqlite import Connection
from ..job.base import Job
from ..log import log


class JobServer:
    db: Connection
    INTERVAL = 1

    def __init__(self, db: Connection, jobs: list[Job]):
        self.db = db
        self.job_map = {j.__class__.__name__: j for j in jobs}

    async def initialize_job_db(self):
        schema = """
        CREATE TABLE IF NOT EXISTS job (
            name TEXT PRIMARY KEY,
            next_run_time INT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_job_next_run_time ON job(next_run_time);
        CREATE INDEX IF NOT EXISTS idx_job_name ON job(name);
        """

        for q in schema.split(";"):
            if not q:
                continue

            await self.db.execute(q)

        await self.db.commit()

        for job in self.job_map.values():
            await self.update_next_run_time(job)

    async def update_next_run_time(self, job: Job):
        query = """
        INSERT INTO job 
        (name, next_run_time)
        VALUES (?, ?)
        ON CONFLICT(name) 
        DO UPDATE SET next_run_time = excluded.next_run_time
        WHERE job.next_run_time < excluded.next_run_time;
        """

        dt = datetime.now() + timedelta(seconds=job.INTERVAL)
        next_run_time = int(dt.timestamp())

        await self.db.execute(query, (job.__class__.__name__, next_run_time))
        await self.db.commit()

    async def get_jobs_to_run(self) -> list[tuple[Job, datetime]]:
        query = """
        SELECT
            name, next_run_time
        FROM job
        WHERE next_run_time <= ?
        """

        results = []
        now = int(datetime.now().timestamp())

        async with self.db.execute(query, (now,)) as cursor:
            async for row in cursor:
                results.append((self.job_map[row[0]], row[1]))

        return results

    async def start(self):
        await self.initialize_job_db()

        now = datetime.now()

        while 1:
            jobs = await self.get_jobs_to_run()

            await gather(*(self.execute_job(j[0], now, j[1]) for j in jobs))
            await sleep(self.INTERVAL)

    async def execute_job(self, job: Job, current: datetime, last: datetime):
        log.info(f"Executing job {job.__class__.__name__}")

        try:
            job.set_run_times(current, last)
            await job.perform()
            await self.update_next_run_time(job)
        except Exception as e:
            log.info(f"Failed to execute job {job.__class__.__name__}")
            log.exception(e)
