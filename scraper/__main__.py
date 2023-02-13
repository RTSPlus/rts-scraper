from datetime import datetime
import sys, os
import asyncio
from typing import NamedTuple
import pytz

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
import psycopg2
import aiohttp

from scraper.job_get_detours import job_get_detours
from scraper.job_get_patterns import job_get_patterns
from scraper.job_get_routes import job_get_routes
from scraper.job_get_vehicles import job_get_vehicles

from scraper.data_types import RequestDataType


class RequestData(NamedTuple):
    get_routes: RequestDataType
    get_patterns: RequestDataType
    get_vehicles: RequestDataType
    get_detours: RequestDataType


### Request Data Definition ###
request_data = RequestData(
    get_routes=RequestDataType(
        db_table_name="request_get_routes",
        job=job_get_routes,
        interval_val={"hours": 12},
        cloudwatch_log_stream="job-get-routes",
    ),
    get_patterns=RequestDataType(
        db_table_name="request_get_patterns",
        job=job_get_patterns,
        interval_val={"hours": 12},
        cloudwatch_log_stream="job-get-patterns",
    ),
    get_vehicles=RequestDataType(
        db_table_name="request_get_vehicles",
        job=job_get_vehicles,
        interval_val={"seconds": 5},
        cloudwatch_log_stream="job-get-vehicles",
    ),
    get_detours=RequestDataType(
        db_table_name="request_get_detours",
        job=job_get_detours,
        interval_val={"hours": 12},
        cloudwatch_log_stream="job-get-detours",
    ),
)


async def main(scheduler: AsyncIOScheduler, con):
    # Setup database connection
    for request in request_data:
        cur = con.cursor()
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {request.db_table_name}(request_time bigint primary key, data text)"
        )
        con.commit()
        cur.close()

    # Setup aiohttp session and add scheduled jobs
    async with aiohttp.ClientSession() as session:
        for request in request_data:
            scheduler.add_job(
                request.job,
                args=[session, con, request],
                trigger="interval",
                next_run_time=datetime.now(pytz.utc),
                **request.interval_val,
            )
        while True:
            await asyncio.sleep(1)


def shutdown(scheduler: AsyncIOScheduler, con):
    scheduler.shutdown()
    if con:
        con.close()


if __name__ == "__main__":
    # Load .env file
    load_dotenv()

    try:
        scheduler = AsyncIOScheduler()

        con = psycopg2.connect(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )

        scheduler.start()

        asyncio.get_event_loop().run_until_complete(main(scheduler, con))
    except KeyboardInterrupt:
        # Gracefully shut down on Cltr+C interrupt
        shutdown(scheduler, con if con else None)
        try:
            sys.exit(0)
        except SystemExit as e:
            os._exit(e.code)
    except Exception:
        # Cleanup and reraise. This will print a backtrace.
        shutdown(scheduler, con if con else None)
        raise
