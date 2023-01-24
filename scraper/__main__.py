import sys, os
import asyncio
import sqlite3

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
import rts_api as rts
import aiohttp

db_name = "bus_data.db"
db_tables = ["request_get_routes", "request_get_patterns"]


async def job(session: aiohttp.ClientSession, con: sqlite3.Connection):
    print(
        await rts.async_api_call(
            session,
            call_type=rts.API_Call.GET_ROUTES,
            hash_key=os.getenv("RTS_HASH_KEY"),
            api_key=os.getenv("RTS_API_KEY"),
        )
    )


### Scheduling Loop ###
async def setup(scheduler: AsyncIOScheduler, con: sqlite3.Connection):
    # Load .env file
    load_dotenv()

    # Setup database connection
    for table in db_tables:
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {table}(id integer primary key, data text)"
        )


def shutdown(scheduler: AsyncIOScheduler, con: sqlite3.Connection):
    scheduler.shutdown()
    con.close()

    try:
        sys.exit(0)
    except SystemExit as e:
        os._exit(e.code)


async def main(scheduler: AsyncIOScheduler, con: sqlite3.Connection):
    await setup(scheduler, con)

    # Setup aiohttp session
    async with aiohttp.ClientSession() as session:
        scheduler.add_job(job, args=[session, con], trigger="interval", seconds=1)

        while True:
            await asyncio.sleep(1)


if __name__ == "__main__":
    try:
        scheduler = AsyncIOScheduler()
        con = sqlite3.connect(db_name)

        scheduler.start()

        asyncio.get_event_loop().run_until_complete(main(scheduler, con))
    except KeyboardInterrupt:
        # Gracefully shut down on Cltr+C interrupt
        shutdown(scheduler, con)
    except Exception:
        # Cleanup and reraise. This will print a backtrace.
        raise
