from time import time
import sys, os
import asyncio
import sqlite3
import json
from types import CoroutineType
from typing import NamedTuple

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
import rts_api as rts
import aiohttp

db_name = "bus_data.db"


class RequestDataType(NamedTuple):
    """
    `interval_val` comes from https://apscheduler.readthedocs.io/en/3.x/modules/triggers/cron.html
    """

    db_table_name: str
    job: CoroutineType
    interval_val: dict[str, int]


class RequestData(NamedTuple):
    get_routes: RequestDataType
    get_patterns: RequestDataType
    get_vehicles: RequestDataType


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def job_get_routes(
    session: aiohttp.ClientSession, con: sqlite3.Connection, req: RequestDataType
):
    xtime = round(time() * 1000)
    results = await rts.async_api_call(
        session,
        call_type=rts.API_Call.GET_ROUTES,
        hash_key=os.getenv("RTS_HASH_KEY"),
        api_key=os.getenv("RTS_API_KEY"),
    )

    cur = con.cursor()
    cur.execute(
        f"insert into {req.db_table_name} values(?, ?)", (xtime, json.dumps(results))
    )
    con.commit()


async def job_get_vehicles(
    session: aiohttp.ClientSession, con: sqlite3.Connection, req: RequestDataType
):
    # First get current routes that are being serviced
    xtime = round(time() * 1000)
    res_routes = (
        await rts.async_api_call(
            session,
            call_type=rts.API_Call.GET_ROUTES,
            hash_key=os.getenv("RTS_HASH_KEY"),
            api_key=os.getenv("RTS_API_KEY"),
            xtime=xtime,
        )
    )["bustime-response"]["routes"]
    routes = [(route["rt"], route["rtnm"]) for route in res_routes]

    # Then we request the vehicles for each route
    # Each GET_VEHICLES request can only serve 10 requests at a time, thus must be split up
    vehicle_futures = []
    chunked = list(chunk(routes, 10))

    for c in chunked:
        # Update xtime for next call
        xtime = round(time() * 1000)

        vehicle_futures.append(
            asyncio.ensure_future(
                rts.async_api_call(
                    session,
                    call_type=rts.API_Call.GET_VEHICLES,
                    params={"rt": ",".join([route[0] for route in c])},
                    hash_key=os.getenv("RTS_HASH_KEY"),
                    api_key=os.getenv("RTS_API_KEY"),
                    xtime=xtime,
                )
            )
        )

    vehicle_responses = await asyncio.gather(*vehicle_futures)
    results = [
        response["bustime-response"]["vehicle"] for response in vehicle_responses
    ]

    if len(results):
        cur = con.cursor()
        cur.execute(
            f"insert into {req.db_table_name} values(?, ?)",
            (xtime, json.dumps(results)),
        )
        con.commit()


async def job_get_patterns(
    session: aiohttp.ClientSession, con: sqlite3.Connection, req: RequestDataType
):
    print("yuh4")


### Request Data Definition ###
request_data = RequestData(
    get_routes=RequestDataType(
        db_table_name="request_get_routes",
        job=job_get_routes,
        interval_val={"seconds": 2},
    ),
    get_patterns=RequestDataType(
        db_table_name="request_get_patterns",
        job=job_get_patterns,
        interval_val={"seconds": 1},
    ),
    get_vehicles=RequestDataType(
        db_table_name="request_get_vehicles",
        job=job_get_vehicles,
        interval_val={"seconds": 1},
    ),
)


async def main(scheduler: AsyncIOScheduler, con: sqlite3.Connection):
    # Load .env file
    load_dotenv()

    # Setup database connection
    for request in request_data:
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {request.db_table_name}(request_time integer primary key, data text)"
        )

    # Setup aiohttp session and add scheduled jobs
    async with aiohttp.ClientSession() as session:
        for request in request_data:
            scheduler.add_job(
                request.job,
                args=[session, con, request],
                trigger="interval",
                **request.interval_val,
            )
        while True:
            await asyncio.sleep(1)


def shutdown(scheduler: AsyncIOScheduler, con: sqlite3.Connection):
    scheduler.shutdown()
    con.close()

    try:
        sys.exit(0)
    except SystemExit as e:
        os._exit(e.code)


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
