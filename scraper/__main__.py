from time import time
from datetime import datetime
import sys, os
import asyncio
import json
from types import CoroutineType
from typing import NamedTuple

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
import rts_api as rts
import psycopg2
import aiohttp

from boto3 import client as boto_client


ENABLE_CLOUDWATCH_LOGS = True

cloudwatch_log_group = "scraper-monitoring"


cloudwatch_logs = None
if ENABLE_CLOUDWATCH_LOGS:
    cloudwatch_logs = boto_client("logs")


def log(tag: str, msg: str, timestamp: int = None, log_stream: str | None = None):
    """
    Logs to both stdout and CloudWatch Logs.
    Requires a log_stream to be passed in if CloudWatch Logs is to be used.
    """
    timestamp = timestamp if timestamp else int(round(time() * 1000))
    message = f"[{tag}][{datetime.fromtimestamp(timestamp/1000)}] {msg}"

    sys.stdout.write(message + "\n")
    sys.stdout.flush()

    if log_stream and cloudwatch_logs:
        cloudwatch_logs.put_log_events(
            logGroupName=cloudwatch_log_group,
            logStreamName=log_stream,
            logEvents=[
                {
                    "timestamp": timestamp,
                    "message": message,
                }
            ],
        )


class RequestDataType(NamedTuple):
    """
    `interval_val` comes from https://apscheduler.readthedocs.io/en/3.x/modules/triggers/interval.html?highlight=hours
    """

    db_table_name: str
    job: CoroutineType
    interval_val: dict[str, int]
    cloudwatch_log_stream: str


class RequestData(NamedTuple):
    get_routes: RequestDataType
    get_patterns: RequestDataType
    get_vehicles: RequestDataType


def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def job_get_routes(session: aiohttp.ClientSession, con, req: RequestDataType):
    xtime = round(time() * 1000)
    results = await rts.async_api_call(
        session,
        call_type=rts.API_Call.GET_ROUTES,
        hash_key=os.getenv("RTS_HASH_KEY"),
        api_key=os.getenv("RTS_API_KEY"),
    )

    try:
        cur = con.cursor()
        cur.execute(
            f"insert into {req.db_table_name} values(%s, %s)",
            (xtime, json.dumps(results)),
        )
        con.commit()

        log(
            req.job.__name__, "Request successful", log_stream=req.cloudwatch_log_stream
        )
        cur.close()
    except Exception as e:
        log(
            req.job.__name__,
            f"Error occurred: {e.args[0]}",
            log_stream=req.cloudwatch_log_stream,
        )


async def job_get_vehicles(session: aiohttp.ClientSession, con, req: RequestDataType):
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

    # Update xtime for next call
    xtime = round(time() * 1000)

    # Then we request the vehicles for each route
    # Each GET_VEHICLES request can only serve 10 requests at a time, thus must be split up
    vehicle_responses = await asyncio.gather(
        *(
            rts.async_api_call(
                session,
                call_type=rts.API_Call.GET_VEHICLES,
                params={"rt": ",".join([route[0] for route in c])},
                hash_key=os.getenv("RTS_HASH_KEY"),
                api_key=os.getenv("RTS_API_KEY"),
                xtime=xtime,
            )
            for c in chunk(routes, 10)
        )
    )

    results = []
    for response in vehicle_responses:
        if "vehicle" in response["bustime-response"]:
            results.extend(response["bustime-response"]["vehicle"])

    if len(results):
        try:
            cur = con.cursor()
            cur.execute(
                f"insert into {req.db_table_name} values(%s, %s)",
                (xtime, json.dumps(results)),
            )
            con.commit()

            log(
                req.job.__name__,
                "Request successful",
                log_stream=req.cloudwatch_log_stream,
            )

            cur.close()
        except Exception as e:
            log(
                req.job.__name__,
                f"Error occurred: {e.args[0]}",
                log_stream=req.cloudwatch_log_stream,
            )
    else:
        log(
            req.job.__name__, "Request successful", log_stream=req.cloudwatch_log_stream
        )


async def job_get_patterns(session: aiohttp.ClientSession, con, req: RequestDataType):
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

    patterns_responses = await asyncio.gather(
        *(
            rts.async_api_call(
                session,
                call_type=rts.API_Call.GET_ROUTE_PATTERNS,
                params={"rt": rt[0]},
                hash_key=os.getenv("RTS_HASH_KEY"),
                api_key=os.getenv("RTS_API_KEY"),
                xtime=xtime,
            )
            for rt in routes
        )
    )

    try:
        cur = con.cursor()
        cur.execute(
            f"insert into {req.db_table_name} values(%s, %s)",
            (xtime, json.dumps(patterns_responses)),
        )
        con.commit()

        log(
            req.job.__name__, "Request successful", log_stream=req.cloudwatch_log_stream
        )
        cur.close()
    except Exception as e:
        log(
            req.job.__name__,
            f"Error occured: {e.args[0]}",
            log_stream=req.cloudwatch_log_stream,
        )


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
