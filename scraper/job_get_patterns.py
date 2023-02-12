from time import time
import os
import json
import asyncio

from aiohttp import ClientSession

import rts_api as rts
from scraper.log import log
from scraper.types import RequestDataType


async def job_get_patterns(session: ClientSession, con, req: RequestDataType):
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
