from time import time
import os
import json
import asyncio

from aiohttp import ClientSession

import rts_api as rts
from scraper.log import log
from scraper.types import RequestDataType
from scraper.util import chunk


async def job_get_vehicles(session: ClientSession, con, req: RequestDataType):
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
            req.job.__name__,
            "Request successful - No results",
            log_stream=req.cloudwatch_log_stream,
        )
