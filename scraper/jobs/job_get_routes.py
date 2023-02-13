from time import time
import os
import json

from aiohttp import ClientSession

import rts_api as rts
from scraper.log import log
from scraper.data_types import RequestDataType


async def job_get_routes(session: ClientSession, con, req: RequestDataType):
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
