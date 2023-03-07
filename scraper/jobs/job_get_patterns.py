from dataclasses import dataclass, asdict
from datetime import datetime
from time import time
import os
import json
import asyncio
from typing import Any, List, Literal, Optional
from psycopg2.extras import Json
from psycopg2.extensions import AsIs

from aiohttp import ClientSession

import rts_api as rts
from scraper.log import log
from scraper.data_types import RequestDataType


@dataclass(frozen=True, kw_only=True)
class Waypoint:
    seq: int
    lat: float
    lon: float

    type: Literal["W"] | Literal["S"] = "W"


@dataclass(frozen=True, kw_only=True)
class Stop(Waypoint):
    stop_id: str
    stop_name: str
    pdist: float

    type: Literal["S"] = "S"


@dataclass(frozen=True, kw_only=True)
class Detour:
    detour_id: str

    stops: List[Stop]
    waypoints: List[Waypoint]


@dataclass(frozen=True)
class PatternResponse:
    pattern_id: int
    direction: Literal["OUTBOUND", "INBOUND"]
    reported_length: float

    waypoints: List[Waypoint]
    stops: List[Stop]

    detour: Optional[Detour]

    def to_sql_dict(self) -> Any:
        return (
            "%(request_time)s, %(pattern_id)s, %(direction)s, %(reported_length)s, %(waypoints)s::jsonb[], %(stops)s::jsonb[], %(detour)s::jsonb, %(waypoints_geom)s, %(stops_geom)s",
            {
                "pattern_id": self.pattern_id,
                "direction": self.direction,
                "reported_length": self.reported_length,
                "waypoints": [Json(asdict(w)) for w in self.waypoints],
                "stops": [Json(asdict(s)) for s in self.stops],
                "detour": Json(asdict(self.detour) if self.detour else None),
                "waypoints_geom": AsIs(
                    f"ST_SetSRID(ST_MakeLine(ARRAY[{','.join([f'ST_MakePoint({w.lon}, {w.lat})' for w in self.waypoints])}]), 4326)"
                ),
                "stops_geom": AsIs(
                    f"ST_SetSRID(ST_Collect(ARRAY[{','.join([f'ST_MakePoint({s.lon}, {s.lat})' for s in self.stops])}]), 4326)"
                ),
            },
        )


def deserialize_pattern_response(response: Any) -> PatternResponse:
    # Build waypoints
    waypoints: List[Waypoint] = []
    stops: List[Stop] = []

    for detour_point in response["pt"]:
        if detour_point["typ"] == "W":
            waypoints.append(
                Waypoint(
                    seq=int(detour_point["seq"]),
                    lat=float(detour_point["lat"]),
                    lon=float(detour_point["lon"]),
                )
            )
        elif detour_point["typ"] == "S":
            stops.append(
                Stop(
                    seq=int(detour_point["seq"]),
                    lat=float(detour_point["lat"]),
                    lon=float(detour_point["lon"]),
                    stop_id=detour_point["stpid"],
                    stop_name=detour_point["stpnm"],
                    pdist=float(detour_point["pdist"]),
                )
            )

    detour = None
    if (
        "dtrid" in response
        and "dtrpt" in response
        and response["dtrid"]
        and response["dtrpt"]
    ):
        detour_stops: List[Stop] = []
        detour_waypoints: List[Waypoint] = []

        for detour_point in response["dtrpt"]:
            if detour_point["typ"] == "W":
                detour_waypoints.append(
                    Waypoint(
                        seq=int(detour_point["seq"]),
                        lat=float(detour_point["lat"]),
                        lon=float(detour_point["lon"]),
                    )
                )
            elif detour_point["typ"] == "S":
                detour_stops.append(
                    Stop(
                        seq=int(detour_point["seq"]),
                        lat=float(detour_point["lat"]),
                        lon=float(detour_point["lon"]),
                        stop_id=detour_point["stpid"],
                        stop_name=detour_point["stpnm"],
                        pdist=float(detour_point["pdist"]),
                    )
                )

        detour = Detour(
            detour_id=response["dtrid"],
            stops=detour_stops,
            waypoints=detour_waypoints,
        )

    return PatternResponse(
        pattern_id=int(response["pid"]),
        direction=response["rtdir"],
        reported_length=float(response["ln"]),
        waypoints=waypoints,
        stops=stops,
        detour=detour,
    )


columns = (
    "request_time",
    "pattern_id",
    "direction",
    "reported_length",
    "waypoints",
    "stops",
    "detour",
    "waypoints_geom",
    "stops_geom",
)
str_columns = ",".join(columns)


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

    # Raw response
    try:
        cur = con.cursor()

        cur.execute(
            f"insert into {req.db_table_name} values(%s, %s)",
            (xtime, json.dumps(patterns_responses)),
        )

        con.commit()

        log(
            req.job.__name__,
            "Request successful - Raw Response",
            log_stream=req.cloudwatch_log_stream,
        )
        cur.close()
    except Exception as e:
        log(
            req.job.__name__,
            f"Error occured - Raw Response: {e.args[0]}",
            log_stream=req.cloudwatch_log_stream,
        )

    # Parsed response
    try:
        cur = con.cursor()

        result_batch: List[Any] = []
        for response in patterns_responses:

            for pattern in map(
                deserialize_pattern_response, response["bustime-response"]["ptr"]
            ):
                # columns, values = pattern.to_sql_tuple()
                _, values = pattern.to_sql_dict()

                # Insert request time as first column
                # values = (datetime.fromtimestamp(insert_time / 1000), *values)
                values["request_time"] = datetime.fromtimestamp(xtime / 1000)
                result_batch.append(values)

        if len(result_batch) > 0:
            insertion_args = ",".join(
                [
                    cur.mogrify(
                        "(%(request_time)s, %(pattern_id)s, %(direction)s, %(reported_length)s, %(waypoints)s::jsonb[], %(stops)s::jsonb[], %(detour)s::jsonb, %(waypoints_geom)s, %(stops_geom)s)",
                        v,
                    ).decode("utf-8")
                    for v in result_batch
                ]
            )
            cur.execute(
                f"INSERT INTO data_patterns ({str_columns}) VALUES {insertion_args} ON CONFLICT DO NOTHING"
            )

        con.commit()

        log(
            req.job.__name__,
            "Request successful - Parsed Response",
            log_stream=req.cloudwatch_log_stream,
        )
        cur.close()
    except Exception as e:
        log(
            req.job.__name__,
            f"Error occured - Parsed Response: {e.args[0]}",
            log_stream=req.cloudwatch_log_stream,
        )
