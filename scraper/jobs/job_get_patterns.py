from dataclasses import dataclass, asdict
from time import time
import os
import json
import asyncio
from typing import Any, List, Literal, Optional
from psycopg2.extras import Json

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

    def to_sql_tuple(self):
        column_name_value_map = [
            ("pattern_id", self.pattern_id),
            ("direction", self.direction),
            ("reported_length", self.reported_length),
            ("waypoints", Json([asdict(w) for w in self.waypoints])),
            # ("waypoints", Json(list(map(asdict, self.waypoints)))),
            ("stops", Json([asdict(s) for s in self.stops])),
            # ("stops", Json(list(map(asdict, self.stops)))),
            ("detour", Json(asdict(self.detour) if self.detour else None)),
        ]

        return tuple(zip(*column_name_value_map))

    def to_sql_dict(self):
        return (
            "%(request_time)s, %(pattern_id)s, %(direction)s, %(reported_length)s, %(waypoints)s::jsonb[], %(stops)s::jsonb[], %(detour)s::jsonb",
            {
                "pattern_id": self.pattern_id,
                "direction": self.direction,
                "reported_length": self.reported_length,
                "waypoints": [Json(asdict(w)) for w in self.waypoints],
                "stops": [Json(asdict(s)) for s in self.stops],
                "detour": Json(asdict(self.detour) if self.detour else None),
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
