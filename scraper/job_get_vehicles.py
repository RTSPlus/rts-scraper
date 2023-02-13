from dataclasses import dataclass
import dataclasses
from time import time
from datetime import datetime
import os
import json
import asyncio
from typing import Literal, NamedTuple
from psycopg2.extensions import AsIs
from aiohttp import ClientSession

import rts_api as rts
from scraper.log import log
from scraper.data_types import RequestDataType
from scraper.util import chunk


class Point(NamedTuple):
    lat: float
    lon: float


@dataclass(frozen=True)
class MiscData:
    """
    Unused data from the vehicle response
    _or is a reserved keyword in python, so it is prefixed with an underscore
    """

    tmstmp: str
    tatripid: str
    origtatripno: str
    tablockid: str
    zone: str
    mode: int
    srvtmstmp: str
    oid: str
    _or: bool
    rid: str
    blk: int
    tripdyn: int
    stst: int
    stsd: str


@dataclass(frozen=True)
class VehicleReponse:
    vehicle_id: int
    coord: Point
    heading: int

    route_num: int
    pattern_id: int
    destination: str
    pdist: int

    is_delayed: bool
    speed_mph: int

    passenger_load: Literal["FULL", "HALF_EMPTY", "EMPTY", "N/A"]
    trip_id: int

    misc_data: MiscData

    def to_sql_tuple(self):
        column_name_value_map = [
            ("vehicle_id", self.vehicle_id),
            ("coord", f"({self.coord.lat},{self.coord.lon})"),
            ("heading", self.heading),
            ("route_num", self.route_num),
            ("pattern_id", self.pattern_id),
            ("destination", self.destination),
            ("pdist", self.pdist),
            ("is_delayed", self.is_delayed),
            ("speed_mph", self.speed_mph),
            ("passenger_load", self.passenger_load),
            ("trip_id", self.trip_id),
            ("misc_data", json.dumps(dataclasses.asdict(self.misc_data))),
            (
                "geom",
                AsIs(
                    f"ST_SetSRID(ST_MakePoint({self.coord.lon}, {self.coord.lat}), 4326)"
                ),
            ),
        ]

        return tuple(zip(*column_name_value_map))


def deserialize_vehicle_response(response):
    return VehicleReponse(
        vehicle_id=int(response["vid"]),
        coord=Point(lat=float(response["lat"]), lon=float(response["lon"])),
        heading=int(response["hdg"]),
        route_num=int(response["rt"]),
        pattern_id=int(response["pid"]),
        destination=response["des"],
        pdist=int(response["pdist"]),
        is_delayed=bool(response["dly"]),
        speed_mph=int(response["spd"]),
        passenger_load=response["psgld"],
        trip_id=int(response["tripid"]),
        misc_data=MiscData(
            tmstmp=response["tmstmp"],
            tatripid=response["tatripid"],
            origtatripno=response["origtatripno"],
            tablockid=response["tablockid"],
            zone=response["zone"],
            mode=response["mode"],
            srvtmstmp=response["srvtmstmp"],
            oid=response["oid"],
            _or=response["or"],
            rid=response["rid"],
            blk=response["blk"],
            tripdyn=response["tripdyn"],
            stst=response["stst"],
            stsd=response["stsd"],
        ),
    )


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
            # Insert raw response
            cur.execute(
                f"INSERT INTO {req.db_table_name} VALUES (%s, %s)",
                (xtime, json.dumps(results)),
            )

            # Insert parsed response
            for r in map(deserialize_vehicle_response, results):
                columns, values = r.to_sql_tuple()

                # Insert request time as first column
                columns = ("request_time", *columns)
                values = (datetime.fromtimestamp(xtime / 1000), *values)

                str_columns = ",".join(columns)
                str_insert = ",".join(["%s"] * len(values))

                cur.execute(
                    f"INSERT INTO data_bus_location ({str_columns}) VALUES ({str_insert})",
                    values,
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
