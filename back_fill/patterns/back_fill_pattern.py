from datetime import datetime
import os
import json
from typing import Any, List

from dotenv import load_dotenv
import psycopg2

from scraper.jobs.job_get_patterns import deserialize_pattern_response

load_dotenv()

con = psycopg2.connect(
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    application_name="Backfill Pattern",
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
str_insert = ",".join(["%s"] * len(columns))
str_insert = f"({str_insert})"


# chunk_size = 1
chunk_size = 1
cur = con.cursor()

# for i in range(1):
for i in range(10000000):
    print(f"Processing chunk {i} ({i * chunk_size})")
    cur.execute(f"SELECT * FROM request_get_patterns OFFSET {i * chunk_size} LIMIT 1")
    rows = cur.fetchall()

    print(f"Processing {len(rows)} rows...")

    start = datetime.now()

    result_batch: List[Any] = []

    for insert_time, response in rows:
        data = json.loads(response)
        for response in data:

            for pattern in map(
                deserialize_pattern_response, response["bustime-response"]["ptr"]
            ):
                # columns, values = pattern.to_sql_tuple()
                insert_cols, values = pattern.to_sql_dict()

                # Insert request time as first column
                # values = (datetime.fromtimestamp(insert_time / 1000), *values)
                values["request_time"] = datetime.fromtimestamp(insert_time / 1000)
                result_batch.append(values)

    print(f"Inserting {len(result_batch)} rows...")

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

    end = datetime.now()
    print(f"Processed {len(rows)} rows in {end - start}")

    if len(rows) == 0:
        break

con.close()

print("closing session")

while not con.closed:
    pass

print("done")
