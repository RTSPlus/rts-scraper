import os
import sqlite3
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

from scraper.jobs.job_get_vehicles import deserialize_vehicle_response

chunk_size = 80

load_dotenv()

con_sqlite = sqlite3.connect("/Users/noah/Downloads/bus_data_2022.db")
cur_sqlite = con_sqlite.cursor()

con_postgres = psycopg2.connect(
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    application_name="Backfill SQLite",
)

print("Postgres connection obtained")

columns = (
    "request_time",
    "vehicle_id",
    "coord",
    "heading",
    "route_num",
    "pattern_id",
    "destination",
    "pdist",
    "is_delayed",
    "speed_mph",
    "passenger_load",
    "trip_id",
    "misc_data",
    "geom",
)
str_columns = ",".join(columns)
str_insert = ",".join(["%s"] * len(columns))
str_insert = f"({str_insert})"

for i in range(4325, 1000000):
    print(f"Processing chunk {i} ({i * chunk_size})")
    res = cur_sqlite.execute(
        f"SELECT * FROM queries limit {i * chunk_size},{chunk_size}"
    )
    rows = res.fetchall()

    print(f"Processing {len(rows)} rows...")

    start = datetime.now()

    cur_postgres = con_postgres.cursor()
    result_batch = []

    for insert_time, response in rows:
        r = json.loads(response)
        for vehicle_obj in map(deserialize_vehicle_response, r):
            columns, values = vehicle_obj.to_sql_tuple()

            # Insert request time as first column
            values = (datetime.fromtimestamp(insert_time / 1000), *values)
            result_batch.append(values)

    if len(result_batch):
        print(f"Inserting {len(result_batch)} rows...")
        insertion_args = ",".join(
            [cur_postgres.mogrify(str_insert, v).decode("utf-8") for v in result_batch]
        )
        cur_postgres.execute(
            f"INSERT INTO data_bus_location ({str_columns}) VALUES {insertion_args} ON CONFLICT DO NOTHING"
        )
        con_postgres.commit()
    cur_postgres.close()

    end = datetime.now()
    print(f"Processed {len(rows)} rows in {end - start}")

    if len(rows) == 0 or len(rows) < chunk_size:
        break

print("done")

con_postgres.close()
con_sqlite.close()
