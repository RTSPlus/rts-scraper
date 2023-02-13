import os
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

from scraper.jobs.job_get_vehicles import deserialize_vehicle_response

chunk_size = 1000

load_dotenv()

con_postgres_rts = psycopg2.connect(
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)

con_postgres_postgres = psycopg2.connect(
    database="postgres",
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)

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

cur_postgres_postgres = con_postgres_postgres.cursor()
# for i in range(1):
for i in range(100000):
    print(f"Processing chunk {i} ({i * chunk_size})")
    cur_postgres_postgres.execute(
        f"SELECT * FROM request_get_vehicles OFFSET {i * chunk_size} LIMIT {chunk_size}"
    )
    rows = cur_postgres_postgres.fetchall()

    print(f"Processing {len(rows)} rows...")

    start = datetime.now()

    cur_postgres_rts = con_postgres_rts.cursor()
    result_batch = []

    for insert_time, response in rows:
        r = json.loads(response)
        for vehicle_obj in map(deserialize_vehicle_response, r):
            columns, values = vehicle_obj.to_sql_tuple()

            # Insert request time as first column
            values = (datetime.fromtimestamp(insert_time / 1000), *values)
            result_batch.append(values)

    print(f"Inserting {len(result_batch)} rows...")

    insertion_args = ",".join(
        [cur_postgres_rts.mogrify(str_insert, v).decode("utf-8") for v in result_batch]
    )
    cur_postgres_rts.execute(
        f"INSERT INTO data_bus_location ({str_columns}) VALUES {insertion_args} ON CONFLICT DO NOTHING"
    )
    con_postgres_rts.commit()
    cur_postgres_rts.close()

    end = datetime.now()
    print(f"Processed {len(rows)} rows in {end - start}")

    if len(rows) == 0 or len(rows) < chunk_size:
        break

print("done")

con_postgres_rts.close()
con_postgres_postgres.close()
