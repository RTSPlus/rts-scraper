import os
import sqlite3
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor

from scraper.jobs.job_get_vehicles import deserialize_vehicle_response

chunk_size = 1000

load_dotenv()

con_sqlite = sqlite3.connect(
    "/Users/noah/Downloads/bus_data_2022.db", check_same_thread=False
)

print("Sqlite connection obtained")

con_postgres = psycopg2.connect(
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    application_name="Backfill SQLite Threaded",
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


sub_chunk_size = 80


def process_chunk(limit_range, start_offset, chunk_i):
    print(f"[Chunk {chunk_i}] Offset {start_offset} Limit {limit_range}")

    for i in range(start_offset, start_offset + limit_range, sub_chunk_size):
        cur_sqlite = con_sqlite.cursor()
        query = cur_sqlite.execute(
            f"select * from queries limit {sub_chunk_size} offset {i}"
        )
        rows = query.fetchall()

        print(f"[Chunk {chunk_i}] Processing {len(rows)} rows...")

        start = datetime.now()
        result_batch = []

        for insert_time, response in rows:
            r = json.loads(response)
            for vehicle_obj in map(deserialize_vehicle_response, r):
                columns, values = vehicle_obj.to_sql_tuple()

                # Insert request time as first column
                values = (datetime.fromtimestamp(insert_time / 1000), *values)
                result_batch.append(values)

        cur_postgres = con_postgres.cursor()
        insertion_args = ",".join(
            [cur_postgres.mogrify(str_insert, v).decode("utf-8") for v in result_batch]
        )
        print(
            f"INSERT INTO data_bus_location ({str_columns}) VALUES {insertion_args} ON CONFLICT DO NOTHING"
        )
        cur_postgres.execute(
            f"INSERT INTO data_bus_location ({str_columns}) VALUES {insertion_args} ON CONFLICT DO NOTHING"
        )
        con_postgres.commit()
        cur_postgres.close()

        end = datetime.now()
        print(f"[Chunk {chunk_i}] Processed {len(rows)} rows in {end - start}")

    return 0


queries_len = 537754
with ThreadPoolExecutor() as executor:
    futures = []
    results = []

    limit = queries_len // (cpu_count())
    for i in range(cpu_count()):
        offset = limit * i
        # if i == cpu_count() - 1:
        #     limit = -1

        print(f"Processing chunk {i} ({offset})")
        futures.append(executor.submit(process_chunk, limit, offset, i))

    for future in futures:
        results.append(future.result())

    print(results)

print("done")

con_postgres.close()
con_sqlite.close()
