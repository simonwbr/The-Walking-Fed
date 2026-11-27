"""Copy the water points of one downloaded file from MongoDB (landing) to PostgreSQL (bronze).

Bronze keeps the source values as text: types are cast in the silver layer (dbt).
"""
import os

import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient

from ingestion.load_water import sha256_of

MONGO_URI = os.getenv("MONGO_URI", "mongodb://walkingfed:walkingfed@localhost:27017/")

PROPERTIES = [
    "name", "operator", "accepts_bottle", "fee", "indoor",
    "offers_cold_water", "offers_warm_water", "offers_hot_water", "description",
    "meta_name_com", "meta_code_com", "meta_name_dep", "meta_code_dep",
    "meta_name_reg", "meta_code_reg", "meta_osm_id", "meta_osm_type", "meta_osm_url",
    "meta_first_update", "meta_last_update", "meta_versions_number", "meta_users_number",
]

DDL = f"""
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE IF NOT EXISTS bronze.water_points (
    {", ".join(f"{p} TEXT" for p in PROPERTIES)},
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def pg_connect():
    """Connection settings come from environment variables (no credentials in the code)."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "walking_fed"),
        user=os.getenv("POSTGRES_USER", "walkingfed"),
        password=os.getenv("POSTGRES_PASSWORD", "walkingfed"),
    )


def main(path):
    sha = sha256_of(path)
    docs = MongoClient(MONGO_URI)["walking_fed"].raw_water.find({"_ingestion.sha256": sha})

    rows = []
    for d in docs:
        props = d.get("properties", {})
        lon, lat = (d.get("geometry") or {}).get("coordinates") or [None, None]
        rows.append(
            [None if props.get(p) is None else str(props.get(p)) for p in PROPERTIES]
            + [lon, lat, d["_ingestion"]["file"], sha, d["_ingestion"]["ingested_at"]]
        )
    if not rows:
        raise ValueError(f"No document in MongoDB for {path}: run the landing task first")

    columns = PROPERTIES + ["longitude", "latitude", "source_file", "file_sha256", "ingested_at"]
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        # Idempotency: replace what a previous run loaded from this same file
        cur.execute("DELETE FROM bronze.water_points WHERE file_sha256 = %s", (sha,))
        execute_values(cur, f"INSERT INTO bronze.water_points ({', '.join(columns)}) VALUES %s",
                       rows, page_size=5000)
    print(f"{len(rows)} rows loaded into bronze.water_points")