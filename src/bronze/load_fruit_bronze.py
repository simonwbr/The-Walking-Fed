"""Copy the Falling Fruit documents of one export from MongoDB (landing) to PostgreSQL (bronze)."""
import os

from psycopg2.extras import execute_values
from pymongo import MongoClient

from bronze.load_water_bronze import pg_connect
from ingestion.load_water import sha256_of

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

LOCATION_COLUMNS = ["id", "type_ids", "unverified", "description", "season_start", "season_stop", "no_season",
                    "author", "address", "created_at", "updated_at", "access", "import_link", "original_ids",
                    "hidden"]
TYPE_COLUMNS = ["id", "parent_id", "scientific_name", "taxonomic_rank", "en_name", "fr_name",
                "en_wikipedia_url", "category_mask", "pending"]

DDL = f"""
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE IF NOT EXISTS bronze.fruit_locations (
    {', '.join(f'{c} TEXT' for c in LOCATION_COLUMNS)},
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS bronze.fruit_types (
    {', '.join(f'{c} TEXT' for c in TYPE_COLUMNS)},
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def text(value):
    """Empty CSV cells become NULL."""
    return value if value not in ("", None) else None


def copy(cur, collection, table, sha, columns, extra=lambda doc: ()):
    rows = [tuple(text(doc.get(c)) for c in columns) + extra(doc)
            + (doc["_ingestion"]["file"], sha, doc["_ingestion"]["ingested_at"])
            for doc in collection.find({"_ingestion.sha256": sha}, {"_id": 0})]
    if not rows:
        raise ValueError(f"No document with sha256 {sha} in {collection.name}: run the landing task first")
    # Idempotency: replace what a previous run loaded from this same file
    cur.execute(f"DELETE FROM {table} WHERE file_sha256 = %s", (sha,))
    names = columns + (["latitude", "longitude"] if table.endswith("locations") else [])
    execute_values(cur, f"INSERT INTO {table} ({', '.join(names)}, source_file, file_sha256, ingested_at) VALUES %s",
                   rows, page_size=5000)
    print(f"{len(rows)} rows loaded into {table}")


def main(locations_path, types_path):
    with MongoClient(MONGO_URI) as client, pg_connect() as conn, conn.cursor() as cur:
        db = client["walking_fed"]
        cur.execute(DDL)
        copy(cur, db.raw_fruit_types, "bronze.fruit_types", sha256_of(types_path), TYPE_COLUMNS)
        copy(cur, db.raw_fruit_locations, "bronze.fruit_locations", sha256_of(locations_path), LOCATION_COLUMNS,
             extra=lambda doc: (float(doc["lat"]), float(doc["lng"])))
