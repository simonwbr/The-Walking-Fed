"""Store the Falling Fruit export (locations + types) in MongoDB (landing zone).

Types are stored whole (about 5,000 rows). Locations are worldwide (about 2.6 million rows):
only the ones inside the study area (metropolitan France) are stored.
Every document gets an `_ingestion` block (source, file, sha256, ingested_at) for lineage.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pymongo import MongoClient

from ingestion.load_water import sha256_of

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
STUDY_AREA = json.loads(os.getenv(
    "STUDY_AREA_BBOX", '{"min_lon": -5.2, "min_lat": 41.3, "max_lon": 9.6, "max_lat": 51.1}'))
CHUNK = 100_000


def in_study_area(df):
    """Rows whose coordinates are numbers inside the study-area rectangle."""
    lat = pd.to_numeric(df["lat"], errors="coerce")
    lng = pd.to_numeric(df["lng"], errors="coerce")
    b = STUDY_AREA
    return lat.between(b["min_lat"], b["max_lat"]) & lng.between(b["min_lon"], b["max_lon"])


def store(db, path, collection, keep_rows=None):
    path = Path(path)
    sha = sha256_of(path)

    # Idempotency: this exact file was already stored, do nothing
    if db.ingestion_log.find_one({"sha256": sha, "collection": collection, "status": "loaded"}):
        print(f"{path.name} already stored in {collection} (same SHA-256), skipped")
        return

    # Replace what a previous, interrupted run may have written for this file
    db[collection].delete_many({"_ingestion.sha256": sha})
    ingestion = {"source": "falling_fruit", "file": path.as_posix(), "sha256": sha,
                 "ingested_at": datetime.now(timezone.utc)}

    # Read in chunks so that the 2.6 million worldwide rows never sit in memory at once.
    # Every column read as text, empty cells kept as "" (descriptions hold line breaks: real CSV parser)
    rows_in_file = rows_stored = 0
    for chunk in pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=CHUNK):
        rows_in_file += len(chunk)
        if keep_rows is not None:
            chunk = chunk[keep_rows(chunk)]
        if chunk.empty:
            continue
        db[collection].insert_many([{**rec, "_ingestion": ingestion} for rec in chunk.to_dict("records")],
                                   ordered=False)
        rows_stored += len(chunk)

    db.ingestion_log.insert_one({"source": "falling_fruit", "collection": collection, "file": path.as_posix(),
                                 "sha256": sha, "rows_in_file": rows_in_file, "rows_stored": rows_stored,
                                 "status": "loaded", "logged_at": datetime.now(timezone.utc)})
    print(f"{rows_stored} of {rows_in_file} rows stored in walking_fed.{collection}")


def main(locations_path, types_path):
    with MongoClient(MONGO_URI) as client:
        db = client["walking_fed"]
        store(db, types_path, "raw_fruit_types")
        store(db, locations_path, "raw_fruit_locations", keep_rows=in_study_area)
