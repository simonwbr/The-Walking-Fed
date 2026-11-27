"""Load the drinking water GeoJSON into the MongoDB landing zone (collection raw_water).

Usage: python src/ingestion/load_water.py <path to the GeoJSON file>
"""
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://walkingfed:walkingfed@localhost:27017/")
SOURCE = "osm-france-drinking-water (Huwise)"


def sha256_of(path):
    """Fingerprint of the file: identifies exactly which version was loaded."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main(path):
    path = Path(path)
    sha = sha256_of(path)
    db = MongoClient(MONGO_URI)["walking_fed"]

    # Idempotency: if this exact file was already loaded, do nothing
    if db.ingestion_log.find_one({"sha256": sha, "status": "loaded"}):
        print(f"{path.name} already loaded (same SHA-256), skipped")
        return

    with open(path, encoding="utf-8") as f:
        features = json.load(f)["features"]

    # One document per point, kept as-is, plus traceability fields
    ingested_at = datetime.now(timezone.utc)
    docs = [{**feature,
             "_ingestion": {"source": SOURCE, "file": path.as_posix(),
                            "sha256": sha, "ingested_at": ingested_at}}
            for feature in features]

    # Remove what a previous failed run of this same file may have left, then insert
    db.raw_water.delete_many({"_ingestion.sha256": sha})
    db.raw_water.insert_many(docs)

    # Register the load only once it has succeeded
    db.ingestion_log.insert_one({
        "source": SOURCE, "file": path.as_posix(), "sha256": sha,
        "size_bytes": path.stat().st_size, "n_records": len(docs),
        "ingested_at": ingested_at, "status": "loaded",
    })
    print(f"{len(docs)} documents loaded into walking_fed.raw_water")


if __name__ == "__main__":
    main(sys.argv[1])