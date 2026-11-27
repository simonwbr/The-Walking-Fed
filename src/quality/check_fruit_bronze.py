"""Data quality checks on bronze.fruit_locations and bronze.fruit_types for one export.
The task fails if any check fails."""
import json
import os

from pymongo import MongoClient

from bronze.load_water_bronze import pg_connect
from ingestion.load_water import sha256_of

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
STUDY_AREA = json.loads(os.getenv(
    "STUDY_AREA_BBOX", '{"min_lon": -5.2, "min_lat": 41.3, "max_lon": 9.6, "max_lat": 51.1}'))
MAX_UNKNOWN_TYPE_SHARE = 0.01


def main(locations_path, types_path):
    loc_sha, type_sha = sha256_of(locations_path), sha256_of(types_path)
    with MongoClient(MONGO_URI) as client:
        db = client["walking_fed"]
        n_mongo_loc = db.raw_fruit_locations.count_documents({"_ingestion.sha256": loc_sha})
        n_mongo_types = db.raw_fruit_types.count_documents({"_ingestion.sha256": type_sha})

    b = STUDY_AREA
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*),
                   COUNT(*) - COUNT(DISTINCT id),
                   COUNT(*) FILTER (WHERE type_ids IS NULL),
                   COUNT(*) FILTER (WHERE NOT (latitude BETWEEN %s AND %s AND longitude BETWEEN %s AND %s))
            FROM bronze.fruit_locations WHERE file_sha256 = %s
        """, (b["min_lat"], b["max_lat"], b["min_lon"], b["max_lon"], loc_sha))
        n_loc, loc_duplicates, n_no_type, n_outside = cur.fetchone()

        cur.execute("SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT id) FROM bronze.fruit_types WHERE file_sha256 = %s",
                    (type_sha,))
        n_types, type_duplicates = cur.fetchone()

        # Type ids used by the locations that do not exist in the types file
        cur.execute("""
            WITH used AS (
                SELECT trim(t) AS type_id
                FROM bronze.fruit_locations, unnest(string_to_array(type_ids, ',')) AS t
                WHERE file_sha256 = %s
            )
            SELECT COUNT(*), COUNT(*) FILTER (WHERE type_id NOT IN
                (SELECT id FROM bronze.fruit_types WHERE file_sha256 = %s))
            FROM used
        """, (loc_sha, type_sha))
        n_links, n_unknown = cur.fetchone()

    checks = {
        "locations: row count bronze = MongoDB": n_loc == n_mongo_loc and n_loc > 0,
        "locations: no duplicate id": loc_duplicates == 0,
        "locations: every location has a type": n_no_type == 0,
        "locations: coordinates inside the study area": n_outside == 0,
        "types: row count bronze = MongoDB": n_types == n_mongo_types and n_types > 0,
        "types: no duplicate id": type_duplicates == 0,
        f"at most {MAX_UNKNOWN_TYPE_SHARE:.0%} of type links to an unknown type": n_unknown <= MAX_UNKNOWN_TYPE_SHARE * n_links,
    }
    for name, passed in checks.items():
        print(f"{'OK    ' if passed else 'FAILED'} {name}")
    print(f"locations={n_loc} (mongo {n_mongo_loc}) types={n_types} (mongo {n_mongo_types}) "
          f"no_type={n_no_type} outside={n_outside} type_links={n_links} unknown_type_links={n_unknown}")
    if not all(checks.values()):
        raise ValueError("Data quality checks failed on bronze.fruit_locations / bronze.fruit_types")
