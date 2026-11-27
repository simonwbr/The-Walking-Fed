"""Data quality checks on bronze.water_points for one file. The task fails if any check fails."""
from pymongo import MongoClient

from bronze.load_water_bronze import MONGO_URI, pg_connect
from ingestion.load_water import sha256_of


def main(path):
    sha = sha256_of(path)
    n_mongo = MongoClient(MONGO_URI)["walking_fed"].raw_water.count_documents({"_ingestion.sha256": sha})

    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*),
                   COUNT(*) - COUNT(DISTINCT (meta_osm_type, meta_osm_id)),
                   COUNT(*) FILTER (WHERE longitude IS NULL OR latitude IS NULL),
                   COUNT(*) FILTER (WHERE latitude NOT BETWEEN -90 AND 90
                                       OR longitude NOT BETWEEN -180 AND 180)
            FROM bronze.water_points
            WHERE file_sha256 = %s
        """, (sha,))
        n_bronze, n_duplicates, n_missing_coords, n_out_of_range = cur.fetchone()

    checks = {
        "row count bronze = MongoDB": n_bronze == n_mongo,
        "no duplicate OSM key": n_duplicates == 0,
        "no missing coordinates": n_missing_coords == 0,
        "coordinates within valid range": n_out_of_range == 0,
    }
    for name, passed in checks.items():
        print(f"{'OK    ' if passed else 'FAILED'} {name}")
    print(f"bronze={n_bronze} mongo={n_mongo} duplicates={n_duplicates} "
          f"missing_coords={n_missing_coords} out_of_range={n_out_of_range}")
    if not all(checks.values()):
        raise ValueError("Data quality checks failed on bronze.water_points")