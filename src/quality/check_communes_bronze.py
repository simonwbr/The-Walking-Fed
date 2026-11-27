"""Data quality checks on bronze.communes for one file. The task fails if any check fails."""
import json

from bronze.load_water_bronze import pg_connect
from ingestion.load_water import sha256_of

MAX_MISSING_GEOMETRY_SHARE = 0.01


def main(path):
    sha = sha256_of(path)
    with open(path, encoding="utf-8") as f:
        n_file = len(json.load(f)["features"])

    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*),
                   COUNT(*) - COUNT(DISTINCT commune_code),
                   COUNT(*) FILTER (WHERE commune_code IS NULL OR commune_name IS NULL),
                   COUNT(*) FILTER (WHERE geometry_geojson IS NULL)
            FROM bronze.communes
            WHERE file_sha256 = %s
        """, (sha,))
        n_bronze, n_duplicates, n_missing_ids, n_missing_geom = cur.fetchone()

    checks = {
        "row count bronze = file": n_bronze == n_file,
        "no duplicate commune code": n_duplicates == 0,
        "no missing code or name": n_missing_ids == 0,
        f"at most {MAX_MISSING_GEOMETRY_SHARE:.0%} without geometry": n_missing_geom <= MAX_MISSING_GEOMETRY_SHARE * n_bronze,
    }
    for name, passed in checks.items():
        print(f"{'OK    ' if passed else 'FAILED'} {name}")
    print(f"bronze={n_bronze} file={n_file} duplicates={n_duplicates} "
          f"missing_ids={n_missing_ids} missing_geometry={n_missing_geom}")
    if not all(checks.values()):
        raise ValueError("Data quality checks failed on bronze.communes")
