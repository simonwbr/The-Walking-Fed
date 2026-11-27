"""Load the French commune boundaries (GeoJSON reference file) into PostgreSQL (bronze).

Geometries are kept as GeoJSON text: they are converted to PostGIS geometries in the silver layer (dbt).
"""
import json
from datetime import datetime, timezone
from pathlib import Path

from psycopg2.extras import execute_values

from bronze.load_water_bronze import pg_connect
from ingestion.load_water import sha256_of

DDL = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE IF NOT EXISTS bronze.communes (
    commune_code TEXT,
    commune_name TEXT,
    geometry_geojson TEXT,
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def main(path):
    path = Path(path)
    sha = sha256_of(path)
    with open(path, encoding="utf-8") as f:
        features = json.load(f)["features"]
    if not features:
        raise ValueError(f"{path} contains no feature")

    ingested_at = datetime.now(timezone.utc)
    rows = [(ft["properties"].get("code"), ft["properties"].get("nom"),
             json.dumps(ft["geometry"]) if ft.get("geometry") else None,
             path.as_posix(), sha, ingested_at)
            for ft in features]

    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        # Idempotency: replace what a previous run loaded from this same file
        cur.execute("DELETE FROM bronze.communes WHERE file_sha256 = %s", (sha,))
        execute_values(cur, "INSERT INTO bronze.communes (commune_code, commune_name, geometry_geojson, "
                            "source_file, file_sha256, ingested_at) VALUES %s", rows, page_size=2000)
    print(f"{len(rows)} rows loaded into bronze.communes")
