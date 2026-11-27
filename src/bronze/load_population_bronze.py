"""Copy the population cells of one GHSL extraction from Parquet (landing) to PostgreSQL (bronze).

The Parquet file is read in batches, so memory use stays small even for the whole of France.
"""
import pyarrow.parquet as pq
from psycopg2.extras import execute_values

from bronze.load_water_bronze import pg_connect

DDL = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE IF NOT EXISTS bronze.population_tiles (
    tile_id BIGINT NOT NULL,
    grid_row INTEGER NOT NULL,
    grid_col INTEGER NOT NULL,
    x_min DOUBLE PRECISION NOT NULL,
    y_max DOUBLE PRECISION NOT NULL,
    cell_size DOUBLE PRECISION NOT NULL,
    center_lon DOUBLE PRECISION NOT NULL,
    center_lat DOUBLE PRECISION NOT NULL,
    population DOUBLE PRECISION,
    epoch INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

COLUMNS = ["tile_id", "grid_row", "grid_col", "x_min", "y_max", "cell_size", "center_lon", "center_lat",
           "population", "epoch", "source_file", "file_sha256", "extracted_at"]
BATCH = 50_000


def main(parquet_path):
    parquet = pq.ParquetFile(parquet_path)
    n_rows = parquet.metadata.num_rows
    if n_rows == 0:
        raise ValueError(f"{parquet_path} is empty: run the landing task first")
    sha = parquet.read_row_group(0, columns=["file_sha256"]).column(0)[0].as_py()

    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        # Idempotency: this extraction is already fully loaded, nothing to do
        cur.execute("SELECT COUNT(*) FROM bronze.population_tiles WHERE file_sha256 = %s", (sha,))
        if cur.fetchone()[0] == n_rows:
            print(f"bronze.population_tiles already holds the {n_rows} rows of this extraction, skipped")
            return
        # Otherwise replace whatever a previous (partial) run loaded from this same raster
        cur.execute("DELETE FROM bronze.population_tiles WHERE file_sha256 = %s", (sha,))
        for batch in parquet.iter_batches(batch_size=BATCH, columns=COLUMNS):
            # to_pylist() gives plain Python values, which psycopg2 understands
            rows = [tuple(r[c] for c in COLUMNS) for r in batch.to_pylist()]
            execute_values(cur, f"INSERT INTO bronze.population_tiles ({', '.join(COLUMNS)}) VALUES %s",
                           rows, page_size=5000)
    print(f"{n_rows} rows loaded into bronze.population_tiles")
