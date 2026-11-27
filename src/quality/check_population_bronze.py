"""Data quality checks on bronze.population_tiles for one extraction. The task fails if any check fails."""
import pandas as pd

from bronze.load_water_bronze import pg_connect


def main(parquet_path):
    df = pd.read_parquet(parquet_path, columns=["file_sha256"])
    sha, n_parquet = df["file_sha256"].iloc[0], len(df)

    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*),
                   COUNT(*) - COUNT(DISTINCT tile_id),
                   COUNT(*) FILTER (WHERE population IS NULL),
                   COUNT(*) FILTER (WHERE population < 0),
                   COALESCE(SUM(population), 0)
            FROM bronze.population_tiles
            WHERE file_sha256 = %s
        """, (sha,))
        n_bronze, n_duplicates, n_null, n_negative, total = cur.fetchone()

    checks = {
        "row count bronze = Parquet": n_bronze == n_parquet,
        "no duplicate tile_id": n_duplicates == 0,
        "no missing population": n_null == 0,
        "no negative population": n_negative == 0,
        "total population > 0": total > 0,
    }
    for name, passed in checks.items():
        print(f"{'OK    ' if passed else 'FAILED'} {name}")
    print(f"bronze={n_bronze} parquet={n_parquet} duplicates={n_duplicates} "
          f"null={n_null} negative={n_negative} total_population={total:,.0f}")
    if not all(checks.values()):
        raise ValueError("Data quality checks failed on bronze.population_tiles")