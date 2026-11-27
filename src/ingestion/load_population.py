"""Extract the study-area cells of a GHSL population raster into a Parquet file (landing zone).

One row per cell of the GHSL grid (Mollweide, ESRI:54009), for every cell of the study-area
rectangle, zero population included. Cells without data (nodata) are skipped. The cell size
(1 km or 100 m) is read from the raster itself.
"""
import json
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.warp import transform
from rasterio.windows import Window, from_bounds

from ingestion.load_water import sha256_of

# Study area as a WGS 84 bounding box (default: metropolitan France, Corsica included),
# configurable without changing the code
STUDY_AREA = json.loads(os.getenv(
    "STUDY_AREA_BBOX", '{"min_lon": -5.2, "min_lat": 41.3, "max_lon": 9.6, "max_lat": 51.1}'))

# Top-left corner of the GHSL world grid, in Mollweide metres.
# With the cell size, it gives every cell a global, stable row and column number.
GRID_X0, GRID_Y0 = -18_041_000, 9_000_000


def main(zip_path, out_path, epoch):
    zip_path, out_path = Path(zip_path), Path(out_path)
    sha = sha256_of(zip_path)

    # Idempotency: if this exact raster was already extracted, do nothing
    if out_path.exists() and pd.read_parquet(out_path, columns=["file_sha256"])["file_sha256"].iloc[0] == sha:
        print(f"{out_path} already extracted from this file (same SHA-256), skipped")
        return

    tif = next(n for n in zipfile.ZipFile(zip_path).namelist() if n.endswith(".tif"))
    with rasterio.open(f"zip://{zip_path}!{tif}") as src:
        cell = src.res[0]
        # Study area corners converted to the raster projection, then to a window of whole cells
        b = STUDY_AREA
        xs, ys = transform("EPSG:4326", src.crs,
                           [b["min_lon"], b["max_lon"], b["min_lon"], b["max_lon"]],
                           [b["min_lat"], b["min_lat"], b["max_lat"], b["max_lat"]])
        window = from_bounds(min(xs), min(ys), max(xs), max(ys), transform=src.transform)
        # Keep only the part of the window that lies inside the raster
        window = window.round_offsets().round_lengths().intersection(Window(0, 0, src.width, src.height))
        population = src.read(1, window=window)
        win = src.window_transform(window)
        nodata, crs = src.nodata, src.crs

    # Coordinates of every cell: left edge (x_min) and top edge (y_max)
    rows, cols = np.indices(population.shape)
    x_min = win.c + cols * cell
    y_max = win.f - rows * cell
    grid_col = np.rint((x_min - GRID_X0) / cell).astype(np.int64)
    grid_row = np.rint((GRID_Y0 - y_max) / cell).astype(np.int64)

    df = pd.DataFrame({
        "tile_id": grid_row.ravel() * 1_000_000 + grid_col.ravel(),
        "grid_row": grid_row.ravel(),
        "grid_col": grid_col.ravel(),
        "x_min": x_min.ravel(),
        "y_max": y_max.ravel(),
        "population": population.ravel(),
    })
    df = df[df["population"] != nodata].reset_index(drop=True)

    # Cell centre in WGS 84 longitude / latitude
    df["center_lon"], df["center_lat"] = transform(
        crs, "EPSG:4326", (df["x_min"] + cell / 2).tolist(), (df["y_max"] - cell / 2).tolist())
    df["cell_size"] = cell
    df["epoch"] = int(epoch)
    df["source_file"] = zip_path.as_posix()
    df["file_sha256"] = sha
    df["extracted_at"] = datetime.now(timezone.utc)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"{len(df)} cells of {cell:.0f} m written to {out_path} "
          f"(populated: {(df['population'] > 0).sum()}, total population: {df['population'].sum():,.0f})")
