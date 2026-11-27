"""The Walking Fed: one DAG from downloaded files to the gold layer.

ingestion (download)  ->  landing (MongoDB / Parquet)  ->  bronze (PostgreSQL)  ->  dbt (silver, gold, tests)

Every source file is stored under DATA_DIR/raw/<source>/<file date>/, so a run is parameterised by
the date (version) of each file. Every step is idempotent: running it twice gives the same result.
"""
import os
import zipfile
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

DATA_DIR = Path(os.environ["DATA_DIR"])

WATER_URL = ("https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
             "osm-france-drinking-water/exports/geojson")
GHSL_URL = ("https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/"
            "GHS_POP_E{epoch}_GLOBE_R2023A_54009_1000/V1-0/GHS_POP_E{epoch}_GLOBE_R2023A_54009_1000_V1_0.zip")
# Falling Fruit exports (data page of fallingfruit.org): paste the links of the two download buttons
FRUIT_URLS = {
    "locations.csv.bz2": "https://fallingfruit.org/locations.csv.bz2",
    "types.csv.bz2": "https://fallingfruit.org/types.csv.bz2",
}
COMMUNES_URL = ("https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/"
                "communes-version-simplifiee.geojson")

# dbt executable and project folder come from the environment (docker-compose.yml), not from the code
DBT_COMMAND = '"$DBT_BIN" {command} --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROJECT_DIR"'


# ---------- File paths, built from the run parameters ----------

def water_file(params):
    return DATA_DIR / "raw" / "water" / params["water_file_date"] / "osm-france-drinking-water.geojson"


def ghsl_zip(params):
    name = f"GHS_POP_E{params['ghsl_epoch']}_GLOBE_R2023A_54009_1000_V1_0.zip"
    return DATA_DIR / "raw" / "population" / params["population_file_date"] / name


def ghsl_parquet(params):
    return DATA_DIR / "landing" / "population" / params["population_file_date"] / "population_tiles.parquet"


def fruit_file(params, name):
    return DATA_DIR / "raw" / "fruit" / params["fruit_file_date"] / name


def communes_file(params):
    return DATA_DIR / "raw" / "communes" / params["communes_file_date"] / "communes-version-simplifiee.geojson"


# ---------- Task functions ----------

def download(url, path):
    """Download a file once. A partial download never takes the final name."""
    import requests

    path = Path(path)
    if path.exists():
        print(f"{path} already downloaded, skipped")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(part, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    part.rename(path)
    print(f"{url} downloaded to {path} ({path.stat().st_size:,} bytes)")


def check_geojson_file(path, min_features):
    """Data quality gate before loading: valid GeoJSON with enough features."""
    import json

    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    n = len(data.get("features", []))
    print(f"{path}: {n} features")
    if data.get("type") != "FeatureCollection" or n < min_features:
        raise ValueError(f"{path} is not a FeatureCollection with at least {min_features} features")


def check_fruit_files(locations_path, types_path):
    """Data quality gate before loading: both bz2 CSV files open and hold the expected columns."""
    import bz2
    import csv

    expected = {locations_path: {"id", "type_ids", "lat", "lng", "hidden", "unverified"},
                types_path: {"id", "en_name", "scientific_name", "category_mask", "pending"}}
    for path, columns in expected.items():
        with bz2.open(path, "rt", encoding="utf-8") as f:
            header = set(next(csv.reader(f)))
        missing = columns - header
        if missing:
            raise ValueError(f"{path} lacks the columns {sorted(missing)}")
        print(f"{path}: OK")


def check_ghsl_file(path):
    """Data quality gate before extraction: valid zip holding exactly one GeoTIFF."""
    if not zipfile.is_zipfile(path):
        raise ValueError(f"{path} is not a valid zip file")
    tifs = [n for n in zipfile.ZipFile(path).namelist() if n.endswith(".tif")]
    if len(tifs) != 1:
        raise ValueError(f"{path} should hold exactly one .tif, found {tifs}")
    print(f"{path}: OK ({tifs[0]})")


def run(module, function="main"):
    """Return a callable that imports src/<module>.py only when the task runs (keeps the DAG fast to parse)."""
    def _call(*args, **kwargs):
        import importlib
        return getattr(importlib.import_module(module), function)(*args, **kwargs)
    return _call


# ---------- DAG ----------

with DAG(
    dag_id="walking_fed_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    # At most 2 tasks at a time: keeps the memory of the Airflow container under control
    max_active_tasks=2,
    params={
        "water_file_date": Param("2026-11-24", type="string", description="Version (date) of the water file"),
        "population_file_date": Param("2026-11-27", type="string", description="Version (date) of the GHSL file"),
        "ghsl_epoch": Param("2025", type="string", description="GHSL population year (1975 to 2030, every 5 years)"),
        "fruit_file_date": Param("2026-11-27", type="string", description="Version (date) of the Falling Fruit export"),
        "communes_file_date": Param("2026-11-27", type="string", description="Version (date) of the commune boundaries"),
    },
    tags=["walking_fed"],
) as dag:

    with TaskGroup("ingestion") as ingestion:
        water_download = PythonOperator(
            task_id="huwise_water_download",
            python_callable=lambda **c: download(WATER_URL, water_file(c["params"])))
        ghsl_download = PythonOperator(
            task_id="ghsl_download",
            python_callable=lambda **c: download(
                GHSL_URL.format(epoch=c["params"]["ghsl_epoch"]), ghsl_zip(c["params"])))
        fruit_download = PythonOperator(
            task_id="fruit_download",
            python_callable=lambda **c: [download(url, fruit_file(c["params"], name))
                                         for name, url in FRUIT_URLS.items()])
        communes_download = PythonOperator(
            task_id="communes_download",
            python_callable=lambda **c: download(COMMUNES_URL, communes_file(c["params"])))

    with TaskGroup("landing") as landing:
        water_check = PythonOperator(
            task_id="check_water_file",
            python_callable=lambda **c: check_geojson_file(water_file(c["params"]), min_features=1000))
        water_mongo = PythonOperator(
            task_id="store_water_in_mongodb",
            python_callable=lambda **c: run("ingestion.load_water")(water_file(c["params"])))
        ghsl_check = PythonOperator(
            task_id="check_ghsl_file",
            python_callable=lambda **c: check_ghsl_file(ghsl_zip(c["params"])))
        ghsl_parquet_task = PythonOperator(
            task_id="store_ghsl_as_parquet",
            python_callable=lambda **c: run("ingestion.load_population")(
                ghsl_zip(c["params"]), ghsl_parquet(c["params"]), c["params"]["ghsl_epoch"]))
        fruit_check = PythonOperator(
            task_id="check_fruit_files",
            python_callable=lambda **c: check_fruit_files(
                fruit_file(c["params"], "locations.csv.bz2"), fruit_file(c["params"], "types.csv.bz2")))
        fruit_mongo = PythonOperator(
            task_id="store_fruit_in_mongodb",
            python_callable=lambda **c: run("ingestion.load_fruit")(
                fruit_file(c["params"], "locations.csv.bz2"), fruit_file(c["params"], "types.csv.bz2")))
        communes_check = PythonOperator(
            task_id="check_communes_file",
            python_callable=lambda **c: check_geojson_file(communes_file(c["params"]), min_features=30000))

    with TaskGroup("bronze") as bronze:
        water_bronze = PythonOperator(
            task_id="load_water_bronze",
            python_callable=lambda **c: run("bronze.load_water_bronze")(water_file(c["params"])))
        water_quality = PythonOperator(
            task_id="check_water_bronze",
            python_callable=lambda **c: run("quality.check_water_bronze")(water_file(c["params"])))
        population_bronze = PythonOperator(
            task_id="load_population_bronze",
            python_callable=lambda **c: run("bronze.load_population_bronze")(ghsl_parquet(c["params"])))
        population_quality = PythonOperator(
            task_id="check_population_bronze",
            python_callable=lambda **c: run("quality.check_population_bronze")(ghsl_parquet(c["params"])))
        fruit_bronze = PythonOperator(
            task_id="load_fruit_bronze",
            python_callable=lambda **c: run("bronze.load_fruit_bronze")(
                fruit_file(c["params"], "locations.csv.bz2"), fruit_file(c["params"], "types.csv.bz2")))
        fruit_quality = PythonOperator(
            task_id="check_fruit_bronze",
            python_callable=lambda **c: run("quality.check_fruit_bronze")(
                fruit_file(c["params"], "locations.csv.bz2"), fruit_file(c["params"], "types.csv.bz2")))
        communes_bronze = PythonOperator(
            task_id="load_communes_bronze",
            python_callable=lambda **c: run("bronze.load_communes_bronze")(communes_file(c["params"])))
        communes_quality = PythonOperator(
            task_id="check_communes_bronze",
            python_callable=lambda **c: run("quality.check_communes_bronze")(communes_file(c["params"])))

    with TaskGroup("dbt") as dbt:
        # Silver and gold are rebuilt; the gold facts and the water dimension are incremental
        dbt_run = BashOperator(task_id="dbt_run", bash_command=DBT_COMMAND.format(command="run"))
        dbt_test = BashOperator(task_id="dbt_test", bash_command=DBT_COMMAND.format(command="test"))

    water_download >> water_check >> water_mongo >> water_bronze >> water_quality
    ghsl_download >> ghsl_check >> ghsl_parquet_task >> population_bronze >> population_quality
    fruit_download >> fruit_check >> fruit_mongo >> fruit_bronze >> fruit_quality
    communes_download >> communes_check >> communes_bronze >> communes_quality
    [water_quality, population_quality, fruit_quality, communes_quality] >> dbt_run >> dbt_test
