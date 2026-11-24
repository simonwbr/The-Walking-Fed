from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="osm_drinking_water_MDB",
    default_args=default_args,
    schedule="@weekly",
    catchup=False,
    tags=["MDB", "osm", "drinking_water"],
) as dag:

    download_drinking_water = BashOperator(
        task_id="store_drinking_water",
        bash_command=(
            "python3 /opt/MongoDB/"
            "downloadMany.py"
        ),
    )