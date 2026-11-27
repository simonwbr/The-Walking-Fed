from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello from The Walking Fed")


with DAG(
    dag_id="hello_walking_fed",
    start_date=datetime(2026, 11, 1),
    schedule=None,       # manual trigger only
    catchup=False,
    tags=["test"],
) as dag:
    PythonOperator(task_id="say_hello", python_callable=say_hello)