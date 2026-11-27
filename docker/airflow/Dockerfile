FROM apache/airflow:2.10.5-python3.11

# Project libraries, with Airflow's official version constraints
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    -c https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt

# dbt in its own virtual environment, to avoid version conflicts with Airflow's libraries.
# Both versions are pinned so that every build installs the same dbt.
RUN python -m venv /opt/airflow/dbt_venv \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-core==1.12.7" "dbt-postgres==1.9.0"