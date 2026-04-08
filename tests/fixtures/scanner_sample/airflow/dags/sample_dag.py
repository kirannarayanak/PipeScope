"""Sample DAG for scanner tests."""
from datetime import datetime

from airflow import DAG

with DAG(
    dag_id="sample",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    pass
