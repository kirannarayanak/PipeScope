from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG("sample_pipeline", schedule=None, catchup=False) as dag:
    extract = BashOperator(task_id="extract", bash_command="echo extract")
    transform = PythonOperator(task_id="transform", python_callable=lambda: 1)
    load = BigQueryInsertJobOperator(
        task_id="load",
        sql="select * from analytics.table_a",
    )
    notify = BashOperator(task_id="notify", bash_command="echo notify")

    extract >> transform >> load
    extract.set_downstream(notify)
