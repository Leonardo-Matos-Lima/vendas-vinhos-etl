from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from vendas_etl.pipeline import run_pipeline

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id = "ETL_vinhos",
    default_args = default_args,
    description = "Pipeline ETL de vendas de vinhos com PySpark",
    start_date = datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    tags = ["etl", "vinhos", "pyspark"]
) as dag:
    executar_etl = PythonOperator(
        task_id = "executar_etl",
        python_callable = run_pipeline
    )