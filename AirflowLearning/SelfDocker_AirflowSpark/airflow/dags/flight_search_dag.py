from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'flight_search_dag',
    default_args=default_args,
    description='A DAG to manage Spark jobs for flight search data',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    ingestion_task = SparkSubmitOperator(
        task_id='flight_search_ingestion',
        application='/shared_volume/data_ingestion.py',
        conn_id='spark_default',
        total_executor_cores=2,
        executor_cores=1,
        executor_memory='1g',
        driver_memory='1g',
        verbose=True,
    )

    waiting_time_task = SparkSubmitOperator(
        task_id='search_waiting_time',
        application='/shared_volume/search_waiting_time.py',
        conn_id='spark_default',
        total_executor_cores=2,
        executor_cores=1,
        executor_memory='1g',
        driver_memory='1g',
        verbose=True,
    )

    nb_search_task = SparkSubmitOperator(
        task_id='nb_search',
        application='/shared_volume/nb_search.py',
        conn_id='spark_default',
        total_executor_cores=2,
        executor_cores=1,
        executor_memory='1g',
        driver_memory='1g',
        verbose=True,
    )

    ingestion_task >> [waiting_time_task, nb_search_task]
