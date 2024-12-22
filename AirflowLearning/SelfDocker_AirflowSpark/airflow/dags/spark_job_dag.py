from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_job_example',
    default_args=default_args,
    description='A simple Spark job DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark'],
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/shared_volume/spark_app.py',  # Path to your Spark application
        conn_id='spark_default',
        verbose=True,
        application_args=['arg1', 'arg2'],  # Arguments for the Spark app
    )

    submit_spark_job
