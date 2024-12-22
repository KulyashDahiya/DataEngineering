from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='Submit Spark job with Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the Spark job task
spark_job = SparkSubmitOperator(
    task_id='run_spark_job',
    conn_id='spark_default',
    application='/path/to/your/spark_application.py',  # Path to your PySpark job
    total_executor_cores=2,
    executor_memory='2g',
    driver_memory='1g',
    name='example_spark_job',
    verbose=True,
    dag=dag,
)

# Task dependencies (if needed)
spark_job
