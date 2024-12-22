from airflow.decorators import dag, task
from datetiem import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    schedule=None,
    catchup=False
)

def my_dag():

    read_data = SparkSubmitOperator(
        task_id="read_data",
        application="./include/scrips/read.py",
        conn_id="my_spark_conn",
        verbose=True
    )

    read_data

my_dag()