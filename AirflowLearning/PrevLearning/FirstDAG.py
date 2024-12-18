#STEP 1
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

#STEP 2

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2023, 9, 6),
    'retries' : 0
}

#STEP 3

dag = DAG(dag_id = 'DAG-1', default_args = default_args, catchup = False, schedule_interval = '@once')

#STEP 4

start = DummyOperator(task_id = 'start', dag = dag)
end = DummyOperator(task_id = 'end', dag = dag)

#STEP 5

start >> end


