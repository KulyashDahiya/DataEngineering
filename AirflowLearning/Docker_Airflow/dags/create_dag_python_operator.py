from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'code2j',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 5)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='Push_Name', key='first_name')
    last_name = ti.xcom_pull(task_ids='Push_Name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello world, My name is {first_name} {last_name}, I am {age} years old.")

def get_name(ti):
    ti.xcom_push(key='first_name', value='Kulyash')
    ti.xcom_push(key='last_name', value='Dahiya')

def get_age(ti):
    ti.xcom_push(key='age', value=24)

with DAG(
    default_args = default_args,
    dag_id = 'our_dag_with_python_operator_v04',
    description = 'Our first dag using python operator',
    start_date = datetime(2024, 12, 17),
    schedule_interval = '@daily'
) as dag:

    task1 = PythonOperator(
        task_id = "greet",
        python_callable=greet,
        # op_kwargs={'age': 24}
    )

    task2 = PythonOperator(
        task_id = "Push_Name",
        python_callable = get_name,
    )

    task3 = PythonOperator(
        task_id = "get_age",
        python_callable = get_age,
    )

    [task2, task3]>>task1
    
    # task2>>task1
    # task3>>task1