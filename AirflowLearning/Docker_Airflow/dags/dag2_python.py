from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


default_args = {
    'owner' : 'code2j',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 5)
}

def print_return():
    print("Hey Yash, You successfully created a dag which calls a python function to run")
    print("Now lets also create and use xcom to push and pull values")
    return 25

def push_values(ti):
    ti.xcom_push(key = 'my_age', value = 24)
    ti.xcom_push(key = 'my_friend', value = 'Jayant')

def pull_values(ti):
    my_age = ti.xcom_pull(task_ids = "PushingValues", key = 'my_age')
    my_friend = ti.xcom_pull(task_ids = "PushingValues", key = "my_friend")
    print(f"My age is {my_age}. My friend is {my_friend}")

with DAG(
    default_args = default_args,
    dag_id = "dag_python_v02",
    description = "This is self created DAG which uses Python Operator",
    start_date = datetime(2024, 12, 20),
    schedule_interval = timedelta(minutes=2)
) as dag:

    task1 = PythonOperator(
        task_id="PrintReturn",
        python_callable=print_return,
    )

    task2 = PythonOperator(
        task_id = "PushingValues",
        python_callable = push_values
    )

    task3 = PythonOperator(
        task_id = "PullValues",
        python_callable = pull_values
    )

    [task1, task2] >> task3