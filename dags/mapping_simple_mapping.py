from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def add_one(x: int):
    return x + 1


def sum_it(values):
    total = sum(values)
    print(f"Total was {total}")

with DAG(dag_id="simple_mapping", start_date=datetime(2022, 3, 4)) as dag:


    add_one = PythonOperator(
        task_id='add_one',
        python_callable= add_one,
        dag=dag,
        )

    sum_it = PythonOperator(
        task_id='sum_it',
        python_callable= sum_it,
        dag=dag,
        )

    added_values = add_one.expand(x=[1, 2, 3])
    sum_it(added_values)