from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id='sample_dag',
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime(2024, 1, 1),  # <<< start_date obrigatório
    tags=['sample'],
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> end


    