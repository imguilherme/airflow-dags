from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id='sample_dag',
    schedule_interval='@daily',
    catchup=False,
    tags=['sample'],
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    inicio >> fim