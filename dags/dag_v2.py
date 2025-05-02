from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import random

def hello_world():
    logging.basicConfig(level=logging.DEBUG)  # Define nível de log, se necessário
    logger = logging.getLogger("airflow.task")
    logger.debug("Este é um log de DEBUG")
    logger.info("Este é um log de INFO")
    random_number = random.randint(0, 10)
    print(f"Número sorteado: {random_number}")  # Também aparece nos logs da UI

with DAG(
    dag_id="dag_exemplo_2",
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    debug_task = PythonOperator(
        task_id="debug_task",
        python_callable=hello_world
    )