from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

def hello_world():
    logging.basicConfig(level=logging.DEBUG)  # Define nível de log, se necessário
    logger = logging.getLogger("airflow.task")
    logger.debug("Este é um log de DEBUG")
    logger.info("Este é um log de INFO")
    print("Mensagem comum via print()")  # Também aparece nos logs da UI

with DAG(
    dag_id="dag_com_debug",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    debug_task = PythonOperator(
        task_id="debug_task",
        python_callable=hello_world
    )