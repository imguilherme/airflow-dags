from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import os
from datetime import datetime
import urllib3

# Desabilitar avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_car_csv():
    # URL do arquivo CSV
    url = "https://consultapublica.car.gov.br/dados-abertos/D.DLK.FRM.000.TEMAS_AMBIENTAIS.CSV"
    
    # Diretório para salvar o arquivo
    output_dir = "/opt/airflow/data/car"
    os.makedirs(output_dir, exist_ok=True)
    
    # Gerando nome do arquivo com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"temas_ambientais_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    try:
        # Fazendo download do arquivo CSV com verify=False para ignorar problemas de SSL
        response = requests.get(url, verify=False)
        response.raise_for_status()
        
        # Salvando o arquivo
        with open(filepath, 'wb') as f:
            f.write(response.content)
        print(f"Arquivo salvo com sucesso em: {filepath}")
        
    except Exception as e:
        print(f"Erro ao baixar o arquivo: {str(e)}")

with DAG(
    dag_id="dag_car_temas_ambientais",
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    download_task = PythonOperator(
        task_id="download_car_csv",
        python_callable=download_car_csv
    )