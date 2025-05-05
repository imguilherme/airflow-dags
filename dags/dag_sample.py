from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import os
from datetime import datetime
import urllib3
import pandas as pd


def download_car_csv():
    # URL do arquivo CSV
    url = "https://www12.senado.leg.br/transparencia/prestacao-de-contas/paginas/old-acoes-de-supervisao-controle-e-de-correicao-1/teste.csv/@@download/file/teste.csv"
    
    # Diretório para salvar o arquivo
    output_dir = "/opt/airflow/data/car"
    os.makedirs(output_dir, exist_ok=True)
    
    # Gerando nome do arquivo com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"temas_ambientais_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    print(f"Iniciando download do arquivo CSV em: {url}")
    
    try:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            f.write(response.content)
        print(f"Arquivo salvo com sucesso em: {filepath}")
        
        # Lendo o arquivo CSV com pandas
        print("\nLendo o arquivo CSV com pandas...")
        df = pd.read_csv(filepath, encoding='latin1', sep=';')
        
        # Exibindo informações do dataset
        print(f"\nTotal de registros: {len(df)}")
        print("\nColunas disponíveis:")
        for col in df.columns:
            print(f"- {col}")
            
        # Exibindo as primeiras 5 linhas
        print("\nPrimeiras 5 linhas do dataset:")
        print(df.head())
        
 
    except Exception as e:
        print(f"Erro ao baixar ou processar o arquivo do site: {str(e)}")

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