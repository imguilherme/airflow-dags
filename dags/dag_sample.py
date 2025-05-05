from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import requests
import os
from datetime import datetime

def download_car_csv():
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("airflow.task")
    
    # URL da API
    url = "https://dados.gov.br/api/publico/recurso/registrar-download"
    
    # Headers necessários
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,pt;q=0.7',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://dados.gov.br',
        'referer': 'https://dados.gov.br/dados/conjuntos-dados/metadados-dados-car',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
    }
    
    # Dados do payload
    payload = {
        "id": "48665648-4fb5-4c5d-ada0-4ffe860835bc",
        "idConjuntoDados": "8c733afb-7f4e-42d4-8307-0b80042e0e86",
        "titulo": "Dados Temas Ambientais",
        "descricao": "Conjunto de dados por temas dos imóveis cadastrados no Cadastro Ambiental Rural (CAR).",
        "link": "https://consultapublica.car.gov.br/dados-abertos/D.DLK.FRM.000.TEMAS_AMBIENTAIS.CSV",
        "formato": "csv",
        "tipo": 1
    }
    
    try:
        # Fazendo a requisição POST para registrar o download
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            logger.info("Download registrado com sucesso")
            
            # Criando diretório para salvar o arquivo se não existir
            output_dir = "/opt/airflow/data/car"
            os.makedirs(output_dir, exist_ok=True)
            
            # Gerando nome do arquivo com timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"temas_ambientais_{timestamp}.csv"
            filepath = os.path.join(output_dir, filename)
            
            # Fazendo download do arquivo CSV
            csv_url = payload["link"]
            csv_response = requests.get(csv_url)
            
            if csv_response.status_code == 200:
                # Salvando o arquivo
                with open(filepath, 'wb') as f:
                    f.write(csv_response.content)
                logger.info(f"Arquivo salvo com sucesso em: {filepath}")
                print(f"Arquivo salvo em: {filepath}")
            else:
                logger.error(f"Erro ao baixar o arquivo CSV. Status code: {csv_response.status_code}")
                print(f"Erro ao baixar o arquivo: {csv_response.status_code}")
                
        else:
            logger.error(f"Erro ao registrar download. Status code: {response.status_code}")
            print(f"Erro: {response.status_code}")
            print(f"Resposta: {response.text}")
            
    except Exception as e:
        logger.error(f"Erro ao processar download: {str(e)}")
        print(f"Erro: {str(e)}")

with DAG(
    dag_id="dag_car_temas_ambientais",
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    debug_task = PythonOperator(
        task_id="donwload_car_csv",
        python_callable=download_car_csv
    )