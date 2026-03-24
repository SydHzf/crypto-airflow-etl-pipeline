from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import csv
import os

default_args = {
    'owner': 'huzaifa',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def extract(**context):
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    response = requests.get(url)
    data = response.json()
    print(f"Extracted data: {data}")
    context['ti'].xcom_push(key='raw_data', value=data)

def transform(**context):
    data = context['ti'].xcom_pull(
        task_ids='extract_data',
        key='raw_data'
    )

    if data is None:
        raise ValueError("No data received from extract task")

    transformed_data = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "bitcoin_price": float(data['bitcoin']['usd']),
        "ethereum_price": float(data['ethereum']['usd'])
    }

    context['ti'].xcom_push(key='clean_data', value=transformed_data)

def load(**context):
    data = context['ti'].xcom_pull(
        task_ids='transform_data',
        key='clean_data'
    )

    if data is None:
        raise ValueError("No data received from transform task")

    file_path = "/opt/airflow/data/output.csv"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    file_exists = os.path.isfile(file_path)

    with open(file_path, 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data.keys())

        if not file_exists:
            writer.writeheader()

        writer.writerow(data)

with DAG(
    dag_id='crypto_etl_pipeline',
    default_args=default_args,
    description='Crypto ETL Pipeline',
    schedule='*/5 * * * *',  
    start_date=datetime(2026, 3, 22),
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load
    )

    extract_task >> transform_task >> load_task