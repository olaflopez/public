from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import requests
import logging
from datetime import datetime

def call_hotel_content_sync():
    base_url = Variable.get("SVC_HOTEL_CONTENT_SYNC_BASE_URL")
    if not base_url:
        raise ValueError("SVC_HOTEL_CONTENT_SYNC_BASE_URL environment variable not set.")

    api = "/utils/v1/atlas/propertyDetails"
    full_url = base_url + api
    
    try:
        response = requests.post(full_url, timeout=1800, verify='/etc/ca-certificates/hyattcacert.pem')  

        if response.status_code == 200:
            logging.info(f"API call to {full_url} succeeded with status code: {response.status_code}.")
        else:
            logging.error(f"API call to {full_url} failed with status code {response.status_code}. Error Response: {response.text}")

    except Exception as e:
        logging.error(f"Error while making the API call: {str(e)}")  
    
with DAG(
    dag_id='sync_hotel_content_data',
    start_date=datetime(2025, 4, 10),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    

    api_call_task = PythonOperator(
        task_id='call_hotel_content_sync',
        python_callable=call_hotel_content_sync
    )

    api_call_task
