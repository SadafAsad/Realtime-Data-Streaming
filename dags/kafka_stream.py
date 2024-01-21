from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

#DAG arguments block
default_args = {
    'owner': 'Sadaf Asadollahi',
    'start_date': datetime.now(),
    'email': ['sadaf98x@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#DAG definition block
dag = DAG(
    'user_automation',
    default_args=default_args,
    description='User Automation',
    schedule=timedelta(days=1)
)

def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    return res.json()['results'][0]

def format_data(res):
    return(pd.json_normalize(res))

def stream_data():
    return format_data(get_data())
