from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

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

#Task ÷

def stream_data():
    import json
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()['results'][0]
    # print(json.dumps(res, indent=3))
