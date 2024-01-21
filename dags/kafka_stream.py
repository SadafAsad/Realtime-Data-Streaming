from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

#DAG arguments block
default_args = {
    'owner': 'Sadaf Asadollahi',
    'start_date': days_ago(0),
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
    schedule_interval=timedelta(days=1)
)

#Task definition block
streaming_data = PythonOperator(
    task_id='stream_data_from_api',
    python_callable=stream_data
)

def stream_data():
    import json
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()['results'][0]
    # print(json.dumps(res, indent=3))
