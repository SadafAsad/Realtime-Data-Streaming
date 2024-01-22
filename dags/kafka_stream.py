from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from kafka import KafkaProducer
import time
import json
import uuid

def get_data():
    res = requests.get("https://randomuser.me/api/")
    return res.json()['results'][0]

def format_data(res):
    data = {}
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    location = res['location']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']

    return data

def stream_data():
    res =  format_data(get_data())
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    producer.send('users_created', json.dumps(res).encode('utf-8'))

# DAG arguments block
default_args = {
    'owner': 'Sadaf Asadollahi',
    'start_date': datetime.now(),
    'email': ['sadaf98x@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition block
dag = DAG(
    'user_automation',
    default_args=default_args,
    description='User Automation',
    schedule=timedelta(days=1)
)

# Task definition block
stream_data = PythonOperator(
    task_id='stream_data_from_api',
    python_callable=stream_data,
    dag=dag
)
