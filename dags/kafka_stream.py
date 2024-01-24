from airflow.operators.python import PythonOperator
from airflow import DAG
from kafka import KafkaProducer
from datetime import datetime, timedelta
import time
import json
import uuid
import logging
import requests

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
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    current_time = time.time()
    while True:
        # streaming for 1 minutes
        if time.time() > current_time+60: 
            break
        try:
            res =  format_data(get_data())
            producer.send('users_info', json.dumps(res).encode('utf-8'))
        except Exception as e:
            # even if there is an error just log it and continue
            logging.error(f'An error occured: {e}')
            continue

# DAG arguments block
default_args = {
    'owner': 'Sadaf Asadollahi',
    'start_date': datetime.now(),
    'email': ['sadaf@sdf.com'],
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

# Tasks definition block
streaming_task = PythonOperator(
    task_id='stream_data_from_api',
    python_callable=stream_data,
    dag=dag
)
