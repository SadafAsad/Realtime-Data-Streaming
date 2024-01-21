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
