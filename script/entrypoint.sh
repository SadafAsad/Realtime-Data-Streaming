#!/bin/bash

# ------------------------------------------------------------------------
# Airflow should follow these commands when trying to initialize scheduler
# ------------------------------------------------------------------------

set -e
if [ -e "opt/airflow/requirements.txt" ]; then
    $(command -v pip) install --user -r requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
    airflow db init && \
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role admin \
        --email admin@admin.admin \
        --password admin
fi

$(command -v airflow) db upgrade

exec airflow webserver
