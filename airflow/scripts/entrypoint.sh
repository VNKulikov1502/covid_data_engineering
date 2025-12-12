#!/usr/bin/env bash

airflow db init
airflow users create \
       --username "${_AIRFLOW_WWW_USER_USERNAME="admin"}" \
       --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME="Airflow"}" \
       --lastname "${_AIRFLOW_WWW_USER_LASTNAME="Admin"}" \
       --email "${_AIRFLOW_WWW_USER_EMAIL="airflowadmin@example.com"}" \
       --role "${_AIRFLOW_WWW_USER_ROLE="Admin"}" \
       --password "${_AIRFLOW_WWW_USER_PASSWORD}" || true

airflow connections add 'trino_conn' \
    --conn-type 'trino' \
    --conn-host 'trino' \
    --conn-login 'admin' \
    --conn-port '8080' \
    || echo "Connection trino_conn already exists"

airflow connections add 'postgres_alerts_conn' \
    --conn-type 'postgres' \
    --conn-host 'postgres_alerts' \
    --conn-login 'postgres' \
    --conn-password 'postgres' \
    --conn-schema 'alerts' \
    --conn-port '5432' \
    || echo "Connection postgres_alerts_conn already exists"

airflow connections add 'postgres_population_conn' \
    --conn-type 'postgres' \
    --conn-host 'postgres_population' \
    --conn-login 'postgres' \
    --conn-password 'postgres' \
    --conn-schema 'postgres' \
    --conn-port '5432' \
    || echo "Connection postgres_population_conn already exists"

airflow connections add 'spark_conn' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-iceberg' \
    --conn-port '7077' \
    || echo "Connection spark_conn already exists"