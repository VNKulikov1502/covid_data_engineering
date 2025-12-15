import os
import requests
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

DAG_ID = 'covid_data_pipeline'
CURSOR_VAR_NAME = 'simulation_cursor_date'
TARGET_TABLE = 'iceberg.raw.daily_reports'

CSV_BUCKET = 'covid-daily-reports-csv' 
WAREHOUSE_BUCKET = 'warehouse'        

MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'password'


def prepare_dates(**kwargs):
    ti = kwargs['ti']
    current_date_str = Variable.get(CURSOR_VAR_NAME, default_var="2021-01-22")
    current_date = datetime.strptime(current_date_str, '%Y-%m-%d')
    next_date = current_date + timedelta(days=1)

    print(f"Дата симуляции: {current_date_str}")

    file_name_github = current_date.strftime('%m-%d-%Y') + ".csv"

    s3_key = f"year={current_date.year}/month={current_date.month}/{current_date.strftime('%Y-%m-%d')}.csv"

    ti.xcom_push(key='github_filename', value=file_name_github)
    ti.xcom_push(key='s3_key', value=s3_key)
    ti.xcom_push(key='next_date_var', value=next_date.strftime('%Y-%m-%d'))


def ingest_github_to_minio(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids='prepare_dates', key='github_filename')
    s3_key = ti.xcom_pull(task_ids='prepare_dates', key='s3_key')

    url = f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{file_name}"

    print(f"Скачиваем: {url}")
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"GitHub error {response.status_code} for {file_name}")

    print(f"Сохраняем в CSV-бакет: s3://{CSV_BUCKET}/{s3_key}")

    s3_client = boto3.client('s3',
                             endpoint_url=MINIO_ENDPOINT,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY
                             )

    try:
        s3_client.head_bucket(Bucket=CSV_BUCKET)
    except:
        s3_client.create_bucket(Bucket=CSV_BUCKET)

    s3_client.put_object(
        Bucket=CSV_BUCKET,
        Key=s3_key,
        Body=response.content
    )

    return f"s3a://{CSV_BUCKET}/{s3_key}"


def advance_cursor(**kwargs):
    ti = kwargs['ti']
    next_date_str = ti.xcom_pull(task_ids='prepare_dates', key='next_date_var')
    if next_date_str:
        Variable.set(CURSOR_VAR_NAME, next_date_str)

with DAG(
        dag_id=DAG_ID,
        start_date=datetime(2025, 12, 12),
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        tags=['ELT', 'covid', 'spark']
) as dag:

    task_prepare = PythonOperator(
        task_id='prepare_dates',
        python_callable=prepare_dates
    )

    task_ingest = PythonOperator(
        task_id='extract_load_to_minio',
        python_callable=ingest_github_to_minio
    )

    task_transform = SparkSubmitOperator(
        task_id='transform_spark_job',
        conn_id='spark_conn',
        application='/opt/airflow/dags/scripts/process_covid_s3.py',
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=[
            "{{ task_instance.xcom_pull(task_ids='extract_load_to_minio') }}",
            TARGET_TABLE
        ]
    )


    task_build_ods = SparkSubmitOperator(
    task_id='build_ods_daily_country_stats',
    conn_id='spark_conn',
    application='/opt/airflow/dags/scripts/build_ods_daily_stats.py',
    packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.hadoop:hadoop-aws:3.3.4",
    application_args=[
        "{{ var.value.simulation_cursor_date }}"
    ]

    )

    task_advance = PythonOperator(
        task_id='advance_cursor',
        python_callable=advance_cursor
    )

    task_prepare >> task_ingest >> task_transform >> task_build_ods >> task_advance