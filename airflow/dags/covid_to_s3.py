import os
import requests
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DAG_ID = 'covid_data_pipeline'
CURSOR_VAR_NAME = 'simulation_cursor_date'
TARGET_TABLE = 'iceberg.raw.daily_reports'

CSV_BUCKET = 'covid-daily-reports-csv'
WAREHOUSE_BUCKET = 'warehouse'

MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'password'

def initialize_cursor_if_missing():
    default_start_date = "2020-01-22"
    try:
        Variable.get(CURSOR_VAR_NAME)
        print(f"Переменная {CURSOR_VAR_NAME} уже существует.")
    except KeyError:
        Variable.set(CURSOR_VAR_NAME, default_start_date)
        print(f"Переменная {CURSOR_VAR_NAME} инициализирована значением: {default_start_date}")

def prepare_dates(**kwargs):
    ti = kwargs['ti']
    current_date_str = Variable.get(CURSOR_VAR_NAME)
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
        print(f"Курсор обновлен на {next_date_str}")


with DAG(
        dag_id=DAG_ID,
        start_date=datetime(2025, 12, 18, 12, 10),
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        tags=['ELT', 'covid', 'spark']
) as dag:

    task_init_cursor = PythonOperator(
        task_id='initialize_cursor_variable',
        python_callable=initialize_cursor_if_missing
    )

    task_prepare = PythonOperator(
        task_id='prepare_dates',
        python_callable=prepare_dates
    )

    task_ingest = PythonOperator(
        task_id='extract_load_to_minio',
        python_callable=ingest_github_to_minio
    )

    task_build_raw = SparkSubmitOperator(
        task_id='build_raw_daily_reports',
        conn_id='spark_conn',
        application='/opt/airflow/dags/scripts/process_covid_raw.py',
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=[
            "{{ task_instance.xcom_pull(task_ids='extract_load_to_minio') }}",
            TARGET_TABLE
        ]
    )


    task_build_ods = SparkSubmitOperator(
        task_id='build_ods_daily_country_stats',
        conn_id='spark_conn',
        application='/opt/airflow/dags/scripts/process_covid_ods.py',
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=[
            "{{ var.value.simulation_cursor_date }}"
        ]
    )

    task_build_dds = SparkSubmitOperator(
        task_id='build_dds_star_schema',
        conn_id='spark_conn',
        application='/opt/airflow/dags/scripts/process_covid_dds.py',
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=[
            "{{ var.value.simulation_cursor_date }}"
        ]
    )

    task_build_data_mart = SparkSubmitOperator(
        task_id='build_data_mart',
        conn_id='spark_conn',
        application='/opt/airflow/dags/scripts/process_covid_data_mart.py',
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=[
            "{{ var.value.simulation_cursor_date }}"
        ]
    )

    task_advance = PythonOperator(
        task_id='advance_cursor',
        python_callable=advance_cursor
    )

    trigger_alerts_pipeline = TriggerDagRunOperator(
        task_id="trigger_covid_alerts_pipeline",
        trigger_dag_id="covid_alerts_pipeline",
        wait_for_completion=False, 
        reset_dag_run=True, 
    )

    task_init_cursor >> task_prepare >> task_ingest

    task_ingest >> task_build_raw >> task_build_ods >> task_build_dds >> task_build_data_mart

    task_build_data_mart >> task_advance >> trigger_alerts_pipeline