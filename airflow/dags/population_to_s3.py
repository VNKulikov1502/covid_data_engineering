from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

TRINO_CONN_ID = 'trino_conn'

def check_population_data_exists():
    hook = BaseHook.get_connection(TRINO_CONN_ID).get_hook()
    sql_query = "SELECT COUNT(*) FROM iceberg.raw.country_population"

    try:
        record_count = hook.get_first(sql_query)[0]
        if record_count > 0:
            return 'skip_insertion'
        else:
            return 'insert_data'
    except Exception:
        return 'insert_data'

with DAG(
        dag_id='population_to_s3',
        start_date=datetime(2025, 12, 12),
        schedule_interval=None,
        catchup=False,
        tags=['initial-load', 'trino']
) as dag:

    create_schema = TrinoOperator(
        task_id='create_iceberg_raw_schema',
        trino_conn_id=TRINO_CONN_ID,
        sql="CREATE SCHEMA IF NOT EXISTS iceberg.raw"
    )

    create_table = TrinoOperator(
        task_id='create_iceberg_population_table',
        trino_conn_id=TRINO_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS iceberg.raw.country_population (
                country VARCHAR,
                country_code VARCHAR,
                year INTEGER,
                population BIGINT
            )
            WITH (
                format = 'PARQUET'
            )
        """
    )

    check_data_branch = BranchPythonOperator(
        task_id='check_data_branch',
        python_callable=check_population_data_exists,
    )

    insert_data = TrinoOperator(
        task_id='insert_data',
        trino_conn_id=TRINO_CONN_ID,
        sql=f"""INSERT INTO iceberg.raw.country_population 
               SELECT country, country_code, year, population 
               FROM source_population.public.country_population
        """
    )

    skip_insertion = EmptyOperator(
        task_id='skip_insertion',
        trigger_rule='none_failed_min_one_success'
    )

    create_schema >> create_table >> check_data_branch >> [insert_data, skip_insertion]