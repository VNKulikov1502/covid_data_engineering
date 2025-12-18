from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email_smtp
from datetime import datetime, timedelta
from pathlib import Path
from airflow.models import Variable
import ast

BASE_DIR = Path(__file__).parent / "sql"
ALERT_DATE = datetime.strptime(Variable.get("simulation_cursor_date"), "%Y-%m-%d").date() - timedelta(days=1)

def create_postgres_schema_and_table():
    pg_hook = PostgresHook(postgres_conn_id="postgres_alerts_conn")
    pg_hook.run("CREATE SCHEMA IF NOT EXISTS alerts;")
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS alerts.covid_alerts (
            alert_id BIGSERIAL PRIMARY KEY,
            alert_date DATE NOT NULL,
            country TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            metric_value DOUBLE PRECISION,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

def run_trino_sql(filename):
    trino_hook = TrinoHook(trino_conn_id="trino_conn")
    sql_path = BASE_DIR / filename
    with open(sql_path) as f:
        sql_template = f.read()
    sql = sql_template.replace("{{ alert_date }}", ALERT_DATE.isoformat())
    trino_hook.run(sql)

def notify_new_alerts():
    pg_hook = PostgresHook(postgres_conn_id="postgres_alerts_conn")
    query = f"""
        SELECT country, description
        FROM alerts.covid_alerts
        WHERE alert_date = DATE '{ALERT_DATE}'
    """
    new_alerts = pg_hook.get_records(query)
    
    if new_alerts:
        message = f"<h3>Появились новые COVID алерты за {ALERT_DATE}</h3><ul>"
        for country, description in new_alerts:
            message += f"<li>{country}:  описание: {description}</li>"
        message += "</ul>"
        
        send_email_smtp(
            to=Variable.get("emails", deserialize_json=True),
            subject=f"Новые COVID алерты за {ALERT_DATE}",
            html_content=message
        )
    else:
        print(f"Новых алертов за {ALERT_DATE} нет.")

with DAG(
    dag_id="covid_alerts_pipeline",
    start_date=datetime(2021, 1, 22),
    schedule_interval=None,
    catchup=False,
    tags=["alerts", "trino", "covid"],
) as dag:

    create_table_task = PythonOperator(
        task_id="create_alerts_table",
        python_callable=create_postgres_schema_and_table
    )

    case_spike_alert = PythonOperator(
        task_id="case_spike_alert",
        python_callable=run_trino_sql,
        op_args=["alert_case_spike.sql"]
    )

    death_spike_alert = PythonOperator(
        task_id="death_spike_alert",
        python_callable=run_trino_sql,
        op_args=["alert_death_spike.sql"]
    )

    incidence_alert = PythonOperator(
        task_id="incidence_100k_alert",
        python_callable=run_trino_sql,
        op_args=["alert_incidence.sql"]
    )

    high_mortality_alert = PythonOperator(
        task_id="deaths_100k_alert",
        python_callable=run_trino_sql,
        op_args=["deaths_incidence.sql"]
    )

    notify_alerts_task = PythonOperator(
        task_id="notify_new_alerts",
        python_callable=notify_new_alerts
    )

    create_table_task >> [
        case_spike_alert, death_spike_alert, incidence_alert, high_mortality_alert
        ] >> notify_alerts_task