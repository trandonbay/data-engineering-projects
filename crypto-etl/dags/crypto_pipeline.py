from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import sys

sys.path.append('/opt/airflow/etl')

from etl import main

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

def get_data(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    df = hook.get_pandas_df("SELECT * FROM clean_prices;")
    html_table = df.to_html(index=False)
    context['ti'].xcom_push(key='table_html', value=html_table)

with DAG(
    dag_id='crypto_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=main
    )

    get_data_task = PythonOperator(
        task_id="get_data",
        python_callable=get_data
    )

    send_email_task = EmailOperator(
        task_id="send_email",
        to="brandontay95@gmail.com",
        subject="Daily Crypto Report",
        html_content="""
        <h3>Latest Prices</h3>
        {{ ti.xcom_pull(task_ids='get_data', key='table_html') }}
        """
    )

    run_etl >> get_data_task >> send_email_task