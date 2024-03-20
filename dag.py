from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from kafka_stream import streaming

default_args = {
    'owner': 'Hamdan',
    'start_date': days_ago(0),
    'email': ['hamdan@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG("fpl_dag",
    default_args=default_args,
    description='this dag streams fpl data to kafka',
    schedule_interval='0 19 8 * 1'
) as dag:
    streaming_task = PythonOperator(
        task_id = 'streaming_data',
        python_callable=streaming,
    )

    streaming_task