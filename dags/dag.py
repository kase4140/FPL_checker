from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import json
from kafka import KafkaProducer
import time
import os
import logging
import requests

default_args = {
    'owner': 'Hamdan',
    'start_date': days_ago(0),
    'email': ['hamdan@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_data():
    # this will have all first 3 queries
    res = requests.get("https://fantasy.premierleague.com/api/leagues-classic/1084104/standings/")
    res = res.json()
    second_json = []
    for i in res["standings"]["results"]:
        res2 = requests.get("https://fantasy.premierleague.com/api/entry/"+str(i["entry"]))
        res2 = res2.json()
        second_json.append(res2)

    return res, second_json

def transformation(res, res2):
    return {"ranking":res, "players":res2}

def streaming():
    # curr_time = time.time()
    producer = KafkaProducer(bootstrap_servers=["kafka1:19092","kafka2:19093","kafka3:19094"])

    # while True:
    #     if time.time() > curr_time + 120:
    #         break
    #     try:
    res, res2 = get_data()
    data = transformation(res, res2)
    print(json.dumps(data, indent=4))
    producer.send("fpl_topic", json.dumps(data).encode("utf-8"))
    # time.sleep(2)
        # except Exception as e:
        #     logging.error(f"an error accurred: {e}")
        #     continue
    return

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