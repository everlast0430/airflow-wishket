# 보안상의 이유로 Extract task에 있는 주소값이 airflow서버의 디비에 저장됨
# 이를 Variable을 이용해 주소값을 가져옴
# 보안상의 이유로 디비 접속에 필요한 민감한 정보들이 airflow서버의 디비에 저장
# 이를 PostgresHook을 사용해 접속에 필요한 정보를 가져옴
# 해당 정보는 에어플로우 웹서버의 디비상에 저장된거고 저장된 곳을 볼려면 admin에 connections에 저장됨

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2

def get_Redshift_connection(autocommit=False):
    # 접속 정보를 가져오고
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    # 접속 정보를 이용해 연결
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract(**context):
    link = context["params"]["url"]
    task_instance = context["task_instance"]
    execution_date = context["execution_date"]
    logging.info(execution_date)
    f = requests.get(link)
    return (f.text)

def transform(**context):
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = text.split("\n")[1:]
    return lines

def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    sql = f"BEGIN; DELETE FROM {schema}.{table};"
    for line in lines:
        if line != '':
            (name, gender) = line.split(",")
            sql += f"INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');"
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)

dag_second_assignment = DAG(
    dag_id = 'name_gender_v4',
    start_date = datetime(2023,4,6),
    schedule = '0 2 * * *',
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries':1,
        'retry_delay': timedelta(minutes=3),
    })

extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'url' : Variable.get("csv_url")
    },
    dag = dag_second_assignment)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = {
    },
    dag = dag_second_assignment)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema' : 'jinwoo',
        'table' : 'name_gender'
    },
    dag = dag_second_assignment)

extract >> transform >> load
