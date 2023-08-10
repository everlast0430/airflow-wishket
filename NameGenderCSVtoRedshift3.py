# ETL의 Extract Transform Load에 해당하는 함수를 각각 만들고 이 함수들을 각각의 task로 정의
# 해당 task들의 작업순서를 지정해줌
# 보안상의 이유로 Extract함수에 있는 주소값이 airflow서버의 디비에 저장됨
# Extract task의 실행결과로 얻어진 task 정보를 Transform task에서 사용하기위해 xcom_pull을 활용함
# 변동이 될 여지가 있는 변수들이 함수들에 있는 경우.. 이 경우에선 DW 스키마 및 테이블에 접근하는 변수를
# task에서 저장되어서 함수로 전달될 수 있도록 context를 load함수에서 활용하는 중.
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2

def get_Redshift_connection():
    host = "learnde.c3t7em1tgxwk.ap-northeast-2.redshift.amazonaws.com"
    user = "jinwoo"
    password = "..."
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={user} password={password} host={host} port={port}")
    conn.set_session(autocommit=True)
    return conn.cursor()

def extract(**context):
    link = context["params"]["url"]
    task_instance = context["task_instance"]
    excution_date = context["excution_date"]
    logging.info(excution_date)
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
    dag_id = 'name_gender_v3',
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
        'url' : "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
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
