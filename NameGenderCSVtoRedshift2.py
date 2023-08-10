# 실제 동작하는 task(PythonOperator)에서 url(혹은 다른 파마미터)을 넘겨줄 수 있는 방식으로 개선
# task가 실행될 때, task 정보가 생성되는데 이를 활용하는 context인자를 활용하는 부분을 추가
# DAG가 실패했을 경우 재시도를 어떻게 할 건지 정의
# etl 함수가 많이 개편된 버전
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

def extract(url):
    logging.info("Extract started")
    f = requests.get(url)
    logging.info("Extarct done")
    return (f.text)

def transform(text):
    logging.info("Transform started")
    lines = text.split("\n")[1:]
    logging.info("Transform done")
    return lines

def load(lines):
    logging.info("Load started")
    cur = get_Redshift_connection()
    sql = "BEGIN; DELETE FROM jinwoo.name_gender;"
    for line in lines:
        if line != '':
            (name, gender) = line.split(",")
            sql += f"INSERT INTO jinwoo.name_gender VALUES ('{name}', '{gender}');"
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)
    logging.info("Load done")

def etl(**context):
    link = context["params"]["url"]
    # task(DAG의 task) 자체에 대한 정보를 얻고 싶다면,context['task_instance'] 혹은 context['ti']를 통해 가능
    # task가 생각나지 않았었고.. task자체에 대한 정보가 뭔지 감이 안잡혔었음. 구체적인 예가 없어서 이해하기 힘들었었음
    task_instance = context["task_instance"]
    execution_date = context["excution_date"]

    logging.info(execution_date)

    data = extract(link)
    lines = transform(data)
    load(lines)

dag_second_assignment = DAG(
    dag_id = 'name_gender_v2',
    start_date = datetime(2023,4,6),
    schedule = '0 2 * * *',
    catchup = False,
    max_active_runs = 1,
    # DAG가 실패했을때 3분간 지연하고 재시도를 1번 함
    default_args = {
        'retries':1,
        'retry_delay': timedelta(minutes=3),
    })

task = PythonOperator(
    task_id = 'perform_etl',
    python_callable = etl,
    # etl 함수에 존재하는 파라미터인 context가 사용할 dict
    params = {
        'url' : "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    },
    dag = dag_second_assignment)
