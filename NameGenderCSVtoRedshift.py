from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import logging
import psycopg2

def get_Redshift_connection():
    host = "learnde.c3t7em1tgxwk.ap-northeast-2.redshift.amazonaws.com"
    user = "jinwoo"
    password = "Wlsdn5405!"
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
    cur.execute(sql)
    logging.info(sql)
    logging.info("Load done")

def etl():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)
    lines = transform(data)
    load(lines)

dag_second_assignment = DAG(
    dag_id = 'name_gender',
    catchup = False,
    start_date = datetime(2023,6,29),
    schedule = '0 2 * * *'
    )

task = PythonOperator(
    task_id = 'perform_etl',
    python_callable = etl,
    dag = dag_second_assignment
    )
