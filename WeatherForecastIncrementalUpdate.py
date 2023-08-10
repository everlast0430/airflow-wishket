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
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract(**context):
    key = context["params"]["key"]
    url = context["params"]["url"]
    lat = context["params"]["lat"]
    lon = context["params"]["lon"]
    url = url.format(lat, lon, key)
    f = requests.get(url)

    return f.json()

def transform(**context):
    weather_info = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")

    days = []
    temp_info = []
    for info in weather_info["daily"]:
        days.append(datetime.fromtimestamp(info["dt"]).strftime('%Y-%m-%d'))
        temp_info.append(info["temp"])

    return days, temp_info

def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    weather_info = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    days = weather_info[0]
    temp_info = weather_info[1]
    cur = get_Redshift_connection()

    # 임시 테이블 생성
    create_sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};
    CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS);INSERT INTO {schema}.temp_{table} SELECT * FROM {schema}.{table};"""
    logging.info(create_sql)

    try:
        cur.execute(create_sql)
        cur.execute("END;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 임시 테이블에 데이터 입력
    insert_sql = ''
    try:
        for day, temp in zip(days,temp_info):
            now = datetime.now().strftime('%Y-%m-%d')
            insert_sql += f"INSERT INTO {schema}.temp_{table} VALUES ('{day}', '{temp['day']}', '{temp['min']}', '{temp['max']}', '{now}');"
            insert_sql += "END;"
            cur.execute(insert_sql)
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 기존 테이블 대체
    alter_sql = f"""DELETE FROM {schema}.{table};
        INSERT INTO {schema}.{table}
        SELECT date, temp, min_temp, max_temp, created_date FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
            FROM {schema}.temp_{table}
    )
    WHERE seq=1;"""

    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    
dag_get_weather_info = DAG(
    dag_id = 'get_weather_info_incremental',
    start_date = datetime(2023,6,30),
    schedule = '0 2 * * *',
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries':1,
        'retry_delay':timedelta(minutes=3),
    })


extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'url' : Variable.get("open_weather_api_url"),
        'key' : Variable.get("open_weather_api_key"),
        'lat' : 37.3387,
        'lon' : 121.8853
    },
    dag = dag_get_weather_info)


transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = {
    },
    dag = dag_get_weather_info)


load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema' : 'jinwoo',
        'table' : 'weather_forecast_incremental'
    },
    dag = dag_get_weather_info)

extract >> transform >> load
