from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import timedelta

from airflow.exceptions import AirflowException

import requests
import logging
import psycopg2

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

def execSQL(**context):

    schema = context['params']['schema']
    table = context['params']['table']
    select_sql = context['params']['sql']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = get_Redshift_connection()

    try:
        sql =  f"""DROP TABLE IF EXISTS {schema}.{table}; CREATE TABLE {schema}.{table} AS"""
        sql += select_sql
        sql += "COMMIT;"
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")

dag = DAG(
    dag_id = "Build_summary",
    start_date = datetime(2023,7,7),
    schedule = '@once',
    catchup = False
)

execsql = PythonOperator(
    task_id = 'execsql',
    python_callable = execSQL,
    params = {
        'schema' : 'jinwoo',
        'table': 'project_day_summit_summary',
        'sql' :
        """
        (SELECT
            count(pp.date_start_recruitment) AS project_count,
            TO_CHAR(pp.date_start_recruitment AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS date
        FROM
            project pp
        WHERE
            pp.date_start_recruitment IS NOT NULL
        GROUP BY
            TO_CHAR(pp.date_start_recruitment AT TIME ZONE 'UTC', 'YYYY-MM-DD'));"""
    },
    dag = dag
)
