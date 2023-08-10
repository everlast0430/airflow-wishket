from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json

dag = DAG(
    dag_id = 'MySQL_to_Redshift',
    start_date = datetime(2023,7,2),
    schedule = '0 2 * * *',
    max_active_runs = 1,
    catchup = True,
    default_args = {
        'retries':1,
        'retry_delay':timedelta(minutes=2),
    }
)

schema = "wishket"
table = "project"
s3_bucket = "s3jinwoo"
s3_key = schema + table + "_upsert"
rs_schema = "jinwoo"
rs_table = "project_upsert"

mysql_to_s3 = SqlToS3Operator(
    task_id = 'mysql_to_s3',
    # 진자 템플릿을 활용하여 airflow 시스템 변수값 읽어오.
    # 에어플로우가 이 대그를 실행할때 지da
    query = "SELECT * FROM wishket.project WHERE DATE_FORMAT(DATE(date_start_recruitment), '%Y-%m-%d') = DATE_FORMAT(DATE('{{ execution_date }}'), '%Y-%m-%d')",
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    sql_conn_id = "mysql_conn_id",
    aws_conn_id = "aws_conn_id",
    verify = False,
    replace = True,
    pd_kwargs={"index": False, "header": False},
    dag = dag
)

s3_to_redshift = S3ToRedshiftOperator(
    task_id = 's3_to_redshift',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = rs_schema,
    table = rs_table,
    copy_options=['csv'],
    method = 'UPSERT',
    upsert_keys = ["title", "date_start_recruitment"],
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_conn_id",
    dag = dag
)

mysql_to_s3 >> s3_to_redshift
