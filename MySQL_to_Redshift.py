from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import datetime
from datetime import timedelta

dag = DAG(
    dag_id = 'MySQL_to_Redshift',
    start_date = datetime(2023,7,2),
    schedule = '0 2 * * *',
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries':1,
        'retry_delay':timedelta(minutes=2),
    }
)

schema = "wishket"
table = "project"
s3_bucket = "s3jinwoo"
s3_key = schema + table
rs_schema = "jinwoo"
rs_table = "project"

mysql_to_s3 = SqlToS3Operator(
    task_id = 'mysql_to_s3',
    query = "SELECT * FROM wishket.project",
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
    schema = schema,
    table = table,
    copy_options=['csv'],
    method = 'REPLACE',
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_conn_id",
    dag = dag
)

mysql_to_s3 >> s3_to_redshift
