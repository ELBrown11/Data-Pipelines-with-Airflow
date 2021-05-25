import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from operators.stage_redshift import StageToRedshiftOperator
import sql
from helpers import SqlQueries


def dimensions_table_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        sql,
        s3_bucket,
        s3_key,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    create_table = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=sql
    )


    copy_task = StageToRedshiftOperator(
        task_id=f"load_{table}_from_s3_to_redshift",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key
    )

    create_table >> copy_task

    return dag