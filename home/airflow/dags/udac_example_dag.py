from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from airflow.operators.subdag_operator import SubDagOperator
from helpers import SqlQueries
from dimensions_subdag import dimensions_table_dag 


#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket="udacity-dend-warehouse"
songs_s3_key = "song_data"
log_s3_key = "log_data"
log_json_file = "log_json_path.json"

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}
dag_name = 'udac_example_dag'
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    table_name="staging_events",
    s3_bucket=s3_bucket,
    s3_key=log_s3_key,
    file_format="JSON",
    log_json_file = log_json_file,
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    table_name="staging_songs",
    s3_bucket=s3_bucket,
    s3_key=songs_s3_key,
    file_format="JSON",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    dag=dag,
    provide_context=True
 )

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = SubDagOperator(
    subdag=dimensions_table_dag(
        parent_dag_name=dag_name,
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        aws_credentials_id="aws_credentials",
        table = "users",
        sql=SqlQueries.user_table_insert,
        s3_bucket=s3_bucket,
        s3_key=log_s3_key,
    ),
    task_id="Load_user_dim_table",
    dag=dag,
)

load_song_dimension_table = SubDagOperator(
    subdag=dimensions_table_dag(
        parent_dag_name=dag_name,
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        aws_credentials_id="aws_credentials",
        table = "songs",
        sql=SqlQueries.song_table_insert,
        s3_bucket=s3_bucket,
        s3_key=songs_s3_key,
    ),
    task_id="Load_song_dim_table",
    dag=dag,
)


load_artist_dimension_table = SubDagOperator(
    subdag=dimensions_table_dag(
        parent_dag_name=dag_name,
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        aws_credentials_id="aws_credentials",
        table = "artists",
        sql=SqlQueries.artist_table_insert,
        s3_bucket=s3_bucket,
        s3_key=songs_s3_key,
    ),
    task_id="Load_artist_dim_table",
    dag=dag,
)



load_time_dimension_table = SubDagOperator(
    subdag=dimensions_table_dag(
        parent_dag_name=dag_name,
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        aws_credentials_id="aws_credentials",
        table = "time",
        sql=SqlQueries.time_table_insert,
        s3_bucket=s3_bucket,
        s3_key=log_s3_key,
    ),
    task_id="Load_time_dim_table",
    dag=dag,
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["artists","songplays","songs","time","users"]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]


stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> [load_song_dimension_table, load_user_dimension_table,  load_artist_dimension_table, load_time_dimension_table]

[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks


run_quality_checks >> end_operator

