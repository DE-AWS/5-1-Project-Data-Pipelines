from datetime import datetime, timedelta
import pendulum
import os
from airflow import conf
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator, DataQualityOperator)
from airflow.helpers import SqlQueries

default_args = {
    'owner': 'patri-carrasco',
    'start_date': pendulum.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}
f = open(os.path.join(conf.get('core','dags_folder'),'create_tables.sql'))
create_tables_sql = f.read()

create_table = PostgresOperator(
    task_id="create_trips_table",
    postgres_conn_id="redshift",
    sql=create_tables_sql
)

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_credentiasl_id='aws_credentials',
        s3_bucket='data-pipelines-398321749864',
        s3_key='log-data',
        log_json_path='log_json_path'

    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_credentiasl_id='aws_credentials',
        s3_bucket='data-pipelines-398321749864',
        s3_key='song-data',
        log_json_path='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        redshift_conn_id="redshift",
        sql=SqlQueries.songplay_table_insert,
        append_data=False
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

final_project_dag = final_project()