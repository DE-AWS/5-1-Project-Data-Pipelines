from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
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


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='stage_events',
        redshift_conn_id='redshift',
        aws_credentiasl_id='aws_credentials',
        s3_bucket='data-pipelines-398321749864',
        s3_key='log-data',
        log_json_path='log_json_path'

    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='stage_songs',
        redshift_conn_id='redshift',
        aws_credentiasl_id='aws_credentials',
        s3_bucket='data-pipelines-398321749864',
        s3_key='log-data',
        log_json_path='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
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