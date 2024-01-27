from datetime import datetime, timedelta
import pendulum
import os

from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


# from common_operator import stage_redshift
default_args = {
    'owner': 'patri-carrasco',
    'start_date': pendulum.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}
# f = open(os.path.join(conf.get('core','dags_folder'),'create_tables.sql'))
# create_tables_sql = f.read()
#
# create_table = PostgresOperator(
#     task_id="create_trips_table",
#     postgres_conn_id="redshift",
#     sql=create_tables_sql
# )

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@monthly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    json_path = 's3://udacity-dend/log_json_path.json'
    extra_params = "FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket="udacity-dend",
        s3_key="log_data",
        log_json_path='s3://udacity-dend/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song_data/A/A/A/',
        #json_path=''
    )

    # load_songplays_table = LoadFactOperator(
    #     task_id='Load_songplays_fact_table',
    #     table='songplays',
    #     redshift_conn_id="redshift",
    #     sql=SqlQueries.songplay_table_insert,
    #     append_data=False
    # )
    #
    # load_user_dimension_table = LoadDimensionOperator(
    #     task_id='Load_user_dim_table',
    #     redshift_conn_id='redshift',
    #     table='users',
    #     sql=SqlQueries.user_table_insert,
    #     append_data=False
    # )
    #
    # load_song_dimension_table = LoadDimensionOperator(
    #     task_id='Load_song_dim_table',
    #     redshift_conn_id='redshift',
    #     table='songs',
    #     sql=SqlQueries.song_table_insert,
    #     append_data=False
    # )
    #
    # load_artist_dimension_table = LoadDimensionOperator(
    #     task_id='Load_artist_dim_table',
    #     redshift_conn_id='redshift',
    #     table='artist',
    #     sql=SqlQueries.artist_table_insert,
    #     append_data=False
    # )
    #
    # load_time_dimension_table = LoadDimensionOperator(
    #     task_id='Load_time_dim_table',
    #     redshift_conn_id='redshift',
    #     table='time',
    #     sql=SqlQueries.time_table_insert
    #
    # )
    #
    # run_quality_checks = DataQualityOperator(
    #     task_id='Run_data_quality_checks',
    # )
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

final_project_dag = final_project()