from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

start_date = datetime(2018, 11, 1)
end_date = datetime(2018, 11, 30)
s3_bucket = "sean-mudrock1"
events_s3_key = "log_data"
songs_s3_key = "song-data/A/A/"
log_json_file = 'log_json_path.json'

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'email_on_retry': False
}

#Get SQL Directory
sql_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),'create_tables.sql')
#Open a sql file to create table
with open(sql_dir,'r')as f:
    CreateTable = f.read()

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    '''create_tables = PostgresOperator(
        task_id='Create_tables',
        postgres_conn_id="redshift",
        sql=CreateTable
    )

    create_tables = RedshiftSQLOperator(
        task_id='Create_tables',
        redshift_conn_id="redshift",
        sql=CreateTable
    )'''

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_default",
        s3_bucket=s3_bucket,
        s3_key=events_s3_key,
        log_json_file=log_json_file
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_default",
        s3_bucket=s3_bucket,
        s3_key=songs_s3_key
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table="songplays",
        redshift_conn_id="redshift",
        sql_query=SqlQueries.songplay_table_insert,
        append_only = True
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table="users",
        redshift_conn_id="redshift",
        sql_query=SqlQueries.user_table_insert

    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table="songs",
        redshift_conn_id="redshift",
        sql_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table="artists",
        redshift_conn_id="redshift",
        sql_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table="time",
        redshift_conn_id="redshift",
        sql_query=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = "redshift",
        tables = ["songplays", "users", "songs", "artists", "time"]
    )
    end_operator = DummyOperator(task_id='End_execution')
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >>  end_operator
final_project_dag = final_project()