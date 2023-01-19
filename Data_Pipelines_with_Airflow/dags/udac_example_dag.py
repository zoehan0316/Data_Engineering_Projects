from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,
                               LoadFactOperator,
                               LoadDimensionOperator,
                               DataQualityOperator,
                               PostgresOperator)
from helpers import SqlQueries

"""
The following airflow DAG is:
1. create tables
2. inserts fact and dimension tables from s3 bucket into Amazon redshift DB
3. finally running the checks on the data to verify

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
"""

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'email_on_failure': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json='s3://udacity-dend/log_json_path.json',
    table='staging_events',
    region='us-west-2',
)
    


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json='auto',
    table='staging_songs',
    region='us-west-2',
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.songplay_table_insert,
    table='songplays',
    truncate=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.user_table_insert,
    table='users',
    truncate=True    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.song_table_insert,
    table='songs',
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.artist_table_insert,
    table='artists',
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.time_table_insert,
    table='time',
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    queries=[{'sql': "SELECT COUNT(*) FROM users WHERE userid is NULL", 'expect': 0},
             {'sql': "SELECT COUNT(*) FROM songs WHERE songid is NULL", 'expect': 0},
             {'sql': "SELECT COUNT(*) FROM artists WHERE artistid is NULL", 'expect': 0}]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator \
    >> create_tables_task \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
    >> run_quality_checks \
    >> end_operator