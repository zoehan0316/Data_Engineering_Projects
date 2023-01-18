from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
Data quality operator is used to check on the final data in Amazon Redshift
"""

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info(f'check data quality on {table} table')
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'data quality check failed: table {table} has no result')
            
            num_records = records[0][0]
            if num_records == 1:
                raise ValueError(f'data quality check failed: table {table} has 0 rows')
            
            self.log.info(f'data quality check on table {table}, passed with {num_records} records')