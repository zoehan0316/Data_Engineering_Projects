from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
the load fact operator used to collect the data from staging tables and inserts into the dimension tables
-sql: insert data sql statement
"""

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        self.log.info('loading data into the {} table'.format(self.table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info(f'truncate {self.table} table')
            redshift.run(f'TRUNCATE {self.table}')
            
        self.log.info(f'insert into {self.table}')    
        insert_sql = f'INSERT INTO {self.table} \n{self.sql}'
        redshift.run(insert_sql)
