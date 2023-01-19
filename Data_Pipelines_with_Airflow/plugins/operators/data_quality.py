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
                 queries=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.queries=queries

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        for query in self.queries:
            sql = query.get('sql')
            if sql is None:
                raise ValueError('No Data Specified')
                break
            expect = query.get('expect')
            if expect is None:
                expect = 0
                
            count = redshift.get_first(sql)[0]
            if count != expect:
                raise ValueError(f'check failed: null {count}')
            else:
                self.log.info(f'check passed: null {count}')
                            
            
            