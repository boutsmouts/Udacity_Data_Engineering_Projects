from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''
CLASS:      DataQualityOperator
PURPOSE:    Airflow operator to establish Postgres hook
            and run a data quality check for every parsed table

ARGUMENTS:  redshift_conn_id    (Connection ID for Postgres hook)
            table               (List of table names)
'''

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Establishing Redshift hook.')
        postgres_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Redshift hook successfully established!')

        for table in self.tables:
            self.log.info(f'Performing data quality check for {table} table')
            num_records = postgres_hook.get_records(f'SELECT COUNT(*) FROM {table}')

            if num_records is None or len(num_records[0]) < 1 or num_records[0][0] < 1:
                self.log.error(f'Quality check returned an error: No records presents in table {table}')
                raise ValueError(f'Quality check returned an error: No records presents in table {table}')

            self.log.info(f'Data quality check passed for {table} table. Found {num_records[0][0]} records.')
