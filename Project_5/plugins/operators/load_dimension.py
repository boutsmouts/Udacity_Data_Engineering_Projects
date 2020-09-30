from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''
CLASS:      LoadDimensionOperator
PURPOSE:    Airflow operator to establish Postgres hook,
            clear desired dimension table,
            and run a specific parsed SQL query to load the dimension table

ARGUMENTS:  redshift_conn_id    (Connection ID for Postgres hook)
            table               (Name of dimension table)
            sql_query           (Desired SQL query to load dimension table)
            overwrite           (Boolean to overwrite or append to any present dimension table - default is append)
'''

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql_query = '',
                 overwrite = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.overwrite = overwrite

    def execute(self, context):
        self.log.info('Establishing Redshift hook.')
        postgres_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Redshift hook successfully established!')

        if self.overwrite:
            self.log.info(f'Clearing data from dimension table {self.table}.')
            postgres_hook.run('DELETE FROM {}'.format(self.table))
            self.log.info(f'Dimension table {self.table} successfully cleared!')

        self.log.info(f'Running SQL query {self.sql_query} to load dimension tables.')
        postgres_hook.run(self.sql_query)
        self.log.info(f'SQL query {self.sql_query} to load dimension tables successfully run.')
