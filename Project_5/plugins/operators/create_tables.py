from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''
CLASS:      CreateTablesOperator
PURPOSE:    Airflow operator to establish a Postgres hook
            and run the specified SQL queries to create tables on redshift

ARGUMENTS:  redshift_conn_id    (Connection ID for Postgres hook)
'''

class CreateTablesOperator(BaseOperator):

    ui_color = '#604380'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Establishing Redshift hook.')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Redshift hook successfully established!')

        self.log.info('Creating new tables on redshift')
        queries = open('/home/workspace/airflow/plugins/operators/create_tables.sql', 'r').read()

        redshift.run(queries)

        self.log.info('All tables successfully created!')
