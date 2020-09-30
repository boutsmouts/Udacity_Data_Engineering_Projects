from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''
CLASS:      StageToRedshiftOperator
PURPOSE:    Airflow operator to establish AWS and Postgres hooks,
            clear given staging table,
            and copy files from S3 bucket into cleared staging table

ARGUMENTS:  redshift_conn_id    (Connection ID for Postgres hook)
            aws_credentials_id  (ID for AWS credentials - must be present in Airflow)
            table               (Name of table)
            s3_bucket           (S3 bucket name)
            s3_key              (S3 key for files)
            region              (Region of AWS service)
            file_format         (File format of source files)
            log_json_key        (S3 key for log mapping file)
'''

class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS {} '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 s3_bucket = '',
                 s3_key = '',
                 region = '',
                 file_format = '',
                 log_json_key = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.log_json_key = log_json_key

    def execute(self, context):
        self.log.info('Establishing AWS hook.')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info('AWS hook successfully established!')

        self.log.info('Establishing Redshift hook.')
        postgres_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Redshift hook successfully established!')

        self.log.info(f'Clearing data from staging table {self.table}.')
        postgres_hook.run('DELETE FROM {}'.format(self.table))
        self.log.info(f'Staging table {self.table} successfully cleared!')

        self.log.info(f'Copying data from S3 bucket {self.s3_bucket} to staging table {self.table}.')
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)

        if self.log_json_key != '':
            rendered_key = self.log_json_key.format(**context)
            log_json_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)

        else:
            log_json_path = 'auto'

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format,
            log_json_path
        )
        postgres_hook.run(formatted_sql)
        self.log.info(f'Data successfully copied from S3 bucket {self.s3_bucket} to staging table {self.table}!')
