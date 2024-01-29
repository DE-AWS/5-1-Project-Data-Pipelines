from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info('DataQualityOperator ')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table, field in self.table:

            self.log.info(f"hola {table}, {field}")

            # Quality check 1 - check that dimension tables have rows
            custom_sql = f"SELECT COUNT(*) FROM {table}"
            rows = redshift.get_first(custom_sql)
            self.log.info(f'Table: {table} has {rows} rows')

            # Quality check 2 - check that key fields dont have null entries
            custom_sql = f"SELECT COUNT(*) FROM {table} WHERE {field} IS NULL"
            rows = redshift.get_first(custom_sql)
            self.log.info(f'Field: {field} in table: {table} has {rows} NULL rows')