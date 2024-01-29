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

            # Quality check 1 - check that dimension tables have rows
            custom_sql = f"SELECT COUNT(*) FROM {table}"
            rows = redshift.get_first(custom_sql)
            self.log.info(f'Table: {table} has {rows[0]} rows')

            if len(rows) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")

            # Quality check 2 - check that key fields dont have null entries
            custom_sql = f"SELECT COUNT(*) FROM {table} WHERE {field} IS NULL"
            rows_null = redshift.get_first(custom_sql)
            self.log.info(f'Field: {field} in table: {table} has {rows_null[0]} NULL rows')

            if rows_null[0] > 0:
                raise ValueError(f"Data quality check failed. {table} has NULL")