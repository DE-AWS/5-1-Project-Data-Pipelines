from airflow.hooks.postgres_hook import PostgresHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 sql='',
                 append_data=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.append_data = append_data
        self.sql = sql
    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Adding data to {}'.format(self.table))

        if not self.append_data:
            redshift.run('DELETE FROM {}'.format(self.table))

        self.log.info("Running sql:{}".format(self.sql))
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql))
        self.log.info("Successfully completed insert into {}".format(self.table))