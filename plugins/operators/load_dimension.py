from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.append_data = append_data
        self.sql = sql

    def execute(self, context):
        self.log.info('LoadDimensionOperator ')
        self.log.info(f" que coño pasa {self.sql}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Loading data')

        self.log.info("Running SQL: {}".format(self.sql))

        if self.append_data:
            sql = 'INSERT INTO %s %s' % (self.table, self.sql)
            redshift.run(sql)
        else:
            sql = 'TRUNCATE TABLE %s;' % (self.table)
            sql = sql + 'INSERT INTO %s %s' % (self.table, self.sql)
            redshift.run(sql)
