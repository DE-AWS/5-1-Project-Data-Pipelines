from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            TIMEFORMAT AS '{}'
            REGION '{}'
            FORMAT AS JSON '{}'
            
        """
    # copy_sql = """
    #           COPY {}
    #           FROM '{}'
    #           ACCESS_KEY_ID '{}'
    #           SECRET_ACCESS_KEY '{}'
    #           IGNOREHEADER {}
    #           DELIMITER '{}'
    #       """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",

                 log_json_path="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_path = log_json_path
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info('Gettting AWS Credentials from the Airflow WebUI AWS Hook')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from destination Redshift table"')
        redshift.run("TRUNCATE TABLE {}".format(self.table))

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(" path s3://{}/{}".format(self.s3_bucket, rendered_key))

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            "auto",
            "us-west-2",
            self.log_json_path,
            self.ignore_headers

        )

        #self.log_json_path = "s3://{}/{}".format(self.s3_bucket, self.log_json_path)

        #self.log.info("Log json path  {}".format(self.log_json_path))

        # if self.log_json_path != '':
        #     # self.log_json_path = "{}".format(self.log_json_path)
        #     # self.log.info("Log json path dentro  {}".format(self.log_json_path))
        #     formatted_sql = StageToRedshiftOperator.copy_sql.format(
        #         self.table,
        #         s3_path,
        #         credentials.access_key,
        #         credentials.secret_key,
        #         self.log_json_path
        #     )
        #     self.log.info("entra en el formatted_sql"),
        # else:
        #     formatted_sql = StageToRedshiftOperator.copy_sql.format(
        #         self.table,
        #         s3_path,
        #         credentials.access_key,
        #         credentials.secret_key,
        #         'auto'
        #     )


        self.log.info(formatted_sql)
        redshift.run(formatted_sql)
        self.log.info('Formatting SQL running completed')




