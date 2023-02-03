from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_query = " COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' FORMAT AS json '{}';"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table_name = "",
                 s3_bucket="",
                 s3_key = "",
                 file_format = "",
                 log_json_file = "",
                 *args, **kwargs):
        
        """
        Collect variables needed for COPY 
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.log_json_file = log_json_file
        self.execution_date = kwargs.get('execution_date')    
            
    def execute(self, context):
        """
        Excuting COPY from S3 to Redshift
        """
        
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        
        # deleting data on Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Deleting data from Redshift staging table")
        redshift_hook.run("DELETE FROM {}".format(self.table_name))   
        
        # Loading from S3
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"Selecting S3-file {s3_path}")
        
        if self.log_json_file != "":
            self.log_json_file = "s3://{}/{}".format(self.s3_bucket, self.log_json_file)
            copy_query = self.copy_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, self.log_json_file)
        else:
            copy_query = self.copy_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, 'auto')
        
        # Copying to redshift
        self.log.info(f"Excuting COPY from S3 to Redshift")
        redshift_hook.run(copy_query)
        self.log.info(f"COPY to Redshift table {self.table_name} successful")