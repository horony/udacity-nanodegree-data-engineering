from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 list_tables,
                 list_pkeys,
                 *args, **kwargs):
        
        """
        Collect variables needed for quality check
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.list_tables = list_tables
        self.list_pkeys = list_pkeys


    def execute(self, context):
        
        """
        Perform quality check for each fact and dimension table
        """
        
        # Establish Redshift connection
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # Iterating tables and performing quality checks
        i = 0
        for table in self.list_tables:
            
            # Checking number of records
            self.log.info(f"Checking number of records in table {table}")
            num_rows = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if num_rows[0][0] >= 1:
                self.log.info(f"Table {table} passed record check ({num_rows[0][0]} records)")
            else:
                raise ValueError(f"Table {table} failed record check")
            
            # Checking if pkey is unique
            key_to_check = self.list_pkeys[i]
            self.log.info(f"Checking if pkey {key_to_check} in table {table} is unique")
            
            
            key_difference = redshift_hook.get_records(f"SELECT COUNT({key_to_check}) - COUNT(DISTINCT {key_to_check}) FROM {table}")
            
            if key_difference[0][0] != 0:
                self.log.info(f"Table {table} passed pkey check")
            else:
                raise ValueError(f"Table {table} failed pkey check")
            
            i = i+1