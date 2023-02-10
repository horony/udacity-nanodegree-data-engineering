from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 dict_tests,
                 *args, **kwargs):
        
        """
        Collect variables needed for quality check
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dict_tests = dict_tests

    def execute(self, context):
        
        """
        Perform quality check for each fact and dimension table
        """
        
        # Establish Redshift connection
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # Iterating tables and performing quality checks
        
        for elem in self.dict_tests:
            
            # Test 1: Checking if table exists
            
            table = elem['table']
            
            self.log.info(f"Checking if table {table} exists in Redshift")
            num_tables = redshift_hook.get_records(f"SELECT count(*) FROM pg_tables WHERE pg_tables.schemaname = 'public' AND pg_tables.tablename = '{table}'")

            if num_tables[0][0] == 1:
                self.log.info(f"Check passed: Table {table} exists")
            else:
                raise ValueError(f"Check failed: Table {table} does not exist")             
            
            # Test 2: Checking number of records
            
            self.log.info(f"Checking number of records in table {table}")
            
            num_rows = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            min_rows_allowed = elem['min_rows']

            if num_rows[0][0] >= min_rows_allowed:
                self.log.info(f"Check passed: Table {table} passed record check ({num_rows[0][0]} records)")
            else:
                raise ValueError(f"Check failed: Table {table} failed record check ({num_rows[0][0]} records)")