from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_query = "",
                 table_name = "",
                 *args, **kwargs):
        
        """
        Collect variables needed for dimension table creation
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_query = sql_query

    def execute(self, context):
        
        """
        INSERT from fact table into dimension tables
        """
        
        self.log.info(f'Starting INSERT from fact to dimension table {self.table_name}')
                
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run("INSERT INTO {} {}".format(self.table_name, self.sql_query))
        
        self.log.info('INSERT successful')