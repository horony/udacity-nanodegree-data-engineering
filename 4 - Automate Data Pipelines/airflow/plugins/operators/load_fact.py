from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id = "",
                 redshift_conn_id = "",
                 sql_query = "",
                 table_name = "",
                 *args, **kwargs):
        
        """
        Collect variables needed for INSERT
        """
            
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name

    def execute(self, context):
        
        """
        Excuting INSERT from stage data to fact table
        """
        
        self.log.info(f'Starting INSERT from stage to fact table {self.table_name}')
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(str(self.sql_query))
        
        self.log.info('INSERT successful')