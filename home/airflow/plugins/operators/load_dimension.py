from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 delete_table=False,
                 table_name="",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.delete_table = delete_table
        self.table_name = table_name

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.delete_table:
            self.log.info(f"Run delete statement on table {self.table_name}")
            redshift_hook.run(f"DELETE FROM {self.table_name}")

        self.log.info(f"Query is running to load dimensions {self.table_name}")
        redshift_hook.run(self.sql_query)
        self.log.info(f"Dimension table: {self.table_name} loaded.")
        # self.log.info('LoadDimensionOperator not implemented yet')
