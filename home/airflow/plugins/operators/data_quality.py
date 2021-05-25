from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        record = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.tables}")
        if len(record) < 1 or len(record[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.tables} returned no result")
        num_records = record[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.tables} contained 0 rows")
        logging.info(f"Data quality on table {self.tables} check passed with {record[0][0]} records")

# self.log.info('DataQualityOperator not implemented yet')