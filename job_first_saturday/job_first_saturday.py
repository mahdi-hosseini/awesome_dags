import calendar
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import SqlServerOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from helpers import Notifications

logger = LoggingMixin().log


def filter_date_stamp(*args, **kwargs):
    return (
        datetime.strptime(kwargs.get("ds"), "%Y-%m-%d").weekday()
        == calendar.SATURDAY
    )


with DAG(
    dag_id="job_first_saturday",
    description="Job that runs every first Saturday of the month",
    schedule_interval="0 0 * * 6",
    start_date=datetime.now(),
    end_date=None,
    default_args={"owner": "John Smith"},
    max_active_runs=1,
    catchup=False,
) as dag:
    with open("README.md") as readme:
        dag.doc_md = readme.read()

    check_if_first_saturday_of_month = ShortCircuitOperator(
        task_id="check_if_first_saturday_of_month",
        python_callable=filter_date_stamp,
        provide_context=True,
    )

    execute_stored_procedure = SqlServerOperator(
        task_id="execute_stored_procedure",
        conn_id="SERVER_CON_NAME",
        database="DBNAME",
        sql="EXEC dbo.some_stored_procedure",
    )

check_if_first_saturday_of_month >> execute_stored_procedure
