### Documentation

Cron expressions provide no mechanism to run every first Saturday of the month and it is further complicated by the fact that Airflow runs one whole cycle after the start date. This DAG runs every Saturday and by using `ShortCircuitOperator` we check whether it is the first Saturday of the month or not.

One important thing to note is that it is an anti-pattern to use Airflow as a pure scheduler without parameterizing your ETL or stored procedure with mutually exclusive dates, the reason is that if Airflow falls behind or for any reason you decide to clear the past runs of a DAG and re-run it all over again, you would end up with duplicate operations. In other words, use your judgement when employing Airflow as a pure job scheduler.


#### DAG Dependencies

* Airflow SQL Server Connection: `SERVER_CON_NAME`

* SQL Server Permission: `SERVER_CON_NAME.DBNAME`