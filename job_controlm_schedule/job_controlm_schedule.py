import collections
from ast import literal_eval
from datetime import datetime, timedelta
from io import BytesIO
from itertools import chain

import pandas as pd
import requests
import urllib3
import xmltodict

from airflow import DAG
from airflow.hooks import SqlServerHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from helpers import Notifications
from sqlalchemy.types import NVARCHAR

logger = LoggingMixin().log


class ScheduleKeys:
    months_mapping = {
        "@JAN": 1,
        "@FEB": 2,
        "@MAR": 3,
        "@APR": 4,
        "@MAY": 5,
        "@JUN": 6,
        "@JUL": 7,
        "@AUG": 8,
        "@SEP": 9,
        "@OCT": 10,
        "@NOV": 11,
        "@DEC": 12,
    }

    main_keys = [
        "@JOBNAME",
        "@DESCRIPTION",
        "@CMDLINE",
        "@RUN_AS",
        "@INTERVAL",
        "@TIMEZONE",
        "@NODEID",
        "@TASKTYPE",
        "@WEEKDAYS",
    ]

    parsed_keys = [
        "@TIMEFROM",
        "@TIMETO",
        "@CREATION_DATE",
        "@CREATION_TIME",
        "@CHANGE_DATE",
        "@CHANGE_TIME",
        "@ACTIVE_FROM",
        "@ACTIVE_TILL",
    ]

    extra_keys = [
        "@ADJUST_COND",
        "@APPLICATION",
        "@APPL_TYPE",
        "@AUTOARCH",
        "@CHANGE_USERID",
        "@CONFIRM",
        "@CREATED_BY",
        "@CREATION_USER",
        "@CRITICAL",
        "@CYCLIC",
        "@CYCLIC_TOLERANCE",
        "@CYCLIC_TYPE",
        "@DAYS",
        "@DAYSCAL",
        "@DAYS_AND_OR",
        "@DOCLIB",
        "@DOCMEM",
        "@IND_CYCLIC",
        "@JOBISN",
        "@MAXDAYS",
        "@MAXRERUN",
        "@MAXRUNS",
        "@MAXWAIT",
        "@MEMNAME",
        "@MULTY_AGENT",
        "@PARENT_FOLDER",
        "@PRIORITY",
        "@RETRO",
        "@RULE_BASED_CALENDAR_RELATIONSHIP",
        "@SHIFT",
        "@SHIFTNUM",
        "@SUB_APPLICATION",
        "@SYSDB",
        "@USE_INSTREAM_JCL",
    ]


def parse(datetime_string, fmt):
    if not datetime_string:
        return datetime_string
    input_fmt, output_fmt = fmt
    return datetime.strptime(datetime_string, input_fmt).strftime(output_fmt)


def append_to_list(input_list, job, key, children_keys):
    if key in job and len(job[key]):
        job_name = job.get("@JOBNAME", None)
        cur = job[key]
        if isinstance(cur, collections.OrderedDict):
            input_list.append(
                (
                    job_name,
                    key,
                    *(cur.get(child_key, None) for child_key in children_keys),
                )
            )
        elif isinstance(cur, list):
            for item in cur:
                input_list.append(
                    (
                        job_name,
                        key,
                        *(
                            item.get(child_key, None)
                            for child_key in children_keys
                        ),
                    )
                )
        else:
            raise Exception(
                f"Unknown type detected for {key}: {type(job[key])}"
            )


def create_dataframes(job_table):

    schedule, conditions, shouts = [], [], []

    for job in job_table:
        months = [
            ScheduleKeys.months_mapping[key]
            for key in job.keys()
            if key in ScheduleKeys.months_mapping.keys()
        ]

        schedule.append(
            (
                *(job.get(key, None) for key in ScheduleKeys.main_keys),
                ",".join(str(e) for e in sorted(months)),
                parse(job.get("@TIMEFROM", None), ("%H%M", "%H:%M")),
                parse(job.get("@TIMETO", None), ("%H%M", "%H:%M")),
                parse(job.get("@CREATION_DATE", None), ("%Y%m%d", "%Y-%m-%d")),
                parse(job.get("@CREATION_TIME", None), ("%H%M%S", "%H:%M:%S")),
                parse(job.get("@CHANGE_DATE", None), ("%Y%m%d", "%Y-%m-%d")),
                parse(job.get("@CHANGE_TIME", None), ("%H%M%S", "%H:%M:%S")),
                parse(job.get("@ACTIVE_FROM", None), ("%Y%m%d", "%Y-%m-%d")),
                parse(job.get("@ACTIVE_TILL", None), ("%Y%m%d", "%Y-%m-%d")),
                *(job.get(key, None) for key in ScheduleKeys.extra_keys),
            )
        )

        for key in ("INCOND", "OUTCOND"):
            append_to_list(conditions, job, key, ("@NAME", "@AND_OR"))

        append_to_list(
            shouts,
            job,
            "SHOUT",
            ("@DEST", "@MESSAGE", "@TIME", "@URGENCY", "@WHEN"),
        )

    schedule_df = pd.DataFrame(
        schedule,
        columns=[
            key.replace("@", "").lower()
            for key in chain(
                ScheduleKeys.main_keys,
                ["months"],
                ScheduleKeys.parsed_keys,
                ScheduleKeys.extra_keys,
            )
        ],
    )

    conditions_df = pd.DataFrame(
        conditions, columns=("jobname", "type", "condition", "and_or")
    )

    shouts_df = pd.DataFrame(
        shouts,
        columns=(
            "jobname",
            "type",
            "destination",
            "message",
            "time",
            "urgency",
            "when",
        ),
    )

    return (schedule_df, conditions_df, shouts_df)


def get_jobs(**kwargs):
    conn = BaseHook.get_connection("CONTROL_M_APICREDS")
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    response = requests.post(
        f"{conn.host}/session/login",
        json={"username": conn.login, "password": conn.password},
        verify=False,
    )
    logger.info(f"logging in to {conn.host}")

    with requests.Session() as session:
        session.headers["Authorization"] = f'Bearer {response.json()["token"]}'
        session.verify = False

        response = session.get(
            f"{conn.host}/deploy/jobs?ctm=*&folder=BIC*&format=XML", stream=True
        )

        logger.info("retrieving schedule from /deploy/jobs endpoint")

        with BytesIO() as bio:
            for chunk in response.iter_content(1024):
                bio.write(chunk)
            bio.seek(0)
            return literal_eval(bio.read().decode("utf-8"))

    raise ValueError("No schedule was received")


def load_schedule(**kwargs):
    sqlserver_hook = SqlServerHook(conn_id="DATAWAREHOUSE", schema="METADATADB")

    control_m_jobs_xml = get_jobs(**kwargs)
    parsed_xml = xmltodict.parse(control_m_jobs_xml)
    schedule, conditions, shouts = create_dataframes(
        parsed_xml["DEFTABLE"]["FOLDER"]["JOB"]
    )

    schedule.to_sql(
        name="CONTROLM_SCHEDULE",
        con=sqlserver_hook.get_sqlalchemy_engine(),
        index=False,
        schema="dbo",
        if_exists="replace",
        dtype={str(col_name): NVARCHAR(None) for col_name in schedule},
    )

    conditions.to_sql(
        name="CONTROLM_CONDITIONS",
        con=sqlserver_hook.get_sqlalchemy_engine(),
        index=False,
        schema="dbo",
        if_exists="replace",
        dtype={str(col_name): NVARCHAR(None) for col_name in conditions},
    )

    shouts.to_sql(
        name="CONTROLM_SHOUTS",
        con=sqlserver_hook.get_sqlalchemy_engine(),
        index=False,
        schema="dbo",
        if_exists="replace",
        dtype={str(col_name): NVARCHAR(None) for col_name in shouts},
    )


with DAG(
    dag_id="job_controlm_schedule",
    description="Control-M schedule data sync",
    schedule_interval="@weekly",
    start_date=datetime(year=2019, month=7, day=25),
    end_date=None,
    default_args={
        "owner": "Mahdi Hosseini",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    catchup=False,
    on_failure_callback=lambda context: Notifications.create_jira_ticket(
        context
    ),
) as dag:
    with open("dags/job_controlm_schedule/README.md") as readme:
        dag.doc_md = readme.read()

    load_schedule_to_sql = PythonOperator(
        task_id="load_schedule_to_sql",
        python_callable=load_schedule,
        provide_context=True,
    )
