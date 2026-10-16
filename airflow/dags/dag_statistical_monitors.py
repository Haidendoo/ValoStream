"""Opt-in daily volume check; deploy with the repository available on PYTHONPATH."""

from datetime import timedelta
import json
import os

from airflow.sdk import DAG, CronDataIntervalTimetable
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
import pendulum


def check_daily_volume(data_interval_end=None):
    if data_interval_end is None:
        raise AirflowException("A scheduled data interval is required; use the monitor CLI for an explicit date")
    # Import at task execution, avoiding DB dependencies during scheduler DAG parsing.
    from ops.statistical_monitor import run_monitor

    target = data_interval_end.in_timezone("UTC").date() - timedelta(days=1)
    report = os.environ["VALOSTREAM_QUALITY_REPORT_FILE"]
    summary = run_monitor(target, report, os.environ.get("VALOSTREAM_QUALITY_SOURCE_SCHEMA", "vault"))
    print(json.dumps(summary, sort_keys=True))
    if summary["status"] != "ok":
        raise AirflowException(f"Daily volume monitor: {summary['status']}; see {report}")


with DAG(
    dag_id="valostream_statistical_monitors",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    # Keep a real data interval regardless of the installation's cron defaults.
    # The run at 02:00 UTC checks the previous completed UTC calendar day.
    schedule=CronDataIntervalTimetable("0 2 * * *", timezone="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["valostream", "quality"],
) as dag:
    PythonOperator(task_id="daily_volume", python_callable=check_daily_volume,
                   execution_timeout=timedelta(minutes=15), do_xcom_push=False)
