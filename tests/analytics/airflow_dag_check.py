"""Run separately under Airflow 3.3.2 / Python 3.11, outside the dbt environment."""

from datetime import timedelta
from importlib.metadata import version
import sys
from types import ModuleType
from unittest.mock import Mock, patch

from airflow.dag_processing.dagbag import DagBag
from airflow.exceptions import AirflowException
from airflow.sdk import CronDataIntervalTimetable
from airflow.serialization.serialized_objects import DagSerialization
from airflow.timetables.base import TimeRestriction
import pendulum


assert version("apache-airflow") == "3.3.2"
bag = DagBag(dag_folder="airflow/dags")
assert not bag.import_errors, bag.import_errors
dag = bag.dags["valostream_statistical_monitors"]
assert dag.catchup is False and dag.max_active_runs == 1
assert dag.is_paused_upon_creation is True
task = dag.get_task("daily_volume")
assert task.retries == 2
assert task.retry_delay == timedelta(minutes=10)
assert task.execution_timeout == timedelta(minutes=15)
assert task.do_xcom_push is False
assert isinstance(dag.timetable, CronDataIntervalTimetable)
assert dag.timetable.expression == "0 2 * * *"
assert dag.timetable.timezone == "UTC"
# Airflow 3.3 separates SDK timetable definitions from scheduler implementations.
# Round-trip the actual DAG through serialization, as the scheduler consumes it.
scheduler_dag = DagSerialization.from_dict(DagSerialization.to_dict(dag))
assert scheduler_dag.timetable.summary == "0 2 * * *"

# Check real timetable transitions, including midnight/month/year boundaries.
# Catchup is enabled only on this test restriction to make historical runs
# deterministic; the production DAG continues to disable catchup.
scheduled_ends = []
for year, month, day in ((2026, 10, 8), (2026, 12, 31)):
    start = pendulum.datetime(year, month, day, 2, tz="UTC")
    restriction = TimeRestriction(earliest=start, latest=None, catchup=True)
    first = scheduler_dag.timetable.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
    assert first.data_interval.start == start
    assert first.data_interval.end == start.add(days=1)
    assert first.run_after == first.data_interval.end
    second = scheduler_dag.timetable.next_dagrun_info(last_automated_data_interval=first.data_interval,
                                                    restriction=restriction)
    assert second.data_interval.start == first.data_interval.end
    assert second.data_interval.end == start.add(days=2)
    scheduled_ends.append(first.data_interval.end)

# Exercise the real callable with an isolated monitor stub: retry/backfill dates
# must come from the scheduled interval, not the worker's wall clock.
monitor = ModuleType("ops.statistical_monitor")
monitor.run_monitor = Mock(return_value={"status": "ok"})
with patch.dict(sys.modules, {"ops.statistical_monitor": monitor}), \
        patch.dict("os.environ", {"VALOSTREAM_QUALITY_REPORT_FILE": "/tmp/quality.json"}):
    try:
        task.python_callable()
    except AirflowException as error:
        assert "data interval" in str(error)
    else:
        raise AssertionError("A manual run without a data interval was accepted")
    monitor.run_monitor.assert_not_called()
    for end in scheduled_ends:
        # Retrying the same scheduled interval must keep the original target day.
        for attempt in range(2):
            task.python_callable(end)
            assert monitor.run_monitor.call_args.args == (
                end.date() - timedelta(days=1), "/tmp/quality.json", "vault")
    for status in ("anomaly", "insufficient_baseline"):
        monitor.run_monitor.return_value = {"status": status}
        try:
            task.python_callable(pendulum.datetime(2026, 10, 9, 2, tz="UTC"))
        except AirflowException:
            pass
        else:
            raise AssertionError(f"{status} was silently successful")
    # Exercise the provider boundary, including context-to-keyword injection.
    # Clone for each attempt, as a real task execution receives a fresh operator.
    # A non-UTC context must still select the same completed UTC calendar day.
    monitor.run_monitor.return_value = {"status": "ok"}
    end = scheduled_ends[0].in_timezone("America/Los_Angeles")
    task.prepare_for_execution().execute(context={"data_interval_end": end})
    assert monitor.run_monitor.call_args.args == (
        scheduled_ends[0].date() - timedelta(days=1), "/tmp/quality.json", "vault")
    monitor.run_monitor.return_value = {"status": "anomaly"}
    try:
        task.prepare_for_execution().execute(context={"data_interval_end": end})
    except AirflowException:
        pass
    else:
        raise AssertionError("PythonOperator swallowed an anomaly")
    monitor.run_monitor.side_effect = RuntimeError("database unavailable")
    try:
        task.python_callable(scheduled_ends[0])
    except RuntimeError:
        pass
    else:
        raise AssertionError("Runtime failure was silently successful")
print("Airflow 3.3.2 DAG parse, timetable boundaries, retry window, operator context, and failure propagation passed.")
