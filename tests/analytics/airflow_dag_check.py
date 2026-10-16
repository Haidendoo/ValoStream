"""Run separately under Airflow 2.11 / Python 3.11, outside the dbt environment."""

import sys
from types import ModuleType
from unittest.mock import Mock, patch

from airflow.models import DagBag
from airflow.exceptions import AirflowException
import pendulum


bag = DagBag(dag_folder="airflow/dags", include_examples=False)
assert not bag.import_errors, bag.import_errors
dag = bag.dags["valostream_statistical_monitors"]
assert dag.catchup is False and dag.max_active_runs == 1
assert dag.is_paused_upon_creation is True
task = dag.get_task("daily_volume")
assert task.retries == 2
assert dag.timetable.summary == "0 2 * * *"

# Exercise the real callable with an isolated monitor stub: retry/backfill dates
# must come from the scheduled interval, not the worker's wall clock.
monitor = ModuleType("ops.statistical_monitor")
monitor.run_monitor = Mock(return_value={"status": "ok"})
with patch.dict(sys.modules, {"ops.statistical_monitor": monitor}), \
        patch.dict("os.environ", {"VALOSTREAM_QUALITY_REPORT_FILE": "/tmp/quality.json"}):
    task.python_callable(pendulum.datetime(2026, 10, 9, 2, tz="UTC"))
    assert monitor.run_monitor.call_args.args[0].isoformat() == "2026-10-08"
    for status in ("anomaly", "insufficient_baseline"):
        monitor.run_monitor.return_value = {"status": status}
        try:
            task.python_callable(pendulum.datetime(2026, 10, 9, 2, tz="UTC"))
        except AirflowException:
            pass
        else:
            raise AssertionError(f"{status} was silently successful")
print("Airflow DAG parse, schedule, retry window, and failure propagation passed.")
