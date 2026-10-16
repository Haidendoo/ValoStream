from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

from ops.statistical_monitor import collect_counts, run_monitor, write_report
from serving.quality import assess_volume, read_report, summarize_report

DAY = date(2026, 10, 8)


def counts(current=100):
    return {(DAY - timedelta(days=i)).isoformat(): 100 if i else current for i in range(8)}


def report(current=100):
    return {"version": 1, "source_ref": "main", "source_hash": "abc123", "state": "complete",
            "target_day": DAY.isoformat(), "checked_at": "2026-10-09T02:00:00+00:00",
            "counts": {name: counts(current) for name in ("interactions", "couriers")}}


class VolumeTest(unittest.TestCase):
    def test_strict_fifty_percent_threshold_in_both_directions(self):
        for current, status in ((100, "ok"), (150, "ok"), (50, "ok"), (151, "anomaly"), (49, "anomaly")):
            with self.subTest(current=current):
                self.assertEqual(assess_volume(counts(current), DAY)["status"], status)

    def test_missing_baseline_day_is_not_zero_or_healthy(self):
        values = counts()
        del values["2026-10-04"]
        result = assess_volume(values, DAY)
        self.assertEqual(result["status"], "insufficient_baseline")
        self.assertEqual(result["missing_days"], ["2026-10-04"])

    def test_missing_target_day_detects_total_outage(self):
        values = counts()
        del values[DAY.isoformat()]
        self.assertEqual(assess_volume(values, DAY)["status"], "anomaly")

    def test_zero_baseline_has_no_division_error(self):
        values = {day: 0 for day in counts()}
        self.assertEqual(assess_volume(values, DAY)["status"], "ok")
        values[DAY.isoformat()] = 1
        result = assess_volume(values, DAY)
        self.assertEqual(result["status"], "anomaly")
        self.assertIsNone(result["relative_change"])

    def test_invalid_counts_rejected(self):
        for invalid in (-1, 1.5, True, "100"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                assess_volume({**counts(), DAY.isoformat(): invalid}, DAY)
        with self.assertRaises(ValueError):
            assess_volume({**counts(), "2026-09-01": 100}, DAY)

    def test_empty_baseline_does_not_report_healthy(self):
        self.assertEqual(assess_volume({}, DAY)["status"], "insufficient_baseline")

    def test_query_bounds_are_complete_days_and_identifiers_are_validated(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [(DAY, 100)]
        result = collect_counts(conn, "test_catalog", "vault", DAY)
        self.assertEqual(result["interactions"], {DAY.isoformat(): 100})
        self.assertIn("query_timeout = 90", cursor.execute.call_args_list[1].args[0])
        for call in cursor.execute.call_args_list[2:]:
            self.assertEqual(call.args[1], (datetime(2026, 10, 1), datetime(2026, 10, 9)))
            self.assertIn("SELECT DISTINCT", call.args[0])
        cursor.reset_mock()
        with self.assertRaises(ValueError):
            collect_counts(conn, "main; DROP TABLE x", "vault", DAY)
        cursor.execute.assert_not_called()


class ReportTest(unittest.TestCase):
    def test_old_report_keeps_old_window_even_if_rerun_today(self):
        data = report()
        data["checked_at"] = datetime.now(timezone.utc).isoformat()
        self.assertEqual(summarize_report(data)["window_end"],
                         datetime(2026, 10, 9, tzinfo=timezone.utc).timestamp())

    def test_invalid_reports_rejected(self):
        for changes in ({"source_ref": "pr_123"}, {"state": "unknown"}, {"counts": {}},
                        {"checked_at": "2026-10-09T02:00:00"},
                        {"checked_at": "2026-10-08T02:00:00+00:00"},
                        {"checked_at": "2099-10-09T02:00:00+00:00"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                summarize_report({**report(), **changes})

    def test_atomic_write_failure_preserves_previous_report_and_cleans_temp(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "latest.json"
            write_report(path, report())
            with patch("ops.statistical_monitor.os.replace", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    write_report(path, report(200))
            self.assertEqual(read_report(path)["status"], "ok")
            self.assertEqual(list(Path(directory).iterdir()), [path])

    def test_monitor_freezes_main_and_closes_before_publishing(self):
        conn = MagicMock()

        @contextmanager
        def frozen(*args):
            self.assertEqual(args[-2:], ("main", "vault"))
            yield {"catalog": "frozen", "schema": "vault", "hash": "abc"}

        with TemporaryDirectory() as directory, patch("ops.statistical_monitor.connection", return_value=conn), \
                patch("ops.statistical_monitor.frozen_catalog", side_effect=frozen), \
                patch("ops.statistical_monitor.collect_counts", return_value=report()["counts"]):
            path = Path(directory) / "latest.json"
            self.assertEqual(run_monitor(DAY, path)["status"], "ok")
            self.assertEqual(read_report(path)["status"], "ok")
            conn.close.assert_called_once()

    def test_database_failure_replaces_previous_green_result(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "latest.json"
            write_report(path, report())
            with patch("ops.statistical_monitor.connection", side_effect=RuntimeError("db down")):
                with self.assertRaisesRegex(RuntimeError, "db down"):
                    run_monitor(DAY, path)
            self.assertEqual(read_report(path)["status"], "error")
            self.assertNotIn("db down", path.read_text())

    def test_current_and_future_days_rejected_before_query(self):
        with patch("ops.statistical_monitor.connection") as connection:
            for day in (datetime.now(timezone.utc).date(), date(2099, 1, 1)):
                with self.assertRaises(ValueError):
                    run_monitor(day, "unused.json")
            connection.assert_not_called()


if __name__ == "__main__":
    unittest.main()
