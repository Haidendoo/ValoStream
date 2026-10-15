from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

from ops.refresh_marts import prune, rollback, writer_lock
from serving.releases import (FreshnessPolicy, Release, ReleaseState, RetiredRelease, read_state,
                              release_lease, selected_release, write_state)

NOW = datetime(2026, 10, 15, 12, tzinfo=timezone.utc)


def release(char, age=0, event_age=0):
    return Release("vs_release_" + char * 32, "main", "abc123", "vault",
                   (NOW - timedelta(seconds=age)).isoformat(),
                   {stream: (NOW - timedelta(seconds=event_age)).isoformat()
                    for stream in ("interactions", "couriers")})


class RetentionTest(unittest.TestCase):
    def setUp(self):
        directory = self.enterContext(TemporaryDirectory())
        self.path = Path(directory) / "active.json"
        self.conn = MagicMock()
        self.cursor = self.conn.cursor.return_value.__enter__.return_value
        self.retired = release("c", age=10000)
        self.state = ReleaseState(release("a"), release("b"), NOW.isoformat(), retired=(
            RetiredRelease(self.retired, (NOW - timedelta(seconds=3600)).isoformat()),
        ))
        write_state(self.path, self.state)

    def prune(self, apply=False):
        return prune(self.conn, self.path, retention_seconds=300, apply=apply, now=NOW)

    def test_dry_run_lists_only_owned_retired_schema(self):
        self.assertEqual(self.prune(), [{"schema": self.retired.schema, "status": "eligible"}])
        self.cursor.execute.assert_not_called()
        self.assertEqual(read_state(self.path), self.state)

    def test_apply_keeps_active_previous_and_removes_record_after_drop(self):
        self.assertEqual(self.prune(True)[0]["status"], "deleted")
        self.cursor.execute.assert_called_once_with(f"DROP DATABASE IF EXISTS {self.retired.schema}")
        remaining = read_state(self.path)
        self.assertEqual(remaining.active, self.state.active)
        self.assertEqual(remaining.previous, self.state.previous)
        self.assertEqual(remaining.published_at, self.state.published_at)
        self.assertEqual(remaining.retired, ())
        self.assertEqual(self.prune(True), [])

    def test_grace_is_measured_since_retirement_not_build_time(self):
        state = replace(self.state, retired=(RetiredRelease(self.retired, NOW.isoformat()),))
        write_state(self.path, state)
        self.assertEqual(self.prune(True)[0]["status"], "grace_period")
        self.cursor.execute.assert_not_called()

    def test_request_lease_blocks_collection_until_request_finishes(self):
        with release_lease(self.path, self.retired.schema):
            self.assertEqual(self.prune(True)[0]["status"], "in_use")
            self.cursor.execute.assert_not_called()
        self.assertEqual(self.prune(True)[0]["status"], "deleted")

    def test_leases_exclude_a_separate_collector_process(self):
        script = """
import sys
from serving.releases import release_lease
try:
    with release_lease(sys.argv[1], sys.argv[2], exclusive=True):
        pass
except BlockingIOError:
    raise SystemExit(3)
"""
        command = [sys.executable, "-c", script, str(self.path), self.retired.schema]
        with release_lease(self.path, self.retired.schema):
            blocked = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(blocked.returncode, 3, blocked.stderr)
        available = subprocess.run(command, capture_output=True, text=True, timeout=10)
        self.assertEqual(available.returncode, 0, available.stderr)

    def test_collector_lease_blocks_request_selection(self):
        with release_lease(self.path, self.state.active.schema, exclusive=True):
            with self.assertRaises(BlockingIOError):
                with selected_release(self.path):
                    self.fail("Unexpected lease acquisition")

    def test_selection_rechecks_pointer_after_acquiring_lease(self):
        updated = replace(self.state, active=release("d"), previous=self.state.active)

        @contextmanager
        def raced_lease(*args):
            write_state(self.path, updated)
            yield

        with patch("serving.releases.release_lease", side_effect=raced_lease):
            with selected_release(self.path) as chosen:
                self.assertEqual(chosen, updated.active)

    def test_failed_drop_or_manifest_write_remains_retryable(self):
        self.cursor.execute.side_effect = RuntimeError("drop failed")
        with self.assertRaises(RuntimeError):
            self.prune(True)
        self.assertEqual(read_state(self.path), self.state)
        self.cursor.execute.side_effect = None
        with patch("ops.refresh_marts.write_state", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.prune(True)
        self.assertEqual(read_state(self.path), self.state)
        self.assertEqual(self.prune(True)[0]["status"], "deleted")

    def test_writer_lock_serializes_collection_and_publication(self):
        with writer_lock(self.path):
            with self.assertRaisesRegex(RuntimeError, "in progress"):
                self.prune(True)
        self.cursor.execute.assert_not_called()

    def test_active_previous_cannot_be_in_retired_ownership_list(self):
        for protected in (self.state.active, self.state.previous):
            with self.assertRaises(ValueError):
                replace(self.state, retired=(RetiredRelease(protected, NOW.isoformat()),))

    def test_corrupt_manifest_never_triggers_database_scan_or_drop(self):
        self.path.write_text("{invalid")
        with self.assertRaises(ValueError):
            self.prune(True)
        self.cursor.execute.assert_not_called()

    def test_rollback_preserves_retirement_history(self):
        with patch("ops.refresh_marts.verify_release"):
            state = rollback(self.conn, self.path)
        self.assertEqual(state.retired, self.state.retired)
        self.assertEqual(state.active, self.state.previous)

    def test_v1_manifest_without_history_or_watermarks_remains_readable(self):
        data = json.loads(self.path.read_text())
        data["version"] = 1
        del data["retired"]
        for key in ("active", "previous"):
            del data[key]["watermarks"]
        self.path.write_text(json.dumps(data))
        state = read_state(self.path)
        self.assertIsNone(state.active.watermarks)
        self.assertEqual(state.retired, ())
        self.assertEqual(self.prune(True), [])


class FreshnessTest(unittest.TestCase):
    def test_fresh_build_does_not_hide_stale_events(self):
        result = FreshnessPolicy(300, 600).evaluate(release("a", event_age=3600), NOW)
        self.assertEqual(result["status"], "stale")
        self.assertEqual(result["build_age_seconds"], 0)
        self.assertEqual(result["event_age_seconds"]["couriers"], 3600)

    def test_unknown_watermark_is_not_treated_as_fresh(self):
        old = replace(release("a"), watermarks=None)
        self.assertEqual(FreshnessPolicy(300, 600).evaluate(old, NOW)["status"], "unknown")
        self.assertEqual(FreshnessPolicy(300, 0).evaluate(old, NOW)["status"], "ok")

    def test_threshold_boundary_disabled_checks_and_future_clock(self):
        self.assertEqual(FreshnessPolicy(300, 600).evaluate(release("a", 300, 600), NOW)["status"], "ok")
        self.assertEqual(FreshnessPolicy(300).evaluate(release("a", 301), NOW)["status"], "stale")
        self.assertEqual(FreshnessPolicy(300, 600).evaluate(release("a", event_age=-301), NOW)["status"], "clock_skew")
        self.assertEqual(FreshnessPolicy().evaluate(release("a", 10000, 10000), NOW)["status"], "unchecked")


if __name__ == "__main__":
    unittest.main()
