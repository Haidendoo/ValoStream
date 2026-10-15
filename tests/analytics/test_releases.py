from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

from ops import refresh_marts as refresh
from serving.releases import Release, ReleaseState, read_state, utc_now, write_state


def release(char):
    return Release("vs_release_" + char * 32, "main", "abc123", "vault", utc_now())


class ReleaseTest(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "active.json"
        self.old = ReleaseState(release("a"), release("b"), utc_now())
        write_state(self.path, self.old)
        self.conn, self.session = MagicMock(), MagicMock()
        self.cursor = self.conn.cursor.return_value.__enter__.return_value
        self.build = self.enterContext(patch.object(refresh, "run_dbt", return_value=0))
        self.verify = self.enterContext(patch.object(refresh, "verify_release"))
        self.enterContext(patch.object(refresh, "capture_watermarks", return_value={
            "interactions": utc_now(), "couriers": utc_now(),
        }))
        self.source_closed = False

        @contextmanager
        def frozen(*args):
            try:
                yield {"catalog": "vs_fixture", "schema": "vault", "ref": "main", "hash": "abc456"}
            finally:
                self.source_closed = True

        self.enterContext(patch.object(refresh, "frozen_catalog", side_effect=frozen))

    def publish(self):
        return refresh.publish(self.conn, self.session, self.path, artifacts=self.directory.name)

    def sql(self):
        return [call.args[0] for call in self.cursor.execute.call_args_list]

    def test_publish_keeps_old_active_until_validation_and_source_cleanup(self):
        def verify(conn, schema):
            self.assertEqual(read_state(self.path), self.old)
        self.verify.side_effect = verify

        def write(path, state):
            self.assertTrue(self.source_closed)
            write_state(path, state)
        with patch.object(refresh, "write_state", side_effect=write):
            result = self.publish()
        self.assertEqual(read_state(self.path), result)
        self.assertEqual(result.previous, self.old.active)
        self.assertEqual(result.active.source_hash, "abc456")
        self.assertEqual(result.retired[0].release, self.old.previous)
        self.assertEqual(self.build.call_args.args[0], ["build"])
        self.assertFalse(any(sql.startswith("DROP DATABASE") for sql in self.sql()))

    def test_initial_publish_has_no_previous_release(self):
        self.path.unlink()
        result = self.publish()
        self.assertIsNone(result.previous)
        self.assertEqual(read_state(self.path), result)

    def test_failed_gate_removes_only_its_output_and_keeps_active_pointer(self):
        self.build.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "quality gate failed"):
            self.publish()
        self.assertEqual(read_state(self.path), self.old)
        self.assertEqual(self.sql()[0].replace("CREATE", "DROP"), self.sql()[-1])
        self.assertNotIn(self.old.active.schema, " ".join(self.sql()))
        self.verify.assert_not_called()

    def test_timeout_and_query_failure_keep_active_pointer(self):
        for failure in (self.build, self.verify):
            with self.subTest(stage=failure):
                self.cursor.reset_mock()
                failure.side_effect = TimeoutError("timeout")
                with self.assertRaises(TimeoutError):
                    self.publish()
                failure.side_effect = None
                self.assertEqual(read_state(self.path), self.old)
                self.assertTrue(self.sql()[-1].startswith("DROP DATABASE"))

    def test_failed_database_creation_does_not_drop_an_existing_database(self):
        self.cursor.execute.side_effect = RuntimeError("already exists")
        with self.assertRaisesRegex(RuntimeError, "already exists"):
            self.publish()
        self.assertEqual(len(self.sql()), 1)
        self.build.assert_not_called()

    def test_corrupt_pointer_aborts_before_build(self):
        self.path.write_text('{"partial":')
        with self.assertRaises(ValueError):
            self.publish()
        self.build.assert_not_called()
        self.cursor.execute.assert_not_called()

    def test_concurrent_publish_and_rollback_are_rejected(self):
        with refresh.writer_lock(self.path):
            for operation in (self.publish, lambda: refresh.rollback(self.conn, self.path)):
                with self.assertRaisesRegex(RuntimeError, "in progress"):
                    operation()
        self.build.assert_not_called()
        self.assertEqual(read_state(self.path), self.old)

    def test_replace_failure_keeps_previous_document_and_validated_database(self):
        with patch("serving.releases.os.replace", side_effect=OSError("disk error")):
            with self.assertRaises(OSError):
                self.publish()
        self.assertEqual(read_state(self.path), self.old)
        self.assertFalse(any(sql.startswith("DROP DATABASE") for sql in self.sql()))
        self.assertFalse(list(self.path.parent.glob(".active.json.*")))

    def test_failure_after_replace_never_drops_potentially_live_database(self):
        def write_then_fail(path, state):
            write_state(path, state)
            raise OSError("directory fsync failed")
        with patch.object(refresh, "write_state", side_effect=write_then_fail):
            with self.assertRaises(OSError):
                self.publish()
        self.assertNotEqual(read_state(self.path).active, self.old.active)
        self.assertFalse(any(sql.startswith("DROP DATABASE") for sql in self.sql()))

    def test_source_cleanup_failure_does_not_publish(self):
        @contextmanager
        def failed_cleanup(*args):
            yield {"catalog": "vs_fixture", "schema": "vault", "ref": "main", "hash": "abc456"}
            raise RuntimeError("catalog cleanup failed")

        with patch.object(refresh, "frozen_catalog", side_effect=failed_cleanup):
            with self.assertRaisesRegex(RuntimeError, "catalog cleanup failed"):
                self.publish()
        self.assertEqual(read_state(self.path), self.old)
        # Validated output remains for inspection; it was never made active.
        self.assertFalse(any(sql.startswith("DROP DATABASE") for sql in self.sql()))

    def test_rollback_checks_previous_tables_before_swapping(self):
        result = refresh.rollback(self.conn, self.path)
        self.verify.assert_called_once_with(self.conn, self.old.previous.schema)
        self.assertEqual(result.active, self.old.previous)
        self.assertEqual(result.previous, self.old.active)
        self.assertEqual(read_state(self.path), result)
        self.build.assert_not_called()

    def test_rollback_missing_or_broken_previous_does_not_change_pointer(self):
        self.verify.side_effect = RuntimeError("missing table")
        with self.assertRaisesRegex(RuntimeError, "missing table"):
            refresh.rollback(self.conn, self.path)
        self.assertEqual(read_state(self.path), self.old)
        write_state(self.path, ReleaseState(self.old.active, None, utc_now()))
        with self.assertRaisesRegex(ValueError, "No previous"):
            refresh.rollback(self.conn, self.path)

    def test_loop_recovers_after_failure_and_waits_between_attempts(self):
        stop = MagicMock()
        stop.is_set.side_effect = [False, False, True]
        build = MagicMock(side_effect=[RuntimeError("dbt failed"), None])
        with patch.object(refresh.log, "exception") as logged:
            refresh.refresh_loop(build, 300, stop)
        self.assertEqual(build.call_count, 2)
        self.assertEqual(stop.wait.call_count, 2)
        stop.wait.assert_called_with(300)
        logged.assert_called_once()

    def test_manifest_rejects_unmanaged_schema_or_unknown_version(self):
        with self.assertRaises(ValueError):
            Release("vault", "main", "abc", "vault", utc_now())
        with self.assertRaises(ValueError):
            ReleaseState(self.old.active, None, utc_now(), version=99)


if __name__ == "__main__":
    unittest.main()
