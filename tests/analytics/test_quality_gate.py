import unittest
from unittest.mock import Mock, MagicMock

from ops.analytics_common import identifier
from ops.nessie_quality_gate import isolated_catalog


class IsolationTest(unittest.TestCase):
    def setUp(self):
        self.conn = MagicMock()
        self.cursor = self.conn.cursor.return_value.__enter__.return_value
        self.session = Mock()
        response = Mock(content=b'{}')
        response.json.return_value = {"reference": {"type": "BRANCH", "name": "main", "hash": "abc123"}}
        self.session.request.return_value = response

    def context(self):
        return isolated_catalog(self.conn, self.session, "http://nessie", "http://nessie/iceberg")

    def statements(self):
        return [call.args[0] for call in self.cursor.execute.call_args_list]

    def test_frozen_branch_and_disposable_outputs(self):
        with self.context() as env:
            self.assertTrue(env["DBT_SOURCE_CATALOG"].startswith("vs_ci_"))
            self.assertTrue(env["DBT_OUTPUT_SCHEMA"].startswith("vs_out_"))
            self.assertEqual(env["DBT_SOURCE_SCHEMA"], "vault")
        calls = self.session.request.call_args_list
        create = calls[1]
        self.assertEqual(create.kwargs["json"]["hash"], "abc123")
        self.assertEqual(calls[-1].args[0], "DELETE")
        sql = self.statements()
        self.assertTrue(sql[-2].startswith("DROP DATABASE vs_out_"))
        self.assertTrue(sql[-1].startswith("DROP CATALOG vs_ci_"))
        self.assertFalse(any("MERGE" in statement for statement in sql))

    def test_failed_build_still_cleans_up(self):
        with self.assertRaisesRegex(RuntimeError, "dbt failed"):
            with self.context():
                raise RuntimeError("dbt failed")
        self.assertTrue(self.statements()[-1].startswith("DROP CATALOG"))
        self.assertEqual(self.session.request.call_args.args[0], "DELETE")

    def test_failed_catalog_creation_only_deletes_owned_branch(self):
        self.cursor.execute.side_effect = RuntimeError("catalog exists")
        with self.assertRaisesRegex(RuntimeError, "catalog exists"):
            with self.context():
                self.fail("Unexpected success")
        self.assertEqual(len(self.statements()), 1)
        self.assertEqual(self.session.request.call_args.args[0], "DELETE")

    def test_failed_database_creation_does_not_drop_existing_database(self):
        def execute(sql, params=None):
            if sql.startswith("CREATE DATABASE"):
                raise RuntimeError("database exists")
        self.cursor.execute.side_effect = execute
        with self.assertRaisesRegex(RuntimeError, "database exists"):
            with self.context():
                self.fail("Unexpected success")
        self.assertFalse(any(sql.startswith("DROP DATABASE") for sql in self.statements()))
        self.assertTrue(self.statements()[-1].startswith("DROP CATALOG"))

    def test_cleanup_continues_if_database_drop_fails(self):
        def execute(sql, params=None):
            if sql.startswith("DROP DATABASE"):
                raise RuntimeError("drop failed")
        self.cursor.execute.side_effect = execute
        with self.assertRaisesRegex(RuntimeError, "drop failed"):
            with self.context():
                pass
        self.assertTrue(self.statements()[-1].startswith("DROP CATALOG"))
        self.assertEqual(self.session.request.call_args.args[0], "DELETE")

    def test_rejected_branch_creation_does_not_delete_anything(self):
        self.session.request.side_effect = [self.session.request.return_value, RuntimeError("branch exists")]
        with self.assertRaisesRegex(RuntimeError, "branch exists"):
            with self.context():
                self.fail("Unexpected success")
        self.assertEqual(self.session.request.call_count, 2)
        self.cursor.execute.assert_not_called()

    def test_sql_identifier_rejects_injection(self):
        for name in ("main; drop database vault", "schema.name", "UPPER", "", "x" * 64):
            with self.assertRaises(ValueError):
                identifier(name)


if __name__ == "__main__":
    unittest.main()
