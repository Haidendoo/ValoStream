from contextlib import ExitStack
from unittest import TestCase, main
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
import pymysql

from serving import app as api
from serving.queries import ServingSource


class ServingTest(TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.factory = self.stack.enter_context(patch.object(api, "get_connection"))
        self.stack.enter_context(patch.object(api, "SOURCE", ServingSource(mart_schema="release_test")))
        self.stack.enter_context(patch.object(api.log, "exception"))
        self.conn = self.factory.return_value
        self.cursor = self.conn.cursor.return_value.__enter__.return_value
        self.cursor.fetchall.return_value = []
        # Avoid lifespan queries so each assertion counts request-owned connections only.
        self.client = TestClient(api.app)
        self.addCleanup(self.client.close)

    def test_unsupported_resolution_and_invalid_inputs_never_query_db(self):
        for url in (
            "/spatial/hexagons?resolution=8", "/spatial/hexagons?resolution=10",
            "/spatial/hexagons?entity=unknown", "/spatial/couriers?status=unknown",
            "/recommendations?user_id=u&lat=91", "/recommendations?user_id=u&lon=-181",
            "/recommendations?user_id=u&lat=nan", "/recommendations?user_id=u&lat=inf",
            "/recommendations?user_id=u&limit=0", "/recommendations?user_id=",
        ):
            with self.subTest(url=url):
                self.assertEqual(self.client.get(url).status_code, 422)
        self.factory.assert_not_called()

    def test_both_supported_resolutions_and_empty_result(self):
        for resolution in (7, 9):
            response = self.client.get(f"/spatial/hexagons?resolution={resolution}")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["resolution"], resolution)
            self.assertEqual(response.json()["hexagons"], [])

    def test_fallback_closes_first_connection_before_opening_second(self):
        first, second = MagicMock(), MagicMock()
        first.cursor.return_value.__enter__.return_value.fetchall.return_value = []
        second.cursor.return_value.__enter__.return_value.fetchall.return_value = []
        calls = []

        def connect():
            if calls:
                first.close.assert_called_once()
                return second
            calls.append(True)
            return first

        self.factory.side_effect = connect
        response = self.client.get("/recommendations?user_id=u&use_h3_filter=true")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.factory.call_count, 2)
        second.close.assert_called_once()
        self.assertEqual(response.json()["total_returned"], 0)

    def test_query_failure_closes_connection_and_does_not_expose_sql(self):
        self.cursor.execute.side_effect = pymysql.OperationalError(1146, "secret_schema.secret_table missing")
        for url in ("/spatial/couriers", "/spatial/hexagons", "/stats", "/recommendations?user_id=u"):
            with self.subTest(url=url):
                self.conn.close.reset_mock()
                response = self.client.get(url)
                self.assertEqual(response.status_code, 503)
                self.assertNotIn("secret", response.text)
                self.conn.close.assert_called_once()

    def test_empty_neighborhood_does_not_fallback_on_database_error(self):
        self.cursor.execute.side_effect = pymysql.OperationalError(2006, "gone away")
        response = self.client.get("/recommendations?user_id=u&use_h3_filter=true")
        self.assertEqual(response.status_code, 503)
        self.factory.assert_called_once()

    def test_zero_distance_is_preserved(self):
        self.cursor.fetchall.return_value = [{
            "merchant_id": "m1", "interaction_count": 4, "dist_meters": 0,
            "avg_lat": 10.775, "avg_lon": 106.7, "sample_h3_res7": "8765D01B0FFFFFF", "score": 4,
        }]
        result = self.client.get("/recommendations?user_id=u").json()["recommendations"][0]
        self.assertEqual(result["distance_meters"], 0)
        self.assertEqual(result["score"], 4)
        self.assertEqual(result["h3_res7"], "8765d01b0ffffff")

    def test_health_checks_active_mart_tables_and_closes(self):
        response = self.client.get("/healthz")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["source"], "marts")
        self.assertEqual(self.cursor.execute.call_count, 9)
        self.conn.close.assert_called_once()
        for call in self.cursor.execute.call_args_list:
            self.assertIn("`release_test`", call.args[0])

    def test_health_missing_mart_reports_not_ready(self):
        self.cursor.execute.side_effect = pymysql.ProgrammingError(1146, "missing")
        self.assertEqual(self.client.get("/healthz").status_code, 503)
        self.conn.close.assert_called_once()

    def test_stats_use_normalized_staging_in_mart_mode(self):
        self.cursor.fetchall.return_value = [{"tbl": "hub_user", "cnt": 2}]
        self.assertEqual(self.client.get("/stats").json(), {"hub_user": 2})
        self.assertIn("`release_test`.`stg_users`", self.cursor.execute.call_args.args[0])

    def test_metrics_record_status_without_user_query_labels(self):
        self.client.get("/spatial/couriers?limit=2")
        text = self.client.get("/metrics").text
        self.assertIn('valostream_api_requests_total{endpoint="/spatial/couriers",status="200"}', text)
        self.assertNotIn('endpoint="/spatial/couriers?limit=', text)


class QueryTest(TestCase):
    def test_configuration_identifiers_are_validated(self):
        for name in ("x; DROP TABLE users", "schema.table", "x`", "", "x" * 65):
            with self.assertRaises(ValueError):
                ServingSource(mart_schema=name)

    def test_filter_values_are_bound_parameters(self):
        source = ServingSource(mart_schema="release_test")
        sql, params = source.live_couriers("idle' OR 1=1", 12)
        self.assertNotIn("idle'", sql)
        self.assertEqual(params, ["idle' OR 1=1", 12])

    def test_recommendation_limit_follows_score_sort(self):
        sql, params = ServingSource(mart_schema="release_test").recommendations(1.0, 2.0, 4, [123, 456])
        self.assertIn("ORDER BY score DESC, interaction_count DESC, merchant_id LIMIT %s", sql)
        self.assertEqual(params, [2.0, 1.0, 123, 456, 4])


if __name__ == "__main__":
    main()
