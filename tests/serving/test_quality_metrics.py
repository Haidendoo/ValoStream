from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from serving import app as api


class QualityMetricsTest(unittest.TestCase):
    def test_missing_corrupt_and_recovered_report(self):
        today = datetime.now(timezone.utc).date()
        target = today - timedelta(days=1)
        counts = {(target - timedelta(days=i)).isoformat(): 100 for i in range(8)}
        report = {"version": 1, "source_ref": "main", "source_hash": "abc", "state": "complete",
                  "target_day": target.isoformat(), "checked_at": datetime.now(timezone.utc).isoformat(),
                  "counts": {"interactions": counts, "couriers": dict(counts)}}
        with TemporaryDirectory() as directory:
            path = Path(directory) / "latest.json"
            with patch.object(api, "QUALITY_REPORT_FILE", path), patch.object(api, "RELEASE_FILE", None):
                client = TestClient(api.app)
                self.assertIn("valostream_quality_report_valid 0.0", client.get("/metrics").text)
                path.write_text(json.dumps(report))
                self.assertIn("valostream_quality_healthy 1.0", client.get("/metrics").text)
                report["counts"]["interactions"][target.isoformat()] = 200
                path.write_text(json.dumps(report))
                self.assertIn("valostream_quality_healthy 0.0", client.get("/metrics").text)
                path.write_text("{broken")
                response = client.get("/metrics")
                self.assertEqual(response.status_code, 200)
                self.assertIn("valostream_quality_report_valid 0.0", response.text)
                self.assertNotIn("valostream_quality_healthy", response.text)
                self.assertNotIn("valostream_quality_window_end_timestamp_seconds", response.text)

    def test_monitor_is_opt_in(self):
        with patch.object(api, "QUALITY_REPORT_FILE", None):
            response = TestClient(api.app).get("/metrics")
        self.assertIn("valostream_quality_report_configured 0.0", response.text)
        self.assertNotIn("valostream_quality_report_valid", response.text)


if __name__ == "__main__":
    unittest.main()
