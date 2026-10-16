"""Exercise eight days and replay deduplication in real, disposable Iceberg tables."""

from datetime import date, datetime, time, timedelta
import os

import pyarrow as pa
from pyiceberg.io.pyarrow import PyArrowFileIO

from nessie_smoke import iceberg_fixtures
from ops.nessie_quality_gate import frozen_catalog
from ops.statistical_monitor import collect_counts
from serving.quality import assess_volume


def main():
    target = date(2026, 10, 8)
    with iceberg_fixtures() as (conn, session, branch, namespace, catalog):
        for table_name, key in (("sat_interaction_context_spatial", "hk_interaction_id"),
                                ("sat_courier_telemetry_spatial", "hk_courier_id")):
            table = catalog.load_table((namespace, table_name))
            # REST table metadata advertises the container endpoint; this test runs
            # on the host, just like the shared fixture loader.
            table.io = PyArrowFileIO({**table.io.properties,
                                     "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:9000")})
            original = table.scan().to_arrow()
            template = original.to_pylist()[0]
            rows = []
            for offset in range(8):
                day = target - timedelta(days=offset)
                for index in range(4 if offset == 0 else 2):
                    rows.append({**template, key: f"event_{offset}_{index}",
                                 "load_date": datetime.combine(day, time(12))})
            # Duplicate every row; counts must still be 2/day baseline and 4 target.
            # Add records just outside each bound; neither may enter the result.
            outside = [{**template, key: "outside_start", "load_date": datetime(2026, 9, 30, 23, 59, 59)},
                       {**template, key: "outside_end", "load_date": datetime(2026, 10, 9)}]
            table.overwrite(pa.Table.from_pylist(rows + rows + outside, schema=original.schema))
        with frozen_catalog(conn, session, os.environ.get("NESSIE_URL", "http://localhost:19120"),
                            os.environ.get("STARROCKS_ICEBERG_URL", "http://valostream-nessie:19120/iceberg"),
                            branch, namespace) as snapshot:
            result = collect_counts(conn, snapshot["catalog"], snapshot["schema"], target)
            for stream, values in result.items():
                assert len(values) == 8, (stream, values)
                assert values[target.isoformat()] == 4, (stream, values)
                summary = assess_volume(values, target)
                assert summary["baseline_mean"] == 2, summary
                assert summary["status"] == "anomaly", summary
    print("Statistical smoke passed: frozen Iceberg, eight-day bounds, replay deduplication, >50% alert.")


if __name__ == "__main__":
    main()
