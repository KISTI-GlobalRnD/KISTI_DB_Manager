import argparse
import importlib.util
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "db_key_health_audit.py"
SPEC = importlib.util.spec_from_file_location("db_key_health_audit", SCRIPT_PATH)
db_key_health_audit = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(db_key_health_audit)


class TestDbKeyHealthAudit(unittest.TestCase):
    def test_parquet_checks_report_bad_key_counts(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            import duckdb  # noqa: F401
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pyarrow and duckdb are required: {exc}")

        with TemporaryDirectory() as td:
            root = Path(td)
            parquet_root = root / "works"
            parquet_root.mkdir()
            pq.write_table(
                pa.table(
                    {
                        "id": [
                            "https://openalex.org/W1",
                            None,
                            "NULL",
                            "",
                            "https://openalex.org/W-1",
                        ]
                    }
                ),
                parquet_root / "part-0.parquet",
            )
            args = argparse.Namespace(
                parquet_root=str(parquet_root),
                duckdb_temp_dir="",
                threads=1,
                memory_limit="1GB",
                key_column="id",
                prefix_length=64,
                sample_limit=10,
                parquet_duplicate_sample=False,
                parquet_prefix_collision_sample=False,
            )
            report = {"checks": {}}

            db_key_health_audit.run_parquet_checks(args, report)

            summary = report["checks"]["parquet_summary"]
            self.assertEqual(summary["status"], "ok")
            self.assertEqual(summary["rows"][0], [5, 4, 1, 1, 2, 3])
            sample_values = {row[0] for row in report["checks"]["parquet_bad_key_sample"]["rows"]}
            self.assertIn("NULL", sample_values)
            self.assertIn("https://openalex.org/W-1", sample_values)


if __name__ == "__main__":
    unittest.main()
