import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path


from KISTI_DB_Manager.cli import main


class TestCLIReportProfile(unittest.TestCase):
    def test_report_profile_prints_markdown(self):
        with tempfile.TemporaryDirectory() as td:
            report_path = Path(td, "run_report.json")
            report = {
                "run_id": "r1",
                "duration_s": 10.0,
                "stats": {
                    "records_read": 1000,
                    "records_ok": 1000,
                    "rows_loaded": 20000,
                    "batches_total": 10,
                    "tables_total": 5,
                },
                "timings_ms": {
                    "pipeline.json.total": 10000,
                    "io.json_parse": 300,
                    "json.flatten": 1200,
                    "json.db.load": 7000,
                    "db.alter": 900,
                    "db.load_data.exec": 5600,
                    "db.load_data.tsv_write": 900,
                },
                "issues": [{"stage": "x"}],
                "artifacts": {
                    "mode": "ingest-fast",
                    "schema_mode": "evolve",
                    "chunk_size": 1000,
                    "parallel_workers": 0,
                },
            }
            report_path.write_text(json.dumps(report), encoding="utf-8")

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = main(["report", "profile", str(report_path), "--top", "3"])

            self.assertEqual(rc, 0)
            out = buf.getvalue()
            self.assertIn("# RunReport Profile", out)
            self.assertIn("db-load-bound-drift-ddl", out)
            self.assertIn("| json.db.load | 7000 |", out)

    def test_report_profile_writes_json(self):
        with tempfile.TemporaryDirectory() as td:
            report_path = Path(td, "run_report.json")
            out_path = Path(td, "profile.json")
            report = {
                "run_id": "r2",
                "duration_s": 4.0,
                "stats": {"records_read": 100, "records_ok": 100},
                "timings_ms": {
                    "pipeline.json.total": 4000,
                    "io.json_parse": 200,
                    "json.flatten": 2500,
                    "json.db.load": 600,
                },
                "issues": [],
                "artifacts": {"mode": "default", "schema_mode": "evolve"},
            }
            report_path.write_text(json.dumps(report), encoding="utf-8")

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = main(["report", "profile", str(report_path), "--as-json", "--out", str(out_path)])

            self.assertEqual(rc, 0)
            self.assertTrue(out_path.exists())
            prof = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(prof["run_id"], "r2")
            self.assertEqual(prof["bottleneck"]["class"], "flatten-bound")


if __name__ == "__main__":
    unittest.main()
