import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch


from KISTI_DB_Manager.cli import main
from KISTI_DB_Manager.pipeline import JsonRunResult
from KISTI_DB_Manager.report import RunReport


class TestCLIJson(unittest.TestCase):
    def test_json_run_writes_report(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            report_path = f"{td}/report.json"

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            fake_report.warn(stage="t", message="w")

            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ), patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = main(["json", "run", "--config", cfg_path, "--report", report_path])

            self.assertEqual(rc, 0)
            with open(report_path, encoding="utf-8") as f:
                saved = json.loads(f.read())
            self.assertIn("run_id", saved)
            self.assertIn("issues", saved)


if __name__ == "__main__":
    unittest.main()
