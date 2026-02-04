import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path


from KISTI_DB_Manager.cli import main


class TestCLIReviewPlan(unittest.TestCase):
    def test_review_plan_writes_outputs(self):
        with tempfile.TemporaryDirectory() as td:
            data_dir = Path(td, "data")
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "x.jsonl").write_text('{"id": 1, "items": [{"a": 1}]}\n{"id": 2}\n', encoding="utf-8")

            cfg_path = Path(td, "config.json")
            out_dir = Path(td, "out")

            cfg = {
                "data_config": {
                    "PATH": str(data_dir),
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                    "KEY_SEP": "__",
                    "index_key": "id",
                    "chunk_size": 10,
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = main(["review", "plan", "--config", str(cfg_path), "--out", str(out_dir), "--max-records", "10"])

            self.assertEqual(rc, 0)
            self.assertTrue((out_dir / "PLAN.md").exists())
            self.assertTrue((out_dir / "plan.html").exists())
            self.assertTrue((out_dir / "schema.svg").exists())
            self.assertTrue((out_dir / "schema.mmd").exists())
            self.assertTrue((out_dir / "ddl.json").exists())
            self.assertTrue((out_dir / "ddl.sql").exists())
            self.assertTrue((out_dir / "plan.json").exists())
            self.assertTrue((out_dir / "plan_run_report.json").exists())


if __name__ == "__main__":
    unittest.main()
