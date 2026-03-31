import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path


from KISTI_DB_Manager.cli import main


class TestCLIReviewSchema(unittest.TestCase):
    def test_review_schema_viewer_writes_outputs_without_db(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            out_dir = Path(td, "out")

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                    "KEY_SEP": "__",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = main(["review", "schema-viewer", "--config", str(cfg_path), "--out", str(out_dir), "--no-db"])

            self.assertEqual(rc, 0)
            self.assertTrue((out_dir / "schema_viewer.html").exists())
            self.assertTrue((out_dir / "schema_viewer.json").exists())
            self.assertTrue((out_dir / "schema.svg").exists())
            self.assertTrue((out_dir / "schema.mmd").exists())
            html = (out_dir / "schema_viewer.html").read_text(encoding="utf-8")
            self.assertIn("Schema Viewer", html)
            self.assertIn("Table Catalog", html)


if __name__ == "__main__":
    unittest.main()
