import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path


from KISTI_DB_Manager.cli import main


class TestCLIReview(unittest.TestCase):
    def test_review_pack_writes_outputs_without_db(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            out_dir = f"{td}/out"

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
            Path(cfg_path).write_text(json.dumps(cfg), encoding="utf-8")

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = main(["review", "pack", "--config", cfg_path, "--out", out_dir, "--no-db"])

            self.assertEqual(rc, 0)
            self.assertTrue(Path(out_dir, "REVIEW.md").exists())
            self.assertTrue(Path(out_dir, "review.html").exists())
            self.assertTrue(Path(out_dir, "schema.svg").exists())
            self.assertTrue(Path(out_dir, "schema.mmd").exists())
            self.assertTrue(Path(out_dir, "review.json").exists())


if __name__ == "__main__":
    unittest.main()
