import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path


from KISTI_DB_Manager.cli import main


class TestCLIReviewDiff(unittest.TestCase):
    def test_review_diff_prints_markdown(self):
        with tempfile.TemporaryDirectory() as td:
            before_path = Path(td, "before_review.json")
            after_path = Path(td, "after_review.json")

            before = {
                "base_table": "t",
                "tables": [
                    {"name_sql": "t", "columns": [{"name": "id", "column_type": "INT"}], "rows": 1},
                    {"name_sql": "t__a", "columns": [{"name": "id"}, {"name": "x"}], "rows": 2},
                ],
            }
            after = {
                "base_table": "t",
                "tables": [
                    {"name_sql": "t", "columns": [{"name": "id", "column_type": "INT"}, {"name": "newc"}], "rows": 1},
                    {"name_sql": "t__b", "columns": [{"name": "id"}], "rows": 3},
                ],
            }

            before_path.write_text(json.dumps(before), encoding="utf-8")
            after_path.write_text(json.dumps(after), encoding="utf-8")

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = main(["review", "diff", str(before_path), str(after_path)])

            self.assertEqual(rc, 0)
            out = buf.getvalue()
            self.assertIn("# Review Pack Diff", out)
            self.assertIn("Added Tables", out)


if __name__ == "__main__":
    unittest.main()
