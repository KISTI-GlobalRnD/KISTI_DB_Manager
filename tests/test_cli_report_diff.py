import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout


from KISTI_DB_Manager.cli import main
from KISTI_DB_Manager.report import RunReport


class TestCLIReportDiff(unittest.TestCase):
    def test_report_diff_prints_markdown(self):
        with tempfile.TemporaryDirectory() as td:
            before_path = f"{td}/before.json"
            after_path = f"{td}/after.json"

            r1 = RunReport()
            r1.bump("rows_loaded", 10)
            r1.warn(stage="x", message="w1")
            r1.finish()
            with open(before_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(r1.to_dict()))

            r2 = RunReport()
            r2.bump("rows_loaded", 15)
            r2.warn(stage="x", message="w2")
            r2.warn(stage="y", message="w3")
            r2.finish()
            with open(after_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(r2.to_dict()))

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = main(["report", "diff", before_path, after_path])

            self.assertEqual(rc, 0)
            out = buf.getvalue()
            self.assertIn("# RunReport Diff", out)
            self.assertIn("rows_loaded", out)


if __name__ == "__main__":
    unittest.main()
