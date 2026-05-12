import tempfile
import unittest
from pathlib import Path

from KISTI_DB_Manager.report import RunReport


class TestRunReport(unittest.TestCase):
    def test_report_counts(self):
        report = RunReport()
        report.warn(stage="s1", message="w1")
        report.error(stage="s2", message="e1")
        self.assertEqual(report.stats.get("issues_warning"), 1)
        self.assertEqual(report.stats.get("issues_error"), 1)
        self.assertEqual(len(report.issues), 2)

    def test_report_exception(self):
        report = RunReport()
        try:
            raise ValueError("boom")
        except Exception as exc:
            report.exception(stage="stage", message="msg", exc=exc, foo=123)
        self.assertEqual(report.stats.get("issues_error"), 1)
        self.assertEqual(report.issues[0].exception_type, "ValueError")
        self.assertIn("foo", report.issues[0].context)

    def test_save_json_does_not_follow_output_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            external = root / "external.json"
            external.write_text("keep", encoding="utf-8")
            out = root / "report.json"
            out.symlink_to(external)

            report = RunReport()
            report.finish()
            with self.assertRaisesRegex(RuntimeError, "not a safe file|symlink"):
                report.save_json(str(out))

            self.assertEqual(external.read_text(encoding="utf-8"), "keep")
            self.assertTrue(out.is_symlink())


if __name__ == "__main__":
    unittest.main()
