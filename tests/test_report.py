import unittest


from KISTI_DB_Manager.report import RunReport


class TestReport(unittest.TestCase):
    def test_report_counts(self):
        r = RunReport()
        r.warn(stage="s1", message="w1")
        r.error(stage="s2", message="e1")
        self.assertEqual(r.stats.get("issues_warning"), 1)
        self.assertEqual(r.stats.get("issues_error"), 1)
        self.assertEqual(len(r.issues), 2)

    def test_report_exception(self):
        r = RunReport()
        try:
            raise ValueError("boom")
        except Exception as e:
            r.exception(stage="stage", message="msg", exc=e, foo=123)
        self.assertEqual(r.stats.get("issues_error"), 1)
        self.assertEqual(r.issues[0].exception_type, "ValueError")
        self.assertIn("foo", r.issues[0].context)


if __name__ == "__main__":
    unittest.main()
