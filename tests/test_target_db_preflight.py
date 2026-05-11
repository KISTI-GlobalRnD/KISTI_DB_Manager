import unittest

from KISTI_DB_Manager import target_db_preflight


class TestTargetDbPreflight(unittest.TestCase):
    def test_driver_inference_prefers_explicit_plan_driver(self):
        driver, inferred = target_db_preflight.db_driver_from_values(
            {"db": {"driver": "postgresql"}},
            {"port": 3306},
        )

        self.assertEqual(driver, "postgresql")
        self.assertFalse(inferred)

    def test_driver_inference_uses_port_when_not_explicit(self):
        driver, inferred = target_db_preflight.db_driver_from_values({}, {"port": 5432})

        self.assertEqual(driver, "postgresql")
        self.assertTrue(inferred)

    def test_planned_tables_applies_prefix_and_selection(self):
        plan = {
            "materialize": {"table_prefix": "raw_"},
            "tables": [
                {"name": "works", "reset": True},
                {"name": "authors", "reset": False},
            ],
        }

        tables = target_db_preflight.planned_tables(plan, table_names=["authors"])

        self.assertEqual(tables, [{"name": "authors", "target_table": "raw_authors", "reset": False}])

    def test_index_definition_match_requires_columns_prefix_and_unique(self):
        expected = {"columns": [("id", 64)], "unique": True}

        self.assertTrue(
            target_db_preflight.index_definition_matches(
                expected,
                {"columns": [("id", 64)], "unique": True},
            )
        )
        self.assertFalse(
            target_db_preflight.index_definition_matches(
                expected,
                {"columns": [("id", None)], "unique": True},
            )
        )

    def test_report_final_status_honors_failure_policy(self):
        report = {"issues": [{"severity": "error", "check": "x"}], "warnings": []}

        self.assertEqual(target_db_preflight.report_final_status(report, hard_fail=True), "failed")
        self.assertEqual(target_db_preflight.report_final_status(report, hard_fail=False), "done_with_issues")


if __name__ == "__main__":
    unittest.main()
