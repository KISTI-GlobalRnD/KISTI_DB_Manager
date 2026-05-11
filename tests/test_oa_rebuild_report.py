import importlib.util
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "oa_rebuild_0330_serving_db.py"
SPEC = importlib.util.spec_from_file_location("oa_rebuild_0330_serving_db", SCRIPT_PATH)
oa_rebuild = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(oa_rebuild)


class TestOaRebuildReport(unittest.TestCase):
    def test_report_completed_validates_table_and_db(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "report.json"
            path.write_text(
                json.dumps(
                    {
                        "finished_at": "2026-04-27T00:00:00+00:00",
                        "issues": [],
                        "artifacts": {
                            "db_name": "openalex_20260330_raw_yjk",
                            "selected_tables": ["works"],
                            "tables_completed_session": ["works"],
                            "per_table": [{"table_original": "works", "files": [{"path": "part-0.parquet"}]}],
                        },
                    }
                ),
                encoding="utf-8",
            )

            self.assertTrue(
                oa_rebuild._report_completed(
                    path,
                    table_name="works",
                    db_name="openalex_20260330_raw_yjk",
                )
            )
            self.assertFalse(
                oa_rebuild._report_completed(
                    path,
                    table_name="works_authorships",
                    db_name="openalex_20260330_raw_yjk",
                )
            )
            self.assertFalse(
                oa_rebuild._report_completed(
                    path,
                    table_name="works",
                    db_name="other_db",
                )
            )

    def test_report_with_issues_is_not_completed(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "report.json"
            path.write_text(
                json.dumps(
                    {
                        "finished_at": "2026-04-27T00:00:00+00:00",
                        "issues": [{"stage": "load", "message": "failed"}],
                        "artifacts": {
                            "db_name": "openalex_20260330_raw_yjk",
                            "selected_tables": ["works"],
                        },
                    }
                ),
                encoding="utf-8",
            )

            self.assertFalse(
                oa_rebuild._report_completed(
                    path,
                    table_name="works",
                    db_name="openalex_20260330_raw_yjk",
                )
            )

    def test_archive_materialize_progress_for_fresh_db_moves_stale_progress(self):
        with TemporaryDirectory() as td:
            run_dir = Path(td)
            progress_path = run_dir / "parquet_materialize" / "progress.json"
            progress_path.parent.mkdir()
            progress_path.write_text('{"tables_completed": 1}', encoding="utf-8")

            backup_path = oa_rebuild._archive_materialize_progress_for_fresh_db(run_dir)

            self.assertIsNotNone(backup_path)
            assert backup_path is not None
            self.assertFalse(progress_path.exists())
            self.assertTrue(backup_path.exists())
            self.assertIn("fresh_db_reset", backup_path.name)
            self.assertEqual(backup_path.read_text(encoding="utf-8"), '{"tables_completed": 1}')

    def test_archive_materialize_progress_for_fresh_db_noops_when_absent(self):
        with TemporaryDirectory() as td:
            self.assertIsNone(oa_rebuild._archive_materialize_progress_for_fresh_db(Path(td)))


if __name__ == "__main__":
    unittest.main()
