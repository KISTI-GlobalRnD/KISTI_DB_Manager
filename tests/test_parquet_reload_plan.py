import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from KISTI_DB_Manager import parquet_finalize, parquet_reload


class TestParquetReloadPlan(unittest.TestCase):
    def _plan(self, run_dir: Path) -> dict:
        return {
            "version": 1,
            "tag": "unit",
            "run_dir": str(run_dir),
            "config": str(run_dir / "config.json"),
            "dotenv": ".env",
            "db_name": "unit_db",
            "parquet_root": str(run_dir / "parquet"),
            "materialize": {"parallel_tables": 1, "parallel_files_per_table": 1, "staging_writer": "duckdb", "file_chunk_rows": 250},
            "validation": {"literal_null_marker": r"\N", "literal_null_marker_compare_mode": "utf8mb4_bin"},
            "finalize": {
                "enabled": True,
                "strict_indexes": True,
                "no_unique_fallback": True,
                "skip_analyze": True,
                "skip_validation": True,
                "indexes": [
                    {
                        "table": "works",
                        "index_name": "idx_works_id",
                        "columns": [{"name": "id", "prefix_len": 64}],
                        "unique": False,
                    }
                ],
            },
            "tables": [{"name": "works", "writer": "duckdb", "chunk_rows": 500, "reset": True, "expected_rows": 2}],
        }

    def test_plan_normalization_and_commands_are_dataset_neutral(self):
        with TemporaryDirectory() as td:
            run_dir = Path(td)
            plan = self._plan(run_dir)
            specs = parquet_reload.normalize_table_specs(plan)

            self.assertEqual(specs[0].name, "works")
            self.assertEqual(specs[0].writer, "duckdb")
            self.assertEqual(specs[0].chunk_rows, 500)

            cmd = parquet_reload.materialize_cmd(plan, specs[0], report_path=run_dir / "report.json")
            self.assertIn("--parquet-root", cmd)
            self.assertIn(str(run_dir / "parquet"), cmd)
            self.assertIn("--confirm-drop-tables", cmd)
            self.assertIn("works", cmd)

            validate = parquet_reload.validation_cmd(plan, specs[0], report_path=run_dir / "validate.json")
            self.assertIn("-m", validate)
            self.assertIn("KISTI_DB_Manager.openalex_reload_validate", validate)
            self.assertIn("--literal-null-marker-compare-mode", validate)
            self.assertIn("utf8mb4_bin", validate)
            self.assertIn("--skip-parquet-key-health", validate)
            self.assertIn("--skip-literal-null-marker-scan", validate)

    def test_literal_marker_full_mode_overrides_large_profile_default(self):
        with TemporaryDirectory() as td:
            run_dir = Path(td)
            plan = self._plan(run_dir)
            plan["validation"]["literal_marker"] = {"mode": "full"}
            spec = parquet_reload.normalize_table_specs(plan)[0]

            validate = parquet_reload.validation_cmd(plan, spec, report_path=run_dir / "validate.json")

            self.assertNotIn("--skip-literal-null-marker-scan", validate)

    def test_materialize_command_passes_artifact_contract_flags(self):
        with TemporaryDirectory() as td:
            run_dir = Path(td)
            plan = self._plan(run_dir)
            plan["preflight"] = {
                "artifact_contract": {
                    "require_schema_manifest": True,
                    "require_id_compaction": True,
                    "strict_schema_manifest": True,
                }
            }
            spec = parquet_reload.normalize_table_specs(plan)[0]

            cmd = parquet_reload.materialize_cmd(plan, spec, report_path=run_dir / "report.json")

            self.assertIn("--require-schema-manifest", cmd)
            self.assertIn("--require-id-compaction", cmd)
            self.assertIn("--strict-schema-manifest", cmd)

    def test_literal_marker_columns_are_passed_to_validator(self):
        with TemporaryDirectory() as td:
            run_dir = Path(td)
            plan = self._plan(run_dir)
            plan["validation"]["literal_marker"] = {"mode": "columns", "columns": ["title", "abstract"]}
            spec = parquet_reload.normalize_table_specs(plan)[0]

            validate = parquet_reload.validation_cmd(plan, spec, report_path=run_dir / "validate.json")

            self.assertEqual(validate.count("--literal-null-marker-column"), 2)
            self.assertIn("title", validate)
            self.assertIn("abstract", validate)

    def test_nested_db_name_is_accepted_for_compatibility(self):
        with TemporaryDirectory() as td:
            run_dir = Path(td)
            plan = self._plan(run_dir)
            del plan["db_name"]
            plan["db"] = {"name": "nested_db"}

            parquet_reload.validate_plan(plan)
            specs = parquet_reload.normalize_table_specs(plan)
            cmd = parquet_reload.materialize_cmd(plan, specs[0], report_path=run_dir / "report.json")

            self.assertEqual(parquet_reload.db_name_from_plan(plan), "nested_db")
            self.assertEqual(parquet_finalize.db_name_from_plan(plan), "nested_db")
            self.assertIn("nested_db", cmd)

    def test_config_db_name_is_used_when_plan_only_sets_driver(self):
        with TemporaryDirectory() as td:
            run_dir = Path(td)
            (run_dir / "config.json").write_text(
                json.dumps({"db_config": {"database": "config_db"}}),
                encoding="utf-8",
            )
            plan = self._plan(run_dir)
            del plan["db_name"]
            plan["db"] = {"driver": "mariadb"}

            parquet_reload.validate_plan(plan)

            self.assertEqual(parquet_reload.db_name_from_plan(plan), "config_db")

    def test_validation_report_with_expected_source_literal_marker_is_accepted(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "validate.json"
            path.write_text(
                json.dumps(
                    {
                        "status": "done",
                        "issues": [],
                        "checks": {
                            "tables": {
                                "works": {
                                    "status": "ok",
                                    "parquet_rows": 1,
                                    "db_rows": 1,
                                    "row_count_match": True,
                                    "literal_null_marker_scan": {
                                        "status": "ok",
                                        "nonzero_columns": {"title": 1},
                                    },
                                    "literal_null_marker_comparison": {
                                        "columns": {
                                            "title": {
                                                "status": "expected_source_literal",
                                                "source_count": 1,
                                            }
                                        }
                                    },
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            result = parquet_reload.verify_validation_report("works", path)

            self.assertEqual(result["validation_status"], "done")
            self.assertEqual(result["literal_marker_nonzero_columns"], {"title": 1})

    def test_mark_table_done_from_validation_report_clears_failed_status(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            status_path = root / "status.json"
            validation_path = root / "validate.json"
            status_path.write_text(
                json.dumps(
                    {
                        "status": "failed",
                        "completed": [],
                        "tables": {"works": {"status": "validating"}},
                        "failed_at": "then",
                        "error": "old",
                    }
                ),
                encoding="utf-8",
            )
            validation_path.write_text(
                json.dumps(
                    {
                        "status": "done",
                        "issues": [],
                        "checks": {
                            "tables": {
                                "works": {
                                    "status": "ok",
                                    "parquet_rows": 2,
                                    "db_rows": 2,
                                    "row_count_match": True,
                                    "literal_null_marker_scan": {"status": "ok", "nonzero_columns": {}},
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            result = parquet_reload.mark_table_done_from_validation_report(
                status_path=status_path,
                table="works",
                validation_report=validation_path,
            )

            self.assertEqual(result["status"], "running")
            self.assertIn("works", result["completed"])
            self.assertEqual(result["tables"]["works"]["status"], "done")
            self.assertNotIn("error", result)

    def test_mark_table_done_does_not_follow_status_symlink(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            external = root / "external_status.json"
            status_path = root / "status.json"
            validation_path = root / "validate.json"
            external.write_text(
                json.dumps(
                    {
                        "status": "failed",
                        "completed": [],
                        "tables": {"works": {"status": "validating"}},
                    }
                ),
                encoding="utf-8",
            )
            status_path.symlink_to(external)
            validation_path.write_text(
                json.dumps(
                    {
                        "status": "done",
                        "issues": [],
                        "checks": {
                            "tables": {
                                "works": {
                                    "status": "ok",
                                    "parquet_rows": 2,
                                    "db_rows": 2,
                                    "row_count_match": True,
                                    "literal_null_marker_scan": {"status": "ok", "nonzero_columns": {}},
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "not a safe file|symlink"):
                parquet_reload.mark_table_done_from_validation_report(
                    status_path=status_path,
                    table="works",
                    validation_report=validation_path,
                )

            self.assertTrue(status_path.is_symlink())
            self.assertEqual(json.loads(external.read_text(encoding="utf-8"))["status"], "failed")

    def test_acquire_lock_does_not_follow_lock_symlink(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            external = root / "external.lock"
            lock_path = root / "reload.lock"
            external.write_text("keep", encoding="utf-8")
            lock_path.symlink_to(external)

            with self.assertRaisesRegex(RuntimeError, "not a safe file|symlink"):
                parquet_reload.acquire_lock(lock_path)

            self.assertTrue(lock_path.is_symlink())
            self.assertEqual(external.read_text(encoding="utf-8"), "keep")

    def test_finalize_index_normalization(self):
        plan = self._plan(Path("/tmp/run"))
        indexes = parquet_finalize.normalize_indexes(plan)

        self.assertEqual(indexes[0]["table"], "works")
        self.assertEqual(indexes[0]["index_name"], "idx_works_id")
        self.assertEqual(indexes[0]["columns"], [("id", 64)])
        self.assertEqual(parquet_finalize.column_sql(indexes[0]["columns"]), "`id`(64)")

    def test_invalid_duplicate_table_is_rejected(self):
        plan = self._plan(Path("/tmp/run"))
        plan["tables"].append({"name": "works"})

        with self.assertRaises(parquet_reload.ReloadPlanError):
            parquet_reload.validate_plan(plan)

    def test_all_planned_tables_done_reports_missing(self):
        plan = self._plan(Path("/tmp/run"))
        plan["tables"].append({"name": "works_authorships", "chunk_rows": 100})
        specs = parquet_reload.normalize_table_specs(plan)

        complete, missing = parquet_reload.all_planned_tables_done(
            {"completed": ["works"], "tables": {"works": {"status": "done"}}},
            specs,
        )

        self.assertFalse(complete)
        self.assertEqual(missing, ["works_authorships"])

    def test_nonstrict_finalizer_allows_index_errors(self):
        with TemporaryDirectory() as td:
            run_dir = Path(td)
            plan = self._plan(run_dir)
            plan["finalize"]["strict_indexes"] = False
            report = run_dir / "finalize.json"
            report.write_text(
                json.dumps(
                    {
                        "status": "done_with_index_errors",
                        "indexes": [
                            {"table": "works", "index_name": "idx_works_id", "status": "mismatch"}
                        ],
                    }
                ),
                encoding="utf-8",
            )

            result = parquet_reload.verify_finalizer(plan, report)

            self.assertEqual(result["status"], "done_with_index_errors")
            self.assertEqual(result["missing_success_entries"], [("works", "idx_works_id")])

    def test_strict_finalizer_rejects_index_errors(self):
        with TemporaryDirectory() as td:
            run_dir = Path(td)
            plan = self._plan(run_dir)
            report = run_dir / "finalize.json"
            report.write_text(
                json.dumps(
                    {
                        "status": "done_with_index_errors",
                        "indexes": [
                            {"table": "works", "index_name": "idx_works_id", "status": "mismatch"}
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaises(parquet_reload.ReloadStepFailed):
                parquet_reload.verify_finalizer(plan, report)


if __name__ == "__main__":
    unittest.main()
