import json
import unittest
from argparse import Namespace
from pathlib import Path
from tempfile import TemporaryDirectory

from KISTI_DB_Manager import openalex_reload_validate as oa_validate


class TestOaValidateServingReload(unittest.TestCase):
    def _base_args(self, **overrides):
        values = {
            "table": ["works"],
            "works_table": "works",
            "key_column": "id",
            "key_pattern": oa_validate.DEFAULT_KEY_PATTERN,
            "prefix_length": 64,
            "sample_limit": 10,
            "literal_null_marker": r"\N",
            "literal_null_marker_compare_mode": "utf8mb4_bin",
            "literal_null_marker_count_mode": "count",
            "literal_null_marker_column": [],
            "literal_null_marker_column_chunk_size": 32,
            "skip_literal_null_marker_scan": False,
            "skip_source_literal_null_marker_scan": False,
            "skip_parquet_key_health": False,
            "skip_db_key_health": False,
            "skip_samples": False,
            "skip_prefix_collision_sample": False,
            "skip_key_bucket_check": False,
            "key_bucket_prefix_length": 1,
            "skip_orphans": False,
            "skip_sample_checksum": False,
            "checksum_table": [],
            "checksum_column": [],
            "checksum_sample_size": 1000,
            "skip_row_bucket_checksum": False,
            "row_bucket_checksum_table": [],
            "row_bucket_checksum_all_tables": False,
            "row_bucket_checksum_column": [],
            "row_bucket_prefix_length": 1,
        }
        values.update(overrides)
        return Namespace(**values)

    def test_load_table_specs_from_file_and_filter(self):
        with TemporaryDirectory() as td:
            run_dir = Path(td)
            specs_path = run_dir / "table_specs.json"
            specs_path.write_text(
                json.dumps(
                    {
                        "specs": [
                            {"target_table": "works", "source_table": "source_works", "source_dir": "/data/works"},
                            {"target_table": "works_authorships", "source_dir": "/data/authorships"},
                            {"target_table": "", "source_dir": "/ignore"},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            specs = oa_validate._load_table_specs(run_dir, None)
            filtered = oa_validate._filter_specs(specs, {"works"})

            self.assertEqual([spec["target_table"] for spec in specs], ["works", "works_authorships"])
            self.assertEqual([spec["target_table"] for spec in filtered], ["works"])

    def test_filter_specs_rejects_missing_table(self):
        with self.assertRaises(SystemExit) as ctx:
            oa_validate._filter_specs([{"target_table": "works", "source_dir": "/data/works"}], {"missing"})

        self.assertIn("missing", str(ctx.exception))

    def test_spec_signature_normalizes_source_dirs(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            result = oa_validate._spec_signature(
                [{"target_table": "works", "source_table": "source_works", "source_dir": str(root / ".." / root.name)}]
            )

            self.assertEqual(result[0]["target_table"], "works")
            self.assertEqual(result[0]["source_dir"], str(root.resolve()))

    def test_validation_options_tracks_resume_sensitive_flags(self):
        self.assertNotEqual(
            oa_validate._validation_options(self._base_args()),
            oa_validate._validation_options(self._base_args(key_bucket_prefix_length=2)),
        )

    def test_reusable_previous_check_prunes_skipped_works_key_subchecks(self):
        previous_options = oa_validate._validation_options(self._base_args())
        current_options = oa_validate._validation_options(
            self._base_args(skip_samples=True, skip_prefix_collision_sample=True)
        )
        previous_checks = {
            "works_key": {
                "parquet": {
                    "status": "ok",
                    "summary": {"status": "ok", "rows": [[2, 2, 0, 0, 0, 0, 2]]},
                    "duplicate_key_file_sample": {"status": "error", "error": "oom"},
                    "prefix_collision_sample": {"status": "ok", "rows": []},
                }
            }
        }

        reused = oa_validate._reusable_previous_check(
            previous_checks,
            section="works_key",
            key="parquet",
            kind="works_key_parquet",
            previous_options=previous_options,
            current_options=current_options,
        )

        self.assertIsNotNone(reused)
        self.assertNotIn("duplicate_key_file_sample", reused)
        self.assertNotIn("prefix_collision_sample", reused)

    def test_reusable_previous_check_prunes_duplicate_samples_when_summary_has_no_duplicates(self):
        previous_options = oa_validate._validation_options(self._base_args())
        current_options = oa_validate._validation_options(self._base_args(skip_prefix_collision_sample=True))
        previous_checks = {
            "works_key": {
                "parquet": {
                    "status": "ok",
                    "summary": {"status": "ok", "rows": [[2, 2, 0, 0, 0, 0, 2]]},
                    "duplicate_key_sample": {"status": "ok", "rows": []},
                    "duplicate_key_file_sample": {"status": "error", "error": "oom"},
                    "prefix_collision_sample": {"status": "ok", "rows": []},
                }
            }
        }

        reused = oa_validate._reusable_previous_check(
            previous_checks,
            section="works_key",
            key="parquet",
            kind="works_key_parquet",
            previous_options=previous_options,
            current_options=current_options,
        )

        self.assertIsNotNone(reused)
        self.assertNotIn("duplicate_key_sample", reused)
        self.assertNotIn("duplicate_key_file_sample", reused)
        self.assertNotIn("prefix_collision_sample", reused)

    def test_reusable_previous_table_check_prunes_literal_scan_when_skipped(self):
        previous_options = oa_validate._validation_options(self._base_args())
        current_options = oa_validate._validation_options(self._base_args(skip_literal_null_marker_scan=True))
        previous_checks = {
            "tables": {
                "works": {
                    "status": "ok",
                    "row_count_match": True,
                    "literal_null_marker_scan": {"status": "ok", "nonzero_columns": {"title": 1}},
                }
            }
        }

        reused = oa_validate._reusable_previous_check(
            previous_checks,
            section="tables",
            key="works",
            kind="table",
            previous_options=previous_options,
            current_options=current_options,
        )

        self.assertIsNotNone(reused)
        self.assertNotIn("literal_null_marker_scan", reused)

    def test_parquet_row_count_uses_footer_metadata(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pyarrow is required: {exc}")

        with TemporaryDirectory() as td:
            root = Path(td)
            pq.write_table(pa.table({"id": ["W1", "W2"]}), root / "part-0.parquet")
            pq.write_table(pa.table({"id": ["W3"]}), root / "part-1.parquet")

            result = oa_validate._parquet_row_count(root)

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["file_count"], 2)
            self.assertEqual(result["rows"], 3)

    def test_parquet_columns_unions_all_file_schemas(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pyarrow is required: {exc}")

        with TemporaryDirectory() as td:
            root = Path(td)
            pq.write_table(pa.table({"id": ["W1"], "title": ["a"]}), root / "part-0.parquet")
            pq.write_table(pa.table({"id": ["W2"], "doi": ["10/x"]}), root / "part-1.parquet")

            self.assertEqual(oa_validate._parquet_columns(root), ["id", "title", "doi"])

    def test_duckdb_works_key_health_counts_bad_keys(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            import duckdb  # noqa: F401
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pyarrow and duckdb are required: {exc}")

        with TemporaryDirectory() as td:
            root = Path(td)
            pq.write_table(
                pa.table(
                    {
                        "id": [
                            "https://openalex.org/W1",
                            None,
                            "NULL",
                            "",
                            "https://openalex.org/W-1",
                        ]
                    }
                ),
                root / "part-0.parquet",
            )

            result = oa_validate._duckdb_works_key_health(
                root,
                key_column="id",
                key_pattern=oa_validate.DEFAULT_KEY_PATTERN,
                prefix_length=64,
                sample_limit=10,
                threads=1,
                memory_limit="1GB",
                temp_dir=None,
                include_samples=True,
                include_prefix_collision_sample=False,
            )

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["summary"]["rows"][0], [5, 4, 1, 1, 1, 1, 4])
            bad_values = {row[0] for row in result["bad_key_sample"]["rows"]}
            self.assertIn("NULL", bad_values)
            self.assertIn("https://openalex.org/W-1", bad_values)
            self.assertNotIn("duplicate_key_sample", result)
            self.assertNotIn("duplicate_key_file_sample", result)

    def test_duckdb_works_key_health_reports_cross_file_duplicates(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            import duckdb  # noqa: F401
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pyarrow and duckdb are required: {exc}")

        with TemporaryDirectory() as td:
            root = Path(td)
            pq.write_table(pa.table({"id": ["https://openalex.org/W1"]}), root / "part-0.parquet")
            pq.write_table(pa.table({"id": ["https://openalex.org/W1"]}), root / "part-1.parquet")

            result = oa_validate._duckdb_works_key_health(
                root,
                key_column="id",
                key_pattern=oa_validate.DEFAULT_KEY_PATTERN,
                prefix_length=64,
                sample_limit=10,
                threads=1,
                memory_limit="1GB",
                temp_dir=None,
                include_samples=True,
                include_prefix_collision_sample=False,
            )

            self.assertEqual(result["status"], "ok")
            duplicate_rows = result["duplicate_key_file_sample"]["rows"]
            self.assertEqual(duplicate_rows[0][0], "https://openalex.org/W1")
            self.assertEqual(duplicate_rows[0][1], 2)
            self.assertEqual(duplicate_rows[0][2], 2)

    def test_duckdb_works_key_health_rejects_missing_source_dir(self):
        try:
            import duckdb  # noqa: F401
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"duckdb is required: {exc}")

        with TemporaryDirectory() as td:
            result = oa_validate._duckdb_works_key_health(
                Path(td) / "missing",
                key_column="id",
                key_pattern=oa_validate.DEFAULT_KEY_PATTERN,
                prefix_length=64,
                sample_limit=10,
                threads=1,
                memory_limit="1GB",
                temp_dir=None,
                include_samples=True,
                include_prefix_collision_sample=False,
            )

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["summary"]["error_type"], "FileNotFoundError")

    def test_db_literal_marker_scan_uses_binary_collation_comparison(self):
        class FakeCursor:
            def __init__(self):
                self.queries = []
                self._rows = []
                self._one = None

            def execute(self, sql, params=()):
                self.queries.append((sql, params))
                if "data_type IN" in sql:
                    self._rows = [("title",)]
                    self._one = None
                elif "SUM(CASE WHEN" in sql:
                    self.assert_binary(sql)
                    self._one = (1,)
                    self._rows = []
                elif "SELECT 1" in sql and "information_schema.columns" in sql:
                    self._one = (1,)
                    self._rows = []
                elif "AS marker_value" in sql:
                    self.assert_binary(sql)
                    self._rows = [[r"\N", "https://openalex.org/W1"]]
                    self._one = None
                else:
                    raise AssertionError(f"unexpected SQL: {sql}")

            @staticmethod
            def assert_binary(sql):
                if "`title` COLLATE utf8mb4_bin = %s" not in sql:
                    raise AssertionError(sql)

            def fetchone(self):
                return self._one

            def fetchall(self):
                return self._rows

        result = oa_validate._db_literal_marker_scan(
            FakeCursor(),
            table="works",
            marker=r"\N",
            sample_limit=10,
            chunk_size=32,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["comparison_mode"], "utf8mb4_bin")
        self.assertEqual(result["nonzero_columns"], {"title": 1})
        self.assertEqual(result["samples"]["title"], [[r"\N", "https://openalex.org/W1"]])

    def test_pick_checksum_columns_uses_defaults_or_requested(self):
        selected, missing = oa_validate._pick_checksum_columns(
            requested_columns=[],
            parquet_columns=["id", "title", "only_parquet"],
            db_columns=["id", "title", "only_db"],
        )
        self.assertEqual(selected, ["id", "title"])
        self.assertEqual(missing, [])

        selected, missing = oa_validate._pick_checksum_columns(
            requested_columns=["id", "missing"],
            parquet_columns=["id"],
            db_columns=["id"],
        )
        self.assertEqual(selected, ["id"])
        self.assertEqual(missing, ["missing"])

    def test_compare_bucket_rows_reports_mismatch(self):
        result = oa_validate._compare_bucket_rows(
            [["a", 2, "10", "20"], ["b", 1, "3", "4"]],
            [["a", 2, "10", "20"], ["b", 1, "3", "5"]],
            sample_limit=10,
        )

        self.assertFalse(result["match"])
        self.assertEqual(result["mismatch_count_sampled"], 1)
        self.assertEqual(result["mismatches"][0]["bucket"], "b")

    def test_duckdb_row_bucket_checksum_summary_is_row_order_independent(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            import duckdb  # noqa: F401
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pyarrow and duckdb are required: {exc}")

        with TemporaryDirectory() as td:
            root = Path(td)
            pq.write_table(pa.table({"id": ["W2", "W1"], "value": ["b", "a"]}), root / "part-0.parquet")
            result = oa_validate._duckdb_row_bucket_checksum_summary(
                root,
                columns=["id", "value"],
                bucket_prefix_length=1,
                threads=1,
                memory_limit="1GB",
                temp_dir=None,
            )

            self.assertEqual(result["status"], "ok")
            self.assertEqual(sum(row[1] for row in result["rows"]), 2)

    def test_compare_checksum_rows_reports_mismatch(self):
        result = oa_validate._compare_checksum_rows(
            [["W1", "aaa"], ["W2", "bbb"]],
            [["W1", "aaa"], ["W2", "ccc"]],
            sample_limit=10,
        )

        self.assertFalse(result["match"])
        self.assertEqual(result["mismatch_count_sampled"], 1)
        self.assertEqual(result["mismatches"][0]["index"], 1)

    def test_collect_issues_flags_mismatches_and_bad_keys(self):
        report = {
            "checks": {
                "tables": {
                    "works": {
                        "status": "ok",
                        "parquet_rows": 3,
                        "db_rows": 2,
                        "row_count_match": False,
                        "literal_null_marker_scan": {
                            "status": "ok",
                            "marker": r"\N",
                            "nonzero_columns": {"doi": 2},
                        },
                    }
                },
                "works_key": {
                    "db": {
                        "summary": {
                            "status": "ok",
                            "rows": [[3, 2, 1, 0, 0, 1, 1]],
                        }
                    },
                    "parquet": {
                        "status": "ok",
                        "summary": {"status": "ok", "rows": [[3, 3, 0, 0, 0, 0, 3]]},
                        "prefix_collision_sample": {"status": "error", "error": "timed out"},
                        "duplicate_key_file_sample": {"status": "ok", "rows": [["https://openalex.org/W1", 2, 2, ["a", "b"]]]},
                    }
                },
                "orphan_checks": {
                    "works_authorships": {
                        "child_key_summary": {"status": "ok", "rows": [[5, 0, 1, 0]]},
                        "orphans": {"status": "ok", "rows": [[2]]},
                    },
                    "works_locations": {
                        "status": "missing_key_column",
                    }
                },
                "sample_checksums": {
                    "works": {
                        "status": "ok",
                        "missing_requested_columns": ["title"],
                        "match": False,
                        "comparison": {
                            "mismatch_count_sampled": 1,
                            "parquet_sample_rows": 2,
                            "db_sample_rows": 2,
                        },
                    }
                },
                "key_buckets": {
                    "works": {
                        "status": "ok",
                        "match": False,
                        "comparison": {
                            "mismatch_count_sampled": 1,
                            "parquet_bucket_count": 16,
                            "db_bucket_count": 16,
                        },
                    }
                },
                "row_bucket_checksums": {
                    "works_authorships": {
                        "status": "ok",
                        "match": False,
                        "comparison": {
                            "mismatch_count_sampled": 1,
                            "parquet_bucket_count": 16,
                            "db_bucket_count": 16,
                        },
                    }
                },
            }
        }

        issues = oa_validate._collect_issues(report)
        issue_keys = {(issue.get("table"), issue.get("check")) for issue in issues}

        self.assertIn(("works", "row_count_match"), issue_keys)
        self.assertIn(("works", "literal_null_marker_rows"), issue_keys)
        self.assertIn(("works", "key_null_rows"), issue_keys)
        self.assertIn(("works", "key_malformed_rows"), issue_keys)
        self.assertIn(("works", "duplicate_key_rows"), issue_keys)
        self.assertIn(("works", "rows_total_equals_rows_with_key"), issue_keys)
        self.assertIn(("works", "prefix_collision_sample_status"), issue_keys)
        self.assertIn(("works_authorships", "child_key_literal_null_rows"), issue_keys)
        self.assertIn(("works_authorships", "orphan_rows"), issue_keys)
        self.assertIn(("works_locations", "orphan_check_status"), issue_keys)
        self.assertIn(("works", "sample_checksum_missing_requested_columns"), issue_keys)
        self.assertIn(("works", "sample_checksum_mismatch"), issue_keys)
        self.assertIn(("works", "duplicate_key_file_sample"), issue_keys)
        self.assertIn(("works", "key_bucket_mismatch"), issue_keys)
        self.assertIn(("works_authorships", "row_bucket_checksum_mismatch"), issue_keys)

    def test_collect_issues_flags_missing_works_key_status(self):
        report = {
            "checks": {
                "tables": {},
                "works_key": {
                    "db": {"status": "missing_key_column"},
                },
                "orphan_checks": {},
            }
        }

        issues = oa_validate._collect_issues(report)

        self.assertIn(("works", "works_key_status"), {(issue.get("table"), issue.get("check")) for issue in issues})


if __name__ == "__main__":
    unittest.main()
