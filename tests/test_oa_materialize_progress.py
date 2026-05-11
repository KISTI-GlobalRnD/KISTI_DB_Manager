import importlib.util
import threading
import unittest
from unittest import mock
from pathlib import Path
from tempfile import TemporaryDirectory


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "oa_materialize_parquet_to_db.py"
SPEC = importlib.util.spec_from_file_location("oa_materialize_parquet_to_db", SCRIPT_PATH)
oa_materialize = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(oa_materialize)


class TestOaMaterializeProgress(unittest.TestCase):
    def test_load_data_preflight_runs_only_for_duckdb_load_data_path(self):
        self.assertTrue(
            oa_materialize._should_run_load_data_preflight(
                load_method="load_data",
                staging_writer="duckdb",
                skip_preflight=False,
            )
        )
        self.assertTrue(
            oa_materialize._should_run_load_data_preflight(
                load_method="auto",
                staging_writer="duckdb",
                skip_preflight=False,
            )
        )
        self.assertFalse(
            oa_materialize._should_run_load_data_preflight(
                load_method="to_sql",
                staging_writer="duckdb",
                skip_preflight=False,
            )
        )
        self.assertFalse(
            oa_materialize._should_run_load_data_preflight(
                load_method="load_data",
                staging_writer="python",
                skip_preflight=False,
            )
        )
        self.assertFalse(
            oa_materialize._should_run_load_data_preflight(
                load_method="load_data",
                staging_writer="duckdb",
                skip_preflight=True,
            )
        )

    def test_drop_confirmation_requires_exact_target_table_list(self):
        with self.assertRaises(SystemExit) as ctx:
            oa_materialize._require_drop_confirmation(["works"], "")
        self.assertIn("--confirm-drop-tables 'works'", str(ctx.exception))

        token = oa_materialize._require_drop_confirmation(
            ["prefix_authors", "prefix_works"],
            "prefix_authors,prefix_works",
        )
        self.assertEqual(token, "prefix_authors,prefix_works")

    def test_pick_table_dirs_rejects_missing_selected_table(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            (root / "works").mkdir()

            with self.assertRaises(SystemExit) as ctx:
                oa_materialize._pick_table_dirs(root, ["works", "missing_table"], None)

        self.assertIn("missing_table", str(ctx.exception))
        self.assertIn("works", str(ctx.exception))

    def test_parquet_preflight_uses_footer_metadata_and_union_schema(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pyarrow is required: {exc}")

        with TemporaryDirectory() as td:
            root = Path(td)
            works_dir = root / "works"
            works_dir.mkdir()
            pq.write_table(pa.table({"id": ["W1", "W2"], "title": ["a", "b"]}), works_dir / "part-0.parquet")
            pq.write_table(pa.table({"id": ["W3"], "doi": ["10/x"]}), works_dir / "part-1.parquet")

            result = oa_materialize._inspect_parquet_tables(
                {"works": sorted(works_dir.glob("*.parquet"))},
                extra_column_name="__extra__",
            )

        table = result["tables"]["works"]
        self.assertEqual(result["status"], "ok")
        self.assertEqual(table["file_count"], 2)
        self.assertEqual(table["rows_total"], 3)
        self.assertEqual(table["schema_variant_count"], 2)
        self.assertEqual(table["union_columns"], ["id", "title", "doi", "__extra__"])

    def test_materialize_table_creates_union_schema_from_preflight(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            file0 = root / "part-0.parquet"
            file1 = root / "part-1.parquet"
            file0.touch()
            file1.touch()
            created_columns: list[list[str]] = []

            def fake_create_table(db_config, *, table_name, columns, name_map, key_sep, column_type):
                created_columns.append(list(columns))
                return oa_materialize.NameMap.build(table_name=table_name, columns=columns, key_sep=key_sep, max_len=64)

            with mock.patch.object(oa_materialize.manage, "create_table_from_columns", side_effect=fake_create_table):
                with mock.patch.object(
                    oa_materialize,
                    "_materialize_one_file",
                    return_value={
                        "stats": {},
                        "timings_ms": {},
                        "errors": [],
                        "files": [{"path": str(file0), "rows": 1}],
                    },
                ) as materialize_file:
                    result = oa_materialize._materialize_one_table(
                        table_original="works",
                        files=[file0, file1],
                        table_preflight={"union_columns": ["id", "title", "doi", "__extra__"]},
                        completed_files=set(),
                        data_config={"KEY_SEP": "__", "extra_column_name": "__extra__"},
                        db_config={},
                        load_method="load_data",
                        limit_rows_per_file=0,
                        table_prefix="",
                        progress_path=root / "progress.json",
                        state={"completed_files": {}, "partial_files": {}},
                        state_lock=threading.Lock(),
                        keep_going=False,
                        load_data_staging_writer="duckdb",
                        load_data_staging_dir=str(root),
                        file_chunk_rows=0,
                        parallel_files_per_table=1,
                    )

            self.assertEqual(created_columns, [["id", "title", "doi", "__extra__"]])
            self.assertEqual(materialize_file.call_count, 2)
            self.assertEqual(result["table_sql"], "works")

    def test_pending_reset_blocks_resume_without_reset_confirmation(self):
        state = {
            "reset": {
                "status": "pending",
                "target_tables": ["works", "works_authorships"],
            }
        }

        with self.assertRaises(SystemExit) as ctx:
            oa_materialize._require_no_pending_reset(state, progress_path=Path("/tmp/progress.json"))

        self.assertIn("--confirm-drop-tables 'works,works_authorships'", str(ctx.exception))

    def test_reset_progress_status_helpers_record_pending_and_completed(self):
        state = {}

        oa_materialize._progress_mark_reset_pending(state, target_tables=["works"])
        self.assertEqual(state["reset"]["status"], "pending")
        self.assertEqual(state["reset"]["target_tables"], ["works"])

        oa_materialize._progress_mark_reset_completed(state)
        self.assertEqual(state["reset"]["status"], "completed")
        self.assertIn("completed_at_utc", state["reset"])

    def test_prepare_session_clears_stale_active_and_scopes_counts(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            table_dir = root / "works_affiliation_agg"
            table_dir.mkdir()
            file0 = table_dir / "part-bucket-0000-0.parquet"
            file1 = table_dir / "part-bucket-0001-0.parquet"
            file0.touch()
            file1.touch()

            state = {
                "active": {
                    "works:part-1.parquet": {
                        "table_original": "works",
                        "file_path": "/old/works/part-1.parquet",
                    }
                },
                "current": {"table_original": "works"},
                "completed_files": {
                    "works": ["part-0.parquet"],
                    "works_affiliation_agg": ["part-bucket-0000-0.parquet", "missing.parquet"],
                },
                "partial_files": {
                    "works_affiliation_agg": {
                        "part-bucket-0001-0.parquet": {"next_offset": 10},
                        "gone.parquet": {"next_offset": 20},
                    }
                },
                "files_loaded": 999,
                "rows_loaded": 999,
            }

            oa_materialize._progress_prepare_session(
                state,
                parquet_root=root,
                table_files={"works_affiliation_agg": [file0, file1]},
                selected_tables=["works_affiliation_agg"],
            )

            self.assertEqual(state["active"], {})
            self.assertIsNone(state["current"])
            self.assertEqual(state["table_count"], 1)
            self.assertEqual(state["table_file_counts"], {"works_affiliation_agg": 2})
            self.assertEqual(state["completed_files"]["works_affiliation_agg"], ["part-bucket-0000-0.parquet"])
            self.assertEqual(list(state["partial_files"]["works_affiliation_agg"]), ["part-bucket-0001-0.parquet"])
            self.assertEqual(state["files_completed"], 1)
            self.assertEqual(state["files_loaded"], 1)
            self.assertEqual(state["partial_rows_before_session"], 10)
            self.assertEqual(state["rows_loaded"], 10)
            self.assertEqual(state["files_loaded_session"], 0)
            self.assertEqual(state["rows_loaded_session"], 0)
            self.assertEqual(state["stale_active_history"][-1]["entries"]["works:part-1.parquet"]["table_original"], "works")

    def test_mark_chunk_done_updates_session_totals(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            table_dir = root / "works_affiliation_agg"
            table_dir.mkdir()
            file0 = table_dir / "part-bucket-0000-0.parquet"
            file1 = table_dir / "part-bucket-0001-0.parquet"
            file0.touch()
            file1.touch()
            progress_path = root / "progress.json"
            lock = threading.Lock()
            state = {
                "completed_files": {"works_affiliation_agg": ["part-bucket-0000-0.parquet"]},
                "partial_files": {},
            }
            oa_materialize._progress_prepare_session(
                state,
                parquet_root=root,
                table_files={"works_affiliation_agg": [file0, file1]},
                selected_tables=["works_affiliation_agg"],
            )
            state["active"] = {"works_affiliation_agg:part-bucket-0001-0.parquet": {"table_original": "works_affiliation_agg"}}

            oa_materialize._progress_mark_chunk_done(
                progress_path,
                state,
                lock,
                active_key="works_affiliation_agg:part-bucket-0001-0.parquet",
                table_original="works_affiliation_agg",
                parquet_file=file1,
                rows=25,
                next_offset=25,
                total_rows=25,
                chunk_rows=25,
                file_complete=True,
            )

            self.assertEqual(state["completed_files"]["works_affiliation_agg"], [
                "part-bucket-0000-0.parquet",
                "part-bucket-0001-0.parquet",
            ])
            self.assertEqual(state["files_completed"], 2)
            self.assertEqual(state["tables_completed"], 1)
            self.assertEqual(state["files_loaded_session"], 1)
            self.assertEqual(state["rows_loaded_session"], 25)
            self.assertIsNone(state["current"])

    def test_mark_chunk_done_completes_zero_row_file(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            table_dir = root / "works"
            table_dir.mkdir()
            file0 = table_dir / "empty.parquet"
            file0.touch()
            progress_path = root / "progress.json"
            lock = threading.Lock()
            state = {"completed_files": {}, "partial_files": {}}
            oa_materialize._progress_prepare_session(
                state,
                parquet_root=root,
                table_files={"works": [file0]},
                selected_tables=["works"],
            )
            state["active"] = {"works:empty.parquet": {"table_original": "works"}}

            oa_materialize._progress_mark_chunk_done(
                progress_path,
                state,
                lock,
                active_key="works:empty.parquet",
                table_original="works",
                parquet_file=file0,
                rows=0,
                next_offset=0,
                total_rows=0,
                chunk_rows=0,
                file_complete=True,
            )

            self.assertEqual(state["completed_files"]["works"], ["empty.parquet"])
            self.assertEqual(state["files_completed"], 1)
            self.assertEqual(state["tables_completed"], 1)
            self.assertEqual(state["files_loaded_session"], 1)
            self.assertEqual(state["rows_loaded_session"], 0)
            self.assertIsNone(state["current"])

    def test_prepare_session_reset_selected_table_clears_resume_state(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            works_dir = root / "works"
            works_dir.mkdir()
            file0 = works_dir / "part-0000.parquet"
            file1 = works_dir / "part-0001.parquet"
            file0.touch()
            file1.touch()

            state = {
                "completed_files": {
                    "works": ["part-0000.parquet", "old.parquet"],
                    "authors": ["part-authors.parquet"],
                },
                "partial_files": {
                    "works": {
                        "part-0001.parquet": {"next_offset": 100},
                    },
                },
            }

            oa_materialize._progress_prepare_session(
                state,
                parquet_root=root,
                table_files={"works": [file0, file1]},
                selected_tables=["works"],
                reset_tables={"works"},
            )

            self.assertEqual(state["completed_files"]["works"], [])
            self.assertNotIn("works", state["partial_files"])
            self.assertEqual(state["completed_files"]["authors"], ["part-authors.parquet"])
            self.assertEqual(state["files_completed"], 0)
            self.assertEqual(state["files_loaded"], 0)
            self.assertEqual(state["partial_rows_before_session"], 0)
            self.assertEqual(state["rows_loaded"], 0)
            self.assertEqual(state["progress_prune_history"][-1]["completed_files"]["works"]["count"], 2)
            self.assertEqual(state["progress_prune_history"][-1]["partial_files"]["works"]["count"], 1)


if __name__ == "__main__":
    unittest.main()
