import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from unittest.mock import patch


from KISTI_DB_Manager.cli import main
from KISTI_DB_Manager.pipeline import JsonRunResult
from KISTI_DB_Manager.report import RunReport


class TestCLIJson(unittest.TestCase):
    def test_json_run_writes_report(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            report_path = f"{td}/report.json"

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            fake_report.warn(stage="t", message="w")

            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ), patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = main(["json", "run", "--config", cfg_path, "--report", report_path])

            self.assertEqual(rc, 0)
            with open(report_path, encoding="utf-8") as f:
                saved = json.loads(f.read())
            self.assertIn("run_id", saved)
            self.assertIn("issues", saved)

    def test_json_run_passes_persist_tsv_options(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            report_path = f"{td}/report.json"

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            fake_report.warn(stage="t", message="w")

            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ) as p_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                rc = main(
                    [
                        "json",
                        "run",
                        "--config",
                        cfg_path,
                        "--mode",
                        "ingest-fast",
                        "--persist-tsv-files",
                        "--persist-tsv-dir",
                        f"{td}/tsv_out",
                        "--report",
                        report_path,
                    ]
                )

            self.assertEqual(rc, 0)
            args, kwargs = p_run.call_args
            data_cfg = args[0]
            self.assertEqual(data_cfg["persist_tsv_files"], True)
            self.assertEqual(data_cfg["persist_tsv_dir"], f"{td}/tsv_out")

    def test_json_run_parse_parquet_mode_sets_parquet_first_path(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                    "json_streaming_load": True,
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ) as p_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                rc = main(["json", "run", "--config", cfg_path, "--mode", "parse-parquet"])

            self.assertEqual(rc, 0)
            args, kwargs = p_run.call_args
            data_cfg = args[0]
            self.assertEqual(data_cfg["persist_parquet_files"], True)
            self.assertEqual(data_cfg["json_streaming_load"], False)
            self.assertEqual(data_cfg["persist_tsv_files"], False)

    def test_json_run_passes_persist_parquet_options(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            report_path = f"{td}/report.json"

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ) as p_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                rc = main(
                    [
                        "json",
                        "run",
                        "--config",
                        cfg_path,
                        "--persist-parquet-files",
                        "--persist-parquet-dir",
                        f"{td}/parquet_out",
                        "--report",
                        report_path,
                    ]
                )

            self.assertEqual(rc, 0)
            args, kwargs = p_run.call_args
            data_cfg = args[0]
            self.assertEqual(data_cfg["persist_parquet_files"], True)
            self.assertEqual(data_cfg["persist_parquet_dir"], f"{td}/parquet_out")

    def test_json_run_passes_flatten_backend_option(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ) as p_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                rc = main(["json", "run", "--config", cfg_path, "--flatten-backend", "rust-arrow"])

            self.assertEqual(rc, 0)
            args, kwargs = p_run.call_args
            data_cfg = args[0]
            self.assertEqual(data_cfg["flatten_backend"], "rust-arrow")

    def test_json_run_passes_rust_db_load_option(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ) as p_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                rc = main(["json", "run", "--config", cfg_path, "--flatten-backend", "rust-arrow", "--rust-db-load"])

            self.assertEqual(rc, 0)
            data_cfg = p_run.call_args.args[0]
            self.assertEqual(data_cfg["flatten_backend"], "rust-arrow")
            self.assertEqual(data_cfg["rust_db_load"], True)

    def test_json_run_passes_rust_raw_jsonl_parse_option(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ) as p_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                rc = main(
                    [
                        "json",
                        "run",
                        "--config",
                        cfg_path,
                        "--flatten-backend",
                        "rust-arrow",
                        "--rust-raw-jsonl-parse",
                    ]
                )

            self.assertEqual(rc, 0)
            data_cfg = p_run.call_args.args[0]
            self.assertEqual(data_cfg["flatten_backend"], "rust-arrow")
            self.assertEqual(data_cfg["rust_raw_jsonl_parse"], True)
            self.assertEqual(data_cfg["rust_raw_jsonl_file_parse"], False)

    def test_json_run_passes_rust_raw_jsonl_file_parse_option(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ) as p_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                rc = main(
                    [
                        "json",
                        "run",
                        "--config",
                        cfg_path,
                        "--flatten-backend",
                        "rust-arrow",
                        "--rust-raw-jsonl-file-parse",
                        "--rust-parallel-table-writes",
                        "--rust-columnar-accumulator",
                        "--rust-parquet-flush-records",
                        "10000",
                    ]
                )

            self.assertEqual(rc, 0)
            data_cfg = p_run.call_args.args[0]
            self.assertEqual(data_cfg["flatten_backend"], "rust-arrow")
            self.assertEqual(data_cfg["rust_raw_jsonl_file_parse"], True)
            self.assertEqual(data_cfg["rust_parallel_table_writes"], True)
            self.assertEqual(data_cfg["rust_columnar_accumulator"], True)
            self.assertEqual(data_cfg["rust_parquet_flush_records"], 10000)

    def test_json_run_accepts_rust_arrow_with_id_compaction(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ) as p_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                rc = main(
                    [
                        "json",
                        "run",
                        "--config",
                        cfg_path,
                        "--mode",
                        "parse-parquet",
                        "--flatten-backend",
                        "rust-arrow",
                        "--id-compaction",
                    ]
                )

            self.assertEqual(rc, 0)
            data_cfg = p_run.call_args.args[0]
            self.assertEqual(data_cfg["flatten_backend"], "rust-arrow")
            self.assertEqual(data_cfg["id_compaction"]["enabled"], True)

    def test_json_run_passes_id_compaction_options(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ) as p_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                rc = main(
                    [
                        "json",
                        "run",
                        "--config",
                        cfg_path,
                        "--id-compaction",
                        "--id-compaction-preset",
                        "openalex",
                        "--id-compaction-mode",
                        "semantic_column_strip",
                        "--id-compaction-collision-policy",
                        "preserve",
                        "--id-compaction-namespace-conflict-policy",
                        "error",
                    ]
                )

            self.assertEqual(rc, 0)
            args, kwargs = p_run.call_args
            data_cfg = args[0]
            self.assertEqual(data_cfg["id_compaction"]["enabled"], True)
            self.assertEqual(data_cfg["id_compaction"]["preset"], "openalex")
            self.assertEqual(data_cfg["id_compaction"]["mode"], "semantic_column_strip")
            self.assertEqual(data_cfg["id_compaction"]["collision_policy"], "preserve")
            self.assertEqual(data_cfg["id_compaction"]["namespace_conflict_policy"], "error")

    def test_json_run_rejects_invalid_id_compaction_policy_from_config(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                    "id_compaction": {
                        "enabled": True,
                        "collision_policy": "silent",
                    },
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            stderr = io.StringIO()
            with patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None), patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline"
            ) as p_run, redirect_stderr(stderr):
                rc = main(["json", "run", "--config", cfg_path])

            self.assertEqual(rc, 2)
            self.assertIn("id_compaction.collision_policy", stderr.getvalue())
            p_run.assert_not_called()

    def test_json_run_rejects_streaming_plus_parquet_conflict(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            stderr = io.StringIO()
            with patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None), patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline"
            ) as p_run, redirect_stderr(stderr):
                rc = main(
                    [
                        "json",
                        "run",
                        "--config",
                        cfg_path,
                        "--json-streaming-load",
                        "--persist-parquet-files",
                    ]
                )

            self.assertEqual(rc, 2)
            self.assertIn("persist_parquet_files=true cannot be combined with json_streaming_load=true", stderr.getvalue())
            p_run.assert_not_called()

    def test_json_run_rejects_tsv_persist_without_streaming(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            stderr = io.StringIO()
            with patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None), patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline"
            ) as p_run, redirect_stderr(stderr):
                rc = main(["json", "run", "--config", cfg_path, "--persist-tsv-files"])

            self.assertEqual(rc, 2)
            self.assertIn("persist_tsv_files=true requires json_streaming_load=true", stderr.getvalue())
            p_run.assert_not_called()

    def test_json_run_defaults_to_parquet_first(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ) as p_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                rc = main(["json", "run", "--config", cfg_path])

            self.assertEqual(rc, 0)
            args, kwargs = p_run.call_args
            data_cfg = args[0]
            self.assertEqual(data_cfg["persist_parquet_files"], True)
            self.assertEqual(data_cfg["json_streaming_load"], False)
            self.assertEqual(data_cfg["persist_tsv_files"], False)


if __name__ == "__main__":
    unittest.main()
