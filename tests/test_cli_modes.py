import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from KISTI_DB_Manager.cli import main
from KISTI_DB_Manager.pipeline import JsonRunResult, TabularRunResult
from KISTI_DB_Manager.report import RunReport


class TestCLIModes(unittest.TestCase):
    def test_json_mode_ingest_fast_skips_index_optimize_by_default(self):
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
            ) as mock_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = main(["json", "run", "--config", cfg_path, "--mode", "ingest-fast", "--report", report_path])

            self.assertEqual(rc, 0)
            _args, kwargs = mock_run.call_args
            self.assertTrue(kwargs["create"])
            self.assertTrue(kwargs["load"])
            self.assertFalse(kwargs["index"])
            self.assertFalse(kwargs["optimize"])

    def test_json_mode_ingest_fast_hybrid_sets_schema_mode(self):
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
            ) as mock_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = main(["json", "run", "--config", cfg_path, "--mode", "ingest-fast-hybrid", "--report", report_path])

            self.assertEqual(rc, 0)
            run_args, _run_kwargs = mock_run.call_args
            self.assertEqual(run_args[0]["schema_mode"], "hybrid")
            self.assertEqual(run_args[0]["schema_hybrid_warmup_batches"], 1)

    def test_json_mode_ingest_fast_hybrid_allows_overriding_warmup_batches(self):
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
            ) as mock_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = main(
                        [
                            "json",
                            "run",
                            "--config",
                            cfg_path,
                            "--mode",
                            "ingest-fast-hybrid",
                            "--schema-hybrid-warmup-batches",
                            "3",
                            "--report",
                            report_path,
                        ]
                    )

            self.assertEqual(rc, 0)
            run_args, _run_kwargs = mock_run.call_args
            self.assertEqual(run_args[0]["schema_mode"], "hybrid")
            self.assertEqual(run_args[0]["schema_hybrid_warmup_batches"], 3)

    def test_json_mode_ingest_fast_allows_overriding_index(self):
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
            ) as mock_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = main(
                        [
                            "json",
                            "run",
                            "--config",
                            cfg_path,
                            "--mode",
                            "ingest-fast",
                            "--index",
                            "--report",
                            report_path,
                        ]
                    )

            self.assertEqual(rc, 0)
            _args, kwargs = mock_run.call_args
            self.assertTrue(kwargs["index"])

    def test_json_mode_ingest_fast_allows_overriding_chunk_size(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            report_path = f"{td}/report.json"
            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                    "chunk_size": 10,
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            with patch(
                "KISTI_DB_Manager.pipeline.run_json_pipeline",
                return_value=JsonRunResult(name_maps={}, report=fake_report),
            ) as mock_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = main(
                        [
                            "json",
                            "run",
                            "--config",
                            cfg_path,
                            "--mode",
                            "ingest-fast",
                            "--chunk-size",
                            "123",
                            "--report",
                            report_path,
                        ]
                    )

            self.assertEqual(rc, 0)
            run_args, run_kwargs = mock_run.call_args
            self.assertEqual(run_kwargs["chunk_size"], 123)
            self.assertEqual(run_args[0]["chunk_size"], 123)

    def test_tabular_mode_finalize_runs_index_optimize_only(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            report_path = f"{td}/report.json"
            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.csv",
                    "file_type": "csv",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            with patch(
                "KISTI_DB_Manager.pipeline.run_tabular_pipeline",
                return_value=TabularRunResult(name_map=None, report=fake_report),
            ) as mock_run, patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = main(["tabular", "run", "--config", cfg_path, "--mode", "finalize", "--report", report_path])

            self.assertEqual(rc, 0)
            _args, kwargs = mock_run.call_args
            self.assertFalse(kwargs["create"])
            self.assertFalse(kwargs["load"])
            self.assertTrue(kwargs["index"])
            self.assertTrue(kwargs["optimize"])


if __name__ == "__main__":
    unittest.main()
