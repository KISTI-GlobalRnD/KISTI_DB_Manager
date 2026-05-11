import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from unittest.mock import patch

from KISTI_DB_Manager.cli import build_parser, main
from KISTI_DB_Manager.json_parallel_profile import parse_worker_list, profile_parallel, recommend_parallel_workers
from KISTI_DB_Manager.pipeline import JsonRunResult


class TestJsonParallelProfile(unittest.TestCase):
    def test_parse_worker_list_accepts_spaces_and_deduplicates(self):
        self.assertEqual(parse_worker_list("0, 2,2, 4,8"), [0, 2, 4, 8])
        self.assertEqual(parse_worker_list(None), [0, 2, 4, 8])

    def test_parse_worker_list_rejects_negative_and_invalid_values(self):
        with self.assertRaises(ValueError):
            parse_worker_list("0,-1")
        with self.assertRaises(ValueError):
            parse_worker_list("0,two")
        with self.assertRaises(ValueError):
            parse_worker_list("0,,2")

    def test_cli_parser_accepts_profile_parallel_args(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "json",
                "profile-parallel",
                "--config",
                "config.json",
                "--workers",
                "0,2",
                "--max-records",
                "100",
                "--chunk-size",
                "25",
                "--id-compaction",
                "--out",
                "runs/profile",
            ]
        )
        self.assertEqual(args.json_cmd, "profile-parallel")
        self.assertEqual(args.workers, "0,2")
        self.assertEqual(args.max_records, 100)
        self.assertEqual(args.chunk_size, 25)
        self.assertEqual(args.id_compaction, True)

    def test_cli_profile_parallel_invokes_orchestrator(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            cfg_path.write_text(
                json.dumps(
                    {
                        "data_config": {
                            "PATH": "data/",
                            "file_name": "x.jsonl",
                            "file_type": "jsonl",
                            "table_name": "tbl",
                        },
                        "db_config": {},
                    }
                ),
                encoding="utf-8",
            )
            result = {
                "status": "done",
                "summary_json_path": str(Path(td, "parallel_profile.json")),
                "summary_md_path": str(Path(td, "parallel_profile.md")),
                "recommended_parallel_workers": 2,
            }
            with patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None), patch(
                "KISTI_DB_Manager.json_parallel_profile.profile_parallel",
                return_value=result,
            ) as p_profile:
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = main(
                        [
                            "json",
                            "profile-parallel",
                            "--config",
                            str(cfg_path),
                            "--workers",
                            "0,2",
                            "--out",
                            str(Path(td, "out")),
                        ]
                    )

            self.assertEqual(rc, 0)
            self.assertIn("recommended_parallel_workers: 2", buf.getvalue())
            self.assertEqual(p_profile.call_args.kwargs["workers"], [0, 2])

    def test_cli_profile_parallel_rejects_bad_workers(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            cfg_path.write_text("{}", encoding="utf-8")
            stderr = io.StringIO()
            with patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None), redirect_stderr(stderr):
                rc = main(["json", "profile-parallel", "--config", str(cfg_path), "--workers", "0,-2"])
            self.assertEqual(rc, 2)
            self.assertIn("worker values must be non-negative", stderr.getvalue())

    def test_recommendation_policy(self):
        base = {"status": "done", "error_count": 0, "artifact_contract_status": "done"}

        fastest = recommend_parallel_workers(
            [
                {"workers": 0, "records_per_s": 100.0, **base},
                {"workers": 4, "records_per_s": 130.0, **base},
            ]
        )
        self.assertEqual(fastest["recommended_parallel_workers"], 4)

        within_five_pct = recommend_parallel_workers(
            [
                {"workers": 0, "records_per_s": 100.0, **base},
                {"workers": 8, "records_per_s": 103.0, **base},
            ]
        )
        self.assertEqual(within_five_pct["recommended_parallel_workers"], 0)

        slower_parallel = recommend_parallel_workers(
            [
                {"workers": 0, "records_per_s": 100.0, **base},
                {"workers": 2, "records_per_s": 90.0, **base},
                {"workers": 4, "records_per_s": 95.0, **base},
            ]
        )
        self.assertEqual(slower_parallel["recommended_parallel_workers"], 0)
        self.assertIn("not recommended", slower_parallel["recommendation_reason"])

        failed_excluded = recommend_parallel_workers(
            [
                {"workers": 0, "records_per_s": 500.0, "status": "failed", "error_count": 1, "artifact_contract_status": "done"},
                {"workers": 2, "records_per_s": 80.0, **base},
            ]
        )
        self.assertEqual(failed_excluded["recommended_parallel_workers"], 2)

        warnings_allowed = recommend_parallel_workers(
            [
                {"workers": 0, "records_per_s": 100.0, **base},
                {
                    "workers": 4,
                    "records_per_s": 130.0,
                    "status": "done_with_warnings",
                    "error_count": 0,
                    "warning_count": 1,
                    "artifact_contract_status": "done_with_warnings",
                },
            ]
        )
        self.assertEqual(warnings_allowed["recommended_parallel_workers"], 4)

        all_failed = recommend_parallel_workers(
            [{"workers": 0, "records_per_s": 100.0, "status": "failed", "error_count": 1, "artifact_contract_status": "failed"}]
        )
        self.assertEqual(all_failed["status"], "failed")
        self.assertIsNone(all_failed["recommended_parallel_workers"])

    def test_profile_parallel_writes_worker_artifacts_and_summary(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            out_dir = Path(td, "profile")
            cfg_path.write_text(
                json.dumps(
                    {
                        "data_config": {
                            "PATH": "data/",
                            "file_name": "x.jsonl",
                            "file_type": "jsonl",
                            "table_name": "tbl",
                        },
                        "db_config": {},
                    }
                ),
                encoding="utf-8",
            )

            def fake_run_json_pipeline(data_config, db_config, **kwargs):
                worker = int(data_config["parallel_workers"])
                duration = {0: 10.0, 2: 7.0}[worker]
                parquet_dir = Path(data_config["persist_parquet_dir"])
                parquet_dir.mkdir(parents=True, exist_ok=True)
                Path(parquet_dir, "placeholder.txt").write_text("x", encoding="utf-8")
                report = kwargs["report"]
                report.set_artifact("parallel_workers", worker)
                report.set_artifact("chunk_size", data_config.get("chunk_size"))
                report.bump("records_read", 1000)
                report.bump("records_ok", 1000)
                report.bump("parquet_files_persisted", 1)
                report.bump("parquet_rows_emitted", 1000)
                report.add_time_ms("io.json_parse", 100)
                report.add_time_ms("json.flatten", int(duration * 800))
                report.add_time_ms("json.parquet.persist", 50)
                report.add_time_ms("pipeline.json.total", int(duration * 1000))
                report.duration_s = duration
                report.finished_at = "2026-01-01T00:00:00+00:00"
                return JsonRunResult(name_maps={}, report=report)

            def fake_inspect(parquet_root, **kwargs):
                return {
                    "status": "done",
                    "parquet_root": str(parquet_root),
                    "input": dict(kwargs),
                    "summary": {},
                    "tables": {},
                    "issues": [],
                    "warnings": [],
                }

            with patch("KISTI_DB_Manager.pipeline.run_json_pipeline", side_effect=fake_run_json_pipeline), patch(
                "KISTI_DB_Manager.parquet_artifacts.inspect_parquet_artifact_contract",
                side_effect=fake_inspect,
            ):
                summary = profile_parallel(
                    config_path=cfg_path,
                    workers=[0, 2],
                    out_dir=out_dir,
                    max_records=1000,
                    chunk_size=500,
                )

            self.assertEqual(summary["recommended_parallel_workers"], 2)
            self.assertTrue(Path(out_dir, "parallel_profile.json").exists())
            self.assertTrue(Path(out_dir, "parallel_profile.md").exists())
            for worker in [0, 2]:
                self.assertTrue(Path(out_dir, f"w{worker}", "run_report.json").exists())
                self.assertTrue(Path(out_dir, f"w{worker}", "artifact_contract.json").exists())
            saved = json.loads(Path(out_dir, "parallel_profile.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["runs"][0]["timings_ms"]["json.parquet.persist"], 50)

    def test_profile_parallel_id_compaction_requires_contract_flags_and_cleanup(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            out_dir = Path(td, "profile")
            cfg_path.write_text(
                json.dumps(
                    {
                        "data_config": {
                            "PATH": "data/",
                            "file_name": "x.jsonl",
                            "file_type": "jsonl",
                            "table_name": "tbl",
                        },
                        "db_config": {},
                    }
                ),
                encoding="utf-8",
            )

            def fake_run_json_pipeline(data_config, db_config, **kwargs):
                parquet_dir = Path(data_config["persist_parquet_dir"])
                parquet_dir.mkdir(parents=True, exist_ok=True)
                Path(parquet_dir, "placeholder.txt").write_text("x", encoding="utf-8")
                report = kwargs["report"]
                report.bump("records_read", 100)
                report.add_time_ms("pipeline.json.total", 1000)
                report.duration_s = 1.0
                report.finished_at = "2026-01-01T00:00:00+00:00"
                return JsonRunResult(name_maps={}, report=report)

            inspect_calls = []

            def fake_inspect(parquet_root, **kwargs):
                inspect_calls.append((Path(parquet_root), dict(kwargs)))
                return {
                    "status": "done",
                    "parquet_root": str(parquet_root),
                    "input": dict(kwargs),
                    "summary": {},
                    "tables": {},
                    "issues": [],
                    "warnings": [],
                }

            with patch("KISTI_DB_Manager.pipeline.run_json_pipeline", side_effect=fake_run_json_pipeline), patch(
                "KISTI_DB_Manager.parquet_artifacts.inspect_parquet_artifact_contract",
                side_effect=fake_inspect,
            ):
                summary = profile_parallel(
                    config_path=cfg_path,
                    workers=[0],
                    out_dir=out_dir,
                    id_compaction=True,
                    cleanup_parquet=True,
                )

            self.assertEqual(summary["recommended_parallel_workers"], 0)
            self.assertFalse(Path(out_dir, "w0", "parquet").exists())
            self.assertTrue(Path(out_dir, "w0", "run_report.json").exists())
            self.assertTrue(Path(out_dir, "w0", "artifact_contract.json").exists())
            self.assertEqual(inspect_calls[0][1]["require_schema_manifest"], True)
            self.assertEqual(inspect_calls[0][1]["require_id_compaction"], True)


if __name__ == "__main__":
    unittest.main()
