import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from unittest.mock import patch

from KISTI_DB_Manager.cli import build_parser, main
from KISTI_DB_Manager.json_parallel_profile import (
    _ProfileQuarantineWriter,
    _aggregate_worker_attempts,
    _assert_profile_child_path,
    _safe_remove_profile_parquet_dir,
    _safe_write_text,
    parse_worker_list,
    profile_parallel,
    recommend_parallel_workers,
)
from KISTI_DB_Manager.pipeline import JsonRunResult
from KISTI_DB_Manager.rust_arrow_backend import parse_backend_list


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

    def test_parse_backend_list_accepts_aliases_and_rejects_invalid_values(self):
        self.assertEqual(parse_backend_list("python, rust_arrow, rust"), ["python", "rust-arrow"])
        self.assertEqual(parse_backend_list(None), ["auto"])
        with self.assertRaises(ValueError):
            parse_backend_list("python,")
        with self.assertRaises(ValueError):
            parse_backend_list("gpu")

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
                "--flatten-backends",
                "python,rust-arrow",
                "--max-records",
                "100",
                "--chunk-size",
                "25",
                "--repeat",
                "3",
                "--no-shuffle-order",
                "--seed",
                "7",
                "--issue-sample-limit",
                "2",
                "--rust-raw-jsonl-parse",
                "--rust-raw-jsonl-file-parse",
                "--rust-parallel-table-writes",
                "--rust-columnar-accumulator",
                "--rust-parquet-flush-records",
                "10000",
                "--rust-parser-backend",
                "simd-json",
                "--id-compaction",
                "--out",
                "runs/profile",
            ]
        )
        self.assertEqual(args.json_cmd, "profile-parallel")
        self.assertEqual(args.workers, "0,2")
        self.assertEqual(args.flatten_backends, "python,rust-arrow")
        self.assertEqual(args.max_records, 100)
        self.assertEqual(args.chunk_size, 25)
        self.assertEqual(args.repeat, 3)
        self.assertEqual(args.shuffle_order, False)
        self.assertEqual(args.seed, 7)
        self.assertEqual(args.issue_sample_limit, 2)
        self.assertEqual(args.rust_raw_jsonl_parse, True)
        self.assertEqual(args.rust_raw_jsonl_file_parse, True)
        self.assertEqual(args.rust_parallel_table_writes, True)
        self.assertEqual(args.rust_columnar_accumulator, True)
        self.assertEqual(args.rust_parquet_flush_records, 10000)
        self.assertEqual(args.rust_parser_backend, "simd-json")
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
                            "--rust-raw-jsonl-parse",
                            "--rust-raw-jsonl-file-parse",
                            "--rust-parallel-table-writes",
                            "--rust-columnar-accumulator",
                            "--rust-parquet-flush-records",
                            "10000",
                            "--rust-parser-backend",
                            "simd-json",
                            "--out",
                            str(Path(td, "out")),
                        ]
                    )

            self.assertEqual(rc, 0)
            self.assertIn("recommended_parallel_workers: 2", buf.getvalue())
            self.assertEqual(p_profile.call_args.kwargs["workers"], [0, 2])
            self.assertEqual(p_profile.call_args.kwargs["flatten_backends"], ["auto"])
            self.assertEqual(p_profile.call_args.kwargs["rust_raw_jsonl_parse"], True)
            self.assertEqual(p_profile.call_args.kwargs["rust_raw_jsonl_file_parse"], True)
            self.assertEqual(p_profile.call_args.kwargs["rust_parallel_table_writes"], True)
            self.assertEqual(p_profile.call_args.kwargs["rust_columnar_accumulator"], True)
            self.assertEqual(p_profile.call_args.kwargs["rust_parquet_flush_records"], 10000)
            self.assertEqual(p_profile.call_args.kwargs["rust_parser_backend"], "simd-json")

    def test_cli_profile_parallel_rejects_bad_workers(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            cfg_path.write_text("{}", encoding="utf-8")
            stderr = io.StringIO()
            with patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None), redirect_stderr(stderr):
                rc = main(["json", "profile-parallel", "--config", str(cfg_path), "--workers", "0,-2"])
            self.assertEqual(rc, 2)
            self.assertIn("worker values must be non-negative", stderr.getvalue())

    def test_profile_child_path_rejects_symlink_after_missing_parent_dotdot(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td, "profile")
            root.mkdir()
            safe = root / "safe"
            safe.mkdir()
            link = root / "safe_link"
            link.symlink_to(safe, target_is_directory=True)

            with self.assertRaisesRegex(RuntimeError, "symlink"):
                _assert_profile_child_path(
                    root,
                    root / "missing_parent" / ".." / "safe_link" / "report.json",
                    purpose="profile child",
                )

    def test_safe_remove_profile_parquet_dir_rejects_unsafe_rmtree_impl(self):
        import shutil

        with tempfile.TemporaryDirectory() as td:
            root = Path(td, "profile")
            parquet_dir = root / "w0" / "parquet"
            parquet_dir.mkdir(parents=True)
            sentinel = parquet_dir / "keep.parquet"
            sentinel.write_text("keep", encoding="utf-8")

            original = getattr(shutil.rmtree, "avoids_symlink_attacks", None)
            shutil.rmtree.avoids_symlink_attacks = False
            try:
                with self.assertRaisesRegex(RuntimeError, "symlink-attack resistant"):
                    _safe_remove_profile_parquet_dir(root, parquet_dir)
            finally:
                if original is None:
                    delattr(shutil.rmtree, "avoids_symlink_attacks")
                else:
                    shutil.rmtree.avoids_symlink_attacks = original

            self.assertTrue(sentinel.exists())

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

        aggregate_failed_attempt_excluded = recommend_parallel_workers(
            [
                {
                    "workers": 0,
                    "records_per_s": 500.0,
                    "status": "done_with_warnings",
                    "error_count": 1,
                    "artifact_contract_status": "failed",
                    "eligible_attempt_count": 1,
                    "failed_attempt_count": 1,
                },
                {"workers": 2, "records_per_s": 80.0, **base, "eligible_attempt_count": 1, "failed_attempt_count": 0},
            ]
        )
        self.assertEqual(aggregate_failed_attempt_excluded["recommended_parallel_workers"], 2)

        mixed_backend_excluded = recommend_parallel_workers(
            [
                {"workers": 0, "records_per_s": 500.0, **base, "effective_backend": "mixed"},
                {"workers": 2, "records_per_s": 80.0, **base},
            ]
        )
        self.assertEqual(mixed_backend_excluded["recommended_parallel_workers"], 2)

        auto_runtime_fallback_excluded = recommend_parallel_workers(
            [
                {
                    "flatten_backend": "auto",
                    "effective_backend": "python",
                    "workers": 0,
                    "records_per_s": 500.0,
                    "rust_arrow_failed_batches": 1,
                    **base,
                },
                {"flatten_backend": "python", "effective_backend": "python", "workers": 2, "records_per_s": 80.0, **base},
            ]
        )
        self.assertEqual(auto_runtime_fallback_excluded["recommended_flatten_backend"], "python")
        self.assertEqual(auto_runtime_fallback_excluded["recommended_parallel_workers"], 2)

        auto_static_fallback_recommends_effective_backend = recommend_parallel_workers(
            [
                {
                    "flatten_backend": "auto",
                    "effective_backend": "python",
                    "workers": 0,
                    "records_per_s": 100.0,
                    "python_fallback_active": True,
                    **base,
                }
            ]
        )
        self.assertEqual(auto_static_fallback_recommends_effective_backend["recommended_flatten_backend"], "python")

        duplicate_effective_backend_collapses_to_best_rate = recommend_parallel_workers(
            [
                {
                    "flatten_backend": "auto",
                    "effective_backend": "python",
                    "workers": 0,
                    "records_per_s": 100.0,
                    "python_fallback_active": True,
                    **base,
                },
                {"flatten_backend": "python", "effective_backend": "python", "workers": 0, "records_per_s": 90.0, **base},
            ]
        )
        self.assertEqual(duplicate_effective_backend_collapses_to_best_rate["recommended_flatten_backend"], "python")
        self.assertEqual(
            duplicate_effective_backend_collapses_to_best_rate["eligible_configurations"],
            [{"flatten_backend": "python", "workers": 0}],
        )

        artifact_contract_warnings_excluded = recommend_parallel_workers(
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
        self.assertEqual(artifact_contract_warnings_excluded["recommended_parallel_workers"], 0)

        all_failed = recommend_parallel_workers(
            [{"workers": 0, "records_per_s": 100.0, "status": "failed", "error_count": 1, "artifact_contract_status": "failed"}]
        )
        self.assertEqual(all_failed["status"], "failed")
        self.assertIsNone(all_failed["recommended_parallel_workers"])

    def test_aggregate_worker_attempts_exposes_partial_effective_state_and_parser_fallbacks(self):
        attempts = [
            {
                "workers": 8,
                "flatten_backend": "rust-arrow",
                "effective_backend": "rust-arrow",
                "status": "done",
                "run_dir": "/tmp/profile/w8/r1",
                "duration_s": 1.0,
                "records_per_s": 100.0,
                "records_read": 100,
                "records_ok": 100,
                "artifact_contract_status": "done",
                "timings_ms": {"rust_arrow.total": 10},
                "rust_raw_jsonl_parse_requested": True,
                "rust_raw_jsonl_parse_effective": True,
                "rust_raw_jsonl_file_parse_requested": True,
                "rust_raw_jsonl_file_parse_effective": True,
                "rust_columnar_accumulator": True,
                "rust_parser_backend": "simd-json",
                "rust_parser_backend_effective": "simd-json",
                "rust_parser_fallbacks": 2,
            },
            {
                "workers": 8,
                "flatten_backend": "rust-arrow",
                "effective_backend": "rust-arrow",
                "status": "done",
                "run_dir": "/tmp/profile/w8/r2",
                "duration_s": 2.0,
                "records_per_s": 50.0,
                "records_read": 100,
                "records_ok": 100,
                "artifact_contract_status": "done",
                "timings_ms": {"rust_arrow.total": 20},
                "rust_raw_jsonl_parse_requested": True,
                "rust_raw_jsonl_parse_effective": True,
                "rust_raw_jsonl_file_parse_requested": True,
                "rust_raw_jsonl_file_parse_effective": False,
                "rust_columnar_accumulator": True,
                "rust_parser_backend": "simd-json",
                "rust_parser_backend_effective": "simd-json",
                "rust_parser_fallbacks": 1,
            },
        ]

        row = _aggregate_worker_attempts(
            worker=8,
            flatten_backend="rust-arrow",
            attempts=attempts,
            issue_sample_limit=5,
        )

        self.assertEqual(row["rust_raw_jsonl_parse_effective_state"], "all")
        self.assertEqual(row["rust_raw_jsonl_file_parse_effective_state"], "partial")
        self.assertEqual(row["rust_raw_jsonl_file_parse_effective"], False)
        self.assertEqual(row["rust_columnar_accumulator_state"], "all")
        self.assertEqual(row["rust_parser_fallbacks"], 3)

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
                report.add_time_ms("rust_arrow.py_to_json", 25)
                report.add_time_ms("json.flatten", int(duration * 800))
                report.add_time_ms("json.parquet.persist", 50)
                report.add_time_ms("rust_arrow.total", int(duration * 850))
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
            self.assertEqual(saved["runs"][0]["timings_ms"]["rust_arrow.py_to_json"], 25)
            self.assertEqual(saved["runs"][0]["timings_ms"]["rust_arrow.unaccounted_ms"], 450)
            md = Path(out_dir, "parallel_profile.md").read_text(encoding="utf-8")
            self.assertIn("rust_arrow.py_to_json_ms", md)
            self.assertIn("rust_arrow.table_assemble_ms", md)
            self.assertIn("rust_arrow.table_write_ms", md)
            self.assertIn("rust_arrow.unaccounted_ms", md)

    def test_profile_parallel_compares_flatten_backends(self):
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
                backend = str(data_config["flatten_backend"])
                duration = {"python": 10.0, "rust-arrow": 7.0}[backend]
                parquet_dir = Path(data_config["persist_parquet_dir"])
                parquet_dir.mkdir(parents=True, exist_ok=True)
                Path(parquet_dir, "placeholder.txt").write_text("x", encoding="utf-8")
                report = kwargs["report"]
                report.set_artifact("flatten_backend", backend)
                report.set_artifact("flatten_backend_effective", backend)
                report.bump("records_read", 1000)
                report.bump("records_ok", 1000)
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
                    workers=[0],
                    flatten_backends=["python", "rust-arrow"],
                    out_dir=out_dir,
                    max_records=1000,
                    shuffle_order=False,
                )

            self.assertEqual(summary["recommended_flatten_backend"], "rust-arrow")
            self.assertEqual(summary["recommended_parallel_workers"], 0)
            self.assertTrue(Path(out_dir, "python", "w0", "run_report.json").exists())
            self.assertTrue(Path(out_dir, "rust_arrow", "w0", "artifact_contract.json").exists())
            md = Path(out_dir, "parallel_profile.md").read_text(encoding="utf-8")
            self.assertIn("recommended_flatten_backend", md)

    def test_profile_parallel_applies_raw_jsonl_parse_only_to_rust_arrow(self):
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
            seen: dict[str, bool] = {}
            seen_parser: dict[str, str] = {}

            def fake_run_json_pipeline(data_config, db_config, **kwargs):
                backend = str(data_config["flatten_backend"])
                seen[backend] = bool(data_config.get("rust_raw_jsonl_parse"))
                seen_parser[backend] = str(data_config.get("rust_parser_backend"))
                parquet_dir = Path(data_config["persist_parquet_dir"])
                parquet_dir.mkdir(parents=True, exist_ok=True)
                Path(parquet_dir, "placeholder.txt").write_text("x", encoding="utf-8")
                report = kwargs["report"]
                report.set_artifact("flatten_backend", backend)
                report.set_artifact("flatten_backend_effective", backend)
                report.set_artifact("rust_raw_jsonl_parse_requested", bool(data_config.get("rust_raw_jsonl_parse")))
                report.set_artifact(
                    "rust_raw_jsonl_parse_effective",
                    backend == "rust-arrow" and bool(data_config.get("rust_raw_jsonl_parse")),
                )
                report.set_artifact("rust_parser_backend", str(data_config.get("rust_parser_backend")))
                if backend == "rust-arrow":
                    report.set_artifact("rust_parser_backend_effective", str(data_config.get("rust_parser_backend")))
                report.bump("records_read", 100)
                report.bump("records_ok", 100)
                report.add_time_ms("pipeline.json.total", 1000)
                if backend == "rust-arrow":
                    report.add_time_ms("rust_arrow.json_parse", 20)
                report.duration_s = 1.0 if backend == "rust-arrow" else 2.0
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
                    workers=[0],
                    flatten_backends=["python", "rust-arrow"],
                    rust_parser_backend="simd-json",
                    out_dir=out_dir,
                    shuffle_order=False,
                )

            self.assertEqual(seen, {"python": False, "rust-arrow": True})
            self.assertEqual(seen_parser, {"python": "serde-json", "rust-arrow": "simd-json"})
            rows = {row["flatten_backend"]: row for row in summary["runs"]}
            self.assertEqual(rows["python"]["rust_raw_jsonl_parse_requested"], False)
            self.assertEqual(rows["python"]["rust_raw_jsonl_parse_effective"], False)
            self.assertEqual(rows["rust-arrow"]["rust_raw_jsonl_parse_requested"], True)
            self.assertEqual(rows["rust-arrow"]["rust_raw_jsonl_parse_effective"], True)
            self.assertEqual(rows["rust-arrow"]["rust_parser_backend_effective"], "simd-json")
            self.assertEqual(rows["rust-arrow"]["timings_ms"]["rust_arrow.json_parse"], 20)
            md = Path(out_dir, "parallel_profile.md").read_text(encoding="utf-8")
            self.assertIn("raw_jsonl", md)
            self.assertIn("simd-json", md)

    def test_profile_parallel_repeats_and_recommends_by_median(self):
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
            durations = {
                (0, 1): 10.0,
                (0, 2): 10.5,
                (0, 3): 9.5,
                (2, 1): 8.0,
                (2, 2): 7.0,
                (2, 3): 9.0,
            }

            def fake_run_json_pipeline(data_config, db_config, **kwargs):
                worker = int(data_config["parallel_workers"])
                run_dir = Path(data_config["persist_parquet_dir"]).parent
                repeat_index = int(run_dir.name[1:]) if run_dir.name.startswith("r") else 1
                duration = durations[(worker, repeat_index)]
                parquet_dir = Path(data_config["persist_parquet_dir"])
                parquet_dir.mkdir(parents=True, exist_ok=True)
                Path(parquet_dir, "placeholder.txt").write_text("x", encoding="utf-8")
                report = kwargs["report"]
                report.bump("records_read", 1000)
                report.bump("records_ok", 1000)
                if worker == 0 and repeat_index == 1:
                    report.warn(stage="json.flatten", message="sample warning")
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
                    repeat=3,
                    shuffle_order=False,
                )

            self.assertEqual(summary["repeat"], 3)
            self.assertEqual(summary["records_per_s_basis"], "median")
            self.assertEqual(summary["recommended_parallel_workers"], 2)
            self.assertEqual(summary["runs"][0]["attempt_count"], 3)
            self.assertEqual(summary["runs"][0]["eligible_attempt_count"], 3)
            self.assertEqual(summary["runs"][0]["status"], "done_with_warnings")
            self.assertEqual(summary["runs"][0]["artifact_contract_status"], "done")
            self.assertAlmostEqual(summary["runs"][0]["records_per_s"], 100.0, places=3)
            self.assertAlmostEqual(summary["runs"][1]["records_per_s"], 125.0, places=3)
            self.assertTrue(Path(out_dir, "w0", "r1", "run_report.json").exists())
            self.assertTrue(Path(out_dir, "w2", "r3", "artifact_contract.json").exists())

    def test_profile_parallel_embeds_issue_samples(self):
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
                report.warn(stage="id_compaction | contract", message="ambiguous | column skipped")
                report.set_artifact("flatten_backend_fallback_reason", "RuntimeError: boom | partial")
                report.set_artifact("rust_arrow_failed_batches", 1)
                report.set_artifact("python_fallback_active", True)
                report.bump("records_read", 100)
                report.add_time_ms("pipeline.json.total", 1000)
                report.duration_s = 1.0
                report.finished_at = "2026-01-01T00:00:00+00:00"
                return JsonRunResult(name_maps={}, report=report)

            def fake_inspect(parquet_root, **kwargs):
                return {
                    "status": "done_with_warnings",
                    "parquet_root": str(parquet_root),
                    "input": dict(kwargs),
                    "summary": {},
                    "tables": {},
                    "issues": [],
                    "warnings": [{"check": "schema_manifest | missing", "message": "schema manifest | missing"}],
                }

            with patch("KISTI_DB_Manager.pipeline.run_json_pipeline", side_effect=fake_run_json_pipeline), patch(
                "KISTI_DB_Manager.parquet_artifacts.inspect_parquet_artifact_contract",
                side_effect=fake_inspect,
            ):
                summary = profile_parallel(
                    config_path=cfg_path,
                    workers=[0],
                    out_dir=out_dir,
                    issue_sample_limit=2,
                )

            samples = summary["runs"][0]["issue_samples"]
            self.assertEqual(summary["status"], "done_with_warnings")
            self.assertEqual(summary["execution_status"], "done_with_warnings")
            self.assertEqual(summary["recommendation_status"], "failed")
            self.assertIsNone(summary["recommended_parallel_workers"])
            self.assertEqual(summary["runs"][0]["flatten_backend_fallback_reason"], "RuntimeError: boom | partial")
            self.assertEqual(summary["runs"][0]["rust_arrow_failed_batches"], 1)
            self.assertEqual(summary["runs"][0]["flatten_backend_fallback_batches"], 1)
            self.assertEqual(summary["runs"][0]["python_fallback_active"], True)
            self.assertEqual(len(samples), 2)
            self.assertEqual(samples[0]["source"], "run_report")
            self.assertEqual(samples[1]["source"], "artifact_contract.warnings")
            md = Path(out_dir, "parallel_profile.md").read_text(encoding="utf-8")
            self.assertIn("RuntimeError: boom \\| partial", md)
            self.assertIn("id_compaction \\| contract", md)
            self.assertIn("ambiguous \\| column skipped", md)
            self.assertIn("schema_manifest \\| missing", md)
            self.assertIn("## Issue Samples", md)

    def test_profile_parallel_rejects_symlink_run_dir_before_cleanup(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            out_dir = Path(td, "profile")
            external = Path(td, "external")
            external_parquet = external / "parquet"
            sentinel = external_parquet / "keep.txt"
            external_parquet.mkdir(parents=True)
            sentinel.write_text("keep", encoding="utf-8")
            out_dir.mkdir()
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
            os.symlink(external, out_dir / "w0", target_is_directory=True)

            with patch("KISTI_DB_Manager.pipeline.run_json_pipeline") as p_run:
                with self.assertRaisesRegex(RuntimeError, "symlink"):
                    profile_parallel(
                        config_path=cfg_path,
                        workers=[0],
                        out_dir=out_dir,
                        cleanup_parquet=True,
                    )

            p_run.assert_not_called()
            self.assertTrue(sentinel.exists())

    def test_profile_parallel_rejects_symlink_out_dir_before_cleanup(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            external = Path(td, "external")
            external.mkdir()
            sentinel = external / "keep.txt"
            sentinel.write_text("keep", encoding="utf-8")
            out_link = Path(td, "profile_link")
            os.symlink(external, out_link, target_is_directory=True)
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

            with patch("KISTI_DB_Manager.pipeline.run_json_pipeline") as p_run:
                with self.assertRaisesRegex(RuntimeError, "symlink"):
                    profile_parallel(
                        config_path=cfg_path,
                        workers=[0],
                        out_dir=out_link,
                        cleanup_parquet=True,
                    )

            p_run.assert_not_called()
            self.assertTrue(sentinel.exists())

    def test_profile_parallel_rejects_symlink_out_dir_after_missing_parent_dotdot(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            external = Path(td, "external")
            external.mkdir()
            sentinel = external / "keep.txt"
            sentinel.write_text("keep", encoding="utf-8")
            out_link = Path(td, "profile_link")
            os.symlink(external, out_link, target_is_directory=True)
            out_dir = Path(td, "missing_parent", "..", "profile_link")
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

            with patch("KISTI_DB_Manager.pipeline.run_json_pipeline") as p_run:
                with self.assertRaisesRegex(RuntimeError, "symlink"):
                    profile_parallel(
                        config_path=cfg_path,
                        workers=[0],
                        out_dir=out_dir,
                        cleanup_parquet=True,
                    )

            p_run.assert_not_called()
            self.assertTrue(sentinel.exists())

    def test_profile_parallel_preflights_all_run_dirs_before_running(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            out_dir = Path(td, "profile")
            stale = out_dir / "w2" / "stale.txt"
            stale.parent.mkdir(parents=True)
            stale.write_text("stale", encoding="utf-8")
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

            with patch("KISTI_DB_Manager.pipeline.run_json_pipeline") as p_run:
                with self.assertRaisesRegex(RuntimeError, "already exists and is not empty"):
                    profile_parallel(
                        config_path=cfg_path,
                        workers=[0, 2],
                        out_dir=out_dir,
                        shuffle_order=False,
                    )

            p_run.assert_not_called()
            self.assertFalse((out_dir / "w0").exists())
            self.assertFalse((out_dir / "parallel_profile.json").exists())

    def test_profile_parallel_partial_worker_failure_is_warning_when_recommendation_succeeds(self):
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
                if worker == 0:
                    raise RuntimeError("sample failure")
                parquet_dir = Path(data_config["persist_parquet_dir"])
                parquet_dir.mkdir(parents=True, exist_ok=True)
                Path(parquet_dir, "placeholder.txt").write_text("x", encoding="utf-8")
                report = kwargs["report"]
                report.bump("records_read", 100)
                report.bump("records_ok", 100)
                report.add_time_ms("io.json_parse", 10)
                report.add_time_ms("rust_arrow.read_line", 4)
                report.add_time_ms("rust_arrow.py_to_json", 5)
                report.add_time_ms("rust_arrow.number_validate", 6)
                report.add_time_ms("json.flatten", 20)
                report.add_time_ms("rust_arrow.table_assemble", 12)
                report.add_time_ms("rust_arrow.columnar_merge", 7)
                report.add_time_ms("json.parquet.persist", 30)
                report.add_time_ms("rust_arrow.id_compaction", 13)
                report.add_time_ms("rust_arrow.table_write", 14)
                report.add_time_ms("rust_arrow.arrow_build", 8)
                report.add_time_ms("rust_arrow.parquet_write", 9)
                report.add_time_ms("rust_arrow.py_result_convert", 11)
                report.add_time_ms("rust_arrow.total", 60)
                report.add_time_ms("pipeline.json.total", 1000)
                report.duration_s = 1.0
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
                    shuffle_order=False,
                )

            self.assertEqual(summary["status"], "done_with_warnings")
            self.assertEqual(summary["execution_status"], "done_with_warnings")
            self.assertEqual(summary["recommendation_status"], "done")
            self.assertEqual(summary["recommended_parallel_workers"], 2)

    def test_profile_parallel_repeat_runtime_fallback_is_not_execution_failed(self):
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
                report.warn(stage="json_pipeline.rust_arrow_auto_fallback", message="fallback")
                report.set_artifact("flatten_backend", "auto")
                report.set_artifact("flatten_backend_effective", "python")
                report.set_artifact("flatten_backend_fallback_reason", "RuntimeError: boom")
                report.set_artifact("rust_arrow_failed_batches", 1)
                report.set_artifact("python_fallback_active", True)
                report.bump("records_read", 100)
                report.bump("records_ok", 100)
                report.add_time_ms("io.json_parse", 10)
                report.add_time_ms("rust_arrow.read_line", 4)
                report.add_time_ms("rust_arrow.py_to_json", 5)
                report.add_time_ms("rust_arrow.number_validate", 6)
                report.add_time_ms("json.flatten", 20)
                report.add_time_ms("rust_arrow.table_assemble", 12)
                report.add_time_ms("rust_arrow.columnar_merge", 7)
                report.add_time_ms("json.parquet.persist", 30)
                report.add_time_ms("rust_arrow.id_compaction", 13)
                report.add_time_ms("rust_arrow.table_write", 14)
                report.add_time_ms("rust_arrow.arrow_build", 8)
                report.add_time_ms("rust_arrow.parquet_write", 9)
                report.add_time_ms("rust_arrow.py_result_convert", 11)
                report.add_time_ms("rust_arrow.total", 60)
                report.add_time_ms("pipeline.json.total", 1000)
                report.duration_s = 1.0
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
                    workers=[0],
                    out_dir=out_dir,
                    repeat=2,
                    shuffle_order=False,
                )

            row = summary["runs"][0]
            self.assertEqual(summary["status"], "done_with_warnings")
            self.assertEqual(summary["execution_status"], "done_with_warnings")
            self.assertEqual(summary["recommendation_status"], "failed")
            self.assertEqual(row["status"], "done_with_warnings")
            self.assertEqual(row["failed_attempt_count"], 0)
            self.assertEqual(row["recommendation_ineligible_attempt_count"], 2)
            self.assertEqual(row["timings_ms"]["io.json_parse"], 10)
            self.assertEqual(row["timings_ms"]["rust_arrow.read_line"], 4)
            self.assertEqual(row["timings_ms"]["rust_arrow.py_to_json"], 5)
            self.assertEqual(row["timings_ms"]["rust_arrow.number_validate"], 6)
            self.assertEqual(row["timings_ms"]["json.flatten"], 20)
            self.assertEqual(row["timings_ms"]["rust_arrow.table_assemble"], 12)
            self.assertEqual(row["timings_ms"]["rust_arrow.columnar_merge"], 7)
            self.assertEqual(row["timings_ms"]["json.parquet.persist"], 30)
            self.assertEqual(row["timings_ms"]["rust_arrow.id_compaction"], 13)
            self.assertEqual(row["timings_ms"]["rust_arrow.table_write"], 14)
            self.assertEqual(row["timings_ms"]["rust_arrow.arrow_build"], 8)
            self.assertEqual(row["timings_ms"]["rust_arrow.parquet_write"], 9)
            self.assertEqual(row["timings_ms"]["rust_arrow.py_result_convert"], 11)
            self.assertEqual(row["timings_ms"]["rust_arrow.total"], 60)
            self.assertEqual(row["timings_ms"]["rust_arrow.unaccounted_ms"], 0)

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
            self.assertEqual(inspect_calls[0][1]["strict_schema_manifest"], True)

    def test_profile_parallel_cleanup_failure_is_reported_without_losing_summary(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            out_dir = Path(td, "profile")
            external = Path(td, "external")
            external.mkdir()
            sentinel = external / "keep.txt"
            sentinel.write_text("keep", encoding="utf-8")
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
                parquet_dir.symlink_to(external, target_is_directory=True)
                report = kwargs["report"]
                report.bump("records_read", 100)
                report.bump("records_ok", 100)
                report.add_time_ms("pipeline.json.total", 1000)
                report.duration_s = 1.0
                report.finished_at = "2026-01-01T00:00:00+00:00"
                return JsonRunResult(name_maps={}, report=report)

            with patch("KISTI_DB_Manager.pipeline.run_json_pipeline", side_effect=fake_run_json_pipeline), patch(
                "KISTI_DB_Manager.parquet_artifacts.inspect_parquet_artifact_contract"
            ) as p_inspect:
                summary = profile_parallel(
                    config_path=cfg_path,
                    workers=[0],
                    out_dir=out_dir,
                    cleanup_parquet=True,
                )

            row = summary["runs"][0]
            p_inspect.assert_not_called()
            self.assertEqual(summary["status"], "failed")
            self.assertEqual(summary["recommendation_status"], "failed")
            self.assertEqual(row["status"], "failed")
            self.assertEqual(row["artifact_contract_status"], "failed")
            self.assertEqual(row["parquet_cleaned"], False)
            self.assertEqual(row["cleanup_error"]["stage"], "json.profile_parallel.cleanup")
            self.assertIn("symlink", row["cleanup_error"]["message"])
            self.assertTrue(any(sample.get("source") == "profile_cleanup" for sample in row["issue_samples"]))
            self.assertTrue(Path(out_dir, "parallel_profile.json").exists())
            self.assertTrue(Path(out_dir, "w0", "run_report.json").exists())
            self.assertTrue(Path(out_dir, "w0", "artifact_contract.json").exists())
            self.assertTrue(sentinel.exists())

    def test_profile_parallel_rejects_run_dir_swapped_to_symlink_before_report_write(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            out_dir = Path(td, "profile")
            external = Path(td, "external")
            external.mkdir()
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
                run_dir = Path(data_config["persist_parquet_dir"]).parent
                run_dir.rmdir()
                run_dir.symlink_to(external, target_is_directory=True)
                report = kwargs["report"]
                report.bump("records_read", 100)
                report.bump("records_ok", 100)
                report.add_time_ms("pipeline.json.total", 1000)
                report.duration_s = 1.0
                report.finished_at = "2026-01-01T00:00:00+00:00"
                return JsonRunResult(name_maps={}, report=report)

            with patch("KISTI_DB_Manager.pipeline.run_json_pipeline", side_effect=fake_run_json_pipeline), patch(
                "KISTI_DB_Manager.parquet_artifacts.inspect_parquet_artifact_contract"
            ) as p_inspect:
                summary = profile_parallel(
                    config_path=cfg_path,
                    workers=[0],
                    out_dir=out_dir,
                )

            p_inspect.assert_not_called()
            row = summary["runs"][0]
            self.assertEqual(summary["status"], "failed")
            self.assertEqual(row["status"], "failed")
            self.assertTrue(any(sample.get("stage") == "json.profile_parallel.report_write" for sample in row["issue_samples"]))
            self.assertEqual(list(external.iterdir()), [])
            self.assertFalse(Path(out_dir, "w0", "run_report.json").exists())
            self.assertFalse(Path(out_dir, "w0", "artifact_contract.json").exists())
            self.assertTrue(Path(out_dir, "parallel_profile.json").exists())

    def test_profile_parallel_rejects_summary_symlink_before_running(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            out_dir = Path(td, "profile")
            external = Path(td, "external.json")
            external.write_text("keep", encoding="utf-8")
            out_dir.mkdir()
            (out_dir / "parallel_profile.json").symlink_to(external)
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

            with patch("KISTI_DB_Manager.pipeline.run_json_pipeline") as p_run:
                with self.assertRaisesRegex(RuntimeError, "summary json"):
                    profile_parallel(
                        config_path=cfg_path,
                        workers=[0],
                        out_dir=out_dir,
                    )

            p_run.assert_not_called()
            self.assertEqual(external.read_text(encoding="utf-8"), "keep")

    def test_profile_quarantine_writer_rejects_symlink_parent_on_close(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td, "profile")
            run_dir = root / "w0"
            run_dir.mkdir(parents=True)
            external = Path(td, "external")
            external.mkdir()
            q = _ProfileQuarantineWriter(root, run_dir / "quarantine.jsonl")

            with self.assertRaisesRegex(RuntimeError, "symlink"):
                with q:
                    q.write(stage="test", record={"id": 1}, exc=RuntimeError("boom"))
                    run_dir.rmdir()
                    run_dir.symlink_to(external, target_is_directory=True)

            self.assertEqual(list(external.iterdir()), [])

    def test_profile_safe_write_does_not_unlink_external_tmp_after_parent_swap(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td, "profile")
            run_dir = root / "w0"
            backup_dir = root / "w0_backup"
            external = Path(td, "external")
            run_dir.mkdir(parents=True)
            external.mkdir()
            external_tmp: Path | None = None

            def fake_replace(src, dst):
                nonlocal external_tmp
                src_path = Path(src)
                run_dir.rename(backup_dir)
                run_dir.symlink_to(external, target_is_directory=True)
                external_tmp = external / src_path.name
                external_tmp.write_text("keep", encoding="utf-8")
                raise RuntimeError("replace failed after parent swap")

            with patch("KISTI_DB_Manager.json_parallel_profile.os.replace", side_effect=fake_replace):
                with self.assertRaisesRegex(RuntimeError, "replace failed"):
                    _safe_write_text(
                        root,
                        run_dir / "run_report.json",
                        "{}\n",
                        purpose="profile run report",
                    )

            self.assertIsNotNone(external_tmp)
            self.assertEqual(external_tmp.read_text(encoding="utf-8"), "keep")
            self.assertEqual(list(external.glob("run_report.json")), [])

    def test_profile_parallel_writes_quarantine_through_safe_writer(self):
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
                with kwargs["quarantine"] as q:
                    q.write(stage="fake", record={"id": 1}, exc=RuntimeError("boom"))
                report = kwargs["report"]
                report.bump("records_read", 1)
                report.bump("records_ok", 1)
                report.add_time_ms("pipeline.json.total", 1000)
                report.duration_s = 1.0
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
                    workers=[0],
                    out_dir=out_dir,
                )

            q_path = Path(summary["runs"][0]["quarantine_path"])
            lines = q_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0])["stage"], "fake")


if __name__ == "__main__":
    unittest.main()
