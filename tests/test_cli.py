import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch


from KISTI_DB_Manager.cli import MissingDependencyError, _ensure_optional_deps, build_parser, main


class TestCLI(unittest.TestCase):
    def test_version(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = main(["version"])
        self.assertEqual(rc, 0)
        self.assertTrue(buf.getvalue().strip())

    def test_missing_dependency_error_returns_2(self):
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

            err = io.StringIO()
            with patch(
                "KISTI_DB_Manager.cli._ensure_optional_deps",
                side_effect=MissingDependencyError("json run requires missing dependencies: orjson"),
            ):
                with redirect_stderr(err):
                    rc = main(["json", "run", "--config", cfg_path, "--dry-run"])

        self.assertEqual(rc, 2)
        self.assertIn("requires missing dependencies", err.getvalue())

    def test_ensure_optional_deps_message(self):
        def _import_or_fail(name: str):
            if name in {"pandas", "orjson"}:
                raise ModuleNotFoundError(name)
            return object()

        with patch("KISTI_DB_Manager.cli.importlib.import_module", side_effect=_import_or_fail):
            with self.assertRaises(MissingDependencyError) as ctx:
                _ensure_optional_deps("json run", ["pandas", "orjson", "pandas"], extras=["json", "db"])

        msg = str(ctx.exception)
        self.assertIn("json run requires missing dependencies: orjson, pandas", msg)
        self.assertIn("pip install -e '.[json,db]'", msg)

    def test_parquet_subcommands_parse(self):
        parser = build_parser()

        reload_args = parser.parse_args(["parquet", "reload", "--plan", "plan.json", "--start-at", "tbl"])
        preflight_args = parser.parse_args(["parquet", "preflight", "--plan", "plan.json", "--table", "tbl"])
        inspect_args = parser.parse_args(
            [
                "parquet",
                "inspect",
                "--parquet-root",
                "parquet",
                "--table",
                "tbl",
                "--require-id-compaction",
                "--strict-schema-manifest",
            ]
        )
        finalize_args = parser.parse_args(["parquet", "finalize", "--plan", "plan.json", "--skip-analyze"])
        mark_args = parser.parse_args(
            [
                "parquet",
                "mark-table-done",
                "--status",
                "status.json",
                "--table",
                "tbl",
                "--validation-report",
                "validate.json",
            ]
        )

        self.assertEqual(reload_args.parquet_cmd, "reload")
        self.assertEqual(reload_args.start_at, "tbl")
        self.assertEqual(preflight_args.parquet_cmd, "preflight")
        self.assertEqual(preflight_args.table, ["tbl"])
        self.assertEqual(inspect_args.parquet_cmd, "inspect")
        self.assertEqual(inspect_args.parquet_root, "parquet")
        self.assertTrue(inspect_args.require_id_compaction)
        self.assertTrue(inspect_args.strict_schema_manifest)
        self.assertEqual(finalize_args.parquet_cmd, "finalize")
        self.assertTrue(finalize_args.skip_analyze)
        self.assertEqual(mark_args.parquet_cmd, "mark-table-done")

    def test_openalex_materialize_dispatches_to_packaged_module(self):
        with patch("KISTI_DB_Manager.openalex_materialize.main", return_value=0) as materialize_main:
            rc = main(["openalex", "materialize", "runs/example", "--dotenv", ".env"])

        self.assertEqual(rc, 0)
        materialize_main.assert_called_once_with(
            ["runs/example", "--dotenv", ".env"],
            prog="kisti-db-manager openalex materialize",
        )

    def test_openalex_benchmark_dispatches_to_packaged_module(self):
        with patch("KISTI_DB_Manager.openalex_benchmark.main", return_value=0) as benchmark_main:
            rc = main(
                [
                    "openalex",
                    "benchmark-load",
                    "runs/example/parquet",
                    "--config",
                    "runs/example/config.json",
                ]
            )

        self.assertEqual(rc, 0)
        benchmark_main.assert_called_once_with(
            ["runs/example/parquet", "--config", "runs/example/config.json"],
            prog="kisti-db-manager openalex benchmark-load",
        )

    def test_smoke_rust_db_load_dispatches_to_packaged_module(self):
        with patch("KISTI_DB_Manager.rust_db_smoke.main", return_value=0) as smoke_main:
            rc = main(["smoke", "rust-db-load", "--dotenv", ".env"])

        self.assertEqual(rc, 0)
        smoke_main.assert_called_once_with(
            ["--dotenv", ".env"],
            prog="kisti-db-manager smoke rust-db-load",
        )


if __name__ == "__main__":
    unittest.main()
