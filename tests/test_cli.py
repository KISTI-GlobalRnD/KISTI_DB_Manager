import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch


from KISTI_DB_Manager.cli import MissingDependencyError, _ensure_optional_deps, main


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


if __name__ == "__main__":
    unittest.main()
