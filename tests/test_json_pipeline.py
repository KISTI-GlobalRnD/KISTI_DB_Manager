import unittest
from unittest.mock import patch


from KISTI_DB_Manager.pipeline import run_json_pipeline


class DummyDF:
    def __init__(self, columns, rows=1):
        self.columns = list(columns)
        self._rows = int(rows)

    def __len__(self):
        return self._rows


class TestJsonPipeline(unittest.TestCase):
    def test_run_json_pipeline_handles_missing_processing_backend(self):
        # Ensure the import path is exercised even when optional deps are installed
        # and earlier tests have already imported the processing backend.
        import sys

        sys.modules.pop("KISTI_DB_Manager.processing", None)

        data_config = {
            "PATH": "data/",
            "file_name": "x.jsonl",
            "file_type": "jsonl",
            "table_name": "base",
            "KEY_SEP": "__",
        }
        db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

        real_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            # Relative import inside the package can come through as `name="processing", level=1`.
            if name.endswith(".processing") or name in {"KISTI_DB_Manager.processing", "processing"}:
                raise ModuleNotFoundError("No module named 'numpy'")
            return real_import(name, globals, locals, fromlist, level)

        with patch("builtins.__import__", side_effect=fake_import):
            res = run_json_pipeline(
                data_config,
                db_config,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

        self.assertEqual(res.name_maps, {})
        self.assertGreaterEqual(len(res.report.issues), 1)

    def test_run_json_pipeline_builds_and_extends_namemaps(self):
        data_config = {
            "PATH": "data/",
            "file_name": "x.jsonl",
            "file_type": "jsonl",
            "table_name": "base",
            "KEY_SEP": "__",
        }
        db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}

        def fake_iter_records(_dc, report=None, max_records=None, with_context=False):
            yield {"id": 1}
            yield {"id": 2}
            yield {"id": 3}

        calls = []

        def fake_extract(batch_records, **_kwargs):
            calls.append(len(batch_records))
            if len(calls) == 1:
                main = DummyDF(["id", "a"], rows=len(batch_records))
            else:
                main = DummyDF(["id", "a", "b"], rows=len(batch_records))
            subs = {"items": DummyDF(["id", "items__x"], rows=1)}
            return main, subs, {}

        with patch("KISTI_DB_Manager.pipeline._iter_json_records", side_effect=fake_iter_records), patch(
            "KISTI_DB_Manager.manage.create_table_from_columns",
            side_effect=lambda *_a, **kw: kw.get("name_map"),
        ) as p_create, patch(
            "KISTI_DB_Manager.manage.fill_table_from_dataframe",
            side_effect=lambda *_a, **kw: kw.get("name_map"),
        ) as p_load:
            res = run_json_pipeline(
                data_config,
                db_config,
                chunk_size=2,
                extract_fn=fake_extract,
                create=True,
                load=True,
                index=False,
                optimize=False,
                continue_on_error=False,
            )

        self.assertEqual(calls, [2, 1])
        self.assertIn("base", res.name_maps)
        self.assertIn("base__items", res.name_maps)
        self.assertIn("b", res.name_maps["base"].columns_original)
        self.assertIn("name_maps_json", res.report.artifacts)
        self.assertEqual(p_create.call_count, 2)  # base + base__items
        self.assertEqual(p_load.call_count, 4)  # 2 batches * 2 tables


if __name__ == "__main__":
    unittest.main()
