import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch


from KISTI_DB_Manager.pipeline import run_tabular_pipeline


class DummyDesc:
    def __init__(self, cols):
        self.index = cols


class TestPipeline(unittest.TestCase):
    def test_run_tabular_pipeline_shares_namemap(self):
        data_config = {
            "PATH": "data/",
            "file_name": "x.csv",
            "file_type": "csv",
            "table_name": "t" * 80,
            "KEY_SEP": "__",
        }
        db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}
        df_desc = DummyDesc(["col1", "col2"])

        def create_side_effect(data_config, db_config, df_desc=None, name_map=None):
            return name_map

        with patch("KISTI_DB_Manager.manage.create_table", side_effect=create_side_effect) as p_create, patch(
            "KISTI_DB_Manager.manage.fill_table_from_file"
        ) as p_load, patch("KISTI_DB_Manager.manage.set_index") as p_index, patch(
            "KISTI_DB_Manager.manage.optimize_table"
        ) as p_opt:
            buf = io.StringIO()
            with redirect_stdout(buf):
                res = run_tabular_pipeline(
                    data_config,
                    db_config,
                    df_desc=df_desc,
                    generate_desc=False,
                    continue_on_error=False,
                )

        self.assertIsNotNone(res.name_map)
        self.assertIn("_name_map", data_config)

        # create called once with a NameMap
        self.assertEqual(p_create.call_count, 1)
        _, kwargs = p_create.call_args
        self.assertIsNotNone(kwargs.get("name_map"))

        # load/index/opt should get the same NameMap object (or compatible)
        self.assertEqual(p_load.call_count, 1)
        load_kwargs = p_load.call_args.kwargs
        self.assertIsNotNone(load_kwargs.get("name_map"))

        self.assertEqual(p_index.call_count, 1)
        idx_kwargs = p_index.call_args.kwargs
        self.assertIsNotNone(idx_kwargs.get("name_map"))

        self.assertEqual(p_opt.call_count, 1)
        opt_kwargs = p_opt.call_args.kwargs
        self.assertIsNotNone(opt_kwargs.get("name_map"))

    def test_run_tabular_pipeline_records_failure(self):
        data_config = {
            "PATH": "data/",
            "file_name": "x.csv",
            "file_type": "csv",
            "table_name": "tbl",
            "KEY_SEP": "__",
        }
        db_config = {"host": "h", "user": "u", "password": "p", "database": "d"}
        df_desc = DummyDesc(["col1"])

        with patch("KISTI_DB_Manager.manage.create_table", side_effect=ValueError("boom")):
            res = run_tabular_pipeline(
                data_config,
                db_config,
                df_desc=df_desc,
                create=True,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
            )

        self.assertGreaterEqual(len(res.report.issues), 1)
        self.assertEqual(res.report.stats.get("create_failed"), 1)


if __name__ == "__main__":
    unittest.main()
