import unittest


from KISTI_DB_Manager.config import (
    DEFAULT_FILE_SEP,
    DEFAULT_KEY_SEP,
    DataConfig,
    normalize_data_config,
)


class TestConfig(unittest.TestCase):
    def test_normalize_data_config_defaults(self):
        cfg = normalize_data_config({"PATH": "x/", "file_name": "a.csv", "table_name": "t", "file_type": "csv"})
        self.assertEqual(cfg["SEP"], DEFAULT_FILE_SEP)
        self.assertEqual(cfg["KEY_SEP"], DEFAULT_KEY_SEP)
        self.assertIn("forced_null", cfg)
        self.assertIn("Conv_DATETIME", cfg)
        self.assertEqual(cfg["include_extra_columns"], True)
        self.assertEqual(cfg["auto_alter_table"], True)
        self.assertEqual(cfg["fallback_on_insert_error"], True)
        self.assertEqual(cfg["fallback_column_type"], "LONGTEXT")
        self.assertEqual(cfg["insert_retry_max"], 5)
        self.assertEqual(cfg["index_prefix_len"], 191)

    def test_normalize_data_config_file_sep_alias(self):
        cfg = normalize_data_config(
            {"PATH": "x/", "file_name": "a.csv", "table_name": "t", "file_type": "csv", "FILE_SEP": ","}
        )
        self.assertEqual(cfg["SEP"], ",")

    def test_dataconfig_roundtrip(self):
        src = {
            "PATH": "x/",
            "file_name": "a.csv",
            "table_name": "t",
            "file_type": "csv",
            "SEP": ",",
            "KEY_SEP": "__",
            "forced_null": True,
            "KEYs": ["id"],
            "chunksize": 1000,
        }
        dc = DataConfig.from_mapping(src)
        out = dc.to_dict()
        self.assertEqual(out["PATH"], "x/")
        self.assertEqual(out["SEP"], ",")
        self.assertEqual(out["KEY_SEP"], "__")
        self.assertEqual(out["forced_null"], True)
        self.assertEqual(out["KEYs"], ["id"])
        self.assertEqual(out["chunksize"], 1000)


if __name__ == "__main__":
    unittest.main()
