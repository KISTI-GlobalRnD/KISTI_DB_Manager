import unittest

from KISTI_DB_Manager.modes import MODES, apply_mode


class TestModes(unittest.TestCase):
    def test_apply_mode_ingest_fast_overrides(self):
        cfg = {"table_name": "t", "file_type": "jsonl"}
        spec = apply_mode("ingest-fast", cfg)
        self.assertEqual(spec.name, "ingest-fast")
        self.assertEqual(cfg["db_load_method"], "auto")
        self.assertEqual(cfg["chunk_size"], 20000)
        self.assertTrue(cfg["json_streaming_load"])
        self.assertFalse(cfg["persist_parquet_files"])
        self.assertFalse(cfg["persist_tsv_files"])
        self.assertTrue(spec.stage_defaults["create"])
        self.assertTrue(spec.stage_defaults["load"])
        self.assertFalse(spec.stage_defaults["index"])
        self.assertFalse(spec.stage_defaults["optimize"])

    def test_apply_mode_ingest_fast_hybrid_overrides(self):
        cfg = {"table_name": "t", "file_type": "jsonl"}
        spec = apply_mode("ingest-fast-hybrid", cfg)
        self.assertEqual(spec.name, "ingest-fast-hybrid")
        self.assertEqual(cfg["db_load_method"], "auto")
        self.assertEqual(cfg["chunk_size"], 20000)
        self.assertEqual(cfg["schema_mode"], "hybrid")
        self.assertEqual(cfg["schema_hybrid_warmup_batches"], 1)
        self.assertTrue(cfg["json_streaming_load"])
        self.assertFalse(cfg["persist_parquet_files"])
        self.assertFalse(cfg["persist_tsv_files"])
        self.assertTrue(spec.stage_defaults["create"])
        self.assertTrue(spec.stage_defaults["load"])
        self.assertFalse(spec.stage_defaults["index"])
        self.assertFalse(spec.stage_defaults["optimize"])

    def test_apply_mode_unknown_raises(self):
        cfg = {}
        with self.assertRaises(ValueError):
            apply_mode("nope", cfg)

    def test_apply_mode_parse_parquet_overrides(self):
        cfg = {"table_name": "t", "file_type": "jsonl", "json_streaming_load": True, "persist_tsv_files": True}
        spec = apply_mode("parse-parquet", cfg)
        self.assertEqual(spec.name, "parse-parquet")
        self.assertFalse(cfg["json_streaming_load"])
        self.assertTrue(cfg["persist_parquet_files"])
        self.assertFalse(cfg["persist_tsv_files"])

    def test_apply_mode_parse_parquet_safe_overrides(self):
        cfg = {"table_name": "t", "file_type": "jsonl"}
        spec = apply_mode("parse-parquet-safe", cfg)
        self.assertEqual(spec.name, "parse-parquet-safe")
        self.assertFalse(cfg["json_streaming_load"])
        self.assertTrue(cfg["persist_parquet_files"])
        self.assertFalse(cfg["persist_tsv_files"])
        self.assertEqual(cfg["parallel_workers"], 0)
        self.assertEqual(cfg["chunk_size"], 5000)

    def test_modes_registry_has_default(self):
        self.assertIn("default", MODES)


if __name__ == "__main__":
    unittest.main()
