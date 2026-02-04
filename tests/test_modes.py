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
        self.assertTrue(spec.stage_defaults["create"])
        self.assertTrue(spec.stage_defaults["load"])
        self.assertFalse(spec.stage_defaults["index"])
        self.assertFalse(spec.stage_defaults["optimize"])

    def test_apply_mode_unknown_raises(self):
        cfg = {}
        with self.assertRaises(ValueError):
            apply_mode("nope", cfg)

    def test_modes_registry_has_default(self):
        self.assertIn("default", MODES)


if __name__ == "__main__":
    unittest.main()
