import tempfile
import unittest
from pathlib import Path

from KISTI_DB_Manager import parquet_delta_merge, parquet_replay_repair


class TestPathSafetyHelpers(unittest.TestCase):
    def test_parquet_delta_cleanup_rejects_symlink_file(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cleanup_root = root / "cleanup"
            cleanup_root.mkdir()
            external = root / "external.parquet"
            link = cleanup_root / "part-0.parquet"
            external.write_text("keep", encoding="utf-8")
            link.symlink_to(external)

            with self.assertRaisesRegex(RuntimeError, "safe regular file|symlink"):
                parquet_delta_merge._cleanup_glob(cleanup_root, "*.parquet")

            self.assertTrue(link.is_symlink())
            self.assertEqual(external.read_text(encoding="utf-8"), "keep")

    def test_parquet_replay_cleanup_rejects_symlink_file(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cleanup_root = root / "cleanup"
            cleanup_root.mkdir()
            external = root / "external.parquet"
            link = cleanup_root / "part-0.parquet"
            external.write_text("keep", encoding="utf-8")
            link.symlink_to(external)

            with self.assertRaisesRegex(RuntimeError, "safe regular file|symlink"):
                parquet_replay_repair._cleanup_glob(cleanup_root, "*.parquet")

            self.assertTrue(link.is_symlink())
            self.assertEqual(external.read_text(encoding="utf-8"), "keep")


if __name__ == "__main__":
    unittest.main()
