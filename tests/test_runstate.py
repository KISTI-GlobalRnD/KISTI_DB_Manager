import json
import tempfile
import unittest
from pathlib import Path

from KISTI_DB_Manager.runstate import (
    JsonRunState,
    UnsafePathError,
    append_text,
    atomic_write_json,
    atomic_write_text,
    safe_rmtree,
    safe_unlink_file,
)


class TestRunState(unittest.TestCase):
    def test_atomic_write_json_does_not_follow_output_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            external = root / "external.json"
            external.write_text("keep", encoding="utf-8")
            out = root / "state.json"
            out.symlink_to(external)

            with self.assertRaisesRegex(RuntimeError, "not a safe file|symlink"):
                atomic_write_json(out, {"status": "done"})

            self.assertEqual(external.read_text(encoding="utf-8"), "keep")
            self.assertTrue(out.is_symlink())

    def test_json_run_state_uses_safe_atomic_write_without_resolving_symlink_target(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out = root / "state.json"

            state = JsonRunState.create(out, {"status": "running"})
            state.set_status("done")

            self.assertEqual(state.path, out)
            payload = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "done")
            self.assertFalse(list(root.glob("*.tmp")))

    def test_atomic_write_text_does_not_follow_output_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            external = root / "external.txt"
            external.write_text("keep", encoding="utf-8")
            out = root / "report.txt"
            out.symlink_to(external)

            with self.assertRaisesRegex(RuntimeError, "not a safe file|symlink"):
                atomic_write_text(out, "changed")

            self.assertEqual(external.read_text(encoding="utf-8"), "keep")
            self.assertTrue(out.is_symlink())

    def test_safe_unlink_file_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            external = root / "external.txt"
            link = root / "out.txt"
            external.write_text("keep", encoding="utf-8")
            link.symlink_to(external)

            with self.assertRaisesRegex(RuntimeError, "safe regular file|symlink"):
                safe_unlink_file(link)

            self.assertTrue(link.is_symlink())
            self.assertEqual(external.read_text(encoding="utf-8"), "keep")

    def test_append_text_does_not_follow_output_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            external = root / "external.log"
            link = root / "build.log"
            external.write_text("keep", encoding="utf-8")
            link.symlink_to(external)

            with self.assertRaisesRegex(UnsafePathError, "not a safe file|symlink"):
                append_text(link, "changed\n")

            self.assertTrue(link.is_symlink())
            self.assertEqual(external.read_text(encoding="utf-8"), "keep")

    def test_safe_rmtree_rejects_symlink_directory(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            external = root / "external"
            link = root / "tree"
            external.mkdir()
            (external / "keep.txt").write_text("keep", encoding="utf-8")
            link.symlink_to(external, target_is_directory=True)

            with self.assertRaisesRegex(RuntimeError, "safe directory|symlink"):
                safe_rmtree(link)

            self.assertTrue(link.is_symlink())
            self.assertEqual((external / "keep.txt").read_text(encoding="utf-8"), "keep")


if __name__ == "__main__":
    unittest.main()
