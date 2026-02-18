import json
import tempfile
import unittest
import zipfile
from pathlib import Path


from KISTI_DB_Manager.pipeline import _iter_json_records


class TestIterJsonRecords(unittest.TestCase):
    def test_iter_json_records_supports_file_names(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "a.jsonl").write_text('{"id":1}\n', encoding="utf-8")
            (root / "b.jsonl").write_text('{"id":2}\n', encoding="utf-8")

            data_config = {
                "PATH": str(root),
                "file_names": ["a.jsonl", "b.jsonl"],
                "file_type": "jsonl",
            }
            got = list(_iter_json_records(data_config))
            self.assertEqual([it["id"] for it in got], [1, 2])

    def test_iter_json_records_supports_file_glob(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "02.jsonl").write_text('{"id":2}\n', encoding="utf-8")
            (root / "01.jsonl").write_text('{"id":1}\n', encoding="utf-8")

            data_config = {
                "PATH": str(root),
                "file_glob": "*.jsonl",
            }
            got = list(_iter_json_records(data_config))
            self.assertEqual([it["id"] for it in got], [1, 2])

    def test_iter_json_records_zip_reads_all_json_members_by_default(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            zip_path = root / "multi.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("a.jsonl", '{"id":1}\n')
                zf.writestr("b.json", json.dumps([{"id": 2}, {"id": 3}], ensure_ascii=False))

            data_config = {
                "PATH": str(root),
                "file_name": "multi.zip",
                "file_type": "zip",
            }
            got = list(_iter_json_records(data_config))
            self.assertEqual([it["id"] for it in got], [1, 2, 3])

    def test_iter_json_records_zip_can_select_multiple_members(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            zip_path = root / "multi.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("a.jsonl", '{"id":1}\n')
                zf.writestr("b.jsonl", '{"id":2}\n')
                zf.writestr("c.json", json.dumps({"id": 3}, ensure_ascii=False))

            data_config = {
                "PATH": str(root),
                "file_name": "multi.zip",
                "file_type": "zip",
                "json_file_names": ["b.jsonl", "c.json"],
            }
            got = list(_iter_json_records(data_config))
            self.assertEqual([it["id"] for it in got], [2, 3])

    def test_iter_json_records_with_context(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "a.jsonl").write_text('{"id":1}\n{"id":2}\n', encoding="utf-8")

            data_config = {
                "PATH": str(root),
                "file_name": "a.jsonl",
                "file_type": "jsonl",
            }
            got = list(_iter_json_records(data_config, with_context=True))
            self.assertEqual(len(got), 2)
            self.assertEqual(got[0][0]["id"], 1)
            self.assertEqual(got[0][1]["line_no"], 1)
            self.assertTrue(str(got[0][1]["source_path"]).endswith("a.jsonl"))

    def test_iter_json_records_supports_source_sampling(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "01.jsonl").write_text('{"id":1}\n', encoding="utf-8")
            (root / "02.jsonl").write_text('{"id":2}\n', encoding="utf-8")
            (root / "03.jsonl").write_text('{"id":3}\n', encoding="utf-8")

            data_config = {
                "PATH": str(root),
                "file_glob": "*.jsonl",
                "sample_randomize_sources": True,
                "sample_seed": 7,
                "sample_max_sources": 2,
            }
            got1 = list(_iter_json_records(data_config))
            got2 = list(_iter_json_records(data_config))

            self.assertEqual(len(got1), 2)
            self.assertEqual(len(got2), 2)
            self.assertEqual([x["id"] for x in got1], [x["id"] for x in got2])
            self.assertTrue(set(x["id"] for x in got1).issubset({1, 2, 3}))


if __name__ == "__main__":
    unittest.main()
