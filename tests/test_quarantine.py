import json
import tempfile
import unittest


from KISTI_DB_Manager.quarantine import NullQuarantineWriter, QuarantineWriter


class TestQuarantine(unittest.TestCase):
    def test_quarantine_writer_writes_jsonl(self):
        with tempfile.TemporaryDirectory() as td:
            path = f"{td}/q.jsonl"
            with QuarantineWriter(path) as q:
                q.write(stage="stage", record={"a": 1}, index=3)

            with open(path, encoding="utf-8") as f:
                line = f.read().strip()
            obj = json.loads(line)
            self.assertEqual(obj["stage"], "stage")
            self.assertEqual(obj["index"], 3)
            self.assertEqual(obj["record"], {"a": 1})

    def test_null_quarantine_noop(self):
        with NullQuarantineWriter() as q:
            q.write(stage="s", record={"x": 1})


if __name__ == "__main__":
    unittest.main()
