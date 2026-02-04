import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path


from KISTI_DB_Manager.cli import main


class TestCLIQuarantineSummary(unittest.TestCase):
    def test_quarantine_summary_writes_outputs(self):
        with tempfile.TemporaryDirectory() as td:
            q_path = Path(td, "q.jsonl")
            out_dir = Path(td, "out")

            q_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:00+00:00",
                                "stage": "stage1",
                                "index": 1,
                                "record": {"id": 1},
                                "context": {"table": "t"},
                                "exception_type": "ValueError",
                                "exception_message": "bad",
                            },
                            ensure_ascii=False,
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:01+00:00",
                                "stage": "stage2",
                                "index": 2,
                                "record": {"id": 2},
                                "context": {},
                            },
                            ensure_ascii=False,
                        ),
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = main(["quarantine", "summary", str(q_path), "--out", str(out_dir)])

            self.assertEqual(rc, 0)
            self.assertTrue((out_dir / "QUARANTINE.md").exists())
            self.assertTrue((out_dir / "quarantine.html").exists())
            self.assertTrue((out_dir / "quarantine_summary.json").exists())


if __name__ == "__main__":
    unittest.main()
