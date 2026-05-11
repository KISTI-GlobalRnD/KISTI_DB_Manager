import json
import tempfile
import unittest
from contextlib import redirect_stdout
import io

from KISTI_DB_Manager.cli import main
from KISTI_DB_Manager.id_compaction_preflight import run_id_compaction_preflight


def _write_jsonl(path, records):
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


class TestIdCompactionPreflight(unittest.TestCase):
    def _config(self, td):
        data_path = f"{td}/records.jsonl"
        _write_jsonl(
            data_path,
            [
                {
                    "id": "https://openalex.org/W1",
                    "author_id": "https://openalex.org/A1",
                    "author_openalex_id": "A2",
                    "landing_page_url": "https://openalex.org/W9",
                },
                {
                    "id": "https://openalex.org/W2",
                    "author_id": "https://ror.org/03yrm5c26",
                },
            ],
        )
        return {
            "PATH": td,
            "file_name": "records.jsonl",
            "file_type": "jsonl",
            "table_name": "works",
            "id_compaction": {"enabled": True},
        }

    def test_preflight_collects_collision_namespace_conflict_and_ambiguous_columns(self):
        with tempfile.TemporaryDirectory() as td:
            result = run_id_compaction_preflight(self._config(td), max_records=10)

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["input"]["records_scanned"], 2)
        self.assertEqual(result["issues"]["collisions"]["works.author_openalex_id"], 1)
        self.assertEqual(result["issues"]["namespace_conflicts"]["works.author_id"], 1)
        self.assertEqual(result["issues"]["ambiguous_columns"]["works.landing_page_url"], 1)
        self.assertEqual(result["examples"]["collisions"]["works.author_openalex_id"][0]["line_no"], 1)

    def test_preflight_can_be_explicitly_disabled(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = self._config(td)
            cfg["id_compaction"] = {"enabled": False}
            result = run_id_compaction_preflight(cfg, max_records=10, force_enable=False)

        self.assertEqual(result["status"], "disabled")
        self.assertEqual(result["input"]["records_scanned"], 0)

    def test_cli_id_compaction_preflight_writes_report_and_uses_exit_code(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            report_path = f"{td}/preflight.json"
            cfg = {"data_config": self._config(td), "db_config": {}}
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            with redirect_stdout(io.StringIO()):
                rc_failed = main(
                    [
                        "json",
                        "id-compaction-preflight",
                        "--config",
                        cfg_path,
                        "--report",
                        report_path,
                        "--max-records",
                        "10",
                    ]
                )
            with redirect_stdout(io.StringIO()):
                rc_allowed = main(
                    [
                        "json",
                        "id-compaction-preflight",
                        "--config",
                        cfg_path,
                        "--allow-issues",
                        "--max-records",
                        "10",
                    ]
                )

            self.assertEqual(rc_failed, 1)
            self.assertEqual(rc_allowed, 0)
            with open(report_path, encoding="utf-8") as f:
                report = json.loads(f.read())
            self.assertEqual(report["status"], "failed")
            self.assertIn("works.author_openalex_id", report["issues"]["collisions"])


if __name__ == "__main__":
    unittest.main()
