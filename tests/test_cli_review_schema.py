import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path


from KISTI_DB_Manager.cli import main
from KISTI_DB_Manager.namemap import NameMap


class TestCLIReviewSchema(unittest.TestCase):
    def test_review_schema_viewer_writes_outputs_without_db(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            out_dir = Path(td, "out")

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "tbl",
                    "KEY_SEP": "__",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = main(["review", "schema-viewer", "--config", str(cfg_path), "--out", str(out_dir), "--no-db"])

            self.assertEqual(rc, 0)
            self.assertTrue((out_dir / "schema_viewer.html").exists())
            self.assertTrue((out_dir / "schema_viewer.json").exists())
            self.assertTrue((out_dir / "schema.svg").exists())
            self.assertTrue((out_dir / "schema.mmd").exists())
            html = (out_dir / "schema_viewer.html").read_text(encoding="utf-8")
            self.assertIn("Schema Viewer", html)
            self.assertIn("Table Catalog", html)

    def test_review_schema_viewer_payload_includes_relationship_context(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td, "config.json")
            report_path = Path(td, "run_report.json")
            out_dir = Path(td, "out")

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.jsonl",
                    "file_type": "jsonl",
                    "table_name": "works",
                    "KEY_SEP": "__",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
            name_maps = {
                name: NameMap.build(table_name=name, columns=["id", "value"], key_sep="__").to_dict()
                for name in ("works", "works__authorships", "works__authorships__institutions")
            }
            report_path.write_text(
                json.dumps({"artifacts": {"name_maps_json": name_maps}}, ensure_ascii=False),
                encoding="utf-8",
            )

            rc = main(
                [
                    "review",
                    "schema-viewer",
                    "--config",
                    str(cfg_path),
                    "--report",
                    str(report_path),
                    "--out",
                    str(out_dir),
                    "--no-db",
                ]
            )

            self.assertEqual(rc, 0)
            payload = json.loads((out_dir / "schema_viewer.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["summary"]["edge_count"], 2)
            edges = {(edge["parent_sql"], edge["child_sql"]): edge for edge in payload["edges"]}
            self.assertIn(("works", "works__authorships"), edges)
            self.assertIn(("works__authorships", "works__authorships__institutions"), edges)
            nested = next(
                table for table in payload["tables"] if table["name_sql"] == "works__authorships__institutions"
            )
            self.assertEqual(nested["relationship_count"], 1)
            self.assertEqual(nested["parent_edges"][0]["parent_sql"], "works__authorships")
            self.assertIn("LEFT JOIN `works__authorships__institutions`", nested["join_sql"])
            html = (out_dir / "schema_viewer.html").read_text(encoding="utf-8")
            self.assertIn("Relationships (", html)


if __name__ == "__main__":
    unittest.main()
