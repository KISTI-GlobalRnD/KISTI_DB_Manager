import json
import re
import tempfile
import unittest
from pathlib import Path

from KISTI_DB_Manager.review_schema import generate_schema_viewer


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "review_schema_contract"


def _load_json(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _normalize_payload(payload: dict) -> dict:
    normalized = json.loads(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    normalized["meta"]["generated_at"] = "<generated_at>"
    normalized["meta"]["config"] = "<config>"
    normalized["meta"]["report"] = "<report>"
    if normalized.get("description_profile"):
        normalized["description_profile"]["source_file"] = "<description_source>"
    if normalized["meta"].get("description_profile"):
        normalized["meta"]["description_profile"]["source_file"] = "<description_source>"

    tables = []
    for table in normalized["tables"]:
        tables.append(
            {
                "name_sql": table["name_sql"],
                "display_short": table["display_short"],
                "role": table["role"],
                "depth": table["depth"],
                "column_count": table["column_count"],
                "relationship_count": table["relationship_count"],
                "issue_warning_count": table["issue_warning_count"],
                "join_sql": table["join_sql"],
                "columns": [
                    {
                        "name": col.get("name"),
                        "column_type": col.get("column_type"),
                        "column_key": col.get("column_key"),
                        "profile_warnings": (col.get("description_profile") or {}).get("warnings"),
                        "profile_type": (col.get("description_profile") or {}).get("suggested_type"),
                    }
                    for col in table.get("columns", [])
                ],
                "parent_edges": [
                    {
                        "parent_sql": edge["parent_sql"],
                        "child_sql": edge["child_sql"],
                        "label": edge["label"],
                    }
                    for edge in table.get("parent_edges", [])
                ],
                "child_edges": [
                    {
                        "parent_sql": edge["parent_sql"],
                        "child_sql": edge["child_sql"],
                        "label": edge["label"],
                    }
                    for edge in table.get("child_edges", [])
                ],
            }
        )

    return {
        "meta": normalized["meta"],
        "summary": normalized["summary"],
        "groups": normalized["groups"],
        "edges": [
            {
                "parent_sql": edge["parent_sql"],
                "child_sql": edge["child_sql"],
                "label": edge["label"],
                "join_sql": edge["join_sql"],
            }
            for edge in normalized["edges"]
        ],
        "tables": tables,
        "description_profile": normalized["description_profile"],
    }


def _svg_contract(svg: str) -> dict:
    marker_candidates = [
        'class="edge-card"',
        ">authorships · 1:N</text>",
        ">institutions · 1:N</text>",
        ">publication_year</text>",
    ]
    return {
        "node_sqls": re.findall(r'data-name-sql="([^"]+)"', svg),
        "edge_sqls": [
            {"parent_sql": parent, "child_sql": child}
            for parent, child in re.findall(
                r'data-parent-sql="([^"]+)" data-child-sql="([^"]+)"',
                svg,
            )
        ],
        "markers": [marker for marker in marker_candidates if marker in svg],
    }


def _html_markers(html: str) -> list[str]:
    marker_candidates = [
        "Schema Viewer",
        "Table Catalog",
        "renderColumnProfile",
        "contains_nulls",
        "works__authorships__institutions",
    ]
    return [marker for marker in marker_candidates if marker in html]


class TestReviewSchemaArtifactContracts(unittest.TestCase):
    maxDiff = None

    def _write_config(self, root: Path) -> Path:
        config_path = root / "config.json"
        config_path.write_text(
            json.dumps(
                {
                    "data_config": {
                        "PATH": str(root),
                        "file_name": "works.csv",
                        "file_type": "csv",
                        "table_name": "works",
                        "KEY_SEP": "__",
                    },
                    "db_config": {
                        "host": "h",
                        "user": "u",
                        "password": "secret",
                        "database": "openalex_contract",
                    },
                }
            ),
            encoding="utf-8",
        )
        return config_path

    def _write_dataset_profile(self, root: Path) -> Path:
        path = root / "dataset_profile.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "backend": "python",
                    "source": {"profile_count": 2, "profile_paths": []},
                    "dataset": {"base_table": "works", "base_table_sql": "works", "key_sep": "__"},
                    "tables": [
                        {"table_sql": "works", "table_original": "works", "row_count": 3},
                        {
                            "table_sql": "works__authorships",
                            "table_original": "works__authorships",
                            "row_count": 5,
                        },
                    ],
                    "relationship_candidates": [
                        {
                            "parent_table_sql": "works",
                            "child_table_sql": "works__authorships",
                            "parent_column_sql": "id",
                            "child_column_sql": "id",
                            "relationship_type": "naming_parent_child",
                            "confidence": 0.8,
                            "evidence": {
                                "source": "table_name_path",
                                "parent_unique_ratio": 1.0,
                                "child_null_ratio": 0.0,
                                "shared_column_name": True,
                            },
                            "warnings": [],
                            "status": "candidate",
                        }
                    ],
                    "warnings": [],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return path

    def test_schema_viewer_artifacts_match_golden_contract(self):
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "out"
            generate_schema_viewer(
                config_path=str(FIXTURE_DIR / "config.json"),
                report_path=str(FIXTURE_DIR / "report.json"),
                description_profile_path=str(FIXTURE_DIR / "description_profile.json"),
                out_dir=str(out_dir),
                db_enabled=False,
            )

            payload = json.loads((out_dir / "schema_viewer.json").read_text(encoding="utf-8"))
            contract = {
                "payload": _normalize_payload(payload),
                "mermaid": (out_dir / "schema.mmd").read_text(encoding="utf-8"),
                "svg": _svg_contract((out_dir / "schema.svg").read_text(encoding="utf-8")),
                "html_markers": _html_markers((out_dir / "schema_viewer.html").read_text(encoding="utf-8")),
            }

        self.assertEqual(contract, _load_json("expected_schema_artifact_contract.json"))

    def test_schema_viewer_auto_detects_dataset_profile_overlay(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_path = self._write_config(root)
            out_dir = root / "out"
            self._write_dataset_profile(root)

            generate_schema_viewer(
                config_path=str(config_path),
                report_path=str(FIXTURE_DIR / "report.json"),
                out_dir=str(out_dir),
                db_enabled=False,
            )

            payload = json.loads((out_dir / "schema_viewer.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["meta"]["dataset_profile"]["relationship_candidate_count"], 1)
            self.assertEqual(payload["summary"]["relationship_candidate_count"], 1)
            self.assertEqual(payload["summary"]["relationship_candidates_on_edges"], 1)
            edge = next(
                item
                for item in payload["edges"]
                if item["parent_sql"] == "works" and item["child_sql"] == "works__authorships"
            )
            self.assertEqual(edge["relationship_source"], "dataset_profile")
            self.assertEqual(edge["relationship_status"], "candidate")
            self.assertEqual(edge["relationship_type"], "naming_parent_child")
            self.assertEqual(edge["relationship_candidate_count"], 1)
            self.assertEqual(edge["relationship_warning_count"], 0)
            self.assertEqual(edge["relationship_candidates"][0]["evidence"]["source"], "table_name_path")
            works = next(table for table in payload["tables"] if table["name_sql"] == "works")
            authorships = next(
                table for table in payload["tables"] if table["name_sql"] == "works__authorships"
            )
            self.assertEqual(works["relationship_candidate_count"], 1)
            self.assertEqual(authorships["relationship_candidate_count"], 1)
            self.assertFalse(works["is_disconnected"])
            self.assertFalse(authorships["is_disconnected"])
            html = (out_dir / "schema_viewer.html").read_text(encoding="utf-8")
            self.assertIn("Relation candidates", html)
            self.assertIn("Relationship Evidence", html)
            self.assertIn("Coverage Gaps", html)
            self.assertIn("Candidate-backed", html)
            self.assertIn("dataset_profile", html)
            self.assertIn("dataset profile overlay", html)
            self.assertIn("table_name_path", html)

    def test_schema_viewer_uses_dataset_profile_as_no_db_table_source(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_path = self._write_config(root)
            self._write_dataset_profile(root)
            out_dir = root / "out"

            generate_schema_viewer(
                config_path=str(config_path),
                out_dir=str(out_dir),
                db_enabled=False,
            )

            payload = json.loads((out_dir / "schema_viewer.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["summary"]["table_count"], 2)
            self.assertEqual(payload["summary"]["edge_count"], 1)
            self.assertEqual(payload["summary"]["relationship_candidates_on_edges"], 1)
            self.assertEqual(payload["edges"][0]["relationship_source"], "dataset_profile")
            self.assertEqual(payload["edges"][0]["relationship_status"], "candidate")
            self.assertEqual([table["name_sql"] for table in payload["tables"]], ["works", "works__authorships"])


if __name__ == "__main__":
    unittest.main()
