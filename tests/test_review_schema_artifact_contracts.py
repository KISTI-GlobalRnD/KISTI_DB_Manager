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
        profiles_dir = root / "profiles"
        profiles_dir.mkdir()

        def write_table_profile(table: str, row_count: int, columns: list[dict]) -> str:
            path = profiles_dir / f"{table}_profile.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": "2.0",
                        "backend": "python",
                        "source": {
                            "file": str(root / f"{table}.parquet"),
                            "row_count": row_count,
                            "table_name": table,
                        },
                        "columns": columns,
                        "warnings": [],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            return str(path.relative_to(root))

        profile_paths = [
            write_table_profile(
                "works",
                3,
                [
                    {
                        "source_column": "id",
                        "sql_column": "id",
                        "suggested_type": "VARCHAR(64)",
                        "type_family": "string",
                        "null_ratio": 0.0,
                        "unique_ratio": 1.0,
                        "is_key_candidate": True,
                        "index_recommended": True,
                        "warnings": "",
                    },
                    {
                        "source_column": "title",
                        "sql_column": "title",
                        "suggested_type": "LONGTEXT",
                        "type_family": "string",
                        "null_ratio": 0.33,
                        "unique_ratio": 0.66,
                        "is_key_candidate": False,
                        "index_recommended": False,
                        "warnings": "contains_nulls",
                    },
                ],
            ),
            write_table_profile(
                "works__authorships",
                5,
                [
                    {
                        "source_column": "id",
                        "sql_column": "id",
                        "suggested_type": "VARCHAR(64)",
                        "type_family": "string",
                        "null_ratio": 0.0,
                        "unique_ratio": 0.6,
                        "is_key_candidate": False,
                        "index_recommended": True,
                        "warnings": "",
                    },
                    {
                        "source_column": "author_id",
                        "sql_column": "author_id",
                        "suggested_type": "VARCHAR(64)",
                        "type_family": "string",
                        "null_ratio": 0.0,
                        "unique_ratio": 0.8,
                        "is_key_candidate": False,
                        "index_recommended": True,
                        "warnings": "",
                    },
                ],
            ),
            write_table_profile(
                "works__authorships__institutions",
                7,
                [
                    {
                        "source_column": "institution__id",
                        "sql_column": "institution__id",
                        "suggested_type": "VARCHAR(64)",
                        "type_family": "string",
                        "null_ratio": 0.1,
                        "unique_ratio": 0.7,
                        "is_key_candidate": False,
                        "index_recommended": True,
                        "warnings": "",
                    }
                ],
            ),
        ]
        path = root / "dataset_profile.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "backend": "python",
                    "source": {"profile_count": 3, "profile_paths": profile_paths},
                    "dataset": {"base_table": "works", "base_table_sql": "works", "key_sep": "__"},
                    "tables": [
                        {"table_sql": "works", "table_original": "works", "row_count": 3},
                        {
                            "table_sql": "works__authorships",
                            "table_original": "works__authorships",
                            "row_count": 5,
                        },
                        {
                            "table_sql": "works__authorships__institutions",
                            "table_original": "works__authorships__institutions",
                            "row_count": 7,
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
                            "confidence_bucket": "high",
                            "review_priority": "accept_hint",
                            "risk_score": 0.2,
                            "status": "candidate",
                        },
                        {
                            "parent_table_sql": "works",
                            "child_table_sql": "works__authorships__institutions",
                            "parent_column_sql": "id",
                            "child_column_sql": "institution__id",
                            "relationship_type": "naming_candidate_only",
                            "confidence": 0.42,
                            "evidence": {
                                "source": "dataset_profile_review",
                                "parent_unique_ratio": 1.0,
                                "child_null_ratio": 0.1,
                                "shared_column_name": False,
                            },
                            "warnings": ["candidate_without_structural_edge"],
                            "confidence_bucket": "low",
                            "review_priority": "high_risk",
                            "risk_score": 0.73,
                            "status": "candidate",
                        }
                    ],
                    "audit": {
                        "mode": "profile_only",
                        "data_scan": "not_performed",
                        "candidate_count": 2,
                        "confidence_buckets": {"high": 1, "low": 1},
                        "review_priority_counts": {"accept_hint": 1, "high_risk": 1},
                        "candidate_warning_count": 1,
                        "warning_counts": {"candidate_without_structural_edge": 1},
                        "skipped_candidate_count": 0,
                        "skip_reason_counts": {},
                        "value_overlap": {"status": "not_computed", "reason": "disabled_by_default"},
                    },
                    "warnings": [],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return path

    def _write_relationship_decisions(self, root: Path) -> Path:
        path = root / "relationship_decisions.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "decisions": [
                        {
                            "parent_table_sql": "works",
                            "child_table_sql": "works__authorships",
                            "parent_column_sql": "id",
                            "child_column_sql": "id",
                            "decision": "accepted",
                            "reason": "authorship rows carry the parent work id",
                            "reviewed_by": "operator",
                            "reviewed_at": "2026-05-20",
                        },
                        {
                            "parent_table_sql": "works",
                            "child_table_sql": "works__authorships__institutions",
                            "parent_column_sql": "id",
                            "child_column_sql": "institution__id",
                            "decision": "rejected",
                            "reason": "institution id is not a work id",
                            "reviewed_by": "operator",
                            "reviewed_at": "2026-05-20",
                        },
                    ],
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
            self.assertEqual(payload["meta"]["dataset_profile"]["relationship_candidate_count"], 2)
            self.assertEqual(payload["meta"]["dataset_profile"]["table_profile_count_loaded"], 3)
            self.assertEqual(payload["meta"]["dataset_profile"]["table_profile_column_count_loaded"], 5)
            self.assertEqual(payload["summary"]["relationship_candidate_count"], 2)
            self.assertEqual(payload["summary"]["relationship_candidates_on_edges"], 2)
            self.assertEqual(payload["summary"]["candidate_only_edge_count"], 1)
            self.assertEqual(payload["summary"]["unmatched_relationship_candidate_count"], 0)
            self.assertEqual(payload["dataset_profile"]["audit"]["review_priority_counts"]["high_risk"], 1)
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
            self.assertEqual(edge["relationship_review_priority"], "accept_hint")
            self.assertEqual(edge["relationship_review_priority_counts"], {"accept_hint": 1})
            self.assertFalse(edge["relationship_needs_review"])
            self.assertEqual(edge["relationship_key_match_sources"], [])
            self.assertEqual(edge["parent_column_sql"], "id")
            self.assertEqual(edge["child_column_sql"], "id")
            self.assertEqual(edge["relationship_candidates"][0]["evidence"]["source"], "table_name_path")
            candidate_only_edge = next(
                item
                for item in payload["edges"]
                if item["parent_sql"] == "works"
                and item["child_sql"] == "works__authorships__institutions"
            )
            self.assertEqual(candidate_only_edge["relationship_source"], "dataset_profile")
            self.assertEqual(candidate_only_edge["relationship_status"], "candidate")
            self.assertEqual(candidate_only_edge["relationship_type"], "naming_candidate_only")
            self.assertEqual(candidate_only_edge["relationship_warning_count"], 1)
            self.assertEqual(candidate_only_edge["relationship_review_priority"], "high_risk")
            self.assertEqual(candidate_only_edge["relationship_review_priority_counts"], {"high_risk": 1})
            self.assertTrue(candidate_only_edge["relationship_needs_review"])
            self.assertEqual(candidate_only_edge["parent_column_sql"], "id")
            self.assertEqual(candidate_only_edge["child_column_sql"], "institution__id")
            self.assertIn("candidate-edge", (out_dir / "schema.svg").read_text(encoding="utf-8"))
            self.assertIn("works -.->|candidate| works__authorships__institutions", (out_dir / "schema.mmd").read_text(encoding="utf-8"))
            works = next(table for table in payload["tables"] if table["name_sql"] == "works")
            authorships = next(
                table for table in payload["tables"] if table["name_sql"] == "works__authorships"
            )
            self.assertEqual(works["relationship_candidate_count"], 2)
            self.assertEqual(works["relationship_review_priority_counts"], {"accept_hint": 1, "high_risk": 1})
            self.assertEqual(works["relationship_needs_review_count"], 1)
            self.assertEqual(authorships["relationship_candidate_count"], 1)
            self.assertEqual(authorships["relationship_needs_review_count"], 0)
            self.assertFalse(works["is_disconnected"])
            self.assertFalse(authorships["is_disconnected"])
            html = (out_dir / "schema_viewer.html").read_text(encoding="utf-8")
            self.assertIn("Relation candidates", html)
            self.assertIn("Relationship Catalog", html)
            self.assertIn("Relationship Evidence", html)
            self.assertIn("Coverage Gaps", html)
            self.assertIn("Candidate-backed", html)
            self.assertIn("Needs review", html)
            self.assertIn("dataset_profile", html)
            self.assertIn("dataset profile overlay", html)
            self.assertIn("table_name_path", html)
            self.assertIn("only-needs-review", html)
            self.assertIn("high risk", html)
            self.assertIn("relationship_needs_review", html)
            self.assertIn("relationship-search", html)
            self.assertIn("relationship-key-source", html)
            self.assertIn("Parent table", html)
            self.assertIn("institution__id", html)

    def test_schema_viewer_surfaces_dataset_profile_value_overlap(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_path = self._write_config(root)
            out_dir = root / "out"
            dataset_profile_path = self._write_dataset_profile(root)
            dataset_profile = json.loads(dataset_profile_path.read_text(encoding="utf-8"))
            dataset_profile["relationship_candidates"][0]["value_overlap"] = {
                "status": "sampled_passed_hint",
                "sampled_max_rows": 1000,
                "parent_sampled_rows": 3,
                "child_sampled_rows": 5,
                "parent_distinct_count": 3,
                "child_non_null_count": 5,
                "child_distinct_count": 3,
                "overlap_distinct_count": 3,
                "orphan_distinct_count": 0,
                "overlap_ratio": 1.0,
                "orphan_ratio": 0.0,
                "parent_coverage_ratio": 1.0,
            }
            dataset_profile["audit"]["data_scan"] = "sampled"
            dataset_profile["audit"]["value_overlap"] = {
                "status": "computed",
                "mode": "candidate_key_sample",
                "candidate_count": 2,
                "computed_candidate_count": 1,
                "skipped_candidate_count": 1,
                "error_count": 0,
                "sampled_max_rows": 1000,
                "max_candidates": 1,
                "status_counts": {"sampled_passed_hint": 1},
            }
            dataset_profile_path.write_text(json.dumps(dataset_profile, ensure_ascii=False), encoding="utf-8")

            generate_schema_viewer(
                config_path=str(config_path),
                report_path=str(FIXTURE_DIR / "report.json"),
                out_dir=str(out_dir),
                db_enabled=False,
            )

            payload = json.loads((out_dir / "schema_viewer.json").read_text(encoding="utf-8"))
            edge = next(
                item
                for item in payload["edges"]
                if item["parent_sql"] == "works" and item["child_sql"] == "works__authorships"
            )
            value_overlap = edge["relationship_candidates"][0]["value_overlap"]
            self.assertEqual(value_overlap["status"], "sampled_passed_hint")
            self.assertEqual(value_overlap["overlap_ratio"], 1.0)
            self.assertEqual(value_overlap["orphan_ratio"], 0.0)
            self.assertEqual(payload["dataset_profile"]["audit"]["data_scan"], "sampled")
            self.assertEqual(payload["dataset_profile"]["audit"]["value_overlap"]["status"], "computed")
            html = (out_dir / "schema_viewer.html").read_text(encoding="utf-8")
            self.assertIn('"value_overlap": {', html)
            self.assertIn("sampled_passed_hint", html)
            self.assertIn("overlap ratio", html)
            self.assertIn("orphan ratio", html)

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
            self.assertEqual(payload["summary"]["table_count"], 3)
            self.assertEqual(payload["summary"]["columns_total"], 5)
            self.assertEqual(payload["summary"]["edge_count"], 3)
            self.assertEqual(payload["summary"]["structural_edge_count"], 2)
            self.assertEqual(payload["summary"]["candidate_only_edge_count"], 1)
            self.assertEqual(payload["summary"]["relationship_candidates_on_edges"], 2)
            self.assertEqual(payload["meta"]["dataset_profile"]["table_profile_count_loaded"], 3)
            self.assertEqual(payload["meta"]["dataset_profile"]["table_profile_column_count_loaded"], 5)
            self.assertEqual(payload["edges"][0]["relationship_source"], "dataset_profile")
            self.assertEqual(payload["edges"][0]["relationship_status"], "candidate")
            self.assertEqual(
                [table["name_sql"] for table in payload["tables"]],
                ["works", "works__authorships", "works__authorships__institutions"],
            )
            works = next(table for table in payload["tables"] if table["name_sql"] == "works")
            self.assertEqual(works["column_count"], 2)
            self.assertEqual(works["columns"][0]["column_key"], "MUL")
            self.assertEqual(works["columns"][0]["description_profile"]["suggested_type"], "VARCHAR(64)")
            self.assertEqual(works["columns"][1]["description_profile"]["warnings"], "contains_nulls")
            html = (out_dir / "schema_viewer.html").read_text(encoding="utf-8")
            self.assertIn("relationship-sort", html)
            self.assertIn("Candidate evidence", html)

    def test_schema_viewer_overlays_relationship_decisions(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config_path = self._write_config(root)
            self._write_dataset_profile(root)
            decisions_path = self._write_relationship_decisions(root)
            out_dir = root / "out"

            generate_schema_viewer(
                config_path=str(config_path),
                out_dir=str(out_dir),
                db_enabled=False,
                relationship_decisions_path=str(decisions_path),
            )

            payload = json.loads((out_dir / "schema_viewer.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["meta"]["relationship_decisions"]["decision_count"], 2)
            self.assertEqual(
                payload["summary"]["relationship_decision_counts"],
                {"accepted": 1, "rejected": 1},
            )
            self.assertEqual(payload["summary"]["relationship_decisions_on_edges"], 2)
            accepted_edge = next(
                item
                for item in payload["edges"]
                if item["parent_sql"] == "works" and item["child_sql"] == "works__authorships"
            )
            self.assertEqual(accepted_edge["relationship_decision_status"], "accepted")
            self.assertEqual(accepted_edge["relationship_decision_count"], 1)
            self.assertEqual(
                accepted_edge["relationship_decisions"][0]["reason"],
                "authorship rows carry the parent work id",
            )
            rejected_edge = next(
                item
                for item in payload["edges"]
                if item["parent_sql"] == "works"
                and item["child_sql"] == "works__authorships__institutions"
            )
            self.assertEqual(rejected_edge["relationship_decision_status"], "rejected")
            works = next(table for table in payload["tables"] if table["name_sql"] == "works")
            self.assertEqual(works["relationship_decision_count"], 2)
            self.assertEqual(works["relationship_decision_counts"], {"accepted": 1, "rejected": 1})
            html = (out_dir / "schema_viewer.html").read_text(encoding="utf-8")
            self.assertIn("Operator decisions", html)
            self.assertIn("relationshipDecisionLabel", html)
            self.assertIn('"decision": "accepted"', html)
            self.assertIn('"decision": "rejected"', html)
            self.assertIn("authorship rows carry the parent work id", html)


if __name__ == "__main__":
    unittest.main()
