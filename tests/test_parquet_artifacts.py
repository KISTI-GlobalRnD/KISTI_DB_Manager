import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from KISTI_DB_Manager.parquet_artifacts import artifact_contract_from_plan, inspect_parquet_artifact_contract


def _write_parquet(path: Path, rows: list[dict]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def _write_manifest(root: Path, *, table: str = "works") -> None:
    payload = {
        "generated_at": "2026-01-01T00:00:00+00:00",
        "id_compaction": {
            "enabled": True,
            "preset": "openalex",
            "mode": "semantic_column_strip",
            "rules_version": "openalex-semantic-column-strip-v2",
            "rules_hash": "abc123",
            "columns": [
                {
                    "table": table,
                    "source_column": "author_id",
                    "new_column": "author_openalex_id",
                }
            ],
            "collisions": {},
            "namespace_conflicts": {},
            "ambiguous_columns": {},
        },
        "tables": {
            table: {
                "columns": {
                    "author_openalex_id": {
                        "sql_column": "author_openalex_id",
                        "source_column": "author_id",
                        "id_namespace": "openalex",
                        "removed_prefix": "https://openalex.org/",
                        "description": "OpenAlex Author ID.",
                        "count": 1,
                    }
                }
            }
        },
    }
    (root / "schema_manifest.json").write_text(json.dumps(payload), encoding="utf-8")


class TestParquetArtifacts(unittest.TestCase):
    def test_inspect_passes_with_manifest_and_compacted_schema(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            _write_parquet(root / "works" / "b000000.parquet", [{"id": "W1", "author_openalex_id": "A1"}])
            _write_manifest(root)

            report = inspect_parquet_artifact_contract(
                root,
                table_names=["works"],
                require_schema_manifest=True,
                require_id_compaction=True,
                strict_schema_manifest=True,
            )

            self.assertEqual(report["status"], "done")
            self.assertEqual(report["summary"]["id_compaction_rules_version"], "openalex-semantic-column-strip-v2")
            self.assertEqual(report["summary"]["id_compaction_rules_hash"], "abc123")
            self.assertEqual(report["tables"]["works"]["row_count"], 1)
            self.assertEqual(report["tables"]["works"]["source_and_compacted_column_count"], 0)

    def test_missing_manifest_warns_by_default_and_fails_when_required(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            _write_parquet(root / "works" / "b000000.parquet", [{"id": "W1"}])

            warning_report = inspect_parquet_artifact_contract(root)
            strict_report = inspect_parquet_artifact_contract(root, require_schema_manifest=True)

            self.assertEqual(warning_report["status"], "done_with_warnings")
            self.assertEqual(strict_report["status"], "failed")
            self.assertEqual(strict_report["issues"][0]["check"], "schema_manifest_missing")

    def test_source_and_compacted_columns_fail_when_id_compaction_required(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            _write_parquet(
                root / "works" / "b000000.parquet",
                [{"id": "W1", "author_id": "https://openalex.org/A1", "author_openalex_id": "A1"}],
            )
            _write_manifest(root)

            report = inspect_parquet_artifact_contract(root, require_id_compaction=True)

            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["issues"][0]["check"], "mixed_compacted_and_source_columns")

    def test_plan_uses_preflight_artifact_contract_options(self):
        with TemporaryDirectory() as td:
            root = Path(td)
            parquet_root = root / "parquet"
            _write_parquet(parquet_root / "works" / "b000000.parquet", [{"id": "W1"}])
            plan = {
                "run_dir": str(root),
                "parquet_root": str(parquet_root),
                "preflight": {"artifact_contract": {"require_schema_manifest": True}},
            }

            report = artifact_contract_from_plan(plan, table_names=["works"])

            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["issues"][0]["check"], "schema_manifest_missing")


if __name__ == "__main__":
    unittest.main()
