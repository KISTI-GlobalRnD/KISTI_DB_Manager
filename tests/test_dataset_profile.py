import json
import tempfile
import unittest
from pathlib import Path

from KISTI_DB_Manager.dataset_profile import build_dataset_profile, resolve_profile_paths
from KISTI_DB_Manager.namemap import NameMap


def _profile_payload(table_name: str, *, row_count: int = 10, include_id: bool = True) -> dict:
    columns = []
    column_names = ["id", "value"] if include_id else ["value"]
    nm = NameMap.build(table_name=table_name, columns=column_names, key_sep="__")
    for source, sql in zip(nm.columns_original, nm.columns_sql):
        columns.append(
            {
                "source_column": source,
                "sql_column": sql,
                "suggested_type": "INT" if source == "id" else "VARCHAR(16)",
                "type_family": "integer" if source == "id" else "string",
                "null_ratio": 0.0,
                "unique_ratio": 1.0 if source == "id" else 0.5,
                "is_key_candidate": source == "id",
                "index_recommended": source == "id",
                "warnings": "",
            }
        )
    return {
        "schema_version": "2.0",
        "backend": "python",
        "source": {"file": f"/tmp/{table_name}.csv", "row_count": row_count, "table_name": table_name},
        "name_map": nm.to_dict(),
        "columns": columns,
        "warnings": [],
    }


def _write_profile(root: Path, table_name: str, **kwargs) -> Path:
    path = root / f"{table_name}_profile.json"
    path.write_text(json.dumps(_profile_payload(table_name, **kwargs), ensure_ascii=False), encoding="utf-8")
    return path


class TestDatasetProfile(unittest.TestCase):
    def test_build_dataset_profile_summarizes_tables_and_naming_relationships(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = [
                _write_profile(root, "works__authorships__institutions", row_count=20),
                _write_profile(root, "works", row_count=10),
                _write_profile(root, "works__authorships", row_count=30),
            ]

            profile = build_dataset_profile(paths, base_table="works", key_sep="__", generated_at="fixed")

            self.assertEqual(profile["schema_version"], "1.0")
            self.assertEqual(profile["dataset"]["base_table"], "works")
            self.assertEqual([table["table_sql"] for table in profile["tables"]], [
                "works",
                "works__authorships",
                "works__authorships__institutions",
            ])
            self.assertEqual(profile["tables"][0]["key_candidates"], ["id"])
            self.assertEqual(len(profile["relationship_candidates"]), 2)
            first = profile["relationship_candidates"][0]
            self.assertEqual(first["parent_table_sql"], "works")
            self.assertEqual(first["child_table_sql"], "works__authorships")
            self.assertEqual(first["status"], "candidate")
            self.assertEqual(first["relationship_type"], "naming_parent_child")
            self.assertEqual(first["evidence"]["source"], "table_name_path")

    def test_build_dataset_profile_skips_naming_relationship_without_id_columns(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = [
                _write_profile(root, "works", include_id=True),
                _write_profile(root, "works__keywords", include_id=False),
            ]

            profile = build_dataset_profile(paths, base_table="works", key_sep="__", generated_at="fixed")

            self.assertEqual(profile["relationship_candidates"], [])

    def test_resolve_profile_paths_accepts_directory_and_glob(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            p1 = _write_profile(root, "works")
            p2 = _write_profile(root, "works__authorships")

            from_dir = resolve_profile_paths([str(root)])
            from_glob = resolve_profile_paths([str(root / "*_profile.json")])

            self.assertEqual(from_dir, sorted([p1.resolve(), p2.resolve()]))
            self.assertEqual(from_glob, sorted([p1.resolve(), p2.resolve()]))


if __name__ == "__main__":
    unittest.main()
