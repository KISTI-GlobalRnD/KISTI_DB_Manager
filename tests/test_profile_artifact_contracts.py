import json
import tempfile
import unittest
from pathlib import Path

from KISTI_DB_Manager.dataset_profile import build_dataset_profile
from KISTI_DB_Manager.description_profile import build_description_profile
from KISTI_DB_Manager.namemap import NameMap


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "profile_contract"


def _load_json(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _normalize_description_profile(profile: dict) -> dict:
    normalized = json.loads(json.dumps(profile, ensure_ascii=False, sort_keys=True))
    normalized["generated_at"] = "<generated_at>"
    normalized["source"]["file"] = "<source_file>"
    return normalized


def _normalize_dataset_profile(profile: dict) -> dict:
    normalized = json.loads(json.dumps(profile, ensure_ascii=False, sort_keys=True))
    normalized["source"]["profile_paths"] = [
        f"<profile:{Path(path).name}>" for path in normalized["source"]["profile_paths"]
    ]
    return normalized


def _table_profile_payload(table_name: str, *, row_count: int = 4) -> dict:
    nm = NameMap.build(table_name=table_name, columns=["id", "value"], key_sep="__")
    columns = []
    for source, sql in zip(nm.columns_original, nm.columns_sql):
        columns.append(
            {
                "source_column": source,
                "sql_column": sql,
                "suggested_type": "INT" if source == "id" else "VARCHAR(16)",
                "type_family": "integer" if source == "id" else "string",
                "row_count": row_count,
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
        "source": {"file": f"/input/{table_name}.csv", "row_count": row_count, "table_name": table_name},
        "name_map": nm.to_dict(),
        "columns": columns,
        "warnings": [],
    }


class TestProfileArtifactContracts(unittest.TestCase):
    maxDiff = None

    def test_description_profile_v2_matches_golden_artifacts(self):
        try:
            import pandas as pd  # noqa: F401
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pandas is required: {exc}")

        desc, profile, _name_map = build_description_profile(
            {
                "PATH": str(FIXTURE_DIR),
                "file_name": "sample.csv",
                "file_type": "csv",
                "table_name": "sample",
                "SEP": ",",
                "KEYs": ["id"],
            }
        )

        expected_profile = _load_json("expected_description_profile_v2.json")
        expected_desc = (FIXTURE_DIR / "expected_description_desc_v2.csv").read_text(encoding="utf-8")

        self.assertEqual(_normalize_description_profile(profile), expected_profile)
        self.assertEqual(desc.to_csv(), expected_desc)

    def test_dataset_profile_v1_matches_golden_artifact(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            profile_paths = []
            for table_name in ("works", "works__authorships"):
                path = root / f"{table_name}_profile.json"
                path.write_text(
                    json.dumps(_table_profile_payload(table_name), ensure_ascii=False),
                    encoding="utf-8",
                )
                profile_paths.append(path)

            profile = build_dataset_profile(
                profile_paths,
                base_table="works",
                key_sep="__",
                generated_at="fixed",
            )

        expected_profile = _load_json("expected_dataset_profile_v1.json")
        self.assertEqual(_normalize_dataset_profile(profile), expected_profile)


if __name__ == "__main__":
    unittest.main()
