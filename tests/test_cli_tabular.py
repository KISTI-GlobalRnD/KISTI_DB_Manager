import io
import json
import tempfile
import unittest
import warnings
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


from KISTI_DB_Manager.cli import main
from KISTI_DB_Manager.description_profile import build_description_profile
from KISTI_DB_Manager.namemap import NameMap
from KISTI_DB_Manager.pipeline import TabularRunResult
from KISTI_DB_Manager.report import RunReport


class TestCLITabular(unittest.TestCase):
    def test_tabular_describe_writes_v2_desc_and_profile(self):
        try:
            import pandas as pd
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pandas is required: {exc}")

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            csv_path = root / "sample.csv"
            cfg_path = root / "config.json"
            desc_path = root / "sample_desc_v2.csv"
            profile_path = root / "sample_profile.json"
            csv_path.write_text(
                "\n".join(
                    [
                        "id,name,value,created_at",
                        "1,Alice,1.23,2024-01-01 12:00:00",
                        "2,Bob,4.56,not-a-date",
                        "3,Charlie,,2024-02-03 09:10:11",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            cfg = {
                "data_config": {
                    "PATH": str(root),
                    "file_name": csv_path.name,
                    "file_type": "csv",
                    "table_name": "sample",
                    "SEP": ",",
                    "KEYs": ["id"],
                }
            }
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

            buf = io.StringIO()
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                with redirect_stdout(buf):
                    rc = main(
                        [
                            "tabular",
                            "describe",
                            "--config",
                            str(cfg_path),
                            "--out-desc",
                            str(desc_path),
                            "--out-profile",
                            str(profile_path),
                            "--backend",
                            "python",
                        ]
                    )

            self.assertEqual(rc, 0)
            self.assertFalse([item for item in caught if issubclass(item.category, UserWarning)])
            self.assertTrue(desc_path.exists())
            self.assertTrue(profile_path.exists())
            desc = pd.read_csv(desc_path, index_col=0)
            self.assertIn("suggested_type", desc.columns)
            self.assertIn("Type", desc.columns)
            self.assertEqual(desc.loc["id", "is_key"], True)
            self.assertEqual(desc.loc["value", "suggested_type"], "DOUBLE")
            self.assertIn("mixed_datetime_parse_success", str(desc.loc["created_at", "warnings"]))
            profile = json.loads(profile_path.read_text(encoding="utf-8"))
            self.assertEqual(profile["schema_version"], "2.0")
            self.assertEqual(profile["source"]["row_count"], 3)
            self.assertEqual(len(profile["columns"]), 4)
            summary = json.loads(buf.getvalue())
            self.assertEqual(summary["status"], "done")
            self.assertEqual(summary["column_count"], 4)

    def test_description_profile_keeps_dot_collision_columns_unique(self):
        try:
            import pandas as pd  # noqa: F401
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pandas is required: {exc}")

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            csv_path = root / "sample.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "a.b,a__b",
                        "1,2",
                        "3,4",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            data_config = {
                "PATH": str(root),
                "file_name": csv_path.name,
                "file_type": "csv",
                "table_name": "sample",
                "SEP": ",",
            }

            desc, profile, nm = build_description_profile(data_config)

            self.assertEqual(list(desc.index), ["a__b__dot", "a__b__raw"])
            self.assertEqual(nm.columns_original, ("a__b__dot", "a__b__raw"))
            self.assertEqual([row["source_column"] for row in profile["columns"]], ["a__b__dot", "a__b__raw"])

    def test_description_profile_resolves_forced_keys_against_raw_columns_first(self):
        try:
            import pandas as pd  # noqa: F401
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pandas is required: {exc}")

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            csv_path = root / "sample.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "a.b,a__b",
                        "same,forced-1",
                        "same,forced-2",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            data_config = {
                "PATH": str(root),
                "file_name": csv_path.name,
                "file_type": "csv",
                "table_name": "sample",
                "SEP": ",",
                "KEYs": ["a__b"],
            }

            desc, _profile, _nm = build_description_profile(data_config)

            self.assertEqual(bool(desc.loc["a__b__dot", "is_key_candidate"]), False)
            self.assertEqual(bool(desc.loc["a__b__raw", "is_key_candidate"]), True)

    def test_tabular_profile_dataset_writes_dataset_profile(self):
        def profile_payload(table_name):
            nm = NameMap.build(table_name=table_name, columns=["id", "value"], key_sep="__")
            return {
                "schema_version": "2.0",
                "backend": "python",
                "source": {"file": f"/tmp/{table_name}.csv", "row_count": 3, "table_name": table_name},
                "name_map": nm.to_dict(),
                "columns": [
                    {
                        "source_column": "id",
                        "sql_column": "id",
                        "suggested_type": "INT",
                        "type_family": "integer",
                        "null_ratio": 0.0,
                        "unique_ratio": 1.0,
                        "is_key_candidate": True,
                        "index_recommended": True,
                        "warnings": "",
                    },
                    {
                        "source_column": "value",
                        "sql_column": "value",
                        "suggested_type": "VARCHAR(16)",
                        "type_family": "string",
                        "null_ratio": 0.0,
                        "unique_ratio": 0.5,
                        "is_key_candidate": False,
                        "index_recommended": False,
                        "warnings": "",
                    },
                ],
                "warnings": [],
            }

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            works = root / "works_profile.json"
            authorships = root / "works__authorships_profile.json"
            out_path = root / "dataset_profile.json"
            works.write_text(json.dumps(profile_payload("works"), ensure_ascii=False), encoding="utf-8")
            authorships.write_text(json.dumps(profile_payload("works__authorships"), ensure_ascii=False), encoding="utf-8")

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = main(
                    [
                        "tabular",
                        "profile-dataset",
                        "--profile",
                        str(authorships),
                        "--profile",
                        str(works),
                        "--base-table",
                        "works",
                        "--out",
                        str(out_path),
                    ]
                )

            self.assertEqual(rc, 0)
            saved = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["schema_version"], "1.0")
            self.assertEqual(len(saved["tables"]), 2)
            self.assertEqual(len(saved["relationship_candidates"]), 1)
            summary = json.loads(buf.getvalue())
            self.assertEqual(summary["status"], "done")
            self.assertEqual(summary["relationship_candidate_count"], 1)

    def test_tabular_run_writes_report(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = f"{td}/config.json"
            report_path = f"{td}/report.json"

            cfg = {
                "data_config": {
                    "PATH": "data/",
                    "file_name": "x.csv",
                    "file_type": "csv",
                    "table_name": "tbl",
                },
                "db_config": {"host": "h", "user": "u", "password": "p", "database": "d"},
            }
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(cfg))

            fake_report = RunReport()
            fake_report.warn(stage="t", message="w")

            with patch(
                "KISTI_DB_Manager.pipeline.run_tabular_pipeline",
                return_value=TabularRunResult(name_map=None, report=fake_report),
            ), patch("KISTI_DB_Manager.cli._ensure_optional_deps", return_value=None):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = main(["tabular", "run", "--config", cfg_path, "--report", report_path])

            self.assertEqual(rc, 0)
            with open(report_path, encoding="utf-8") as f:
                saved = json.loads(f.read())
            self.assertIn("run_id", saved)
            self.assertIn("issues", saved)


if __name__ == "__main__":
    unittest.main()
