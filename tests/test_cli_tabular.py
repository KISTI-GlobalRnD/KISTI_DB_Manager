import io
import json
import tempfile
import unittest
import warnings
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


from KISTI_DB_Manager.cli import main
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
