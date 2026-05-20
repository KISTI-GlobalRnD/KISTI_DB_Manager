import tempfile
import unittest
from pathlib import Path

from KISTI_DB_Manager.rust_arrow_backend import (
    persist_json_lines_batch_to_parquet,
    rust_arrow_available,
)


@unittest.skipUnless(rust_arrow_available(), "kisti_json_rs extension is not installed")
class TestRustArrowExtensionSmoke(unittest.TestCase):
    def test_raw_jsonl_extension_writes_main_and_subtable_parquet(self):
        try:
            import pyarrow.parquet as pq
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"pyarrow is required: {exc}")

        lines = [
            (
                b'{"id":"W1","display_name":"Alpha","authorships":'
                b'[{"author_position":"first","author":{"id":"https://openalex.org/A1"}}]}'
            ),
            (
                b'{"id":"W2","display_name":"Beta","authorships":'
                b'[{"author_position":"last","author":{"id":"https://openalex.org/A2"}}]}'
            ),
        ]

        with tempfile.TemporaryDirectory() as td:
            parquet_dir = Path(td) / "parquet"
            result = persist_json_lines_batch_to_parquet(
                lines,
                base_table="works",
                index_key="id",
                except_keys=[],
                excepted_expand_dict=False,
                sep="__",
                parquet_dir=parquet_dir,
                batch_idx=0,
                index_offset=0,
                record_contexts=None,
                parallel_workers=0,
                columnar_accumulator=True,
                parser_backend="serde-json",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["effective_backend"], "rust-arrow")
            self.assertEqual(result["parser_backend"], "serde-json")
            self.assertEqual(result["records_ok"], 2)
            self.assertEqual(result["records_failed"], 0)
            self.assertEqual(result["parquet_tables_written"], 2)
            self.assertEqual(result["parquet_files_persisted"], 2)

            tables = {item["table"]: item for item in result["tables"]}
            self.assertEqual(sorted(tables), ["works", "works__authorships"])
            self.assertEqual(tables["works"]["rows"], 2)
            self.assertEqual(tables["works__authorships"]["rows"], 2)
            self.assertEqual(tables["works"]["columns"], ["id", "display_name"])
            self.assertEqual(
                tables["works__authorships"]["columns"],
                ["id", "authorships__author__id", "authorships__author_position"],
            )

            works = pq.read_table(parquet_dir / "works" / "b000000.parquet").to_pydict()
            authorships = pq.read_table(
                parquet_dir / "works__authorships" / "b000000.parquet"
            ).to_pydict()

        self.assertEqual(works["id"], ["W1", "W2"])
        self.assertEqual(works["display_name"], ["Alpha", "Beta"])
        self.assertEqual(authorships["id"], ["W1", "W2"])
        self.assertEqual(
            authorships["authorships__author__id"],
            ["https://openalex.org/A1", "https://openalex.org/A2"],
        )
        self.assertEqual(authorships["authorships__author_position"], ["first", "last"])

    def test_simd_json_feature_gate_is_explicit(self):
        with tempfile.TemporaryDirectory() as td:
            try:
                result = persist_json_lines_batch_to_parquet(
                    [b'{"id":"W1"}'],
                    base_table="works",
                    index_key="id",
                    except_keys=[],
                    excepted_expand_dict=False,
                    sep="__",
                    parquet_dir=Path(td) / "parquet",
                    batch_idx=0,
                    index_offset=0,
                    record_contexts=None,
                    parallel_workers=0,
                    parser_backend="simd-json",
                )
            except RuntimeError as exc:
                self.assertIn("simd-json Cargo feature", str(exc))
            else:
                self.assertTrue(result["ok"])
                self.assertEqual(result["parser_backend"], "simd-json")


if __name__ == "__main__":
    unittest.main()
